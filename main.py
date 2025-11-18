# main.py
# Bybit MACD multi-timeframe scanner — updated to use external BybitWebSocketClient
# and improved REST parsing/signing safety. Minimal changes to integrate the
# separate bybit_client.py / bybit_ws_client.py provided by the user.

import os
import time
import hmac
import hashlib
import asyncio
import json
import zlib
import gzip
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import httpx
import aiosqlite
import websockets
from fastapi import FastAPI, Query, Depends, Header, HTTPException, status
from pydantic import BaseModel
from dotenv import load_dotenv

# Import the user's existing WS client (minimal change: use their client for public WS)
# The file bybit_ws_client.py you provided expects to be in the same package.
from bybit_ws_client import BybitWebSocketClient

load_dotenv()

# ---------- Configuration / env ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
BYBIT_USE_MAINNET = os.getenv("BYBIT_USE_MAINNET", "false").lower() == "true"
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "false").lower() == "true"
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "5"))
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
ROOT_SCAN_LOOKBACK = int(os.getenv("ROOT_SCAN_LOOKBACK", "3"))
DB_PATH = os.getenv("DB_PATH", "scanner.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "info").lower()

# New security / persistence settings
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")  # protect admin endpoints; set in production
MAX_RAW_WS_MESSAGES = int(os.getenv("MAX_RAW_WS_MESSAGES", "1000"))  # prune raw messages to this count
MAX_RAW_WS_MSG_BYTES = int(os.getenv("MAX_RAW_WS_MSG_BYTES", "2048"))  # truncate message length

# Hosts & endpoints
MAINNET_API_HOST = "https://api.bybit.com"
TESTNET_API_HOST = "https://api-testnet.bybit.com"
PRIMARY_API_HOST = MAINNET_API_HOST if BYBIT_USE_MAINNET else TESTNET_API_HOST
API_HOSTS = [PRIMARY_API_HOST]
if PRIMARY_API_HOST == MAINNET_API_HOST:
    API_HOSTS.append(TESTNET_API_HOST)
else:
    API_HOSTS.append(MAINNET_API_HOST)

# Websocket URLs (we will use the user's BybitWebSocketClient which uses the correct path)
PUBLIC_WS_URL = "wss://stream.bybit.com/v5/public" if BYBIT_USE_MAINNET else "wss://stream-testnet.bybit.com/v5/public"

# Private WS URL will be chosen only if TRADING_ENABLED is true (private connections only needed when trading)
if TRADING_ENABLED:
    PRIVATE_WS_URL = "wss://stream.bybit.com/v5/private" if BYBIT_USE_MAINNET else "wss://stream-testnet.bybit.com/v5/private"
else:
    PRIVATE_WS_URL = None

PUBLIC_ENDPOINT_CANDIDATES = [
    "/v5/market/instruments-info?category=linear",
    "/v5/market/instruments-info?category=perpetual",
    "/v5/market/instruments-info?category=linear&instType=PERPETUAL",
    "/v5/market/instruments-info",
    "/v5/market/symbols",
    "/v5/market/tickers",
    "/v2/public/symbols",
    "/v2/public/tickers",
]
KLINE_ENDPOINTS_TO_TRY = [
    "/v5/market/kline",
    "/v2/public/kline/list",
    "/v2/public/kline",
]

TF_MAP = {"5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "D"}
CANDIDATE_PUBLIC_TEMPLATES = [
    "klineV2.{interval}.{symbol}",
    "kline.{symbol}.{interval}",
    "klineV2:{interval}:{symbol}",
    "kline:{symbol}:{interval}",
    "kline:{interval}:{symbol}",
]

MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
LEVERAGE = int(os.getenv("LEVERAGE", "3"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.015"))
BREAKEVEN_PCT = float(os.getenv("BREAKEVEN_PCT", "0.005"))
STABLECOINS = {"USDT", "BUSD", "USDC", "TUSD", "DAI"}

# ---------- Globals ----------
app = FastAPI()
httpx_client = httpx.AsyncClient(timeout=20)
db: Optional[aiosqlite.Connection] = None

symbols_cache: Optional[List[Dict[str, Any]]] = None
active_root_signals: Dict[str, Dict[str, Any]] = {}
last_root_processed: Dict[str, int] = {}

# Use user's BybitWebSocketClient for public WS connectivity (minimal change)
public_ws: Optional[BybitWebSocketClient] = None
private_ws = None

# ---------- Utilities ----------
def log(*args, **kwargs):
    if LOG_LEVEL != "none":
        ts = datetime.now(timezone.utc).isoformat()
        print(ts, *args, **kwargs)

def now_ts_ms() -> int:
    return int(time.time() * 1000)

async def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram not configured; skipping message:", text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        r = await httpx_client.post(url, json=payload)
        if r.status_code != 200:
            log("Telegram send failed:", r.text)
    except Exception as e:
        log("Telegram error:", e)

# ---------- Admin auth dependency ----------
async def require_admin_auth(authorization: Optional[str] = Header(None), x_api_key: Optional[str] = Header(None)):
    """
    Protect admin endpoints with ADMIN_API_KEY.
    Accepts:
      Authorization: Bearer <ADMIN_API_KEY>
      or X-API-KEY: <ADMIN_API_KEY>
    If ADMIN_API_KEY not set, endpoints remain unprotected but a warning is logged.
    """
    if not ADMIN_API_KEY:
        log("WARNING: ADMIN_API_KEY not set — admin endpoints are UNPROTECTED. Set ADMIN_API_KEY in env for production.")
        return
    token = None
    if authorization:
        auth = authorization.strip()
        if auth.lower().startswith("bearer "):
            token = auth[7:].strip()
        else:
            token = auth
    if x_api_key:
        token = x_api_key.strip()
    if not token or token != ADMIN_API_KEY:
        log("Admin auth failed")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

# ---------- EMA / MACD ----------
def ema(values: List[float], period: int) -> List[float]:
    if not values or period <= 0:
        return []
    k = 2 / (period + 1)
    emas = []
    ema_prev = values[0]
    emas.append(ema_prev)
    for v in values[1:]:
        ema_prev = v * k + ema_prev * (1 - k)
        emas.append(ema_prev)
    return emas

def macd_hist(prices: List[float], fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL) -> List[Optional[float]]:
    if len(prices) < slow + signal:
        return []
    ema_fast = ema(prices, fast)
    ema_slow = ema(prices, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = ema(macd_line, signal)
    hist = [m - s for m, s in zip(macd_line[len(macd_line) - len(signal_line):], signal_line)]
    padding = len(prices) - len(hist)
    return [None] * padding + hist

# ---------- Resilient public GET ----------
async def resilient_public_get(endpoints: List[str], params: Dict[str, Any] = None, timeout: int = 12) -> Optional[Dict[str, Any]]:
    for host in API_HOSTS:
        for ep in endpoints:
            url = host + ep
            try:
                r = await httpx_client.get(url, params=params or {}, timeout=timeout)
            except Exception as e:
                log("Network error for", url, "->", e)
                continue
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    log("Invalid JSON from", url)
                    continue
            else:
                # safe excerpt logging (r.text is a string, not awaitable)
                body_excerpt = (r.text[:200] + "...") if r.text else ""
                log("Public GET", url, "returned", r.status_code, "body_excerpt:", body_excerpt)
    return None

# ---------- Signed request ----------
def bybit_sign_v5(api_secret: str, timestamp: str, method: str, path: str, body: str) -> str:
    # Ensure method is uppercased and body is empty string for GET
    prehash = timestamp + method.upper() + path + (body or "")
    return hmac.new(api_secret.encode(), prehash.encode(), hashlib.sha256).hexdigest()

async def bybit_signed_request(method: str, endpoint: str, payload: Dict[str, Any] = None):
    ts = str(int(time.time() * 1000))
    body = json.dumps(payload) if payload else ""
    signature = bybit_sign_v5(BYBIT_API_SECRET or "", ts, method.upper(), endpoint, body)
    headers = {
        "Content-Type": "application/json",
        "X-BAPI-API-KEY": BYBIT_API_KEY or "",
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-SIGN": signature,
    }
    url = PRIMARY_API_HOST + endpoint
    try:
        if method.upper() == "GET":
            r = await httpx_client.get(url, params=payload or {}, headers=headers)
        else:
            r = await httpx_client.post(url, content=body or "{}", headers=headers)
        try:
            return r.json()
        except Exception:
            # r.text is a string property; do not await it
            log("Signed request returned non-json", r.text)
            return {}
    except Exception as e:
        log("Signed request error:", e)
        return {}

# ---------- SQLite init & helpers ----------
async def init_db():
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("""CREATE TABLE IF NOT EXISTS root_signals (id TEXT PRIMARY KEY, symbol TEXT, root_tf TEXT, flip_time INTEGER, flip_price REAL, status TEXT, created_at INTEGER)""")
    await db.execute("""CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, symbol TEXT, side TEXT, qty REAL, entry_price REAL, sl_price REAL, created_at INTEGER, open BOOLEAN, raw_response TEXT)""")
    await db.execute("""CREATE TABLE IF NOT EXISTS raw_ws_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, topic TEXT, message TEXT, created_at INTEGER)""")
    await db.execute("""CREATE TABLE IF NOT EXISTS public_subscriptions (topic TEXT PRIMARY KEY, created_at INTEGER)""")
    await db.execute("""CREATE TABLE IF NOT EXISTS private_subscriptions (topic TEXT PRIMARY KEY, created_at INTEGER)""")
    await db.commit()
    log("DB initialized at", DB_PATH)

async def persist_root_signal(sig: Dict[str, Any]):
    await db.execute("INSERT OR REPLACE INTO root_signals (id,symbol,root_tf,flip_time,flip_price,status,created_at) VALUES (?,?,?,?,?,?,?)",
                     (sig["id"], sig["symbol"], sig["root_tf"], sig["root_flip_time"], sig["root_flip_price"], sig.get("status", "watching"), sig["created_at"]))
    await db.commit()

async def remove_root_signal(sig_id: str):
    await db.execute("DELETE FROM root_signals WHERE id = ?", (sig_id,))
    await db.commit()

async def persist_trade(tr: Dict[str, Any]):
    await db.execute("INSERT OR REPLACE INTO trades (id,symbol,side,qty,entry_price,sl_price,created_at,open,raw_response) VALUES (?,?,?,?,?,?,?,?,?)",
                     (tr["id"], tr["symbol"], tr["side"], tr["qty"], tr.get("entry_price"), tr.get("sl_price"), tr["created_at"], tr.get("open", True), json.dumps(tr.get("raw"))))
    await db.commit()

async def update_trade_close(trade_id: str):
    await db.execute("UPDATE trades SET open = 0 WHERE id = ?", (trade_id,))
    await db.commit()

# Updated persist_raw_ws with truncation and pruning
async def persist_raw_ws(source: str, topic: Optional[str], message: str):
    """
    Save raw websocket messages with truncation and DB pruning to avoid unbounded growth.
    """
    try:
        if not isinstance(message, str):
            message = str(message)
        if len(message) > MAX_RAW_WS_MSG_BYTES:
            message = message[:MAX_RAW_WS_MSG_BYTES] + "...[TRUNCATED]"
        await db.execute("INSERT INTO raw_ws_messages (source,topic,message,created_at) VALUES (?,?,?,?)",
                         (source, topic or "", message, now_ts_ms()))
        await db.commit()
        # prune oldest rows if over limit
        async with db.execute("SELECT COUNT(*) FROM raw_ws_messages") as cur:
            row = await cur.fetchone()
            total = row[0] if row else 0
        if total > MAX_RAW_WS_MESSAGES:
            to_delete = total - MAX_RAW_WS_MESSAGES
            await db.execute("DELETE FROM raw_ws_messages WHERE id IN (SELECT id FROM raw_ws_messages ORDER BY id ASC LIMIT ?)", (to_delete,))
            await db.commit()
    except Exception as e:
        log("persist_raw_ws error:", e)

async def add_public_subscription(topic: str):
    await db.execute("INSERT OR REPLACE INTO public_subscriptions (topic,created_at) VALUES (?,?)", (topic, now_ts_ms()))
    await db.commit()

async def remove_public_subscription(topic: str):
    await db.execute("DELETE FROM public_subscriptions WHERE topic = ?", (topic,))
    await db.commit()

async def add_private_subscription(topic: str):
    await db.execute("INSERT OR REPLACE INTO private_subscriptions (topic,created_at) VALUES (?,?)", (topic, now_ts_ms()))
    await db.commit()

async def remove_private_subscription(topic: str):
    await db.execute("DELETE FROM private_subscriptions WHERE topic = ?", (topic,))
    await db.commit()

# ---------- Symbols & klines ----------
async def get_tradable_usdt_symbols() -> List[str]:
    """
    Discover USDT perpetual symbols robustly. Prefer /v5/market/instruments-info variants,
    then fall back to older symbol endpoints. Filters by quoteCoin == USDT and
    contract/perpetual types where available.
    """
    global symbols_cache
    if symbols_cache:
        return [s["name"] for s in symbols_cache]

    # Try instrument-info variants first (these return rich metadata)
    instrument_endpoints = [
        "/v5/market/instruments-info?category=linear",
        "/v5/market/instruments-info?category=perpetual",
        "/v5/market/instruments-info?category=linear&instType=PERPETUAL",
        "/v5/market/instruments-info",
    ]
    res = await resilient_public_get(instrument_endpoints)
    # If no result from instruments-info, try the older public endpoints
    if not res:
        res = await resilient_public_get([ep for ep in PUBLIC_ENDPOINT_CANDIDATES if "instruments-info" not in ep])
    if not res:
        log("Symbol discovery failed on all endpoints/hosts")
        return []

    # robustly extract the actual list of symbol items from various response shapes:
    result_list = []
    if isinstance(res, dict):
        maybe = res.get("result") or res.get("data") or res.get("symbols") or res.get("list") or res
    else:
        maybe = res

    # If maybe is dict and contains category->list or list directly
    if isinstance(maybe, dict):
        if isinstance(maybe.get("list"), list):
            result_list = maybe.get("list")
        elif isinstance(maybe.get("data"), list):
            result_list = maybe.get("data")
        elif isinstance(maybe.get("symbols"), list):
            result_list = maybe.get("symbols")
        else:
            # attempt to find any nested list that looks like instrument list
            for k, v in maybe.items():
                if isinstance(v, list):
                    # further validate (list of dicts containing 'symbol' or 'name')
                    if v and isinstance(v[0], dict) and ("symbol" in v[0] or "name" in v[0] or "s" in v[0]):
                        result_list = v
                        break
    elif isinstance(maybe, list):
        result_list = maybe
    else:
        result_list = []

    symbols = []
    for item in result_list:
        name = None
        if isinstance(item, dict):
            # prefer explicit fields
            name = item.get("symbol") or item.get("name") or item.get("symbolName") or item.get("s")
            quote = (item.get("quoteCoin") or item.get("quote") or item.get("quoteCurrency") or item.get("quote_currency") or "").upper()
            contract = (item.get("contractType") or item.get("instType") or item.get("contract_type") or item.get("instrumentType") or "").lower()
        else:
            name = str(item)
            quote = ""
            contract = ""

        if not name:
            continue

        # Only care about USDT-denominated perpetual/linear contracts
        if not name.endswith("USDT"):
            continue

        # If response includes explicit quoteCoin info, ensure it's USDT
        if quote and quote != "USDT":
            continue

        # If contract info exists, require perpetual/linear/perpetual-like types
        if contract and not any(x in contract for x in ("perpetual", "linear")):
            continue

        base = name.replace("USDT", "")
        if base.upper() in STABLECOINS:
            continue

        symbols.append({"name": name, "raw": item})

    # If we found none, persist a short sample for debugging and log keys for quick inspection
    if not symbols:
        try:
            sample = json.dumps(maybe)[:1500]
            log("Symbol discovery: result_list empty — maybe keys:", list(maybe.keys()) if isinstance(maybe, dict) else type(maybe))
            await persist_raw_ws("http", "instruments-info-sample", sample)
            log("Persisted instruments-info sample to raw_ws_messages (use /debug/ws/messages to inspect).")
        except Exception as e:
            log("Error persisting instruments-info sample:", e)

    symbols_cache = symbols
    log("Discovered", len(symbols), "USDT symbols")
    return [s["name"] for s in symbols if s.get("name")]

async def fetch_klines(symbol: str, timeframe: str, limit: int = 100) -> List[Dict[str, Any]]:
    params = {"symbol": symbol, "interval": timeframe, "limit": limit}
    for host in API_HOSTS:
        for ep in KLINE_ENDPOINTS_TO_TRY:
            url = host + ep
            try:
                r = await httpx_client.get(url, params=params, timeout=12)
            except Exception as e:
                log("Kline network error", url, e)
                continue
            if r.status_code != 200:
                continue
            try:
                data = r.json()
            except Exception:
                log("Invalid JSON from", url)
                continue
            res = data.get("result") or data.get("data") or data.get("list") or []
            klist = []
            if isinstance(res, dict) and "list" in res:
                klist = res["list"]
            elif isinstance(res, list):
                klist = res
            elif isinstance(data.get("result"), dict) and "list" in data["result"]:
                klist = data["result"]["list"]
            # normalize
            klines = []
            for k in klist:
                try:
                    o = float(k.get("open", k.get("Open", 0)))
                    c = float(k.get("close", k.get("Close", 0)))
                    h = float(k.get("high", 0))
                    l = float(k.get("low", 0))
                    start = int(k.get("start", k.get("open_time", k.get("t", 0))))
                    end = int(k.get("end", k.get("close_time", 0)))
                    klines.append({"open": o, "high": h, "low": l, "close": c, "start": start, "end": end})
                except Exception:
                    continue
            if klines:
                return klines
    log("fetch_klines: no klines found for", symbol, timeframe)
    return []

# ---------- Signal utilities ----------
def check_macd_flip_recent(klines: List[Dict[str, Any]], lookback: int = ROOT_SCAN_LOOKBACK) -> Optional[int]:
    closes = [k["close"] for k in klines]
    hist = macd_hist(closes)
    n = len(hist)
    if n < 2:
        return None
    start_idx = max(0, n - lookback - 5)
    for i in range(start_idx, n - 1):
        h = hist[i]
        if h is None:
            continue
        if h < 0:
            for j in range(i + 1, n):
                hj = hist[j]
                if hj is None:
                    continue
                if hj > 0:
                    return j
    return None

def macd_positive(klines: List[Dict[str, Any]]) -> bool:
    closes = [k["close"] for k in klines]
    hist = macd_hist(closes)
    if not hist:
        return False
    last = hist[-1]
    return bool(last and last > 0)

# ---------- Trading helpers ----------
async def get_account_usdt_balance() -> float:
    if not BYBIT_API_KEY or not BYBIT_API_SECRET or not TRADING_ENABLED:
        simulated = 1000.0
        return simulated
    try:
        res = await bybit_signed_request("GET", "/v5/account/wallet-balance", {"coin": "USDT"})
        result = res.get("result") or {}
        coin = result.get("USDT") or {}
        total = float(coin.get("walletBalance") or coin.get("wallet_balance") or coin.get("availableBalance") or 0)
        return total
    except Exception as e:
        log("get_account_usdt_balance error:", e)
        return 0.0

def compute_qty_from_usdt(usdt_amount: float, price: float, leverage: int = LEVERAGE) -> float:
    if price <= 0:
        return 0.0
    qty = (usdt_amount * leverage) / price
    return float(int(qty * 1000) / 1000.0)

async def place_market_entry_and_stop(symbol: str, side: str, qty: float, stop_price: float) -> Dict[str, Any]:
    if not TRADING_ENABLED or not BYBIT_API_KEY or not BYBIT_API_SECRET:
        klines = await fetch_klines(symbol, TF_MAP["5m"], limit=2)
        entry_price = klines[-1]["close"] if klines else 0.0
        trade_id = f"sim-{symbol}-{int(time.time())}"
        tr = {"id": trade_id, "symbol": symbol, "side": side, "qty": qty, "entry_price": entry_price, "sl_price": stop_price, "created_at": now_ts_ms(), "open": True, "raw": {"simulated": True}}
        await persist_trade(tr)
        if private_ws:
            try:
                await private_ws.subscribe_topic(f"order.{symbol}")
            except Exception:
                pass
        await send_telegram(f"[SIM] Entered {symbol} {side} qty={qty} entry={entry_price} SL={stop_price}")
        return {"retCode": 0, "result": tr}
    try:
        entry_payload = {"category": "linear", "symbol": symbol, "side": side, "orderType": "Market", "qty": str(qty), "timeInForce": "ImmediateOrCancel", "reduceOnly": False, "closeOnTrigger": False}
        entry_res = await bybit_signed_request("POST", "/v5/order/create", entry_payload)
        res_obj = entry_res.get("result") or {}
        entry_id = res_obj.get("orderId") or res_obj.get("order_id") or f"by-{int(time.time())}"
        filled_avg = res_obj.get("filled_avg_price") or res_obj.get("filledAvgPrice")
        entry_price = float(filled_avg) if filled_avg else None
        stop_side = "Sell" if side.lower() in ("buy", "long") else "Buy"
        stop_payload = {"category": "linear", "symbol": symbol, "side": stop_side, "orderType": "Market", "qty": str(qty), "triggerBy": "LastPrice", "basePrice": None, "triggerPrice": str(stop_price), "timeInForce": "ImmediateOrCancel"}
        stop_res = await bybit_signed_request("POST", "/v5/stop-order/create", stop_payload)
        tr = {"id": entry_id, "symbol": symbol, "side": side, "qty": qty, "entry_price": entry_price, "sl_price": stop_price, "created_at": now_ts_ms(), "open": True, "raw": {"entry": entry_res, "stop": stop_res}}
        await persist_trade(tr)
        if private_ws:
            try:
                await private_ws.subscribe_topic(f"order.{symbol}")
            except Exception:
                pass
        await send_telegram(f"Order placed {symbol} side={side} qty={qty} entry_id={entry_id}")
        return {"retCode": 0, "result": tr}
    except Exception as e:
        log("place_market_entry_and_stop error:", e)
        return {"retCode": -1, "retMsg": str(e)}

# ---------- Adapter to dispatch BybitWebSocketClient messages into on_kline_event ----------
async def _ws_kline_dispatcher(data):
    """
    Adapter callback for BybitWebSocketClient subscriptions.
    It parses incoming WS message shapes and calls on_kline_event(symbol, interval_label, candle).
    """
    try:
        if not isinstance(data, dict):
            return
        topic = data.get("topic") or ""
        # Try to extract symbol and interval token from topic string
        parts = str(topic).replace(":", ".").split(".")
        interval_token = None
        symbol_token = None
        for p in parts:
            if isinstance(p, str) and p.endswith("USDT"):
                symbol_token = p
            if isinstance(p, str) and (p.isdigit() or p.endswith("m") or p.endswith("h") or p in TF_MAP.values()):
                interval_token = p
        interval_label = None
        if interval_token:
            for k, v in TF_MAP.items():
                if v == interval_token or interval_token == k:
                    interval_label = k
                    break
        # extract close/start from payload data
        payload = data.get("data") or []
        if isinstance(payload, list) and payload:
            first = payload[0]
            start = first.get("start") or first.get("t")
            close_val = first.get("close") or first.get("c") or first.get("close_price")
            try:
                close = float(close_val) if close_val is not None else None
            except Exception:
                close = None
            if symbol_token and interval_label and close is not None:
                await on_kline_event(symbol_token, interval_label, {"start": start, "close": close})
    except Exception as e:
        log("WS dispatch error:", e)

# ---------- Event handlers ----------
async def on_kline_event(symbol: str, interval: str, candle: Dict[str, Any]):
    if not symbol or not interval:
        return
    for key, sig in list(active_root_signals.items()):
        if sig["symbol"] != symbol:
            continue
        root_tf = sig["root_tf"]
        check_tfs = ["5m", "15m", "4h", "1d"] if root_tf == "1h" else ["5m", "15m", "1h", "1d"]
        if interval not in check_tfs:
            continue
        asyncio.create_task(evaluate_signal_fast(sig["id"]))

async def on_private_msg(obj: dict):
    try:
        topic = obj.get("topic") or (obj.get("arg") or {}).get("topic") or ""
        data = obj.get("data") or []
        if isinstance(data, list) and data:
            item = data[0]
            order_id = item.get("orderId") or item.get("order_id") or item.get("orderID")
            status = item.get("orderStatus") or item.get("status") or item.get("execStatus")
            filled_price = item.get("filled_avg_price") or item.get("price") or item.get("avgPrice")
            symbol = item.get("symbol") or item.get("s")
            if order_id and status and str(status).lower() in ("filled", "closed", "cancelled", "canceled", "triggered"):
                try:
                    await update_trade_close(order_id)
                except Exception as e:
                    log("DB update error:", e)
                if str(status).lower() == "filled":
                    await send_telegram(f"Order {order_id} {symbol} status={status} filled_price={filled_price}")
    except Exception as e:
        log("on_private_msg error:", e)

# ---------- Scanning & watcher ----------
async def root_scanner_loop(root_tf: str):
    tf_token = TF_MAP[root_tf]
    log("Root scanner started for", root_tf)
    while True:
        try:
            symbols = await get_tradable_usdt_symbols()
            for symbol in symbols:
                base = symbol.replace("USDT", "")
                if base.upper() in STABLECOINS:
                    continue
                klines = await fetch_klines(symbol, tf_token, limit=ROOT_SCAN_LOOKBACK + 3)
                if not klines:
                    continue
                last_candle_end = klines[-1]["end"]
                last_processed = last_root_processed.get(f"{symbol}-{root_tf}", 0)
                if last_candle_end == last_processed:
                    continue
                flip_idx = check_macd_flip_recent(klines, lookback=ROOT_SCAN_LOOKBACK)
                if flip_idx is not None:
                    flip_candle = klines[flip_idx]
                    key = f"{symbol}-{root_tf}-{flip_candle['start']}"
                    sig = {"id": key, "symbol": symbol, "root_tf": root_tf, "root_flip_time": flip_candle["start"], "root_flip_candle_end": flip_candle["end"], "root_flip_price": flip_candle["close"], "created_at": now_ts_ms(), "status": "watching"}
                    active_root_signals[key] = sig
                    await persist_root_signal(sig)
                    await send_telegram(f"Root flip detected {symbol} {root_tf} @ {flip_candle['close']}")
                    # subscribe via user's BybitWebSocketClient adapter (pass dispatcher)
                    if public_ws:
                        try:
                            await public_ws.subscribe_kline(symbol, TF_MAP["5m"], _ws_kline_dispatcher)
                            topic = f"kline.{TF_MAP['5m']}.{symbol}"
                            await add_public_subscription(topic)
                        except Exception as e:
                            log("Public WS subscribe (5m) failed:", e)
                        try:
                            await public_ws.subscribe_kline(symbol, TF_MAP["15m"], _ws_kline_dispatcher)
                            topic = f"kline.{TF_MAP['15m']}.{symbol}"
                            await add_public_subscription(topic)
                        except Exception as e:
                            log("Public WS subscribe (15m) failed:", e)
                last_root_processed[f"{symbol}-{root_tf}"] = last_candle_end
        except Exception as e:
            log("root_scanner_loop error:", e)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

async def evaluate_signal_fast(sig_id: str):
    sig = active_root_signals.get(sig_id)
    if not sig:
        return
    symbol = sig["symbol"]
    root_tf = sig["root_tf"]
    root_end = sig["root_flip_candle_end"]
    now_ms = now_ts_ms()
    if now_ms > (root_end * 1000):
        active_root_signals.pop(sig_id, None)
        await remove_root_signal(sig_id)
        # Attempt to unsubscribe via BybitWebSocketClient (best-effort)
        if public_ws and getattr(public_ws, "ws", None):
            try:
                topic5 = f"kline.{TF_MAP['5m']}.{symbol}"
                await public_ws.ws.send_json({"op": "unsubscribe", "args": [topic5]})
                public_ws.callbacks.pop(topic5, None)
                await remove_public_subscription(topic5)
                log("Public WS unsubscribed", topic5)
            except Exception as e:
                log("Public unsubscribe failed:", e)
            try:
                topic15 = f"kline.{TF_MAP['15m']}.{symbol}"
                await public_ws.ws.send_json({"op": "unsubscribe", "args": [topic15]})
                public_ws.callbacks.pop(topic15, None)
                await remove_public_subscription(topic15)
                log("Public WS unsubscribed", topic15)
            except Exception as e:
                log("Public unsubscribe failed:", e)
        if private_ws:
            try:
                await private_ws.unsubscribe_topic(f"order.{symbol}")
            except Exception:
                pass
        log("Signal expired and cleaned:", sig_id)
        return
    check_tfs = ["5m", "15m", "4h", "1d"] if root_tf == "1h" else ["5m", "15m", "1h", "1d"]
    alignment_ok = True
    for tf in check_tfs:
        klines = await fetch_klines(symbol, TF_MAP[tf], limit=10)
        if not klines or not macd_positive(klines):
            alignment_ok = False
            break
    if not alignment_ok:
        return
    most_recent_flip_tf = None
    most_recent_flip_ts = 0
    for tf in check_tfs:
        klines = await fetch_klines(symbol, TF_MAP[tf], limit=20)
        idx = check_macd_flip_recent(klines, lookback=5)
        if idx is not None:
            flip_ts = klines[idx]["start"]
            if flip_ts > most_recent_flip_ts:
                most_recent_flip_ts = flip_ts
                most_recent_flip_tf = tf
    if not most_recent_flip_tf:
        return
    if sig.get("status") == "acted":
        return
    sig["status"] = "acted"
    await persist_root_signal(sig)
    await send_telegram(f"Signal: {symbol} root={root_tf} last_flip_tf={most_recent_flip_tf} price={sig['root_flip_price']}")
    balance = await get_account_usdt_balance()
    per_trade = max(1.0, balance / max(1, MAX_OPEN_TRADES))
    suggested_price = sig.get("root_flip_price") or 0.0
    qty = compute_qty_from_usdt(per_trade, suggested_price or 1.0)
    side = "Buy"
    stop_price = round((suggested_price or 1.0) * (1 - STOP_LOSS_PCT), 8)
    async with db.execute("SELECT COUNT(*) FROM trades WHERE open = 1") as cur:
        row = await cur.fetchone()
        open_count = row[0] if row else 0
    if open_count >= MAX_OPEN_TRADES:
        await send_telegram(f"Max open trades reached ({MAX_OPEN_TRADES}), skipping entry for {symbol}")
        return
    res = await place_market_entry_and_stop(symbol, side, qty, stop_price)
    log("place result:", res)

async def active_signal_watcher_loop():
    while True:
        try:
            for sig_id in list(active_root_signals.keys()):
                await evaluate_signal_fast(sig_id)
        except Exception as e:
            log("active_signal_watcher_loop error:", e)
        await asyncio.sleep(60)

# ---------- Debug endpoints (protected) ----------
@app.get("/debug/ws/messages")
async def debug_ws_messages(limit: int = Query(50, gt=0, le=1000), _auth=Depends(require_admin_auth)):
    rows = []
    async with db.execute("SELECT id,source,topic,message,created_at FROM raw_ws_messages ORDER BY id DESC LIMIT ?", (limit,)) as cur:
        rows = await cur.fetchall()
    out = []
    for r in rows:
        try:
            msg = json.loads(r[3]) if r[3] else None
        except Exception:
            msg = r[3]
        out.append({"id": r[0], "source": r[1], "topic": r[2], "message": msg, "created_at": r[4]})
    return {"count": len(out), "messages": out}

@app.post("/debug/ws/clear")
async def debug_ws_clear(_auth=Depends(require_admin_auth)):
    await db.execute("DELETE FROM raw_ws_messages")
    await db.commit()
    return {"cleared": True}

# ---------- Status / control (protected) ----------
class ToggleRequest(BaseModel):
    trading_enabled: Optional[bool] = None
    max_open_trades: Optional[int] = None

@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

# Respond to GET and HEAD explicitly to avoid 405 from some proxies/health checks
@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"service": "bybit-macd-scanner", "status": "ok", "public_ws": PUBLIC_WS_URL, "private_ws": PRIVATE_WS_URL}

@app.post("/toggle")
async def toggle(req: ToggleRequest, _auth=Depends(require_admin_auth)):
    global TRADING_ENABLED, MAX_OPEN_TRADES, PRIVATE_WS_URL, private_ws
    if req.trading_enabled is not None:
        TRADING_ENABLED = req.trading_enabled
        # update PRIVATE_WS_URL and startup/teardown of private_ws accordingly
        if TRADING_ENABLED:
            PRIVATE_WS_URL = "wss://stream.bybit.com/v5/private" if BYBIT_USE_MAINNET else "wss://stream-testnet.bybit.com/v5/private"
            # if private_ws not running, create and start it
            if not private_ws and PRIVATE_WS_URL:
                private_ws = PrivateWebsocketManager(PRIVATE_WS_URL, BYBIT_API_KEY, BYBIT_API_SECRET)
                asyncio.create_task(private_ws.connect_and_auth())
        else:
            # disable private ws if running
            try:
                if private_ws:
                    asyncio.create_task(private_ws.close())
            except Exception:
                pass
            private_ws = None
            PRIVATE_WS_URL = None
    if req.max_open_trades is not None:
        MAX_OPEN_TRADES = req.max_open_trades
    return {"trading_enabled": TRADING_ENABLED, "max_open_trades": MAX_OPEN_TRADES}

@app.get("/status")
async def status(_auth=Depends(require_admin_auth)):
    async with db.execute("SELECT id,symbol,root_tf,flip_time,status FROM root_signals") as cur:
        roots = await cur.fetchall()
    async with db.execute("SELECT id,symbol,side,qty,entry_price,sl_price,created_at,open FROM trades") as cur:
        trades = await cur.fetchall()
    async with db.execute("SELECT topic,created_at FROM private_subscriptions") as cur:
        priv = await cur.fetchall()
    async with db.execute("SELECT topic,created_at FROM public_subscriptions") as cur:
        pub = await cur.fetchall()
    return {
        "active_root_signals": [dict(id=r[0], symbol=r[1], root_tf=r[2], flip_time=r[3], status=r[4]) for r in roots],
        "trades": [dict(id=t[0], symbol=t[1], side=t[2], qty=t[3], entry_price=t[4], sl_price=t[5], created_at=t[6], open=bool(t[7])) for t in trades],
        "private_subscriptions": [dict(topic=p[0], created_at=p[1]) for p in priv],
        "public_subscriptions": [dict(topic=p[0], created_at=p[1]) for p in pub],
        "trading_enabled": TRADING_ENABLED,
        "max_open_trades": MAX_OPEN_TRADES,
        "public_ws_connected": bool(public_ws and getattr(public_ws, "ws", None)),
        "private_ws_connected": bool(private_ws and private_ws.conn and private_ws.authenticated) if private_ws else False,
    }

# ---------- Startup ----------
@app.on_event("startup")
async def startup():
    global public_ws, private_ws
    await init_db()

    # debug log to show endpoints in use (helpful for diagnosing 404s)
    log("Startup config:",
        "BYBIT_USE_MAINNET=", BYBIT_USE_MAINNET,
        "TRADING_ENABLED=", TRADING_ENABLED,
        "API_HOSTS=", API_HOSTS,
        "PUBLIC_WS_URL=", PUBLIC_WS_URL,
        "PRIVATE_WS_URL=", PRIVATE_WS_URL)

    # Use the user's BybitWebSocketClient for public WS (minimal integration changes)
    public_ws = BybitWebSocketClient(category="linear")
    # connect in background (it will create its own session and listen task)
    asyncio.create_task(public_ws.connect())

    # Only create and start private websocket manager when trading is enabled
    if TRADING_ENABLED and PRIVATE_WS_URL:
        private_ws = PrivateWebsocketManager(PRIVATE_WS_URL, BYBIT_API_KEY, BYBIT_API_SECRET)
        asyncio.create_task(private_ws.connect_and_auth())

    asyncio.create_task(root_scanner_loop("1h"))
    asyncio.create_task(root_scanner_loop("4h"))
    asyncio.create_task(active_signal_watcher_loop())
    log("Background tasks started")

# ---------- Run note ----------
# Start with:
# uvicorn main:app --host 0.0.0.0 --port 8000
