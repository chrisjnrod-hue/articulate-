# main.py
# Bybit MACD multi-timeframe scanner — updated to fetch USDT perpetuals and
# subscribe/unsubscribe only 5m and 15m klines for root signals.

import os
import time
import hmac
import hashlib
import asyncio
import json
import zlib
import gzip
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import httpx
import aiosqlite
import websockets
from fastapi import FastAPI, Query, Depends, Header, HTTPException, status
from pydantic import BaseModel
from dotenv import load_dotenv

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

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
MAX_RAW_WS_MESSAGES = int(os.getenv("MAX_RAW_WS_MESSAGES", "1000"))
MAX_RAW_WS_MSG_BYTES = int(os.getenv("MAX_RAW_WS_MSG_BYTES", "2048"))

# Hosts & endpoints
MAINNET_API_HOST = "https://api.bybit.com"
TESTNET_API_HOST = "https://api-testnet.bybit.com"
PRIMARY_API_HOST = MAINNET_API_HOST if BYBIT_USE_MAINNET else TESTNET_API_HOST
API_HOSTS = [PRIMARY_API_HOST]
if PRIMARY_API_HOST == MAINNET_API_HOST:
    API_HOSTS.append(TESTNET_API_HOST)
else:
    API_HOSTS.append(MAINNET_API_HOST)

# Use the linear public WS path for linear (USDT) data
PUBLIC_WS_URL = "wss://stream.bybit.com/v5/public/linear" if BYBIT_USE_MAINNET else "wss://stream-testnet.bybit.com/v5/public/linear"
PRIVATE_WS_URL = ("wss://stream.bybit.com/v5/private" if BYBIT_USE_MAINNET else "wss://stream-testnet.bybit.com/v5/private") if TRADING_ENABLED else None

# REST endpoints to attempt for instruments and klines
INSTRUMENTS_ENDPOINTS = [
    "/v5/market/instruments-info?category=linear&instType=PERPETUAL",
    "/v5/market/instruments-info?category=linear",
    "/v5/market/instruments-info?category=perpetual",
    "/v5/market/instruments-info",
]
KLINE_ENDPOINTS = ["/v5/market/kline", "/v2/public/kline/list", "/v2/public/kline"]

TF_MAP = {"5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "D"}
CANDIDATE_PUBLIC_TEMPLATES = [
    "klineV2.{interval}.{symbol}",
    "kline.{interval}.{symbol}",
    "klineV2:{interval}:{symbol}",
    "kline:{interval}:{symbol}",
    "kline:{symbol}:{interval}",
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

public_ws = None
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
    if not ADMIN_API_KEY:
        log("WARNING: ADMIN_API_KEY not set — admin endpoints are UNPROTECTED.")
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
                body_excerpt = (r.text[:200] + "...") if r.text else ""
                log("Public GET", url, "returned", r.status_code, "body_excerpt:", body_excerpt)
    return None

# ---------- Signed request ----------
def bybit_sign_v5(api_secret: str, timestamp: str, method: str, path: str, body: str) -> str:
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

async def persist_raw_ws(source: str, topic: Optional[str], message: str):
    try:
        if not isinstance(message, str):
            message = str(message)
        if len(message) > MAX_RAW_WS_MSG_BYTES:
            message = message[:MAX_RAW_WS_MSG_BYTES] + "...[TRUNCATED]"
        await db.execute("INSERT INTO raw_ws_messages (source,topic,message,created_at) VALUES (?,?,?,?)",
                         (source, topic or "", message, now_ts_ms()))
        await db.commit()
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

# ---------- Helper functions added ----------
async def get_tradable_usdt_symbols() -> List[str]:
    """
    Return list of symbols that are linear PERPETUAL and quote USDT and status Trading.
    Will try several endpoints for compatibility.
    """
    resp = await resilient_public_get(INSTRUMENTS_ENDPOINTS)
    if not resp:
        log("Could not fetch instruments; empty list returned")
        return []
    items = []
    try:
        items = resp.get("result", {}).get("list", []) or resp.get("data", []) or resp.get("list", [])
    except Exception:
        items = resp.get("list", []) or []
    out = []
    for it in items:
        try:
            symbol = it.get("symbol") or it.get("name")
            status = it.get("status") or it.get("state") or ""
            quote = (it.get("quoteCoin") or it.get("quoteAsset") or it.get("quote")).upper() if it else ""
            inst_type = (it.get("instType") or it.get("type") or "").upper()
            # prefer PERPETUAL linear contracts quoting USDT and currently Trading
            if not symbol:
                continue
            if quote != "USDT":
                continue
            if isinstance(status, str) and status.lower() != "trading":
                continue
            # If instType present check PERPETUAL or similar
            if inst_type and "PERPETUAL" not in inst_type and "PERP" not in inst_type:
                # still allow if unclear
                pass
            out.append(symbol)
        except Exception:
            continue
    log("Found", len(out), "USDT perpetual symbols")
    return out

async def fetch_klines(symbol: str, interval_token: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Fetch klines via v5 kline endpoint. interval_token should be the token used in TF_MAP ('5','15','60', etc).
    Returns list of candles as dicts with keys: start, end, close
    """
    params = {"category": "linear", "symbol": symbol, "interval": str(interval_token), "limit": limit}
    resp = await resilient_public_get([KLINE_ENDPOINTS[0]], params=params)
    if not resp:
        return []
    # v5 returns result.list usually
    items = resp.get("result", {}).get("list", []) or resp.get("list", []) or resp.get("data", [])
    candles = []
    for it in items:
        # Support multiple possible key names
        start = it.get("start") or it.get("t") or it.get("open_time")
        end = it.get("end") or it.get("close_time") or None
        close_val = it.get("close") or it.get("c") or it.get("closePrice")
        try:
            close = float(close_val) if close_val is not None else None
        except Exception:
            close = None
        if start is None or close is None:
            continue
        candles.append({"start": int(start), "end": int(end) if end else None, "close": close})
    return candles

def check_macd_flip_recent(klines: List[Dict[str, Any]], lookback: int = 3) -> Optional[int]:
    """
    Return index in klines list where MACD histogram flipped from <=0 to >0 within lookback (searching latest region).
    Returns index in klines (0-based) or None.
    """
    closes = [c["close"] for c in klines if c.get("close") is not None]
    if len(closes) < MACD_SLOW + MACD_SIGNAL:
        return None
    hist = macd_hist(closes)
    if not hist:
        return None
    # find flips where previous <=0 and current >0
    for idx in range(len(hist)-1, max(-1, len(hist)-1 - lookback), -1):
        if idx <= 0:
            continue
        prev = hist[idx-1] or 0
        cur = hist[idx] or 0
        if prev <= 0 and cur > 0:
            return idx
    return None

def macd_positive(klines: List[Dict[str, Any]]) -> bool:
    closes = [c["close"] for c in klines if c.get("close") is not None]
    if not closes:
        return False
    hist = macd_hist(closes)
    if not hist:
        return False
    last = hist[-1]
    return last is not None and last > 0

def compute_qty_from_usdt(usdt_amount: float, price: float) -> float:
    if price <= 0:
        return 0.0
    # simple qty = usdt / price
    qty = usdt_amount / price
    return round(qty, 6)

async def get_account_usdt_balance() -> float:
    # conservative default / no-auth fallback
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        return 1000.0
    # try to call account endpoints (safe fallback if auth not present will return None)
    try:
        res = await bybit_signed_request("GET", "/v5/account/wallet-balance", {"coin": "USDT"})
        # parse for available USDT balance
        bal = 0.0
        data = res.get("result", {}).get("list", []) if isinstance(res, dict) else []
        if data:
            for item in data:
                if item.get("coin") == "USDT":
                    bal = float(item.get("availableBalance") or item.get("available", 0) or 0)
                    break
        return bal or 0.0
    except Exception:
        return 0.0

async def place_market_entry_and_stop(symbol: str, side: str, qty: float, stop_price: float) -> Dict[str, Any]:
    """
    Placeholder: don't place real orders unless you intentionally enable trading.
    If TRADING_ENABLED is False this returns a simulated response.
    Replace with your real order-placement logic if you enable trading.
    """
    if not TRADING_ENABLED:
        log("Trading disabled - simulated order", symbol, side, qty, stop_price)
        order_id = str(uuid.uuid4())
        tr = {"id": order_id, "symbol": symbol, "side": side, "qty": qty, "entry_price": None, "sl_price": stop_price, "created_at": now_ts_ms(), "open": True, "raw": {"simulated": True}}
        await persist_trade(tr)
        return {"simulated": True, "order_id": order_id}
    # Example of how you might place order; left as stub for safety.
    try:
        payload = {
            "category": "linear",
            "symbol": symbol,
            "side": side.upper(),
            "orderType": "Market",
            "qty": qty,
            "timeInForce": "ImmediateOrCancel",
            "reduceOnly": False,
            "closeOnTrigger": False,
        }
        res = await bybit_signed_request("POST", "/v5/order/create", payload)
        return res
    except Exception as e:
        log("place_market_entry_and_stop error:", e)
        return {"error": str(e)}

# ---------- WebSocket managers (public & private) ----------
class PublicWebsocketManager:
    def __init__(self, ws_url: str, detect_symbol: str = "BTCUSDT"):
        self.ws_url = ws_url
        self.conn = None
        self.detect_template: Optional[str] = None
        self.subscribed_topics: set = set()
        self.detect_symbol = detect_symbol
        self._recv_task = None
        self._lock = asyncio.Lock()
        self._reconnect_backoff = 1
        self._stop = False

    def _maybe_decompress(self, msg):
        try:
            if isinstance(msg, str):
                return msg
            if isinstance(msg, bytes):
                try:
                    return gzip.decompress(msg).decode("utf-8")
                except Exception:
                    pass
                try:
                    return zlib.decompress(msg, -zlib.MAX_WBITS).decode("utf-8")
                except Exception:
                    pass
                try:
                    return msg.decode("utf-8")
                except Exception:
                    pass
            return None
        except Exception:
            return None

    async def connect_and_detect(self, timeout: float = 8.0):
        if not await self._connect():
            return False
        try:
            detected = await self._auto_detect_template(timeout=timeout)
            if detected:
                self.detect_template = detected
                log("Public WS template detected:", detected)
            else:
                log("Public WS auto-detect failed; will attempt on-the-fly")
        except Exception as e:
            log("Public detect error:", e)
        if not self._recv_task:
            self._recv_task = asyncio.create_task(self._recv_loop())
        return True

    async def _connect(self):
        backoff = self._reconnect_backoff
        while not self._stop:
            try:
                self.conn = await websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10, max_size=2**24)
                log("Public WS connected")
                self._reconnect_backoff = 1
                return True
            except Exception as e:
                log("Public WS connect failed:", e, "retrying in", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
        return False

    async def _auto_detect_template(self, timeout: float = 8.0) -> Optional[str]:
        async def _recv_for(seconds: float, expect_topics: set):
            end = time.time() + seconds
            while time.time() < end:
                try:
                    msg = await asyncio.wait_for(self.conn.recv(), timeout=max(0.5, end - time.time()))
                except asyncio.TimeoutError:
                    continue
                body = self._maybe_decompress(msg)
                if not body:
                    continue
                try:
                    obj = json.loads(body)
                except Exception:
                    continue
                # check for topic or symbol presence
                topic = obj.get("topic") or (obj.get("arg") or {}).get("topic") or obj.get("type")
                if topic and isinstance(topic, str):
                    for t in expect_topics:
                        if t in topic:
                            return True, obj
                data = obj.get("data") or []
                if isinstance(data, list) and data:
                    first = data[0]
                    s = first.get("symbol") or first.get("s")
                    if s and self.detect_symbol in s:
                        return True, obj
            return False, None

        for tmpl in CANDIDATE_PUBLIC_TEMPLATES:
            try:
                t1 = tmpl.format(interval=TF_MAP["5m"], symbol=self.detect_symbol)
                t2 = tmpl.format(interval=TF_MAP["15m"], symbol=self.detect_symbol)
                topics = [t1, t2]
                await self.conn.send(json.dumps({"op": "subscribe", "args": topics}))
                ok, obj = await _recv_for(timeout, set(topics))
                if ok:
                    return tmpl
                try:
                    await self.conn.send(json.dumps({"op": "unsubscribe", "args": topics}))
                except Exception:
                    pass
            except Exception:
                continue
        return None

    async def _recv_loop(self):
        while not self._stop:
            if not self.conn:
                await self._connect()
                await self._resubscribe_all()
            try:
                raw = await self.conn.recv()
                body = self._maybe_decompress(raw)
                if not body:
                    continue
                try:
                    obj = json.loads(body)
                except Exception:
                    continue
                topic = obj.get("topic") or (obj.get("arg") or {}).get("topic") or ""
                await persist_raw_ws("public", topic, json.dumps(obj))
                asyncio.create_task(self.handle_message(obj))
            except Exception as e:
                log("Public WS recv error/disconnect:", e)
                try:
                    if self.conn:
                        await self.conn.close()
                except Exception:
                    pass
                self.conn = None
                await asyncio.sleep(min(self._reconnect_backoff, 60))
                self._reconnect_backoff = min(self._reconnect_backoff * 2 if self._reconnect_backoff > 0 else 1, 60)

    async def handle_message(self, obj):
        topic = obj.get("topic") or (obj.get("arg") or {}).get("topic") or obj.get("type")
        data = obj.get("data") or []
        if topic and "kline" in str(topic):
            topic_str = str(topic)
            sep = "." if "." in topic_str else ":"
            parts = topic_str.split(sep)
            interval_token = None
            symbol_token = None
            for p in parts:
                if p.isdigit() or p.endswith("m") or p.endswith("h") or p in TF_MAP.values():
                    interval_token = p
                if p.endswith("USDT"):
                    symbol_token = p
            interval_label = None
            if interval_token:
                for k, v in TF_MAP.items():
                    if v == interval_token or interval_token == k:
                        interval_label = k
                        break
            if isinstance(data, list) and data:
                first = data[0]
                start = first.get("start") or first.get("t")
                close_val = first.get("close") or first.get("c")
                try:
                    close = float(close_val) if close_val is not None else None
                except Exception:
                    close = None
                if symbol_token and interval_label and close is not None:
                    await on_kline_event(symbol_token, interval_label, {"start": start, "close": close})
                    return
        # fallback parsing for list-of-candles payloads
        if isinstance(data, list) and data:
            first = data[0]
            symbol = first.get("symbol") or first.get("s")
            interval = first.get("interval") or first.get("period")
            start = first.get("start") or first.get("t")
            close_val = first.get("close") or first.get("c")
            try:
                close = float(close_val) if close_val is not None else None
            except Exception:
                close = None
            if symbol and interval and close is not None:
                interval_label = None
                for k, v in TF_MAP.items():
                    if v == str(interval) or interval == k:
                        interval_label = k
                        break
                if not interval_label and isinstance(interval, str) and interval.endswith("m"):
                    interval_label = interval
                if interval_label:
                    await on_kline_event(symbol, interval_label, {"start": start, "close": close})

    async def _resubscribe_all(self):
        if not self.subscribed_topics:
            return
        try:
            msg = json.dumps({"op": "subscribe", "args": list(self.subscribed_topics)})
            if self.conn:
                await self.conn.send(msg)
                log("Public WS re-subscribed to", len(self.subscribed_topics), "topics")
        except Exception as e:
            log("Failed to re-subscribe public topics:", e)

    async def subscribe_kline(self, symbol: str, interval_token: str):
        """
        Subscribe to a single kline topic for symbol using the detected template or trial templates.
        interval_token should be '5' or '15' (TF_MAP values).
        """
        if self.detect_template:
            topic = self.detect_template.format(interval=interval_token, symbol=symbol)
            async with self._lock:
                if not self.conn:
                    await self._connect()
                try:
                    await self.conn.send(json.dumps({"op": "subscribe", "args": [topic]}))
                    self.subscribed_topics.add(topic)
                    await add_public_subscription(topic)
                    log("Public WS subscribed to", topic)
                    return True
                except Exception as e:
                    log("Public subscribe failed:", e)
        else:
            # try each candidate template until we see some data or success reply
            async with self._lock:
                if not self.conn:
                    await self._connect()
                for tmpl in CANDIDATE_PUBLIC_TEMPLATES:
                    try:
                        topic = tmpl.format(interval=interval_token, symbol=symbol)
                    except Exception:
                        continue
                    try:
                        await self.conn.send(json.dumps({"op": "subscribe", "args": [topic]}))
                        try:
                            msg = await asyncio.wait_for(self.conn.recv(), timeout=2.0)
                            body = self._maybe_decompress(msg)
                            if body and (symbol in body or "kline" in body or '"success":true' in body):
                                self.detect_template = tmpl
                                self.subscribed_topics.add(topic)
                                await add_public_subscription(topic)
                                log("On-the-fly detected template:", tmpl, "subscribed", topic)
                                return True
                        except asyncio.TimeoutError:
                            pass
                    except Exception:
                        continue
        log("subscribe_kline failed for", symbol, interval_token)
        return False

    async def unsubscribe_kline(self, symbol: str, interval_token: str):
        if self.detect_template:
            topic = self.detect_template.format(interval=interval_token, symbol=symbol)
        else:
            topic = None
            for t in list(self.subscribed_topics):
                if symbol in t and str(interval_token) in t:
                    topic = t
                    break
        if not topic:
            return
        async with self._lock:
            try:
                if self.conn:
                    await self.conn.send(json.dumps({"op": "unsubscribe", "args": [topic]}))
                self.subscribed_topics.discard(topic)
                await remove_public_subscription(topic)
                log("Public WS unsubscribed", topic)
            except Exception as e:
                log("Public unsubscribe failed:", e)

    async def close(self):
        self._stop = True
        try:
            if self.conn:
                await self.conn.close()
        except Exception:
            pass
        self.conn = None

class PrivateWebsocketManager:
    def __init__(self, ws_url: str, api_key: Optional[str], api_secret: Optional[str]):
        self.ws_url = ws_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.conn = None
        self._recv_task = None
        self._lock = asyncio.Lock()
        self.authenticated = False
        self._reconnect_backoff = 1
        self._stop = False
        self.subscribed_topics: set = set()

    def _maybe_decompress(self, msg):
        try:
            if isinstance(msg, str):
                return msg
            if isinstance(msg, bytes):
                try:
                    return gzip.decompress(msg).decode("utf-8")
                except Exception:
                    pass
                try:
                    return zlib.decompress(msg, -zlib.MAX_WBITS).decode("utf-8")
                except Exception:
                    pass
                try:
                    return msg.decode("utf-8")
                except Exception:
                    pass
            return None
        except Exception:
            return None

    async def _connect(self):
        backoff = self._reconnect_backoff
        while not self._stop:
            try:
                self.conn = await websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10, max_size=2**24)
                log("Private WS connected")
                self._reconnect_backoff = 1
                return True
            except Exception as e:
                log("Private WS connect failed:", e, "retrying in", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
        return False

    def _compute_signatures(self, expires: int):
        if not self.api_key or not self.api_secret:
            return {}
        sec = self.api_secret.encode()
        out = {}
        ph = f"{expires}{self.api_key}".encode()
        out["api_expires_sig"] = hmac.new(sec, ph, hashlib.sha256).hexdigest()
        ph2 = f"{self.api_key}{expires}".encode()
        out["api_sig_expires"] = hmac.new(sec, ph2, hashlib.sha256).hexdigest()
        return out

    async def connect_and_auth(self):
        if not await self._connect():
            return False
        if not self._recv_task:
            self._recv_task = asyncio.create_task(self._recv_loop())
        if self.api_key and self.api_secret:
            await self._attempt_auth()
        return True

    async def _attempt_auth(self, detect_timeout: float = 6.0):
        sigs = self._compute_signatures(int(time.time()) + 8)
        candidate_msgs = []
        if "api_expires_sig" in sigs:
            candidate_msgs.append({"op": "auth", "args": [self.api_key, str(int(time.time()) + 8), sigs["api_expires_sig"]]})
        if "api_sig_expires" in sigs:
            candidate_msgs.append({"op": "auth", "args": [self.api_key, sigs["api_sig_expires"], str(int(time.time()) + 8)]})
        for msg in candidate_msgs:
            try:
                async with self._lock:
                    await self.conn.send(json.dumps(msg))
                try:
                    raw = await asyncio.wait_for(self.conn.recv(), timeout=detect_timeout)
                except asyncio.TimeoutError:
                    raw = None
                if raw:
                    body = self._maybe_decompress(raw)
                    if not body:
                        continue
                    try:
                        obj = json.loads(body)
                    except Exception:
                        continue
                    if isinstance(obj, dict) and (obj.get("success") is True or obj.get("ret_code") == 0):
                        self.authenticated = True
                        await persist_raw_ws("private", "auth", json.dumps(obj))
                        log("Private WS auth success")
                        return True
                    await persist_raw_ws("private", "auth_reply", json.dumps(obj))
            except Exception as e:
                log("Private auth attempt error:", e)
                continue
        log("Private WS auth attempts finished; no success detected")
        return False

    async def _recv_loop(self):
        while not self._stop:
            if not self.conn:
                await self._connect()
                if self.api_key and self.api_secret:
                    await self._attempt_auth()
            try:
                raw = await self.conn.recv()
                body = self._maybe_decompress(raw)
                if not body:
                    continue
                try:
                    obj = json.loads(body)
                except Exception:
                    continue
                topic = obj.get("topic") or (obj.get("arg") or {}).get("topic") or ""
                await persist_raw_ws("private", topic, json.dumps(obj))
                asyncio.create_task(self.handle_message(obj))
            except Exception as e:
                log("Private WS recv error/disconnect:", e)
                try:
                    if self.conn:
                        await self.conn.close()
                except Exception:
                    pass
                self.conn = None
                await asyncio.sleep(min(self._reconnect_backoff, 60))
                self._reconnect_backoff = min(self._reconnect_backoff * 2 if self._reconnect_backoff > 0 else 1, 60)

    async def handle_message(self, obj: dict):
        asyncio.create_task(on_private_msg(obj))

    async def subscribe_topic(self, topic: str):
        async with self._lock:
            if not self.conn:
                await self._connect()
            try:
                await self.conn.send(json.dumps({"op": "subscribe", "args": [topic]}))
                self.subscribed_topics.add(topic)
                await add_private_subscription(topic)
                log("Private WS subscribed to", topic)
                return True
            except Exception as e:
                log("Private subscribe failed:", e)
                return False

    async def unsubscribe_topic(self, topic: str):
        async with self._lock:
            try:
                if self.conn:
                    await self.conn.send(json.dumps({"op": "unsubscribe", "args": [topic]}))
                self.subscribed_topics.discard(topic)
                await remove_private_subscription(topic)
                log("Private WS unsubscribed", topic)
            except Exception as e:
                log("Private WS unsubscribe failed:", e)

    async def close(self):
        self._stop = True
        try:
            if self.conn:
                await self.conn.close()
        except Exception:
            pass
        self.conn = None

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
                last_candle_end = klines[-1]["end"] or klines[-1]["start"]
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
                    if public_ws:
                        # subscribe only 5m and 15m when root flip detected
                        await public_ws.subscribe_kline(symbol, TF_MAP["5m"])
                        await public_ws.subscribe_kline(symbol, TF_MAP["15m"])
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
    root_end = sig.get("root_flip_candle_end") or sig.get("root_flip_time")
    now_ms = now_ts_ms()
    # expire after candle end + some buffer (here we check timestamp in seconds -> ms)
    if root_end and now_ms > ((root_end or 0) * 1000):
        active_root_signals.pop(sig_id, None)
        await remove_root_signal(sig_id)
        if public_ws:
            # unsubscribe only the two we subscribed earlier
            await public_ws.unsubscribe_kline(symbol, TF_MAP["5m"])
            await public_ws.unsubscribe_kline(symbol, TF_MAP["15m"])
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

@app.get("/")
async def root():
    return {"service": "bybit-macd-scanner", "status": "ok", "public_ws": PUBLIC_WS_URL, "private_ws": PRIVATE_WS_URL}

@app.post("/toggle")
async def toggle(req: ToggleRequest, _auth=Depends(require_admin_auth)):
    global TRADING_ENABLED, MAX_OPEN_TRADES, PRIVATE_WS_URL, private_ws
    if req.trading_enabled is not None:
        TRADING_ENABLED = req.trading_enabled
        if TRADING_ENABLED:
            PRIVATE_WS_URL = "wss://stream.bybit.com/v5/private" if BYBIT_USE_MAINNET else "wss://stream-testnet.bybit.com/v5/private"
            if not private_ws and PRIVATE_WS_URL:
                private_ws = PrivateWebsocketManager(PRIVATE_WS_URL, BYBIT_API_KEY, BYBIT_API_SECRET)
                asyncio.create_task(private_ws.connect_and_auth())
        else:
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
        "public_ws_connected": bool(public_ws and public_ws.conn),
        "private_ws_connected": bool(private_ws and private_ws.conn and private_ws.authenticated) if private_ws else False,
    }

# ---------- Startup ----------
@app.on_event("startup")
async def startup():
    global public_ws, private_ws
    await init_db()

    log("Startup config:",
        "BYBIT_USE_MAINNET=", BYBIT_USE_MAINNET,
        "TRADING_ENABLED=", TRADING_ENABLED,
        "API_HOSTS=", API_HOSTS,
        "PUBLIC_WS_URL=", PUBLIC_WS_URL,
        "PRIVATE_WS_URL=", PRIVATE_WS_URL)

    public_ws = PublicWebsocketManager(PUBLIC_WS_URL)
    # ensure detection / template discovery before scanners start
    ok = await public_ws.connect_and_detect(timeout=8.0)
    if not ok:
        log("Warning: public websocket connect/detect failed during startup, scanners will still run but subscriptions may fail initially.")

    if TRADING_ENABLED and PRIVATE_WS_URL:
        private_ws = PrivateWebsocketManager(PRIVATE_WS_URL, BYBIT_API_KEY, BYBIT_API_SECRET)
        asyncio.create_task(private_ws.connect_and_auth())

    # start scanner loops AFTER attempting WS detection (so subscribe_kline works)
    asyncio.create_task(root_scanner_loop("1h"))
    asyncio.create_task(root_scanner_loop("4h"))
    asyncio.create_task(active_signal_watcher_loop())
    log("Background tasks started")

# ---------- Run note ----------
# Start with:
# uvicorn main:app --host 0.0.0.0 --port 8000
