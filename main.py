# main.py
# Bybit MACD multi-timeframe scanner — enhanced:
# - Per-symbol/timeframe candle cache (deque) with large configurable max length
# - Detect root flips and publish current root signals to logs + Telegram
# - Require full MTF alignment before trading (5m,15m,1h,1d for 4h root; 5m,15m,4h,1d for 1h root)
# - Require flips to occur on the most recent candle for the timeframe to qualify (strict)
# - Super-signal generation: when two or more of (1h,4h,1d) align in the same 5m scan loop
# - Filters: skip stablecoins, skip symbols starting with digits, skip symbols younger than 3 months
# - Never trade stablecoins; simulated trades unless TRADING_ENABLED True
#
# Recommended: run server-side (uvicorn). Protect ADMIN_API_KEY for debug endpoints.

import os
import time
import hmac
import hashlib
import asyncio
import json
import zlib
import gzip
import uuid
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from collections import deque, defaultdict

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

# candle cache sizing and behavior
CANDLE_CACHE_MAX = int(os.getenv("CANDLE_CACHE_MAX", "2000"))     # keep up to N candles per symbol/tf
CANDLE_CACHE_TTL = int(os.getenv("CANDLE_CACHE_TTL", "300"))      # seconds TTL for cache refresh per symbol/tf

# skip tokens beginning with digits (spam tokens like 1000000FOO)
SKIP_DIGIT_PREFIX = os.getenv("SKIP_DIGIT_PREFIX", "true").lower() == "true"

# optional fast-test limit: set >0 to scan only N symbols (helpful for dev/testing)
SYMBOL_SCAN_LIMIT = int(os.getenv("SYMBOL_SCAN_LIMIT", "0"))

ROOT_SIGNALS_LOG_INTERVAL = int(os.getenv("ROOT_SIGNALS_LOG_INTERVAL", "60"))

# Hosts & endpoints
MAINNET_API_HOST = "https://api.bybit.com"
TESTNET_API_HOST = "https://api-testnet.bybit.com"
PRIMARY_API_HOST = MAINNET_API_HOST if BYBIT_USE_MAINNET else TESTNET_API_HOST
API_HOSTS = [PRIMARY_API_HOST]
if PRIMARY_API_HOST == MAINNET_API_HOST:
    API_HOSTS.append(TESTNET_API_HOST)
else:
    API_HOSTS.append(MAINNET_API_HOST)

PUBLIC_WS_URL = "wss://stream.bybit.com/v5/public/linear" if BYBIT_USE_MAINNET else "wss://stream-testnet.bybit.com/v5/public/linear"
PRIVATE_WS_URL = ("wss://stream.bybit.com/v5/private" if BYBIT_USE_MAINNET else "wss://stream-testnet.bybit.com/v5/private") if TRADING_ENABLED else None

INSTRUMENTS_ENDPOINTS = [
    "/v5/market/instruments-info?category=linear&instType=PERPETUAL",
    "/v5/market/instruments-info?category=linear",
    "/v5/market/instruments-info?category=perpetual",
    "/v5/market/instruments-info",
    "/v2/public/symbols",
    "/v2/public/tickers",
]
KLINE_ENDPOINTS = ["/v5/market/kline", "/v2/public/kline/list", "/v2/public/kline"]

# timeframe tokens used by Bybit / our logic
TF_MAP = {"5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "D"}

CANDIDATE_PUBLIC_TEMPLATES = [
    "klineV2.{interval}.{symbol}",
    "kline.{interval}.{symbol}",
    "klineV2:{interval}:{symbol}",
    "kline:{interval}:{symbol}",
    "kline:{symbol}:{interval}",
]

# MACD params
MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
# minimum candles required for MACD calculations (slow + signal + small buffer)
MIN_CANDLES_REQUIRED = MACD_SLOW + MACD_SIGNAL + 5

LEVERAGE = int(os.getenv("LEVERAGE", "3"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.015"))
BREAKEVEN_PCT = float(os.getenv("BREAKEVEN_PCT", "0.005"))
STABLECOINS = {"USDT", "BUSD", "USDC", "TUSD", "DAI"}

# how many months old a symbol must be to be tradable
MIN_SYMBOL_AGE_MONTHS = int(os.getenv("MIN_SYMBOL_AGE_MONTHS", "3"))

# ---------- Globals ----------
app = FastAPI()
httpx_client = httpx.AsyncClient(timeout=20)
db: Optional[aiosqlite.Connection] = None

# symbol -> info dict (from instruments endpoint) for optional filtering and metadata
symbols_info_cache: Dict[str, Dict[str, Any]] = {}

# in-memory candle cache:
# candles_cache[symbol][interval_token] = deque([...], maxlen=CANDLE_CACHE_MAX)
candles_cache: Dict[str, Dict[str, deque]] = defaultdict(lambda: {})
# per-cache last update timestamp: candles_cache_ts[symbol][interval] = unix_ts
candles_cache_ts: Dict[str, Dict[str, int]] = defaultdict(lambda: {})

# root signals stored in-memory; persisted to DB in root_signals table
active_root_signals: Dict[str, Dict[str, Any]] = {}
last_root_processed: Dict[str, int] = {}

public_ws = None
private_ws = None

# regex for symbols starting with digits
_re_leading_digit = re.compile(r"^\d")

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
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    try:
        r = await httpx_client.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            log("Telegram send failed:", r.status_code, r.text)
    except Exception as e:
        log("Telegram error:", e)

async def send_root_signals_telegram():
    """Send a concise Telegram message listing current root signals and TFs."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if not active_root_signals:
        await send_telegram("Root signals: 0")
        return
    lines = ["Root signals:"]
    for sig in active_root_signals.values():
        sym = sig.get("symbol")
        tf = sig.get("root_tf")
        status = sig.get("status", "watching")
        price = sig.get("root_flip_price")
        created = sig.get("created_at")
        lines.append(f"- {sym} root={tf} status={status} price={price}")
    await send_telegram("\n".join(lines))

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
                    log("Invalid JSON from", url, "body excerpt:", (r.text[:400] + "...") if r.text else "")
                    continue
            else:
                body_excerpt = (r.text[:400] + "...") if r.text else ""
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
    await db.execute("""CREATE TABLE IF NOT EXISTS root_signals (id TEXT PRIMARY KEY, symbol TEXT, root_tf TEXT, flip_time INTEGER, flip_price REAL, status TEXT, priority TEXT, created_at INTEGER)""")
    await db.execute("""CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, symbol TEXT, side TEXT, qty REAL, entry_price REAL, sl_price REAL, created_at INTEGER, open BOOLEAN, raw_response TEXT)""")
    await db.execute("""CREATE TABLE IF NOT EXISTS raw_ws_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, topic TEXT, message TEXT, created_at INTEGER)""")
    await db.execute("""CREATE TABLE IF NOT EXISTS public_subscriptions (topic TEXT PRIMARY KEY, created_at INTEGER)""")
    await db.execute("""CREATE TABLE IF NOT EXISTS private_subscriptions (topic TEXT PRIMARY KEY, created_at INTEGER)""")
    await db.commit()
    log("DB initialized at", DB_PATH)

async def persist_root_signal(sig: Dict[str, Any]):
    await db.execute("INSERT OR REPLACE INTO root_signals (id,symbol,root_tf,flip_time,flip_price,status,priority,created_at) VALUES (?,?,?,?,?,?,?,?)",
                     (sig["id"], sig["symbol"], sig["root_tf"], sig["root_flip_time"], sig["root_flip_price"], sig.get("status", "watching"), sig.get("priority"), sig["created_at"]))
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

# ---------- Helper functions (robust instruments fetch) ----------
async def get_tradable_usdt_symbols() -> List[str]:
    """
    Return list of symbols that are linear PERPETUAL and quote USDT and status Trading.
    Exclude stablecoins, tokens starting with digits (spam), and very new symbols (< MIN_SYMBOL_AGE_MONTHS).
    Caches symbol info in symbols_info_cache.
    """
    global symbols_info_cache
    found = []
    last_resp_excerpt = None
    for ep in INSTRUMENTS_ENDPOINTS:
        url = PRIMARY_API_HOST + ep
        try:
            r = await httpx_client.get(url, timeout=12)
        except Exception as e:
            log("Network error getting instruments from", url, "->", e)
            continue
        body_excerpt = (r.text[:800] + "...") if r.text else ""
        if r.status_code != 200:
            log("Instruments endpoint", url, "returned", r.status_code, "excerpt:", body_excerpt)
            last_resp_excerpt = body_excerpt
            continue
        try:
            resp = r.json()
        except Exception:
            log("Could not parse JSON from", url, "excerpt:", body_excerpt)
            last_resp_excerpt = body_excerpt
            continue

        items = []
        if isinstance(resp, dict):
            items = resp.get("result", {}).get("list", []) or resp.get("result", {}).get("data", []) or resp.get("data", []) or resp.get("list", []) or resp.get("result", []) or resp.get("symbols", []) or resp.get("rows", [])
        elif isinstance(resp, list):
            items = resp

        if not items:
            last_resp_excerpt = body_excerpt
            continue

        for it in items:
            try:
                if isinstance(it, dict):
                    symbol = (it.get("symbol") or it.get("name") or it.get("code") or it.get("symbolName") or "").strip()
                    quote = (it.get("quoteCoin") or it.get("quoteAsset") or it.get("quote_currency") or it.get("quote") or it.get("quoteCurrency") or "").upper()
                    status = (it.get("status") or it.get("state") or it.get("is_trading") or it.get("active") or "").lower()
                    inst_type = (it.get("instType") or it.get("type") or it.get("contractType") or "").upper()
                    # listing/launch time fields may vary: 'launchTime', 'launch_time', 'listTime', 'listedAt'
                    launch_ts = None
                    for k in ("launchTime", "launch_time", "listTime", "listedAt", "listed_time"):
                        if k in it and it[k]:
                            try:
                                # often milliseconds
                                v = int(float(it[k]))
                                if v > 10**12:
                                    v = v // 1000
                                launch_ts = v
                                break
                            except Exception:
                                continue
                else:
                    # if item is not dict skip
                    continue

                if not symbol:
                    continue
                if not (symbol.upper().endswith("USDT") or quote == "USDT"):
                    continue
                # skip stablecoins themselves and small quote tokens
                base = symbol[:-4] if symbol.upper().endswith("USDT") else symbol
                if base.upper() in STABLECOINS:
                    continue
                # skip digit-prefixed tokens if configured
                if SKIP_DIGIT_PREFIX and _re_leading_digit.match(symbol):
                    continue
                # skip tokens with explicit non-trading status
                if status and isinstance(status, str) and status not in ("trading", "listed", "normal", "active", "open"):
                    continue
                # skip symbols younger than MIN_SYMBOL_AGE_MONTHS if launch_ts available
                if launch_ts:
                    age_days = (int(time.time()) - launch_ts) / 86400.0
                    if age_days < (MIN_SYMBOL_AGE_MONTHS * 30):
                        continue
                # store info for later use
                symbols_info_cache[symbol] = {"symbol": symbol, "quote": quote, "inst_type": inst_type, "launch_ts": launch_ts, "status": status}
                found.append(symbol)
            except Exception:
                continue

        if found:
            break
        else:
            last_resp_excerpt = body_excerpt

    uniq = sorted(list({s for s in found}))
    log("get_tradable_usdt_symbols: Found", len(uniq), "symbols")
    if SYMBOL_SCAN_LIMIT and SYMBOL_SCAN_LIMIT > 0:
        uniq = uniq[:SYMBOL_SCAN_LIMIT]
        log("Symbol scan limited to first", SYMBOL_SCAN_LIMIT, "symbols for testing")
    return uniq

# ---------- Candle cache helpers ----------
def cache_get(symbol: str, interval_token: str) -> Optional[deque]:
    return candles_cache.get(symbol, {}).get(interval_token)

def cache_set(symbol: str, interval_token: str, dq: deque):
    if symbol not in candles_cache:
        candles_cache[symbol] = {}
    candles_cache[symbol][interval_token] = dq
    if symbol not in candles_cache_ts:
        candles_cache_ts[symbol] = {}
    candles_cache_ts[symbol][interval_token] = int(time.time())

def cache_needs_refresh(symbol: str, interval_token: str) -> bool:
    ts = candles_cache_ts.get(symbol, {}).get(interval_token)
    if not ts:
        return True
    return (int(time.time()) - ts) > CANDLE_CACHE_TTL

def merge_into_cache(symbol: str, interval_token: str, candles: List[Dict[str, Any]]):
    """
    Merge new candles (assumed chronological ascending) into cache deque, keep unique by start.
    """
    if not candles:
        return
    dq = cache_get(symbol, interval_token)
    if dq is None:
        dq = deque(maxlen=CANDLE_CACHE_MAX)
    # existing starts for quick check
    existing_starts = set(x["start"] for x in dq)
    for c in candles:
        if c["start"] not in existing_starts:
            dq.append(c)
            existing_starts.add(c["start"])
    # ensure sorted ascending (deque maintains append order; but to be safe)
    sorted_list = sorted(list(dq), key=lambda x: x["start"])
    dq = deque(sorted_list, maxlen=CANDLE_CACHE_MAX)
    cache_set(symbol, interval_token, dq)

async def update_candles_cache_from_fetch(symbol: str, interval_token: str, limit: int):
    klines = await fetch_klines(symbol, interval_token, limit=limit)
    if not klines:
        return
    merge_into_cache(symbol, interval_token, klines)

# ---------- Fetch and normalize klines ----------
async def fetch_klines(symbol: str, interval_token: str, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Fetch klines via v5 kline endpoint. interval_token should be the token used in TF_MAP ('5','15','60', etc).
    Returns list of candles as dicts with keys: start (seconds), end (seconds or None), close (float)
    """
    params = {"category": "linear", "symbol": symbol, "interval": str(interval_token), "limit": limit}
    resp = await resilient_public_get([KLINE_ENDPOINTS[0]], params=params)
    if not resp:
        return []
    items = []
    if isinstance(resp, dict):
        items = resp.get("result", {}).get("list", []) or resp.get("list", []) or resp.get("data", []) or resp.get("result", []) or resp.get("rows", [])
    elif isinstance(resp, list):
        items = resp
    candles = []
    for it in items:
        try:
            start = None
            end = None
            close_val = None
            if isinstance(it, dict):
                start = it.get("start") or it.get("t") or it.get("open_time")
                end = it.get("end") or it.get("close_time") or it.get("close_time_ms")
                close_val = it.get("close") or it.get("c") or it.get("closePrice") or it.get("close_price")
            elif isinstance(it, (list, tuple)):
                if len(it) >= 5:
                    start = it[0]
                    close_val = it[4]
                else:
                    continue
            else:
                continue
            if start is None or close_val is None:
                continue
            start_int = int(float(start))
            # convert ms -> s if needed
            if start_int > 10**12:
                start_int = start_int // 1000
            end_int = None
            if end is not None:
                try:
                    end_int = int(float(end))
                    if end_int > 10**12:
                        end_int = end_int // 1000
                except Exception:
                    end_int = None
            close = float(close_val)
            candles.append({"start": start_int, "end": end_int, "close": close})
        except Exception:
            continue
    candles.sort(key=lambda c: c["start"])
    return candles

# ---------- MACD helpers ----------
def check_macd_flip_recent_from_closes(closes: List[float], lookback: int = 3) -> Optional[int]:
    """
    Return index where MACD histogram flipped from <=0 to >0 within lookback range (index in closes list)
    """
    if len(closes) < MACD_SLOW + MACD_SIGNAL:
        return None
    hist = macd_hist(closes)
    if not hist:
        return None
    for idx in range(len(hist) - 1, max(-1, len(hist) - 1 - lookback), -1):
        if idx <= 0:
            continue
        prev = hist[idx - 1] or 0
        cur = hist[idx] or 0
        if prev <= 0 and cur > 0:
            return idx
    return None

def macd_positive_from_closes(closes: List[float]) -> bool:
    hist = macd_hist(closes)
    if not hist:
        return False
    last = hist[-1]
    return last is not None and last > 0

# ---------- Filters ----------
def is_stablecoin_symbol(symbol: str) -> bool:
    base = symbol[:-4] if symbol.upper().endswith("USDT") else symbol
    return base.upper() in STABLECOINS

def symbol_too_new(symbol: str) -> bool:
    info = symbols_info_cache.get(symbol)
    if not info:
        return False
    launch = info.get("launch_ts")
    if not launch:
        return False
    age_days = (int(time.time()) - int(launch)) / 86400.0
    return age_days < (MIN_SYMBOL_AGE_MONTHS * 30)

# ---------- WebSocket managers (public minimal) ----------
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

    async def connect_and_detect(self, timeout: float = 8.0):
        if not await self._connect():
            return False
        # try auto detect templates by subscribing to candidate topics for BTCUSDT
        for tmpl in CANDIDATE_PUBLIC_TEMPLATES:
            try:
                t1 = tmpl.format(interval=TF_MAP["5m"], symbol=self.detect_symbol)
                t2 = tmpl.format(interval=TF_MAP["15m"], symbol=self.detect_symbol)
                await self.conn.send(json.dumps({"op": "subscribe", "args": [t1, t2]}))
                # wait briefly for any reply
                try:
                    msg = await asyncio.wait_for(self.conn.recv(), timeout=1.0)
                    body = self._maybe_decompress(msg)
                    if body and (self.detect_symbol in body or "kline" in body):
                        self.detect_template = tmpl
                        log("Public WS template detected:", tmpl)
                        try:
                            await self.conn.send(json.dumps({"op": "unsubscribe", "args": [t1, t2]}))
                        except Exception:
                            pass
                        break
                except Exception:
                    # continue trying templates
                    try:
                        await self.conn.send(json.dumps({"op": "unsubscribe", "args": [t1, t2]}))
                    except Exception:
                        pass
                    continue
            except Exception:
                continue
        # start minimal recv task to persist incoming messages if needed
        if not self._recv_task:
            self._recv_task = asyncio.create_task(self._recv_loop())
        return True

    async def _recv_loop(self):
        while not self._stop:
            if not self.conn:
                await self._connect()
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

# ---------- Scanning & signal logic ----------
def tf_list_for_root(root_tf: str) -> List[str]:
    """
    Return the list of lower timeframes to check for alignment based on root timeframe, per user request:
      - root 4h: check 5m,15m,1h,1d
      - root 1h: check 5m,15m,4h,1d
    """
    if root_tf == "4h":
        return ["5m", "15m", "1h", "1d"]
    if root_tf == "1h":
        return ["5m", "15m", "4h", "1d"]
    # fallback standard
    return ["5m", "15m", "1h", "1d"]

async def ensure_cached_candles(symbol: str, tf: str, required: int):
    """
    Ensure the candles_cache for (symbol, tf) contains at least `required` candles,
    fetching and merging as needed.
    """
    token = TF_MAP[tf]
    dq = cache_get(symbol, token)
    if dq and len(dq) >= required and not cache_needs_refresh(symbol, token):
        return
    # fetch enough candles: request required*2 to be safe and to fill cache
    try:
        fetched = await fetch_klines(symbol, token, limit=max(required * 2, required + 50))
        if fetched:
            merge_into_cache(symbol, token, fetched)
            log(f"Updated cache for {symbol} {tf}: now {len(cache_get(symbol, token) or [])} candles")
    except Exception as e:
        log("ensure_cached_candles fetch error:", e)

def candles_to_closes(dq: deque, last_n: Optional[int] = None) -> List[float]:
    arr = list(dq)
    if last_n:
        arr = arr[-last_n:]
    return [x["close"] for x in arr if x.get("close") is not None]

async def detect_flip_on_last_candle(symbol: str, tf: str) -> bool:
    """
    Return True if MACD flipped (<=0 -> >0) on the most recent candle of tf for symbol.
    Strict: flip must be on the last candle.
    """
    token = TF_MAP[tf]
    await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
    dq = cache_get(symbol, token)
    if not dq or len(dq) < MIN_CANDLES_REQUIRED:
        return False
    closes = candles_to_closes(dq)
    if len(closes) < MACD_SLOW + MACD_SIGNAL:
        return False
    hist = macd_hist(closes)
    if not hist or len(hist) < 2:
        return False
    # check last two histogram values: prev <=0, last >0
    prev = hist[-2] or 0
    last = hist[-1] or 0
    return (prev <= 0 and last > 0)

async def check_mtf_alignment(symbol: str, root_tf: str) -> bool:
    """
    Check required MTF alignment for root. All TFs in tf_list_for_root must have macd_positive (last hist >0).
    """
    tfs = tf_list_for_root(root_tf)
    for tf in tfs:
        token = TF_MAP[tf]
        await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
        dq = cache_get(symbol, token)
        if not dq or len(dq) < MIN_CANDLES_REQUIRED:
            return False
        closes = candles_to_closes(dq)
        if not macd_positive_from_closes(closes):
            return False
    return True

async def root_scanner_loop(root_tf: str):
    log("Root scanner started for", root_tf)
    while True:
        try:
            symbols = await get_tradable_usdt_symbols()
            for symbol in symbols:
                # filters
                if is_stablecoin_symbol(symbol):
                    continue
                if SKIP_DIGIT_PREFIX and _re_leading_digit.match(symbol):
                    continue
                if symbol_too_new(symbol):
                    log("Skipping", symbol, "— too new")
                    continue

                # ensure we have enough candles for root timeframe calculation (for root detection)
                try:
                    await ensure_cached_candles(symbol, root_tf, MIN_CANDLES_REQUIRED)
                    dq_root = cache_get(symbol, TF_MAP[root_tf])
                    if not dq_root or len(dq_root) < MIN_CANDLES_REQUIRED:
                        continue
                    # check if a flip occurred on the last candle for the root timeframe
                    flipped_root = await detect_flip_on_last_candle(symbol, root_tf)
                    if not flipped_root:
                        continue
                    # Now create a root signal (watching) and subscribe to lower TFs 5m/15m for monitoring
                    last_candle = list(dq_root)[-1]
                    key = f"{symbol}-{root_tf}-{last_candle['start']}"
                    if key in active_root_signals:
                        # already recorded
                        last_root_processed[f"{symbol}-{root_tf}"] = last_candle["start"]
                        continue
                    sig = {
                        "id": key,
                        "symbol": symbol,
                        "root_tf": root_tf,
                        "root_flip_time": last_candle["start"],
                        "root_flip_candle_end": last_candle.get("end"),
                        "root_flip_price": last_candle["close"],
                        "created_at": now_ts_ms(),
                        "status": "watching",
                        "priority": None,
                    }
                    active_root_signals[key] = sig
                    await persist_root_signal(sig)
                    log("Root flip detected and added:", symbol, root_tf, last_candle["start"])
                    # update logs + telegram list
                    await log_current_root_signals()
                    await send_root_signals_telegram()
                    # subscribe to 5m and 15m for fast monitoring if public_ws available
                    if public_ws:
                        await public_ws.subscribe_kline(symbol, TF_MAP["5m"])
                        await public_ws.subscribe_kline(symbol, TF_MAP["15m"])
                    last_root_processed[f"{symbol}-{root_tf}"] = last_candle["start"]
                except Exception as e:
                    log("Error scanning symbol", symbol, "for root", root_tf, "->", e)
                    continue
        except Exception as e:
            log("root_scanner_loop outer error:", e)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

async def evaluate_signal_fast(sig_id: str):
    """
    Evaluate a root signal for full MTF alignment and take action if alignment is complete.
    Ensures:
     - For 4h root: check 5m,15m,1h,1d
     - For 1h root: check 5m,15m,4h,1d
    Act only if all TFs positive and the last flips occurred on their last candles (strict).
    Super-signal rules handled separately in 5m scan loop (see below).
    """
    sig = active_root_signals.get(sig_id)
    if not sig:
        return
    symbol = sig["symbol"]
    root_tf = sig["root_tf"]
    if is_stablecoin_symbol(symbol) or symbol_too_new(symbol):
        # remove and skip
        active_root_signals.pop(sig_id, None)
        await remove_root_signal(sig_id)
        log("Removed untradeable signal:", sig_id)
        await log_current_root_signals()
        await send_root_signals_telegram()
        return

    # Check all MTF alignment
    tfs = tf_list_for_root(root_tf)
    # First: ensure candles available and each TF is positive and flipped on last candle
    for tf in tfs:
        await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
        dq = cache_get(symbol, TF_MAP[tf])
        if not dq or len(dq) < MIN_CANDLES_REQUIRED:
            log("Not enough candles for", symbol, tf)
            return
        closes = candles_to_closes(dq)
        # require macd_positive (last hist > 0)
        if not macd_positive_from_closes(closes):
            log("MTF alignment failed: not positive on", tf, "for", symbol)
            return
        # require flip on last candle for this timeframe (strict)
        # detect_flip_on_last_candle returns True only if flip on last candle
        flipped = await detect_flip_on_last_candle(symbol, tf)
        if not flipped:
            log("MTF alignment failed: no last-candle flip on", tf, "for", symbol)
            return

    # All TFs aligned (and flipped on most recent candle)
    if sig.get("status") == "acted":
        return
    # Mark priority if this is a "super" candidate (handled elsewhere) - here it's full alignment => ready to trade if enabled
    sig["status"] = "acted"
    sig["priority"] = "full-mtf"
    await persist_root_signal(sig)
    await log_current_root_signals()
    await send_root_signals_telegram()
    # Place trade or simulate
    balance = await get_account_usdt_balance()
    per_trade = max(1.0, balance / max(1, MAX_OPEN_TRADES))
    suggested_price = sig.get("root_flip_price") or 0.0
    qty = compute_qty_from_usdt(per_trade, suggested_price or 1.0)
    side = "Buy"
    stop_price = round((suggested_price or 1.0) * (1 - STOP_LOSS_PCT), 8)
    if is_stablecoin_symbol(symbol):
        log("Should not trade stablecoin:", symbol)
        return
    if symbol_too_new(symbol):
        log("Should not trade new symbol:", symbol)
        return
    if not TRADING_ENABLED:
        # simulated order
        order_id = str(uuid.uuid4())
        tr = {"id": order_id, "symbol": symbol, "side": side, "qty": qty, "entry_price": None, "sl_price": stop_price, "created_at": now_ts_ms(), "open": True, "raw": {"simulated": True}}
        await persist_trade(tr)
        await send_telegram(f"Simulated trade for {symbol} qty={qty} side={side} priority={sig.get('priority')}")
        log("Simulated trade placed for", symbol, "qty", qty)
        return
    # real trading path (user must enable and test on testnet first)
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
        await persist_trade({"id": res.get("result", {}).get("orderId", str(uuid.uuid4())), "symbol": symbol, "side": side, "qty": qty, "entry_price": None, "sl_price": stop_price, "created_at": now_ts_ms(), "open": True, "raw": res})
        await send_telegram(f"Placed market order for {symbol} side={side} qty={qty} priority={sig.get('priority')}")
        log("Placed real order result:", res)
    except Exception as e:
        log("Error placing order:", e)

# ---------- Super-signal detection during frequent 5m checks ----------
async def super_signal_scanner_loop_5m():
    """
    Runs roughly on a 5m cadence (or every SCAN_INTERVAL_SECONDS but focused on 5m conditions).
    Detect when 2 or more of (1h,4h,1d) have last-candle flips within the same scan iteration,
    and emit a 'super' signal (priority). Do not trade immediately — mark priority and wait for full MTF alignment.
    """
    while True:
        try:
            symbols = await get_tradable_usdt_symbols()
            for symbol in symbols:
                if is_stablecoin_symbol(symbol) or (SKIP_DIGIT_PREFIX and _re_leading_digit.match(symbol)) or symbol_too_new(symbol):
                    continue
                # Only check the long TFs: 1h,4h,1d
                flips = []
                for tf in ["1h", "4h", "1d"]:
                    try:
                        flipped = await detect_flip_on_last_candle(symbol, tf)
                        if flipped:
                            flips.append(tf)
                    except Exception:
                        continue
                if len(flips) >= 2:
                    # super candidate found
                    # create a super signal id keyed by symbol + timestamp
                    ts = int(time.time())
                    key = f"SUPER-{symbol}-{ts}"
                    sig = {
                        "id": key,
                        "symbol": symbol,
                        "root_tf": "5m",  # 5m scanning loop triggers super
                        "root_flip_time": ts,
                        "root_flip_candle_end": None,
                        "root_flip_price": None,
                        "created_at": now_ts_ms(),
                        "status": "watching",
                        "priority": "super",
                        "super_components": flips,
                    }
                    # avoid duplicates: check if existing active super for symbol (recent)
                    exists = any(s.get("symbol") == symbol and s.get("priority") == "super" for s in active_root_signals.values())
                    if not exists:
                        active_root_signals[key] = sig
                        await persist_root_signal(sig)
                        log("SUPER signal detected for", symbol, "components:", flips)
                        await log_current_root_signals()
                        await send_root_signals_telegram()
            # one loop then sleep
        except Exception as e:
            log("super_signal_scanner_loop_5m error:", e)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

# ---------- Startup helpers and logging ----------
async def log_current_root_signals():
    try:
        count = len(active_root_signals)
        if count == 0:
            log("Current root signals: 0")
            return
        log(f"Current root signals: {count}")
        idx = 0
        for key, sig in active_root_signals.items():
            idx += 1
            symbol = sig.get("symbol")
            root_tf = sig.get("root_tf")
            status = sig.get("status")
            priority = sig.get("priority")
            created = sig.get("created_at")
            comps = sig.get("super_components", "")
            log(f" {idx}. id={key} symbol={symbol} root_tf={root_tf} status={status} priority={priority} components={comps} created_at={created}")
    except Exception as e:
        log("log_current_root_signals error:", e)

async def load_persisted_root_signals():
    try:
        async with db.execute("SELECT id,symbol,root_tf,flip_time,flip_price,status,priority,created_at FROM root_signals") as cur:
            rows = await cur.fetchall()
        for r in rows:
            try:
                rid = r[0]
                symbol = r[1]
                root_tf = r[2]
                flip_time = r[3]
                flip_price = r[4]
                status = r[5] or "watching"
                priority = r[6]
                created_at = r[7] or now_ts_ms()
                active_root_signals[rid] = {
                    "id": rid,
                    "symbol": symbol,
                    "root_tf": root_tf,
                    "root_flip_time": flip_time,
                    "root_flip_candle_end": flip_time,
                    "root_flip_price": flip_price,
                    "status": status,
                    "priority": priority,
                    "created_at": created_at,
                }
            except Exception:
                continue
        log("Loaded", len(rows), "persisted root signals from DB")
    except Exception as e:
        log("load_persisted_root_signals error:", e)

# ---------- Utility endpoints for diagnostics ----------
@app.get("/debug/current_roots")
async def debug_current_roots(_auth=Depends(require_admin_auth)):
    return {"count": len(active_root_signals), "signals": list(active_root_signals.values())}

@app.get("/debug/check_symbol")
async def debug_check_symbol(symbol: str = Query(..., min_length=1), tf: str = Query("1h"), _auth=Depends(require_admin_auth)):
    if tf not in TF_MAP:
        raise HTTPException(status_code=400, detail=f"Unknown timeframe {tf}. Valid: {list(TF_MAP.keys())}")
    token = TF_MAP[tf]
    # ensure cache updated
    await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
    dq = cache_get(symbol, token)
    if not dq:
        return {"symbol": symbol, "tf": tf, "klines_count": 0, "error": "no cached klines"}
    closes = candles_to_closes(dq)
    hist = macd_hist(closes)
    flip_on_last = await detect_flip_on_last_candle(symbol, tf)
    return {
        "symbol": symbol,
        "tf": tf,
        "cached_klines": len(dq),
        "macd_hist_len": len(hist),
        "macd_last": hist[-1] if hist else None,
        "flip_on_last": flip_on_last,
        "sample_last_klines": list(dq)[-5:],
    }

# ---------- Startup & background tasks ----------
@app.on_event("startup")
async def startup():
    global public_ws, private_ws
    await init_db()
    await load_persisted_root_signals()
    log("Startup config:", "BYBIT_USE_MAINNET=", BYBIT_USE_MAINNET, "TRADING_ENABLED=", TRADING_ENABLED, "PUBLIC_WS_URL=", PUBLIC_WS_URL, "MIN_CANDLES_REQUIRED=", MIN_CANDLES_REQUIRED)
    public_ws = PublicWebsocketManager(PUBLIC_WS_URL)
    try:
        ok = await public_ws.connect_and_detect(timeout=6.0)
        if not ok:
            log("Public WS detect warning")
    except Exception:
        log("Public WS connect error")
    # Start root scanners for 1h and 4h
    asyncio.create_task(root_scanner_loop("1h"))
    asyncio.create_task(root_scanner_loop("4h"))
    # Super-signal scanner (driven by small-cadence loop, suitable to run every SCAN_INTERVAL_SECONDS)
    asyncio.create_task(super_signal_scanner_loop_5m())
    # Periodic logger of current root signals and periodic telegram send (optional)
    async def periodic_root_logger():
        while True:
            try:
                await log_current_root_signals()
                # push Telegram list periodically (only if signals present)
                if active_root_signals:
                    await send_root_signals_telegram()
            except Exception as e:
                log("periodic_root_logger error:", e)
            await asyncio.sleep(max(30, ROOT_SIGNALS_LOG_INTERVAL))
    asyncio.create_task(periodic_root_logger())
    log("Background tasks started")

# ---------- Run note ----------
# Start with:
# uvicorn main:app --host 0.0.0.0 --port 8000
