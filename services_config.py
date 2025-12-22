# services_config.py - Configuration, utilities, and core helpers
# Configuration, DB, cache, MACD, and helper functions

import os
import time
import hmac
import hashlib
import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from collections import deque, defaultdict

import httpx
import aiosqlite

# ---------- Configuration / env ----------
TELEGRAM_BOT_TOKEN = os. getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
BYBIT_USE_MAINNET = os.getenv("BYBIT_USE_MAINNET", "false").lower() == "true"
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "false").lower() == "true"
MAX_OPEN_TRADES = int(os. getenv("MAX_OPEN_TRADES", "5"))
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
DB_PATH = os.getenv("DB_PATH", "scanner. db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "info").lower()

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
MAX_RAW_WS_MESSAGES = int(os.getenv("MAX_RAW_WS_MESSAGES", "1000"))
MAX_RAW_WS_MSG_BYTES = int(os.getenv("MAX_RAW_WS_MSG_BYTES", "2048"))

PUBLIC_REQ_CONCURRENCY = int(os.getenv("PUBLIC_REQ_CONCURRENCY", "8"))
PUBLIC_REQ_RETRIES = int(os.getenv("PUBLIC_REQ_RETRIES", "3"))

DISCOVERY_CONCURRENCY = int(os.getenv("DISCOVERY_CONCURRENCY", "24"))
ROOT_DEDUP_SECONDS = int(os.getenv("ROOT_DEDUP_SECONDS", "300"))
ROOT_SIGNALS_LOG_INTERVAL = int(os.getenv("ROOT_SIGNALS_LOG_INTERVAL", "30"))
FLIP_STABILITY_SECONDS = int(os.getenv("FLIP_STABILITY_SECONDS", "0"))

TELEGRAM_WORKER_CONCURRENCY = 1
TELEGRAM_RETRY_LIMIT = 4

CANDLE_CACHE_MAX = int(os.getenv("CANDLE_CACHE_MAX", "2000"))
CANDLE_CACHE_TTL = int(os.getenv("CANDLE_CACHE_TTL", "300"))

MAINNET_API_HOST = "https://api.bybit.com"
TESTNET_API_HOST = "https://api-testnet.bybit.com"
PRIMARY_API_HOST = MAINNET_API_HOST if BYBIT_USE_MAINNET else TESTNET_API_HOST
API_HOSTS = [PRIMARY_API_HOST]
if PRIMARY_API_HOST == MAINNET_API_HOST: 
    API_HOSTS. append(TESTNET_API_HOST)
else:
    API_HOSTS. append(MAINNET_API_HOST)

PUBLIC_WS_URL = "wss://stream.bybit.com/v5/public/linear" if BYBIT_USE_MAINNET else "wss://stream-testnet.bybit.com/v5/public/linear"
PRIVATE_WS_URL = ("wss://stream.bybit.com/v5/private" if BYBIT_USE_MAINNET else "wss://stream-testnet.bybit.com/v5/private") if TRADING_ENABLED else None

INSTRUMENTS_ENDPOINTS = [
    "/v5/market/instruments-info? category=linear&instType=PERPETUAL",
    "/v5/market/instruments-info?category=linear",
    "/v5/market/instruments-info?category=perpetual",
    "/v5/market/instruments-info",
    "/v2/public/symbols",
    "/v2/public/tickers",
]
KLINE_ENDPOINTS = ["/v5/market/kline", "/v2/public/kline/list", "/v2/public/kline"]

TF_MAP = {"5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "D"}
REVERSE_TF_MAP = {v: k for k, v in TF_MAP.items()}

CANDIDATE_PUBLIC_TEMPLATES = [
    "klineV2.{interval}. {symbol}",
    "kline. {interval}.{symbol}",
    "klineV2:{interval}:{symbol}",
    "kline:{interval}:{symbol}",
    "kline:{symbol}:{interval}",
]

MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
MIN_CANDLES_REQUIRED = MACD_SLOW + MACD_SIGNAL + 5

LEVERAGE = int(os.getenv("LEVERAGE", "3"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.015"))
BREAKEVEN_PCT = float(os.getenv("BREAKEVEN_PCT", "0.005"))
STABLECOINS = {"USDT", "BUSD", "USDC", "TUSD", "DAI"}

# ---------- Globals ----------
app = None
httpx_client = httpx.AsyncClient(timeout=20)
db:  Optional[aiosqlite.Connection] = None

symbols_info_cache: Dict[str, Dict[str, Any]] = {}

candles_cache: Dict[str, Dict[str, deque]] = defaultdict(lambda: {})
candles_cache_ts: Dict[str, Dict[str, float]] = defaultdict(lambda: {})

active_root_signals: Dict[str, Dict[str, Any]] = {}
active_signal_index: Dict[str, set] = defaultdict(set)
recent_root_signals: Dict[str, int] = {}
last_root_processed:  Dict[str, int] = {}

root_signal_symbols: set = set()

public_ws = None
private_ws = None

PUBLIC_REQUEST_SEMAPHORE = asyncio.Semaphore(PUBLIC_REQ_CONCURRENCY)

TELEGRAM_QUEUE:  "asyncio.Queue[Tuple[str,int]]" = asyncio.Queue()
_TELEGRAM_WORKER_TASK: Optional[asyncio.Task] = None

symbol_locks: Dict[str, asyncio. Lock] = {}
observed_flip_registry: Dict[Tuple[str, str, int], Dict[str, int]] = {}

# ---------- Utilities ----------
def log(*args, **kwargs):
    """Log with ISO timestamp."""
    if LOG_LEVEL != "none":
        ts = datetime.now(timezone.utc).isoformat()
        print(ts, *args, **kwargs)


def now_ts_ms() -> int:
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)


def get_symbol_lock(symbol: str) -> asyncio.Lock:
    """Get or create an asyncio lock for a symbol."""
    lock = symbol_locks.get(symbol)
    if lock is None: 
        lock = asyncio.Lock()
        symbol_locks[symbol] = lock
    return lock


# ---------- Cache helpers ----------
def cache_get(symbol: str, token: str) -> Optional[deque]:
    """Get cached candles deque for symbol/token."""
    return candles_cache. get(symbol, {}).get(token)


def cache_needs_refresh(symbol: str, token: str) -> bool:
    """Check if cache needs refresh based on TTL."""
    ts = candles_cache_ts. get(symbol, {}).get(token, 0)
    return (time.time() - ts) > CANDLE_CACHE_TTL


def candles_to_closes(dq: deque) -> List[float]:
    """Extract closing prices from deque of candles."""
    return [float(c. get("close", 0)) for c in dq]


def last_candle_is_closed(dq: deque, token: str, safety_seconds: int = 0) -> bool:
    """Check if the last candle in deque is closed."""
    if not dq:
        return False
    last = list(dq)[-1]
    start = last. get("start", 0)
    interval_sec = interval_seconds_from_token(token)
    candle_end = start + interval_sec
    current_time = int(time.time())
    return current_time >= (candle_end + safety_seconds)


def interval_seconds_from_token(token: str) -> int:
    """Convert token (5, 15, 60, 240, D) to seconds."""
    if token == "D":
        return 86400
    try:
        return int(token) * 60
    except: 
        return 0


def macd_positive_from_closes(closes: List[float]) -> bool:
    """Check if MACD histogram is positive (last value > 0)."""
    if len(closes) < MACD_SLOW + MACD_SIGNAL: 
        return False
    hist = macd_hist(closes)
    if not hist or len(hist) == 0:
        return False
    last_hist = hist[-1]
    return last_hist is not None and last_hist > 0


# ---------- Signal index helpers ----------
def signal_exists_for(symbol: str, signal_type: str) -> bool:
    """Check if active signal exists for symbol/type."""
    return signal_type in active_signal_index.get(symbol, set())


def register_signal_index(symbol: str, signal_type: str):
    """Register signal in index."""
    if symbol not in active_signal_index:
        active_signal_index[symbol] = set()
    active_signal_index[symbol].add(signal_type)


def unregister_signal_index(symbol: str, signal_type: str):
    """Remove signal from index."""
    if symbol in active_signal_index: 
        active_signal_index[symbol].discard(signal_type)
        if not active_signal_index[symbol]:
            del active_signal_index[symbol]


# ---------- Stablecoin helper ----------
def is_stablecoin_symbol(symbol: str) -> bool:
    """Check if symbol is a stablecoin pair."""
    for stab in STABLECOINS: 
        if symbol.startswith(stab) or symbol.endswith(stab):
            return True
    return False


# ---------- Timeframe helpers ----------
def tf_list_for_root(root_tf: str) -> List[str]:
    """Get required timeframes for a root timeframe."""
    if root_tf == "1h":
        return ["5m", "15m", "1h"]
    elif root_tf == "4h":
        return ["5m", "15m", "1h", "4h"]
    else:
        return ["5m", "15m"]


# ---------- EMA / MACD ----------
def ema(values: List[float], period:  int) -> List[float]:
    """Calculate Exponential Moving Average."""
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
    """Calculate MACD histogram with proper handling of None padding."""
    if len(prices) < slow + signal:
        return [None] * len(prices)

    ema_fast = ema(prices, fast)
    ema_slow = ema(prices, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = ema(macd_line, signal)

    # Align histogram length with prices
    hist = [None] * len(prices)
    start_idx = len(prices) - len(signal_line)
    for i, (m, s) in enumerate(zip(macd_line[start_idx:], signal_line)):
        hist[start_idx + i] = m - s

    return hist


# ---------- Resilient public GET ----------
async def resilient_public_get(endpoints: List[str], params: Dict[str, Any] = None, timeout: int = 12) -> Optional[Dict[str, Any]]: 
    """Resilient public API request with retries and fallback hosts."""
    last_exc = None
    for attempt in range(PUBLIC_REQ_RETRIES):
        for host in API_HOSTS:
            for ep in endpoints:
                url = host + ep
                await PUBLIC_REQUEST_SEMAPHORE. acquire()
                try:
                    try:
                        r = await httpx_client.get(url, params=params or {}, timeout=timeout)
                    except Exception as e:
                        last_exc = e
                        log("Network error for", url, "->", e)
                        continue
                    if r.status_code == 429:
                        retry_after = 5
                        try:
                            retry_after = int(r.headers.get("Retry-After", retry_after))
                        except Exception:
                            try:
                                body = r.json()
                                retry_after = int(body.get("parameters", {}).get("retry_after", retry_after))
                            except Exception:
                                pass
                        log("Received 429 from", url, "retry_after", retry_after)
                        await asyncio.sleep(retry_after)
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
                finally:
                    try:
                        PUBLIC_REQUEST_SEMAPHORE. release()
                    except Exception: 
                        pass
        backoff = min(60, 2 ** attempt)
        await asyncio.sleep(backoff)
    if last_exc:
        log("resilient_public_get exhausted retries; last exception:", last_exc)
    return None


# ---------- Signed request ----------
def bybit_sign_v5(api_secret: str, timestamp: str, method: str, path: str, body: str) -> str:
    """Generate Bybit V5 API signature."""
    prehash = timestamp + method. upper() + path + (body or "")
    return hmac.new(api_secret.encode(), prehash.encode(), hashlib.sha256).hexdigest()


async def bybit_signed_request(method: str, endpoint: str, payload: Dict[str, Any] = None):
    """Make authenticated request to Bybit API."""
    ts = str(int(time.time() * 1000))
    body = json.dumps(payload) if payload else ""
    signature = bybit_sign_v5(BYBIT_API_SECRET or "", ts, method. upper(), endpoint, body)
    headers = {
        "Content-Type": "application/json",
        "X-BAPI-API-KEY": BYBIT_API_KEY or "",
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-SIGN":  signature,
    }
    url = PRIMARY_API_HOST + endpoint
    for attempt in range(3):
        try:
            if method. upper() == "GET":
                r = await httpx_client.get(url, params=payload or {}, headers=headers, timeout=20)
            else:
                r = await httpx_client.post(url, content=body or "{}", headers=headers, timeout=20)
            if r.status_code == 429:
                retry_after = 5
                try:
                    retry_after = int(r. headers.get("Retry-After", retry_after))
                except Exception:
                    try: 
                        body_json = r.json()
                        retry_after = int(body_json.get("parameters", {}).get("retry_after", retry_after))
                    except Exception: 
                        pass
                log("Bybit signed request 429, retry_after", retry_after)
                await asyncio.sleep(retry_after)
                continue
            try:
                return r.json()
            except Exception:
                log("Signed request returned non-json", r.text)
                return {}
        except Exception as e: 
            log("Signed request error:", e)
            await asyncio.sleep(min(60, 2 ** attempt))
            continue
    return {}


# ---------- Kline fetching and caching ----------
async def fetch_klines(symbol: str, token: str, limit: int = 200) -> Optional[List[Dict[str, Any]]]:
    """Fetch klines from Bybit API."""
    params = {"symbol": symbol, "interval": token, "limit": limit}
    result = await resilient_public_get(KLINE_ENDPOINTS, params=params, timeout=12)
    if not result:
        return None
    try:
        data = result.get("result", {}) if isinstance(result. get("result"), dict) else result. get("result", [])
        if isinstance(data, dict):
            data = data.get("list", [])

        klines = []
        for item in data:
            if not item or len(item) < 5:
                continue
            try:
                start = int(float(item[0]))
                close = float(item[4])
                klines.append({"start": start, "close": close})
            except Exception:
                continue
        return klines if klines else None
    except Exception as e:
        log("parse klines error:", e)
        return None


def merge_into_cache(symbol: str, token: str, candles: List[Dict[str, Any]]):
    """Merge fetched candles into cache with TTL tracking."""
    if symbol not in candles_cache:
        candles_cache[symbol] = {}
    if token not in candles_cache[symbol]:
        candles_cache[symbol][token] = deque(maxlen=CANDLE_CACHE_MAX)

    dq = candles_cache[symbol][token]
    for c in candles:
        if not any(x["start"] == c["start"] for x in dq):
            dq.append(c)

    # Always update TTL timestamp
    if symbol not in candles_cache_ts:
        candles_cache_ts[symbol] = {}
    candles_cache_ts[symbol][token] = time.time()

    log(f"Cache merge for {symbol} {token}:  {len(dq)} candles, TTL reset")


# ---------- Ensure candles cached ----------
async def ensure_cached_candles(symbol: str, tf: str, required:  int):
    """Ensure we have enough cached candles, refreshing if needed."""
    token = TF_MAP[tf]
    dq = cache_get(symbol, token)

    # Check both quantity AND freshness
    if dq and len(dq) >= required and not cache_needs_refresh(symbol, token):
        log(f"Cache hit for {symbol} {tf}:  {len(dq)} candles, fresh")
        return

    try:
        log(f"Fetching {symbol} {tf}:  cache {'empty' if not dq else 'stale or too small'}")
        fetched = await fetch_klines(symbol, token, limit=max(required * 2, required + 50))
        if fetched: 
            merge_into_cache(symbol, token, fetched)
            log(f"Updated cache for {symbol} {tf}:  now {len(cache_get(symbol, token) or [])} candles")
        else:
            log(f"No klines fetched for {symbol} {tf}")
    except Exception as e: 
        log("ensure_cached_candles fetch error:", e)


# ---------- Detect flip ----------
async def detect_flip(symbol: str, tf: str) -> Tuple[Optional[str], Optional[int]]:
    """Detect MACD flip (open candles allowed for real-time detection)."""
    token = TF_MAP. get(tf) if tf in TF_MAP else tf
    if token is None:
        token = TF_MAP. get(tf, tf)

    await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
    dq = cache_get(symbol, token)
    if not dq or len(dq) < MIN_CANDLES_REQUIRED: 
        return None, None

    closes = candles_to_closes(dq)
    if len(closes) < MACD_SLOW + MACD_SIGNAL:
        return None, None

    hist = macd_hist(closes)
    if not hist or len(hist) < 2:
        return None, None

    last_start = list(dq)[-1]["start"]

    # Allow mid-candle detection (don't skip open candles)
    is_closed = last_candle_is_closed(dq, token, safety_seconds=3)
    if is_closed:
        observed_flip_registry. pop((symbol, token, last_start), None)
        return None, None

    prev = hist[-2] or 0
    cur = hist[-1] or 0

    if prev <= 0 and cur > 0:
        now_s = int(time.time())
        key = (symbol, token, last_start)
        rec = observed_flip_registry.get(key)
        if rec is None:
            observed_flip_registry[key] = {"first_seen": now_s, "last_seen": now_s, "count": 1}
        else:
            rec["last_seen"] = now_s
            rec["count"] = rec. get("count", 0) + 1
            observed_flip_registry[key] = rec
        return "open", last_start

    return None, None


def flip_is_stable_enough(symbol: str, tf: str, start:  int) -> bool:
    """Check if flip is stable enough based on FLIP_STABILITY_SECONDS."""
    if FLIP_STABILITY_SECONDS <= 0:
        return True
    token = TF_MAP.get(tf) if tf in TF_MAP else tf
    if token is None and tf in REVERSE_TF_MAP: 
        token = tf
    if token is None:
        token = TF_MAP.get(tf, tf)
    key = (symbol, token, start)
    rec = observed_flip_registry.get(key)
    if not rec:
        return False
    return (int(time.time()) - int(rec["first_seen"])) >= FLIP_STABILITY_SECONDS


# ---------- Raw WS persistence ----------
async def persist_raw_ws(source: str, topic: str, message: str):
    """Persist raw WebSocket message to database."""
    try:
        if not db: 
            return
        await db.execute(
            "INSERT INTO raw_ws_messages (source, topic, message, created_at) VALUES (?,?,?,?)",
            (source, topic, message, now_ts_ms())
        )
        await db.commit()
    except Exception: 
        pass
