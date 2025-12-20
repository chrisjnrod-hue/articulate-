# services.py
# Core helpers: configuration, DB, MACD, candle cache, resilient requests,
# persistence, telegram queue, signal add/remove and small endpoints/startup glue.

import os
import time
import hmac
import hashlib
import asyncio
import json
import uuid
import re
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from collections import deque, defaultdict

import httpx
import aiosqlite
from fastapi import Depends, Header, HTTPException, status

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

PUBLIC_REQ_CONCURRENCY = int(os.getenv("PUBLIC_REQ_CONCURRENCY", "8"))
PUBLIC_REQ_RETRIES = int(os.getenv("PUBLIC_REQ_RETRIES", "3"))

DISCOVERY_CONCURRENCY = int(os.getenv("DISCOVERY_CONCURRENCY", "24"))
ROOT_DEDUP_SECONDS = int(os.getenv("ROOT_DEDUP_SECONDS", "300"))
FLIP_STABILITY_SECONDS = int(os.getenv("FLIP_STABILITY_SECONDS", "0"))

TELEGRAM_WORKER_CONCURRENCY = 1
TELEGRAM_RETRY_LIMIT = 4

CANDLE_CACHE_MAX = int(os.getenv("CANDLE_CACHE_MAX", "2000"))
CANDLE_CACHE_TTL = int(os.getenv("CANDLE_CACHE_TTL", "300"))

SKIP_DIGIT_PREFIX = os.getenv("SKIP_DIGIT_PREFIX", "false").lower() == "true"
SYMBOL_SCAN_LIMIT = int(os.getenv("SYMBOL_SCAN_LIMIT", "0"))

ROOT_SIGNALS_LOG_INTERVAL = int(os.getenv("ROOT_SIGNALS_LOG_INTERVAL", "60"))

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

TF_MAP = {"5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "D"}
REVERSE_TF_MAP = {v: k for k, v in TF_MAP.items()}

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
MIN_CANDLES_REQUIRED = MACD_SLOW + MACD_SIGNAL + 5

LEVERAGE = int(os.getenv("LEVERAGE", "3"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.015"))
BREAKEVEN_PCT = float(os.getenv("BREAKEVEN_PCT", "0.005"))
STABLECOINS = {"USDT", "BUSD", "USDC", "TUSD", "DAI"}

MIN_SYMBOL_AGE_MONTHS = int(os.getenv("MIN_SYMBOL_AGE_MONTHS", "0"))

# ---------- Globals ----------
app = None
httpx_client = httpx.AsyncClient(timeout=20)
db: Optional[aiosqlite.Connection] = None

symbols_info_cache: Dict[str, Dict[str, Any]] = {}

candles_cache: Dict[str, Dict[str, deque]] = defaultdict(lambda: {})
candles_cache_ts: Dict[str, Dict[str, int]] = defaultdict(lambda: {})

active_root_signals: Dict[str, Dict[str, Any]] = {}
active_signal_index: Dict[str, set] = defaultdict(set)
recent_root_signals: Dict[str, int] = {}
last_root_processed: Dict[str, int] = {}

public_ws = None
private_ws = None

PUBLIC_REQUEST_SEMAPHORE = asyncio.Semaphore(PUBLIC_REQ_CONCURRENCY)

TELEGRAM_QUEUE: "asyncio.Queue[Tuple[str,int]]" = asyncio.Queue()
_TELEGRAM_WORKER_TASK: Optional[asyncio.Task] = None

_re_leading_digit = re.compile(r"^\d")
symbol_locks: Dict[str, asyncio.Lock] = {}
observed_flip_registry: Dict[Tuple[str, str, int], Dict[str, int]] = {}

# ---------- Utilities ----------
def log(*args, **kwargs):
    if LOG_LEVEL != "none":
        ts = datetime.now(timezone.utc).isoformat()
        print(ts, *args, **kwargs)

def now_ts_ms() -> int:
    return int(time.time() * 1000)

def get_symbol_lock(symbol: str) -> asyncio.Lock:
    lock = symbol_locks.get(symbol)
    if lock is None:
        lock = asyncio.Lock()
        symbol_locks[symbol] = lock
    return lock

# ---------- Telegram worker & queue ----------
async def _telegram_worker():
    while True:
        try:
            item = await TELEGRAM_QUEUE.get()
            if item is None:
                TELEGRAM_QUEUE.task_done()
                continue
            text, attempt = item
            if attempt is None:
                attempt = 0
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
            try:
                r = await httpx_client.post(url, json=payload, timeout=10)
                if r.status_code == 200:
                    try:
                        data = r.json()
                    except Exception:
                        data = {"raw_text": (r.text[:400] + "...") if r.text else ""}
                    log("Telegram sent ok (worker) response:", data)
                elif r.status_code == 429:
                    retry_after = 5
                    try:
                        data = r.json()
                        retry_after = int(data.get("parameters", {}).get("retry_after", retry_after))
                    except Exception:
                        try:
                            retry_after = int(r.headers.get("Retry-After", retry_after))
                        except Exception:
                            pass
                    log("Telegram send failed: 429 rate limit, retry after", retry_after)
                    await asyncio.sleep(retry_after)
                    if attempt + 1 < TELEGRAM_RETRY_LIMIT:
                        await TELEGRAM_QUEUE.put((text, attempt + 1))
                    else:
                        log("Telegram send retry limit reached, dropping message")
                else:
                    log("Telegram send failed:", r.status_code, r.text)
                    if attempt + 1 < TELEGRAM_RETRY_LIMIT:
                        backoff = min(60, (2 ** attempt))
                        await asyncio.sleep(backoff)
                        await TELEGRAM_QUEUE.put((text, attempt + 1))
                    else:
                        log("Telegram send retry limit reached, dropping message")
            except Exception as e:
                log("Telegram worker error:", e)
                if attempt + 1 < TELEGRAM_RETRY_LIMIT:
                    backoff = min(60, (2 ** attempt))
                    await asyncio.sleep(backoff)
                    await TELEGRAM_QUEUE.put((text, attempt + 1))
                else:
                    log("Telegram send failed after retries, dropping")
            finally:
                TELEGRAM_QUEUE.task_done()
        except Exception as e:
            log("Unexpected telegram worker loop error:", e)
            await asyncio.sleep(1)

async def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram not configured; skipping message:", text)
        return
    global _TELEGRAM_WORKER_TASK
    if _TELEGRAM_WORKER_TASK is None or _TELEGRAM_WORKER_TASK.done():
        try:
            _TELEGRAM_WORKER_TASK = asyncio.create_task(_telegram_worker())
        except Exception:
            pass
    try:
        await TELEGRAM_QUEUE.put((text, 0))
    except Exception as e:
        log("Failed to enqueue telegram message:", e)

async def send_root_signals_telegram():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram not configured; skipping root signals telegram")
        return
    if not active_root_signals:
        await send_telegram("Root signals: 0")
        return
    lines = ["Root signals summary:"]
    for sig in active_root_signals.values():
        sym = sig.get("symbol")
        tf = sig.get("root_tf")
        status = sig.get("status", "watching")
        lines.append(f"{sym} {tf} {status}")
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
    last_exc = None
    for attempt in range(PUBLIC_REQ_RETRIES):
        for host in API_HOSTS:
            for ep in endpoints:
                url = host + ep
                await PUBLIC_REQUEST_SEMAPHORE.acquire()
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
                        PUBLIC_REQUEST_SEMAPHORE.release()
                    except Exception:
                        pass
        backoff = min(60, 2 ** attempt)
        await asyncio.sleep(backoff)
    if last_exc:
        log("resilient_public_get exhausted retries; last exception:", last_exc)
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
    for attempt in range(3):
        try:
            if method.upper() == "GET":
                r = await httpx_client.get(url, params=payload or {}, headers=headers, timeout=20)
            else:
                r = await httpx_client.post(url, content=body or "{}", headers=headers, timeout=20)
            if r.status_code == 429:
                retry_after = 5
                try:
                    retry_after = int(r.headers.get("Retry-After", retry_after))
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

# ---------- SQLite init & persistence ----------
async def init_db():
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("""CREATE TABLE IF NOT EXISTS root_signals (id TEXT PRIMARY KEY, symbol TEXT, root_tf TEXT, flip_time INTEGER, flip_price REAL, status TEXT, priority TEXT, signal_type TEXT, components TEXT, created_at INTEGER)""")
    await db.execute("""CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, symbol TEXT, side TEXT, qty REAL, entry_price REAL, sl_price REAL, created_at INTEGER, open BOOLEAN, raw_response TEXT)""")
    await db.execute("""CREATE TABLE IF NOT EXISTS raw_ws_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, topic TEXT, message TEXT, created_at INTEGER)""")
    await db.execute("""CREATE TABLE IF NOT EXISTS public_subscriptions (topic TEXT PRIMARY KEY, created_at INTEGER)""")
    await db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_root_signals_symbol_tf_flip ON root_signals(symbol, root_tf, flip_time)""")
    await db.commit()
    log("DB initialized at", DB_PATH)

async def persist_root_signal(sig: Dict[str, Any]):
    comps = json.dumps(sig.get("components") or [])
    try:
        await db.execute("INSERT OR IGNORE INTO root_signals (id,symbol,root_tf,flip_time,flip_price,status,priority,signal_type,components,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                         (sig["id"], sig["symbol"], sig.get("root_tf"), sig.get("root_flip_time") or sig.get("flip_time"), sig.get("root_flip_price") or sig.get("flip_price"), sig.get("status", "watching"), sig.get("priority"), sig.get("signal_type", "root"), comps, sig.get("created_at")))
        await db.commit()
    except Exception as e:
        log("persist_root_signal DB error:", e)

async def remove_root_signal(sig_id: str):
    await db.execute("DELETE FROM root_signals WHERE id = ?", (sig_id,))
    await db.commit()

# ---------- Add / remove signals with improved notification and dedupe ----------
async def add_signal(sig: Dict[str, Any]) -> bool:
    sid = sig["id"]
    stype = sig.get("signal_type", "root")
    sym = sig["symbol"]
    root_time = sig.get("root_flip_time") or sig.get("flip_time")
    lock = get_symbol_lock(sym)
    async with lock:
        if signal_exists_for(sym, stype):
            log("Duplicate signal suppressed (inside lock, by active_signal_index):", sym, stype)
            return False
        if stype == "root" and root_time is not None:
            existing_recent = recent_root_signals.get(sym)
            if existing_recent == root_time:
                log("Duplicate root signal suppressed by recent_root_signals:", sym, root_time)
                return False
            for existing in active_root_signals.values():
                if existing.get("symbol") == sym and existing.get("signal_type", "root") == stype:
                    if existing.get("root_flip_time") == root_time:
                        log("Duplicate existing signal found (inside lock):", sym, stype, root_time)
                        return False
            try:
                root_tf = sig.get("root_tf")
                token = TF_MAP.get(root_tf)
                if token:
                    await ensure_cached_candles(sym, root_tf, MIN_CANDLES_REQUIRED)
                    dq = cache_get(sym, token)
                    if not dq or len(dq) == 0:
                        log("add_signal: cannot validate root signal, missing cache for", sym, root_tf)
                        return False
                    last_start = list(dq)[-1]["start"]
                    if last_candle_is_closed(dq, token, safety_seconds=3):
                        log("add_signal: last candle is closed for", sym, root_tf, "rejecting root signal with flip_time", root_time)
                        return False
                    if last_start != root_time:
                        log("add_signal: root_flip_time does not match current open candle for", sym, root_tf, "expected", last_start, "got", root_time)
                        return False
            except Exception as e:
                log("add_signal validation error:", e)
                return False
        if sid in active_root_signals:
            log("Signal id already present, skipping:", sid)
            return False
        active_root_signals[sid] = sig
        register_signal_index(sym, stype)
        # Change 1: store wall-clock time (cooldown timestamp) instead of candle start time
        if stype == "root":
            recent_root_signals[sym] = int(time.time())
        try:
            await persist_root_signal(sig)
        except Exception as e:
            log("persist_root_signal error:", e)
        log("Added signal:", sid, stype, sym)
        # send per-signal update once per addition
        try:
            await send_telegram(f"Added signal: {sid} {stype} {sym}")
        except Exception:
            pass

    # Out of lock: run subscription / prewarm and alignment notifier asynchronously in order
    try:
        async def _post_add_flow():
            # For root signals: subscribe this symbol to 5m and 15m (active roots only)
            if stype == "root":
                try:
                    global public_ws
                    # subscribe to 5m and 15m for this symbol so MTF checks get live updates
                    if public_ws:
                        try:
                            await public_ws.subscribe_kline(sym, TF_MAP["5m"])
                        except Exception as e:
                            log("Error subscribing 5m for", sym, e)
                        try:
                            await public_ws.subscribe_kline(sym, TF_MAP["15m"])
                        except Exception as e:
                            log("Error subscribing 15m for", sym, e)
                    # prewarm caches for the small TFs so alignment check sees data
                    try:
                        await asyncio.gather(
                            ensure_cached_candles(sym, "5m", MIN_CANDLES_REQUIRED),
                            ensure_cached_candles(sym, "15m", MIN_CANDLES_REQUIRED),
                        )
                    except Exception as e:
                        log("Prewarm small TFs error for", sym, e)
                except Exception as e:
                    log("post_add subscribe/prewarm error:", e)
            # Finally, run notifier which will now find fresh cache and possibly alert
            try:
                await notify_alignment_if_ready(sig)
            except Exception as e:
                log("notify_alignment_if_ready post-add error:", e)

        asyncio.create_task(_post_add_flow())
    except Exception:
        pass
    return True

async def remove_signal(sig_id: str):
    sig = active_root_signals.pop(sig_id, None)
    if not sig:
        return
    stype = sig.get("signal_type", "root")
    sym = sig.get("symbol")
    if stype == "root":
        # Change 2: cleanup with pop, we don't care about the candle timestamp here anymore
        try:
            recent_root_signals.pop(sym, None)
        except Exception:
            pass
    unregister_signal_index(sym, stype)
    try:
        await remove_root_signal(sig_id)
    except Exception:
        pass

    # If we removed a root, unsubscribe 5m/15m for that symbol only if no other root remains
    if stype == "root":
        try:
            other_root_exists = any(x.get("symbol") == sym and x.get("signal_type") == "root" for x in active_root_signals.values())
            global public_ws
            if public_ws and not other_root_exists:
                try:
                    await public_ws.unsubscribe_kline(sym, TF_MAP["5m"])
                except Exception as e:
                    log("Error unsubscribing 5m for", sym, e)
                try:
                    await public_ws.unsubscribe_kline(sym, TF_MAP["15m"])
                except Exception as e:
                    log("Error unsubscribing 15m for", sym, e)
        except Exception:
            pass

    log("Removed signal:", sig_id)

# ---------- ensure_cached_candles ----------
async def ensure_cached_candles(symbol: str, tf: str, required: int):
    token = TF_MAP[tf]
    dq = cache_get(symbol, token)
    if dq and len(dq) >= required and not cache_needs_refresh(symbol, token):
        return
    try:
        fetched = await fetch_klines(symbol, token, limit=max(required * 2, required + 50))
        if fetched:
            merge_into_cache(symbol, token, fetched)
            log(f"Updated cache for {symbol} {tf}: now {len(cache_get(symbol, token) or [])} candles")
    except Exception as e:
        log("ensure_cached_candles fetch error:", e)

# ---------- Detect flip: OPEN ONLY ----------
async def detect_flip(symbol: str, tf: str) -> Tuple[Optional[str], Optional[int]]:
    token = TF_MAP.get(tf) if tf in TF_MAP else tf
    # allow passing token directly
    if token is None:
        token = TF_MAP.get(tf, tf)
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
    if last_candle_is_closed(dq, token, safety_seconds=3):
        observed_flip_registry.pop((symbol, token, last_start), None)
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
            rec["count"] = rec.get("count", 0) + 1
            observed_flip_registry[key] = rec
        return "open", last_start
    return None, None

def flip_is_stable_enough(symbol: str, tf: str, start: int) -> bool:
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
    try:
        if not db:
            return
        await db.execute("INSERT INTO raw_ws_messages (source, topic, message, created_at) VALUES (?,?,?,?)", (source, topic, message, now_ts_ms()))
        await db.commit()
    except Exception:
        pass

# ---------- Load persisted signals ----------
async def load_persisted_root_signals():
    try:
        async with db.execute("SELECT id,symbol,root_tf,flip_time,flip_price,status,priority,signal_type,components,created_at FROM root_signals") as cur:
            rows = await cur.fetchall()
        max_flip_per_symbol: Dict[str,int] = {}
        loaded = 0
        for r in rows:
            try:
                rid = r[0]
                symbol = r[1]
                root_tf = r[2]
                flip_time = r[3]
                flip_price = r[4]
                status = r[5] or "watching"
                priority = r[6]
                signal_type = r[7] or "root"
                comps = []
                try:
                    comps = json.loads(r[8]) if r[8] else []
                except Exception:
                    comps = []
                created_at = r[9] or now_ts_ms()

                expired = False
                try:
                    if signal_type == "root" and root_tf and flip_time:
                        token = TF_MAP.get(root_tf)
                        if token:
                            expiry = flip_time + interval_seconds_from_token(token)
                            if int(time.time()) >= expiry:
                                expired = True
                except Exception:
                    pass

                if expired:
                    try:
                        await remove_root_signal(rid)
                        log("Removed expired persisted signal from DB:", rid)
                    except Exception as e:
                        log("Error removing expired persisted signal", rid, e)
                    continue

                active_root_signals[rid] = {
                    "id": rid,
                    "symbol": symbol,
                    "root_tf": root_tf,
                    "root_flip_time": flip_time,
                    "root_flip_price": flip_price,
                    "status": status,
                    "priority": priority,
                    "signal_type": signal_type,
                    "components": comps,
                    "created_at": created_at,
                }
                register_signal_index(symbol, signal_type)
                if symbol and flip_time:
                    cur_max = max_flip_per_symbol.get(symbol)
                    if cur_max is None or flip_time > cur_max:
                        max_flip_per_symbol[symbol] = flip_time
                loaded += 1
            except Exception:
                continue
        for sym, t in max_flip_per_symbol.items():
            try:
                recent_root_signals[sym] = t
            except Exception:
                pass
        log("Loaded", loaded, "persisted root signals from DB and seeded recent_root_signals")
    except Exception as e:
        log("load_persisted_root_signals error:", e)

# ---------- Notify persisted roots after startup ----------
async def _notify_loaded_roots_after_startup():
    """
    After startup has loaded persisted root signals and WS/connect tasks have been started,
    ensure subscriptions & caches for each persisted root and run the alignment notifier.

    This will make persisted roots (loaded from DB) also trigger MTF alignment notifications.
    """
    try:
        if not active_root_signals:
            return
        sem = asyncio.Semaphore(min(10, DISCOVERY_CONCURRENCY or 10))
        async def _worker(sig):
            sym = sig.get("symbol")
            root_tf = sig.get("root_tf")
            try:
                # Wait briefly for public_ws to be connected (bounded)
                wait_attempts = 0
                while wait_attempts < 8:
                    if public_ws and getattr(public_ws, "conn", None):
                        break
                    await asyncio.sleep(1)
                    wait_attempts += 1
                # Subscribe small TFs (5m/15m) so notifier can see live updates
                if public_ws:
                    try:
                        await public_ws.subscribe_kline(sym, TF_MAP["5m"])
                    except Exception as e:
                        log("Startup subscribe 5m error for", sym, e)
                    try:
                        await public_ws.subscribe_kline(sym, TF_MAP["15m"])
                    except Exception as e:
                        log("Startup subscribe 15m error for", sym, e)
                # Ensure caches for required TFs
                tfs = tf_list_for_root(root_tf)
                for tf in tfs:
                    try:
                        await ensure_cached_candles(sym, tf, MIN_CANDLES_REQUIRED)
                    except Exception as e:
                        log("Startup prewarm error for", sym, tf, e)
                # Give a tiny moment for WS merges to occur if any in-flight
                await asyncio.sleep(0.5)
                # Run notifier
                try:
                    await notify_alignment_if_ready(sig)
                except Exception as e:
                    log("Startup notify_alignment_if_ready error for", sym, e)
            except Exception as e:
                log("notify_loaded_roots worker error for", sym, e)

        tasks = []
        for sig in list(active_root_signals.values()):
            if sig.get("signal_type") != "root":
                continue
            async def _wrap(s=sig):
                async with sem:
                    await _worker(s)
            tasks.append(asyncio.create_task(_wrap()))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        log("notify_loaded_roots_after_startup error:", e)

# ---------- Debug helpers ----------
def log_current_root_signals():
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
            stype = sig.get("signal_type", "root")
            created = sig.get("created_at")
            comps = sig.get("components", "")
            flip_kind = sig.get("flip_kind", "")
            log(f" {idx}. id={key} symbol={symbol} type={stype} root_tf={root_tf} status={status} priority={priority} components={comps} flip_kind={flip_kind} created_at={created}")
    except Exception as e:
        log("log_current_root_signals error:", e)

# ---------- Debug endpoints & startup glue ----------
def init_app(fastapi_app):
    """
    Register endpoints and startup event. Start background tasks by importing scanners lazily.
    """
    global app
    app = fastapi_app

    @app.get("/debug/current_roots")
    async def debug_current_roots(_auth=Depends(require_admin_auth)):
        return {"count": len(active_root_signals), "signals": list(active_root_signals.values())}

    @app.get("/debug/check_symbol")
    async def debug_check_symbol(symbol: str, tf: str = "1h", _auth=Depends(require_admin_auth)):
        if tf not in TF_MAP:
            raise HTTPException(status_code=400, detail=f"Unknown timeframe {tf}. Valid: {list(TF_MAP.keys())}")
        token = TF_MAP[tf]
        await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
        dq = cache_get(symbol, token)
        if not dq:
            return {"symbol": symbol, "tf": tf, "klines_count": 0, "error": "no cached klines"}
        closes = candles_to_closes(dq)
        hist = macd_hist(closes)
        flip_kind, flip_ts = await detect_flip(symbol, tf)
        closed = last_candle_is_closed(dq, token)
        registry_info = None
        last_start = list(dq)[-1]["start"] if dq else None
        if last_start:
            key = (symbol, token, last_start)
            registry_info = observed_flip_registry.get(key)
        return {
            "symbol": symbol,
            "tf": tf,
            "cached_klines": len(dq),
            "macd_hist_len": len(hist),
            "macd_last": hist[-1] if hist else None,
            "flip_kind": flip_kind,
            "flip_ts": flip_ts,
            "last_closed": closed,
            "sample_last_klines": list(dq)[-5:],
            "observed_flip_registry": registry_info,
        }

    @app.post("/admin/send_root_summary")
    async def admin_send_root_summary(_auth=Depends(require_admin_auth)):
        await send_root_signals_telegram()
        return {"status": "ok", "sent": True}

    @app.get("/")
    async def root_route():
        return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}

    @app.get("/health")
    async def health():
        db_ok = db is not None
        public_ws_connected = False
        try:
            public_ws_connected = bool(public_ws and getattr(public_ws, "conn", None))
        except Exception:
            public_ws_connected = False
        return {"status": "ok", "db": db_ok, "public_ws_connected": public_ws_connected, "time": datetime.now(timezone.utc).isoformat()}

    # Debug endpoint you requested for manual subscribe testing.
    @app.post("/debug/ws/subscribe_test")
    async def debug_ws_subscribe_test(symbol: str, tf: str = "15m", _auth=Depends(require_admin_auth)):
        """
        Example: POST /debug/ws/subscribe_test?symbol=ETHUSDT&tf=15m
        Will attempt to subscribe the public websocket to the kline topic for the given symbol/tf.
        """
        if tf not in TF_MAP:
            raise HTTPException(status_code=400, detail=f"Unknown tf {tf}. Valid: {list(TF_MAP.keys())}")
        if not public_ws:
            raise HTTPException(status_code=500, detail="public_ws not initialized")
        token = TF_MAP[tf]
        topic = f"klineV2.{token}.{symbol}"
        try:
            await public_ws.subscribe_kline(symbol, token)
            # prewarm cache (best-effort)
            try:
                await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
            except Exception:
                pass
            return {"status": "ok", "subscribed": topic}
        except Exception as e:
            log("debug_ws_subscribe_test error:", e)
            raise HTTPException(status_code=500, detail=f"subscribe failed: {e}")

    @app.on_event("startup")
    async def startup_event():
        global public_ws, _TELEGRAM_WORKER_TASK
        await init_db()
        await load_persisted_root_signals()
        log("Startup config:", "BYBIT_USE_MAINNET=", BYBIT_USE_MAINNET, "TRADING_ENABLED=", TRADING_ENABLED, "PUBLIC_WS_URL=", PUBLIC_WS_URL, "MIN_CANDLES_REQUIRED=", MIN_CANDLES_REQUIRED, "SKIP_DIGIT_PREFIX=", SKIP_DIGIT_PREFIX, "MIN_SYMBOL_AGE_MONTHS=", MIN_SYMBOL_AGE_MONTHS, "FLIP_STABILITY_SECONDS=", FLIP_STABILITY_SECONDS)

        # Import scanners lazily so scanners can import services at runtime without circular import problems.
        try:
            import scanners as _sc
            # expose scanners' process_inprogress_update implementation to services (overrides placeholder)
            globals()['process_inprogress_update'] = _sc.process_inprogress_update
            public_ws = _sc.PublicWebsocketManager(PUBLIC_WS_URL)
            ok = await public_ws.connect_and_detect(timeout=6.0)
            if not ok:
                log("Public WS detect warning")
        except Exception:
            log("Public WS connect error")

        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and (_TELEGRAM_WORKER_TASK is None or _TELEGRAM_WORKER_TASK.done()):
            try:
                _TELEGRAM_WORKER_TASK = asyncio.create_task(_telegram_worker())
            except Exception:
                pass

        # Start background scanner loops from scanners module
        try:
            import scanners as _sc
            asyncio.create_task(_sc.root_scanner_loop("1h"))
            asyncio.create_task(_sc.root_scanner_loop("4h"))
            asyncio.create_task(_sc.evaluate_signals_loop())
            asyncio.create_task(_sc.expire_signals_loop())
            asyncio.create_task(_sc.periodic_root_logger())
        except Exception as e:
            log("Error starting scanner background tasks:", e)

        # Kick off post-startup notification for persisted roots (subscribes + prewarms + notifier)
        try:
            asyncio.create_task(_notify_loaded_roots_after_startup())
        except Exception as e:
            log("Failed to schedule notify_loaded_roots_after_startup:", e)

        log("Background tasks started")
