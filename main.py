# main.py
# Bybit MACD multi-timeframe scanner — flip-on-open ONLY with robust dedupe, websockets, telegram queue, persistence
#
# Full single-file implementation. Replace your existing main.py with this file.
#
# Run:
#   uvicorn main:app --host 0.0.0.0 --port 8000

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
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from collections import deque, defaultdict

import httpx
import aiosqlite
import websockets
from fastapi import FastAPI, Query, Depends, Header, HTTPException, status
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

# concurrency for public API requests (to avoid huge parallel bursts)
PUBLIC_REQ_CONCURRENCY = int(os.getenv("PUBLIC_REQ_CONCURRENCY", "8"))
# number of retries for public requests
PUBLIC_REQ_RETRIES = int(os.getenv("PUBLIC_REQ_RETRIES", "3"))

# Telegram queue settings
TELEGRAM_WORKER_CONCURRENCY = 1
TELEGRAM_RETRY_LIMIT = int(os.getenv("TELEGRAM_RETRY_LIMIT", "4"))

# candle cache sizing and behavior
CANDLE_CACHE_MAX = int(os.getenv("CANDLE_CACHE_MAX", "2000"))     # keep up to N candles per symbol/tf
CANDLE_CACHE_TTL = int(os.getenv("CANDLE_CACHE_TTL", "300"))      # seconds TTL for cache refresh per symbol/tf

# skip tokens beginning with digits (spam tokens like 1000000FOO)
SKIP_DIGIT_PREFIX = os.getenv("SKIP_DIGIT_PREFIX", "true").lower() == "true"

# optional fast-test limit: set >0 to scan only N symbols (helpful for dev/testing)
SYMBOL_SCAN_LIMIT = int(os.getenv("SYMBOL_SCAN_LIMIT", "0"))

ROOT_SIGNALS_LOG_INTERVAL = int(os.getenv("ROOT_SIGNALS_LOG_INTERVAL", "60"))

# Debugging flags for detect_flip
DETECT_DEBUG = os.getenv("DETECT_DEBUG", "false").lower() == "true"
DETECT_DEBUG_SYMBOLS = set(x.strip().upper() for x in os.getenv("DETECT_DEBUG_SYMBOLS", "").split(",") if x.strip())

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

# API endpoints tried by fetch_klines (we iterate until we get usable open/close data)
KLINE_ENDPOINTS = ["/v5/market/kline", "/v2/public/kline/list", "/v2/public/kline"]

INSTRUMENTS_ENDPOINTS = [
    "/v5/market/instruments-info?category=linear&instType=PERPETUAL",
    "/v5/market/instruments-info?category=linear",
    "/v5/market/instruments-info?category=perpetual",
    "/v5/market/instruments-info",
    "/v2/public/symbols",
    "/v2/public/tickers",
]

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

# super window seconds (5 minutes)
SUPER_WINDOW_SECONDS = int(os.getenv("SUPER_WINDOW_SECONDS", "300"))

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

# active signals in memory and indexes
active_root_signals: Dict[str, Dict[str, Any]] = {}
active_signal_index: Dict[str, set] = defaultdict(set)

# dedupe guards
recent_root_signals: Dict[str, int] = {}
recent_super_signals: Dict[str, Tuple[int, ...]] = {}
recent_entry_signals: Dict[str, int] = {}

last_root_processed: Dict[str, int] = {}

public_ws = None
private_ws = None

# concurrency primitives
PUBLIC_REQUEST_SEMAPHORE = asyncio.Semaphore(PUBLIC_REQ_CONCURRENCY)

# Telegram queue + worker
TELEGRAM_QUEUE: "asyncio.Queue[Tuple[str,int]]" = asyncio.Queue()
_TELEGRAM_WORKER_TASK: Optional[asyncio.Task] = None

# regex for symbols starting with digits
_re_leading_digit = re.compile(r"^\d")

# per-symbol asyncio locks to avoid race conditions that create duplicate signals/trades
symbol_locks: Dict[str, asyncio.Lock] = {}

def get_symbol_lock(symbol: str) -> asyncio.Lock:
    lock = symbol_locks.get(symbol)
    if lock is None:
        lock = asyncio.Lock()
        symbol_locks[symbol] = lock
    return lock

# ---------- Utilities ----------
def log(*args, **kwargs):
    if LOG_LEVEL != "none":
        ts = datetime.now(timezone.utc).isoformat()
        print(ts, *args, **kwargs)

def now_ts_ms() -> int:
    return int(time.time() * 1000)

# ---------------- Telegram worker & queue ----------------
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
            if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
                log("Telegram not configured; would send:", text)
                TELEGRAM_QUEUE.task_done()
                continue
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
                    log("Telegram send failed:", r.status_code, r.text)
                    if attempt + 1 < TELEGRAM_RETRY_LIMIT:
                        backoff = min(60, (2 ** attempt))
                        await asyncio.sleep(backoff)
                        await TELEGRAM_QUEUE.put((text, attempt + 1))
            except Exception as e:
                log("Telegram worker error:", e)
                if attempt + 1 < TELEGRAM_RETRY_LIMIT:
                    backoff = min(60, (2 ** attempt))
                    await asyncio.sleep(backoff)
                    await TELEGRAM_QUEUE.put((text, attempt + 1))
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

# ---------- SQLite init & helpers ----------
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

# ---------- Helper functions (robust instruments fetch) ----------
async def get_tradable_usdt_symbols() -> List[str]:
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
                    launch_ts = None
                    for k in ("launchTime", "launch_time", "listTime", "listedAt", "listed_time"):
                        if k in it and it[k]:
                            try:
                                v = int(float(it[k]))
                                if v > 10**12:
                                    v = v // 1000
                                launch_ts = v
                                break
                            except Exception:
                                continue
                else:
                    continue

                if not symbol:
                    continue
                if not (symbol.upper().endswith("USDT") or quote == "USDT"):
                    continue
                base = symbol[:-4] if symbol.upper().endswith("USDT") else symbol
                if base.upper() in STABLECOINS:
                    continue
                if SKIP_DIGIT_PREFIX and _re_leading_digit.match(symbol):
                    continue
                if status and isinstance(status, str) and status not in ("trading", "listed", "normal", "active", "open"):
                    continue
                if launch_ts:
                    age_days = (int(time.time()) - launch_ts) / 86400.0
                    if age_days < (MIN_SYMBOL_AGE_MONTHS * 30):
                        continue
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
    If 'open' is missing on any candle, backfill it from previous close when possible to guarantee open exists.
    """
    if not candles:
        return
    dq = cache_get(symbol, interval_token)
    if dq is None:
        dq = deque(maxlen=CANDLE_CACHE_MAX)
    existing_starts = set(x["start"] for x in dq)
    for c in candles:
        if c["start"] not in existing_starts:
            dq.append(c)
            existing_starts.add(c["start"])
    # ensure sorted ascending and backfill 'open' where possible
    sorted_list = sorted(list(dq), key=lambda x: x["start"])
    # backfill opens: if a candle lacks 'open', set it to previous candle's close (best-effort)
    prev_close = None
    for i, c in enumerate(sorted_list):
        if "open" not in c or c.get("open") is None:
            if prev_close is not None:
                c["open"] = prev_close
                # mark that open was backfilled so detect_flip can treat it as synthetic
                c["_open_backfilled"] = True
            else:
                # if there's no prev_close, set open equal to close as fallback and mark backfilled
                c["open"] = c.get("close")
                c["_open_backfilled"] = True
        else:
            c["_open_backfilled"] = False
        prev_close = c.get("close")
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
    Try multiple KLINE endpoints until we can obtain open+close candles.
    Normalizes results to dicts with keys: start (seconds), end (seconds or None), open (float), close (float)
    """
    params = {"category": "linear", "symbol": symbol, "interval": str(interval_token), "limit": limit}
    # Try endpoints in order; prefer first but fallback if open missing in returned items
    for ep in KLINE_ENDPOINTS:
        resp = await resilient_public_get([ep], params=params)
        if not resp:
            continue
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
                open_val = None
                if isinstance(it, dict):
                    # Various key possibilities
                    start = it.get("start") or it.get("t") or it.get("open_time")
                    end = it.get("end") or it.get("close_time") or it.get("close_time_ms")
                    close_val = it.get("close") or it.get("c") or it.get("closePrice") or it.get("close_price")
                    open_val = it.get("open") or it.get("o") or it.get("openPrice") or it.get("open_price")
                elif isinstance(it, (list, tuple)):
                    # common arrays: [start, open, high, low, close, ...]
                    if len(it) >= 5:
                        start = it[0]
                        open_val = it[1]
                        close_val = it[4]
                    else:
                        continue
                else:
                    continue
                if start is None or close_val is None:
                    continue
                start_int = int(float(start))
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
                open_f = None
                if open_val is not None:
                    try:
                        open_f = float(open_val)
                    except Exception:
                        open_f = None
                candle = {"start": start_int, "end": end_int, "close": close}
                if open_f is not None:
                    candle["open"] = open_f
                candles.append(candle)
            except Exception:
                continue
        # If we obtained candles, return them (merge will backfill opens if necessary)
        if candles:
            candles.sort(key=lambda c: c["start"])
            return candles
    # nothing found
    return []

# ---------- Interval helpers ----------
def interval_seconds_from_token(token: str) -> int:
    if token == "5": return 300
    if token == "15": return 900
    if token == "60": return 3600
    if token == "240": return 3600*4
    if token.upper() == "D" or token == "1D": return 86400
    try:
        m = int(token)
        return m * 60
    except Exception:
        return 300

def last_candle_is_closed(dq: deque, token: str, safety_seconds: int = 3) -> bool:
    if not dq:
        return False
    last = list(dq)[-1]
    start = last["start"]
    interval_seconds = interval_seconds_from_token(token)
    end = start + interval_seconds
    now_s = int(time.time())
    return now_s >= (end + safety_seconds)

def candles_to_closes(dq: deque, last_n: Optional[int] = None) -> List[float]:
    arr = list(dq)
    if last_n:
        arr = arr[-last_n:]
    return [x["close"] for x in arr if x.get("close") is not None]

def candles_to_opens(dq: deque, last_n: Optional[int] = None) -> List[Optional[float]]:
    arr = list(dq)
    if last_n:
        arr = arr[-last_n:]
    return [x.get("open") for x in arr]

# ---------- Detect flip: OPEN ONLY (strict) ----------
async def detect_flip(symbol: str, tf: str) -> Tuple[Optional[str], Optional[int]]:
    """
    Strict detection of "open of first green after previous closed red":
      - previous closed candle must be red (prev_close < prev_open) when real opens available
      - previous CLOSED MACD histogram < 0
      - current in-progress MACD histogram > 0
      - in-progress candle must be green (inprog_open < inprog_close) when real opens available
    Only triggers on the in-progress candle (i.e., flip-on-open).
    Special handling: if opens were backfilled (synthetic), treat them as missing and use fallback comparison
    (inprog_close > prev_closed_close) together with strict MACD transition.
    """
    token = TF_MAP[tf]
    await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
    dq = cache_get(symbol, token)
    if not dq or len(dq) < MIN_CANDLES_REQUIRED:
        return None, None

    # if last candle is closed, skip (we only act on open flips)
    if last_candle_is_closed(dq, token, safety_seconds=3):
        return None, None

    closes = candles_to_closes(dq)
    if len(closes) < MACD_SLOW + MACD_SIGNAL + 1:
        return None, None

    closed_closes = closes[:-1]
    inprog_close = closes[-1]

    opens = candles_to_opens(dq)
    prev_closed_open = opens[-2] if len(opens) >= 2 else None
    inprog_open = opens[-1] if len(opens) >= 1 else None

    if len(closed_closes) < MACD_SLOW + MACD_SIGNAL:
        return None, None

    hist_closed = macd_hist(closed_closes)
    hist_with_inprog = macd_hist(closed_closes + [inprog_close])
    if not hist_closed or not hist_with_inprog:
        return None, None

    prev = hist_closed[-1]
    cur = hist_with_inprog[-1]
    if prev is None or cur is None:
        return None, None

    # strict MACD transition
    if not (prev < 0 and cur > 0):
        if DETECT_DEBUG or symbol.upper() in DETECT_DEBUG_SYMBOLS:
            log("detect_flip debug (MACD transition failed):", symbol, tf, "prev_hist", prev, "cur_hist", cur)
        return None, None

    # read prev closed close value
    try:
        prev_closed_close = list(dq)[-2]["close"]
    except Exception:
        prev_closed_close = None

    # Determine whether opens are synthetic (backfilled)
    prev_open_backfilled = False
    inprog_open_backfilled = False
    try:
        prev_entry = list(dq)[-2]
        prev_open_backfilled = bool(prev_entry.get("_open_backfilled"))
    except Exception:
        prev_open_backfilled = False
    try:
        inprog_entry = list(dq)[-1]
        inprog_open_backfilled = bool(inprog_entry.get("_open_backfilled"))
    except Exception:
        inprog_open_backfilled = False

    # If opens are present and NOT backfilled, use strict open-based color checks.
    if prev_closed_open is not None and inprog_open is not None and (not prev_open_backfilled) and (not inprog_open_backfilled):
        # previous must be red and in-progress must be green
        if prev_closed_close is None:
            if DETECT_DEBUG or symbol.upper() in DETECT_DEBUG_SYMBOLS:
                log("detect_flip debug: missing prev_closed_close", symbol, tf)
            return None, None
        if not (prev_closed_close < prev_closed_open):
            if DETECT_DEBUG or symbol.upper() in DETECT_DEBUG_SYMBOLS:
                log("detect_flip debug (prev not red):", symbol, tf, "prev_close", prev_closed_close, "prev_open", prev_closed_open)
            return None, None
        if not (inprog_close > inprog_open):
            if DETECT_DEBUG or symbol.upper() in DETECT_DEBUG_SYMBOLS:
                log("detect_flip debug (inprog not green):", symbol, tf, "inprog_close", inprog_close, "inprog_open", inprog_open)
            return None, None
        last_start = list(dq)[-1]["start"]
        if DETECT_DEBUG or symbol.upper() in DETECT_DEBUG_SYMBOLS:
            log("detect_flip debug (accepted strict open-based):", symbol, tf, "prev_hist", prev, "cur_hist", cur, "prev_close", prev_closed_close, "prev_open", prev_closed_open, "inprog_open", inprog_open, "inprog_close", inprog_close)
        return "open", last_start
    else:
        # Fallback when open values are missing or were backfilled:
        # require inprog_close > prev_closed_close (weaker) but still require strict MACD transition which we already checked
        if prev_closed_close is None:
            if DETECT_DEBUG or symbol.upper() in DETECT_DEBUG_SYMBOLS:
                log("detect_flip debug: fallback missing prev_closed_close", symbol, tf)
            return None, None
        if inprog_close > prev_closed_close:
            last_start = list(dq)[-1]["start"]
            if DETECT_DEBUG or symbol.upper() in DETECT_DEBUG_SYMBOLS:
                log("detect_flip debug (accepted fallback close-based):", symbol, tf, "prev_hist", prev, "cur_hist", cur, "prev_close", prev_closed_close, "inprog_close", inprog_close, "prev_open_backfilled", prev_open_backfilled, "inprog_open_backfilled", inprog_open_backfilled)
            return "open", last_start
        else:
            if DETECT_DEBUG or symbol.upper() in DETECT_DEBUG_SYMBOLS:
                log("detect_flip debug (fallback close check failed):", symbol, tf, "prev_close", prev_closed_close, "inprog_close", inprog_close)
            return None, None

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
        for tmpl in CANDIDATE_PUBLIC_TEMPLATES:
            try:
                t1 = tmpl.format(interval=TF_MAP["5m"], symbol=self.detect_symbol)
                t2 = tmpl.format(interval=TF_MAP["15m"], symbol=self.detect_symbol)
                await self.conn.send(json.dumps({"op": "subscribe", "args": [t1, t2]}))
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
                    try:
                        await self.conn.send(json.dumps({"op": "unsubscribe", "args": [t1, t2]}))
                    except Exception:
                        pass
                    continue
            except Exception:
                continue
        if not self._recv_task:
            self._recv_task = asyncio.create_task(self._recv_loop())
        return True

    async def subscribe_kline(self, symbol: str, interval_token: str):
        topic = f"klineV2.{interval_token}.{symbol}"
        if topic in self.subscribed_topics:
            return
        if not self.conn:
            connected = await self._connect()
            if not connected:
                return
        try:
            await self.conn.send(json.dumps({"op": "subscribe", "args": [topic]}))
            self.subscribed_topics.add(topic)
            log("WS subscribed to", topic)
        except Exception as e:
            log("WS subscribe error:", e)

    def _extract_kline_entries(self, obj: Dict[str, Any]):
        out = []
        try:
            data = obj.get("data") or obj.get("result") or []
            topic = obj.get("topic") or ""
            if isinstance(data, dict):
                data = [data]
            for entry in (data or []):
                if not isinstance(entry, dict):
                    continue
                symbol = entry.get("symbol") or entry.get("s") or None
                interval = entry.get("interval") or entry.get("kline", {}).get("interval") or entry.get("i") or None
                if not symbol or not interval:
                    if topic and "kline" in topic:
                        parts = topic.split(".")
                        if len(parts) >= 3:
                            interval = interval or parts[1]
                            symbol = symbol or parts[2]
                confirm = entry.get("confirm") or entry.get("is_closed") or entry.get("isFinal") or False
                kline = entry.get("kline") or {}
                start = entry.get("start") or kline.get("start") or entry.get("t") or None
                end = entry.get("end") or kline.get("end") or entry.get("endTime") or None
                close = entry.get("close") or kline.get("close") or entry.get("c") or None
                open_v = entry.get("open") or kline.get("open") or entry.get("o") or None
                if symbol and interval and start and close is not None:
                    try:
                        start_i = int(float(start))
                        if start_i > 10**12:
                            start_i = start_i // 1000
                        end_i = None
                        if end is not None:
                            end_i = int(float(end))
                            if end_i > 10**12:
                                end_i = end_i // 1000
                        close_f = float(close)
                        candle = {"start": start_i, "end": end_i, "close": close_f}
                        if open_v is not None:
                            try:
                                candle["open"] = float(open_v)
                            except Exception:
                                pass
                        out.append((symbol, str(interval), candle, bool(confirm)))
                    except Exception:
                        continue
        except Exception:
            pass
        return out

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
                    topic = obj.get("topic") or obj.get("arg", {}).get("topic") or ""
                    await persist_raw_ws("public", topic, json.dumps(obj))
                    entries = self._extract_kline_entries(obj)
                    for symbol, interval_token, candle, is_closed in entries:
                        try:
                            merge_into_cache(symbol, str(interval_token), [candle])
                            log("Merged kline from WS into cache for", symbol, interval_token, "start", candle.get("start"), "closed?", is_closed)
                        except Exception:
                            continue
                except Exception:
                    continue
            except Exception as e:
                log("Public WS recv error:", e)
                try:
                    if self.conn:
                        await self.conn.close()
                except Exception:
                    pass
                self.conn = None
                await asyncio.sleep(5)
                try:
                    await self._connect()
                except Exception:
                    await asyncio.sleep(5)

# ---------- Scanning & signal logic ----------
def tf_list_for_root(root_tf: str) -> List[str]:
    if root_tf == "4h":
        return ["5m", "15m", "1h", "1d"]
    if root_tf == "1h":
        return ["5m", "15m", "4h", "1d"]
    return ["5m", "15m", "1h", "1d"]

def signal_exists_for(symbol: str, signal_type: str) -> bool:
    return signal_type in active_signal_index.get(symbol, set())

def register_signal_index(symbol: str, signal_type: str):
    active_signal_index[symbol].add(signal_type)

def unregister_signal_index(symbol: str, signal_type: str):
    if symbol in active_signal_index and signal_type in active_signal_index[symbol]:
        active_signal_index[symbol].remove(signal_type)

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

        if stype == "super":
            comps = sig.get("components") or []
            try:
                comp_times = tuple(sorted(int(c.get("flip_time")) for c in comps if c.get("flip_time") is not None))
            except Exception:
                comp_times = tuple()
            if comp_times and recent_super_signals.get(sym) == comp_times:
                log("Duplicate super signal suppressed by recent_super_signals:", sym, comp_times)
                return False
            for existing in active_root_signals.values():
                if existing.get("symbol") == sym and existing.get("signal_type") == "super":
                    ex_comps = existing.get("components") or []
                    try:
                        ex_times = tuple(sorted(int(c.get("flip_time")) for c in ex_comps if c.get("flip_time") is not None))
                    except Exception:
                        ex_times = tuple()
                    if ex_times and comp_times and ex_times == comp_times:
                        log("Duplicate existing super signal found (inside lock):", sym, comp_times)
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
        if stype == "root" and root_time is not None:
            recent_root_signals[sym] = root_time
        if stype == "super":
            comps = sig.get("components") or []
            try:
                comp_times = tuple(sorted(int(c.get("flip_time")) for c in comps if c.get("flip_time") is not None))
                if comp_times:
                    recent_super_signals[sym] = comp_times
            except Exception:
                pass
        if stype == "entry":
            try:
                recent_entry_signals[sym] = int(time.time())
            except Exception:
                pass
        try:
            await persist_root_signal(sig)
        except Exception as e:
            log("persist_root_signal error:", e)
        log("Added signal:", sid, stype, sym)
        try:
            await send_telegram(f"Added signal: {sid} {stype} {sym}")
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
        rtime = sig.get("root_flip_time") or sig.get("flip_time")
        try:
            if rtime is not None and recent_root_signals.get(sym) == rtime:
                del recent_root_signals[sym]
        except Exception:
            pass
    if stype == "super":
        comps = sig.get("components") or []
        try:
            comp_times = tuple(sorted(int(c.get("flip_time")) for c in comps if c.get("flip_time") is not None))
            if comp_times and recent_super_signals.get(sym) == comp_times:
                del recent_super_signals[sym]
        except Exception:
            pass
    if stype == "entry":
        try:
            if recent_entry_signals.get(sym):
                del recent_entry_signals[sym]
        except Exception:
            pass
    unregister_signal_index(sym, stype)
    try:
        await remove_root_signal(sig_id)
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

# ---------- Root scanning ----------
async def root_scanner_loop(root_tf: str):
    log("Root scanner started for", root_tf)
    while True:
        try:
            symbols = await get_tradable_usdt_symbols()
            if symbols:
                try:
                    await asyncio.gather(*(ensure_cached_candles(symbol, root_tf, MIN_CANDLES_REQUIRED) for symbol in symbols))
                except Exception as e:
                    log("Prewarm error:", e)
            for symbol in symbols:
                if is_stablecoin_symbol(symbol):
                    continue
                if SKIP_DIGIT_PREFIX and _re_leading_digit.match(symbol):
                    continue
                if symbol_too_new(symbol):
                    continue
                try:
                    flip_kind, last_start = await detect_flip(symbol, root_tf)
                    if not flip_kind:
                        continue
                    exists_same = any(s.get("symbol") == symbol and s.get("signal_type") == "root" and s.get("root_flip_time") == last_start for s in active_root_signals.values())
                    if exists_same:
                        continue
                    if signal_exists_for(symbol, "root"):
                        continue
                    dq_root = cache_get(symbol, TF_MAP[root_tf])
                    if not dq_root:
                        continue
                    last_candle = list(dq_root)[-1]
                    key = f"ROOT-{symbol}-{root_tf}-{last_start}"
                    sig = {
                        "id": key,
                        "symbol": symbol,
                        "root_tf": root_tf,
                        "root_flip_time": last_start,
                        "root_flip_price": last_candle.get("close"),
                        "created_at": now_ts_ms(),
                        "status": "watching",
                        "priority": None,
                        "signal_type": "root",
                        "components": [],
                        "flip_kind": flip_kind,
                    }
                    added = await add_signal(sig)
                    if added:
                        log("Root signal created:", key)
                except Exception as e:
                    log("root_scanner_loop error:", e)
        except Exception as e:
            log("root_scanner_loop outer error:", e)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

# ---------- Evaluate existing signals (root/super) for entry/trade ----------
async def evaluate_signals_loop():
    def macd_positive_from_closes(closes: List[float]) -> bool:
        h = macd_hist(closes)
        if not h:
            return False
        last = h[-1]
        return bool(last and last > 0)

    async def has_open_trade(symbol: str) -> bool:
        try:
            async with db.execute("SELECT COUNT(*) FROM trades WHERE symbol = ? AND open = 1", (symbol,)) as cur:
                row = await cur.fetchone()
                return bool(row and row[0] > 0)
        except Exception:
            return False

    while True:
        try:
            ids = list(active_root_signals.keys())
            for sid in ids:
                try:
                    sig = active_root_signals.get(sid)
                    if not sig:
                        continue
                    stype = sig.get("signal_type", "root")
                    symbol = sig.get("symbol")
                    root_tf = sig.get("root_tf")
                    if is_stablecoin_symbol(symbol) or symbol_too_new(symbol):
                        await remove_signal(sid)
                        continue
                    if stype not in ("root", "super"):
                        continue
                    if stype == "root":
                        required_tfs = tf_list_for_root(root_tf)
                    else:
                        required_tfs = ["5m", "15m", "1h", "1d"]
                    await asyncio.gather(*(ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED) for tf in required_tfs))
                    tf_status = {}
                    for tf in required_tfs:
                        token = TF_MAP[tf]
                        dq = cache_get(symbol, token)
                        if not dq or len(dq) < MIN_CANDLES_REQUIRED:
                            tf_status[tf] = {"has": False}
                            continue
                        closed = last_candle_is_closed(dq, token, safety_seconds=3)
                        closes = candles_to_closes(dq)
                        positive = macd_positive_from_closes(closes)
                        flip_kind, last_ts = await detect_flip(symbol, tf)
                        flip_last = True if flip_kind == "open" else False
                        last_ts_val = list(dq)[-1]["start"] if dq else None
                        tf_status[tf] = {"has": True, "closed": closed, "positive": positive, "flip_last": flip_last, "last_ts": last_ts_val, "flip_kind": flip_kind}
                    if root_tf not in tf_status or not tf_status[root_tf].get("has"):
                        continue
                    if not tf_status[root_tf].get("flip_last"):
                        continue
                    other_tfs = [tf for tf in required_tfs if tf != root_tf]
                    negatives = [tf for tf in other_tfs if not tf_status.get(tf, {}).get("positive")]
                    if len(negatives) > 1:
                        continue
                    if len(negatives) == 1:
                        tf_to_flip = negatives[0]
                        st = tf_status.get(tf_to_flip, {})
                        if not st.get("has"):
                            continue
                        if not st.get("flip_last"):
                            continue
                        for tf in other_tfs:
                            if tf == tf_to_flip:
                                continue
                            if not tf_status.get(tf, {}).get("positive"):
                                break
                        else:
                            pass
                    if signal_exists_for(symbol, "entry"):
                        continue
                    if await has_open_trade(symbol):
                        log("Open trade exists for", symbol, "— skipping entry")
                        continue
                    ts = int(time.time())
                    entry_id = f"ENTRY-{symbol}-{ts}"
                    entry_sig = {
                        "id": entry_id,
                        "symbol": symbol,
                        "root_tf": root_tf,
                        "signal_type": "entry",
                        "priority": "entry",
                        "status": "ready",
                        "components": required_tfs,
                        "created_at": now_ts_ms(),
                        "last_flip_ts": now_ts_ms(),
                    }
                    added = await add_signal(entry_sig)
                    if not added:
                        continue
                    lock = get_symbol_lock(symbol)
                    async with lock:
                        if await has_open_trade(symbol):
                            log("Open trade found (post-lock) for", symbol, "— skipping trade placement")
                        else:
                            dq5 = cache_get(symbol, TF_MAP["5m"])
                            last_price = None
                            if dq5 and len(dq5):
                                last_price = list(dq5)[-1]["close"]
                            price = last_price or 1.0
                            balance = 1000.0
                            per_trade = max(1.0, balance / max(1, MAX_OPEN_TRADES))
                            qty = max(0.0001, round(per_trade / price, 6))
                            side = "Buy"
                            stop_price = round(price * (1 - STOP_LOSS_PCT), 8)

                            if TRADING_ENABLED:
                                try:
                                    res = await bybit_signed_request("POST", "/v5/order/create", {"category": "linear", "symbol": symbol, "side": side, "orderType": "Market", "qty": qty})
                                    oid = res.get("result", {}).get("orderId", str(uuid.uuid4()))
                                    await persist_trade({"id": oid, "symbol": symbol, "side": side, "qty": qty, "entry_price": None, "sl_price": stop_price, "created_at": now_ts_ms(), "open": True, "raw": res})
                                    log("Placed real order", symbol, res)
                                    await send_telegram(f"Placed real order {symbol} qty={qty} side={side}")
                                except Exception as e:
                                    log("Error placing real order:", e)
                            else:
                                oid = str(uuid.uuid4())
                                await persist_trade({"id": oid, "symbol": symbol, "side": side, "qty": qty, "entry_price": None, "sl_price": stop_price, "created_at": now_ts_ms(), "open": True, "raw": {"simulated": True}})
                                log("Simulated trade for", symbol, "qty", qty)
                                await send_telegram(f"Simulated trade for {symbol} qty={qty} side={side} (entry)")
                        if entry_id in active_root_signals:
                            active_root_signals[entry_id]["status"] = "acted"
                            try:
                                await persist_root_signal(active_root_signals[entry_id])
                            except Exception:
                                pass
                except Exception as e:
                    log("evaluate_signals_loop error for", sid, e)
            await asyncio.sleep(max(5, SCAN_INTERVAL_SECONDS // 2))
        except Exception as e:
            log("evaluate_signals_loop outer error:", e)
            await asyncio.sleep(5)

# ---------- Super-signal detection during frequent 5m checks ----------
async def super_signal_scanner_loop_5m():
    while True:
        try:
            symbols = await get_tradable_usdt_symbols()
            for symbol in symbols:
                if is_stablecoin_symbol(symbol) or (SKIP_DIGIT_PREFIX and _re_leading_digit.match(symbol)) or symbol_too_new(symbol):
                    continue
                flips = []
                for tf in ["1h", "4h", "1d"]:
                    try:
                        flip_kind, last_ts = await detect_flip(symbol, tf)
                        if flip_kind == "open" and last_ts:
                            flips.append((tf, last_ts))
                    except Exception:
                        continue
                if len(flips) >= 2:
                    times = [t for _, t in flips]
                    if max(times) - min(times) <= SUPER_WINDOW_SECONDS:
                        if signal_exists_for(symbol, "super"):
                            continue
                        comp_times_sorted = tuple(sorted(times))
                        existing = recent_super_signals.get(symbol)
                        if existing == comp_times_sorted:
                            continue
                        root_time = max(times)
                        key = f"SUPER-{symbol}-{root_time}"
                        components = [{"tf": tf, "flip_time": t} for tf, t in flips]
                        sig = {
                            "id": key,
                            "symbol": symbol,
                            "root_tf": "multi",
                            "root_flip_time": root_time,
                            "root_flip_price": None,
                            "created_at": now_ts_ms(),
                            "status": "watching",
                            "priority": "super",
                            "signal_type": "super",
                            "components": components,
                        }
                        added = await add_signal(sig)
                        if added:
                            try:
                                recent_super_signals[symbol] = comp_times_sorted
                            except Exception:
                                pass
                            log("SUPER signal detected for", symbol, "components:", sig["components"])
        except Exception as e:
            log("super_signal_scanner_loop_5m error:", e)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

# ---------- Signal expiration ----------
async def expire_signals_loop():
    while True:
        try:
            now_s = int(time.time())
            to_remove = []
            for sid, sig in list(active_root_signals.items()):
                try:
                    stype = sig.get("signal_type", "root")
                    if stype == "root":
                        root_tf = sig.get("root_tf")
                        flip_time = sig.get("root_flip_time") or sig.get("flip_time")
                        if root_tf and flip_time:
                            token = TF_MAP.get(root_tf)
                            if token:
                                expiry = flip_time + interval_seconds_from_token(token)
                                if now_s >= expiry:
                                    to_remove.append(sid)
                    elif stype == "super":
                        flip_time = sig.get("root_flip_time")
                        if flip_time and now_s >= (flip_time + SUPER_WINDOW_SECONDS):
                            to_remove.append(sid)
                except Exception:
                    continue
            for sid in to_remove:
                try:
                    log("Expiring signal:", sid)
                    await remove_signal(sid)
                except Exception as e:
                    log("Error expiring signal", sid, e)
        except Exception as e:
            log("expire_signals_loop error:", e)
        await asyncio.sleep(max(5, SCAN_INTERVAL_SECONDS // 2))

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
            stype = sig.get("signal_type", "root")
            created = sig.get("created_at")
            comps = sig.get("components", "")
            flip_kind = sig.get("flip_kind", "")
            log(f" {idx}. id={key} symbol={symbol} type={stype} root_tf={root_tf} status={status} priority={priority} components={comps} flip_kind={flip_kind} created_at={created}")
    except Exception as e:
        log("log_current_root_signals error:", e)

async def send_root_signals_telegram():
    try:
        if not active_root_signals:
            await send_telegram("Startup: no active signals")
            return
        lines = ["Active signals summary:"]
        for sig in active_root_signals.values():
            stype = sig.get("signal_type", "root")
            symbol = sig.get("symbol")
            root_tf = sig.get("root_tf")
            flip_time = sig.get("root_flip_time") or sig.get("flip_time")
            created = sig.get("created_at")
            comps = sig.get("components", "")
            lines.append(f"- {stype.upper()}: {symbol} tf={root_tf} flip_time={flip_time} created={created} comps={comps}")
        text = "\n".join(lines)
        await send_telegram(text)
    except Exception as e:
        log("send_root_signals_telegram error:", e)

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
                    elif signal_type == "super":
                        if flip_time and int(time.time()) >= (flip_time + SUPER_WINDOW_SECONDS):
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

# ---------- Debug endpoints ----------
@app.get("/debug/current_roots")
async def debug_current_roots(_auth=Depends(require_admin_auth)):
    return {"count": len(active_root_signals), "signals": list(active_root_signals.values()), "recent_root_signals": recent_root_signals, "recent_super_signals": recent_super_signals, "recent_entry_signals": recent_entry_signals}

@app.get("/debug/check_symbol")
async def debug_check_symbol(symbol: str = Query(..., min_length=1), tf: str = Query("1h"), _auth=Depends(require_admin_auth)):
    if tf not in TF_MAP:
        raise HTTPException(status_code=400, detail=f"Unknown timeframe {tf}. Valid: {list(TF_MAP.keys())}")
    token = TF_MAP[tf]
    await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
    dq = cache_get(symbol, token)
    if not dq:
        return {"symbol": symbol, "tf": tf, "klines_count": 0, "error": "no cached klines"}
    closes = candles_to_closes(dq)
    opens = candles_to_opens(dq)
    hist = macd_hist(closes)
    flip_kind, flip_ts = await detect_flip(symbol, tf)
    closed = last_candle_is_closed(dq, token)
    sample = list(dq)[-10:]
    return {
        "symbol": symbol,
        "tf": tf,
        "cached_klines": len(dq),
        "macd_hist_len": len(hist),
        "macd_last": hist[-1] if hist else None,
        "flip_kind": flip_kind,
        "flip_ts": flip_ts,
        "last_closed": closed,
        "sample_last_klines": sample,
        "sample_opens": opens[-10:],
        "sample_closes": closes[-10:],
    }

@app.post("/admin/send_root_summary")
async def admin_send_root_summary(_auth=Depends(require_admin_auth)):
    await send_root_signals_telegram()
    return {"status": "ok", "sent": True}

@app.get("/")
async def root():
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

# ---------- Startup & background tasks ----------
@app.on_event("startup")
async def startup():
    global public_ws, private_ws, _TELEGRAM_WORKER_TASK
    await init_db()
    await load_persisted_root_signals()
    # Start telegram worker early
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and (_TELEGRAM_WORKER_TASK is None or _TELEGRAM_WORKER_TASK.done()):
        try:
            _TELEGRAM_WORKER_TASK = asyncio.create_task(_telegram_worker())
        except Exception:
            pass
    # Notify current persisted signals on deploy
    try:
        await send_root_signals_telegram()
    except Exception as e:
        log("Startup telegram send failed:", e)
    log("Startup config:", "BYBIT_USE_MAINNET=", BYBIT_USE_MAINNET, "TRADING_ENABLED=", TRADING_ENABLED, "PUBLIC_WS_URL=", PUBLIC_WS_URL, "MIN_CANDLES_REQUIRED=", MIN_CANDLES_REQUIRED)
    public_ws = PublicWebsocketManager(PUBLIC_WS_URL)
    try:
        ok = await public_ws.connect_and_detect(timeout=6.0)
        if not ok:
            log("Public WS detect warning")
    except Exception:
        log("Public WS connect error")
    # Start background tasks
    asyncio.create_task(root_scanner_loop("1h"))
    asyncio.create_task(root_scanner_loop("4h"))
    asyncio.create_task(super_signal_scanner_loop_5m())
    asyncio.create_task(evaluate_signals_loop())
    asyncio.create_task(expire_signals_loop())
    async def periodic_root_logger():
        while True:
            try:
                await log_current_root_signals()
            except Exception as e:
                log("periodic_root_logger error:", e)
            await asyncio.sleep(max(30, ROOT_SIGNALS_LOG_INTERVAL))
    asyncio.create_task(periodic_root_logger())
    log("Background tasks started")

# ---------- Run note ----------
# Start with:
# uvicorn main:app --host 0.0.0.0 --port 8000
