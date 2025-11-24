# main.py
# Bybit MACD multi-timeframe scanner — enhanced, flip-on-open ONLY
# - Detects MACD histogram flip only when the in-progress (open) candle's histogram turns > 0
# - Closed-candle flips are ignored entirely (no "closed" flip_kind)
# - Super signals: require 2+ flips among 1h/4h/1d within SUPER_WINDOW_SECONDS; flips must be open flips
# - Entry/trade: root + last-to-flip rules applied; flip must be observed on in-progress candle (open)
#
# Run: uvicorn main:app --host 0.0.0.0 --port 8000

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
from typing import Dict, Any, List, Optional, Tuple
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

# concurrency for public API requests (to avoid huge parallel bursts)
PUBLIC_REQ_CONCURRENCY = int(os.getenv("PUBLIC_REQ_CONCURRENCY", "8"))
# number of retries for public requests
PUBLIC_REQ_RETRIES = int(os.getenv("PUBLIC_REQ_RETRIES", "3"))

# Discovery concurrency: how many symbols to detect flips for in parallel
DISCOVERY_CONCURRENCY = int(os.getenv("DISCOVERY_CONCURRENCY", "24"))

# Dedup / discovery tuning for root signals
# How long to suppress re-creating a root signal for the same symbol (seconds)
ROOT_DEDUP_SECONDS = int(os.getenv("ROOT_DEDUP_SECONDS", "300"))

# Flip stability handling:
# If >0, require that a detected open flip is observed for at least FLIP_STABILITY_SECONDS
# inside the same in-progress candle before creating a root signal.
# Set to 0 to accept flips immediately (max discovery).
FLIP_STABILITY_SECONDS = int(os.getenv("FLIP_STABILITY_SECONDS", "0"))

# Telegram queue settings
TELEGRAM_WORKER_CONCURRENCY = 1
TELEGRAM_RETRY_LIMIT = 4

# candle cache sizing and behavior
CANDLE_CACHE_MAX = int(os.getenv("CANDLE_CACHE_MAX", "2000"))     # keep up to N candles per symbol/tf
CANDLE_CACHE_TTL = int(os.getenv("CANDLE_CACHE_TTL", "300"))      # seconds TTL for cache refresh per symbol/tf

# skip tokens beginning with digits (spam tokens like 1000000FOO)
# Default changed to False to maximise discovery. Set to "true" if you want original behavior.
SKIP_DIGIT_PREFIX = os.getenv("SKIP_DIGIT_PREFIX", "false").lower() == "true"

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
# Reverse mapping for quick token->tf lookup (used by WS in-progress updates)
REVERSE_TF_MAP = {v: k for k, v in TF_MAP.items()}

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
# Default set to 0 to maximise discovery. Increase if you want to filter newly-listed tokens.
MIN_SYMBOL_AGE_MONTHS = int(os.getenv("MIN_SYMBOL_AGE_MONTHS", "0"))

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

# root signals stored in-memory; persisted to DB in root_signals table
active_root_signals: Dict[str, Dict[str, Any]] = {}
# helper index: active signals per symbol -> set of types (root,super,entry)
active_signal_index: Dict[str, set] = defaultdict(set)

# recent_root_signals: additional dedupe guard mapping symbol -> most recent root_flip_time we've created (seconds)
recent_root_signals: Dict[str, int] = {}

# recent_super_signals: dedupe map for super signals mapping symbol -> last_super_ts
recent_super_signals: Dict[str, int] = {}

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
    # create lock lazily
    lock = symbol_locks.get(symbol)
    if lock is None:
        lock = asyncio.Lock()
        symbol_locks[symbol] = lock
    return lock

# registry for observed in-progress flips to support stability checks
# key = (symbol, token, candle_start) -> {"first_seen": epoch_seconds, "last_seen": epoch_seconds, "count": int}
observed_flip_registry: Dict[Tuple[str, str, int], Dict[str, int]] = {}

# ---------- Utilities ----------
def log(*args, **kwargs):
    if LOG_LEVEL != "none":
        ts = datetime.now(timezone.utc).isoformat()
        print(ts, *args, **kwargs)

def now_ts_ms() -> int:
    return int(time.time() * 1000)

async def send_root_signals_telegram():
    """Send a concise Telegram message listing current root signals and TFs."""
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

# ---------------- Telegram worker & queue ----------------
async def _telegram_worker():
    """
    Background worker that pulls messages from TELEGRAM_QUEUE and sends them,
    honoring Telegram rate limits (Retry-After header / body).
    Each queue item: (text, attempt_count)
    """
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
                # send synchronously (we keep small concurrency by running single worker)
                r = await httpx_client.post(url, json=payload, timeout=10)
                if r.status_code == 200:
                    try:
                        data = r.json()
                    except Exception:
                        data = {"raw_text": (r.text[:400] + "...") if r.text else ""}
                    log("Telegram sent ok (worker) response:", data)
                elif r.status_code == 429:
                    # rate limited -> parse retry_after
                    retry_after = 5
                    try:
                        data = r.json()
                        retry_after = int(data.get("parameters", {}).get("retry_after", retry_after))
                    except Exception:
                        # maybe header
                        try:
                            retry_after = int(r.headers.get("Retry-After", retry_after))
                        except Exception:
                            pass
                    log("Telegram send failed: 429 rate limit, retry after", retry_after)
                    # requeue after sleeping
                    await asyncio.sleep(retry_after)
                    if attempt + 1 < TELEGRAM_RETRY_LIMIT:
                        await TELEGRAM_QUEUE.put((text, attempt + 1))
                    else:
                        log("Telegram send retry limit reached, dropping message")
                else:
                    log("Telegram send failed:", r.status_code, r.text)
                    # exponential backoff retry
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
    """
    Enqueue Telegram message for background worker to send, to honor rate-limits and avoid blocking.
    If TELEGRAM_BOT_TOKEN/CHAT_ID not set, just log.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram not configured; skipping message:", text)
        return
    # ensure worker started
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
    """
    Resilient GET that:
      - Limits concurrency via PUBLIC_REQUEST_SEMAPHORE
      - Retries on network errors and HTTP 429 with exponential backoff and honoring Retry-After
      - Rotates through API_HOSTS and endpoints
    """
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
                    # handle 429 specially
                    if r.status_code == 429:
                        retry_after = 5
                        try:
                            # prefer Retry-After header
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
        # exponential backoff before next big retry
        backoff = min(60, 2 ** attempt)
        await asyncio.sleep(backoff)
    # after retries, return None
    if last_exc:
        log("resilient_public_get exhausted retries; last exception:", last_exc)
    return None

# ---------- Signed request ----------
def bybit_sign_v5(api_secret: str, timestamp: str, method: str, path: str, body: str) -> str:
    prehash = timestamp + method.upper() + path + (body or "")
    return hmac.new(api_secret.encode(), prehash.encode(), hashlib.sha256).hexdigest()

async def bybit_signed_request(method: str, endpoint: str, payload: Dict[str, Any] = None):
    """
    Sends signed requests to Bybit with simple retry/backoff and 429 handling.
    """
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
    # Defensive DB-level uniqueness to avoid duplicates across processes:
    # unique on (symbol, root_tf, flip_time)
    await db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_root_signals_symbol_tf_flip ON root_signals(symbol, root_tf, flip_time)""")
    await db.commit()
    log("DB initialized at", DB_PATH)

async def persist_root_signal(sig: Dict[str, Any]):
    """
    Persist root signal defensively: use INSERT OR IGNORE to avoid DB errors if a duplicate slips through.
    We still keep in-memory registration in add_signal.
    """
    comps = json.dumps(sig.get("components") or [])
    try:
        # Attempt to insert; if duplicate (by unique index) the insert will be ignored.
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

# ---------- Helper functions (robust instruments fetch) ----------
async def get_tradable_usdt_symbols() -> List[str]:
    """
    Return list of symbols that are linear PERPETUAL and quote USDT and status Trading.
    To maximise discovery this function no longer aggressively filters:
      - SKIP_DIGIT_PREFIX is now disabled by default
      - MIN_SYMBOL_AGE_MONTHS default is 0 (no age-based filter)
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
                # optionally skip digit-prefixed tokens if configured
                if SKIP_DIGIT_PREFIX and _re_leading_digit.match(symbol):
                    continue
                # skip explicit non-trading status
                if status and isinstance(status, str) and status not in ("trading", "listed", "normal", "active", "open"):
                    continue
                # skip symbols younger than MIN_SYMBOL_AGE_MONTHS if launch_ts available and threshold > 0
                if MIN_SYMBOL_AGE_MONTHS > 0 and launch_ts:
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
    # use resilient_public_get which acquires the semaphore internally
    klines = await fetch_klines(symbol, interval_token, limit=limit)
    if not klines:
        return
    merge_into_cache(symbol, interval_token, klines)

# ---------- Fetch and normalize klines ----------
async def fetch_klines(symbol: str, interval_token: str, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Fetch klines via v5 kline endpoint. interval_token should be the token used in TF_MAP ('5','15','60', etc).
    Returns list of candles as dicts with keys: start (seconds), end (seconds or None), close (float)
    Note: Bybit kline endpoint usually returns the in-progress candle as the last item with its latest close (not confirmed).
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

    async def subscribe_kline(self, symbol: str, interval_token: str):
        """
        Subscribe to kline messages for symbol/interval_token (interval_token is numeric token like '5' or '15').
        Keeps a simple registry so we don't resubscribe repeatedly.
        """
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

    def _maybe_decompress(self, msg):
        try:
            if isinstance(msg, bytes):
                try:
                    return gzip.decompress(msg).decode()
                except Exception:
                    try:
                        return zlib.decompress(msg, -zlib.MAX_WBITS).decode()
                    except Exception:
                        return msg.decode()
            return msg if isinstance(msg, str) else None
        except Exception:
            return None

    def _extract_kline_entries(self, obj: Dict[str, Any]):
        """
        Extracts possible kline entries from a websocket message object.
        Returns list of tuples: (symbol, interval_token, candle_dict, is_closed_bool)
        """
        out = []
        try:
            # Bybit v5 often provides 'topic' and 'data' array of dicts.
            data = obj.get("data") or obj.get("result") or []
            topic = obj.get("topic") or ""
            # If data is a dict with 'kline' key or direct fields
            if isinstance(data, dict):
                data = [data]
            for entry in (data or []):
                # entry might be e.g. {"symbol":"BTCUSDT","interval":"5","start":"...","end":"...","close":"...","confirm":True}
                if not isinstance(entry, dict):
                    continue
                symbol = entry.get("symbol") or entry.get("s") or None
                interval = entry.get("interval") or entry.get("kline", {}).get("interval") or entry.get("i") or None
                # fallback: parse topic like klineV2.5.BTCUSDT
                if not symbol or not interval:
                    if topic and "kline" in topic:
                        parts = topic.split(".")
                        if len(parts) >= 3:
                            interval = interval or parts[1]
                            symbol = symbol or parts[2]
                # determine closed/confirmed
                confirm = entry.get("confirm") or entry.get("is_closed") or entry.get("isFinal") or entry.get("isFinal") or False
                # sometimes 'kline' nested
                kline = entry.get("kline") or {}
                start = entry.get("start") or kline.get("start") or entry.get("t") or None
                end = entry.get("end") or kline.get("end") or entry.get("endTime") or None
                close = entry.get("close") or kline.get("close") or entry.get("c") or None
                # normalize types
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
                        out.append((symbol, str(interval), {"start": start_i, "end": end_i, "close": close_f}, bool(confirm)))
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
                    # try to extract any kline entries and merge closed candles into cache
                    entries = self._extract_kline_entries(obj)
                    for symbol, interval_token, candle, is_closed in entries:
                        try:
                            # merge the kline into cache; if it's not confirmed (is_closed False), it's the in-progress candle
                            merge_into_cache(symbol, str(interval_token), [candle])
                            log("Merged kline from WS into cache for", symbol, interval_token, "start", candle.get("start"), "closed?", is_closed)
                            # If this is an in-progress update (not closed), schedule immediate mid-candle detection.
                            if not is_closed:
                                # schedule but don't await: mid-candle watcher will check stability & dedupe
                                asyncio.create_task(process_inprogress_update(symbol, str(interval_token)))
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

# persistence helper for raw ws messages
async def persist_raw_ws(source: str, topic: str, message: str):
    try:
        if not db:
            return
        await db.execute("INSERT INTO raw_ws_messages (source, topic, message, created_at) VALUES (?,?,?,?)", (source, topic, message, now_ts_ms()))
        # keep DB writes small; commit occasionally or rely on sqlite's default behavior
        await db.commit()
    except Exception:
        pass

# ---------- Helper: interval seconds & last-candle-closed ----------
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

def signal_exists_for(symbol: str, signal_type: str) -> bool:
    return signal_type in active_signal_index.get(symbol, set())

def register_signal_index(symbol: str, signal_type: str):
    active_signal_index[symbol].add(signal_type)

def unregister_signal_index(symbol: str, signal_type: str):
    if symbol in active_signal_index and signal_type in active_signal_index[symbol]:
        active_signal_index[symbol].remove(signal_type)

async def add_signal(sig: Dict[str, Any]) -> bool:
    """
    Add signal with duplication guard:
      - If a signal of same type already active for that symbol -> skip
      - Persist to DB and register in-memory otherwise
    Uses a per-symbol lock to avoid race conditions that create duplicate signals.
    Additional robust guards:
      - recent_root_signals stores latest created root flip time per symbol to suppress repeated inserts
      - inside-lock re-check looks for any active signal with same symbol+type+root_flip_time
      - For root signals: validate that the flip_time corresponds to the current in-progress root candle (reject old/past flips)
    """
    sid = sig["id"]
    stype = sig.get("signal_type", "root")
    sym = sig["symbol"]
    # root_flip_time if provided (for dedupe)
    root_time = sig.get("root_flip_time") or sig.get("flip_time")
    lock = get_symbol_lock(sym)
    async with lock:
        # Guard: if another active signal of this type exists for the symbol, skip
        if signal_exists_for(sym, stype):
            log("Duplicate signal suppressed (inside lock, by active_signal_index):", sym, stype)
            return False

        # Strong guard for root signals: avoid creating two root signals with same root flip time
        if stype == "root" and root_time is not None:
            # if we've recently created the same root flip for this symbol, suppress
            existing_recent = recent_root_signals.get(sym)
            if existing_recent == root_time:
                log("Duplicate root signal suppressed by recent_root_signals:", sym, root_time)
                return False
            # also check existing active_root_signals entries for same symbol/type/root_flip_time
            for existing in active_root_signals.values():
                if existing.get("symbol") == sym and existing.get("signal_type", "root") == stype:
                    if existing.get("root_flip_time") == root_time:
                        log("Duplicate existing signal found (inside lock):", sym, stype, root_time)
                        return False

            # Validate that the root signal corresponds to CURRENT in-progress root candle
            # (reject historically-created root signals that don't match a live in-progress candle)
            try:
                root_tf = sig.get("root_tf")
                token = TF_MAP.get(root_tf) if root_tf in TF_MAP else root_tf
                if token:
                    # ensure cache is populated and fresh
                    await ensure_cached_candles(sym, root_tf, MIN_CANDLES_REQUIRED)
                    dq = cache_get(sym, token)
                    if not dq or len(dq) == 0:
                        log("add_signal: cannot validate root signal, missing cache for", sym, root_tf)
                        return False
                    last_start = list(dq)[-1]["start"]
                    # if last candle is closed we do NOT accept a root signal here (we only want open flips)
                    if last_candle_is_closed(dq, token, safety_seconds=3):
                        log("add_signal: last candle is closed for", sym, root_tf, "rejecting root signal with flip_time", root_time)
                        return False
                    if last_start != root_time:
                        log("add_signal: root_flip_time does not match current open candle for", sym, root_tf, "expected", last_start, "got", root_time)
                        return False
            except Exception as e:
                log("add_signal validation error:", e)
                return False

        # Final guard: if a signal with same id already present, skip
        if sid in active_root_signals:
            log("Signal id already present, skipping:", sid)
            return False

        # All clear — register in memory and persist
        active_root_signals[sid] = sig
        register_signal_index(sym, stype)
        # For root signals record recent flip time to prevent immediate duplicates from other coroutines/tasks
        if stype == "root" and root_time is not None:
            recent_root_signals[sym] = root_time
        # For super signals record recent super time to prevent duplicates during the window
        if stype == "super":
            try:
                recent_super_signals[sym] = int(time.time())
            except Exception:
                pass
        try:
            await persist_root_signal(sig)
        except Exception as e:
            log("persist_root_signal error:", e)
        log("Added signal:", sid, stype, sym)
        # send per-signal update once per addition
        try:
            await send_telegram(f"Added signal: {sid} {stype} {sym} components={sig.get('components')}")
        except Exception:
            pass

        # Immediately schedule an evaluation for this symbol so entries are checked promptly.
        # This reduces missed confirmations when the alignment already exists at the time of root creation.
        try:
            if stype in ("root", "super"):
                asyncio.create_task(evaluate_signal_once(sym, sid))
        except Exception:
            pass
        return True

async def remove_signal(sig_id: str):
    sig = active_root_signals.pop(sig_id, None)
    if not sig:
        return
    stype = sig.get("signal_type", "root")
    sym = sig.get("symbol")
    # If removing a root signal, clear recent_root_signals entry if it matches this flip time.
    if stype == "root":
        rtime = sig.get("root_flip_time") or sig.get("flip_time")
        try:
            if rtime is not None and recent_root_signals.get(sym) == rtime:
                # remove mapping to allow future same-flip-time signals after explicit removal
                del recent_root_signals[sym]
        except Exception:
            pass

    # If removing super, remove recent_super_signals entry if appropriate
    if stype == "super":
        try:
            if recent_super_signals.get(sym):
                del recent_super_signals[sym]
        except Exception:
            pass

    unregister_signal_index(sym, stype)
    try:
        await remove_root_signal(sig_id)
    except Exception:
        pass
    log("Removed signal:", sig_id)

async def has_open_trade(symbol: str) -> bool:
    if not db:
        return False
    try:
        async with db.execute("SELECT COUNT(*) FROM trades WHERE open = 1 AND symbol = ?", (symbol,)) as cur:
            row = await cur.fetchone()
            if row and row[0] > 0:
                return True
    except Exception:
        pass
    return False

# ---------- ensure_cached_candles (unchanged behaviour but accept tf or token) ----------
async def ensure_cached_candles(symbol: str, tf: str, required: int):
    """
    Ensure the candles_cache for (symbol, tf) contains at least `required` candles,
    fetching and merging as needed.

    Accepts tf either as TF name ('1h') or token ('60').
    """
    # allow passing tf as token or tf-name
    token = TF_MAP.get(tf, tf)
    # if a tf-name was passed but not present in TF_MAP, token will equal tf which might be invalid; we still try
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

# ---------- Detect flip: OPEN ONLY (ignore closed flips) ----------
async def detect_flip(symbol: str, tf: str) -> Tuple[Optional[str], Optional[int]]:
    """
    Detects flip and returns:
      - ("open", last_start) if the most recent candle is IN-PROGRESS (not closed) and
        its current histogram moved from prev_closed_hist <= 0 to current_in_progress_hist > 0.
      - (None, None) otherwise.

    This function now records observed open flips into observed_flip_registry so
    mid-candle repeated updates (WS or polling) can be used to decide stability.
    Accepts tf either as TF name ('1h') or token ('60').
    """
    token = TF_MAP.get(tf, tf)
    # ensure cached candles
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
    # If last candle is closed -> ignore (we only want open flips)
    if last_candle_is_closed(dq, token, safety_seconds=3):
        # cleanup any registry entries for this candle (it's closed)
        observed_flip_registry.pop((symbol, token, last_start), None)
        return None, None
    # last candle is in-progress -> consider flip-on-open only
    prev = hist[-2] or 0
    cur = hist[-1] or 0
    if prev <= 0 and cur > 0:
        # record first seen time for this candle flip
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
    """
    Decide whether a flip observed on (symbol, tf, start) has been stable long enough.
    If FLIP_STABILITY_SECONDS == 0 -> always True.
    """
    if FLIP_STABILITY_SECONDS <= 0:
        return True
    token = TF_MAP.get(tf, tf)
    key = (symbol, token, start)
    rec = observed_flip_registry.get(key)
    if not rec:
        return False
    # stable if first_seen was at least FLIP_STABILITY_SECONDS ago
    return (int(time.time()) - int(rec["first_seen"])) >= FLIP_STABILITY_SECONDS

# ---------- Mid-candle in-progress update processor ----------
async def process_inprogress_update(symbol: str, interval_token: str):
    """
    Called whenever a websocket in-progress kline update is merged into cache.
    If the kline token corresponds to a root timeframe (1h/4h),
    attempt immediate detect_flip and create root signals if conditions met and flip stability + dedupe pass.

    NOTE: Super signal creation has been centralized to the 5m super scanner to avoid duplication.
    """
    try:
        # Map token -> tf name, if unknown skip
        tf = REVERSE_TF_MAP.get(interval_token)
        if not tf:
            return
        # Only consider root-relevant frames for immediate root creation (1h,4h)
        if tf not in ("1h", "4h"):
            return

        # Dedup guard: if recently created root for symbol skip immediate reprocessing
        now_s = int(time.time())
        recent = recent_root_signals.get(symbol)
        if recent and (now_s - recent) < ROOT_DEDUP_SECONDS:
            log("WS mid-candle: skipped (recent root dedup) for", symbol, "token", interval_token, f"age_s={now_s-recent}")
            return

        # If symbol already has active root/entry we skip but log
        if signal_exists_for(symbol, "root"):
            log("WS mid-candle: skipped (already has active root) for", symbol)
            return
        if signal_exists_for(symbol, "entry"):
            log("WS mid-candle: skipped (already has active entry) for", symbol)
            return

        # Attempt detect_flip on this tf
        flip_kind, last_start = await detect_flip(symbol, tf)
        if not flip_kind:
            return

        # Require flip stability before creating root signal
        if not flip_is_stable_enough(symbol, tf, last_start):
            log("WS mid-candle: flip observed but not yet stable for", symbol, tf, "start", last_start)
            return

        # For root creation only allow if tf is one of the root scanners' tfs (1h/4h)
        if tf in ("1h", "4h"):
            dq_root = cache_get(symbol, TF_MAP[tf])
            if not dq_root:
                log("WS mid-candle: missing root cache after detect for", symbol, tf)
                return
            last_candle = list(dq_root)[-1]
            key = f"ROOT-{symbol}-{tf}-{last_start}"
            sig = {
                "id": key,
                "symbol": symbol,
                "root_tf": tf,
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
                log("WS mid-candle: Root signal created:", key)
            return

    except Exception as e:
        log("process_inprogress_update error for", symbol, interval_token, e)

# ---------- Root scanning (maximise discovery, parallel detect) ----------
async def root_scanner_loop(root_tf: str):
    """
    Main root scanner loop updated to maximise discovery:
      - Does not perform hard global filtering by default (SKIP_DIGIT_PREFIX disabled, MIN_SYMBOL_AGE_MONTHS default 0).
      - Batches detect_flip across symbols with a bounded concurrency (DISCOVERY_CONCURRENCY).
      - Uses recent_root_signals and ROOT_DEDUP_SECONDS to avoid immediate duplicates.
      - Logs skips explicitly for dedupe/active-signal/existing-root-time to surface why a symbol was not processed.
    """
    log("Root scanner started for", root_tf, "DISCOVERY_CONCURRENCY=", DISCOVERY_CONCURRENCY, "ROOT_DEDUP_SECONDS=", ROOT_DEDUP_SECONDS)
    semaphore = asyncio.Semaphore(DISCOVERY_CONCURRENCY)
    while True:
        try:
            symbols = await get_tradable_usdt_symbols()
            now_s = int(time.time())
            if not symbols:
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)
                continue

            # Prewarm caches for the root timeframe but bounded to avoid huge bursts
            try:
                # limited parallel prewarm with concurrency semaphore
                async def _prewarm(sym):
                    async with semaphore:
                        try:
                            await ensure_cached_candles(sym, root_tf, MIN_CANDLES_REQUIRED)
                        except Exception:
                            pass
                # Run prewarm in batches to avoid creating huge simultaneous requests
                batch = []
                for s in symbols:
                    batch.append(asyncio.create_task(_prewarm(s)))
                    if len(batch) >= DISCOVERY_CONCURRENCY:
                        await asyncio.gather(*batch, return_exceptions=True)
                        batch = []
                if batch:
                    await asyncio.gather(*batch, return_exceptions=True)
            except Exception as e:
                log("Prewarm error:", e)

            # Concurrent detect_flip runs
            tasks = []

            async def _detect_and_maybe_create(sym: str):
                async with semaphore:
                    try:
                        # Dedup: if we recently created a root flip for this symbol (within ROOT_DEDUP_SECONDS), skip and log
                        recent = recent_root_signals.get(sym)
                        if recent and (now_s - recent) < ROOT_DEDUP_SECONDS:
                            log("Skipped symbol (root dedup):", sym, f"recent_root={recent}, age_s={now_s - recent} (<{ROOT_DEDUP_SECONDS}s)")
                            return
                        # If a root or entry already active for this symbol, skip but log (we avoid silent skipping)
                        if signal_exists_for(sym, "root"):
                            log("Skipped symbol (already has active root):", sym)
                            return
                        if signal_exists_for(sym, "entry"):
                            log("Skipped symbol (already has active entry):", sym)
                            return

                        flip_kind, last_start = await detect_flip(sym, root_tf)
                        if not flip_kind:
                            # nothing to do
                            return
                        # Require stability before creating root signal
                        if not flip_is_stable_enough(sym, root_tf, last_start):
                            log("Skipped symbol (flip not yet stable):", sym, "tf=", root_tf, "start=", last_start)
                            return
                        # duplication guard: if an active root already exists for this symbol and same root_tf and same flip_time skip
                        exists_same = any(s.get("symbol") == sym and s.get("signal_type") == "root" and s.get("root_flip_time") == last_start for s in active_root_signals.values())
                        if exists_same:
                            log("Skipped symbol (existing in-memory same root flip):", sym, last_start)
                            return
                        dq_root = cache_get(sym, TF_MAP[root_tf])
                        if not dq_root:
                            log("Skipped symbol (missing root cache after detect):", sym)
                            return
                        last_candle = list(dq_root)[-1]
                        key = f"ROOT-{sym}-{root_tf}-{last_start}"
                        sig = {
                            "id": key,
                            "symbol": sym,
                            "root_tf": root_tf,
                            "root_flip_time": last_start,
                            "root_flip_price": last_candle.get("close"),
                            "created_at": now_ts_ms(),
                            "status": "watching",
                            "priority": None,
                            "signal_type": "root",
                            "components": [],
                            "flip_kind": flip_kind,  # will be "open"
                        }
                        added = await add_signal(sig)
                        if added:
                            log("Root signal created:", key)
                    except Exception as e:
                        log("Error in detect+create for", sym, e)

            # Launch detection tasks in controlled batches so the event loop stays responsive
            batch = []
            for symbol in symbols:
                batch.append(asyncio.create_task(_detect_and_maybe_create(symbol)))
                if len(batch) >= DISCOVERY_CONCURRENCY:
                    await asyncio.gather(*batch, return_exceptions=True)
                    batch = []
            if batch:
                await asyncio.gather(*batch, return_exceptions=True)

        except Exception as e:
            log("root_scanner_loop outer error:", e)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

# ---------- Evaluate existing signals (root/super) for entry/trade ----------
async def evaluate_signal_once(symbol: str, signal_id: Optional[str] = None):
    """
    Evaluate a single signal (identified by signal_id) for possible entry immediately.
    This is used as an immediate trigger right after add_signal to reduce missed opportunities.
    If signal_id is None, evaluate all active signals for the symbol (used by periodic evaluator).
    The logic mirrors evaluate_signals_loop but is scoped to a symbol (and optionally one signal id).
    """
    try:
        # determine which signals to evaluate for this symbol
        sig_ids = []
        if signal_id:
            if signal_id in active_root_signals:
                sig_ids = [signal_id]
            else:
                return
        else:
            sig_ids = [sid for sid, s in active_root_signals.items() if s.get("symbol") == symbol]

        for sid in sig_ids:
            sig = active_root_signals.get(sid)
            if not sig:
                continue
            stype = sig.get("signal_type", "root")
            symbol = sig.get("symbol")
            root_tf = sig.get("root_tf")
            # Sanity checks
            if is_stablecoin_symbol(symbol) or symbol_too_new(symbol):
                await remove_signal(sid)
                continue
            if stype not in ("root", "super"):
                continue
            # Build required TF list
            if stype == "root":
                required_tfs = tf_list_for_root(root_tf)
            else:
                required_tfs = ["5m", "15m", "1h", "1d"]
            # Ensure cached candles for all TFs (parallel)
            await asyncio.gather(*(ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED) for tf in required_tfs))
            tf_status = {}
            # For each timeframe determine:
            for tf in required_tfs:
                token = TF_MAP.get(tf, tf)
                dq = cache_get(symbol, token)
                if not dq or len(dq) < MIN_CANDLES_REQUIRED:
                    tf_status[tf] = {"has": False}
                    continue
                closed = last_candle_is_closed(dq, token, safety_seconds=3)
                closes = candles_to_closes(dq)
                positive = macd_positive_from_closes(closes)
                flip_kind, last_ts = await detect_flip(symbol, tf)
                # only treat flip_last True for "open" flips (we ignore closed flips)
                flip_last = True if flip_kind == "open" else False
                last_ts_val = list(dq)[-1]["start"] if dq else None
                tf_status[tf] = {"has": True, "closed": closed, "positive": positive, "flip_last": flip_last, "last_ts": last_ts_val, "flip_kind": flip_kind}
            # Necessary root checks
            if stype == "root":
                if root_tf not in tf_status or not tf_status[root_tf].get("has"):
                    continue
                if not tf_status[root_tf].get("flip_last"):
                    continue
            # Among other TFs count negatives
            other_tfs = [tf for tf in required_tfs if tf != root_tf] if stype == "root" else [tf for tf in required_tfs if tf != "1d"]
            negatives = [tf for tf in other_tfs if not tf_status.get(tf, {}).get("positive")]
            # If >1 negative -> not ready
            if len(negatives) > 1:
                continue
            # If exactly 1 negative -> that tf must have flip_last True (open flip)
            if len(negatives) == 1:
                tf_to_flip = negatives[0]
                st = tf_status.get(tf_to_flip, {})
                if not st.get("has"):
                    continue
                if not st.get("flip_last"):
                    continue
                # ensure all other tfs (excluding root and tf_to_flip) are positive
                for tf in other_tfs:
                    if tf == tf_to_flip:
                        continue
                    if not tf_status.get(tf, {}).get("positive"):
                        break
                else:
                    pass  # OK
            else:
                # zero negatives -> full alignment
                pass

            # Achieved entry readiness. Duplication guards:
            if signal_exists_for(symbol, "entry"):
                continue
            if await has_open_trade(symbol):
                log("Open trade exists for", symbol, "— skipping entry")
                continue

            # Build entry signal and attempt trade
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
            # add_signal will check duplication and register under symbol lock
            added = await add_signal(entry_sig)
            if not added:
                continue

            # Acquire symbol lock again to ensure only one trade per symbol is placed (avoid race with other loops)
            lock = get_symbol_lock(symbol)
            async with lock:
                # Re-check open trade guard inside lock
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
                # mark entry signal as acted (update persisted and in-memory)
                if entry_id in active_root_signals:
                    active_root_signals[entry_id]["status"] = "acted"
                    try:
                        await persist_root_signal(active_root_signals[entry_id])
                    except Exception:
                        pass

    except Exception as e:
        log("evaluate_signal_once error for", symbol, e)

async def evaluate_signals_loop():
    """
    Periodically iterate active signals and evaluate if entry/trade conditions are met.
    This loop works co-operatively with immediate evaluate_signal_once triggers launched when signals are created.
    """
    while True:
        try:
            ids = list(active_root_signals.keys())
            for sid in ids:
                try:
                    sig = active_root_signals.get(sid)
                    if not sig:
                        continue
                    # Evaluate this signal (single symbol) via the single-signal evaluator
                    await evaluate_signal_once(sig.get("symbol"), sid)
                except Exception as e:
                    log("evaluate_signals_loop error for", sid, e)
            await asyncio.sleep(max(5, SCAN_INTERVAL_SECONDS // 2))
        except Exception as e:
            log("evaluate_signals_loop outer error:", e)
            await asyncio.sleep(5)

# ---------- Super-signal detection during frequent 5m checks ----------
async def super_signal_scanner_loop_5m():
    """
    Runs every SCAN_INTERVAL_SECONDS. Detect when 2 or more of (1h,4h,1d) have open flips
    within the same SUPER_WINDOW_SECONDS window, and emit a 'super' signal.
    Implemented with bounded concurrency and explicit skip logging.
    """
    log("Super scanner started (5m loop), DISCOVERY_CONCURRENCY=", DISCOVERY_CONCURRENCY)
    semaphore = asyncio.Semaphore(DISCOVERY_CONCURRENCY)
    while True:
        try:
            symbols = await get_tradable_usdt_symbols()
            if not symbols:
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)
                continue

            tasks = []
            async def _check_super(sym: str):
                async with semaphore:
                    try:
                        # Dedup: avoid creating repeated supers for the same symbol within SUPER_WINDOW_SECONDS
                        last_super = recent_super_signals.get(sym)
                        if last_super and (int(time.time()) - last_super) < SUPER_WINDOW_SECONDS:
                            return

                        flips = []
                        # for each long TF check whether it flipped on its most recent candle (open flip only)
                        for tf in ["1h", "4h", "1d"]:
                            try:
                                flip_kind, last_ts = await detect_flip(sym, tf)
                                if flip_kind == "open" and last_ts:
                                    # ensure flip stability if configured
                                    if flip_is_stable_enough(sym, tf, last_ts):
                                        flips.append((tf, last_ts))
                            except Exception:
                                continue
                        if len(flips) >= 2:
                            times = [t for _, t in flips]
                            if max(times) - min(times) <= SUPER_WINDOW_SECONDS:
                                # duplication guard: check existing active super for symbol
                                if signal_exists_for(sym, "super"):
                                    log("Skipped super creation (already active) for", sym)
                                    return
                                # recent_super_signals also prevents multiple near duplicates
                                ts = int(time.time())
                                key = f"SUPER-{sym}-{ts}"
                                sig = {
                                    "id": key,
                                    "symbol": sym,
                                    "root_tf": "multi",
                                    "root_flip_time": ts,
                                    "root_flip_price": None,
                                    "created_at": now_ts_ms(),
                                    "status": "watching",
                                    "priority": "super",
                                    "signal_type": "super",
                                    "components": [tf for tf, _ in flips],
                                }
                                added = await add_signal(sig)
                                if added:
                                    # mark recent super creation time
                                    recent_super_signals[sym] = ts
                                    log("SUPER signal detected for", sym, "components:", sig["components"])
                    except Exception as e:
                        log("super check error for", sym, e)

            # run checks in bounded batches
            batch = []
            for s in symbols:
                batch.append(asyncio.create_task(_check_super(s)))
                if len(batch) >= DISCOVERY_CONCURRENCY:
                    await asyncio.gather(*batch, return_exceptions=True)
                    batch = []
            if batch:
                await asyncio.gather(*batch, return_exceptions=True)
        except Exception as e:
            log("super_signal_scanner_loop_5m error:", e)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

# ---------- Signal expiration (new) ----------
async def expire_signals_loop():
    """
    Periodically remove expired signals:
      - root signals expire when the root timeframe's candle closes (flip_time + interval_seconds)
      - super signals expire after SUPER_WINDOW_SECONDS
      - entry signals can be removed later when trade placed or manually
    This ensures persisted and in-memory signals don't linger and cause repeated forwarding.
    """
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
                                # add small safety margin
                                if now_s >= expiry:
                                    to_remove.append(sid)
                    elif stype == "super":
                        flip_time = sig.get("root_flip_time")
                        if flip_time and now_s >= (flip_time + SUPER_WINDOW_SECONDS):
                            to_remove.append(sid)
                    # entry signals may have own logic; we don't auto-expire entry here
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

async def load_persisted_root_signals():
    """
    Load persisted signals into memory and seed recent_root_signals so we won't recreate signals on restart.
    Remove expired persisted signals (DB) so they aren't re-forwarded.
    """
    try:
        async with db.execute("SELECT id,symbol,root_tf,flip_time,flip_price,status,priority,signal_type,components,created_at FROM root_signals") as cur:
            rows = await cur.fetchall()
        # track max flip times per symbol to seed recent_root_signals
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

                # Determine if persisted signal expired -> if expired delete from DB and skip loading
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

                # Keep the signal in memory
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
                # record max flip_time
                if symbol and flip_time:
                    cur_max = max_flip_per_symbol.get(symbol)
                    if cur_max is None or flip_time > cur_max:
                        max_flip_per_symbol[symbol] = flip_time
                loaded += 1
            except Exception:
                continue
        # seed recent_root_signals from DB to avoid re-creating persisted signals after restart
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
    return {"count": len(active_root_signals), "signals": list(active_root_signals.values())}

@app.get("/debug/check_symbol")
async def debug_check_symbol(symbol: str = Query(..., min_length=1), tf: str = Query("1h"), _auth=Depends(require_admin_auth)):
    if tf not in TF_MAP and tf not in TF_MAP.values():
        raise HTTPException(status_code=400, detail=f"Unknown timeframe {tf}. Valid: {list(TF_MAP.keys())}")
    token = TF_MAP.get(tf, tf)
    # ensure cache updated
    await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
    dq = cache_get(symbol, token)
    if not dq:
        return {"symbol": symbol, "tf": tf, "klines_count": 0, "error": "no cached klines"}
    closes = candles_to_closes(dq)
    hist = macd_hist(closes)
    flip_kind, flip_ts = await detect_flip(symbol, tf)
    closed = last_candle_is_closed(dq, token)
    # provide observed flip registry info if available
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

# Admin endpoint to trigger the root signals summary manually
@app.post("/admin/send_root_summary")
async def admin_send_root_summary(_auth=Depends(require_admin_auth)):
    """
    Manually trigger sending the current root signals summary to Telegram.
    Protected by ADMIN_API_KEY.
    """
    await send_root_signals_telegram()
    return {"status": "ok", "sent": True}

# Simple root/health endpoints so pinging the server shows it is up
@app.get("/")
async def root():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}

@app.get("/health")
async def health():
    # minimal health info: DB connected, WS connected status (best-effort)
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
    log("Startup config:", "BYBIT_USE_MAINNET=", BYBIT_USE_MAINNET, "TRADING_ENABLED=", TRADING_ENABLED, "PUBLIC_WS_URL=", PUBLIC_WS_URL, "MIN_CANDLES_REQUIRED=", MIN_CANDLES_REQUIRED, "SKIP_DIGIT_PREFIX=", SKIP_DIGIT_PREFIX, "MIN_SYMBOL_AGE_MONTHS=", MIN_SYMBOL_AGE_MONTHS, "FLIP_STABILITY_SECONDS=", FLIP_STABILITY_SECONDS)
    public_ws = PublicWebsocketManager(PUBLIC_WS_URL)
    try:
        ok = await public_ws.connect_and_detect(timeout=6.0)
        if not ok:
            log("Public WS detect warning")
    except Exception:
        log("Public WS connect error")
    # ensure telegram worker started
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and (_TELEGRAM_WORKER_TASK is None or _TELEGRAM_WORKER_TASK.done()):
        try:
            _TELEGRAM_WORKER_TASK = asyncio.create_task(_telegram_worker())
        except Exception:
            pass
    # Start background loops (root scanners run concurrently for 1h and 4h)
    asyncio.create_task(root_scanner_loop("1h"))
    asyncio.create_task(root_scanner_loop("4h"))
    asyncio.create_task(super_signal_scanner_loop_5m())
    asyncio.create_task(evaluate_signals_loop())
    asyncio.create_task(expire_signals_loop())
    # Periodic logger of current root signals (console-only; no automatic Telegram summary)
    async def periodic_root_logger():
        while True:
            try:
                await log_current_root_signals()
                # NOTE: removed automatic send_root_signals_telegram() here to avoid repeated "Root signals summary" messages.
                # If you want the summary in Telegram, call /admin/send_root_summary manually.
            except Exception as e:
                log("periodic_root_logger error:", e)
            await asyncio.sleep(max(30, ROOT_SIGNALS_LOG_INTERVAL))
    asyncio.create_task(periodic_root_logger())
    log("Background tasks started")

# ---------- Run note ----------
# Start with:
# uvicorn main:app --host 0.0.0.0 --port 8000
