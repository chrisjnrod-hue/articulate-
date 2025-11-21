# main.py
# Bybit MACD multi-timeframe scanner — flip-on-open ONLY with robust dedupe, websockets, telegram queue, persistence
#
# Features:
# - Strict flip-on-open MACD detection (prev histogram from closed candles only; current uses in-progress candle)
# - Root signals (per-symbol, per-root-tf), Super signals (multi-TF within window), Entry signals (MTF confirmation -> trade)
# - Signal persistence to SQLite with defensive INSERT OR IGNORE and unique index on symbol/root_tf/flip_time
# - Robust in-process dedupe (per-symbol locks + recent_* maps) for root/super/entry
# - Expiration of signals at root candle close and super-window end
# - Telegram background worker with queue + rate-limit/retry handling
# - Resilient HTTP helpers with concurrency limiting and 429 handling
# - Public WebSocket manager to ingest kline updates and merge into in-memory candle cache
# - Admin/debug endpoints for inspection and manual summary push
#
# Run:
#   uvicorn main:app --host 0.0.0.0 --port 8000
#
# Notes:
# - This is a drop-in replacement intended to be complete and robust. Keep environment variables in .env
# - If running multiple instances, consider adding a distributed lock (Redis or DB advisory) to avoid cross-process duplicate trades.
# - The code is verbose with extra logging lines to aid debugging on Render or similar hosts.

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
import math
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple, Deque
from collections import deque, defaultdict

import httpx
import aiosqlite
import websockets
from fastapi import FastAPI, Query, Depends, Header, HTTPException, status
from fastapi.responses import FileResponse, Response, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

# ---------- Configuration / environment ----------
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

# concurrency / HTTP tuning
PUBLIC_REQ_CONCURRENCY = int(os.getenv("PUBLIC_REQ_CONCURRENCY", "8"))
PUBLIC_REQ_RETRIES = int(os.getenv("PUBLIC_REQ_RETRIES", "3"))
HTTP_REQUEST_TIMEOUT = int(os.getenv("HTTP_REQUEST_TIMEOUT", "20"))

# Telegram settings
TELEGRAM_WORKER_CONCURRENCY = 1
TELEGRAM_RETRY_LIMIT = int(os.getenv("TELEGRAM_RETRY_LIMIT", "4"))

# Candle cache sizing
CANDLE_CACHE_MAX = int(os.getenv("CANDLE_CACHE_MAX", "2000"))
CANDLE_CACHE_TTL = int(os.getenv("CANDLE_CACHE_TTL", "300"))

# Skip tokens beginning with digits (spam)
SKIP_DIGIT_PREFIX = os.getenv("SKIP_DIGIT_PREFIX", "true").lower() == "true"

# Limit scanned symbols for dev/testing
SYMBOL_SCAN_LIMIT = int(os.getenv("SYMBOL_SCAN_LIMIT", "0"))

# periodic logging interval
ROOT_SIGNALS_LOG_INTERVAL = int(os.getenv("ROOT_SIGNALS_LOG_INTERVAL", "60"))

# Bybit hosts / endpoints
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

# candidate websocket templates to detect bybit schema
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
# minimum candles required for MACD calculations (slow + signal + buffer)
MIN_CANDLES_REQUIRED = MACD_SLOW + MACD_SIGNAL + 5

# trading parameters
LEVERAGE = int(os.getenv("LEVERAGE", "3"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.015"))
BREAKEVEN_PCT = float(os.getenv("BREAKEVEN_PCT", "0.005"))
STABLECOINS = {"USDT", "BUSD", "USDC", "TUSD", "DAI"}

# min symbol listing age (months)
MIN_SYMBOL_AGE_MONTHS = int(os.getenv("MIN_SYMBOL_AGE_MONTHS", "3"))

# super detection window seconds (default 5 minutes)
SUPER_WINDOW_SECONDS = int(os.getenv("SUPER_WINDOW_SECONDS", "300"))

# ---------- Globals ----------
app = FastAPI(title="Bybit MACD Multi-TF Scanner")
# allow local UI access if needed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

httpx_client = httpx.AsyncClient(timeout=HTTP_REQUEST_TIMEOUT)
db: Optional[aiosqlite.Connection] = None

# symbol metadata cache (from instruments endpoint)
symbols_info_cache: Dict[str, Dict[str, Any]] = {}

# in-memory candle cache: candles_cache[symbol][interval] = deque([...])
candles_cache: Dict[str, Dict[str, deque]] = defaultdict(lambda: {})
candles_cache_ts: Dict[str, Dict[str, int]] = defaultdict(lambda: {})

# signals in memory and helper indexes
active_root_signals: Dict[str, Dict[str, Any]] = {}
active_signal_index: Dict[str, set] = defaultdict(set)

# dedupe maps (in-process)
recent_root_signals: Dict[str, int] = {}
recent_super_signals: Dict[str, Tuple[int, ...]] = {}
recent_entry_signals: Dict[str, int] = {}

# per-symbol locks
symbol_locks: Dict[str, asyncio.Lock] = {}

# websocket manager instance
public_ws = None
private_ws = None

# concurrency primitives
PUBLIC_REQUEST_SEMAPHORE = asyncio.Semaphore(PUBLIC_REQ_CONCURRENCY)

# telegram queue & worker
TELEGRAM_QUEUE: "asyncio.Queue[Tuple[str,int]]" = asyncio.Queue()
_TELEGRAM_WORKER_TASK: Optional[asyncio.Task] = None

_re_leading_digit = re.compile(r"^\d")

# ---------- Utility helpers ----------
def log(*args, **kwargs):
    """
    Simple timestamped logger. Respects LOG_LEVEL (non-empty).
    """
    if LOG_LEVEL != "none":
        ts = datetime.now(timezone.utc).isoformat()
        print(ts, *args, **kwargs)

def now_ts_ms() -> int:
    return int(time.time() * 1000)

def safe_json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, default=str)
    except Exception:
        try:
            return str(obj)
        except Exception:
            return "<unserializable>"

# ---------- Telegram worker & helpers ----------
async def _telegram_worker():
    """
    Background worker that sends messages to Telegram from TELEGRAM_QUEUE.
    Each queue item: (text, attempt_count)
    Respects Telegram rate-limits (handles 429 -> retry_after) and performs exponential backoff on other errors.
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
                r = await httpx_client.post(url, json=payload, timeout=10)
                if r.status_code == 200:
                    try:
                        data = r.json()
                    except Exception:
                        data = {"raw_text": (r.text[:400] + "...") if r.text else ""}
                    log("Telegram sent ok (worker) response:", data)
                elif r.status_code == 429:
                    # rate limited
                    retry_after = 5
                    try:
                        data = r.json()
                        retry_after = int(data.get("parameters", {}).get("retry_after", retry_after))
                    except Exception:
                        try:
                            retry_after = int(r.headers.get("Retry-After", retry_after))
                        except Exception:
                            pass
                    log("Telegram rate limited, retry_after:", retry_after)
                    await asyncio.sleep(retry_after)
                    if attempt + 1 < TELEGRAM_RETRY_LIMIT:
                        await TELEGRAM_QUEUE.put((text, attempt + 1))
                    else:
                        log("Telegram retry limit reached, dropping message")
                else:
                    log("Telegram send failed:", r.status_code, r.text[:400])
                    if attempt + 1 < TELEGRAM_RETRY_LIMIT:
                        backoff = min(60, 2 ** attempt)
                        await asyncio.sleep(backoff)
                        await TELEGRAM_QUEUE.put((text, attempt + 1))
                    else:
                        log("Telegram worker: retries exhausted, dropping message")
            except Exception as e:
                log("Telegram worker exception:", e)
                if attempt + 1 < TELEGRAM_RETRY_LIMIT:
                    backoff = min(60, 2 ** attempt)
                    await asyncio.sleep(backoff)
                    await TELEGRAM_QUEUE.put((text, attempt + 1))
                else:
                    log("Telegram worker: unrecoverable send error, dropping message")
            finally:
                TELEGRAM_QUEUE.task_done()
        except Exception as e:
            log("Telegram worker top-level error:", e)
            await asyncio.sleep(1)

async def send_telegram(text: str):
    """
    Enqueue a telegram message for background sending.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram not configured; skipping:", text)
        return
    global _TELEGRAM_WORKER_TASK
    if _TELEGRAM_WORKER_TASK is None or _TELEGRAM_WORKER_TASK.done():
        try:
            _TELEGRAM_WORKER_TASK = asyncio.create_task(_telegram_worker())
        except Exception as e:
            log("Failed to start telegram worker:", e)
    try:
        await TELEGRAM_QUEUE.put((text, 0))
    except Exception as e:
        log("Failed to enqueue telegram:", e)

async def send_root_signals_telegram():
    """
    On-demand send of concise root signals summary.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram not configured; skipping root summary")
        return
    if not active_root_signals:
        await send_telegram("Root signals: 0")
        return
    lines = ["Root signals summary:"]
    for sig in active_root_signals.values():
        sym = sig.get("symbol")
        tf = sig.get("root_tf")
        stype = sig.get("signal_type", "root")
        status = sig.get("status", "watching")
        lines.append(f"{sym} {tf} {stype} {status}")
    await send_telegram("\n".join(lines))

# ---------- Admin authentication ----------
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

# ---------- EMA / MACD helpers ----------
def ema(values: List[float], period: int) -> List[float]:
    if not values or period <= 0:
        return []
    k = 2.0 / (period + 1.0)
    emas = []
    ema_prev = values[0]
    emas.append(ema_prev)
    for v in values[1:]:
        ema_prev = v * k + ema_prev * (1.0 - k)
        emas.append(ema_prev)
    return emas

def macd_hist(prices: List[float], fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL) -> List[Optional[float]]:
    """
    Compute MACD histogram values aligned with 'prices'.
    Returns list same length as prices, with leading None padding where not enough data.
    """
    if len(prices) < slow + signal:
        return []
    ema_fast = ema(prices, fast)
    ema_slow = ema(prices, slow)
    # align by use of same length lists but ema_slow may be shorter if slow > fast
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = ema(macd_line, signal)
    # hist aligns to end of macd_line with padding
    hist = [m - s for m, s in zip(macd_line[len(macd_line) - len(signal_line):], signal_line)]
    padding = len(prices) - len(hist)
    return [None] * padding + hist

# ---------- Resilient public GET ----------
async def resilient_public_get(endpoints: List[str], params: Dict[str, Any] = None, timeout: int = 12) -> Optional[Dict[str, Any]]:
    """
    Rotate over hosts and endpoints, limit concurrency, respect 429 retry-after, exponential backoff on failures.
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
                            log("Invalid JSON from", url, "excerpt:", (r.text[:400] + "...") if r.text else "")
                            continue
                    else:
                        body_excerpt = (r.text[:400] + "...") if r.text else ""
                        log("Public GET", url, "returned", r.status_code, "excerpt:", body_excerpt)
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

# ---------- Bybit signed requests ----------
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
            log("Signed request exception:", e)
            await asyncio.sleep(min(60, 2 ** attempt))
    return {}

# ---------- SQLite init & helpers ----------
async def init_db():
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS root_signals (
            id TEXT PRIMARY KEY,
            symbol TEXT,
            root_tf TEXT,
            flip_time INTEGER,
            flip_price REAL,
            status TEXT,
            priority TEXT,
            signal_type TEXT,
            components TEXT,
            created_at INTEGER
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id TEXT PRIMARY KEY,
            symbol TEXT,
            side TEXT,
            qty REAL,
            entry_price REAL,
            sl_price REAL,
            created_at INTEGER,
            open BOOLEAN,
            raw_response TEXT
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS raw_ws_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            topic TEXT,
            message TEXT,
            created_at INTEGER
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS public_subscriptions (
            topic TEXT PRIMARY KEY,
            created_at INTEGER
        )
    """)
    # unique index to protect against cross-process duplicates at DB level
    await db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_root_signals_symbol_tf_flip ON root_signals(symbol, root_tf, flip_time)""")
    await db.commit()
    log("DB initialized at", DB_PATH)

async def persist_root_signal(sig: Dict[str, Any]):
    comps = json.dumps(sig.get("components") or [])
    try:
        await db.execute("""
            INSERT OR IGNORE INTO root_signals (id,symbol,root_tf,flip_time,flip_price,status,priority,signal_type,components,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            sig["id"],
            sig["symbol"],
            sig.get("root_tf"),
            sig.get("root_flip_time") or sig.get("flip_time"),
            sig.get("root_flip_price") or sig.get("flip_price"),
            sig.get("status", "watching"),
            sig.get("priority"),
            sig.get("signal_type", "root"),
            comps,
            sig.get("created_at")
        ))
        await db.commit()
    except Exception as e:
        log("persist_root_signal DB error:", e)

async def remove_root_signal(sig_id: str):
    try:
        await db.execute("DELETE FROM root_signals WHERE id = ?", (sig_id,))
        await db.commit()
    except Exception as e:
        log("remove_root_signal error:", e)

async def persist_trade(tr: Dict[str, Any]):
    try:
        await db.execute("""
            INSERT OR REPLACE INTO trades (id,symbol,side,qty,entry_price,sl_price,created_at,open,raw_response)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            tr["id"],
            tr["symbol"],
            tr["side"],
            tr["qty"],
            tr.get("entry_price"),
            tr.get("sl_price"),
            tr["created_at"],
            tr.get("open", True),
            json.dumps(tr.get("raw"))
        ))
        await db.commit()
    except Exception as e:
        log("persist_trade error:", e)

async def update_trade_close(trade_id: str):
    try:
        await db.execute("UPDATE trades SET open = 0 WHERE id = ?", (trade_id,))
        await db.commit()
    except Exception as e:
        log("update_trade_close error:", e)

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
    try:
        await db.execute("INSERT OR REPLACE INTO public_subscriptions (topic,created_at) VALUES (?,?)", (topic, now_ts_ms()))
        await db.commit()
    except Exception as e:
        log("add_public_subscription error:", e)

async def remove_public_subscription(topic: str):
    try:
        await db.execute("DELETE FROM public_subscriptions WHERE topic = ?", (topic,))
        await db.commit()
    except Exception as e:
        log("remove_public_subscription error:", e)

# ---------- Instruments fetch / symbol filtering helpers ----------
async def get_tradable_usdt_symbols() -> List[str]:
    """
    Fetch instruments using several endpoints and return USDT linear/perpetual tradable symbols.
    Applies filtering: stablecoins, digit-prefix tokens (configurable), symbol age, status checks.
    Caches symbol info into symbols_info_cache for later use.
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
            # try many common shapes
            items = resp.get("result", {}).get("list", []) \
                    or resp.get("result", {}).get("data", []) \
                    or resp.get("data", []) \
                    or resp.get("list", []) \
                    or resp.get("result", []) \
                    or resp.get("symbols", []) \
                    or resp.get("rows", [])
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
                                # convert ms -> s if necessary
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
                # require USDT quote
                if not (symbol.upper().endswith("USDT") or quote == "USDT"):
                    continue
                # exclude stablecoin bases (e.g., USDTUSDT)
                base = symbol[:-4] if symbol.upper().endswith("USDT") else symbol
                if base.upper() in STABLECOINS:
                    continue
                # optional skip of digit-prefixed tokens
                if SKIP_DIGIT_PREFIX and _re_leading_digit.match(symbol):
                    continue
                # filter by status if present
                if status and isinstance(status, str) and status not in ("trading", "listed", "normal", "active", "open"):
                    continue
                # filter by symbol age if known
                if launch_ts:
                    age_days = (int(time.time()) - launch_ts) / 86400.0
                    if age_days < (MIN_SYMBOL_AGE_MONTHS * 30):
                        continue
                symbols_info_cache[symbol] = {
                    "symbol": symbol,
                    "quote": quote,
                    "inst_type": inst_type,
                    "launch_ts": launch_ts,
                    "status": status
                }
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
    Merge new candles (chronological ascending) into deque, dedupe by start time.
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
    # ensure sorted ascending by start
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
    Fetch klines via Bybit v5 kline endpoint. Returns list of dicts: {'start': seconds, 'end': seconds|None, 'close': float}
    """
    params = {"category": "linear", "symbol": symbol, "interval": str(interval_token), "limit": limit}
    resp = await resilient_public_get([KLINE_ENDPOINTS[0]], params=params)
    if not resp:
        return []
    items = []
    if isinstance(resp, dict):
        items = resp.get("result", {}).get("list", []) \
                or resp.get("list", []) \
                or resp.get("data", []) \
                or resp.get("result", []) \
                or resp.get("rows", [])
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

# ---------- Detect flip (STRICT open-only) ----------
async def detect_flip(symbol: str, tf: str) -> Tuple[Optional[str], Optional[int]]:
    """
    Strict 'flip-on-open' detection:
      - prev_closed_hist computed from closed candles (closes[:-1])
      - cur_inprog_hist computed by appending in-progress close and recomputing
      - if prev_closed_hist <= 0 and cur_inprog_hist > 0 -> return ("open", last_start)
      - else return (None, None)
    """
    if tf not in TF_MAP:
        return None, None
    token = TF_MAP[tf]
    await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
    dq = cache_get(symbol, token)
    if not dq or len(dq) < MIN_CANDLES_REQUIRED:
        return None, None
    arr = list(dq)
    closes = [c["close"] for c in arr if c.get("close") is not None]
    if len(closes) < MACD_SLOW + MACD_SIGNAL + 1:
        return None, None
    # require last candle to be in-progress (not closed)
    if last_candle_is_closed(dq, token, safety_seconds=3):
        return None, None
    closed_closes = closes[:-1]
    inprog_close = closes[-1]
    if len(closed_closes) < MACD_SLOW + MACD_SIGNAL:
        return None, None
    hist_closed = macd_hist(closed_closes)
    hist_with_inprog = macd_hist(closed_closes + [inprog_close])
    if not hist_closed or not hist_with_inprog:
        return None, None
    prev_closed_hist = hist_closed[-1] if hist_closed else 0
    cur_inprog_hist = hist_with_inprog[-1] if hist_with_inprog else 0
    last_start = arr[-1]["start"]
    if (prev_closed_hist is None or prev_closed_hist <= 0) and (cur_inprog_hist is not None and cur_inprog_hist > 0):
        return "open", last_start
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

# ---------- Signal index helpers & dedupe ----------
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
    """
    Add a signal with multiple duplication guards:
      - active_signal_index guard (fast)
      - idempotency: existing id
      - recent_root_signals for root
      - recent_super_signals for super (component times tuple)
      - recent_entry_signals for entry (short cooldown)
      - per-symbol asyncio lock to serialize modifications and avoid races
    On success persist to DB.
    """
    sid = sig.get("id")
    stype = sig.get("signal_type", "root")
    sym = sig.get("symbol")
    if not sid or not sym:
        log("add_signal: invalid signal missing id or symbol:", sid, sym)
        return False
    root_time = sig.get("root_flip_time") or sig.get("flip_time")
    lock = get_symbol_lock(sym)
    async with lock:
        # fast guard: same type already active for this symbol
        if signal_exists_for(sym, stype):
            log("Duplicate signal suppressed (active_signal_index):", sym, stype)
            return False
        # idempotency
        if sid in active_root_signals:
            log("Signal id already present, skipping:", sid)
            return False
        # root-specific dedupe & validation
        if stype == "root" and root_time is not None:
            existing_recent = recent_root_signals.get(sym)
            if existing_recent == root_time:
                log("Duplicate root suppressed by recent_root_signals:", sym, root_time)
                return False
            # validate current open candle matches root_time
            try:
                root_tf = sig.get("root_tf")
                token = TF_MAP.get(root_tf)
                if token:
                    await ensure_cached_candles(sym, root_tf, MIN_CANDLES_REQUIRED)
                    dq = cache_get(sym, token)
                    if not dq or len(dq) == 0:
                        log("add_signal: cannot validate root signal; cache missing:", sym, root_tf)
                        return False
                    last_start = list(dq)[-1]["start"]
                    if last_candle_is_closed(dq, token, safety_seconds=3):
                        log("add_signal: last root candle is closed; rejecting root:", sym, root_tf)
                        return False
                    if last_start != root_time:
                        log("add_signal: root_flip_time mismatch -> reject", sym, root_tf, "expected", last_start, "got", root_time)
                        return False
            except Exception as e:
                log("add_signal root validation error:", e)
                return False
        # super-specific dedupe
        if stype == "super":
            comp_times = tuple(sorted([int(x.get("flip_time")) for x in (sig.get("components") or []) if x.get("flip_time")]))
            existing = recent_super_signals.get(sym)
            if existing == comp_times:
                log("Duplicate super suppressed by recent_super_signals:", sym, comp_times)
                return False
        # entry-specific dedupe
        if stype == "entry":
            existing_entry = recent_entry_signals.get(sym)
            created_at = sig.get("created_at") or now_ts_ms()
            if existing_entry and abs(created_at - existing_entry) < 3000:  # 3 seconds
                log("Duplicate entry suppressed by recent_entry_signals:", sym, existing_entry, created_at)
                return False
        # Passed all checks -> register
        active_root_signals[sid] = sig
        register_signal_index(sym, stype)
        if stype == "root" and root_time is not None:
            recent_root_signals[sym] = root_time
        if stype == "super":
            comp_times = tuple(sorted([int(x.get("flip_time")) for x in (sig.get("components") or []) if x.get("flip_time")]))
            if comp_times:
                recent_super_signals[sym] = comp_times
        if stype == "entry":
            recent_entry_signals[sym] = sig.get("created_at") or now_ts_ms()
        try:
            await persist_root_signal(sig)
        except Exception as e:
            log("persist_root_signal error:", e)
        log("Added signal:", sid, stype, sym)
        # Send per-signal notification (single-line) via Telegram queue
        try:
            await send_telegram(f"Added signal: {sid} {stype} {sym}")
        except Exception:
            pass
        return True

async def remove_signal(sig_id: str):
    """
    Remove signal from in-memory maps and DB, clear recent dedupe entries if appropriate.
    """
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
        try:
            comp_times = tuple(sorted([int(x.get("flip_time")) for x in (sig.get("components") or []) if x.get("flip_time")]))
            if comp_times and recent_super_signals.get(sym) == comp_times:
                del recent_super_signals[sym]
        except Exception:
            pass
    if stype == "entry":
        try:
            if sym in recent_entry_signals:
                del recent_entry_signals[sym]
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

# ---------- Scanners & evaluation loops ----------
async def root_scanner_loop(root_tf: str):
    """
    Scans symbols for root flips on specified root_tf (1h and 4h typically).
    Only emits signals for flips detected on the in-progress candle (open flips).
    """
    log("Root scanner started for", root_tf)
    while True:
        try:
            symbols = await get_tradable_usdt_symbols()
            # Prewarm caches for the root timeframe
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
                    # duplication guard: active root for same symbol & same flip time
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
                    log("root_scanner_loop error for", symbol, e)
        except Exception as e:
            log("root_scanner_loop outer error:", e)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

async def super_signal_scanner_loop_5m():
    """
    Runs frequently (SCAN_INTERVAL_SECONDS). Detects when 2+ of (1h,4h,1d) flip open within SUPER_WINDOW_SECONDS
    and emits a super signal with components list [{"tf":..., "flip_time":...}, ...].
    """
    log("Super signal scanner started (5m cadence)")
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
                            log("SUPER signal detected for", symbol, "components:", components)
        except Exception as e:
            log("super_signal_scanner_loop_5m error:", e)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

async def evaluate_signals_loop():
    """
    Evaluate active root/super signals for entry/trade conditions:
      - For root signals: required TFs (tf_list_for_root). Root must have flip_last True (flip on open)
      - Among other TFs at most one may be negative; that negative (if present) must have flip_last True (open)
      - If conditions satisfied and no open trade / entry exists, create entry signal and place trade (or simulate)
    """
    log("Evaluator loop started")
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
                    # Ensure caches
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
                    # Root must exist and have flip_last True
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
                    # Ready for entry
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
                    # Acquire symbol lock and place trade (or simulate)
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

async def expire_signals_loop():
    """
    Remove expired signals:
      - root signals: when root candle closes (flip_time + interval)
      - super signals: flip_time + SUPER_WINDOW_SECONDS
      - entry signals: not auto-expired here (can be managed by trade lifecycle)
    """
    log("Expire loop started")
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

# ---------- Persistence loader ----------
async def load_persisted_root_signals():
    """
    Load persisted signals from DB into memory on startup, skip expired persisted signals and seed recent_root_signals map.
    """
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
                # expired?
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

# ---------- Debug & admin endpoints ----------
@app.get("/debug/current_roots")
async def debug_current_roots(_auth=Depends(require_admin_auth)):
    return JSONResponse({
        "count": len(active_root_signals),
        "signals": list(active_root_signals.values()),
        "recent_root_signals": recent_root_signals,
        "recent_super_signals": recent_super_signals,
        "recent_entry_signals": recent_entry_signals
    })

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
    hist = macd_hist(closes)
    flip_kind, flip_ts = await detect_flip(symbol, tf)
    closed = last_candle_is_closed(dq, token)
    return {
        "symbol": symbol,
        "tf": tf,
        "cached_klines": len(dq),
        "macd_hist_len": len(hist),
        "macd_last": hist[-1] if hist else None,
        "flip_kind": flip_kind,
        "flip_ts": flip_ts,
        "last_closed": closed,
        "sample_last_klines": list(dq)[-10:],
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

# ---------- Ensure cached candles ----------
async def ensure_cached_candles(symbol: str, tf: str, required: int):
    """
    Ensure that the cache for (symbol, tf) has at least `required` candles.
    If cache is missing or stale, fetch klines and merge.
    """
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

def candles_to_closes(dq: Deque[Dict[str, Any]], last_n: Optional[int] = None) -> List[float]:
    arr = list(dq)
    if last_n:
        arr = arr[-last_n:]
    return [x["close"] for x in arr if x.get("close") is not None]

def macd_positive_from_closes(closes: List[float]) -> bool:
    hist = macd_hist(closes)
    if not hist:
        return False
    last = hist[-1]
    return last is not None and last > 0

# ---------- Interval & helper ----------
# interval_seconds_from_token and last_candle_is_closed already defined above; reuse those

# ---------- Startup ----------
@app.on_event("startup")
async def startup():
    global public_ws, private_ws, _TELEGRAM_WORKER_TASK
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
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and (_TELEGRAM_WORKER_TASK is None or _TELEGRAM_WORKER_TASK.done()):
        try:
            _TELEGRAM_WORKER_TASK = asyncio.create_task(_telegram_worker())
        except Exception:
            pass
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

# ---------- Run hint ----------
# Start server:
# uvicorn main:app --host 0.0.0.0 --port 8000

# End of file.
