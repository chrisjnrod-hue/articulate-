# main.py
# Bybit MACD multi-timeframe scanner — full patched version
# - Preserves original scanner, WS, MACD and trade logic
# - Adds atomic DB inserts (BEGIN IMMEDIATE), startup migrations & dedupe
# - Adds last_notified_at notification-safety and TELEGRAM_DEDUP_SECONDS
# - Adds browser-friendly DB diagnostics & cleanup endpoints
#
# Run: uvicorn main:app --host 0.0.0.0 --port $PORT

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
from fastapi import FastAPI, Query, Depends, Header, HTTPException, status, Request
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

# dedupe telegram identical messages within this many seconds (prevents repeated pushes)
TELEGRAM_DEDUP_SECONDS = int(os.getenv("TELEGRAM_DEDUP_SECONDS", "60"))

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

# super window seconds (5 minutes)
SUPER_WINDOW_SECONDS = int(os.getenv("SUPER_WINDOW_SECONDS", "300"))

# ---------- Globals ----------
app = FastAPI()

# Configure a more tolerant httpx client (longer timeouts, increased connection limits)
httpx_client = httpx.AsyncClient(
    timeout=httpx.Timeout(30.0, connect=10.0),
    limits=httpx.Limits(max_keepalive_connections=20, max_connections=60),
    headers={"User-Agent": "bybit-macd-scanner/1.0"},
    trust_env=False,
)

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

# dedupe tracking for telegram messages (hash -> last_sent_ts)
last_telegram_sent: Dict[str, int] = {}

last_root_processed: Dict[str, int] = {}

public_ws = None
private_ws = None

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

# small helper for instance identification (useful for debugging and migrations)
INSTANCE_ID = os.getenv("INSTANCE_ID") or os.getenv("HOSTNAME") or f"inst-{os.getpid()}"

# ---------- Utilities ----------
def log(*args, **kwargs):
    if LOG_LEVEL != "none":
        ts = datetime.now(timezone.utc).isoformat()
        print(ts, *args, **kwargs)

def now_ts_ms() -> int:
    return int(time.time() * 1000)

# ---------- Telegram helpers ----------
async def send_telegram(text: str):
    """
    Deduplicate identical telegram text payloads sent within TELEGRAM_DEDUP_SECONDS seconds.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram not configured; skipping message:", text)
        return
    # compute simple hash of text
    key = hashlib.sha256(text.encode()).hexdigest()
    now_ts = int(time.time())
    last = last_telegram_sent.get(key)
    if last and (now_ts - last) < TELEGRAM_DEDUP_SECONDS:
        log("Telegram dedup skip (recently sent):", text[:120])
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    try:
        r = await httpx_client.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            log("Telegram send failed:", r.status_code, r.text)
        else:
            last_telegram_sent[key] = now_ts
    except Exception as e:
        log("Telegram error:", repr(e))

async def send_root_signals_telegram():
    """Send a concise Telegram message listing current root signals and TFs."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if not active_root_signals:
        await send_telegram("Root signals: 0")
        return
    # Normalize ordering so message hashes match across instances
    lines = ["Root signals:"]
    # sort by symbol then signal_type to keep stable order
    for sig in sorted(active_root_signals.values(), key=lambda s: (s.get("symbol") or "", s.get("signal_type") or "")):
        sym = sig.get("symbol")
        tf = sig.get("root_tf")
        status = sig.get("status", "watching")
        price = sig.get("root_flip_price")
        stype = sig.get("signal_type", "root")
        lines.append(f"- {sym} type={stype} root={tf} status={status} price={price}")
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

# ---------- Resilient public GET (improved logging + retry/backoff) ----------
async def resilient_public_get(endpoints: List[str], params: Dict[str, Any] = None, timeout: int = 12, retries: int = 3, backoff_base: float = 0.5) -> Optional[Dict[str, Any]]:
    """
    Try each host+endpoint pair. On network error or non-200, retry a few times with exponential backoff.
    Logs full exception repr to help diagnose network failures (DNS, TLS, connection refused, etc).
    """
    for host in API_HOSTS:
        for ep in endpoints:
            url = host + ep
            attempt = 0
            while attempt <= retries:
                try:
                    r = await httpx_client.get(url, params=params or {}, timeout=timeout)
                    if r.status_code == 200:
                        try:
                            return r.json()
                        except Exception:
                            log("Invalid JSON from", url, "body excerpt:", (r.text[:400] + "...") if r.text else "")
                            break
                    else:
                        body_excerpt = (r.text[:400] + "...") if r.text else ""
                        log(f"Public GET {url} returned {r.status_code} body_excerpt: {body_excerpt}")
                        # If 5xx allow retry with backoff, otherwise move on
                        if 500 <= r.status_code < 600:
                            attempt += 1
                            wait = backoff_base * (2 ** (attempt - 1))
                            log(f"Retrying {url} after {wait}s due to {r.status_code}")
                            await asyncio.sleep(wait)
                            continue
                        else:
                            break
                except Exception as e:
                    # log the full exception (repr) for easier diagnosis in your logs
                    log(f"Network error for {url} -> {repr(e)} (attempt {attempt+1}/{retries+1})")
                    attempt += 1
                    if attempt > retries:
                        break
                    wait = backoff_base * (2 ** (attempt - 1))
                    await asyncio.sleep(wait)
            # end attempts loop for this endpoint
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
        log("Signed request error:", repr(e))
        return {}

# ---------- SQLite init & helpers & startup migrations ----------
async def init_db():
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("""CREATE TABLE IF NOT EXISTS root_signals (
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
    )""")
    await db.execute("""CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY,
        symbol TEXT,
        side TEXT,
        qty REAL,
        entry_price REAL,
        sl_price REAL,
        created_at INTEGER,
        open BOOLEAN,
        raw_response TEXT
    )""")
    await db.execute("""CREATE TABLE IF NOT EXISTS raw_ws_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, topic TEXT, message TEXT, created_at INTEGER)""")
    await db.execute("""CREATE TABLE IF NOT EXISTS public_subscriptions (topic TEXT PRIMARY KEY, created_at INTEGER)""")
    # unique constraint: one active root signal per (symbol, signal_type) is helpful as a safety net
    await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_root_signals_symbol_type ON root_signals(symbol, signal_type)")
    await db.commit()
    log("DB initialized at", DB_PATH)
    # Run runtime migrations (add columns and dedupe persisted duplicates)
    await run_db_migrations()

async def run_db_migrations():
    """
    Make non-destructive schema additions and dedupe existing duplicates on startup.
    This runs automatically so you don't need shell access on Render.
    """
    try:
        # add instance_id and last_notified_at columns if missing
        cols = []
        async with db.execute("PRAGMA table_info(root_signals)") as cur:
            async for row in cur:
                cols.append(row[1])
        if "instance_id" not in cols:
            try:
                await db.execute("ALTER TABLE root_signals ADD COLUMN instance_id TEXT DEFAULT 'unknown'")
                log("Added column root_signals.instance_id")
            except Exception as e:
                log("Could not add instance_id to root_signals:", repr(e))
        if "last_notified_at" not in cols:
            try:
                await db.execute("ALTER TABLE root_signals ADD COLUMN last_notified_at INTEGER DEFAULT NULL")
                log("Added column root_signals.last_notified_at")
            except Exception as e:
                log("Could not add last_notified_at to root_signals:", repr(e))

        cols_tr = []
        async with db.execute("PRAGMA table_info(trades)") as cur:
            async for row in cur:
                cols_tr.append(row[1])
        if "instance_id" not in cols_tr:
            try:
                await db.execute("ALTER TABLE trades ADD COLUMN instance_id TEXT DEFAULT 'unknown'")
                log("Added column trades.instance_id")
            except Exception as e:
                log("Could not add instance_id to trades:", repr(e))
        await db.commit()
    except Exception as e:
        log("run_db_migrations introspection error:", repr(e))

    # Dedupe persisted root_signals: keep earliest created_at per (symbol, signal_type, status)
    try:
        dup_query = "SELECT symbol, signal_type, status, COUNT(*) FROM root_signals GROUP BY symbol, signal_type, status HAVING COUNT(*) > 1"
        groups = []
        async with db.execute(dup_query) as cur:
            async for row in cur:
                groups.append((row[0], row[1], row[2]))
        if groups:
            log("Dedupe startup: cleaning", len(groups), "groups")
            for symbol, stype, status in groups:
                sel = "SELECT rowid, id, created_at FROM root_signals WHERE symbol = ? AND signal_type = ? AND status = ? ORDER BY created_at ASC, rowid ASC"
                rows = []
                async with db.execute(sel, (symbol, stype, status)) as cur:
                    async for r in cur:
                        rows.append(r)
                if len(rows) <= 1:
                    continue
                keep_rowid = rows[0][0]
                del_sql = "DELETE FROM root_signals WHERE symbol = ? AND signal_type = ? AND status = ? AND rowid != ?"
                await db.execute(del_sql, (symbol, stype, status, keep_rowid))
                log(f"Dedupe: kept rowid {keep_rowid} for {symbol}/{stype}/{status}, removed {len(rows)-1} rows")
            await db.commit()
            try:
                await db.execute("VACUUM")
            except Exception:
                pass
        else:
            log("Dedupe: no duplicate root_signals found on startup")
    except Exception as e:
        log("Dedupe error:", repr(e))

# ---------- Atomic DB creation helpers ----------
async def try_create_signal_atomic(sig: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]]]:
    comps = json.dumps(sig.get("components") or [])
    symbol = sig["symbol"]
    stype = sig.get("signal_type", "root")
    status = sig.get("status", "watching")
    try:
        await db.execute("BEGIN IMMEDIATE")
        async with db.execute("SELECT id, created_at FROM root_signals WHERE symbol = ? AND signal_type = ? AND status = ? LIMIT 1", (symbol, stype, status)) as cur:
            row = await cur.fetchone()
        if row:
            await db.execute("ROLLBACK")
            return False, {"id": row[0], "created_at": row[1]}
        try:
            await db.execute(
                "INSERT INTO root_signals (id, symbol, root_tf, flip_time, flip_price, status, priority, signal_type, components, created_at, instance_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (sig["id"], symbol, sig.get("root_tf"), sig.get("root_flip_time") or sig.get("flip_time"),
                 sig.get("root_flip_price") or sig.get("flip_price"), status, sig.get("priority"),
                 stype, comps, sig["created_at"], INSTANCE_ID)
            )
        except Exception:
            await db.execute(
                "INSERT INTO root_signals (id, symbol, root_tf, flip_time, flip_price, status, priority, signal_type, components, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (sig["id"], symbol, sig.get("root_tf"), sig.get("root_flip_time") or sig.get("flip_time"),
                 sig.get("root_flip_price") or sig.get("flip_price"), status, sig.get("priority"),
                 stype, comps, sig["created_at"])
            )
        await db.execute("COMMIT")
        return True, sig
    except Exception as e:
        try:
            await db.execute("ROLLBACK")
        except Exception:
            pass
        log("try_create_signal_atomic error:", repr(e))
        return False, None

async def try_create_trade_atomic(tr: Dict[str, Any]) -> bool:
    try:
        await db.execute("BEGIN IMMEDIATE")
        async with db.execute("SELECT 1 FROM trades WHERE symbol = ? AND open = 1 LIMIT 1", (tr["symbol"],)) as cur:
            row = await cur.fetchone()
        if row:
            await db.execute("ROLLBACK")
            return False
        try:
            await db.execute("INSERT INTO trades (id,symbol,side,qty,entry_price,sl_price,created_at,open,raw_response,instance_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
                             (tr["id"], tr["symbol"], tr["side"], tr["qty"], tr.get("entry_price"), tr.get("sl_price"), tr["created_at"], 1, json.dumps(tr.get("raw") or {}), INSTANCE_ID))
        except Exception:
            await db.execute("INSERT INTO trades (id,symbol,side,qty,entry_price,sl_price,created_at,open,raw_response) VALUES (?,?,?,?,?,?,?,?,?)",
                             (tr["id"], tr["symbol"], tr["side"], tr["qty"], tr.get("entry_price"), tr.get("sl_price"), tr["created_at"], 1, json.dumps(tr.get("raw") or {})))
        await db.execute("COMMIT")
        return True
    except Exception as e:
        try:
            await db.execute("ROLLBACK")
        except Exception:
            pass
        log("try_create_trade_atomic error:", repr(e))
        return False

# ---------- Other DB helpers (persist_raw_ws etc.) ----------
async def remove_root_signal(sig_id: str):
    await db.execute("DELETE FROM root_signals WHERE id = ?", (sig_id,))
    await db.commit()

async def persist_root_signal(sig: Dict[str, Any]):
    comps = json.dumps(sig.get("components") or [])
    try:
        await db.execute("""
            INSERT INTO root_signals (id, symbol, root_tf, flip_time, flip_price, status, priority, signal_type, components, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, signal_type) DO UPDATE SET
                id = excluded.id,
                root_tf = excluded.root_tf,
                flip_time = excluded.flip_time,
                flip_price = excluded.flip_price,
                status = excluded.status,
                priority = excluded.priority,
                components = excluded.components,
                created_at = excluded.created_at
        """, (sig["id"], sig["symbol"], sig.get("root_tf"), sig.get("root_flip_time") or sig.get("flip_time"), sig.get("root_flip_price") or sig.get("flip_price"), sig.get("status", "watching"), sig.get("priority"), sig.get("signal_type", "root"), comps, sig["created_at"]))
        await db.commit()
    except Exception as e:
        log("persist_root_signal error:", repr(e))
        raise

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

# ---------- ensure_cached_candles (restored) ----------
async def ensure_cached_candles(symbol: str, tf: str, required: int):
    """
    Ensure the candles cache for (symbol, tf) contains at least `required` candles.
    Fetches and merges if needed.
    """
    token = TF_MAP.get(tf, tf)
    dq = cache_get(symbol, token)
    if dq and len(dq) >= required and not cache_needs_refresh(symbol, token):
        return
    try:
        fetched = await fetch_klines(symbol, token, limit=max(required * 2, required + 50))
        if fetched:
            merge_into_cache(symbol, token, fetched)
            log(f"Updated cache for {symbol} {tf}: now {len(cache_get(symbol, token) or [])} candles")
    except Exception as e:
        log("ensure_cached_candles fetch error:", repr(e))

# ---------- Helper functions (robust instruments fetch, caching, macd, detect_flip, etc.) ----------
async def get_tradable_usdt_symbols() -> List[str]:
    global symbols_info_cache
    found = []
    last_resp_excerpt = None
    for ep in INSTRUMENTS_ENDPOINTS:
        url = PRIMARY_API_HOST + ep
        try:
            r = await httpx_client.get(url, timeout=12)
        except Exception as e:
            log("Network error getting instruments from", url, "->", repr(e))
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
    sorted_list = sorted(list(dq), key=lambda x: x["start"])
    dq = deque(sorted_list, maxlen=CANDLE_CACHE_MAX)
    cache_set(symbol, interval_token, dq)

async def update_candles_cache_from_fetch(symbol: str, interval_token: str, limit: int):
    klines = await fetch_klines(symbol, interval_token, limit=limit)
    if not klines:
        return
    merge_into_cache(symbol, interval_token, klines)

def candles_to_closes(dq: deque, last_n: Optional[int] = None) -> List[float]:
    arr = list(dq)
    if last_n:
        arr = arr[-last_n:]
    return [x["close"] for x in arr if x.get("close") is not None]

async def fetch_klines(symbol: str, interval_token: str, limit: int = 200) -> List[Dict[str, Any]]:
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

async def detect_flip(symbol: str, tf: str) -> Tuple[Optional[str], Optional[int]]:
    token = TF_MAP[tf]
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
        return None, None
    prev = hist[-2] or 0
    cur = hist[-1] or 0
    if prev <= 0 and cur > 0:
        return "open", last_start
    return None, None

# ---------- Scanning & evaluation loops (root, super, evaluate) ----------
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
    lock = get_symbol_lock(sym)
    async with lock:
        if signal_exists_for(sym, stype):
            log("Duplicate signal suppressed (in-memory):", sym, stype)
            return False
        created, existing = await try_create_signal_atomic(sig)
        if not created:
            if existing:
                log("Duplicate signal suppressed (db existing):", sym, stype, "existing_id:", existing.get("id"))
            else:
                log("Duplicate signal suppressed (db insert failed):", sym, stype)
            return False
        active_root_signals[sid] = sig
        register_signal_index(sym, stype)
        log("Added signal (atomic):", sid, stype, sym, "instance:", INSTANCE_ID)
        try:
            await send_root_signals_telegram()
        except Exception as e:
            log("send_root_signals_telegram error:", repr(e))
        return True

async def remove_signal(sig_id: str):
    sig = active_root_signals.pop(sig_id, None)
    if not sig:
        return
    unregister_signal_index(sig["symbol"], sig.get("signal_type", "root"))
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

def chunked(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]

async def root_scanner_loop(root_tf: str):
    log("Root scanner started for", root_tf)
    while True:
        try:
            symbols = await get_tradable_usdt_symbols()
            if symbols:
                try:
                    CHUNK = 12
                    for chunk in chunked(symbols, CHUNK):
                        await asyncio.gather(*(ensure_cached_candles(symbol, root_tf, MIN_CANDLES_REQUIRED) for symbol in chunk))
                        await asyncio.sleep(0.25)
                except Exception as e:
                    log("Prewarm error:", repr(e))
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
                    key = f"ROOT-{symbol}-{root_tf}-{last_start}-{uuid.uuid4().hex[:8]}"
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
                        log("Root flip detected and added:", symbol, root_tf, last_start, "flip_kind:", flip_kind)
                        if public_ws:
                            await public_ws.subscribe_kline(symbol, TF_MAP["5m"])
                            await public_ws.subscribe_kline(symbol, TF_MAP["15m"])
                except Exception as e:
                    log("Error scanning symbol", symbol, "for root", root_tf, "->", repr(e))
                    continue
        except Exception as e:
            log("root_scanner_loop outer error:", repr(e))
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

async def evaluate_signals_loop():
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
                    entry_id = f"ENTRY-{symbol}-{ts}-{uuid.uuid4().hex[:6]}"
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
                                    trade_obj = {"id": oid, "symbol": symbol, "side": side, "qty": qty, "entry_price": None, "sl_price": stop_price, "created_at": now_ts_ms(), "open": True, "raw": res}
                                    created = await try_create_trade_atomic(trade_obj)
                                    if created:
                                        log("Placed real order and persisted", symbol, "order_oid:", oid)
                                        await send_telegram(f"Placed real order {symbol} qty={qty} side={side}")
                                    else:
                                        log("Real order persistence skipped because open trade exists for", symbol)
                                except Exception as e:
                                    log("Error placing real order:", repr(e))
                            else:
                                oid = str(uuid.uuid4())
                                trade_obj = {"id": oid, "symbol": symbol, "side": side, "qty": qty, "entry_price": None, "sl_price": stop_price, "created_at": now_ts_ms(), "open": True, "raw": {"simulated": True}}
                                created = await try_create_trade_atomic(trade_obj)
                                if created:
                                    log("Simulated trade recorded for", symbol, "qty", qty)
                                    await send_telegram(f"Simulated trade for {symbol} qty={qty} side={side} (entry)")
                                else:
                                    log("Simulated trade aborted - open trade already exists for", symbol)
                        if entry_id in active_root_signals:
                            active_root_signals[entry_id]["status"] = "acted"
                            try:
                                await persist_root_signal(active_root_signals[entry_id])
                            except Exception:
                                pass
                except Exception as e:
                    log("evaluate_signals_loop error for", sid, repr(e))
            await asyncio.sleep(max(5, SCAN_INTERVAL_SECONDS // 2))
        except Exception as e:
            log("evaluate_signals_loop outer error:", repr(e))
            await asyncio.sleep(5)

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
                        ts = int(time.time())
                        key = f"SUPER-{symbol}-{ts}-{uuid.uuid4().hex[:6]}"
                        sig = {
                            "id": key,
                            "symbol": symbol,
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
                            log("SUPER signal detected for", symbol, "components:", sig["components"])
        except Exception as e:
            log("super_signal_scanner_loop_5m error:", repr(e))
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

# ---------- Startup helpers and logging ----------
async def log_current_root_signals():
    """
    Log a compact list of current in-memory root signals.
    Called periodically by the periodic_root_logger background task.
    """
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
        log("log_current_root_signals error:", repr(e))

async def load_persisted_root_signals():
    try:
        async with db.execute("SELECT id,symbol,root_tf,flip_time,flip_price,status,priority,signal_type,components,created_at FROM root_signals") as cur:
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
                signal_type = r[7] or "root"
                comps = []
                try:
                    comps = json.loads(r[8]) if r[8] else []
                except Exception:
                    comps = []
                created_at = r[9] or now_ts_ms()
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
            except Exception:
                continue
        log("Loaded", len(rows), "persisted root signals from DB")
    except Exception as e:
        log("load_persisted_root_signals error:", repr(e))

# ---------- Debug endpoints: DB diagnostics & cleanup ----------
@app.get("/debug/db/duplicates")
async def debug_db_duplicates(_auth=Depends(require_admin_auth)):
    """
    Returns groups where root_signals has more than one row for the same (symbol, signal_type, status)
    """
    out = []
    try:
        async with db.execute("SELECT symbol, signal_type, status, COUNT(*) as c FROM root_signals GROUP BY symbol, signal_type, status HAVING c>1") as cur:
            async for row in cur:
                out.append({"symbol": row[0], "signal_type": row[1], "status": row[2], "count": row[3]})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {repr(e)}")
    return {"duplicates": out}

@app.get("/debug/db/signals")
async def debug_db_signals(symbol: str = Query(..., min_length=1), _auth=Depends(require_admin_auth)):
    """
    Returns all persisted root_signals rows for a symbol (includes rowid, id, fields), ordered by created_at.
    """
    try:
        rows_out = []
        q = "SELECT rowid, id, symbol, root_tf, flip_time, flip_price, status, priority, signal_type, components, created_at, instance_id, last_notified_at FROM root_signals WHERE symbol = ? ORDER BY created_at ASC"
        async with db.execute(q, (symbol,)) as cur:
            async for r in cur:
                rows_out.append({
                    "rowid": r[0],
                    "id": r[1],
                    "symbol": r[2],
                    "root_tf": r[3],
                    "flip_time": r[4],
                    "flip_price": r[5],
                    "status": r[6],
                    "priority": r[7],
                    "signal_type": r[8],
                    "components": json.loads(r[9]) if r[9] else [],
                    "created_at": r[10],
                    "instance_id": r[11] if len(r) > 11 else None,
                    "last_notified_at": r[12] if len(r) > 12 else None
                })
        return {"symbol": symbol, "rows": rows_out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {repr(e)}")

@app.post("/debug/db/clean")
async def debug_db_clean(symbol: str = Query(..., min_length=1), _auth=Depends(require_admin_auth)):
    """
    Remove duplicate root_signals rows for a symbol (keeps earliest created_at).
    Returns summary of removed rows.
    """
    try:
        # find all rows for symbol grouped by (signal_type, status)
        deleted_total = 0
        details = []
        groups_q = "SELECT signal_type, status FROM root_signals WHERE symbol = ? GROUP BY signal_type, status"
        async with db.execute(groups_q, (symbol,)) as cur:
            async for g in cur:
                stype = g[0]; status = g[1]
                sel = "SELECT rowid, id, created_at FROM root_signals WHERE symbol = ? AND signal_type = ? AND status = ? ORDER BY created_at ASC, rowid ASC"
                rows = []
                async with db.execute(sel, (symbol, stype, status)) as c2:
                    async for r in c2:
                        rows.append(r)
                if len(rows) <= 1:
                    continue
                keep_rowid = rows[0][0]
                to_remove = [r[0] for r in rows[1:]]
                del_sql = "DELETE FROM root_signals WHERE rowid IN ({})".format(",".join("?" for _ in to_remove))
                await db.execute(del_sql, tuple(to_remove))
                deleted_total += len(to_remove)
                details.append({"symbol": symbol, "signal_type": stype, "status": status, "kept_rowid": keep_rowid, "removed_count": len(to_remove), "removed_rowids": to_remove})
        await db.commit()
        try:
            await db.execute("VACUUM")
        except Exception:
            pass
        return {"symbol": symbol, "deleted_total": deleted_total, "details": details}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {repr(e)}")

# ---------- Existing debug endpoints (kept) ----------
@app.get("/debug/current_roots")
async def debug_current_roots(_auth=Depends(require_admin_auth)):
    return {"count": len(active_root_signals), "signals": list(active_root_signals.values())}

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
        "sample_last_klines": list(dq)[-5:],
    }

# Simple root/health endpoints so pinging the server shows it is up
@app.get("/")
async def root():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat(), "instance": INSTANCE_ID}

@app.get("/health")
async def health():
    db_ok = db is not None
    public_ws_connected = False
    try:
        public_ws_connected = bool(public_ws and getattr(public_ws, "conn", None))
    except Exception:
        public_ws_connected = False
    return {"status": "ok", "db": db_ok, "public_ws_connected": public_ws_connected, "instance": INSTANCE_ID, "time": datetime.now(timezone.utc).isoformat()}

# ---------- Startup & background tasks ----------
@app.on_event("startup")
async def startup():
    global public_ws, private_ws
    await init_db()
    await load_persisted_root_signals()
    log("Startup config:", "BYBIT_USE_MAINNET=", BYBIT_USE_MAINNET, "TRADING_ENABLED=", TRADING_ENABLED, "PUBLIC_WS_URL=", PUBLIC_WS_URL, "MIN_CANDLES_REQUIRED=", MIN_CANDLES_REQUIRED, "INSTANCE_ID=", INSTANCE_ID)
    public_ws = PublicWebsocketManager(PUBLIC_WS_URL)
    try:
        ok = await public_ws.connect_and_detect(timeout=6.0)
        if not ok:
            log("Public WS detect warning")
    except Exception:
        log("Public WS connect error")
    # Start background tasks (original behavior preserved)
    asyncio.create_task(root_scanner_loop("1h"))
    asyncio.create_task(root_scanner_loop("4h"))
    asyncio.create_task(super_signal_scanner_loop_5m())
    asyncio.create_task(evaluate_signals_loop())
    async def periodic_root_logger():
        while True:
            try:
                await log_current_root_signals()
                if active_root_signals:
                    await send_root_signals_telegram()
            except Exception as e:
                log("periodic_root_logger error:", repr(e))
            await asyncio.sleep(max(30, ROOT_SIGNALS_LOG_INTERVAL))
    asyncio.create_task(periodic_root_logger())
    log("Background tasks started")

# ---------- Run note ----------
# Start with:
# uvicorn main:app --host 0.0.0.0 --port $PORT
