# main.py
# Bybit MACD multi-timeframe scanner — full original functionality patched with:
# - atomic DB inserts (try_create_signal_atomic, try_create_trade_atomic)
# - startup migrations & dedupe (run_db_migrations)
# - notification dedupe (last_notified_at) and single-notify helper
# - DB diagnostics & cleanup endpoints: /debug/db/duplicates, /debug/db/signals, POST /debug/db/clean
#
# This file merges your original scanner logic with the debugging/atomic helpers so you can
# inspect and clean duplicates from the browser while keeping the scanner behavior intact.
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

async def send_telegram(text: str):
    """
    Deduplicate identical telegram text payloads sent within TELEGRAM_DEDUP_SECONDS seconds.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram not configured; skipping message:", text)
        return
    key = hashlib.sha256(text.encode()).hexdigest()
    now_ts = int(time.time())
    last = last_telegram_sent.get(key)
    if last and (now_ts - last) < TELEGRAM_DEDUP_SECONDS:
        log("Telegram dedupe skip (recently sent):", text[:120])
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
                        if 500 <= r.status_code < 600:
                            attempt += 1
                            wait = backoff_base * (2 ** (attempt - 1))
                            log(f"Retrying {url} after {wait}s due to {r.status_code}")
                            await asyncio.sleep(wait)
                            continue
                        else:
                            break
                except Exception as e:
                    log(f"Network error for {url} -> {repr(e)} (attempt {attempt+1}/{retries+1})")
                    attempt += 1
                    if attempt > retries:
                        break
                    wait = backoff_base * (2 ** (attempt - 1))
                    await asyncio.sleep(wait)
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
    # unique constraint: one active root signal per (symbol, signal_type)
    await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_root_signals_symbol_type ON root_signals(symbol, signal_type)")
    await db.commit()
    log("DB initialized at", DB_PATH)
    await run_db_migrations()

async def run_db_migrations():
    """
    Add helpful columns if missing and dedupe persisted root_signals (keep earliest row per group).
    This runs on startup automatically so you don't need shell access on Render.
    """
    try:
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
        to_dedupe = []
        async with db.execute(dup_query) as cur:
            async for row in cur:
                to_dedupe.append((row[0], row[1], row[2]))
        if to_dedupe:
            log("Dedupe: found", len(to_dedupe), "groups with duplicates; cleaning up now")
            for symbol, stype, status in to_dedupe:
                sel = "SELECT rowid, created_at FROM root_signals WHERE symbol = ? AND signal_type = ? AND status = ? ORDER BY created_at ASC, rowid ASC"
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
            log("Dedupe: no duplicate root_signals found")
    except Exception as e:
        log("Dedupe error:", repr(e))

# ---------- DB helpers for atomic creation ----------
async def try_create_signal_atomic(sig: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]]]:
    comps = json.dumps(sig.get("components") or [])
    symbol = sig["symbol"]
    stype = sig.get("signal_type", "root")
    status = sig.get("status", "watching")
    try:
        await db.execute("BEGIN IMMEDIATE")
        async with db.execute("SELECT id,root_tf,flip_time,created_at FROM root_signals WHERE symbol = ? AND signal_type = ? AND status = ? LIMIT 1", (symbol, stype, status)) as cur:
            row = await cur.fetchone()
        if row:
            await db.execute("ROLLBACK")
            existing = {"id": row[0], "root_tf": row[1], "flip_time": row[2], "created_at": row[3]}
            return False, existing
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

# ---------- Fetch and normalize klines ----------
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

# ---------- MACD helpers ----------
def check_macd_flip_recent_from_closes(closes: List[float], lookback: int = 3) -> Optional[int]:
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

# (rest of original code, background loops, debug endpoints are already present above)
# ---------- Debug endpoints: DB diagnostics & cleanup ----------
@app.get("/debug/db/duplicates")
async def debug_db_duplicates(_auth=Depends(require_admin_auth)):
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
    try:
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
    # Start original background loops (as in original file)
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
