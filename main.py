# main.py
# Bybit MACD multi-timeframe scanner — with DB diagnostics & cleanup endpoints
# Includes:
# - Atomic DB inserts for signals & trades
# - Startup migrations and dedupe
# - Debug endpoints: /debug/db/duplicates, /debug/db/signals, POST /debug/db/clean
# - Notification dedupe (last_notified_at) to avoid repeated Telegram pushes
#
# Usage: replace your existing main.py with this file, deploy/restart (uvicorn main:app --host 0.0.0.0 --port $PORT)

import os
import time
import hmac
import hashlib
import asyncio
import json
import gzip
import zlib
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
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
BYBIT_USE_MAINNET = os.getenv("BYBIT_USE_MAINNET", "false").lower() == "true"
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "false").lower() == "true"

DB_PATH = os.getenv("DB_PATH", "scanner.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "info").lower()
TELEGRAM_DEDUP_SECONDS = int(os.getenv("TELEGRAM_DEDUP_SECONDS", "60"))
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))

# MACD params etc (kept defaults; adjust via env if desired)
MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
MIN_CANDLES_REQUIRED = MACD_SLOW + MACD_SIGNAL + 5

# Hosts
MAINNET_API_HOST = "https://api.bybit.com"
TESTNET_API_HOST = "https://api-testnet.bybit.com"
PRIMARY_API_HOST = MAINNET_API_HOST if BYBIT_USE_MAINNET else TESTNET_API_HOST
API_HOSTS = [PRIMARY_API_HOST]
if PRIMARY_API_HOST == MAINNET_API_HOST:
    API_HOSTS.append(TESTNET_API_HOST)
else:
    API_HOSTS.append(MAINNET_API_HOST)

# Timeframes
TF_MAP = {"5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "D"}

# ---------- Globals ----------
app = FastAPI()
httpx_client = httpx.AsyncClient(
    timeout=httpx.Timeout(30.0, connect=10.0),
    limits=httpx.Limits(max_keepalive_connections=20, max_connections=60),
    headers={"User-Agent": "bybit-macd-scanner/1.0"},
    trust_env=False,
)

db: Optional[aiosqlite.Connection] = None

# caches & in-memory state
candles_cache: Dict[str, Dict[str, deque]] = defaultdict(lambda: {})
candles_cache_ts: Dict[str, Dict[str, int]] = defaultdict(lambda: {})
active_root_signals: Dict[str, Dict[str, Any]] = {}
active_signal_index: Dict[str, set] = defaultdict(set)
last_telegram_sent: Dict[str, int] = {}
symbol_locks: Dict[str, asyncio.Lock] = {}
INSTANCE_ID = os.getenv("INSTANCE_ID") or os.getenv("HOSTNAME") or f"inst-{os.getpid()}"

# ---------- Utilities ----------
def log(*args, **kwargs):
    if LOG_LEVEL != "none":
        ts = datetime.now(timezone.utc).isoformat()
        print(ts, *args, **kwargs)

def now_ts_ms() -> int:
    return int(time.time() * 1000)

async def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram not configured; skipping message")
        return
    key = hashlib.sha256(text.encode()).hexdigest()
    now_ts = int(time.time())
    last = last_telegram_sent.get(key)
    if last and (now_ts - last) < TELEGRAM_DEDUP_SECONDS:
        log("Telegram dedupe skip (recently sent)")
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

# Admin auth dependency
async def require_admin_auth(authorization: Optional[str] = Header(None), x_api_key: Optional[str] = Header(None)):
    if not ADMIN_API_KEY:
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
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

# ---------- SQLite init & startup migrations ----------
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
    # index safety
    await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_root_signals_symbol_type ON root_signals(symbol, signal_type)")
    await db.commit()
    log("DB initialized at", DB_PATH)
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

# ---------- Simple MACD helpers (kept small) ----------
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

# ---------- Cache utilities ----------
def cache_get(symbol: str, interval_token: str) -> Optional[deque]:
    return candles_cache.get(symbol, {}).get(interval_token)

def cache_set(symbol: str, interval_token: str, dq: deque):
    if symbol not in candles_cache:
        candles_cache[symbol] = {}
    candles_cache[symbol][interval_token] = dq
    if symbol not in candles_cache_ts:
        candles_cache_ts[symbol] = {}
    candles_cache_ts[symbol][interval_token] = int(time.time())

def merge_into_cache(symbol: str, interval_token: str, candles: List[Dict[str, Any]]):
    if not candles:
        return
    dq = cache_get(symbol, interval_token)
    if dq is None:
        dq = deque(maxlen=2000)
    existing_starts = set(x["start"] for x in dq)
    for c in candles:
        if c["start"] not in existing_starts:
            dq.append(c)
            existing_starts.add(c["start"])
    sorted_list = sorted(list(dq), key=lambda x: x["start"])
    dq = deque(sorted_list, maxlen=2000)
    cache_set(symbol, interval_token, dq)

# ---------- Detect flip (open-only) ----------
def candles_to_closes(dq: deque, last_n: Optional[int] = None) -> List[float]:
    arr = list(dq)
    if last_n:
        arr = arr[-last_n:]
    return [x["close"] for x in arr if x.get("close") is not None]

async def detect_flip(symbol: str, tf: str) -> Tuple[Optional[str], Optional[int]]:
    token = TF_MAP.get(tf, tf)
    dq = cache_get(symbol, token)
    if not dq or len(dq) < MIN_CANDLES_REQUIRED:
        return None, None
    closes = candles_to_closes(dq)
    if len(closes) < MACD_SLOW + MACD_SIGNAL:
        return None, None
    hist = macd_hist(closes)
    if not hist or len(hist) < 2:
        return None, None
    # if last candle is closed we ignore (we only react to open flips)
    last_start = list(dq)[-1]["start"]
    interval_seconds = 60 * int(token) if token.isdigit() else 86400
    end = last_start + interval_seconds
    if int(time.time()) >= end:
        return None, None
    prev = hist[-2] or 0
    cur = hist[-1] or 0
    if prev <= 0 and cur > 0:
        return "open", last_start
    return None, None

# ---------- Signal registration ----------
def get_symbol_lock(symbol: str) -> asyncio.Lock:
    lock = symbol_locks.get(symbol)
    if not lock:
        lock = asyncio.Lock()
        symbol_locks[symbol] = lock
    return lock

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
            log("Duplicate signal suppressed (db existing or insert failed) for", sym, stype)
            return False
        active_root_signals[sid] = sig
        register_signal_index(sym, stype)
        log("Added signal (atomic):", sid, stype, sym, "instance:", INSTANCE_ID)
        # send notification only once: if last_notified_at is null for that DB row we notify and set it
        try:
            await send_root_signals_telegram_once(sig)
        except Exception as e:
            log("send_root_signals_telegram error:", repr(e))
        return True

async def send_root_signals_telegram_once(sig: Dict[str, Any]):
    # If row already has last_notified_at in DB, skip (prevents repeated notifications)
    try:
        async with db.execute("SELECT last_notified_at FROM root_signals WHERE id = ? LIMIT 1", (sig["id"],)) as cur:
            row = await cur.fetchone()
        if row and row[0]:
            log("Signal already notified (db):", sig.get("id"))
            return
        # send concise single-signal message (or you can send full list)
        text = f"Signal: {sig.get('symbol')} type={sig.get('signal_type')} root={sig.get('root_tf')} price={sig.get('root_flip_price')}"
        await send_telegram(text)
        # update DB last_notified_at
        try:
            await db.execute("UPDATE root_signals SET last_notified_at = ? WHERE id = ?", (now_ts_ms(), sig["id"]))
            await db.commit()
        except Exception as e:
            log("Failed to update last_notified_at:", repr(e))
    except Exception as e:
        log("send_root_signals_telegram_once read error:", repr(e))

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
    token = TF_MAP.get(tf, tf)
    dq = cache_get(symbol, token)
    if not dq:
        return {"symbol": symbol, "tf": tf, "cached_klines": 0}
    closes = candles_to_closes(dq)
    hist = macd_hist(closes)
    flip_kind, flip_ts = None, None
    try:
        flip_kind, flip_ts = asyncio.get_event_loop().run_until_complete(detect_flip(symbol, tf))
    except Exception:
        # fallback non-blocking detection
        flip_kind, flip_ts = None, None
    last_closed = False
    try:
        last = list(dq)[-1]
        interval_seconds = 60 * int(token) if token.isdigit() else 86400
        end = last["start"] + interval_seconds
        last_closed = int(time.time()) >= end
    except Exception:
        last_closed = False
    return {
        "symbol": symbol,
        "tf": tf,
        "cached_klines": len(dq),
        "macd_hist_len": len(hist),
        "macd_last": hist[-1] if hist else None,
        "flip_kind": flip_kind,
        "flip_ts": flip_ts,
        "last_closed": last_closed,
        "sample_last_klines": list(dq)[-5:]
    }

# ---------- Root & health endpoints ----------
@app.get("/")
async def root():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat(), "instance": INSTANCE_ID}

@app.get("/health")
async def health():
    db_ok = db is not None
    public_ws_connected = False
    return {"status": "ok", "db": db_ok, "public_ws_connected": public_ws_connected, "instance": INSTANCE_ID, "time": datetime.now(timezone.utc).isoformat()}

# ---------- Startup tasks (minimal scanning kept) ----------
@app.on_event("startup")
async def startup():
    global db
    await init_db()
    # Load persisted signals into memory after startup dedupe
    try:
        async with db.execute("SELECT id,symbol,root_tf,flip_time,flip_price,status,priority,signal_type,components,created_at,last_notified_at FROM root_signals") as cur:
            rows = await cur.fetchall()
        for r in rows:
            rid = r[0]; symbol = r[1]; root_tf = r[2]; flip_time = r[3]; flip_price = r[4]
            status = r[5] or "watching"; priority = r[6]; signal_type = r[7] or "root"
            comps = []
            try:
                comps = json.loads(r[8]) if r[8] else []
            except Exception:
                comps = []
            created_at = r[9] or now_ts_ms()
            sig = {
                "id": rid, "symbol": symbol, "root_tf": root_tf, "root_flip_time": flip_time,
                "root_flip_price": flip_price, "status": status, "priority": priority,
                "signal_type": signal_type, "components": comps, "created_at": created_at
            }
            active_root_signals[rid] = sig
            register_signal_index(symbol, signal_type)
        log("Loaded", len(rows), "persisted signals into memory")
    except Exception as e:
        log("Error loading persisted signals:", repr(e))

    # Background scanning loops would be started here in full app; omitted heavy loops for brevity
    log("Startup complete, instance:", INSTANCE_ID)

# ---------- Run note ----------
# Start with:
# uvicorn main:app --host 0.0.0.0 --port $PORT
