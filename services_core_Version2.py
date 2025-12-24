# services_core.py
# Core helpers: configuration, DB, candle cache, resilient requests,
# persistence, telegram queue, signal add/remove and endpoints/startup glue.

import os
import time
import hmac
import hashlib
import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Callable
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
DB_PATH = os.getenv("DB_PATH", "scanner.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "info").lower()

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
MAX_RAW_WS_MESSAGES = int(os.getenv("MAX_RAW_WS_MESSAGES", "1000"))

PUBLIC_REQ_CONCURRENCY = int(os.getenv("PUBLIC_REQ_CONCURRENCY", "8"))
PUBLIC_REQ_RETRIES = int(os.getenv("PUBLIC_REQ_RETRIES", "3"))

DISCOVERY_CONCURRENCY = int(os.getenv("DISCOVERY_CONCURRENCY", "24"))
ROOT_DEDUP_SECONDS = int(os.getenv("ROOT_DEDUP_SECONDS", "300"))
FLIP_STABILITY_SECONDS = int(os.getenv("FLIP_STABILITY_SECONDS", "0"))

TELEGRAM_WORKER_CONCURRENCY = 1
TELEGRAM_RETRY_LIMIT = 4

CANDLE_CACHE_MAX = int(os.getenv("CANDLE_CACHE_MAX", "2000"))
CANDLE_CACHE_TTL = int(os.getenv("CANDLE_CACHE_TTL", "300"))

# MTF monitoring interval (mtf module uses this)
MTF_MONITORING_INTERVAL = int(os.getenv("MTF_MONITORING_INTERVAL", "5"))  # default 5s

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
]
KLINE_ENDPOINTS = ["/v5/market/kline", "/v2/public/kline/list", "/v2/public/kline"]

TF_MAP = {"5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "D"}
REVERSE_TF_MAP = {v: k for k, v in TF_MAP.items()}

MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
MIN_CANDLES_REQUIRED = MACD_SLOW + MACD_SIGNAL + 5

LEVERAGE = int(os.getenv("LEVERAGE", "3"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.015"))
STABLECOINS = {"USDT", "BUSD", "USDC", "TUSD", "DAI"}

# ---------- Globals ----------
app = None
httpx_client = httpx.AsyncClient(timeout=20)
db: Optional[aiosqlite.Connection] = None

symbols_info_cache: Dict[str, Dict[str, Any]] = {}

candles_cache: Dict[str, Dict[str, deque]] = defaultdict(lambda: {})
candles_cache_ts: Dict[str, Dict[str, float]] = defaultdict(lambda: {})

# Active signals: id -> signal dict
active_root_signals: Dict[str, Dict[str, Any]] = {}
active_signal_index: Dict[str, set] = defaultdict(set)
recent_root_signals: Dict[str, int] = {}

# Flip observation registry (keyed by (symbol, token, start))
observed_flip_registry: Dict[Tuple[str, str, int], Dict[str, int]] = {}

public_ws = None
private_ws = None

PUBLIC_REQUEST_SEMAPHORE = asyncio.Semaphore(PUBLIC_REQ_CONCURRENCY)

TELEGRAM_QUEUE: "asyncio.Queue[Tuple[str,int]]" = asyncio.Queue()
_TELEGRAM_WORKER_TASK: Optional[asyncio.Task] = None

# Background task holder for MTF loop (created at startup)
_MTF_MONITORING_TASK: Optional[asyncio.Task] = None

symbol_locks: Dict[str, asyncio.Lock] = {}

# MTF notify callback (set by services_mtf via register_mtf_notify_callback)
_mtf_notify_cb: Optional[Callable[[Dict[str, Any]], Any]] = None


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
    return candles_cache.get(symbol, {}).get(token)


def cache_needs_refresh(symbol: str, token: str) -> bool:
    """Check if cache needs refresh based on TTL."""
    ts = candles_cache_ts.get(symbol, {}).get(token, 0)
    return (time.time() - ts) > CANDLE_CACHE_TTL


def candles_to_closes(dq: deque) -> List[float]:
    """Extract closing prices from deque of candles."""
    return [float(c.get("close", 0)) for c in dq]


def last_candle_is_closed(dq: deque, token: str, safety_seconds: int = 0) -> bool:
    """Check if the last candle in deque is closed."""
    if not dq:
        return False
    last = list(dq)[-1]
    start = last.get("start", 0)
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


# ---------- Resilient public GET ----------
async def resilient_public_get(endpoints: List[str], params: Dict[str, Any] = None, timeout: int = 12) -> Optional[Dict[str, Any]]:
    """Resilient public API request with retries and fallback hosts."""
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
                        log("Received 429, retry_after", retry_after)
                        await asyncio.sleep(retry_after)
                        continue
                    if r.status_code == 200:
                        try:
                            return r.json()
                        except Exception:
                            log("Invalid JSON from", url)
                            continue
                    else:
                        log("Public GET", url, "returned", r.status_code)
                finally:
                    try:
                        PUBLIC_REQUEST_SEMAPHORE.release()
                    except Exception:
                        pass
        backoff = min(60, 2 ** attempt)
        await asyncio.sleep(backoff)
    if last_exc:
        log("resilient_public_get exhausted retries")
    return None


# ---------- Signed request ----------
def bybit_sign_v5(api_secret: str, timestamp: str, method: str, path: str, body: str) -> str:
    """Generate Bybit V5 API signature."""
    prehash = timestamp + method.upper() + path + (body or "")
    return hmac.new(api_secret.encode(), prehash.encode(), hashlib.sha256).hexdigest()


async def bybit_signed_request(method: str, endpoint: str, payload: Dict[str, Any] = None):
    """Make authenticated request to Bybit API."""
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
                log("Bybit 429, retry_after", retry_after)
                await asyncio.sleep(retry_after)
                continue
            try:
                return r.json()
            except Exception:
                log("Signed request returned non-json")
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
        data = result.get("result", {}) if isinstance(result.get("result"), dict) else result.get("result", [])
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

    if symbol not in candles_cache_ts:
        candles_cache_ts[symbol] = {}
    candles_cache_ts[symbol][token] = time.time()

    log(f"Cache merge for {symbol} {token}:  {len(dq)} candles")


# ---------- Ensure candles cached ----------
async def ensure_cached_candles(symbol: str, tf: str, required: int):
    """Ensure we have enough cached candles, refreshing if needed."""
    token = TF_MAP[tf]
    dq = cache_get(symbol, token)

    if dq and len(dq) >= required and not cache_needs_refresh(symbol, token):
        return

    try:
        log(f"Fetching {symbol} {tf}")
        fetched = await fetch_klines(symbol, token, limit=max(required * 2, required + 50))
        if fetched:
            merge_into_cache(symbol, token, fetched)
        else:
            log(f"No klines fetched for {symbol} {tf}")
    except Exception as e:
        log("ensure_cached_candles error:", e)


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


# ---------- SQLite init & persistence ----------
async def init_db():
    """Initialize SQLite database and apply migrations."""
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.execute(
        """CREATE TABLE IF NOT EXISTS root_signals (
            id TEXT PRIMARY KEY,
            symbol TEXT,
            root_tf TEXT,
            flip_time INTEGER,
            flip_price REAL,
            status TEXT,
            priority TEXT,
            signal_type TEXT,
            components TEXT,
            created_at INTEGER,
            mtf_alerted INTEGER DEFAULT 0
        )"""
    )
    await db.execute(
        """CREATE TABLE IF NOT EXISTS trades (
            id TEXT PRIMARY KEY,
            symbol TEXT,
            side TEXT,
            qty REAL,
            entry_price REAL,
            sl_price REAL,
            created_at INTEGER,
            open BOOLEAN,
            raw_response TEXT
        )"""
    )
    await db.execute(
        """CREATE TABLE IF NOT EXISTS raw_ws_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            topic TEXT,
            message TEXT,
            created_at INTEGER
        )"""
    )
    await db.execute(
        """CREATE TABLE IF NOT EXISTS public_subscriptions (
            topic TEXT PRIMARY KEY,
            created_at INTEGER
        )"""
    )
    await db.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS ux_root_signals_symbol_tf_flip
           ON root_signals(symbol, root_tf, flip_time)"""
    )
    await db.commit()

    # Migration: ensure mtf_alerted column exists (for older DBs)
    try:
        async with db.execute("PRAGMA table_info(root_signals)") as cur:
            cols = await cur.fetchall()
        col_names = [c[1] for c in cols]
        if "mtf_alerted" not in col_names:
            try:
                await db.execute("ALTER TABLE root_signals ADD COLUMN mtf_alerted INTEGER DEFAULT 0")
                await db.commit()
                log("DB migration: added mtf_alerted column")
            except Exception:
                pass
    except Exception:
        pass

    log("DB initialized at", DB_PATH)


async def persist_root_signal(sig: Dict[str, Any]):
    """Persist root signal to database (INSERT OR IGNORE)."""
    comps = json.dumps(sig.get("components") or [])
    mtf_alerted = 1 if sig.get("mtf_alerted") else 0
    try:
        await db.execute(
            """INSERT OR IGNORE INTO root_signals
               (id,symbol,root_tf,flip_time,flip_price,status,priority,signal_type,components,created_at,mtf_alerted)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                sig["id"],
                sig["symbol"],
                sig.get("root_tf"),
                sig.get("root_flip_time") or sig.get("flip_time"),
                sig.get("root_flip_price") or sig.get("flip_price"),
                sig.get("status", "watching"),
                sig.get("priority"),
                sig.get("signal_type", "root"),
                comps,
                sig.get("created_at"),
                mtf_alerted,
            ),
        )
        await db.commit()
    except Exception as e:
        log("persist_root_signal error:", e)


async def update_root_mtf_alerted(sig_id: str, alerted: bool):
    """Update mtf_alerted flag for a persisted root signal."""
    try:
        await db.execute("UPDATE root_signals SET mtf_alerted = ? WHERE id = ?", (1 if alerted else 0, sig_id))
        await db.commit()
    except Exception as e:
        log("update_root_mtf_alerted error:", e)


async def persist_trade(trade: Dict[str, Any]):
    """Persist trade to database."""
    try:
        await db.execute(
            """INSERT OR REPLACE INTO trades
               (id,symbol,side,qty,entry_price,sl_price,created_at,open,raw_response)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                trade["id"],
                trade["symbol"],
                trade["side"],
                trade["qty"],
                trade.get("entry_price"),
                trade.get("sl_price"),
                trade["created_at"],
                trade.get("open", True),
                json.dumps(trade.get("raw", {})),
            ),
        )
        await db.commit()
    except Exception as e:
        log("persist_trade error:", e)


async def remove_root_signal(sig_id: str):
    """Remove root signal from database."""
    try:
        await db.execute("DELETE FROM root_signals WHERE id = ?", (sig_id,))
        await db.commit()
    except Exception as e:
        log("remove_root_signal error:", e)


# ---------- Load persisted signals ----------
async def load_persisted_root_signals():
    """Load persisted root signals from database into memory."""
    try:
        async with db.execute(
            "SELECT id,symbol,root_tf,flip_time,flip_price,status,priority,signal_type,components,created_at,mtf_alerted FROM root_signals"
        ) as cur:
            rows = await cur.fetchall()
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
                mtf_alerted = bool(r[10]) if len(r) > 10 else False

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
                        log("Removed expired signal:", rid)
                    except Exception as e:
                        log("Error removing expired signal", rid, e)
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
                    "mtf_alerted": mtf_alerted,
                }
                register_signal_index(symbol, signal_type)
                if symbol and flip_time:
                    recent_root_signals[symbol] = int(time.time())
                loaded += 1
            except Exception:
                continue
        log("Loaded", loaded, "persisted root signals")
    except Exception as e:
        log("load_persisted_root_signals error:", e)


async def _notify_loaded_roots_after_startup():
    """After startup, setup subscriptions for persisted roots."""
    try:
        if not active_root_signals:
            return
        sem = asyncio.Semaphore(min(10, DISCOVERY_CONCURRENCY or 10))

        async def _worker(sig):
            sym = sig.get("symbol")
            root_tf = sig.get("root_tf")
            try:
                wait_attempts = 0
                while wait_attempts < 8:
                    if public_ws and getattr(public_ws, "conn", None):
                        break
                    await asyncio.sleep(1)
                    wait_attempts += 1
                if public_ws:
                    try:
                        await public_ws.subscribe_kline(sym, TF_MAP["5m"])
                    except Exception as e:
                        log("Startup subscribe 5m error for", sym, e)
                    try:
                        await public_ws.subscribe_kline(sym, TF_MAP["15m"])
                    except Exception as e:
                        log("Startup subscribe 15m error for", sym, e)

                all_tfs = ["5m", "15m", "1h", "4h", "1d"]
                for tf in all_tfs:
                    try:
                        await ensure_cached_candles(sym, tf, MIN_CANDLES_REQUIRED)
                    except Exception as e:
                        log("Startup prewarm error for", sym, tf, e)

                await asyncio.sleep(0.5)
                try:
                    if _mtf_notify_cb:
                        await _mtf_notify_cb(sig)
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


# ---------- MTF callback registration (for services_mtf) ----------
def register_mtf_notify_callback(cb: Callable[[Dict[str, Any]], Any]):
    """Register MTF notify callback provided by services_mtf."""
    global _mtf_notify_cb
    _mtf_notify_cb = cb
    log("MTF notify callback registered")


async def notify_alignment_if_ready(sig: Dict[str, Any]):
    """Compatibility wrapper so scanners and other code can call s.notify_alignment_if_ready(sig)."""
    if _mtf_notify_cb:
        try:
            await _mtf_notify_cb(sig)
        except Exception as e:
            log("notify_alignment_if_ready callback error:", e)


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


# ---------- Telegram worker & queue ----------
async def _telegram_worker():
    """Background worker for Telegram message queue."""
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
                    log("Telegram sent ok")
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
                    log("Telegram 429, retry_after", retry_after)
                    await asyncio.sleep(retry_after)
                    if attempt + 1 < TELEGRAM_RETRY_LIMIT:
                        await TELEGRAM_QUEUE.put((text, attempt + 1))
                else:
                    log("Telegram failed:", r.status_code)
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
            log("Telegram worker loop error:", e)
            await asyncio.sleep(1)


async def send_telegram(text: str):
    """Enqueue a Telegram message."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram not configured, skipping:", text)
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
        log("Failed to enqueue telegram:", e)


# ---------- Add / remove signals ----------
async def add_signal(sig: Dict[str, Any]) -> bool:
    """Add signal with validation and deduplication."""
    sid = sig["id"]
    stype = sig.get("signal_type", "root")
    sym = sig["symbol"]
    root_time = sig.get("root_flip_time") or sig.get("flip_time")
    lock = get_symbol_lock(sym)
    async with lock:
        if signal_exists_for(sym, stype):
            log("Duplicate signal suppressed:", sym, stype)
            return False
        if stype == "root" and root_time is not None:
            existing_recent = recent_root_signals.get(sym)
            if existing_recent == root_time:
                log("Duplicate root signal (recent):", sym, root_time)
                return False
            for existing in active_root_signals.values():
                if existing.get("symbol") == sym and existing.get("signal_type", "root") == stype:
                    if existing.get("root_flip_time") == root_time:
                        log("Duplicate existing signal:", sym, stype, root_time)
                        return False
            try:
                root_tf = sig.get("root_tf")
                token = TF_MAP.get(root_tf)
                if token:
                    await ensure_cached_candles(sym, root_tf, MIN_CANDLES_REQUIRED)
                    dq = cache_get(sym, token)
                    if not dq or len(dq) == 0:
                        log("add_signal: missing cache for", sym, root_tf)
                        return False
                    last_start = list(dq)[-1]["start"]
                    if last_candle_is_closed(dq, token, safety_seconds=3):
                        log("add_signal: last candle closed for", sym, root_tf)
                        return False
                    if last_start != root_time:
                        log("add_signal: root_flip_time mismatch for", sym, root_tf)
                        return False
            except Exception as e:
                log("add_signal validation error:", e)
                return False
        if sid in active_root_signals:
            log("Signal id already present:", sid)
            return False

        # Initialize mtf_alerted for root signals (persisted)
        if stype == "root":
            sig["mtf_alerted"] = bool(sig.get("mtf_alerted", False))

        active_root_signals[sid] = sig
        register_signal_index(sym, stype)
        if stype == "root":
            recent_root_signals[sym] = int(time.time())
        try:
            await persist_root_signal(sig)
        except Exception as e:
            log("persist_root_signal error:", e)
        log("Added signal:", sid, stype, sym)
        try:
            await send_telegram(f"Added signal: {sid}")
        except Exception:
            pass

    # Post-add: subscribe and prewarm, then call MTF notify if registered
    try:
        async def _post_add_flow():
            if stype == "root":
                try:
                    global public_ws
                    if public_ws:
                        try:
                            await public_ws.subscribe_kline(sym, TF_MAP["5m"])
                        except Exception as e:
                            log("Subscribe 5m error:", sym, e)
                        try:
                            await public_ws.subscribe_kline(sym, TF_MAP["15m"])
                        except Exception as e:
                            log("Subscribe 15m error:", sym, e)

                    all_tfs = ["5m", "15m", "1h", "4h", "1d"]
                    try:
                        tasks = [ensure_cached_candles(sym, tf, MIN_CANDLES_REQUIRED) for tf in all_tfs]
                        await asyncio.gather(*tasks, return_exceptions=True)
                    except Exception as e:
                        log("Prewarm all TFs error:", sym, e)
                except Exception as e:
                    log("post_add error:", e)
            try:
                # Invoke registered MTF callback (non-blocking)
                if _mtf_notify_cb:
                    await _mtf_notify_cb(sig)
            except Exception as e:
                log("notify_alignment_if_ready post-add error:", e)

        asyncio.create_task(_post_add_flow())
    except Exception:
        pass
    return True


async def remove_signal(sig_id: str):
    """Remove signal from active tracking."""
    sig = active_root_signals.pop(sig_id, None)
    if not sig:
        return
    stype = sig.get("signal_type", "root")
    sym = sig.get("symbol")
    if stype == "root":
        try:
            recent_root_signals.pop(sym, None)
        except Exception:
            pass
    unregister_signal_index(sym, stype)
    try:
        await remove_root_signal(sig_id)
    except Exception:
        pass

    if stype == "root":
        try:
            other_root_exists = any(
                x.get("symbol") == sym and x.get("signal_type") == "root"
                for x in active_root_signals.values()
            )
            global public_ws
            if public_ws and not other_root_exists:
                try:
                    await public_ws.unsubscribe_kline(sym, TF_MAP["5m"])
                except Exception as e:
                    log("Unsubscribe 5m error:", sym, e)
                try:
                    await public_ws.unsubscribe_kline(sym, TF_MAP["15m"])
                except Exception as e:
                    log("Unsubscribe 15m error:", sym, e)
        except Exception:
            pass

    log("Removed signal:", sig_id)


# ---------- Exported wrappers for scanners compatibility ----------
# scanners.py expects detect_flip and flip_is_stable_enough to live in services module.
async def detect_flip(symbol: str, tf: str):
    try:
        import services_mtf as mtf
        return await mtf.detect_flip(symbol, tf)
    except Exception:
        return None, None


def flip_is_stable_enough(symbol: str, tf: str, start: int) -> bool:
    try:
        import services_mtf as mtf
        return mtf.flip_is_stable_enough(symbol, tf, start)
    except Exception:
        return False


# ---------- Tradable symbols ----------
async def get_tradable_usdt_symbols() -> List[str]:
    """Get list of tradable USDT symbols."""
    global symbols_info_cache

    if symbols_info_cache and len(symbols_info_cache) > 100:
        return [s for s in symbols_info_cache.keys() if s.endswith("USDT")]

    result = await resilient_public_get(INSTRUMENTS_ENDPOINTS, timeout=12)
    if not result:
        return []

    try:
        symbols = []
        data = result.get("result", {})
        if isinstance(data, dict):
            items = data.get("list", [])
        else:
            items = data if isinstance(data, list) else []

        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol") or item.get("instId")
            if symbol and symbol.endswith("USDT") and item.get("status") == "Trading":
                symbols.append(symbol)
                symbols_info_cache[symbol] = item

        return symbols
    except Exception as e:
        log("get_tradable_usdt_symbols error:", e)
        return []


# ---------- Open trades ----------
async def has_open_trade(symbol: str) -> bool:
    """Check if symbol has open trade."""
    try:
        async with db.execute("SELECT id FROM trades WHERE symbol = ?  AND open = ? ", (symbol, True)) as cur:
            row = await cur.fetchone()
        return row is not None
    except Exception:
        return False


# ---------- Debug helpers ----------
def log_current_root_signals():
    """Log current root signals for debugging."""
    try:
        count = len(active_root_signals)
        if count == 0:
            log("Current root signals:  0")
            return
        log(f"Current root signals: {count}")
        idx = 0
        for key, sig in active_root_signals.items():
            idx += 1
            symbol = sig.get("symbol")
            root_tf = sig.get("root_tf")
            status = sig.get("status")
            stype = sig.get("signal_type", "root")
            alerted = sig.get("mtf_alerted", False)
            log(f"{idx}. {symbol} {root_tf} {status} {stype} alerted={alerted}")
    except Exception as e:
        log("log_current_root_signals error:", e)


# ---------- FastAPI app initialization ----------
def init_app(fastapi_app):
    """Register endpoints and startup event."""
    global app, _TELEGRAM_WORKER_TASK, _MTF_MONITORING_TASK
    app = fastapi_app

    @app.get("/debug/current_roots")
    async def debug_current_roots(_auth=Depends(require_admin_auth)):
        return {"count": len(active_root_signals), "signals": list(active_root_signals.values())}

    @app.get("/debug/check_symbol")
    async def debug_check_symbol(symbol: str, tf: str = "1h", _auth=Depends(require_admin_auth)):
        if tf not in TF_MAP:
            raise HTTPException(status_code=400, detail=f"Unknown timeframe {tf}")
        token = TF_MAP[tf]
        await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
        dq = cache_get(symbol, token)
        if not dq:
            return {"symbol": symbol, "tf": tf, "error": "no cached klines"}
        try:
            import services_mtf as mtf  # local import for debugging
            closes = candles_to_closes(dq)
            hist = mtf.macd_hist(closes)
            flip_kind, flip_ts = await detect_flip(symbol, tf)
            closed = last_candle_is_closed(dq, token)
            return {
                "symbol": symbol,
                "tf": tf,
                "cached_klines": len(dq),
                "macd_last": hist[-1] if hist else None,
                "flip_kind": flip_kind,
                "flip_ts": flip_ts,
                "last_closed": closed,
            }
        except Exception as e:
            log("debug_check_symbol error:", e)
            return {"error": str(e)}

    @app.post("/admin/send_root_summary")
    async def admin_send_root_summary(_auth=Depends(require_admin_auth)):
        await send_telegram("Root signals summary")
        return {"status": "ok"}

    @app.get("/")
    async def root_route():
        return {"status": "ok"}

    @app.get("/health")
    async def health():
        db_ok = db is not None
        public_ws_connected = False
        try:
            public_ws_connected = bool(public_ws and getattr(public_ws, "conn", None))
        except Exception:
            public_ws_connected = False
        mtf_loop_running = False
        try:
            mtf_loop_running = _MTF_MONITORING_TASK is not None and not _MTF_MONITORING_TASK.done()
        except Exception:
            pass
        return {
            "status": "ok",
            "db": db_ok,
            "public_ws_connected": public_ws_connected,
            "mtf_monitoring_loop": mtf_loop_running,
        }

    @app.on_event("startup")
    async def startup_event():
        global public_ws, _TELEGRAM_WORKER_TASK, _MTF_MONITORING_TASK
        await init_db()
        await load_persisted_root_signals()
        log("Startup config:", "BYBIT_USE_MAINNET=", BYBIT_USE_MAINNET, "TRADING_ENABLED=", TRADING_ENABLED)

        try:
            import scanners as _sc

            globals()["process_inprogress_update"] = _sc.process_inprogress_update
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

        try:
            import scanners as _sc

            asyncio.create_task(_sc._continuous_kline_fetcher("1h"))
            asyncio.create_task(_sc._continuous_kline_fetcher("4h"))
            asyncio.create_task(_sc.root_scanner_loop("1h"))
            asyncio.create_task(_sc.root_scanner_loop("4h"))
            asyncio.create_task(_sc.evaluate_signals_loop())
            asyncio.create_task(_sc.expire_signals_loop())
            asyncio.create_task(_sc.periodic_root_logger())
        except Exception as e:
            log("Error starting scanner tasks:", e)

        try:
            await _notify_loaded_roots_after_startup()
        except Exception as e:
            log("Failed to schedule notify_loaded_roots_after_startup:", e)

        # Import and start MTF module here (deferred import to avoid circular module-level imports)
        try:
            import services_mtf as _mtf
            # start the monitoring loop and keep a handle
            try:
                _MTF_MONITORING_TASK = _mtf.start_mtf_monitoring_loop()
                log("[STARTUP] ✅ MTF Alignment Monitoring Loop scheduled")
            except Exception as e:
                log("[STARTUP] ❌ Failed to schedule MTF monitoring loop:", e)
        except Exception as e:
            log("Failed to import/start services_mtf:", e)

        log("Background tasks started")


# ---------- Admin auth dependency ----------
async def require_admin_auth(authorization: Optional[str] = Header(None), x_api_key: Optional[str] = Header(None)):
    """Validate admin API key from header."""
    if not ADMIN_API_KEY:
        log("WARNING:  ADMIN_API_KEY not set — admin endpoints are UNPROTECTED.")
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