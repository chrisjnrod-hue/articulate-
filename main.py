# main.py
# Bybit MACD multi-timeframe scanner — final update (v12 -> v12.1)
# - Enforces: root must flip on its most-recent closed candle.
# - Enforces: among remaining TFs at most one may be negative; that single TF (the "last-to-flip")
#   must flip on its most-recent closed candle for the signal to promote to "full".
# - All other TFs must be MACD-positive (no current flip required).
# - SUPER_WINDOW_SECONDS default 300 (5 minutes).
# - Scans all USDT perpetuals by default (SYMBOL_SCAN_LIMIT = 0).
# - Candle-cache, closed-candle checks, deduplication, persistence, Telegram list notifications.
# - Debug endpoints included: /debug/current_roots, /debug/check_symbol, /debug/last_to_flip
#
# Configure via environment variables (see defaults below).
# Run with: uvicorn main:app --host 0.0.0.0 --port 8000

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
from typing import Dict, Any, List, Optional
from collections import deque, defaultdict

import httpx
import aiosqlite
import websockets
from fastapi import FastAPI, Query, Depends, Header, HTTPException, status
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

# ---------- Configuration (env with sensible defaults) ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
BYBIT_USE_MAINNET = os.getenv("BYBIT_USE_MAINNET", "true").lower() == "true"
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "false").lower() == "true"

DB_PATH = os.getenv("DB_PATH", "scanner.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "info").lower()
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
ROOT_SCAN_LOOKBACK = int(os.getenv("ROOT_SCAN_LOOKBACK", "3"))
SYMBOL_SCAN_LIMIT = int(os.getenv("SYMBOL_SCAN_LIMIT", "0"))  # 0 = scan all USDT symbols

CANDLE_CACHE_MAX = int(os.getenv("CANDLE_CACHE_MAX", "2000"))
CANDLE_CACHE_TTL = int(os.getenv("CANDLE_CACHE_TTL", "300"))

SKIP_DIGIT_PREFIX = os.getenv("SKIP_DIGIT_PREFIX", "true").lower() == "true"
MIN_SYMBOL_AGE_MONTHS = int(os.getenv("MIN_SYMBOL_AGE_MONTHS", "3"))

# MACD params (configurable)
MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
MIN_CANDLES_REQUIRED = MACD_SLOW + MACD_SIGNAL + 5

# Super window (5 minutes default)
SUPER_WINDOW_SECONDS = int(os.getenv("SUPER_WINDOW_SECONDS", "300"))

# Bybit endpoints
MAINNET_API_HOST = "https://api.bybit.com"
TESTNET_API_HOST = "https://api-testnet.bybit.com"
PRIMARY_API_HOST = MAINNET_API_HOST if BYBIT_USE_MAINNET else TESTNET_API_HOST
API_HOSTS = [PRIMARY_API_HOST, TESTNET_API_HOST] if PRIMARY_API_HOST == MAINNET_API_HOST else [PRIMARY_API_HOST, MAINNET_API_HOST]

PUBLIC_WS_URL = "wss://stream.bybit.com/v5/public/linear" if BYBIT_USE_MAINNET else "wss://stream-testnet.bybit.com/v5/public/linear"
INSTRUMENTS_ENDPOINTS = [
    "/v5/market/instruments-info?category=linear&instType=PERPETUAL",
    "/v5/market/instruments-info?category=linear",
    "/v5/market/instruments-info",
    "/v2/public/symbols",
]
KLINE_ENDPOINTS = ["/v5/market/kline"]

TF_MAP = {"5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "D"}
STABLECOINS = {"USDT", "BUSD", "USDC", "TUSD", "DAI"}

# ---------- Globals ----------
app = FastAPI()
httpx_client = httpx.AsyncClient(timeout=20)
db: Optional[aiosqlite.Connection] = None

symbols_info_cache: Dict[str, Dict[str, Any]] = {}
candles_cache: Dict[str, Dict[str, deque]] = defaultdict(lambda: {})
candles_cache_ts: Dict[str, Dict[str, int]] = defaultdict(lambda: {})

active_root_signals: Dict[str, Dict[str, Any]] = {}
active_signal_index: Dict[str, set] = defaultdict(set)

public_ws = None
_re_leading_digit = re.compile(r"^\d")

# ---------- Logging / Utilities ----------
def log(*args, **kwargs):
    if LOG_LEVEL != "none":
        ts = datetime.now(timezone.utc).isoformat()
        print(ts, *args, **kwargs)

def now_ts_ms() -> int:
    return int(time.time() * 1000)

async def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        await httpx_client.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
    except Exception as e:
        log("Telegram send error:", e)

async def build_and_send_signal_list_telegram():
    roots, supers, fulls = [], [], []
    for s in active_root_signals.values():
        t = s.get("signal_type")
        line = f"{s['symbol']} root={s.get('root_tf')} created={datetime.fromtimestamp(s['created_at']/1000).isoformat()} pri={s.get('priority')}"
        if t == "root":
            roots.append(line)
        elif t == "super":
            supers.append(line + f" comps={s.get('components')}")
        elif t == "full":
            fulls.append(line)
    parts = [
        f"Root signals ({len(roots)}):\n" + ("\n".join(roots) if roots else "none"),
        f"Super signals ({len(supers)}):\n" + ("\n".join(supers) if supers else "none"),
        f"Full signals ({len(fulls)}):\n" + ("\n".join(fulls) if fulls else "none"),
    ]
    await send_telegram("\n\n".join(parts))

# ---------- DB ----------
async def init_db():
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("""CREATE TABLE IF NOT EXISTS root_signals (
        id TEXT PRIMARY KEY, symbol TEXT, root_tf TEXT, signal_type TEXT, priority TEXT, status TEXT, components TEXT, created_at INTEGER
    )""")
    await db.execute("""CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY, symbol TEXT, side TEXT, qty REAL, entry_price REAL, sl_price REAL, created_at INTEGER, open BOOLEAN, raw_response TEXT
    )""")
    await db.execute("""CREATE TABLE IF NOT EXISTS raw_ws_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, topic TEXT, message TEXT, created_at INTEGER
    )""")
    await db.commit()
    log("DB initialized at", DB_PATH)

async def persist_signal_db(sig: Dict[str, Any]):
    comps = json.dumps(sig.get("components") or [])
    await db.execute("INSERT OR REPLACE INTO root_signals (id,symbol,root_tf,signal_type,priority,status,components,created_at) VALUES (?,?,?,?,?,?,?,?)",
                     (sig["id"], sig["symbol"], sig["root_tf"], sig["signal_type"], sig.get("priority"), sig.get("status"), comps, sig["created_at"]))
    await db.commit()

async def remove_signal_db(sig_id: str):
    await db.execute("DELETE FROM root_signals WHERE id = ?", (sig_id,))
    await db.commit()

# ---------- REST helpers ----------
async def resilient_public_get(endpoints: List[str], params: Dict[str, Any] = None, timeout: int = 12):
    for host in API_HOSTS:
        for ep in endpoints:
            url = host + ep
            try:
                r = await httpx_client.get(url, params=params or {}, timeout=timeout)
            except Exception:
                continue
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    continue
    return None

async def get_tradable_usdt_symbols() -> List[str]:
    found = []
    for ep in INSTRUMENTS_ENDPOINTS:
        url = PRIMARY_API_HOST + ep
        try:
            r = await httpx_client.get(url, timeout=12)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        try:
            resp = r.json()
        except Exception:
            continue
        items = []
        if isinstance(resp, dict):
            items = resp.get("result", {}).get("list", []) or resp.get("data", []) or resp.get("list", []) or []
        elif isinstance(resp, list):
            items = resp
        for it in items:
            if not isinstance(it, dict):
                continue
            symbol = (it.get("symbol") or it.get("name") or "").strip()
            quote = (it.get("quoteCoin") or it.get("quoteAsset") or it.get("quote") or "").upper()
            if not symbol:
                continue
            if not (symbol.upper().endswith("USDT") or quote == "USDT"):
                continue
            base = symbol[:-4] if symbol.upper().endswith("USDT") else symbol
            if base.upper() in STABLECOINS:
                continue
            if SKIP_DIGIT_PREFIX and _re_leading_digit.match(symbol):
                continue
            launch_ts = None
            for k in ("launchTime", "launch_time", "listTime", "listedAt"):
                if k in it and it[k]:
                    try:
                        v = int(float(it[k]))
                        if v > 10**12:
                            v = v // 1000
                        launch_ts = v
                        break
                    except Exception:
                        continue
            symbols_info_cache[symbol] = {"symbol": symbol, "quote": quote, "launch_ts": launch_ts}
            found.append(symbol)
        if found:
            break
    uniq = sorted(set(found))
    if SYMBOL_SCAN_LIMIT and SYMBOL_SCAN_LIMIT > 0:
        uniq = uniq[:SYMBOL_SCAN_LIMIT]
    log("Found", len(uniq), "USDT perpetual symbols")
    return uniq

async def fetch_klines(symbol: str, interval_token: str, limit: int = 200) -> List[Dict[str, Any]]:
    params = {"category": "linear", "symbol": symbol, "interval": str(interval_token), "limit": limit}
    resp = await resilient_public_get([KLINE_ENDPOINTS[0]], params=params)
    if not resp:
        return []
    items = []
    if isinstance(resp, dict):
        items = resp.get("result", {}).get("list", []) or resp.get("data", []) or resp.get("list", []) or []
    elif isinstance(resp, list):
        items = resp
    candles = []
    for it in items:
        try:
            if isinstance(it, dict):
                start = it.get("start") or it.get("t") or it.get("open_time")
                end = it.get("end") or it.get("close_time")
                close_val = it.get("close") or it.get("c")
            elif isinstance(it, (list, tuple)) and len(it) >= 5:
                start = it[0]
                end = None
                close_val = it[4]
            else:
                continue
            if start is None or close_val is None:
                continue
            start_i = int(float(start))
            if start_i > 10**12:
                start_i = start_i // 1000
            end_i = None
            if end is not None:
                try:
                    end_i = int(float(end))
                    if end_i > 10**12:
                        end_i = end_i // 1000
                except Exception:
                    end_i = None
            close = float(close_val)
            candles.append({"start": start_i, "end": end_i, "close": close})
        except Exception:
            continue
    candles.sort(key=lambda x: x["start"])
    return candles

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
    existing = {c["start"] for c in dq}
    for c in candles:
        if c["start"] not in existing:
            dq.append(c)
            existing.add(c["start"])
    arr = sorted(list(dq), key=lambda x: x["start"])
    dq = deque(arr, maxlen=CANDLE_CACHE_MAX)
    cache_set(symbol, interval_token, dq)

async def ensure_cached_candles(symbol: str, tf: str, required: int):
    token = TF_MAP[tf]
    dq = cache_get(symbol, token)
    if dq and len(dq) >= required and not cache_needs_refresh(symbol, token):
        return
    fetched = await fetch_klines(symbol, token, limit=max(required*2, 200))
    if fetched:
        merge_into_cache(symbol, token, fetched)
        log(f"Cache updated for {symbol} {tf}: {len(cache_get(symbol, token) or [])} candles")

def closes_from_dq(dq: deque, last_n: Optional[int] = None) -> List[float]:
    arr = list(dq)
    if last_n:
        arr = arr[-last_n:]
    return [x["close"] for x in arr if x.get("close") is not None]

# ---------- MACD helpers ----------
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

def macd_positive_from_closes(closes: List[float]) -> bool:
    hist = macd_hist(closes)
    if not hist:
        return False
    last = hist[-1]
    return last is not None and last > 0

def flip_on_last_from_closes(closes: List[float]) -> bool:
    if len(closes) < MACD_SLOW + MACD_SIGNAL:
        return False
    hist = macd_hist(closes)
    if not hist or len(hist) < 2:
        return False
    prev = hist[-2] or 0
    cur = hist[-1] or 0
    return (prev <= 0 and cur > 0)

# ---------- Candle closed helper ----------
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

# ---------- Signal management & dedupe ----------
def signal_key_for(symbol: str, signal_type: str, root_tf: str, ts: int) -> str:
    return f"{signal_type}-{symbol}-{root_tf}-{ts}"

def has_active_signal(symbol: str, signal_type: str) -> bool:
    return signal_type in active_signal_index.get(symbol, set())

def register_active_signal(symbol: str, signal_type: str):
    active_signal_index[symbol].add(signal_type)

def unregister_active_signal(symbol: str, signal_type: str):
    if symbol in active_signal_index and signal_type in active_signal_index[symbol]:
        active_signal_index[symbol].remove(signal_type)

async def add_signal(sig: Dict[str, Any]):
    stype = sig.get("signal_type")
    symbol = sig.get("symbol")
    if has_active_signal(symbol, stype):
        log("Duplicate signal skipped:", symbol, stype)
        return False
    active_root_signals[sig["id"]] = sig
    register_active_signal(symbol, stype)
    await persist_signal_db(sig)
    log("Added signal:", sig["id"], stype, symbol)
    await build_and_send_signal_list_telegram()
    return True

async def remove_signal(sig_id: str):
    sig = active_root_signals.pop(sig_id, None)
    if not sig:
        return
    unregister_active_signal(sig["symbol"], sig["signal_type"])
    await remove_signal_db(sig_id)
    log("Removed signal:", sig_id)
    await build_and_send_signal_list_telegram()

# ---------- Root scanner (strict last-candle closed checks) ----------
def tf_list_for_root(root_tf: str) -> List[str]:
    if root_tf == "4h":
        return ["5m", "15m", "1h", "1d"]
    if root_tf == "1h":
        return ["5m", "15m", "4h", "1d"]
    return ["5m", "15m", "1h", "1d"]

async def root_scanner_loop(root_tf: str):
    log("Root scanner started for", root_tf)
    while True:
        try:
            symbols = await get_tradable_usdt_symbols()
            for symbol in symbols:
                base = symbol[:-4] if symbol.upper().endswith("USDT") else symbol
                if base.upper() in STABLECOINS:
                    continue
                if SKIP_DIGIT_PREFIX and _re_leading_digit.match(symbol):
                    continue
                info = symbols_info_cache.get(symbol, {})
                if info.get("launch_ts"):
                    age_days = (time.time() - info["launch_ts"]) / 86400.0
                    if age_days < (MIN_SYMBOL_AGE_MONTHS * 30):
                        continue
                # ensure cache for root tf
                await ensure_cached_candles(symbol, root_tf, MIN_CANDLES_REQUIRED)
                token = TF_MAP[root_tf]
                dq = cache_get(symbol, token)
                if not dq or len(dq) < MIN_CANDLES_REQUIRED:
                    continue
                # ensure last candle closed before detection
                if not last_candle_is_closed(dq, token, safety_seconds=3):
                    continue
                closes = closes_from_dq(dq)
                if not flip_on_last_from_closes(closes):
                    continue
                last_candle = list(dq)[-1]
                ts = last_candle["start"]
                key = signal_key_for(symbol, "root", root_tf, ts)
                sig = {
                    "id": key,
                    "symbol": symbol,
                    "root_tf": root_tf,
                    "signal_type": "root",
                    "priority": None,
                    "status": "watching",
                    "components": [],
                    "created_at": now_ts_ms(),
                    "last_flip_ts": ts
                }
                added = await add_signal(sig)
                if added:
                    try:
                        if public_ws and getattr(public_ws, "detect_template", None):
                            await public_ws.subscribe_kline(symbol, TF_MAP["5m"])
                            await public_ws.subscribe_kline(symbol, TF_MAP["15m"])
                    except Exception:
                        pass
        except Exception as e:
            log("root_scanner_loop error:", e)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

# ---------- Super detection (2+ long TF flips within window) ----------
async def super_signal_scanner_loop():
    log("Super scanner started, window:", SUPER_WINDOW_SECONDS)
    while True:
        try:
            symbols = await get_tradable_usdt_symbols()
            for symbol in symbols:
                base = symbol[:-4] if symbol.upper().endswith("USDT") else symbol
                if base.upper() in STABLECOINS:
                    continue
                if SKIP_DIGIT_PREFIX and _re_leading_digit.match(symbol):
                    continue
                flips = []
                comps = []
                for tf in ["1h", "4h", "1d"]:
                    await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
                    token = TF_MAP[tf]
                    dq = cache_get(symbol, token)
                    if not dq or len(dq) < MIN_CANDLES_REQUIRED:
                        continue
                    if not last_candle_is_closed(dq, token, safety_seconds=3):
                        continue
                    closes = closes_from_dq(dq)
                    if flip_on_last_from_closes(closes):
                        last_ts = list(dq)[-1]["start"]
                        flips.append((tf, last_ts))
                        comps.append(tf)
                if len(flips) >= 2:
                    times = [t for _, t in flips]
                    if max(times) - min(times) <= SUPER_WINDOW_SECONDS:
                        ts = int(time.time())
                        key = signal_key_for(symbol, "super", "multi", ts)
                        sig = {
                            "id": key,
                            "symbol": symbol,
                            "root_tf": "multi",
                            "signal_type": "super",
                            "priority": "super",
                            "status": "watching",
                            "components": comps,
                            "created_at": now_ts_ms(),
                            "last_flip_ts": max(times)
                        }
                        if not has_active_signal(symbol, "super"):
                            await add_signal(sig)
        except Exception as e:
            log("super_scanner_loop error:", e)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

# ---------- Evaluate signals for "full" alignment with final rule ----------
async def evaluate_signal(sig_id: str):
    sig = active_root_signals.get(sig_id)
    if not sig:
        return
    symbol = sig["symbol"]
    signal_type = sig["signal_type"]
    root_tf = sig["root_tf"]

    if signal_type == "root":
        required_tfs = tf_list_for_root(root_tf)
    elif signal_type == "super":
        required_tfs = ["5m", "15m", "1h", "1d"]
    else:
        required_tfs = tf_list_for_root(root_tf)

    base = symbol[:-4] if symbol.upper().endswith("USDT") else symbol
    if base.upper() in STABLECOINS:
        await remove_signal(sig_id)
        return
    info = symbols_info_cache.get(symbol, {})
    if info.get("launch_ts"):
        age_days = (time.time() - info["launch_ts"]) / 86400.0
        if age_days < (MIN_SYMBOL_AGE_MONTHS * 30):
            await remove_signal(sig_id)
            return

    tf_status = {}
    for tf in required_tfs:
        await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
        token = TF_MAP[tf]
        dq = cache_get(symbol, token)
        if not dq or len(dq) < MIN_CANDLES_REQUIRED:
            tf_status[tf] = {"has": False}
            continue
        if not last_candle_is_closed(dq, token, safety_seconds=3):
            tf_status[tf] = {"has": True, "closed": False}
            continue
        closes = closes_from_dq(dq)
        positive = macd_positive_from_closes(closes)
        flip_last = flip_on_last_from_closes(closes)
        last_ts = list(dq)[-1]["start"]
        tf_status[tf] = {"has": True, "closed": True, "positive": positive, "flip_last": flip_last, "last_ts": last_ts}

    # Root must be present and flipped on last closed candle
    if root_tf not in tf_status or not tf_status[root_tf].get("has") or not tf_status[root_tf].get("closed"):
        return
    if not tf_status[root_tf].get("flip_last"):
        return

    # Among the other TFs, count negatives
    other_tfs = [tf for tf in required_tfs if tf != root_tf]
    negatives = [tf for tf in other_tfs if not tf_status.get(tf, {}).get("positive")]
    # If more than one other TF negative -> not ready
    if len(negatives) > 1:
        return
    if len(negatives) == 1:
        tf_to_flip = negatives[0]
        status = tf_status.get(tf_to_flip, {})
        if not status.get("has") or not status.get("closed"):
            return
        if not status.get("flip_last"):
            return
        for tf in other_tfs:
            if tf == tf_to_flip:
                continue
            if not tf_status.get(tf, {}).get("positive"):
                return
    else:
        # zero negatives -> all others positive -> full alignment
        pass

    # Achieved full alignment
    if not has_active_signal(symbol, "full"):
        ts = int(time.time())
        key = signal_key_for(symbol, "full", root_tf, ts)
        full_sig = {
            "id": key,
            "symbol": symbol,
            "root_tf": root_tf,
            "signal_type": "full",
            "priority": "full-mtf",
            "status": "ready",
            "components": required_tfs,
            "created_at": now_ts_ms(),
            "last_flip_ts": now_ts_ms()
        }
        await add_signal(full_sig)

    # Trading: dedupe via 'full' existence; simulate if disabled
    async with db.execute("SELECT COUNT(*) FROM trades WHERE open = 1") as cur:
        row = await cur.fetchone()
        open_count = row[0] if row else 0
    if open_count >= 5:
        log("Max open trades reached, skipping execution for", symbol)
        return

    dq5 = cache_get(symbol, TF_MAP["5m"])
    last_price = None
    if dq5 and len(dq5):
        last_price = list(dq5)[-1]["close"]
    price = last_price or 1.0
    balance = 1000.0
    per_trade = max(1.0, balance / 5)
    qty = max(0.0001, round(per_trade / price, 6))
    side = "Buy"
    stop_price = round(price * (1 - 0.02), 8)

    if TRADING_ENABLED:
        try:
            res = await bybit_signed_request("POST", "/v5/order/create", {"category": "linear", "symbol": symbol, "side": side, "orderType": "Market", "qty": qty})
            oid = res.get("result", {}).get("orderId", str(uuid.uuid4()))
            await db.execute("INSERT OR REPLACE INTO trades (id,symbol,side,qty,entry_price,sl_price,created_at,open,raw_response) VALUES (?,?,?,?,?,?,?,?,?)",
                             (oid, symbol, side, qty, None, stop_price, now_ts_ms(), True, json.dumps(res)))
            await db.commit()
            await send_telegram(f"Placed real order {symbol} qty={qty} side={side}")
            log("Placed real order", symbol, res)
        except Exception as e:
            log("Error placing real order:", e)
    else:
        oid = str(uuid.uuid4())
        await db.execute("INSERT OR REPLACE INTO trades (id,symbol,side,qty,entry_price,sl_price,created_at,open,raw_response) VALUES (?,?,?,?,?,?,?,?,?)",
                         (oid, symbol, side, qty, None, stop_price, now_ts_ms(), True, json.dumps({"simulated": True})))
        await db.commit()
        await send_telegram(f"Simulated order for {symbol} qty={qty} side={side} (full-mtf)")
        log("Simulated trade for", symbol, "qty", qty)

# ---------- Minimal Public WS (template detection & raw persisting) ----------
class PublicWS:
    def __init__(self, url: str):
        self.url = url
        self.conn = None
        self.detect_template = None
        self._task = None
    async def connect(self):
        try:
            self.conn = await websockets.connect(self.url, ping_interval=20, ping_timeout=10, max_size=2**24)
            log("Public WS connected")
            return True
        except Exception as e:
            log("Public WS connect failed", e)
            return False
    async def connect_and_detect(self):
        ok = await self.connect()
        if not ok:
            return False
        try:
            t = "klineV2.{interval}.{symbol}".format(interval=TF_MAP["5m"], symbol="BTCUSDT")
            await self.conn.send(json.dumps({"op": "subscribe", "args": [t]}))
            try:
                msg = await asyncio.wait_for(self.conn.recv(), timeout=1.0)
                body = self._maybe_decompress(msg)
                if body and "kline" in body:
                    self.detect_template = "klineV2.{interval}.{symbol}"
                await self.conn.send(json.dumps({"op": "unsubscribe", "args": [t]}))
            except Exception:
                pass
        except Exception:
            pass
        if not self._task:
            self._task = asyncio.create_task(self._recv_loop())
        return True
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
    async def _recv_loop(self):
        while True:
            try:
                raw = await self.conn.recv()
                body = self._maybe_decompress(raw)
                if not body:
                    continue
                try:
                    obj = json.loads(body)
                    topic = obj.get("topic") or obj.get("arg", {}).get("topic") or ""
                    await persist_raw_ws("public", topic, json.dumps(obj))
                except Exception:
                    continue
            except Exception as e:
                log("PublicWS recv error:", e)
                try:
                    if self.conn:
                        await self.conn.close()
                except Exception:
                    pass
                self.conn = None
                await asyncio.sleep(5)
                try:
                    await self.connect()
                except Exception:
                    await asyncio.sleep(5)

async def persist_raw_ws(source: str, topic: Optional[str], message: str):
    if not db:
        return
    if len(message) > 2000:
        message = message[:2000] + "...[truncated]"
    await db.execute("INSERT INTO raw_ws_messages (source,topic,message,created_at) VALUES (?,?,?,?)", (source, topic or "", message, now_ts_ms()))
    await db.commit()

# ---------- Debug endpoints ----------
async def require_admin(authorization: Optional[str] = Header(None), x_api_key: Optional[str] = Header(None)):
    if not ADMIN_API_KEY:
        return
    token = None
    if authorization:
        auth = authorization.strip()
        token = auth[7:].strip() if auth.lower().startswith("bearer ") else auth
    if x_api_key:
        token = x_api_key
    if not token or token != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

@app.get("/debug/current_roots")
async def debug_current_roots(_auth=Depends(require_admin)):
    return {"count": len(active_root_signals), "signals": list(active_root_signals.values())}

@app.get("/debug/check_symbol")
async def debug_check_symbol(symbol: str = Query(...), tf: str = Query("1h"), _auth=Depends(require_admin)):
    if tf not in TF_MAP:
        raise HTTPException(status_code=400, detail="invalid tf")
    await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
    token = TF_MAP[tf]
    dq = cache_get(symbol, token)
    if not dq:
        return {"symbol": symbol, "tf": tf, "cached": 0}
    closes = closes_from_dq(dq)
    hist = macd_hist(closes)
    flip = flip_on_last_from_closes(closes)
    closed = last_candle_is_closed(dq, token)
    return {"symbol": symbol, "tf": tf, "cached": len(dq), "macd_last": hist[-1] if hist else None, "flip_on_last": flip, "last_closed": closed, "sample": list(dq)[-5:]}

@app.get("/debug/last_to_flip")
async def debug_last_to_flip(symbol: str = Query(...), root_tf: str = Query("4h"), _auth=Depends(require_admin)):
    """
    Returns which timeframe (if any) is currently the 'last-to-flip' for given symbol/root_tf.
    """
    required_tfs = tf_list_for_root(root_tf)
    await asyncio.gather(*(ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED) for tf in required_tfs))
    status = {}
    negatives = []
    for tf in required_tfs:
        token = TF_MAP[tf]
        dq = cache_get(symbol, token)
        if not dq or len(dq) < MIN_CANDLES_REQUIRED or not last_candle_is_closed(dq, token):
            status[tf] = {"ready": False}
            continue
        closes = closes_from_dq(dq)
        positive = macd_positive_from_closes(closes)
        flip_last = flip_on_last_from_closes(closes)
        status[tf] = {"ready": True, "positive": positive, "flip_last": flip_last, "last_ts": list(dq)[-1]["start"]}
        if not positive:
            negatives.append(tf)
    last_to_flip = negatives[0] if len(negatives) == 1 else None
    return {"symbol": symbol, "root_tf": root_tf, "status": status, "last_to_flip": last_to_flip, "negatives": negatives}

# ---------- Startup ----------
@app.on_event("startup")
async def startup():
    global public_ws
    await init_db()
    public_ws = PublicWS(PUBLIC_WS_URL)
    try:
        await public_ws.connect_and_detect()
    except Exception:
        pass
    try:
        async with db.execute("SELECT id,symbol,root_tf,signal_type,priority,status,components,created_at FROM root_signals") as cur:
            rows = await cur.fetchall()
        for r in rows:
            try:
                comps = json.loads(r[6]) if r[6] else []
            except Exception:
                comps = []
            active_root_signals[r[0]] = {"id": r[0], "symbol": r[1], "root_tf": r[2], "signal_type": r[3], "priority": r[4], "status": r[5], "components": comps, "created_at": r[7], "last_flip_ts": None}
            active_signal_index[r[1]].add(r[3])
        log("Loaded persisted signals:", len(active_root_signals))
    except Exception as e:
        log("persisted load error:", e)

    asyncio.create_task(root_scanner_loop("1h"))
    asyncio.create_task(root_scanner_loop("4h"))
    asyncio.create_task(super_signal_scanner_loop())

    async def evaluator_loop():
        while True:
            try:
                ids = list(active_root_signals.keys())
                for sid in ids:
                    try:
                        await evaluate_signal(sid)
                    except Exception as e:
                        log("evaluate loop error for", sid, e)
                await asyncio.sleep(max(10, SCAN_INTERVAL_SECONDS // 2))
            except Exception as e:
                log("evaluator loop outer error:", e)
                await asyncio.sleep(5)
    asyncio.create_task(evaluator_loop())

    async def periodic_logger():
        while True:
            try:
                await log_current_root_signals()
                if active_root_signals:
                    await build_and_send_signal_list_telegram()
            except Exception as e:
                log("periodic_logger error:", e)
            await asyncio.sleep(max(30, SCAN_INTERVAL_SECONDS))
    asyncio.create_task(periodic_logger())

    log("Startup complete; background tasks started")

async def log_current_root_signals():
    if not active_root_signals:
        log("Current root signals: 0")
        return
    log("Current root signals total:", len(active_root_signals))
    for i, s in enumerate(active_root_signals.values(), 1):
        log(f"{i}. {s['signal_type'].upper()} {s['symbol']} root={s.get('root_tf')} pri={s.get('priority')} status={s.get('status')} comps={s.get('components')} created={datetime.fromtimestamp(s['created_at']/1000).isoformat()}")

# ---------- Helper stub for signed requests (user to implement full auth if needed) ----------
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

# ---------- Run note ----------
# uvicorn main:app --host 0.0.0.0 --port 8000
