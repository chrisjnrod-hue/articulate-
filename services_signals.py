# services_signals.py - Signal management, MTF evaluation, and database operations

import asyncio
import json
import time
from typing import Dict, Any, List, Optional

import aiosqlite

from services_config import (
    log, now_ts_ms, get_symbol_lock, cache_get, signal_exists_for,
    register_signal_index, unregister_signal_index, is_stablecoin_symbol,
    tf_list_for_root, bybit_signed_request, ensure_cached_candles,
    detect_flip, candles_to_closes, macd_positive_from_closes, macd_hist,
    interval_seconds_from_token, TF_MAP, MIN_CANDLES_REQUIRED,
    active_root_signals, recent_root_signals, root_signal_symbols,
    DB_PATH, TRADING_ENABLED, MAX_OPEN_TRADES, STOP_LOSS_PCT,
)

# Import db from config
from services_config import db as db_ref

# Re-export for external use
db = db_ref


# ---------- MTF Evaluation ----------
async def _evaluate_root_signal_for_mtf(sig:  Dict[str, Any]):
    """Evaluate root signal for MTF alignment - called immediately after signal creation."""
    try:
        symbol = sig. get("symbol")
        root_tf = sig.get("root_tf")
        stype = sig.get("signal_type")

        if stype != "root" or not symbol or not root_tf:
            log(f"❌ MTF eval SKIP {symbol}:  invalid stype/symbol/root_tf")
            return

        if signal_exists_for(symbol, "entry"):
            log(f"❌ MTF eval SKIP {symbol}: already has active entry")
            return

        required_tfs = tf_list_for_root(root_tf)
        log(f"🔍 MTF eval START {symbol} {root_tf}, required_tfs={required_tfs}")

        # ✅ AGGRESSIVE:   Fetch all required TFs with retries
        log(f"📥 FETCHING all TFs for {symbol}...")
        for tf in required_tfs: 
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
                    token = TF_MAP[tf]
                    dq = cache_get(symbol, token)
                    if dq and len(dq) >= MIN_CANDLES_REQUIRED:
                        log(f"  ✓ Fetched {symbol} {tf} (attempt {attempt+1}) - {len(dq)} candles")
                        break
                    else:
                        log(f"  ⚠ {symbol} {tf} (attempt {attempt+1}) - only {len(dq) if dq else 0} candles, retrying...")
                        await asyncio.sleep(0.5)
                except Exception as e: 
                    log(f"  ✗ Fetch error {symbol} {tf} (attempt {attempt+1}): {e}")
                    if attempt < max_retries - 1:
                        await asyncio. sleep(0.5)

        # ✅ Gather and log ALL TF status
        tf_status = {}
        for tf in required_tfs:
            token = TF_MAP[tf]
            dq = cache_get(symbol, token)

            log(f"  Checking {symbol} {tf}:  token={token}, dq_exists={bool(dq)}, candles={len(dq) if dq else 0}")

            if not dq or len(dq) < MIN_CANDLES_REQUIRED:
                tf_status[tf] = {
                    "has":  False,
                    "reason": "insufficient_cache",
                    "candles": len(dq) if dq else 0,
                    "required": MIN_CANDLES_REQUIRED
                }
                log(f"    ❌ FAILED:  {len(dq) if dq else 0}/{MIN_CANDLES_REQUIRED} candles")
                continue

            # ✅ TF has data, analyze it
            try:
                closes = candles_to_closes(dq)
                positive = macd_positive_from_closes(closes)
                flip_kind, flip_ts = await detect_flip(symbol, tf)
                flip_last = (flip_kind == "open")
                hist = macd_hist(closes)
                macd_last = hist[-1] if hist else None

                tf_status[tf] = {
                    "has": True,
                    "positive": positive,
                    "flip_last": flip_last,
                    "flip_kind": flip_kind,
                    "candle_count": len(dq),
                    "macd_last": macd_last,
                    "closes_count": len(closes)
                }
                log(f"    ✓ OK: positive={positive}, flip={flip_kind}, macd_last={macd_last:. 6f}, candles={len(dq)}")
            except Exception as e: 
                log(f"    ✗ ANALYSIS ERROR: {e}")
                tf_status[tf] = {"has": False, "reason": f"analysis_error:{e}"}
                continue

        # ✅ Log full status
        log(f"  📊 Full TF Status: {tf_status}")

        # ✅ Check alignment conditions
        root_status = tf_status.get(root_tf, {})
        log(f"  Root TF {root_tf} status: has={root_status.get('has')}, flip_last={root_status.get('flip_last')}, positive={root_status.get('positive')}")

        if not root_status.get("has"):
            log(f"❌ MTF eval FAIL {symbol}: root TF {root_tf} missing or insufficient cache ({root_status.get('candles', 0)}/{MIN_CANDLES_REQUIRED})")
            return

        if not root_status.get("flip_last"):
            log(f"❌ MTF eval FAIL {symbol}: root TF {root_tf} flip not open (is {root_status.get('flip_kind')})")
            return

        other_tfs = [tf for tf in required_tfs if tf != root_tf]
        negatives = [tf for tf in other_tfs if not tf_status.get(tf, {}).get("positive")]

        alignment_ok = False
        alignment_reason = ""

        if len(negatives) == 0:
            alignment_ok = True
            alignment_reason = "ALL_POSITIVE"
            log(f"  ✅ Alignment:  All TFs positive")
        elif len(negatives) == 1:
            tf_to_flip = negatives[0]
            st = tf_status.get(tf_to_flip, {})
            log(f"  One negative TF {tf_to_flip}: has={st.get('has')}, flip_last={st.get('flip_last')}")

            if st.get("has") and st.get("flip_last"):
                remaining_positive = all(tf_status.get(tf, {}).get("positive") for tf in other_tfs if tf != tf_to_flip)
                if remaining_positive: 
                    alignment_ok = True
                    alignment_reason = f"ONE_FLIPPING({tf_to_flip})_REST_POSITIVE"
                    log(f"  ✅ Alignment: {tf_to_flip} flipping, others positive")
                else:
                    alignment_reason = f"ONE_FLIPPING_BUT_OTHERS_NEGATIVE"
                    log(f"  ❌ Alignment: {tf_to_flip} flipping but others negative")
            else:
                alignment_reason = f"ONE_NEGATIVE_NO_FLIP({tf_to_flip})"
                log(f"  ❌ Alignment: {tf_to_flip} negative and no flip")
        else:
            alignment_reason = f"TOO_MANY_NEGATIVE({len(negatives)})"
            log(f"  ❌ Alignment: Too many negative TFs:  {negatives}")

        if not alignment_ok:
            log(f"❌ MTF eval FAIL {symbol}: reason={alignment_reason}")
            return

        log(f"✅ MTF ALIGNMENT SUCCESS {symbol} {root_tf}:  {alignment_reason}")

        if signal_exists_for(symbol, "entry"):
            log(f"❌ MTF entry SKIP {symbol}: entry already exists")
            return

        if await has_open_trade(symbol):
            log(f"❌ MTF entry SKIP {symbol}: open trade exists")
            return

        # ✅ CREATE ENTRY SIGNAL
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
            "tf_status": tf_status,
        }
        log(f"📝 Adding entry signal: {entry_id}")
        added = await add_signal(entry_sig)
        if not added:
            log(f"❌ MTF entry SKIP {symbol}: failed to add entry signal")
            return

        log(f"✅ Entry signal added: {entry_id}")

        try:
            from services import send_telegram
            msg = f"✅ MTF ALIGNED:   {symbol} root={root_tf} {alignment_reason}"
            log(msg)
            await send_telegram(msg)
        except Exception as e:
            log(f"Telegram error: {e}")

        # ✅ EXECUTE TRADE
        lock = get_symbol_lock(symbol)
        async with lock:
            if await has_open_trade(symbol):
                log("MTF trade SKIP (post-lock):", symbol)
            else:
                dq5 = cache_get(symbol, TF_MAP["5m"])
                last_price = None
                if dq5 and len(dq5):
                    last_price = list(dq5)[-1]["close"]
                price = last_price or 1. 0
                balance = 1000.0
                per_trade = max(1.0, balance / max(1, MAX_OPEN_TRADES))
                qty = max(0.0001, round(per_trade / price, 6))
                side = "Buy"
                stop_price = round(price * (1 - STOP_LOSS_PCT), 8)

                if TRADING_ENABLED:
                    try:
                        import uuid
                        res = await bybit_signed_request("POST", "/v5/order/create", {"category": "linear", "symbol": symbol, "side": side, "orderType": "Market", "qty": qty})
                        oid = res.get("result", {}).get("orderId", str(uuid.uuid4()))
                        await persist_trade({"id": oid, "symbol": symbol, "side": side, "qty": qty, "entry_price": None, "sl_price": stop_price, "created_at": now_ts_ms(), "open":  True, "raw": res})
                        log("🚀 REAL ORDER:", symbol, qty, side)
                        from services import send_telegram
                        await send_telegram(f"🚀 REAL ORDER: {symbol} qty={qty} side={side}")
                    except Exception as e: 
                        log("Error placing real order:", e)
                else:
                    import uuid
                    oid = str(uuid.uuid4())
                    await persist_trade({"id":  oid, "symbol": symbol, "side": side, "qty":  qty, "entry_price": None, "sl_price": stop_price, "created_at": now_ts_ms(), "open": True, "raw": {"simulated": True}})
                    log("📊 SIMULATED TRADE:", symbol, qty, side)
                    from services import send_telegram
                    await send_telegram(f"📊 SIMULATED:   {symbol} qty={qty} side={side}")

                if entry_id in active_root_signals:
                    active_root_signals[entry_id]["status"] = "acted"
                    try:
                        await persist_root_signal(active_root_signals[entry_id])
                    except Exception: 
                        pass

    except Exception as e:
        log(f"❌ _evaluate_root_signal_for_mtf EXCEPTION: {e}")
        import traceback
        log(traceback.format_exc())


# ---------- Add / remove signals ----------
async def add_signal(sig: Dict[str, Any]) -> bool:
    """Add signal with validation and deduplication."""
    sid = sig["id"]
    stype = sig. get("signal_type", "root")
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
                if existing. get("symbol") == sym and existing.get("signal_type", "root") == stype:
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
                    # Allow mid-candle detection
                    flip_kind, _ = await detect_flip(sym, root_tf)
                    if flip_kind is None:
                        log(
                            "add_signal: last candle is closed for",
                            sym,
                            root_tf,
                            "rejecting root signal with flip_time",
                            root_time,
                        )
                        return False
                    if last_start != root_time:
                        log(
                            "add_signal: root_flip_time does not match current open candle for",
                            sym,
                            root_tf,
                            "expected",
                            last_start,
                            "got",
                            root_time,
                        )
                        return False
            except Exception as e:
                log("add_signal validation error:", e)
                return False
        if sid in active_root_signals:
            log("Signal id already present, skipping:", sid)
            return False
        active_root_signals[sid] = sig
        register_signal_index(sym, stype)
        if stype == "root":
            recent_root_signals[sym] = int(time.time())
            root_signal_symbols.add(sym)
        try:
            await persist_root_signal(sig)
        except Exception as e: 
            log("persist_root_signal error:", e)
        log("Added signal:", sid, stype, sym)
        try:
            from services import send_telegram
            await send_telegram(f"Added signal: {sid} {stype} {sym}")
        except Exception: 
            pass

        # ✅ NEW:  Immediately evaluate for MTF alignment if root signal
        if stype == "root": 
            try:
                asyncio.create_task(_evaluate_root_signal_for_mtf(sig))
            except Exception as e:
                log("MTF eval task creation error:", e)

    try:
        async def _post_add_flow():
            if stype == "root":
                try:
                    from services_config import public_ws
                    if public_ws:
                        try:
                            await public_ws.subscribe_kline(sym, TF_MAP["5m"])
                        except Exception as e: 
                            log("Error subscribing 5m for", sym, e)
                        try:
                            await public_ws.subscribe_kline(sym, TF_MAP["15m"])
                        except Exception as e:
                            log("Error subscribing 15m for", sym, e)
                    try:
                        await asyncio.gather(
                            ensure_cached_candles(sym, "5m", MIN_CANDLES_REQUIRED),
                            ensure_cached_candles(sym, "15m", MIN_CANDLES_REQUIRED),
                        )
                    except Exception as e:
                        log("Prewarm small TFs error for", sym, e)
                except Exception as e:
                    log("post_add subscribe/prewarm error:", e)
            try:
                from services import notify_alignment_if_ready
                await notify_alignment_if_ready(sig)
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
    sym = sig. get("symbol")
    if stype == "root":
        try:
            recent_root_signals.pop(sym, None)
            root_signal_symbols.discard(sym)
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
                x. get("symbol") == sym and x.get("signal_type") == "root"
                for x in active_root_signals.values()
            )
            from services_config import public_ws
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


# ---------- SQLite init & persistence ----------
async def init_db():
    """Initialize SQLite database."""
    from services_config import db as db_conn
    import aiosqlite
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute(
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
            created_at INTEGER
        )"""
    )
    await db_conn.execute(
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
    await db_conn.execute(
        """CREATE TABLE IF NOT EXISTS raw_ws_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            topic TEXT,
            message TEXT,
            created_at INTEGER
        )"""
    )
    await db_conn.execute(
        """CREATE TABLE IF NOT EXISTS public_subscriptions (
            topic TEXT PRIMARY KEY,
            created_at INTEGER
        )"""
    )
    await db_conn.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS ux_root_signals_symbol_tf_flip
           ON root_signals(symbol, root_tf, flip_time)"""
    )
    await db_conn.commit()
    log("DB initialized at", DB_PATH)


async def persist_root_signal(sig:  Dict[str, Any]):
    """Persist root signal to database."""
    from services_config import db as db_conn
    comps = json.dumps(sig.get("components") or [])
    try:
        await db_conn. execute(
            """INSERT OR IGNORE INTO root_signals
               (id,symbol,root_tf,flip_time,flip_price,status,priority,signal_type,components,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                sig["id"],
                sig["symbol"],
                sig. get("root_tf"),
                sig.get("root_flip_time") or sig.get("flip_time"),
                sig.get("root_flip_price") or sig.get("flip_price"),
                sig. get("status", "watching"),
                sig.get("priority"),
                sig.get("signal_type", "root"),
                comps,
                sig.get("created_at"),
            ),
        )
        await db_conn.commit()
    except Exception as e:
        log("persist_root_signal DB error:", e)


async def persist_trade(trade:  Dict[str, Any]):
    """Persist trade to database."""
    from services_config import db as db_conn
    try: 
        await db_conn.execute(
            """INSERT OR REPLACE INTO trades
               (id,symbol,side,qty,entry_price,sl_price,created_at,open,raw_response)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                trade["id"],
                trade["symbol"],
                trade["side"],
                trade["qty"],
                trade. get("entry_price"),
                trade.get("sl_price"),
                trade["created_at"],
                trade. get("open", True),
                json.dumps(trade.get("raw", {})),
            ),
        )
        await db_conn.commit()
    except Exception as e: 
        log("persist_trade DB error:", e)


async def remove_root_signal(sig_id: str):
    """Remove root signal from database."""
    from services_config import db as db_conn
    try:
        await db_conn.execute("DELETE FROM root_signals WHERE id = ?", (sig_id,))
        await db_conn.commit()
    except Exception as e: 
        log("remove_root_signal DB error:", e)


# ---------- Tradable symbols ----------
async def get_tradable_usdt_symbols() -> List[str]:
    """Get list of tradable USDT symbols."""
    from services_config import symbols_info_cache, INSTRUMENTS_ENDPOINTS, resilient_public_get
    global symbols_info_cache

    # Use cache if available
    if symbols_info_cache and len(symbols_info_cache) > 100:
        return [s for s in symbols_info_cache. keys() if s.endswith("USDT")]

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
        log("get_tradable_usdt_symbols parse error:", e)
        return []


# ---------- Open trades ----------
async def has_open_trade(symbol: str) -> bool:
    """Check if symbol has open trade."""
    from services_config import db as db_conn
    try:
        async with db_conn.execute("SELECT id FROM trades WHERE symbol = ?  AND open = ? ", (symbol, True)) as cur:
            row = await cur.fetchone()
        return row is not None
    except Exception: 
        return False
