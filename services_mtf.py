# services_mtf.py
# Simplified MTF logic: MACD helpers, detect_flip, flip stability,
# check_signal_alignment (single-shot, idempotent), and monitoring loop.
# Registers notify callback with core and exposes start function.

import time
import asyncio
from typing import List, Dict, Any, Optional

# Import core as a module (core provides DB, cache, send_telegram, add_signal, etc.)
import services_core as core


# ---------- EMA / MACD ----------
def ema(values: List[float], period: int) -> List[float]:
    """Calculate Exponential Moving Average."""
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


def macd_hist(prices: List[float], fast: int = core.MACD_FAST, slow: int = core.MACD_SLOW, signal: int = core.MACD_SIGNAL) -> List[Optional[float]]:
    """Calculate MACD histogram."""
    if len(prices) < slow + signal:
        return [None] * len(prices)

    ema_fast = ema(prices, fast)
    ema_slow = ema(prices, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = ema(macd_line, signal)

    hist = [None] * len(prices)
    start_idx = len(prices) - len(signal_line)
    for i, (m, s) in enumerate(zip(macd_line[start_idx:], signal_line)):
        hist[start_idx + i] = m - s

    return hist


def macd_positive_from_closes(closes: List[float]) -> bool:
    """Check if MACD histogram is positive (last value > 0)."""
    if len(closes) < core.MACD_SLOW + core.MACD_SIGNAL:
        return False
    hist = macd_hist(closes)
    if not hist or len(hist) == 0:
        return False
    last_hist = hist[-1]
    return last_hist is not None and last_hist > 0


# ---------- Detect flip ----------
async def detect_flip(symbol: str, tf: str) -> (Optional[str], Optional[int]):
    """Detect MACD flip on open candles (returns 'open' and start timestamp if open flip)."""
    token = core.TF_MAP.get(tf) if tf in core.TF_MAP else tf
    if token is None:
        token = core.TF_MAP.get(tf, tf)

    await core.ensure_cached_candles(symbol, tf, core.MIN_CANDLES_REQUIRED)
    dq = core.cache_get(symbol, token)
    if not dq or len(dq) < core.MIN_CANDLES_REQUIRED:
        return None, None

    closes = core.candles_to_closes(dq)
    if len(closes) < core.MACD_SLOW + core.MACD_SIGNAL:
        return None, None

    hist = macd_hist(closes)
    if not hist or len(hist) < 2:
        return None, None

    last_start = list(dq)[-1]["start"]

    is_closed = core.last_candle_is_closed(dq, token, safety_seconds=3)
    if is_closed:
        core.observed_flip_registry.pop((symbol, token, last_start), None)
        return None, None

    prev = hist[-2] or 0
    cur = hist[-1] or 0

    if prev <= 0 and cur > 0:
        now_s = int(time.time())
        key = (symbol, token, last_start)
        rec = core.observed_flip_registry.get(key)
        if rec is None:
            core.observed_flip_registry[key] = {"first_seen": now_s, "last_seen": now_s, "count": 1}
        else:
            rec["last_seen"] = now_s
            rec["count"] = rec.get("count", 0) + 1
            core.observed_flip_registry[key] = rec
        return "open", last_start

    return None, None


def flip_is_stable_enough(symbol: str, tf: str, start: int) -> bool:
    """Check if flip is stable enough based on FLIP_STABILITY_SECONDS."""
    if core.FLIP_STABILITY_SECONDS <= 0:
        return True
    token = core.TF_MAP.get(tf) if tf in core.TF_MAP else tf
    if token is None and tf in core.REVERSE_TF_MAP:
        token = tf
    if token is None:
        token = core.TF_MAP.get(tf, tf)
    key = (symbol, token, start)
    rec = core.observed_flip_registry.get(key)
    if not rec:
        return False
    return (int(time.time()) - int(rec["first_seen"])) >= core.FLIP_STABILITY_SECONDS


# ---------- Simplified MTF alignment check ----------
async def check_signal_alignment(sig: Dict[str, Any]):
    """
    Simplified alignment checker:
    - Verifies root flip is open
    - Ensures caches for all required TFs exist
    - If all TFs positive and signal not yet alerted -> send telegram, create entry signal, mark alerted
    - Idempotent and safe to call repeatedly
    """
    try:
        signal_id = sig.get("id")
        symbol = sig.get("symbol")
        root_tf = sig.get("root_tf")
        stype = sig.get("signal_type", "root")

        core.log(f"[MTF-SIMPLE] Checking alignment for {symbol} {root_tf} id={signal_id}")

        if stype != "root" or not symbol or not root_tf:
            core.log("[MTF-SIMPLE] Skip: invalid signal", {"id": signal_id, "signal_type": stype, "symbol": symbol, "root_tf": root_tf})
            return

        # Quick check: already alerted?
        in_mem = core.active_root_signals.get(signal_id)
        if in_mem and in_mem.get("mtf_alerted"):
            core.log("[MTF-SIMPLE] Skip: already alerted for", signal_id)
            return

        # Ensure root flip is open
        root_token = core.TF_MAP.get(root_tf)
        if not root_token:
            core.log("[MTF-SIMPLE] Skip: root_tf token missing", root_tf)
            return

        dq_root = core.cache_get(symbol, root_token)
        if not dq_root or len(dq_root) < core.MIN_CANDLES_REQUIRED:
            core.log("[MTF-SIMPLE] Skip: root cache missing/too small", {"symbol": symbol, "root_tf": root_tf, "have": 0 if not dq_root else len(dq_root)})
            return

        flip_kind, flip_ts = await detect_flip(symbol, root_tf)
        if flip_kind != "open":
            core.log("[MTF-SIMPLE] Skip: root flip not open for", {"symbol": symbol, "root_tf": root_tf, "flip_kind": flip_kind})
            return

        # Check all TFs
        required_tfs = ["5m", "15m", "1h", "4h", "1d"]
        tf_status = {}
        for tf in required_tfs:
            token = core.TF_MAP.get(tf)
            if not token:
                tf_status[tf] = {"has": False, "positive": False}
                continue
            await core.ensure_cached_candles(symbol, tf, core.MIN_CANDLES_REQUIRED)
            dq = core.cache_get(symbol, token)
            if not dq or len(dq) < core.MIN_CANDLES_REQUIRED:
                tf_status[tf] = {"has": False, "positive": False}
                continue
            closes = core.candles_to_closes(dq)
            positive = macd_positive_from_closes(closes)
            tf_status[tf] = {"has": True, "positive": positive}

        missing = [t for t, s in tf_status.items() if not s["has"]]
        negative = [t for t, s in tf_status.items() if s["has"] and not s["positive"]]

        core.log(f"[MTF-SIMPLE] {symbol} missing={missing if missing else 'none'} negative={negative if negative else 'none'}")

        if missing:
            # Not ready yet: caches missing -> let monitor loop re-check
            core.log("[MTF-SIMPLE] Early return: missing TF caches", {"symbol": symbol, "missing": missing})
            return

        if negative:
            # Not aligned yet
            core.log("[MTF-SIMPLE] Early return: some TFs negative", {"symbol": symbol, "negative": negative})
            return

        # All TFs positive -> attempt to fire alert once
        lock = core.get_symbol_lock(symbol)
        async with lock:
            current = core.active_root_signals.get(signal_id)
            if not current:
                core.log("[MTF-SIMPLE] Signal removed before alert", signal_id)
                return
            if current.get("mtf_alerted"):
                core.log("[MTF-SIMPLE] Already alerted inside lock", signal_id)
                return

            # Send telegram and create entry signal
            last_tf_msg = ""  # we don't track last flip in simplified flow
            msg = f"✅ MTF ENTRY: {symbol} {root_tf} — All aligned{last_tf_msg}"
            try:
                await core.send_telegram(msg)
            except Exception as e:
                core.log("[MTF-SIMPLE] send_telegram error:", e)

            # Mark alerted (in-memory + DB)
            current["mtf_alerted"] = True
            try:
                await core.update_root_mtf_alerted(signal_id, True)
            except Exception as e:
                core.log("[MTF-SIMPLE] update_root_mtf_alerted error:", e)

            # Create entry signal (signal_type = "entry")
            entry_id = f"ENTRY-{symbol}-{root_tf}-{core.now_ts_ms()}"
            entry_sig = {
                "id": entry_id,
                "symbol": symbol,
                "root_tf": root_tf,
                "root_flip_time": flip_ts,
                "root_flip_price": current.get("root_flip_price") or 0,
                "created_at": core.now_ts_ms(),
                "status": "entry",
                "priority": None,
                "signal_type": "entry",
                "components": [],
                "flip_kind": "open",
            }

            try:
                added = await core.add_signal(entry_sig)
                core.log("[MTF-SIMPLE] Entry signal add result:", added)
            except Exception as e:
                core.log("[MTF-SIMPLE] Error creating entry signal:", e)

    except Exception as e:
        core.log("[MTF-SIMPLE] EXCEPTION in check_signal_alignment:", e)
        import traceback
        core.log(traceback.format_exc())


# Register simplified notify function with core for compatibility
try:
    core.register_mtf_notify_callback(check_signal_alignment)
except Exception:
    core.log("services_mtf: registering MTF callback failed at import time")


# ---------- Monitoring loop ----------
async def mtf_monitoring_loop():
    """Periodic loop that checks active root signals for alignment."""
    core.log("[MTF-SIMPLE-LOOP] Started")
    while True:
        try:
            to_check = [sig for sig in list(core.active_root_signals.values()) if sig.get("signal_type") == "root" and not sig.get("mtf_alerted")]
            if to_check:
                core.log(f"[MTF-SIMPLE-LOOP] Checking {len(to_check)} root signals")
                for sig in to_check:
                    try:
                        await check_signal_alignment(sig)
                    except Exception as e:
                        core.log("[MTF-SIMPLE-LOOP] check_signal_alignment error:", e)
            await asyncio.sleep(core.MTF_MONITORING_INTERVAL)
        except Exception as e:
            core.log("[MTF-SIMPLE-LOOP] error:", e)
            await asyncio.sleep(core.MTF_MONITORING_INTERVAL)


def start_mtf_monitoring_loop() -> asyncio.Task:
    """Create and return background task for monitoring loop."""
    task = asyncio.create_task(mtf_monitoring_loop())
    return task
