# services.py - Telegram, alignment checks, and FastAPI endpoints
# COMPLETE FILE - Import this in your FastAPI app

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from fastapi import Depends, Header, HTTPException, status

from services_config import (
    log, now_ts_ms, httpx_client, TF_MAP, MIN_CANDLES_REQUIRED,
    active_root_signals, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    TELEGRAM_QUEUE, TELEGRAM_RETRY_LIMIT, ADMIN_API_KEY,
    cache_get, candles_to_closes, macd_positive_from_closes, macd_hist,
    interval_seconds_from_token, signal_exists_for, is_stablecoin_symbol,
    tf_list_for_root, detect_flip, last_candle_is_closed, public_ws,
    SCAN_INTERVAL_SECONDS, observed_flip_registry, BYBIT_USE_MAINNET,
    TRADING_ENABLED, PUBLIC_WS_URL, INSTRUMENTS_ENDPOINTS, resilient_public_get,
    candles_cache_ts, ensure_cached_candles, bybit_signed_request,
    MAX_OPEN_TRADES, STOP_LOSS_PCT, db
)

from services_signals import (
    add_signal, remove_signal, init_db, get_tradable_usdt_symbols,
    has_open_trade, load_persisted_root_signals, _notify_loaded_roots_after_startup,
    persist_root_signal, persist_trade
)


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
                if r. status_code == 200:
                    try:
                        data = r.json()
                    except Exception:
                        data = {"raw_text": (r.text[: 400] + "...") if r.text else ""}
                    log("Telegram sent ok (worker) response:", data)
                elif r.status_code == 429:
                    retry_after = 5
                    try:
                        data = r. json()
                        retry_after = int(data. get("parameters", {}).get("retry_after", retry_after))
                    except Exception:
                        try:
                            retry_after = int(r.headers.get("Retry-After", retry_after))
                        except Exception:
                            pass
                    log("Telegram send failed:  429 rate limit, retry after", retry_after)
                    await asyncio.sleep(retry_after)
                    if attempt + 1 < TELEGRAM_RETRY_LIMIT:
                        await TELEGRAM_QUEUE.put((text, attempt + 1))
                    else:
                        log("Telegram send retry limit reached, dropping message")
                else:
                    log("Telegram send failed:", r.status_code, r.text)
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


_TELEGRAM_WORKER_TASK:  Optional[asyncio.Task] = None


async def send_telegram(text: str):
    """Enqueue a Telegram message."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram not configured; skipping message:", text)
        return
    global _TELEGRAM_WORKER_TASK
    if _TELEGRAM_WORKER_TASK is None or _TELEGRAM_WORKER_TASK.done():
        try:
            _TELEGRAM_WORKER_TASK = asyncio. create_task(_telegram_worker())
        except Exception: 
            pass
    try:
        await TELEGRAM_QUEUE.put((text, 0))
    except Exception as e:
        log("Failed to enqueue telegram message:", e)


async def send_root_signals_telegram():
    """Send summary of active root signals via Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: 
        log("Telegram not configured; skipping root signals telegram")
        return
    if not active_root_signals: 
        await send_telegram("Root signals: 0")
        return
    lines = ["Root signals summary: "]
    for sig in active_root_signals. values():
        sym = sig.get("symbol")
        tf = sig.get("root_tf")
        status = sig.get("status", "watching")
        lines.append(f"{sym} {tf} {status}")
    await send_telegram("\n".join(lines))


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
        token = x_api_key. strip()
    if not token or token != ADMIN_API_KEY: 
        log("Admin auth failed")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")


# ---------- Alignment check ----------
async def notify_alignment_if_ready(sig: Dict[str, Any]):
    """Check if signal has reached alignment/entry conditions."""
    try:
        symbol = sig.get("symbol")
        root_tf = sig.get("root_tf")
        stype = sig.get("signal_type")

        if stype != "root" or not symbol or not root_tf:
            return

        if signal_exists_for(symbol, "entry"):
            return

        required_tfs = tf_list_for_root(root_tf)

        tf_status = {}
        for tf in required_tfs:
            token = TF_MAP[tf]
            dq = cache_get(symbol, token)
            if not dq or len(dq) < MIN_CANDLES_REQUIRED:
                tf_status[tf] = {"has":  False}
                continue
            closes = candles_to_closes(dq)
            positive = macd_positive_from_closes(closes)
            flip_kind, _ = await detect_flip(symbol, tf)
            flip_last = (flip_kind == "open")
            tf_status[tf] = {"has": True, "positive": positive, "flip_last": flip_last, "flip_kind": flip_kind}

        if not tf_status. get(root_tf, {}).get("has"):
            log(f"Alignment check FAIL {symbol} {root_tf}: root TF missing cache")
            return
        if not tf_status.get(root_tf, {}).get("flip_last"):
            log(f"Alignment check FAIL {symbol} {root_tf}: root TF flip not open")
            return

        log(f"Alignment check SUCCESS for {symbol} {root_tf}:  conditions met")
        log(f"  TF Status: {tf_status}")
    except Exception as e:
        log(f"notify_alignment_if_ready error:  {e}")


# ---------- FastAPI app initialization ----------
def init_app(fastapi_app):
    """Register endpoints and startup event."""
    @fastapi_app.get("/debug/current_roots")
    async def debug_current_roots(_auth=Depends(require_admin_auth)):
        return {"count": len(active_root_signals), "signals": list(active_root_signals.values())}

    @fastapi_app.get("/debug/check_symbol")
    async def debug_check_symbol(symbol: str, tf: str = "1h", _auth=Depends(require_admin_auth)):
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

    @fastapi_app.post("/admin/send_root_summary")
    async def admin_send_root_summary(_auth=Depends(require_admin_auth)):
        await send_root_signals_telegram()
        return {"status": "ok", "sent": True}

    @fastapi_app.get("/")
    async def root_route():
        return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}

    @fastapi_app. get("/health")
    async def health():
        db_ok = db is not None
        public_ws_connected = False
        try: 
            public_ws_connected = bool(public_ws and getattr(public_ws, "conn", None))
        except Exception: 
            public_ws_connected = False
        return {
            "status": "ok",
            "db":  db_ok,
            "public_ws_connected": public_ws_connected,
            "time":  datetime.now(timezone.utc).isoformat(),
        }

    @fastapi_app. post("/debug/ws/subscribe_test")
    async def debug_ws_subscribe_test(symbol:  str, tf: str = "15m", _auth=Depends(require_admin_auth)):
        """Subscribe test endpoint."""
        if tf not in TF_MAP: 
            raise HTTPException(status_code=400, detail=f"Unknown tf {tf}. Valid: {list(TF_MAP. keys())}")
        if not public_ws:
            raise HTTPException(status_code=500, detail="public_ws not initialized")
        token = TF_MAP[tf]
        topic = f"klineV2.{token}. {symbol}"
        try: 
            await public_ws. subscribe_kline(symbol, token)
            try:
                await ensure_cached_candles(symbol, tf, MIN_CANDLES_REQUIRED)
            except Exception: 
                pass
            return {"status": "ok", "subscribed":  topic}
        except Exception as e:
            log("debug_ws_subscribe_test error:", e)
            raise HTTPException(status_code=500, detail=f"subscribe failed: {e}")

    @fastapi_app.on_event("startup")
    async def startup_event():
        global _TELEGRAM_WORKER_TASK
        await init_db()
        await load_persisted_root_signals()
        log(
            "Startup config:",
            "BYBIT_USE_MAINNET=", BYBIT_USE_MAINNET,
            "TRADING_ENABLED=", TRADING_ENABLED,
            "PUBLIC_WS_URL=", PUBLIC_WS_URL,
            "MIN_CANDLES_REQUIRED=", MIN_CANDLES_REQUIRED,
            "SCAN_INTERVAL_SECONDS=", SCAN_INTERVAL_SECONDS,
        )

        try:
            import scanners as _sc
            globals()["process_inprogress_update"] = _sc.process_inprogress_update
            from services_config import public_ws as pw
            pw = _sc.PublicWebsocketManager(PUBLIC_WS_URL)
            ok = await pw.connect_and_detect(timeout=6. 0)
            if not ok: 
                log("Public WS detect warning")
        except Exception: 
            log("Public WS connect error")

        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and (_TELEGRAM_WORKER_TASK is None or _TELEGRAM_WORKER_TASK.done()):
            try:
                _TELEGRAM_WORKER_TASK = asyncio. create_task(_telegram_worker())
            except Exception:
                pass

        try:
            import scanners as _sc

            # Start continuous kline fetchers for root TFs
            asyncio.create_task(_sc._continuous_kline_fetcher("1h"))
            asyncio.create_task(_sc._continuous_kline_fetcher("4h"))

            # Start scanner loops for root TF detection
            asyncio.create_task(_sc.root_scanner_loop("1h"))
            asyncio. create_task(_sc.root_scanner_loop("4h"))

            # Start evaluation/signal loops (alignment check & trade)
            asyncio.create_task(_sc.evaluate_signals_loop())
            asyncio. create_task(_sc.expire_signals_loop())
            asyncio. create_task(_sc.periodic_root_logger())

            # Start continuous subscription maintenance for root signals
            asyncio.create_task(_sc._continuous_root_signal_subscriptions())
        except Exception as e:
            log("Error starting scanner background tasks:", e)

        try:
            asyncio.create_task(_notify_loaded_roots_after_startup())
        except Exception as e: 
            log("Failed to schedule notify_loaded_roots_after_startup:", e)

        log("Background tasks started successfully")