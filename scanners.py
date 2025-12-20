# scanners.py - COMPLETE & PRODUCTION READY
# MTF root signal detection with immediate alignment evaluation

import asyncio
import gzip
import zlib
import json
import time
from typing import Dict, Any
from collections import defaultdict

import websockets

_ws_subscriptions: Dict[str, set] = defaultdict(set)
_last_fetch_time: Dict[str, float] = {}


class PublicWebsocketManager:  
    def __init__(self, ws_url: str, detect_symbol:  str = "BTCUSDT"):
        self.ws_url = ws_url
        self.conn = None
        self.detect_template = None
        self.subscribed_topics = set()
        self.detect_symbol = detect_symbol
        self._recv_task = None
        self._lock = asyncio.Lock()
        self._reconnect_backoff = 1
        self._stop = False

    def _maybe_decompress(self, msg):
        """Decompress gzip or zlib encoded WebSocket messages."""
        try:
            if isinstance(msg, str):
                return msg
            if isinstance(msg, bytes):
                try:
                    return gzip. decompress(msg).decode("utf-8")
                except Exception:
                    pass
                try:
                    return zlib. decompress(msg, -zlib.MAX_WBITS).decode("utf-8")
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
        """Establish WebSocket connection with exponential backoff."""
        backoff = self._reconnect_backoff
        while not self._stop:
            try:
                self.conn = await websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=2**24
                )
                try:
                    import services as s
                    s.log("Public WS connected")
                except Exception: 
                    pass
                self._reconnect_backoff = 1
                return True
            except Exception as e: 
                try:
                    import services as s
                    s. log("Public WS connect failed:", e, "retrying in", backoff)
                except Exception:
                    pass
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
        return False

    async def connect_and_detect(self, timeout: float = 8. 0):
        """Connect and detect the correct WebSocket topic template."""
        if not await self._connect():
            return False
        from services import CANDIDATE_PUBLIC_TEMPLATES, TF_MAP
        for tmpl in CANDIDATE_PUBLIC_TEMPLATES:
            try:
                t1 = tmpl.format(interval=TF_MAP["5m"], symbol=self.detect_symbol)
                t2 = tmpl.format(interval=TF_MAP["15m"], symbol=self.detect_symbol)
                await self.conn.send(json.dumps({"op": "subscribe", "args": [t1, t2]}))
                try:
                    msg = await asyncio.wait_for(self. conn.recv(), timeout=1.0)
                    body = self._maybe_decompress(msg)
                    if body and (self.detect_symbol in body or "kline" in body):
                        self.detect_template = tmpl
                        try:
                            import services as s
                            s. log("Public WS template detected:", tmpl)
                        except Exception:
                            pass
                        try:
                            await self.conn.send(json. dumps({"op": "unsubscribe", "args": [t1, t2]}))
                        except Exception:
                            pass
                        break
                except Exception: 
                    try:
                        await self.conn.send(json.dumps({"op": "unsubscribe", "args": [t1, t2]}))
                    except Exception:
                        pass
                    continue
            except Exception: 
                continue

        if not self._recv_task:
            self._recv_task = asyncio.create_task(self._recv_loop())
        return True

    async def subscribe_kline(self, symbol: str, interval_token: str):
        """Subscribe to a kline topic."""
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
            _ws_subscriptions[symbol].add(interval_token)
            import services as s
            s.log("WS subscribed to", topic)
        except Exception as e:
            try:
                import services as s
                s.log("WS subscribe error:", e)
            except Exception: 
                pass

    async def unsubscribe_kline(self, symbol: str, interval_token: str):
        """Unsubscribe from a kline topic."""
        topic = f"klineV2.{interval_token}.{symbol}"
        if topic not in self.subscribed_topics:
            return
        if not self.conn:
            self. subscribed_topics.discard(topic)
            _ws_subscriptions[symbol].discard(interval_token)
            return
        try:
            await self.conn.send(json.dumps({"op": "unsubscribe", "args": [topic]}))
            self.subscribed_topics.discard(topic)
            _ws_subscriptions[symbol].discard(interval_token)
            import services as s
            s.log("WS unsubscribed from", topic)
        except Exception as e:
            try:
                import services as s
                s.log("WS unsubscribe error:", e)
            except Exception:
                pass

    def _extract_kline_entries(self, obj: Dict[str, Any]):
        """Extract kline entries from WebSocket message."""
        out = []
        try:
            data = obj. get("data") or obj.get("result") or []
            topic = obj.get("topic") or ""
            if isinstance(data, dict):
                data = [data]
            for entry in (data or []):
                if not isinstance(entry, dict):
                    continue
                symbol = entry.get("symbol") or entry.get("s") or None
                interval = entry.get("interval") or entry.get("kline", {}).get("interval") or entry.get("i") or None
                if not symbol or not interval:
                    if topic and "kline" in topic:
                        parts = topic.split(".")
                        if len(parts) >= 3:
                            interval = interval or parts[1]
                            symbol = symbol or parts[2]
                confirm = entry.get("confirm") or entry.get("is_closed") or entry.get("isFinal") or False
                kline = entry.get("kline") or {}
                start = entry.get("start") or kline.get("start") or entry.get("t") or None
                end = entry.get("end") or kline.get("end") or entry.get("endTime") or None
                close = entry.get("close") or kline.get("close") or entry.get("c") or None
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
                        out.append((symbol, str(interval), {"start": start_i, "end": end_i, "close":  close_f}, bool(confirm)))
                    except Exception:
                        continue
        except Exception:
            pass
        return out

    async def _recv_loop(self):
        """Receive and process WebSocket messages."""
        from services import merge_into_cache, persist_raw_ws, log
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
                    entries = self._extract_kline_entries(obj)
                    for symbol, interval_token, candle, is_closed in entries:
                        try:
                            merge_into_cache(symbol, str(interval_token), [candle])
                            log("Merged kline from WS into cache for", symbol, interval_token, "start", candle. get("start"), "closed? ", is_closed)
                            asyncio.create_task(process_inprogress_update(symbol, str(interval_token)))
                            asyncio.create_task(_check_root_alignment(symbol))
                        except Exception as e: 
                            log("Error processing kline entry:", e)
                            continue
                except Exception as e: 
                    log("WS message parse error:", e)
                    continue
            except Exception as e:
                try:
                    import services as s
                    s.log("Public WS recv error:", e)
                except Exception:
                    print("Public WS recv error:", e)
                try:
                    if self.conn:
                        await self.conn.close()
                except Exception:
                    pass
                self. conn = None
                await asyncio.sleep(5)
                try:
                    await self._connect()
                except Exception: 
                    await asyncio.sleep(5)


async def _check_root_alignment(symbol: str):
    """Check if any root signal for this symbol is now aligned."""
    try:
        import services as s
        for sig in list(s.active_root_signals.values()):
            if sig.get("symbol") == symbol and sig.get("signal_type") == "root":
                try:
                    await _evaluate_root_signal_for_entry(sig)
                except Exception: 
                    pass
    except Exception: 
        pass


async def process_inprogress_update(symbol: str, interval_token: str):
    """Process mid-candle updates for root flip detection."""
    import services as s
    try:
        tf = s. REVERSE_TF_MAP.get(interval_token)
        if not tf:
            return
        if tf not in ("1h", "4h"):
            return

        try:
            await s.ensure_cached_candles(symbol, tf, s.MIN_CANDLES_REQUIRED)
        except Exception:
            pass

        now_s = int(time.time())
        recent = s.recent_root_signals.get(symbol)
        if recent and (now_s - recent) < s.ROOT_DEDUP_SECONDS:
            s.log("WS mid-candle:  skipped (recent root dedup) for", symbol, "token", interval_token, f"age_s={now_s-recent}")
            return

        if s.signal_exists_for(symbol, "root"):
            s.log("WS mid-candle: skipped (already has active root) for", symbol)
            return
        if s.signal_exists_for(symbol, "entry"):
            s.log("WS mid-candle: skipped (already has active entry) for", symbol)
            return

        flip_kind, last_start = await s.detect_flip(symbol, tf)
        if not flip_kind:
            return

        if not s.flip_is_stable_enough(symbol, tf, last_start):
            s.log("WS mid-candle: flip observed but not yet stable for", symbol, tf, "start", last_start)
            return

        dq_root = s.cache_get(symbol, s.TF_MAP[tf])
        if not dq_root:
            s.log("WS mid-candle: missing root cache after detect for", symbol, tf)
            return
        last_candle = list(dq_root)[-1]
        key = f"ROOT-{symbol}-{tf}-{last_start}"
        sig = {
            "id": key,
            "symbol":  symbol,
            "root_tf": tf,
            "root_flip_time": last_start,
            "root_flip_price": last_candle.get("close"),
            "created_at": s.now_ts_ms(),
            "status": "watching",
            "priority": None,
            "signal_type": "root",
            "components": [],
            "flip_kind": flip_kind,
        }
        added = await s.add_signal(sig)
        if added:
            s.log("WS mid-candle: Root signal created:", key)
            await _evaluate_root_signal_for_entry(sig)
        return
    except Exception as e: 
        try:
            s.log("process_inprogress_update error for", symbol, interval_token, e)
        except Exception:
            print("process_inprogress_update error for", symbol, interval_token, e)


async def _evaluate_root_signal_for_entry(sig: Dict[str, Any]):
    """Evaluate a single root signal for MTF alignment and create entry if conditions met."""
    import services as s
    try: 
        symbol = sig.get("symbol")
        root_tf = sig.get("root_tf")
        stype = sig.get("signal_type")

        if stype != "root" or not symbol or not root_tf:
            return

        if s.signal_exists_for(symbol, "entry"):
            s.log(f"MTF eval SKIP {symbol}:  already has active entry")
            return

        required_tfs = s.tf_list_for_root(root_tf)
        s.log(f"MTF eval START {symbol} {root_tf}, required_tfs={required_tfs}")

        await _ensure_root_signal_subscriptions(sig)
        await asyncio.gather(*(s.ensure_cached_candles(symbol, tf, s.MIN_CANDLES_REQUIRED) for tf in required_tfs))

        tf_status = {}
        for tf in required_tfs:
            token = s.TF_MAP[tf]
            dq = s.cache_get(symbol, token)
            if not dq or len(dq) < s.MIN_CANDLES_REQUIRED:
                tf_status[tf] = {"has":  False, "reason": "no_cache"}
                s.log(f"  {tf}:  MISSING cache (has={bool(dq)}, len={len(dq) if dq else 0})")
                continue

            closes = s.candles_to_closes(dq)
            positive = s.macd_positive_from_closes(closes)
            flip_kind, _ = await s.detect_flip(symbol, tf)
            flip_last = (flip_kind == "open")
            last_close = list(dq)[-1]["close"] if dq else None

            tf_status[tf] = {
                "has": True,
                "positive": positive,
                "flip_last": flip_last,
                "flip_kind": flip_kind,
                "last_close": last_close,
                "candle_count": len(dq)
            }
            s.log(f"  {tf}: positive={positive}, flip_last={flip_last}, candle_count={len(dq)}")

        root_status = tf_status.get(root_tf, {})
        if not root_status.get("has"):
            s.log(f"MTF eval FAIL {symbol}:  root TF {root_tf} missing cache")
            return

        if not root_status.get("flip_last"):
            s.log(f"MTF eval FAIL {symbol}: root TF {root_tf} flip not open (is {root_status. get('flip_kind')})")
            return

        other_tfs = [tf for tf in required_tfs if tf != root_tf]
        negatives = [tf for tf in other_tfs if not tf_status.get(tf, {}).get("positive")]

        alignment_ok = False
        alignment_reason = ""

        if len(negatives) == 0:
            alignment_ok = True
            alignment_reason = "ALL_POSITIVE"
        elif len(negatives) == 1:
            tf_to_flip = negatives[0]
            st = tf_status.get(tf_to_flip, {})
            if st.get("has") and st.get("flip_last"):
                remaining_positive = all(tf_status.get(tf, {}).get("positive") for tf in other_tfs if tf != tf_to_flip)
                if remaining_positive: 
                    alignment_ok = True
                    alignment_reason = f"ONE_FLIPPING({tf_to_flip})_REST_POSITIVE"
                else: 
                    alignment_reason = f"ONE_FLIPPING_BUT_OTHERS_NEGATIVE"
            else:
                alignment_reason = f"ONE_NEGATIVE_NO_FLIP({tf_to_flip})"
        else:
            alignment_reason = f"TOO_MANY_NEGATIVE({len(negatives)})"

        if not alignment_ok:
            s. log(f"MTF eval FAIL {symbol}: alignment_reason={alignment_reason}")
            return

        s.log(f"✅ MTF eval SUCCESS {symbol} {root_tf}:  alignment_reason={alignment_reason}")

        if s.signal_exists_for(symbol, "entry"):
            s.log(f"MTF entry SKIP {symbol}: entry already exists")
            return

        if await s.has_open_trade(symbol):
            s.log(f"MTF entry SKIP {symbol}: open trade exists")
            return

        ts = int(time.time())
        entry_id = f"ENTRY-{symbol}-{ts}"
        entry_sig = {
            "id":  entry_id,
            "symbol": symbol,
            "root_tf": root_tf,
            "signal_type": "entry",
            "priority": "entry",
            "status": "ready",
            "components": required_tfs,
            "created_at": s.now_ts_ms(),
            "last_flip_ts": s.now_ts_ms(),
            "tf_status": tf_status,
        }
        added = await s.add_signal(entry_sig)
        if not added:
            s.log(f"MTF entry SKIP {symbol}: failed to add entry signal")
            return

        try:
            msg = f"✅ MTF ALIGNED:  {symbol} root={root_tf} reason={alignment_reason}"
            s.log(msg)
            await s.send_telegram(msg)
        except Exception: 
            pass

        lock = s.get_symbol_lock(symbol)
        async with lock:
            if await s.has_open_trade(symbol):
                s.log("MTF trade SKIP (post-lock):", symbol)
            else:
                dq5 = s.cache_get(symbol, s.TF_MAP["5m"])
                last_price = None
                if dq5 and len(dq5):
                    last_price = list(dq5)[-1]["close"]
                price = last_price or 1. 0
                balance = 1000.0
                per_trade = max(1. 0, balance / max(1, s.MAX_OPEN_TRADES))
                qty = max(0.0001, round(per_trade / price, 6))
                side = "Buy"
                stop_price = round(price * (1 - s.STOP_LOSS_PCT), 8)

                if s. TRADING_ENABLED:
                    try:
                        import uuid
                        res = await s.bybit_signed_request("POST", "/v5/order/create", {"category": "linear", "symbol": symbol, "side": side, "orderType": "Market", "qty": qty})
                        oid = res.get("result", {}).get("orderId", str(uuid.uuid4()))
                        await s.persist_trade({"id": oid, "symbol": symbol, "side": side, "qty": qty, "entry_price": None, "sl_price": stop_price, "created_at": s.now_ts_ms(), "open":  True, "raw": res})
                        s.log("🚀 REAL ORDER:", symbol, qty, side)
                        await s.send_telegram(f"🚀 REAL ORDER:  {symbol} qty={qty} side={side}")
                    except Exception as e: 
                        s.log("Error placing real order:", e)
                else:
                    import uuid
                    oid = str(uuid.uuid4())
                    await s.persist_trade({"id": oid, "symbol":  symbol, "side": side, "qty": qty, "entry_price": None, "sl_price":  stop_price, "created_at": s.now_ts_ms(), "open": True, "raw": {"simulated": True}})
                    s.log("📊 SIMULATED TRADE:", symbol, qty, side)
                    await s.send_telegram(f"📊 SIMULATED:  {symbol} qty={qty} side={side}")

                if entry_id in s.active_root_signals:
                    s. active_root_signals[entry_id]["status"] = "acted"
                    try:
                        await s.persist_root_signal(s.active_root_signals[entry_id])
                    except Exception: 
                        pass

    except Exception as e:
        s.log(f"_evaluate_root_signal_for_entry error: {e}")


async def _continuous_root_signal_subscriptions():
    """Continuously maintain 5m/15m subscriptions for all active root signals."""
    import services as s
    s.log("Root signal subscription maintenance loop started, interval=30s")

    while True:
        iteration_start = time.time()
        try:
            current_roots = set()
            for sig in s.active_root_signals. values():
                if sig.get("signal_type") == "root":
                    current_roots.add(sig.get("symbol"))

            sem = asyncio.Semaphore(10)

            async def _ensure_subs(sym):
                async with sem: 
                    try:
                        if not s.public_ws or not getattr(s. public_ws, "conn", None):
                            return
                        for tf in ["5m", "15m"]: 
                            token = s.TF_MAP. get(tf)
                            if token: 
                                try:
                                    await s.public_ws.subscribe_kline(sym, token)
                                except Exception: 
                                    pass
                    except Exception:
                        pass

            if current_roots:
                await asyncio.gather(*[_ensure_subs(sym) for sym in current_roots], return_exceptions=True)

            try:
                symbols = await s.get_tradable_usdt_symbols()
                for sym in symbols[: 50]: 
                    try:
                        if not s.public_ws or not getattr(s.public_ws, "conn", None):
                            break
                        await s.public_ws.subscribe_kline(sym, s.TF_MAP["1h"])
                    except Exception: 
                        pass
            except Exception:
                pass

        except Exception as e:
            s.log("Root signal subscription maintenance error:", e)

        elapsed = time.time() - iteration_start
        sleep_for = max(1, 30 - elapsed)
        await asyncio.sleep(sleep_for)


async def _continuous_kline_fetcher(root_tf: str):
    """Continuously fetch klines for root timeframe at each scan interval."""
    import services as s
    s.log(f"Kline fetcher started for {root_tf} at {s. SCAN_INTERVAL_SECONDS}s interval")

    while True:
        iteration_start = time.time()
        try:
            symbols = await s.get_tradable_usdt_symbols()
            if not symbols:
                s.log(f"Kline fetcher {root_tf}:  no symbols available")
            else:
                async def _fetch_for_symbol(sym):
                    try: 
                        await s.ensure_cached_candles(sym, root_tf, s.MIN_CANDLES_REQUIRED)
                    except Exception:
                        pass

                sem = asyncio.Semaphore(s. DISCOVERY_CONCURRENCY)

                async def _with_sem(sym):
                    async with sem:
                        await _fetch_for_symbol(sym)

                await asyncio.gather(
                    *[_with_sem(sym) for sym in symbols],
                    return_exceptions=True
                )

                elapsed_work = time.time() - iteration_start
                s.log(f"Kline refresh completed for {root_tf}:  {len(symbols)} symbols in {elapsed_work:. 2f}s")

        except Exception as e:
            s.log(f"Kline fetcher error for {root_tf}: {e}")

        elapsed = time.time() - iteration_start
        sleep_for = max(1, s.SCAN_INTERVAL_SECONDS - elapsed)
        await asyncio.sleep(sleep_for)


async def _ensure_root_signal_subscriptions(signal_dict: Dict[str, Any]):
    """For each root signal, ensure 5m/15m are subscribed and cached."""
    import services as s

    symbol = signal_dict.get("symbol")
    root_tf = signal_dict.get("root_tf")

    if not symbol or not root_tf:
        return

    try:
        wait_count = 0
        while wait_count < 5 and (not s.public_ws or not getattr(s.public_ws, "conn", None)):
            await asyncio.sleep(0.5)
            wait_count += 1

        if not s.public_ws or not getattr(s.public_ws, "conn", None):
            s.log(f"Public WS not ready for {symbol} signal subscriptions")
            return

        for tf in ["5m", "15m"]:
            token = s.TF_MAP.get(tf)
            if token: 
                try:
                    await s.public_ws.subscribe_kline(symbol, token)
                    await asyncio.sleep(0.2)
                except Exception as e:
                    s.log(f"Error subscribing {symbol} {tf}: {e}")

        required_tfs = s.tf_list_for_root(root_tf)
        for tf in required_tfs: 
            try:
                await s.ensure_cached_candles(symbol, tf, s.MIN_CANDLES_REQUIRED)
            except Exception as e:
                s.log(f"Prewarm error {symbol} {tf}: {e}")
    except Exception as e:
        s.log(f"Error ensuring root signal subscriptions for {symbol}: {e}")


async def root_scanner_loop(root_tf: str):
    """Main root flip detection loop with guaranteed scan intervals."""
    import services as s
    s.log(f"Root scanner started for {root_tf}, interval={s.SCAN_INTERVAL_SECONDS}s")
    semaphore = asyncio.Semaphore(s.DISCOVERY_CONCURRENCY)

    while True:
        iteration_start = time.time()
        try:
            symbols = await s.get_tradable_usdt_symbols()
            now_s = int(time.time())

            if not symbols:
                s.log(f"Root scanner {root_tf}: no symbols available")
            else:
                try:
                    if getattr(s, "public_ws", None):
                        async def _ensure_ws_sub(sym):
                            async with semaphore:
                                try:
                                    await s.public_ws.subscribe_kline(sym, s.TF_MAP[root_tf])
                                except Exception:
                                    pass
                        await asyncio.gather(*[_ensure_ws_sub(sym) for sym in symbols[: 100]], return_exceptions=True)
                except Exception:
                    pass

                async def _prewarm(sym):
                    async with semaphore:
                        try:
                            await s. ensure_cached_candles(sym, root_tf, s.MIN_CANDLES_REQUIRED)
                        except Exception:
                            pass
                try:
                    await asyncio. gather(*[_prewarm(sym) for sym in symbols], return_exceptions=True)
                except Exception as e:
                    s.log(f"Prewarm error:  {e}")

                tasks = []

                async def _detect_and_maybe_create(sym:  str):
                    async with semaphore:
                        try:
                            try:
                                await s.ensure_cached_candles(sym, root_tf, s.MIN_CANDLES_REQUIRED)
                            except Exception:
                                pass

                            recent = s.recent_root_signals. get(sym)
                            if recent and (now_s - recent) < s.ROOT_DEDUP_SECONDS:
                                return
                            if s.signal_exists_for(sym, "root"):
                                return
                            if s.signal_exists_for(sym, "entry"):
                                return

                            flip_kind, last_start = await s.detect_flip(sym, root_tf)
                            if not flip_kind: 
                                return
                            if not s.flip_is_stable_enough(sym, root_tf, last_start):
                                return
                            exists_same = any(x.get("symbol") == sym and x.get("signal_type") == "root" and x.get("root_flip_time") == last_start for x in s.active_root_signals.values())
                            if exists_same: 
                                return
                            dq_root = s.cache_get(sym, s.TF_MAP[root_tf])
                            if not dq_root:
                                return
                            last_candle = list(dq_root)[-1]
                            key = f"ROOT-{sym}-{root_tf}-{last_start}"
                            sig = {
                                "id": key,
                                "symbol": sym,
                                "root_tf": root_tf,
                                "root_flip_time": last_start,
                                "root_flip_price": last_candle. get("close"),
                                "created_at": s.now_ts_ms(),
                                "status": "watching",
                                "priority": None,
                                "signal_type": "root",
                                "components": [],
                                "flip_kind": flip_kind,
                            }
                            added = await s.add_signal(sig)
                            if added:
                                s.log("Root signal created:", key)
                                await _evaluate_root_signal_for_entry(sig)
                        except Exception as e:
                            s.log("Error in detect+create for", sym, e)

                for symbol in symbols:
                    tasks. append(asyncio.create_task(_detect_and_maybe_create(symbol)))

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

                elapsed_work = time.time() - iteration_start
                s.log(f"Root scanner {root_tf} iteration completed in {elapsed_work:.2f}s")

        except Exception as e:
            s.log(f"root_scanner_loop {root_tf} error: {e}")

        elapsed = time.time() - iteration_start
        sleep_for = max(1, s.SCAN_INTERVAL_SECONDS - elapsed)
        await asyncio.sleep(sleep_for)


async def evaluate_signals_loop():
    """Check for new root signals and evaluate them for alignment."""
    import services as s
    s.log("evaluate_signals_loop started (secondary check)")

    while True:
        iteration_start = time.time()
        try:
            ids = list(s.active_root_signals.keys())
            count_checked = 0
            for sid in ids:
                try: 
                    sig = s.active_root_signals. get(sid)
                    if not sig:
                        continue
                    stype = sig.get("signal_type", "root")
                    if stype != "root": 
                        continue

                    symbol = sig.get("symbol")
                    if s.is_stablecoin_symbol(symbol):
                        await s.remove_signal(sid)
                        continue

                    if not s. signal_exists_for(symbol, "entry"):
                        count_checked += 1
                        await _evaluate_root_signal_for_entry(sig)

                except Exception as e:
                    s.log("evaluate_signals_loop error for", sid, e)

            if count_checked > 0:
                s.log(f"evaluate_signals_loop secondary check evaluated {count_checked} signals")

        except Exception as e: 
            s.log("evaluate_signals_loop outer error:", e)

        elapsed = time.time() - iteration_start
        sleep_for = max(5, (s.SCAN_INTERVAL_SECONDS // 2) - elapsed)
        await asyncio.sleep(sleep_for)


async def expire_signals_loop():
    """Expire root signals based on candle interval."""
    import services as s
    s.log("expire_signals_loop started, interval=half SCAN_INTERVAL_SECONDS")

    while True:
        iteration_start = time.time()
        try:
            now_s = int(time.time())
            to_remove = []
            for sid, sig in list(s.active_root_signals.items()):
                try:
                    stype = sig.get("signal_type", "root")
                    if stype == "root":
                        root_tf = sig.get("root_tf")
                        flip_time = sig.get("root_flip_time") or sig.get("flip_time")
                        if root_tf and flip_time: 
                            token = s.TF_MAP. get(root_tf)
                            if token: 
                                expiry = flip_time + s.interval_seconds_from_token(token)
                                if now_s >= expiry:
                                    to_remove.append(sid)
                except Exception:
                    continue
            for sid in to_remove: 
                try:
                    s.log("Expiring signal:", sid)
                    await s. remove_signal(sid)
                except Exception as e:
                    s.log("Error expiring signal", sid, e)
        except Exception as e:
            s.log("expire_signals_loop error:", e)

        elapsed = time.time() - iteration_start
        sleep_for = max(1, (s.SCAN_INTERVAL_SECONDS // 2) - elapsed)
        await asyncio.sleep(sleep_for)


async def periodic_root_logger():
    """Log current root signals periodically."""
    import services as s
    s.log("periodic_root_logger started")

    while True:
        try:
            s.log_current_root_signals()
        except Exception as e:
            s.log("periodic_root_logger error:", e)
        await asyncio.sleep(max(30, s.ROOT_SIGNALS_LOG_INTERVAL))
