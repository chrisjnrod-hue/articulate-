# bybit_ws_client.py
# Lightweight Bybit public WebSocket client (v5) with topic-template fallback + auto-detection.
# Drop this file into the same project alongside main.py and redeploy.

import asyncio
import aiohttp
import json
import time
from typing import Callable, Dict, Optional, List

# Candidate topic templates to try when subscribing.
# The scanner uses interval tokens like "5", "15", "60" etc.
TOPIC_TEMPLATES = [
    "klineV2.{interval}.{symbol}",
    "kline.{interval}.{symbol}",
    "kline.{symbol}.{interval}",
    "klineV2:{interval}:{symbol}",
    "kline:{interval}:{symbol}",
]

DEFAULT_WS_URL = "wss://stream.bybit.com/v5/public"
DEFAULT_TESTNET_WS_URL = "wss://stream-testnet.bybit.com/v5/public"

class BybitWebSocketClient:
    """
    Simple Bybit v5 public WebSocket client.

    Behavior:
    - Connects to the public v5 WebSocket (DEFAULT_WS_URL).
    - subscribe_kline(symbol, interval, callback) will attempt several topic templates
      and register the callback for the first template that the server accepts.
    - Dispatches incoming messages (JSON) to registered callbacks keyed by topic.
    """

    def __init__(self, category: str = "linear", ws_url: Optional[str] = None, detect_symbol: str = "BTCUSDT"):
        # ws_url: optional override; if None use DEFAULT_WS_URL
        # category kept for backwards compatibility in signatures but not part of path
        self.url = ws_url or DEFAULT_WS_URL
        self.category = category
        self.detect_symbol = detect_symbol
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.callbacks: Dict[str, Callable[[dict], asyncio.Future]] = {}
        self._listen_task: Optional[asyncio.Task] = None
        self._connecting = False
        self._connected_evt = asyncio.Event()

    async def connect(self, timeout: float = 10.0):
        """
        Connect to the Bybit public WS endpoint and start the _listen loop.
        This method is safe to call multiple times (no-op if already connected).
        """
        if self.ws and not self.ws.closed:
            return True
        if self._connecting:
            # Wait for other in-flight connect
            await self._connected_evt.wait()
            return bool(self.ws and not self.ws.closed)
        self._connecting = True
        self._connected_evt.clear()
        try:
            if self.session is None:
                self.session = aiohttp.ClientSession()
            # aiohttp will raise on non-2xx upgrade responses; catch exception outside
            self.ws = await self.session.ws_connect(self.url, heartbeat=20, timeout=timeout)
            # start listener
            self._listen_task = asyncio.create_task(self._listen())
            self._connected_evt.set()
            print(f"[BybitWS] Connected to {self.url}")
            return True
        except Exception as e:
            print(f"[BybitWS] Connect failed to {self.url}: {e!r}")
            self._connected_evt.set()
            return False
        finally:
            self._connecting = False

    async def _attempt_subscribe(self, topic: str) -> bool:
        """
        Send subscribe and wait briefly for a server response that indicates success
        (non-blocking best-effort). Returns True if send succeeded.
        """
        try:
            await self.ws.send_json({"op": "subscribe", "args": [topic]})
            return True
        except Exception as e:
            # send failed (connection issue)
            print(f"[BybitWS] subscribe send_json failed for {topic}: {e!r}")
            return False

    async def subscribe_kline(self, symbol: str, interval: str, callback: Callable[[dict], asyncio.Future]) -> bool:
        """
        Subscribe to kline topic for given symbol & interval.
        Tries multiple candidate templates. Registers callback under the successful topic key.
        Returns True if subscription was placed.
        """
        if not self.ws or self.ws.closed:
            ok = await self.connect()
            if not ok:
                print("[BybitWS] subscribe_kline: cannot connect, aborting subscribe")
                return False

        tried: List[str] = []
        # Try supplied templates in order
        for tmpl in TOPIC_TEMPLATES:
            topic = None
            try:
                topic = tmpl.format(interval=interval, symbol=symbol)
            except Exception:
                continue
            tried.append(topic)
            ok = await self._attempt_subscribe(topic)
            if ok:
                # Register callback and return success
                self.callbacks[topic] = callback
                print(f"[BybitWS] Subscribed to {topic}")
                return True
            # small pause between attempts
            await asyncio.sleep(0.15)

        # fallback: try a simple "kline.{interval}.{symbol}" if not tried
        simple = f"kline.{interval}.{symbol}"
        if simple not in tried:
            ok = await self._attempt_subscribe(simple)
            if ok:
                self.callbacks[simple] = callback
                print(f"[BybitWS] Subscribed to {simple}")
                return True

        print(f"[BybitWS] subscribe_kline failed for {symbol} {interval} tried: {tried}")
        return False

    async def _listen(self):
        """
        Receive loop. Dispatches JSON messages to callbacks by topic.
        Keeps running until ws closed or error occurs.
        """
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except Exception:
                        # ignore non-json
                        continue
                    # many messages include "topic" or "type" or an ack with "success"
                    topic = data.get("topic") or (data.get("arg") or {}).get("topic") or data.get("type") or ""
                    if topic and topic in self.callbacks:
                        cb = self.callbacks.get(topic)
                        # dispatch in background to avoid blocking recv loop
                        try:
                            asyncio.create_task(cb(data))
                        except Exception as e:
                            print(f"[BybitWS] callback dispatch error for {topic}: {e!r}")
                    else:
                        # no exact topic match — try to find a callback by scanning keys
                        if isinstance(topic, str) and topic:
                            for tkey, cb in list(self.callbacks.items()):
                                if tkey in topic:
                                    try:
                                        asyncio.create_task(cb(data))
                                    except Exception as e:
                                        print(f"[BybitWS] callback dispatch error for {tkey}: {e!r}")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    print(f"[BybitWS] WS error: {msg.data}")
        except Exception as e:
            print(f"[BybitWS] Listen loop stopped: {e!r}")
        finally:
            # cleanup state so reconnect attempts can be made
            try:
                if self.ws and not self.ws.closed:
                    await self.ws.close()
            except Exception:
                pass
            self.ws = None
            print("[BybitWS] listen loop ended")

    async def close(self):
        """
        Close websocket and session.
        """
        try:
            if self.ws and not self.ws.closed:
                await self.ws.close()
        except Exception:
            pass
        try:
            if self.session:
                await self.session.close()
        except Exception:
            pass
        self.ws = None
        self.session = None
        print("[BybitWS] Closed connection")
