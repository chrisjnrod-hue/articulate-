# Minimal PublicWebsocketManager implementation
# Put this file alongside main.py (repo root) and ensure `websockets` is in requirements.txt
# This is a lightweight, resilient reader that opens a connection, spawns a reader task,
# and provides connect_and_detect()/send()/close() methods used by main.py startup.
#
# NOTE: This is a simple starter implementation for Bybit public streams. For production:
# - Add JSON message parsing for Bybit's subscription format
# - Add ping/pong, heartbeats, backoff/reconnect policies, and error handling improvements
# - Consider integrating with main.persist_raw_ws or other stateful handlers (avoid circular imports)

import asyncio
import traceback
from typing import Optional, Callable
import websockets

class PublicWebsocketManager:
    """
    Minimal WebSocket manager used by main.py at startup.
    - Connects to the given URL.
    - Starts a background reader task that logs or forwards messages.
    - Exposes `conn` on success and `connect_and_detect` async method that returns True on success.
    """

    def __init__(self, url: str, on_message: Optional[Callable[[str], None]] = None):
        self.url = url
        self.conn: Optional[websockets.WebSocketClientProtocol] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._running = False
        # Optional callback to forward messages into your app (should be async or schedule work)
        self.on_message = on_message

    async def connect_and_detect(self, timeout: float = 6.0) -> bool:
        """
        Attempt to open a websocket connection and start the reader.
        Returns True if connection established, False otherwise.
        """
        try:
            # Wait for connect with a timeout to avoid blocking startup forever
            self.conn = await asyncio.wait_for(websockets.connect(self.url), timeout=timeout)
            self._running = True
            # Start background reader
            self._reader_task = asyncio.create_task(self._reader())
            print("PublicWebsocketManager: connected to", self.url)
            return True
        except Exception as e:
            print("PublicWebsocketManager: connect failed:", e)
            traceback.print_exc()
            self.conn = None
            return False

    async def _reader(self):
        """
        Continuously read messages and call on_message (if provided) or print.
        This will exit if the connection is closed or an exception occurs.
        """
        try:
            async for msg in self.conn:
                try:
                    if self.on_message:
                        # If callback is async, schedule as a task to avoid blocking the reader
                        if asyncio.iscoroutinefunction(self.on_message):
                            asyncio.create_task(self.on_message(msg))
                        else:
                            # run sync handler in executor to avoid blocking
                            loop = asyncio.get_running_loop()
                            loop.run_in_executor(None, self.on_message, msg)
                    else:
                        # Default behavior: print a short excerpt to logs
                        print("PublicWebsocketManager message:", (msg[:400] + "...") if isinstance(msg, str) and len(msg) > 400 else msg)
                except Exception as ex:
                    print("PublicWebsocketManager: error handling message:", ex)
                    traceback.print_exc()
        except Exception as e:
            print("PublicWebsocketManager: reader error:", e)
            traceback.print_exc()
        finally:
            self._running = False
            try:
                if self.conn:
                    await self.conn.close()
            except Exception:
                pass
            print("PublicWebsocketManager: reader exiting / connection closed")

    async def send(self, message: str):
        """
        Send a text message over the websocket connection.
        """
        if not self.conn:
            raise RuntimeError("PublicWebsocketManager: not connected")
        await self.conn.send(message)

    async def close(self):
        """
        Close the connection and cancel reader task.
        """
        self._running = False
        try:
            if self.conn:
                await self.conn.close()
        except Exception:
            pass
        if self._reader_task:
            try:
                self._reader_task.cancel()
                await self._reader_task
            except Exception:
                pass
        print("PublicWebsocketManager: closed")
