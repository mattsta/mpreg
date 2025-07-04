import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import websockets.client
from loguru import logger


@dataclass
class Connection:
    """Encapsulates a websocket connection to a remote peer."""

    url: str
    websocket: Optional[websockets.client.WebSocketClientProtocol] = field(default=None, init=False)
    _receive_queue: asyncio.Queue = field(default_factory=asyncio.Queue, init=False)
    _listener_task: Optional[asyncio.Task] = field(default=None, init=False)

    async def connect(self) -> None:
        """Establishes a websocket connection to the peer."""
        if self.websocket and not self.websocket.closed:
            logger.info("[{}] Connection already open.", self.url)
            return

        logger.info("[{}] Attempting to connect...", self.url)
        try:
            self.websocket = await websockets.connect(self.url, user_agent_header=None)
            logger.info("[{}] Connected.", self.url)
            self._listener_task = asyncio.create_task(self._listen_for_messages())
        except Exception as e:
            logger.error("[{}] Failed to connect: {}", self.url, e)
            self.websocket = None
            raise

    async def disconnect(self) -> None:
        """Closes the websocket connection."""
        if self.websocket and not self.websocket.closed:
            logger.info("[{}] Disconnecting...", self.url)
            await self.websocket.close()
            self.websocket = None
            if self._listener_task:
                self._listener_task.cancel()
                await self._listener_task
            logger.info("[{}] Disconnected.", self.url)

    async def send(self, message: bytes) -> None:
        """Sends a message over the websocket connection."""
        if not self.websocket or self.websocket.closed:
            raise ConnectionError(f"Connection to {self.url} is not open.")
        try:
            await self.websocket.send(message)
        except Exception as e:
            logger.error("[{}] Failed to send message: {}", self.url, e)
            raise

    async def receive(self) -> bytes:
        """Receives a message from the websocket connection."""
        return await self._receive_queue.get()

    async def _listen_for_messages(self) -> None:
        """Listens for incoming messages and puts them into the receive queue."""
        if not self.websocket:
            return
        try:
            async for message in self.websocket:
                await self._receive_queue.put(message)
        except websockets.ConnectionClosedOK:
            logger.info("[{}] Connection closed gracefully.", self.url)
        except websockets.ConnectionClosedError as e:
            logger.error("[{}] Connection closed with error: {}", self.url, e)
        except asyncio.CancelledError:
            logger.info("[{}] Listener task cancelled.", self.url)
        except Exception as e:
            logger.error("[{}] Error in listener: {}", self.url, e)
        finally:
            await self.disconnect()

    @property
    def is_connected(self) -> bool:
        """Checks if the websocket connection is currently open."""
        return self.websocket is not None and not self.websocket.closed
