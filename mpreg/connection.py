import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import websockets.client
import websockets.server
from loguru import logger

if TYPE_CHECKING:
    from .server import Cluster


@dataclass(eq=True)
class Connection:
    """Encapsulates a websocket connection to a remote peer."""

    url: str
    max_retries: int = field(default=5, repr=False)
    base_delay: float = field(default=1.0, repr=False)
    websocket: (
        websockets.client.WebSocketClientProtocol
        | websockets.server.WebSocketServerProtocol
        | None
    ) = field(default=None, init=False)
    _receive_queue: asyncio.Queue[bytes] = field(
        default_factory=asyncio.Queue, init=False
    )
    _listener_task: asyncio.Task[None] | None = field(default=None, init=False)
    cluster: "Cluster | None" = field(default=None, init=False)

    async def connect(self) -> None:
        """Establishes a websocket connection to the peer with exponential backoff.

        This method attempts to connect to the peer. If the connection fails,
        it retries with an exponentially increasing delay up to `max_retries`.
        """
        if self.websocket and not self.websocket.closed:
            logger.info("[{}] Connection already open.", self.url)
            return

        for attempt in range(self.max_retries + 1):
            logger.info(
                "[{}] Attempting to connect (attempt {}/{})",
                self.url,
                attempt + 1,
                self.max_retries + 1,
            )
            try:
                self.websocket = await websockets.connect(
                    self.url, user_agent_header=None
                )
                logger.info("[{}] Connected.", self.url)
                self._listener_task = asyncio.create_task(self._listen_for_messages())
                return
            except Exception as e:
                logger.error("[{}] Failed to connect: {}.", self.url, e)
                self.websocket = None
                if attempt < self.max_retries:
                    delay = self.base_delay * (2**attempt)
                    logger.info("[{}] Retrying in {:.2f} seconds...", self.url, delay)
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        "[{}] Max reconnection attempts reached. Giving up.", self.url
                    )
                    raise ConnectionError(
                        f"Failed to connect to {self.url} after {self.max_retries + 1} attempts."
                    )

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
        message = await self._receive_queue.get()
        if isinstance(message, bytes):
            return message
        elif isinstance(message, str):
            return message.encode("utf-8")
        else:
            raise TypeError(f"Unexpected message type: {type(message)}")

    async def _listen_for_messages(self) -> None:
        """Listens for incoming messages and puts them into the receive queue."""
        if not self.websocket:
            return
        try:
            async for message in self.websocket:
                message_bytes = None
                if isinstance(message, bytes):
                    message_bytes = message
                elif isinstance(message, str):
                    message_bytes = message.encode("utf-8")
                else:
                    logger.warning(f"Received unexpected message type: {type(message)}")
                    continue

                # If we have a cluster reference, check for internal-answer messages
                if self.cluster and message_bytes:
                    try:
                        parsed_msg = self.cluster.serializer.deserialize(message_bytes)
                        if parsed_msg.get("role") == "internal-answer":
                            logger.info(
                                "Processing internal-answer in client connection"
                            )
                            # Import here to avoid circular import
                            from .model import RPCInternalAnswer

                            internal_answer = RPCInternalAnswer.model_validate(
                                parsed_msg
                            )
                            # Store answer and notify waiting process
                            self.cluster.answer[internal_answer.u] = (
                                internal_answer.answer
                            )
                            if internal_answer.u in self.cluster.waitingFor:
                                self.cluster.waitingFor[internal_answer.u].set()
                                logger.info(
                                    "Notified waiting process for u={}",
                                    internal_answer.u,
                                )
                            continue
                    except Exception as e:
                        logger.debug(
                            "Failed to parse message as internal-answer: {}", e
                        )
                        # Fall through to normal queue processing

                await self._receive_queue.put(message_bytes)
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

    def __hash__(self) -> int:
        """Hash based on URL for set storage."""
        return hash(self.url)
