import asyncio
import resource
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import websockets.client
import websockets.server
from loguru import logger

if TYPE_CHECKING:
    from ..server import Cluster


@dataclass(eq=True, slots=True)
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
    _receive_queue: asyncio.Queue[bytes | None] = field(
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
            logger.debug("[{}] Connection already open.", self.url)
            return

        # Check if we're approaching file descriptor limits
        try:
            soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
            # Get approximate current usage (not exact, but good enough for warning)
            import psutil

            current_process = psutil.Process()
            current_fds = current_process.num_fds()

            if current_fds > soft_limit * 0.8:  # 80% of limit
                logger.warning(
                    "[{}] Approaching file descriptor limit: {}/{} used",
                    self.url,
                    current_fds,
                    soft_limit,
                )
                # Add a small delay to allow other connections to complete/cleanup
                await asyncio.sleep(0.5)
        except Exception:
            # Resource checking failed, but continue with connection
            pass

        for attempt in range(self.max_retries + 1):
            logger.info(
                "[{}] Attempting to connect (attempt {}/{})",
                self.url,
                attempt + 1,
                self.max_retries + 1,
            )
            try:
                # Add proper timeouts and limits for large cluster scalability
                self.websocket = await asyncio.wait_for(
                    websockets.connect(
                        self.url,
                        user_agent_header=None,
                        open_timeout=5.0,  # 5 second connection timeout
                        close_timeout=2.0,  # 2 second close timeout
                        max_size=2**20,  # 1MB max message size
                        max_queue=32,  # Limit message queue
                    ),
                    timeout=8.0,  # Overall timeout including retries
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
        """Closes the websocket connection with proper resource cleanup."""
        # Cancel listener task first to stop message processing
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await asyncio.wait_for(self._listener_task, timeout=1.0)
            except (TimeoutError, asyncio.CancelledError):
                pass  # Task cancellation/timeout is expected during cleanup
            finally:
                self._listener_task = None

        # Close websocket connection with timeout
        if self.websocket and not self.websocket.closed:
            logger.debug("[{}] Disconnecting...", self.url)
            try:
                await asyncio.wait_for(self.websocket.close(), timeout=2.0)
            except TimeoutError:
                logger.warning("[{}] Websocket close timed out", self.url)
            except Exception as e:
                logger.debug("[{}] Error during disconnect: {}", self.url, e)
            finally:
                self.websocket = None

        # Clear receive queue to free memory
        try:
            while not self._receive_queue.empty():
                try:
                    self._receive_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

        except Exception as e:
            logger.debug("[{}] Error clearing receive queue: {}", self.url, e)

        # Signal any pending receive() calls that the connection is closed
        try:
            self._receive_queue.put_nowait(None)
        except asyncio.QueueFull:
            pass  # Queue is full, but that's okay since we're closing

        logger.debug("[{}] Disconnected and cleaned up", self.url)

    async def send(self, message: bytes) -> None:
        """Sends a message over the websocket connection."""
        if not self.websocket or self.websocket.closed:
            raise ConnectionError(f"Connection to {self.url} is not open.")
        try:
            await self.websocket.send(message)
        except Exception as e:
            logger.error("[{}] Failed to send message: {}", self.url, e)
            raise

    async def receive(self) -> bytes | None:
        """Receives a message from the websocket connection."""
        try:
            message = await self._receive_queue.get()
            if message is None:
                # Sentinel value indicating connection is closed
                return None
            elif isinstance(message, bytes):
                return message
            elif isinstance(message, str):
                return message.encode("utf-8")
            else:
                raise TypeError(f"Unexpected message type: {type(message)}")
        except asyncio.CancelledError:
            # Clean cancellation during shutdown
            return None

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
