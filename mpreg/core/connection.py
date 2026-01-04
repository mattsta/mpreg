from __future__ import annotations

import asyncio
import resource
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from loguru import logger

from .transport.factory import TransportFactory
from .transport.interfaces import (
    TransportConfig,
    TransportConnectionError,
    TransportError,
    TransportInterface,
)

if TYPE_CHECKING:
    from ..server import Cluster


@dataclass(eq=True, slots=True)
class Connection:
    """Encapsulate a transport-backed connection to a remote peer."""

    url: str
    max_retries: int = field(default=5, repr=False)
    base_delay: float = field(default=1.0, repr=False)
    open_timeout: float = field(default=5.0, repr=False)
    connect_timeout: float = field(default=8.0, repr=False)
    close_timeout: float = field(default=2.0, repr=False)
    max_size: int = field(default=2**20, repr=False)
    max_queue: int = field(default=32, repr=False)
    transport_config: TransportConfig | None = field(default=None, repr=False)
    _transport: TransportInterface | None = field(default=None, init=False)
    cluster: Cluster | None = field(default=None, init=False)

    @classmethod
    def from_transport(
        cls, transport: TransportInterface, *, url: str | None = None
    ) -> Connection:
        connection = cls(url=url or transport.url)
        connection._transport = transport
        return connection

    def _build_transport_config(self) -> TransportConfig:
        if self.transport_config:
            return self.transport_config
        effective_timeout = min(self.connect_timeout, self.open_timeout)
        return TransportConfig(
            connect_timeout=effective_timeout,
            protocol_options={"max_message_size": self.max_size},
        )

    async def connect(self) -> None:
        """Establish a transport connection to the peer with exponential backoff."""
        if self._transport and self._transport.connected:
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
                await asyncio.sleep(0.5)
        except Exception:
            pass

        for attempt in range(self.max_retries + 1):
            logger.debug(
                "[{}] Attempting to connect (attempt {}/{})",
                self.url,
                attempt + 1,
                self.max_retries + 1,
            )
            try:
                config = self._build_transport_config()
                transport = TransportFactory.create(self.url, config)
                await transport.connect()
                self._transport = transport
                logger.debug("[{}] Connected.", self.url)
                return
            except (TransportError, Exception) as exc:
                logger.error("[{}] Failed to connect: {}", self.url, exc)
                self._transport = None
                if attempt < self.max_retries:
                    delay = self.base_delay * (2**attempt)
                    logger.debug(
                        "[{}] Retrying in {:.2f} seconds...",
                        self.url,
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        "[{}] Max reconnection attempts reached. Giving up.", self.url
                    )
                    raise ConnectionError(
                        f"Failed to connect to {self.url} after {self.max_retries + 1} attempts."
                    )

    async def disconnect(self) -> None:
        """Close the transport connection with cleanup."""
        if self._transport:
            try:
                await asyncio.wait_for(
                    self._transport.disconnect(), timeout=self.close_timeout
                )
            except (TimeoutError, asyncio.CancelledError):
                pass
            except Exception as exc:
                logger.debug("[{}] Error during disconnect: {}", self.url, exc)
            finally:
                self._transport = None

    async def send(self, message: bytes) -> None:
        """Send a message over the transport connection."""
        if not self._transport or not self._transport.connected:
            raise ConnectionError(f"Connection to {self.url} is not open.")
        try:
            await self._transport.send(message)
        except TransportConnectionError as exc:
            logger.debug("[{}] Failed to send message (closed): {}", self.url, exc)
            raise
        except TransportError as exc:
            logger.error("[{}] Failed to send message: {}", self.url, exc)
            raise

    async def receive(self) -> bytes | None:
        """Receive a message from the transport connection."""
        if not self._transport or not self._transport.connected:
            return None
        try:
            return await self._transport.receive()
        except TransportConnectionError:
            return None
        except asyncio.CancelledError:
            return None
        except TransportError as exc:
            logger.error("[{}] Failed to receive message: {}", self.url, exc)
            return None

    @property
    def is_connected(self) -> bool:
        """Check if the transport connection is currently open."""
        return self._transport is not None and self._transport.connected

    def __hash__(self) -> int:
        """Hash based on URL for set storage."""
        return hash(self.url)
