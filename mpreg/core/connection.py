from __future__ import annotations

import asyncio
import os
import resource
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from loguru import logger

from mpreg.datastructures.type_aliases import TenantId

from .transport.factory import TransportFactory
from .transport.interfaces import (
    TransportConfig,
    TransportConnectionError,
    TransportError,
    TransportInterface,
)

if TYPE_CHECKING:
    from ..server import Cluster


_DIAG_TRUE_VALUES = frozenset({"1", "true", "yes", "on", "enabled", "debug"})
PEER_DIAL_DIAG_ENABLED = (
    os.environ.get("MPREG_DEBUG_PEER_DIAL", "").strip().lower() in _DIAG_TRUE_VALUES
)


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
    tenant_id: TenantId | None = field(default=None, repr=False, compare=False)
    _transport: TransportInterface | None = field(default=None, init=False)
    cluster: Cluster | None = field(default=None, init=False)

    @classmethod
    def from_transport(
        cls, transport: TransportInterface, *, url: str | None = None
    ) -> Connection:
        connection = cls(url=url or transport.url)
        connection._transport = transport
        return connection

    def _base_connect_timeout(self) -> float:
        """Resolve the baseline timeout used for the first dial attempt."""
        effective_timeout = self.connect_timeout
        if effective_timeout <= 0:
            effective_timeout = self.open_timeout
        if effective_timeout <= 0:
            effective_timeout = 1.0
        return effective_timeout

    def _connect_timeout_for_attempt(self, attempt: int) -> float:
        """Increase timeout budget across retries to absorb transient load."""
        base_timeout = self._base_connect_timeout()
        if self.max_retries <= 0 or attempt <= 0:
            return base_timeout
        retry_ratio = min(
            max(float(attempt) / float(self.max_retries), 0.0),
            1.0,
        )
        multiplier = 1.0 + (retry_ratio * 2.0)
        return min(base_timeout * 4.0, base_timeout * multiplier)

    def _build_transport_config(
        self, *, connect_timeout_override: float | None = None
    ) -> TransportConfig:
        if self.transport_config:
            return self.transport_config
        # Respect explicit connection timeout. `open_timeout` remains as legacy
        # fallback for callers that only set that field.
        effective_timeout = (
            connect_timeout_override
            if connect_timeout_override is not None
            else self._base_connect_timeout()
        )
        return TransportConfig(
            connect_timeout=effective_timeout,
            protocol_options={"max_message_size": self.max_size},
        )

    async def connect(self) -> None:
        """Establish a transport connection to the peer with exponential backoff."""
        if self._transport and self._transport.connected:
            logger.debug("[{}] Connection already open.", self.url)
            return
        diagnostics_enabled = PEER_DIAL_DIAG_ENABLED
        if diagnostics_enabled:
            logger.error(
                "[DIAG_CONN] connect_start url={} retries={} base_delay={:.3f}s "
                "connect_timeout={:.3f}s open_timeout={:.3f}s",
                self.url,
                self.max_retries,
                self.base_delay,
                self.connect_timeout,
                self.open_timeout,
            )

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
            attempt_started_at = time.monotonic()
            try:
                attempt_timeout = self._connect_timeout_for_attempt(attempt)
                config = self._build_transport_config(
                    connect_timeout_override=attempt_timeout
                )
                transport = TransportFactory.create(self.url, config)
                await transport.connect()
                self._transport = transport
                logger.debug("[{}] Connected.", self.url)
                if diagnostics_enabled:
                    elapsed = time.monotonic() - attempt_started_at
                    logger.error(
                        "[DIAG_CONN] connect_success url={} attempt={}/{} timeout={:.3f}s elapsed={:.3f}s",
                        self.url,
                        attempt + 1,
                        self.max_retries + 1,
                        attempt_timeout,
                        elapsed,
                    )
                return
            except asyncio.CancelledError:
                self._transport = None
                raise
            except (TransportError, Exception) as exc:
                logger.debug(
                    "[{}] Connection attempt failed (attempt {}/{}): {}",
                    self.url,
                    attempt + 1,
                    self.max_retries + 1,
                    exc,
                )
                if diagnostics_enabled:
                    elapsed = time.monotonic() - attempt_started_at
                    logger.error(
                        "[DIAG_CONN] connect_failure url={} attempt={}/{} timeout={:.3f}s elapsed={:.3f}s error={}",
                        self.url,
                        attempt + 1,
                        self.max_retries + 1,
                        attempt_timeout,
                        elapsed,
                        exc,
                    )
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
                    logger.debug(
                        "[{}] Max reconnection attempts reached. Giving up.",
                        self.url,
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
            except TimeoutError, asyncio.CancelledError:
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
