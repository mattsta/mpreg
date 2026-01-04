"""
Core transport layer interfaces and types for MPREG.

This module defines the abstract interfaces that all transport implementations
must follow, enabling pluggable transport protocols while maintaining a
consistent API across the entire MPREG system.
"""

from __future__ import annotations

import ssl
from abc import ABC, abstractmethod
from collections.abc import AsyncIterable
from dataclasses import dataclass, field
from enum import Enum
from types import TracebackType
from typing import Any, Protocol, runtime_checkable
from urllib.parse import ParseResult


@runtime_checkable
class FileReader(Protocol):
    """Protocol for file-like objects with read method."""

    def read(self, size: int = -1) -> bytes: ...
    def seek(self, offset: int, whence: int = 0) -> int: ...


# Type alias for stream data
StreamData = bytes | FileReader | AsyncIterable[bytes]

from .defaults import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_HEARTBEAT_TIMEOUT,
    DEFAULT_MAX_MESSAGE_SIZE,
    DEFAULT_MAX_RETRIES,
    DEFAULT_MAX_STREAM_SIZE,
    DEFAULT_READ_BUFFER_SIZE,
    DEFAULT_READ_TIMEOUT,
    DEFAULT_RETRY_BACKOFF,
    DEFAULT_RETRY_DELAY,
    DEFAULT_WRITE_BUFFER_SIZE,
    DEFAULT_WRITE_TIMEOUT,
)


class TransportProtocol(Enum):
    """Supported transport protocols."""

    WEBSOCKET = "ws"
    WEBSOCKET_SECURE = "wss"
    TCP = "tcp"
    TCP_SECURE = "tcps"


class TransportError(Exception):
    """Base exception for transport-related errors."""

    pass


class TransportConnectionError(TransportError):
    """Raised when transport connection fails."""

    pass


class TransportTimeoutError(TransportError):
    """Raised when transport operation times out."""

    pass


@dataclass(frozen=True, slots=True)
class SecurityConfig:
    """Security configuration for transport connections."""

    # TLS/SSL configuration
    ssl_context: ssl.SSLContext | None = None
    verify_cert: bool = True
    cert_file: str | None = None
    key_file: str | None = None
    ca_file: str | None = None

    # Authentication configuration
    auth_token: str | None = None
    api_key: str | None = None

    def create_ssl_context(self) -> ssl.SSLContext | None:
        """Create SSL context from configuration."""
        if self.ssl_context:
            return self.ssl_context

        if not self.verify_cert and not self.cert_file:
            return None

        context = ssl.create_default_context()

        if not self.verify_cert:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        if self.cert_file and self.key_file:
            context.load_cert_chain(self.cert_file, self.key_file)

        if self.ca_file:
            context.load_verify_locations(self.ca_file)

        return context


@dataclass(slots=True)
class TransportConfig:
    """Configuration for transport connections."""

    # Connection settings
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    read_timeout: float | None = DEFAULT_READ_TIMEOUT
    write_timeout: float | None = DEFAULT_WRITE_TIMEOUT

    # Retry configuration
    max_retries: int = DEFAULT_MAX_RETRIES
    retry_delay: float = DEFAULT_RETRY_DELAY
    retry_backoff: float = DEFAULT_RETRY_BACKOFF

    # Buffer settings
    read_buffer_size: int = DEFAULT_READ_BUFFER_SIZE
    write_buffer_size: int = DEFAULT_WRITE_BUFFER_SIZE

    # Health check settings
    heartbeat_interval: float | None = None
    heartbeat_timeout: float = DEFAULT_HEARTBEAT_TIMEOUT

    # Security configuration
    security: SecurityConfig = field(default_factory=SecurityConfig)

    # Protocol-specific settings
    protocol_options: dict[str, Any] = field(default_factory=dict)

    def get_max_message_size(self) -> int:
        """Get maximum message size for this transport configuration."""
        value = self.protocol_options.get("max_message_size", DEFAULT_MAX_MESSAGE_SIZE)
        return value if isinstance(value, int) else DEFAULT_MAX_MESSAGE_SIZE

    def get_max_stream_size(self) -> int:
        """Get maximum stream size for this transport configuration."""
        value = self.protocol_options.get("max_stream_size", DEFAULT_MAX_STREAM_SIZE)
        return value if isinstance(value, int) else DEFAULT_MAX_STREAM_SIZE


class TransportInterface(ABC):
    """Abstract interface for all transport implementations.

    This interface defines the contract that all transport protocols must
    implement, ensuring consistent behavior across WebSocket, TCP, and
    future transport types.

    Example Usage:
        transport = SomeTransport(url, config)
        await transport.connect()
        try:
            await transport.send(b"Hello")
            response = await transport.receive()
        finally:
            await transport.disconnect()
    """

    def __init__(self, url: str, config: TransportConfig) -> None:
        """Initialize transport with URL and configuration.

        Args:
            url: Transport URL (e.g., "ws://host:port", "tcp://host:port")
            config: Transport configuration options
        """
        self.url = url
        self.config = config
        self._parsed_url: ParseResult | None = None
        self._connected = False

    @property
    def connected(self) -> bool:
        """Check if transport is currently connected."""
        return self._connected

    @property
    def parsed_url(self) -> ParseResult:
        """Get parsed URL components."""
        if not self._parsed_url:
            from urllib.parse import urlparse

            self._parsed_url = urlparse(self.url)
        return self._parsed_url

    @property
    def protocol(self) -> TransportProtocol:
        """Get transport protocol type."""
        scheme = self.parsed_url.scheme.lower()
        try:
            return TransportProtocol(scheme)
        except ValueError:
            raise TransportError(f"Unsupported transport protocol: {scheme}")

    @property
    def is_secure(self) -> bool:
        """Check if transport uses encryption."""
        return self.protocol in (
            TransportProtocol.WEBSOCKET_SECURE,
            TransportProtocol.TCP_SECURE,
        )

    @abstractmethod
    async def connect(self) -> None:
        """Establish transport connection.

        Raises:
            TransportConnectionError: If connection fails
            TransportTimeoutError: If connection times out
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close transport connection gracefully."""
        pass

    @abstractmethod
    async def send(self, data: bytes) -> None:
        """Send data over transport.

        Args:
            data: Raw bytes to send

        Raises:
            TransportConnectionError: If not connected
            TransportTimeoutError: If send times out
            TransportError: If send fails
        """
        pass

    @abstractmethod
    async def receive(self) -> bytes:
        """Receive data from transport.

        Returns:
            Raw bytes received

        Raises:
            TransportConnectionError: If not connected
            TransportTimeoutError: If receive times out
            TransportError: If receive fails
        """
        pass

    @abstractmethod
    async def ping(self) -> float:
        """Send ping and measure round-trip time.

        Returns:
            Round-trip time in seconds

        Raises:
            TransportConnectionError: If not connected
            TransportTimeoutError: If ping times out
        """
        pass

    async def send_stream(
        self, data_stream: StreamData, chunk_size: int = 64 * 1024
    ) -> None:
        """Send large data as a stream of chunks.

        Default implementation falls back to regular send for protocols
        that don't support streaming. Override in protocols that support it.

        Args:
            data_stream: Bytes data, file-like object, or async iterator
            chunk_size: Size of each chunk (default 64KB)

        Raises:
            TransportConnectionError: If not connected
            TransportTimeoutError: If send times out
            TransportError: If send fails
        """
        # Default implementation: convert to bytes and send normally
        if isinstance(data_stream, FileReader):
            # File-like object
            data_stream.seek(0)  # Reset to beginning
            data = data_stream.read()
        elif isinstance(data_stream, AsyncIterable):
            # Async iterator
            chunks = []
            async for chunk in data_stream:
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                chunks.append(chunk)
            data = b"".join(chunks)
        else:
            # Assume bytes
            data = data_stream

        await self.send(data)

    async def receive_stream(self) -> bytes:
        """Receive streamed data and reassemble into complete message.

        Default implementation falls back to regular receive for protocols
        that don't support streaming. Override in protocols that support it.

        Returns:
            Complete reassembled message bytes

        Raises:
            TransportConnectionError: If not connected
            TransportTimeoutError: If receive times out
            TransportError: If receive fails or stream error
        """
        # Default implementation: receive normally
        return await self.receive()

    async def __aenter__(self) -> TransportInterface:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Async context manager exit."""
        await self.disconnect()


class TransportListener(ABC):
    """Abstract interface for transport listeners (servers).

    This interface defines how transport protocols should handle
    incoming connections and serve requests.
    """

    def __init__(self, host: str, port: int, config: TransportConfig) -> None:
        """Initialize transport listener.

        Args:
            host: Host address to bind to
            port: Port number to listen on
            config: Transport configuration
        """
        self.host = host
        self.port = port
        self.config = config
        self._listening = False

    @property
    def listening(self) -> bool:
        """Check if listener is active."""
        return self._listening

    @property
    def endpoint(self) -> str:
        """Get listener endpoint URL."""
        protocol = self._get_protocol_scheme()
        return f"{protocol}://{self.host}:{self.port}"

    @abstractmethod
    def _get_protocol_scheme(self) -> str:
        """Get protocol scheme for this listener type."""
        pass

    @abstractmethod
    async def start(self) -> None:
        """Start listening for connections.

        Raises:
            TransportError: If listener fails to start
        """
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop listening and close all connections."""
        pass

    @abstractmethod
    async def accept(self) -> TransportInterface:
        """Accept incoming connection.

        Returns:
            Transport interface for the accepted connection

        Raises:
            TransportError: If accept fails
        """
        pass

    async def __aenter__(self) -> TransportListener:
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Async context manager exit."""
        await self.stop()
