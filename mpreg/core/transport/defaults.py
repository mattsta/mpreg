"""
Centralized transport configuration defaults for MPREG.

This module provides consistent default values across all transport protocols
to ensure uniform behavior and prevent configuration drift between different
transport implementations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Core message size limits - consistent across all transports
DEFAULT_MAX_MESSAGE_SIZE = 20 * 1024 * 1024  # 20MB (up from 1MB for state restoration)
DEFAULT_MAX_STREAM_SIZE = 100 * 1024 * 1024  # 100MB for streaming transfers
DEFAULT_CHUNK_SIZE = 64 * 1024  # 64KB default chunk size

# Connection timeouts
DEFAULT_CONNECT_TIMEOUT = 30.0  # 30 seconds
DEFAULT_READ_TIMEOUT = 60.0  # 60 seconds for large messages
DEFAULT_WRITE_TIMEOUT = 60.0  # 60 seconds for large messages
DEFAULT_HEARTBEAT_TIMEOUT = 10.0  # 10 seconds for ping/pong

# Buffer sizes (consistent across transports)
DEFAULT_READ_BUFFER_SIZE = 128 * 1024  # 128KB
DEFAULT_WRITE_BUFFER_SIZE = 128 * 1024  # 128KB

# Retry configuration
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_RETRY_BACKOFF = 2.0

# Connection type enumeration for clarity
from enum import Enum


class ConnectionType(Enum):
    """Type of connection for configuration purposes."""

    CLIENT = "client"  # User-facing client connections
    INTERNAL = "internal"  # Node-to-node gossip and control plane


@dataclass(frozen=True, slots=True)
class TransportDefaults:
    """Centralized transport defaults with connection type awareness."""

    # Message size limits
    max_message_size: int = DEFAULT_MAX_MESSAGE_SIZE
    max_stream_size: int = DEFAULT_MAX_STREAM_SIZE
    chunk_size: int = DEFAULT_CHUNK_SIZE

    # Timeouts
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    read_timeout: float = DEFAULT_READ_TIMEOUT
    write_timeout: float = DEFAULT_WRITE_TIMEOUT
    heartbeat_timeout: float = DEFAULT_HEARTBEAT_TIMEOUT

    # Buffer sizes
    read_buffer_size: int = DEFAULT_READ_BUFFER_SIZE
    write_buffer_size: int = DEFAULT_WRITE_BUFFER_SIZE

    # Retry configuration
    max_retries: int = DEFAULT_MAX_RETRIES
    retry_delay: float = DEFAULT_RETRY_DELAY
    retry_backoff: float = DEFAULT_RETRY_BACKOFF

    @classmethod
    def for_client_connections(cls) -> TransportDefaults:
        """Get defaults optimized for client connections.

        Client connections handle user data and may need to transfer
        large state restoration data (5-20MB).
        """
        return cls(
            max_message_size=DEFAULT_MAX_MESSAGE_SIZE,  # 20MB for state restoration
            max_stream_size=DEFAULT_MAX_STREAM_SIZE,  # 100MB streaming
            read_timeout=120.0,  # 2 minutes for large transfers
            write_timeout=120.0,  # 2 minutes for large transfers
        )

    @classmethod
    def for_internal_connections(cls) -> TransportDefaults:
        """Get defaults optimized for internal node-to-node connections.

        Internal connections handle gossip protocol, consensus, and
        control plane data which is typically smaller and more frequent.
        """
        return cls(
            max_message_size=5 * 1024 * 1024,  # 5MB for internal messages
            max_stream_size=20 * 1024 * 1024,  # 20MB for internal streaming
            read_timeout=30.0,  # 30 seconds for internal messages
            write_timeout=30.0,  # 30 seconds for internal messages
            heartbeat_timeout=5.0,  # 5 seconds for faster failure detection
        )

    def to_protocol_options(self) -> dict[str, Any]:
        """Convert to protocol options dictionary."""
        return {
            "max_message_size": self.max_message_size,
            "max_stream_size": self.max_stream_size,
            "chunk_size": self.chunk_size,
        }


# Pre-configured defaults for common use cases
CLIENT_DEFAULTS = TransportDefaults.for_client_connections()
INTERNAL_DEFAULTS = TransportDefaults.for_internal_connections()

# Protocol-specific port offsets for multi-protocol adapter
DEFAULT_PORT_OFFSETS = {
    "ws": 0,  # WebSocket
    "wss": 1,  # WebSocket Secure
    "tcp": 2,  # TCP
    "tcps": 3,  # TCP Secure
}
