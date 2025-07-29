"""
MPREG Transport Layer Abstraction

This module provides a pluggable transport layer that supports multiple protocols
including WebSocket (ws://), WebSocket Secure (wss://), TCP, and TCP+TLS.

The transport layer is designed to be:
- Protocol agnostic - same interface for all transport types
- Security aware - built-in TLS/SSL support
- Performance optimized - minimal abstraction overhead
- Well tested - comprehensive coverage across protocols

Example Usage:
    # WebSocket transport
    transport = TransportFactory.create("ws://localhost:6666")
    await transport.connect()

    # TCP with TLS transport
    transport = TransportFactory.create("tcps://localhost:6666")
    await transport.connect()

    # Send/receive messages
    await transport.send(b"Hello, World!")
    response = await transport.receive()
"""

from .factory import TransportFactory
from .interfaces import (
    SecurityConfig,
    TransportConfig,
    TransportConnectionError,
    TransportError,
    TransportInterface,
    TransportTimeoutError,
)
from .tcp_transport import TCPTransport
from .websocket_transport import WebSocketTransport

__all__ = [
    "TransportInterface",
    "TransportConfig",
    "TransportError",
    "TransportConnectionError",
    "TransportTimeoutError",
    "SecurityConfig",
    "TransportFactory",
    "WebSocketTransport",
    "TCPTransport",
]
