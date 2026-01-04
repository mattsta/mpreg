"""
Transport factory and multi-protocol adapter system for MPREG.

This module provides the central factory for creating transport instances
and managing multiple protocol adapters simultaneously.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from loguru import logger

from mpreg.datastructures.type_aliases import PortNumber

from ..port_allocator import allocate_port, release_port
from .adapter_registry import (
    ProtocolPortAssignment,
    ProtocolPortAssignmentCallback,
    get_adapter_endpoint_registry,
)
from .defaults import CLIENT_DEFAULTS, INTERNAL_DEFAULTS, ConnectionType
from .interfaces import (
    TransportConfig,
    TransportError,
    TransportInterface,
    TransportListener,
    TransportProtocol,
)


@dataclass(frozen=True, slots=True)
class ConnectionStats:
    """Statistics for a specific transport protocol."""

    protocol: str
    active_connections: int
    total_connections: int


@dataclass(frozen=True, slots=True)
class AdapterStatus:
    """Overall status of the multi-protocol adapter."""

    running: bool
    connection_type: str
    active_protocols: list[str]
    endpoints: dict[str, str]
    total_active_connections: int
    protocol_stats: list[ConnectionStats]


adapter_endpoint_registry = get_adapter_endpoint_registry()


@dataclass(slots=True)
class ProtocolSpec:
    """Specification for a transport protocol.

    This defines the wire protocol, message format, and behavior
    so external clients can implement compatible implementations.
    """

    name: str
    version: str
    description: str

    # Protocol behavior specification
    message_framing: (
        str  # e.g., "length-prefixed", "websocket-frames", "newline-delimited"
    )
    max_message_size: int | None = None
    supports_binary: bool = True
    supports_streaming: bool = True
    connection_oriented: bool = True

    # Security specification
    security_schemes: list[str] = field(default_factory=list)  # e.g., ["TLS", "none"]
    auth_methods: list[str] = field(
        default_factory=list
    )  # e.g., ["bearer", "basic", "none"]

    # Protocol-specific metadata
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for external documentation."""
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "message_framing": self.message_framing,
            "max_message_size": self.max_message_size,
            "supports_binary": self.supports_binary,
            "supports_streaming": self.supports_streaming,
            "connection_oriented": self.connection_oriented,
            "security_schemes": self.security_schemes,
            "auth_methods": self.auth_methods,
            "metadata": self.metadata,
        }


class TransportRegistry:
    """Registry for transport implementations and their specifications."""

    def __init__(self) -> None:
        self._transports: dict[TransportProtocol, type[TransportInterface]] = {}
        self._listeners: dict[TransportProtocol, type[TransportListener]] = {}
        self._specs: dict[TransportProtocol, ProtocolSpec] = {}

    def register_transport(
        self,
        protocol: TransportProtocol,
        transport_class: type[TransportInterface],
        listener_class: type[TransportListener],
        spec: ProtocolSpec,
    ) -> None:
        """Register a transport implementation.

        Args:
            protocol: Transport protocol enum
            transport_class: Client transport implementation
            listener_class: Server listener implementation
            spec: Protocol specification for external clients
        """
        self._transports[protocol] = transport_class
        self._listeners[protocol] = listener_class
        self._specs[protocol] = spec

    def get_transport_class(
        self, protocol: TransportProtocol
    ) -> type[TransportInterface]:
        """Get transport class for protocol."""
        if protocol not in self._transports:
            raise TransportError(
                f"No transport registered for protocol: {protocol.value}"
            )
        return self._transports[protocol]

    def get_listener_class(
        self, protocol: TransportProtocol
    ) -> type[TransportListener]:
        """Get listener class for protocol."""
        if protocol not in self._listeners:
            raise TransportError(
                f"No listener registered for protocol: {protocol.value}"
            )
        return self._listeners[protocol]

    def get_protocol_spec(self, protocol: TransportProtocol) -> ProtocolSpec:
        """Get protocol specification."""
        if protocol not in self._specs:
            raise TransportError(f"No specification for protocol: {protocol.value}")
        return self._specs[protocol]

    def list_protocols(self) -> list[TransportProtocol]:
        """List all registered protocols."""
        return list(self._transports.keys())

    def get_all_specs(self) -> dict[str, dict[str, Any]]:
        """Get all protocol specifications for documentation."""
        return {
            protocol.value: spec.to_dict() for protocol, spec in self._specs.items()
        }


# Global transport registry
_registry = TransportRegistry()


class TransportFactory:
    """Factory for creating transport instances and managing multi-protocol adapters."""

    @staticmethod
    def create(url: str, config: TransportConfig | None = None) -> TransportInterface:
        """Create transport instance for URL.

        Args:
            url: Transport URL (e.g., "ws://host:port", "tcp://host:port")
            config: Optional transport configuration

        Returns:
            Transport instance for the protocol

        Raises:
            TransportError: If protocol is not supported
        """
        if config is None:
            config = TransportConfig()

        parsed = urlparse(url)
        try:
            protocol = TransportProtocol(parsed.scheme.lower())
        except ValueError:
            raise TransportError(f"Unsupported transport protocol: {parsed.scheme}")

        transport_class = _registry.get_transport_class(protocol)
        return transport_class(url, config)

    @staticmethod
    def create_listener(
        protocol: str | TransportProtocol,
        host: str,
        port: int,
        config: TransportConfig | None = None,
    ) -> TransportListener:
        """Create transport listener for protocol.

        Args:
            protocol: Transport protocol name or enum
            host: Host address to bind to
            port: Port to listen on
            config: Optional transport configuration

        Returns:
            Transport listener for the protocol

        Raises:
            TransportError: If protocol is not supported
        """
        if config is None:
            config = TransportConfig()

        if isinstance(protocol, str):
            try:
                protocol = TransportProtocol(protocol.lower())
            except ValueError:
                raise TransportError(f"Unsupported transport protocol: {protocol}")

        listener_class = _registry.get_listener_class(protocol)
        return listener_class(host, port, config)

    @staticmethod
    def get_protocol_spec(protocol: str | TransportProtocol) -> ProtocolSpec:
        """Get protocol specification for external client development.

        Args:
            protocol: Transport protocol name or enum

        Returns:
            Protocol specification

        Raises:
            TransportError: If protocol is not supported
        """
        if isinstance(protocol, str):
            try:
                protocol = TransportProtocol(protocol.lower())
            except ValueError:
                raise TransportError(f"Unsupported transport protocol: {protocol}")

        return _registry.get_protocol_spec(protocol)

    @staticmethod
    def list_supported_protocols() -> list[str]:
        """List all supported transport protocols."""
        return [protocol.value for protocol in _registry.list_protocols()]

    @staticmethod
    def get_all_protocol_specs() -> dict[str, dict[str, Any]]:
        """Get all protocol specifications for documentation generation."""
        return _registry.get_all_specs()


@dataclass(slots=True)
class MultiProtocolAdapterConfig:
    """Configuration for multi-protocol adapter with connection type awareness."""

    # Connection type for this adapter instance
    connection_type: ConnectionType = ConnectionType.CLIENT

    # Protocol configurations (auto-populated based on connection type)
    protocols: dict[TransportProtocol, TransportConfig] = field(default_factory=dict)

    # Binding configuration
    host: str = "127.0.0.1"
    # Use 0 to auto-allocate per-protocol ports via the port allocator.
    base_port: int = 0
    port_category: str | None = None
    port_assignment_callback: ProtocolPortAssignmentCallback | None = None
    port_release_callback: ProtocolPortAssignmentCallback | None = None
    port_offset: dict[TransportProtocol, int] = field(
        default_factory=lambda: {
            TransportProtocol.WEBSOCKET: 0,  # port + 0
            TransportProtocol.WEBSOCKET_SECURE: 1,  # port + 1
            TransportProtocol.TCP: 2,  # port + 2
            TransportProtocol.TCP_SECURE: 3,  # port + 3
        }
    )

    # Auto-start configuration
    auto_start_protocols: list[TransportProtocol] = field(
        default_factory=lambda: [TransportProtocol.WEBSOCKET]
    )

    def __post_init__(self) -> None:
        """Initialize protocol configurations based on connection type."""

        def _compose_callbacks(
            primary: ProtocolPortAssignmentCallback,
            secondary: ProtocolPortAssignmentCallback | None,
        ) -> ProtocolPortAssignmentCallback:
            if secondary is None or secondary is primary:
                return primary

            def _combined(assignment: ProtocolPortAssignment):
                primary_result = primary(assignment)
                secondary_result = secondary(assignment)
                if inspect.isawaitable(primary_result) or inspect.isawaitable(
                    secondary_result
                ):

                    async def _await_both():
                        if inspect.isawaitable(primary_result):
                            await primary_result
                        if inspect.isawaitable(secondary_result):
                            await secondary_result

                    return _await_both()
                return None

            return _combined

        if self.base_port < 0:
            raise ValueError("base_port must be >= 0 (use 0 for auto-allocation)")
        if not self.port_category:
            self.port_category = (
                "servers"
                if self.connection_type == ConnectionType.CLIENT
                else "federation"
            )
        self.port_assignment_callback = _compose_callbacks(
            adapter_endpoint_registry.record,
            self.port_assignment_callback,
        )
        self.port_release_callback = _compose_callbacks(
            adapter_endpoint_registry.release,
            self.port_release_callback,
        )
        if not self.protocols:
            # Use appropriate defaults based on connection type
            base_defaults = (
                CLIENT_DEFAULTS.to_protocol_options()
                if self.connection_type == ConnectionType.CLIENT
                else INTERNAL_DEFAULTS.to_protocol_options()
            )

            # Create default configs for all protocols
            for protocol in TransportProtocol:
                self.protocols[protocol] = TransportConfig(
                    protocol_options=base_defaults.copy()
                )


class MultiProtocolAdapter:
    """Multi-protocol connection manager for handling multiple transport types simultaneously.

    This enhanced adapter provides connection type awareness, allowing different
    configurations for client connections (user data) vs internal connections
    (node-to-node gossip and control plane).

    Features:
    - Multiple transport protocols active simultaneously
    - Connection type awareness (CLIENT vs INTERNAL)
    - Automatic configuration based on connection type
    - Dynamic protocol addition/removal
    - Connection pooling and management
    - Consistent defaults across all transports

    Example:
        def report_port(assignment: ProtocolPortAssignment) -> None:
            logger.info(
                "Assigned {} {} on {}",
                assignment.connection_type.value,
                assignment.protocol.value,
                assignment.endpoint,
            )

        # Client connection adapter (auto-allocated ports)
        client_config = MultiProtocolAdapterConfig(
            connection_type=ConnectionType.CLIENT,
            base_port=0,
            port_category="servers",
            port_assignment_callback=report_port,
        )
        client_adapter = MultiProtocolAdapter(client_config, handle_client_connection)
        await client_adapter.start()

        # Internal connection adapter (auto-allocated ports)
        internal_config = MultiProtocolAdapterConfig(
            connection_type=ConnectionType.INTERNAL,
            base_port=0,
            port_category="federation",
            port_assignment_callback=report_port,
        )
        internal_adapter = MultiProtocolAdapter(
            internal_config, handle_internal_connection
        )
        await internal_adapter.start()
    """

    def __init__(
        self,
        config: MultiProtocolAdapterConfig,
        connection_handler: Callable | None = None,
    ) -> None:
        """Initialize multi-protocol adapter.

        Args:
            config: Multi-protocol configuration
            connection_handler: Async callable to handle new connections
        """
        self.config = config
        self.connection_handler = connection_handler
        self._listeners: dict[TransportProtocol, TransportListener] = {}
        self._listen_tasks: dict[TransportProtocol, asyncio.Task] = {}
        self._active_connections: dict[TransportProtocol, list[TransportInterface]] = {}
        self._connection_stats: dict[TransportProtocol, int] = {}
        self._protocol_ports: dict[TransportProtocol, PortNumber] = {}
        self._auto_allocated_ports: set[TransportProtocol] = set()
        self._running = False

    @property
    def running(self) -> bool:
        """Check if adapter is running."""
        return self._running

    @property
    def active_protocols(self) -> list[TransportProtocol]:
        """Get list of active transport protocols."""
        return list(self._listeners.keys())

    @property
    def endpoints(self) -> dict[str, str]:
        """Get all active endpoints by protocol name."""
        return {
            protocol.value: listener.endpoint
            for protocol, listener in self._listeners.items()
        }

    @property
    def connection_type(self) -> ConnectionType:
        """Get the connection type for this adapter."""
        return self.config.connection_type

    @property
    def active_connection_count(self) -> int:
        """Get total number of active connections across all protocols."""
        return sum(
            len(connections) for connections in self._active_connections.values()
        )

    @property
    def connection_stats(self) -> list[ConnectionStats]:
        """Get connection statistics by protocol."""
        return [
            ConnectionStats(
                protocol=protocol.value,
                active_connections=len(self._active_connections.get(protocol, [])),
                total_connections=self._connection_stats.get(protocol, 0),
            )
            for protocol in self._listeners
        ]

    def get_status(self) -> AdapterStatus:
        """Get comprehensive adapter status."""
        return AdapterStatus(
            running=self.running,
            connection_type=self.connection_type.value,
            active_protocols=[p.value for p in self.active_protocols],
            endpoints=self.endpoints,
            total_active_connections=self.active_connection_count,
            protocol_stats=self.connection_stats,
        )

    def _resolve_protocol_port(self, protocol: TransportProtocol) -> PortNumber:
        if protocol in self._protocol_ports:
            return self._protocol_ports[protocol]

        if self.config.base_port > 0:
            port = self.config.base_port + self.config.port_offset.get(protocol, 0)
            self._protocol_ports[protocol] = port
            return port

        port = allocate_port(self.config.port_category)
        self._protocol_ports[protocol] = port
        self._auto_allocated_ports.add(protocol)
        return port

    def _release_protocol_port(self, protocol: TransportProtocol) -> None:
        if protocol not in self._protocol_ports:
            return
        port = self._protocol_ports.pop(protocol)
        if protocol in self._auto_allocated_ports:
            release_port(port)
            self._auto_allocated_ports.discard(protocol)

    async def _notify_port_assignment(
        self, protocol: TransportProtocol, port: PortNumber
    ) -> None:
        callback = self.config.port_assignment_callback
        if not callback:
            return
        assignment = ProtocolPortAssignment(
            protocol=protocol,
            host=self.config.host,
            port=port,
            connection_type=self.connection_type,
        )
        result = callback(assignment)
        if inspect.isawaitable(result):
            await result

    async def _notify_port_release(
        self, protocol: TransportProtocol, port: PortNumber
    ) -> None:
        callback = self.config.port_release_callback
        if not callback:
            return
        assignment = ProtocolPortAssignment(
            protocol=protocol,
            host=self.config.host,
            port=port,
            connection_type=self.connection_type,
        )
        result = callback(assignment)
        if inspect.isawaitable(result):
            await result

    async def start(
        self,
        protocols: list[TransportProtocol] | None = None,
    ) -> None:
        """Start multi-protocol adapter.

        Args:
            protocols: Optional list of protocols to start.
                      If None, uses auto_start_protocols from config.
        """
        if self._running:
            raise TransportError("Multi-protocol adapter is already running")

        if protocols is None:
            protocols = self.config.auto_start_protocols

        # Start listeners for each protocol
        for protocol in protocols:
            await self._start_protocol(protocol)

        self._running = True

    async def stop(self) -> None:
        """Stop multi-protocol adapter and all listeners."""
        if not self._running:
            return

        # Cancel all listen tasks
        for task in self._listen_tasks.values():
            task.cancel()

        # Wait for tasks to complete
        if self._listen_tasks:
            await asyncio.gather(*self._listen_tasks.values(), return_exceptions=True)

        # Stop all listeners
        for protocol, listener in list(self._listeners.items()):
            await listener.stop()
            port = self._protocol_ports.get(protocol)
            if port is not None:
                await self._notify_port_release(protocol, port)

        # Clear all tracking data
        self._listeners.clear()
        self._listen_tasks.clear()
        self._active_connections.clear()
        for protocol in list(self._protocol_ports.keys()):
            self._release_protocol_port(protocol)
        self._protocol_ports.clear()
        self._auto_allocated_ports.clear()
        # Note: Don't clear connection_stats as they are historical
        self._running = False

    async def add_protocol(self, protocol: TransportProtocol) -> None:
        """Dynamically add a protocol to the running adapter.

        Args:
            protocol: Transport protocol to add
        """
        if protocol in self._listeners:
            raise TransportError(f"Protocol {protocol.value} is already active")

        await self._start_protocol(protocol)

    async def remove_protocol(self, protocol: TransportProtocol) -> None:
        """Dynamically remove a protocol from the adapter.

        Args:
            protocol: Transport protocol to remove
        """
        if protocol not in self._listeners:
            raise TransportError(f"Protocol {protocol.value} is not active")

        # Cancel listen task
        if protocol in self._listen_tasks:
            self._listen_tasks[protocol].cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listen_tasks[protocol]
            del self._listen_tasks[protocol]

        # Stop listener
        await self._listeners[protocol].stop()
        del self._listeners[protocol]
        port = self._protocol_ports.get(protocol)
        if port is not None:
            await self._notify_port_release(protocol, port)
        self._release_protocol_port(protocol)

    async def _start_protocol(self, protocol: TransportProtocol) -> None:
        """Start listener for a specific protocol."""
        port = self._resolve_protocol_port(protocol)
        transport_config = self.config.protocols.get(protocol, TransportConfig())

        listener = TransportFactory.create_listener(
            protocol, self.config.host, port, transport_config
        )

        try:
            await listener.start()
        except Exception:
            self._release_protocol_port(protocol)
            raise

        self._listeners[protocol] = listener
        await self._notify_port_assignment(protocol, port)

        # Start accept loop if we have a connection handler
        if self.connection_handler:
            task = asyncio.create_task(self._accept_loop(protocol, listener))
            self._listen_tasks[protocol] = task

    async def _accept_loop(
        self,
        protocol: TransportProtocol,
        listener: TransportListener,
    ) -> None:
        """Accept loop for incoming connections."""
        try:
            while True:
                try:
                    transport = await listener.accept()
                    # Handle connection in background task
                    asyncio.create_task(self._handle_connection(protocol, transport))
                except Exception as e:
                    # Log error but continue accepting
                    logger.warning(
                        "Error accepting {} connection: {}",
                        protocol.value,
                        e,
                    )
        except asyncio.CancelledError:
            pass

    async def _handle_connection(
        self,
        protocol: TransportProtocol,
        transport: TransportInterface,
    ) -> None:
        """Handle individual connection with tracking."""
        # Initialize connection tracking for this protocol
        if protocol not in self._active_connections:
            self._active_connections[protocol] = []
        if protocol not in self._connection_stats:
            self._connection_stats[protocol] = 0

        # Track the connection
        self._active_connections[protocol].append(transport)
        self._connection_stats[protocol] += 1

        try:
            if self.connection_handler:
                await self.connection_handler(protocol, transport)
        except Exception as e:
            logger.warning(
                "Error handling {} connection: {}",
                protocol.value,
                e,
            )
        finally:
            # Clean up connection tracking
            if transport in self._active_connections[protocol]:
                self._active_connections[protocol].remove(transport)
            await transport.disconnect()

    async def __aenter__(self) -> MultiProtocolAdapter:
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.stop()


def register_transport(
    protocol: TransportProtocol,
    transport_class: type[TransportInterface],
    listener_class: type[TransportListener],
    spec: ProtocolSpec,
) -> None:
    """Register a transport implementation in the global registry.

    This function is used by transport implementations to register themselves
    with the factory system.

    Args:
        protocol: Transport protocol enum
        transport_class: Client transport implementation
        listener_class: Server listener implementation
        spec: Protocol specification for external clients
    """
    _registry.register_transport(protocol, transport_class, listener_class, spec)
