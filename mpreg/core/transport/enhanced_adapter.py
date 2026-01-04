"""
Enhanced MultiProtocolAdapter with circuit breaker, correlation, and health monitoring.

This module extends MPREG's existing MultiProtocolAdapter with enterprise-grade
reliability features while preserving the unified transport interface.

Key enhancements:
- Circuit breaker protection for connection resilience
- Request correlation tracking for debugging and monitoring
- Sophisticated health scoring and trend analysis
- Integration with existing transport infrastructure
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from loguru import logger

from .circuit_breaker import CircuitBreaker, create_circuit_breaker
from .correlation import create_correlation_tracker
from .enhanced_health import (
    ConnectionHealthMonitor,
    TransportHealthAggregator,
    create_connection_health_monitor,
    create_transport_health_aggregator,
)
from .factory import (
    AdapterStatus,
    ConnectionStats,
    MultiProtocolAdapter,
    MultiProtocolAdapterConfig,
    ProtocolPortAssignmentCallback,
    TransportFactory,
)
from .interfaces import TransportInterface, TransportProtocol

# Type aliases for enhanced adapter
type EndpointUrl = str
type ConnectionId = str


@dataclass(frozen=True, slots=True)
class EnhancedConnectionStats(ConnectionStats):
    """Enhanced connection statistics with health and performance metrics."""

    health_score: float
    circuit_breaker_state: str
    average_latency_ms: float
    success_rate_percent: float
    correlation_count: int


@dataclass(frozen=True, slots=True)
class EnhancedAdapterStatus(AdapterStatus):
    """Enhanced adapter status with reliability metrics."""

    overall_health_score: float
    circuit_breakers_open: int
    total_correlations_tracked: int
    average_response_time_ms: float
    enhanced_stats: list[EnhancedConnectionStats]


@dataclass(slots=True)
class EnhancedMultiProtocolAdapterConfig(MultiProtocolAdapterConfig):
    """Configuration for enhanced multi-protocol adapter."""

    # Circuit breaker settings
    enable_circuit_breakers: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout_ms: float = 60000.0
    circuit_breaker_success_threshold: int = 3

    # Correlation tracking settings
    enable_correlation_tracking: bool = True
    correlation_timeout_ms: float = 30000.0
    max_correlation_history: int = 10000

    # Health monitoring settings
    enable_health_monitoring: bool = True
    health_calculation_interval_ms: float = 5000.0
    max_recent_operations_tracked: int = 100


class EnhancedMultiProtocolAdapter:
    """
    Enhanced multi-protocol adapter with circuit breaker, correlation, and health monitoring.

    This adapter extends MPREG's existing MultiProtocolAdapter functionality with
    enterprise-grade reliability features while preserving unified transport usage.

    Features:
    - Circuit breaker protection for connection resilience
    - Request correlation tracking for debugging and monitoring
    - Sophisticated health scoring and performance analytics
    - Seamless integration with existing MultiProtocolAdapter
    """

    def __init__(
        self,
        config: EnhancedMultiProtocolAdapterConfig,
        connection_handler: Callable | None = None,
    ) -> None:
        """Initialize enhanced multi-protocol adapter."""
        self.config = config
        self.connection_handler = connection_handler

        # Create underlying MultiProtocolAdapter
        self.base_adapter = MultiProtocolAdapter(config, connection_handler)

        # Enhancement components
        self.circuit_breakers: dict[str, CircuitBreaker] = {}
        self.correlation_tracker = (
            create_correlation_tracker(
                correlation_timeout_ms=config.correlation_timeout_ms,
                max_history=config.max_correlation_history,
            )
            if config.enable_correlation_tracking
            else None
        )
        self.health_aggregators: dict[str, TransportHealthAggregator] = {}
        self.connection_monitors: dict[str, ConnectionHealthMonitor] = {}

        # Background tasks
        self._enhancement_tasks: list[asyncio.Task] = []

    @property
    def running(self) -> bool:
        """Check if adapter is running."""
        return self.base_adapter.running

    @property
    def active_protocols(self) -> list[TransportProtocol]:
        """Get list of active transport protocols."""
        return self.base_adapter.active_protocols

    @property
    def endpoints(self) -> dict[str, str]:
        """Get all active endpoints by protocol name."""
        return self.base_adapter.endpoints

    @property
    def connection_type(self):
        """Get the connection type for this adapter."""
        return self.base_adapter.connection_type

    @property
    def active_connection_count(self) -> int:
        """Get total number of active connections across all protocols."""
        return self.base_adapter.active_connection_count

    async def start(self, protocols: list[TransportProtocol] | None = None) -> None:
        """Start enhanced multi-protocol adapter."""
        # Start base adapter
        await self.base_adapter.start(protocols)

        # Initialize enhancement components for each protocol
        if protocols is None:
            protocols = self.config.auto_start_protocols

        for protocol in protocols:
            await self._initialize_protocol_enhancements(protocol)

        # Start background enhancement tasks
        await self._start_enhancement_tasks()

        logger.info(
            f"Enhanced multi-protocol adapter started with {len(protocols)} protocols"
        )

    async def stop(self) -> None:
        """Stop enhanced multi-protocol adapter."""
        # Stop enhancement tasks
        for task in self._enhancement_tasks:
            task.cancel()

        if self._enhancement_tasks:
            await asyncio.gather(*self._enhancement_tasks, return_exceptions=True)

        self._enhancement_tasks.clear()

        # Stop base adapter
        await self.base_adapter.stop()

        # Clean up enhancement components
        self.circuit_breakers.clear()
        self.health_aggregators.clear()
        self.connection_monitors.clear()

        logger.info("Enhanced multi-protocol adapter stopped")

    async def add_protocol(self, protocol: TransportProtocol) -> None:
        """Dynamically add a protocol with enhancements."""
        await self.base_adapter.add_protocol(protocol)
        await self._initialize_protocol_enhancements(protocol)

    async def remove_protocol(self, protocol: TransportProtocol) -> None:
        """Dynamically remove a protocol and its enhancements."""
        await self.base_adapter.remove_protocol(protocol)
        await self._cleanup_protocol_enhancements(protocol)

    async def send_with_enhancements(
        self,
        protocol: TransportProtocol,
        data: bytes,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Send data with full enhancement tracking."""
        endpoint = self.endpoints.get(protocol.value)
        if not endpoint:
            raise ValueError(f"Protocol {protocol.value} not active")

        # Check circuit breaker
        if self.config.enable_circuit_breakers:
            breaker = self.circuit_breakers.get(endpoint)
            if breaker and not breaker.can_execute():
                raise ConnectionError(f"Circuit breaker open for {endpoint}")

        # Start correlation tracking
        if self.correlation_tracker and correlation_id is None:
            correlation_id = self.correlation_tracker.start_correlation()

        start_time = time.time()

        try:
            # Get a connection (simplified - in real implementation would get from base adapter)
            transport = TransportFactory.create(
                endpoint, self.config.protocols.get(protocol)
            )
            await transport.connect()

            try:
                # Send data
                await transport.send(data)
                response = await transport.receive()

                latency_ms = (time.time() - start_time) * 1000.0
                success = True

                # Record success in enhancements
                await self._record_operation_result(
                    endpoint, transport, latency_ms, success
                )

                # Complete correlation tracking
                if self.correlation_tracker and correlation_id:
                    correlation_result = self.correlation_tracker.complete_correlation(
                        correlation_id,
                        response_data=response,
                        endpoint=endpoint,
                        connection_id=str(id(transport)),
                        success=success,
                    )

                    return {
                        "response": response,
                        "correlation_result": correlation_result,
                        "latency_ms": latency_ms,
                        "success": success,
                    }

                return {
                    "response": response,
                    "latency_ms": latency_ms,
                    "success": success,
                }

            finally:
                await transport.disconnect()

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000.0
            success = False

            # Record failure in enhancements
            await self._record_operation_result(endpoint, None, latency_ms, success)

            # Complete correlation tracking with error
            if self.correlation_tracker and correlation_id:
                correlation_result = self.correlation_tracker.complete_correlation(
                    correlation_id,
                    response_data=b"",
                    endpoint=endpoint,
                    connection_id="failed",
                    success=success,
                    error_message=str(e),
                )

                # Don't raise exception if we have correlation tracking - return error result
                return {
                    "response": None,
                    "correlation_result": correlation_result,
                    "latency_ms": latency_ms,
                    "success": success,
                    "error": str(e),
                }

            raise

    def get_enhanced_status(self) -> EnhancedAdapterStatus:
        """Get enhanced adapter status with reliability metrics."""
        base_status = self.base_adapter.get_status()

        # Calculate enhanced metrics
        overall_health_scores = []
        circuit_breakers_open = 0
        total_correlations = 0
        enhanced_stats = []

        for protocol_name, endpoint in self.endpoints.items():
            # Health metrics
            if endpoint in self.health_aggregators:
                health_snapshot = self.health_aggregators[
                    endpoint
                ].get_transport_health_snapshot()
                overall_health_scores.append(health_snapshot.overall_health_score)

            # Circuit breaker status
            if endpoint in self.circuit_breakers:
                breaker = self.circuit_breakers[endpoint]
                if breaker.state.value == "open":
                    circuit_breakers_open += 1

            # Enhanced connection stats
            base_stat = next(
                (s for s in base_status.protocol_stats if s.protocol == protocol_name),
                None,
            )
            if base_stat:
                enhanced_stat = EnhancedConnectionStats(
                    protocol=base_stat.protocol,
                    active_connections=base_stat.active_connections,
                    total_connections=base_stat.total_connections,
                    health_score=health_snapshot.overall_health_score
                    if endpoint in self.health_aggregators
                    else 1.0,
                    circuit_breaker_state=self.circuit_breakers[endpoint].state.value
                    if endpoint in self.circuit_breakers
                    else "closed",
                    average_latency_ms=health_snapshot.average_response_time_ms
                    if endpoint in self.health_aggregators
                    else 0.0,
                    success_rate_percent=100.0
                    - (
                        health_snapshot.error_rate_percent
                        if endpoint in self.health_aggregators
                        else 0.0
                    ),
                    correlation_count=0,  # Would be calculated from correlation tracker
                )
                enhanced_stats.append(enhanced_stat)

        # Correlation metrics
        if self.correlation_tracker:
            correlation_stats = self.correlation_tracker.get_correlation_statistics()
            total_correlations = correlation_stats["total_correlation_history"]

        overall_health = (
            sum(overall_health_scores) / len(overall_health_scores)
            if overall_health_scores
            else 1.0
        )

        return EnhancedAdapterStatus(
            running=base_status.running,
            connection_type=base_status.connection_type,
            active_protocols=base_status.active_protocols,
            endpoints=base_status.endpoints,
            total_active_connections=base_status.total_active_connections,
            protocol_stats=base_status.protocol_stats,
            overall_health_score=overall_health,
            circuit_breakers_open=circuit_breakers_open,
            total_correlations_tracked=total_correlations,
            average_response_time_ms=0.0,  # Would be calculated from health aggregators
            enhanced_stats=enhanced_stats,
        )

    async def _initialize_protocol_enhancements(
        self, protocol: TransportProtocol
    ) -> None:
        """Initialize enhancement components for a protocol."""
        endpoint = self.endpoints.get(protocol.value)
        if not endpoint:
            return

        # Initialize circuit breaker
        if self.config.enable_circuit_breakers:
            self.circuit_breakers[endpoint] = create_circuit_breaker(
                endpoint,
                failure_threshold=self.config.circuit_breaker_failure_threshold,
                recovery_timeout_ms=self.config.circuit_breaker_recovery_timeout_ms,
                success_threshold=self.config.circuit_breaker_success_threshold,
            )

        # Initialize health monitoring
        if self.config.enable_health_monitoring:
            self.health_aggregators[endpoint] = create_transport_health_aggregator(
                endpoint
            )

        logger.debug(
            f"Initialized enhancements for protocol {protocol.value} at {endpoint}"
        )

    async def _cleanup_protocol_enhancements(self, protocol: TransportProtocol) -> None:
        """Clean up enhancement components for a protocol."""
        endpoint = self.endpoints.get(protocol.value)
        if not endpoint:
            return

        # Clean up circuit breaker
        self.circuit_breakers.pop(endpoint, None)

        # Clean up health monitoring
        self.health_aggregators.pop(endpoint, None)

        # Clean up connection monitors for this endpoint
        to_remove = [
            conn_id
            for conn_id, monitor in self.connection_monitors.items()
            if monitor.endpoint == endpoint
        ]
        for conn_id in to_remove:
            del self.connection_monitors[conn_id]

        logger.debug(f"Cleaned up enhancements for protocol {protocol.value}")

    async def _record_operation_result(
        self,
        endpoint: str,
        transport: TransportInterface | None,
        latency_ms: float,
        success: bool,
    ) -> None:
        """Record operation result in all enhancement systems."""
        # Record in circuit breaker
        if endpoint in self.circuit_breakers:
            breaker = self.circuit_breakers[endpoint]
            if success:
                breaker.record_success()
            else:
                breaker.record_failure()

        # Record in health monitoring
        if endpoint in self.health_aggregators and transport:
            connection_id = str(id(transport))

            # Get or create connection monitor
            if connection_id not in self.connection_monitors:
                monitor = create_connection_health_monitor(
                    connection_id,
                    endpoint,
                    max_recent_operations=self.config.max_recent_operations_tracked,
                )
                self.connection_monitors[connection_id] = monitor
                self.health_aggregators[endpoint].add_connection_monitor(monitor)

            # Record operation
            monitor = self.connection_monitors[connection_id]
            monitor.record_operation(latency_ms, success)

    async def _start_enhancement_tasks(self) -> None:
        """Start background tasks for enhancements."""
        if self.correlation_tracker:
            task = asyncio.create_task(self._correlation_cleanup_task())
            self._enhancement_tasks.append(task)

        logger.debug(
            f"Started {len(self._enhancement_tasks)} enhancement background tasks"
        )

    async def _correlation_cleanup_task(self) -> None:
        """Background task for correlation cleanup."""
        try:
            while True:
                await asyncio.sleep(60.0)  # Clean up every minute

                if self.correlation_tracker:
                    expired_count = (
                        self.correlation_tracker.cleanup_expired_correlations()
                    )
                    if expired_count > 0:
                        logger.debug(f"Cleaned up {expired_count} expired correlations")

        except asyncio.CancelledError:
            logger.info("Correlation cleanup task cancelled")
        except Exception as e:
            logger.error(f"Correlation cleanup task error: {e}")

    async def __aenter__(self) -> EnhancedMultiProtocolAdapter:
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.stop()


# Factory function for creating enhanced adapters
def create_enhanced_multi_protocol_adapter(
    connection_type=None,
    host: str = "127.0.0.1",
    base_port: int = 0,
    port_category: str | None = None,
    port_assignment_callback: ProtocolPortAssignmentCallback | None = None,
    port_release_callback: ProtocolPortAssignmentCallback | None = None,
    enable_circuit_breakers: bool = True,
    enable_correlation_tracking: bool = True,
    enable_health_monitoring: bool = True,
    circuit_breaker_failure_threshold: int = 5,
    circuit_breaker_recovery_timeout_ms: float = 60000.0,
    circuit_breaker_success_threshold: int = 3,
    correlation_timeout_ms: float = 30000.0,
    max_correlation_history: int = 10000,
    connection_handler: Callable | None = None,
) -> EnhancedMultiProtocolAdapter:
    """Create an enhanced multi-protocol adapter with reliability features."""
    from .defaults import ConnectionType

    if connection_type is None:
        connection_type = ConnectionType.CLIENT

    config = EnhancedMultiProtocolAdapterConfig(
        connection_type=connection_type,
        host=host,
        base_port=base_port,
        port_category=port_category,
        port_assignment_callback=port_assignment_callback,
        port_release_callback=port_release_callback,
        enable_circuit_breakers=enable_circuit_breakers,
        enable_correlation_tracking=enable_correlation_tracking,
        enable_health_monitoring=enable_health_monitoring,
        circuit_breaker_failure_threshold=circuit_breaker_failure_threshold,
        circuit_breaker_recovery_timeout_ms=circuit_breaker_recovery_timeout_ms,
        circuit_breaker_success_threshold=circuit_breaker_success_threshold,
        correlation_timeout_ms=correlation_timeout_ms,
        max_correlation_history=max_correlation_history,
    )

    return EnhancedMultiProtocolAdapter(config, connection_handler)
