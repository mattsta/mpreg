"""
Correlation tracking for MPREG transport operations.

This module provides correlation tracking functionality that can be integrated
with MPREG's existing transport infrastructure to add request correlation,
performance monitoring, and debugging capabilities.

Key features:
- Unique correlation ID generation and tracking
- Request correlation across transport operations
- Performance metrics collection
- Integration with existing MPREG transport interfaces
"""

from __future__ import annotations

import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from mpreg.datastructures.type_aliases import CorrelationId

# Type aliases for correlation semantics
type EndpointUrl = str
type ConnectionId = str
type TransportLatencyMs = float


@dataclass(frozen=True, slots=True)
class CorrelationResult:
    """Result of a correlated transport operation."""

    correlation_id: CorrelationId
    response_data: bytes
    latency_ms: TransportLatencyMs
    endpoint_used: EndpointUrl
    connection_id: ConnectionId
    success: bool
    error_message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CorrelationConfig:
    """Configuration for correlation tracking."""

    correlation_timeout_ms: float = 30000.0  # 30 seconds
    max_correlation_history: int = 10000
    enable_performance_tracking: bool = True


@dataclass(slots=True)
class CorrelationTracker:
    """Tracker for correlation IDs and request performance."""

    config: CorrelationConfig = field(default_factory=CorrelationConfig)
    active_correlations: dict[CorrelationId, float] = field(default_factory=dict)
    correlation_history: deque[CorrelationResult] = field(
        default_factory=lambda: deque(maxlen=10000)
    )

    def generate_correlation_id(self) -> CorrelationId:
        """Generate a new unique correlation ID."""
        return str(uuid.uuid4())

    def start_correlation(
        self, correlation_id: CorrelationId | None = None
    ) -> CorrelationId:
        """Start tracking a correlation."""
        if correlation_id is None:
            correlation_id = self.generate_correlation_id()

        self.active_correlations[correlation_id] = time.time()
        return correlation_id

    def complete_correlation(
        self,
        correlation_id: CorrelationId,
        response_data: bytes,
        endpoint: EndpointUrl,
        connection_id: ConnectionId,
        success: bool = True,
        error_message: str | None = None,
        **metadata: Any,
    ) -> CorrelationResult:
        """Complete a correlation and record the result."""
        start_time = self.active_correlations.pop(correlation_id, time.time())
        latency_ms = (time.time() - start_time) * 1000.0

        result = CorrelationResult(
            correlation_id=correlation_id,
            response_data=response_data,
            latency_ms=latency_ms,
            endpoint_used=endpoint,
            connection_id=connection_id,
            success=success,
            error_message=error_message,
            metadata=metadata,
        )

        if self.config.enable_performance_tracking:
            self.correlation_history.append(result)

        return result

    def cleanup_expired_correlations(self) -> int:
        """Clean up expired correlations and return count removed."""
        current_time = time.time()
        timeout_seconds = self.config.correlation_timeout_ms / 1000.0

        expired_correlations = [
            corr_id
            for corr_id, start_time in self.active_correlations.items()
            if (current_time - start_time) > timeout_seconds
        ]

        for corr_id in expired_correlations:
            del self.active_correlations[corr_id]

        return len(expired_correlations)

    def get_correlation_statistics(self) -> dict[str, Any]:
        """Get correlation tracking statistics."""
        recent_results = list(self.correlation_history)[-100:]
        successful_results = [r for r in recent_results if r.success]

        if successful_results:
            avg_latency = sum(r.latency_ms for r in successful_results) / len(
                successful_results
            )
            success_rate = len(successful_results) / len(recent_results) * 100.0
        else:
            avg_latency = 0.0
            success_rate = 0.0 if recent_results else 100.0

        return {
            "active_correlations": len(self.active_correlations),
            "total_correlation_history": len(self.correlation_history),
            "average_latency_ms": avg_latency,
            "success_rate_percent": success_rate,
            "recent_operations": len(recent_results),
        }


# Factory function for creating correlation trackers
def create_correlation_tracker(
    correlation_timeout_ms: float = 30000.0,
    max_history: int = 10000,
    enable_performance_tracking: bool = True,
) -> CorrelationTracker:
    """Create a correlation tracker with specified configuration."""
    config = CorrelationConfig(
        correlation_timeout_ms=correlation_timeout_ms,
        max_correlation_history=max_history,
        enable_performance_tracking=enable_performance_tracking,
    )

    return CorrelationTracker(config=config)
