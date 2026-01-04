"""
Circuit breaker pattern for MPREG transport layer.

This module provides circuit breaker functionality that can be integrated
with MPREG's existing MultiProtocolAdapter and transport infrastructure
to add resilience and failure handling capabilities.

Key features:
- State-based circuit breaker (CLOSED, OPEN, HALF_OPEN)
- Configurable failure thresholds and recovery timeouts
- Success tracking for recovery validation
- Integration-ready for existing transport infrastructure
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from loguru import logger

# Type aliases for circuit breaker semantics

type EndpointUrl = str
type FailureCount = int
type SuccessCount = int


class CircuitBreakerState(Enum):
    """Circuit breaker states for connection health management."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, rejecting requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass(frozen=True, slots=True)
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""

    failure_threshold: int = 5
    recovery_timeout_ms: float = 60000.0  # 1 minute
    success_threshold: int = 3  # Successes needed to close circuit


class CircuitBreakerProtocol(Protocol):
    """Protocol for circuit breaker implementations."""

    def record_success(self) -> None:
        """Record a successful operation."""
        ...

    def record_failure(self) -> None:
        """Record a failed operation."""
        ...

    def can_execute(self) -> bool:
        """Check if operations can be executed."""
        ...

    @property
    def state(self) -> CircuitBreakerState:
        """Get current circuit breaker state."""
        ...


@dataclass(slots=True)
class CircuitBreaker:
    """Circuit breaker for connection health management."""

    endpoint: EndpointUrl
    config: CircuitBreakerConfig
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: FailureCount = 0
    success_count: SuccessCount = 0
    last_failure_time: float = 0.0

    def record_success(self) -> None:
        """Record a successful operation."""
        self.failure_count = 0

        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.success_count = 0
                logger.info(f"Circuit breaker CLOSED for {self.endpoint}")

    def record_failure(self) -> None:
        """Record a failed operation."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        self.success_count = 0

        if (
            self.state == CircuitBreakerState.CLOSED
            and self.failure_count >= self.config.failure_threshold
        ):
            self.state = CircuitBreakerState.OPEN
            logger.warning(f"Circuit breaker OPEN for {self.endpoint}")

    def can_execute(self) -> bool:
        """Check if operations can be executed."""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            # Check if recovery timeout has passed
            if (
                time.time() - self.last_failure_time
            ) * 1000 > self.config.recovery_timeout_ms:
                self.state = CircuitBreakerState.HALF_OPEN
                self.success_count = 0
                logger.info(f"Circuit breaker HALF_OPEN for {self.endpoint}")
                return True
            return False
        else:  # HALF_OPEN
            return True


# Factory function for creating circuit breakers
def create_circuit_breaker(
    endpoint: EndpointUrl,
    failure_threshold: int = 5,
    recovery_timeout_ms: float = 60000.0,
    success_threshold: int = 3,
) -> CircuitBreaker:
    """Create a circuit breaker with specified configuration."""
    config = CircuitBreakerConfig(
        failure_threshold=failure_threshold,
        recovery_timeout_ms=recovery_timeout_ms,
        success_threshold=success_threshold,
    )

    return CircuitBreaker(endpoint=endpoint, config=config)
