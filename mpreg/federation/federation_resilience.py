"""
Enhanced Federation Resilience for MPREG.

This module provides comprehensive resilience patterns for federated operations:
- Advanced circuit breakers with adaptive thresholds
- Health monitoring and auto-recovery
- Graceful degradation strategies
- Retry policies with exponential backoff
- Connection pooling and load balancing
- Performance-based routing decisions
- Real-time alerting and monitoring
"""

import asyncio
import random
import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from threading import RLock
from typing import Any

from loguru import logger

from ..core.statistics import (
    CircuitBreakerState,
    ClusterHealthInfo,
    FederationHealth,
    HealthCheckResult,
    ResilienceMetrics,
    ResourceMetrics,
)
from .federation_optimized import CircuitBreaker, ClusterIdentity


@dataclass(frozen=True, slots=True)
class AlertData:
    """Data structure for health alerts."""

    cluster_id: str
    health_score: float
    consecutive_failures: int
    recent_errors: list[str]
    average_latency_ms: float


class HealthStatus(Enum):
    """Health status levels for federation components."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class RecoveryStrategy(Enum):
    """Recovery strategies for unhealthy clusters."""

    IMMEDIATE_RETRY = "immediate_retry"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    CIRCUIT_BREAKER = "circuit_breaker"
    GRACEFUL_DEGRADATION = "graceful_degradation"
    FAILOVER = "failover"


@dataclass(slots=True)
class HealthCheckConfiguration:
    """Configuration for health check operations."""

    check_interval_seconds: float = 30.0
    timeout_seconds: float = 5.0
    consecutive_failures_threshold: int = 3
    consecutive_successes_threshold: int = 2
    adaptive_interval: bool = True
    min_interval_seconds: float = 10.0
    max_interval_seconds: float = 300.0
    health_check_timeout_multiplier: float = 1.5


@dataclass(slots=True)
class RetryConfiguration:
    """Configuration for retry policies."""

    max_attempts: int = 3
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_multiplier: float = 2.0
    jitter_factor: float = 0.1
    retry_on_timeout: bool = True
    retry_on_connection_error: bool = True
    retry_on_server_error: bool = False


@dataclass(slots=True)
class ClusterHealthMetrics:
    """Comprehensive health metrics for a federated cluster."""

    cluster_id: str
    status: HealthStatus = HealthStatus.UNKNOWN
    last_health_check: float = 0.0
    consecutive_failures: int = 0
    consecutive_successes: int = 0

    # Performance metrics
    average_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    success_rate: float = 1.0
    throughput_per_second: float = 0.0

    # Resource metrics
    cpu_usage_percent: float = 0.0
    memory_usage_percent: float = 0.0
    connection_count: int = 0
    queue_depth: int = 0

    # Error tracking
    recent_errors: deque[str] = field(default_factory=lambda: deque(maxlen=10))
    error_rate_per_minute: float = 0.0

    # Health score (0-100)
    health_score: float = 100.0

    _lock: RLock = field(default_factory=RLock)


@dataclass(slots=True)
class AdaptiveCircuitBreaker(CircuitBreaker):
    """
    Enhanced circuit breaker with adaptive thresholds and intelligent recovery.

    Extends the base CircuitBreaker with:
    - Dynamic failure thresholds based on cluster health
    - Performance-based timeout adjustments
    - Recovery prediction algorithms
    - Health score integration
    """

    # Adaptive behavior configuration
    adaptive_thresholds: bool = True
    base_failure_threshold: int = 5
    max_failure_threshold: int = 20
    health_score_weight: float = 0.3
    performance_weight: float = 0.4
    stability_weight: float = 0.3

    # Performance tracking
    recent_latencies: deque[float] = field(default_factory=lambda: deque(maxlen=100))
    baseline_latency_ms: float = 100.0
    latency_threshold_multiplier: float = 3.0

    def update_adaptive_thresholds(self, health_metrics: ClusterHealthMetrics) -> None:
        """Update circuit breaker thresholds based on cluster health."""
        if not self.adaptive_thresholds:
            return

        with self._lock:
            # Calculate adaptive threshold based on health score
            health_factor = health_metrics.health_score / 100.0

            # Lower threshold for unhealthy clusters
            new_threshold = max(
                2,  # Minimum threshold
                int(self.base_failure_threshold * health_factor),
            )

            if new_threshold != self.failure_threshold:
                logger.info(
                    f"Adaptive circuit breaker threshold updated: "
                    f"{self.failure_threshold} -> {new_threshold} "
                    f"(health score: {health_metrics.health_score:.1f})"
                )
                self.failure_threshold = new_threshold

    def record_latency(self, latency_ms: float) -> None:
        """Record latency measurement for performance tracking."""
        with self._lock:
            self.recent_latencies.append(latency_ms)

            # Check if latency indicates performance degradation
            if len(self.recent_latencies) >= 10:
                avg_recent = sum(list(self.recent_latencies)[-10:]) / 10
                if (
                    avg_recent
                    > self.baseline_latency_ms * self.latency_threshold_multiplier
                ):
                    logger.warning(
                        f"Performance degradation detected: "
                        f"avg {avg_recent:.1f}ms > threshold {self.baseline_latency_ms * self.latency_threshold_multiplier:.1f}ms"
                    )
                    # Treat as a soft failure - increment by 1 for type safety
                    self.failure_count += 1

    def predict_recovery_time(self) -> float:
        """Predict when the circuit might recover based on patterns."""
        if self.state != "open":
            return 0.0

        # Simple prediction based on recent failure patterns
        time_since_failure = time.time() - self.last_failure_time
        remaining_timeout = max(0, self.current_timeout - time_since_failure)

        # Add some intelligence based on failure pattern
        if self.failure_count > self.failure_threshold * 2:
            # Severe failures, add extra recovery time
            return remaining_timeout * 1.5

        return remaining_timeout


@dataclass(slots=True)
class FederationHealthMonitor:
    """
    Comprehensive health monitoring for federation clusters.

    Provides:
    - Continuous health checking
    - Performance monitoring
    - Alerting and notifications
    - Auto-recovery coordination
    - Health trend analysis
    """

    cluster_id: str
    health_config: HealthCheckConfiguration = field(
        default_factory=HealthCheckConfiguration
    )
    retry_config: RetryConfiguration = field(default_factory=RetryConfiguration)

    # Health state
    cluster_health: dict[str, ClusterHealthMetrics] = field(default_factory=dict)
    global_health: HealthStatus = HealthStatus.UNKNOWN

    # Monitoring tasks
    _monitoring_tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)

    # Alert callbacks
    alert_callbacks: list[Callable[[str, HealthStatus, "AlertData"], None]] = field(
        default_factory=list
    )

    # Performance tracking
    _performance_history: defaultdict[str, deque[float]] = field(
        default_factory=lambda: defaultdict(lambda: deque(maxlen=1000))
    )

    _lock: RLock = field(default_factory=RLock)

    async def start_monitoring(self) -> None:
        """Start health monitoring for all registered clusters."""
        logger.info(
            f"Starting federation health monitoring for cluster {self.cluster_id}"
        )

        # Start health check tasks for each cluster
        for cluster_id in self.cluster_health:
            task = asyncio.create_task(self._monitor_cluster_health(cluster_id))
            self._monitoring_tasks.add(task)

        # Start global health assessment task
        global_task = asyncio.create_task(self._assess_global_health())
        self._monitoring_tasks.add(global_task)

        # Start performance trend analysis
        trend_task = asyncio.create_task(self._analyze_performance_trends())
        self._monitoring_tasks.add(trend_task)

    async def stop_monitoring(self) -> None:
        """Stop all health monitoring tasks."""
        logger.info("Stopping federation health monitoring")
        self._shutdown_event.set()

        # Cancel all monitoring tasks
        for task in self._monitoring_tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete
        if self._monitoring_tasks:
            await asyncio.gather(*self._monitoring_tasks, return_exceptions=True)

        self._monitoring_tasks.clear()

    def register_cluster(self, cluster_identity: ClusterIdentity) -> None:
        """Register a cluster for health monitoring."""
        with self._lock:
            if cluster_identity.cluster_id not in self.cluster_health:
                self.cluster_health[cluster_identity.cluster_id] = ClusterHealthMetrics(
                    cluster_id=cluster_identity.cluster_id
                )
                logger.info(
                    f"Registered cluster {cluster_identity.cluster_id} for health monitoring"
                )

    def unregister_cluster(self, cluster_id: str) -> None:
        """Unregister a cluster from health monitoring."""
        with self._lock:
            if cluster_id in self.cluster_health:
                del self.cluster_health[cluster_id]
                logger.info(f"Unregistered cluster {cluster_id} from health monitoring")

    def add_alert_callback(
        self, callback: Callable[[str, HealthStatus, AlertData], None]
    ) -> None:
        """Add a callback function for health alerts."""
        self.alert_callbacks.append(callback)

    async def _monitor_cluster_health(self, cluster_id: str) -> None:
        """Monitor health for a specific cluster."""
        while not self._shutdown_event.is_set():
            try:
                health_metrics = self.cluster_health.get(cluster_id)
                if not health_metrics:
                    break

                # Perform health check
                check_result = await self._perform_health_check(cluster_id)

                # Update health metrics
                await self._update_health_metrics(cluster_id, check_result)

                # Determine sleep interval (adaptive)
                sleep_interval = self._calculate_check_interval(health_metrics)

                await asyncio.sleep(sleep_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring cluster {cluster_id}: {e}")
                await asyncio.sleep(self.health_config.check_interval_seconds)

    async def _perform_health_check(self, cluster_id: str) -> HealthCheckResult:
        """Perform a comprehensive health check for a cluster."""
        start_time = time.time()

        try:
            # This would be implemented to actually check cluster health
            # For now, simulate a health check
            await asyncio.sleep(0.1)  # Simulate network call

            # Simulate various health states
            if random.random() < 0.95:  # 95% success rate for demo
                latency_ms = random.uniform(10, 100)
                resource_metrics = ResourceMetrics(
                    cpu_usage=random.uniform(10, 80),
                    memory_usage=random.uniform(20, 70),
                    connection_count=random.randint(5, 50),
                    queue_depth=random.randint(0, 10),
                )
                return HealthCheckResult(
                    cluster_id=cluster_id,
                    status=HealthStatus.HEALTHY.value,
                    latency_ms=latency_ms,
                    timestamp=time.time(),
                    resource_metrics=resource_metrics,
                )
            else:
                return HealthCheckResult(
                    cluster_id=cluster_id,
                    status=HealthStatus.UNHEALTHY.value,
                    latency_ms=1000.0,  # High latency indicates issues
                    timestamp=time.time(),
                    error_message="Connection timeout",
                )

        except Exception as e:
            return HealthCheckResult(
                cluster_id=cluster_id,
                status=HealthStatus.CRITICAL.value,
                latency_ms=time.time() - start_time * 1000,
                timestamp=time.time(),
                error_message=str(e),
            )

    async def _update_health_metrics(
        self, cluster_id: str, result: HealthCheckResult
    ) -> None:
        """Update health metrics based on check result."""
        with self._lock:
            health_metrics = self.cluster_health.get(cluster_id)
            if not health_metrics:
                return

            health_metrics.last_health_check = result.timestamp

            if result.status == HealthStatus.HEALTHY.value:
                health_metrics.consecutive_failures = 0
                health_metrics.consecutive_successes += 1
                health_metrics.status = HealthStatus.HEALTHY

                # Update performance metrics
                health_metrics.average_latency_ms = (
                    health_metrics.average_latency_ms * 0.9 + result.latency_ms * 0.1
                )

                # Update health score
                health_metrics.health_score = min(
                    100.0, health_metrics.health_score + 5.0
                )

            else:
                health_metrics.consecutive_successes = 0
                health_metrics.consecutive_failures += 1

                # Update status based on consecutive failures
                if (
                    health_metrics.consecutive_failures
                    >= self.health_config.consecutive_failures_threshold
                ):
                    # Convert string status back to enum
                    if result.status == HealthStatus.UNHEALTHY.value:
                        health_metrics.status = HealthStatus.UNHEALTHY
                    elif result.status == HealthStatus.CRITICAL.value:
                        health_metrics.status = HealthStatus.CRITICAL
                    else:
                        health_metrics.status = HealthStatus.DEGRADED

                # Add error to recent errors
                if result.error_message:
                    health_metrics.recent_errors.append(
                        f"{time.strftime('%H:%M:%S')}: {result.error_message}"
                    )

                # Decrease health score
                health_metrics.health_score = max(
                    0.0, health_metrics.health_score - 10.0
                )

            # Update resource metrics from resource_metrics field
            if result.resource_metrics:
                health_metrics.cpu_usage_percent = result.resource_metrics.cpu_usage
                health_metrics.memory_usage_percent = (
                    result.resource_metrics.memory_usage
                )
                health_metrics.connection_count = (
                    result.resource_metrics.connection_count
                )
                health_metrics.queue_depth = result.resource_metrics.queue_depth

            # Trigger alerts if status changed
            await self._check_and_trigger_alerts(cluster_id, health_metrics)

    async def _check_and_trigger_alerts(
        self, cluster_id: str, health_metrics: ClusterHealthMetrics
    ) -> None:
        """Check if alerts should be triggered based on health changes."""
        # Trigger alerts for status changes or critical conditions
        if health_metrics.status in [HealthStatus.UNHEALTHY, HealthStatus.CRITICAL]:
            alert_data = AlertData(
                cluster_id=cluster_id,
                health_score=health_metrics.health_score,
                consecutive_failures=health_metrics.consecutive_failures,
                recent_errors=list(health_metrics.recent_errors),
                average_latency_ms=health_metrics.average_latency_ms,
            )

            # Call alert callbacks
            for callback in self.alert_callbacks:
                try:
                    callback(cluster_id, health_metrics.status, alert_data)
                except Exception as e:
                    logger.error(f"Error calling alert callback: {e}")

    def _calculate_check_interval(self, health_metrics: ClusterHealthMetrics) -> float:
        """Calculate adaptive check interval based on cluster health."""
        if not self.health_config.adaptive_interval:
            return self.health_config.check_interval_seconds

        # Shorter intervals for unhealthy clusters
        if health_metrics.status in [HealthStatus.UNHEALTHY, HealthStatus.CRITICAL]:
            return self.health_config.min_interval_seconds
        elif health_metrics.status == HealthStatus.DEGRADED:
            return self.health_config.check_interval_seconds * 0.5
        else:
            # Longer intervals for healthy clusters
            return min(
                self.health_config.max_interval_seconds,
                self.health_config.check_interval_seconds * 2.0,
            )

    async def _assess_global_health(self) -> None:
        """Assess overall federation health."""
        while not self._shutdown_event.is_set():
            try:
                with self._lock:
                    if not self.cluster_health:
                        self.global_health = HealthStatus.UNKNOWN
                    else:
                        healthy_count = sum(
                            1
                            for metrics in self.cluster_health.values()
                            if metrics.status == HealthStatus.HEALTHY
                        )

                        total_count = len(self.cluster_health)
                        health_ratio = healthy_count / total_count

                        if health_ratio >= 0.8:
                            self.global_health = HealthStatus.HEALTHY
                        elif health_ratio >= 0.6:
                            self.global_health = HealthStatus.DEGRADED
                        else:
                            self.global_health = HealthStatus.UNHEALTHY

                await asyncio.sleep(60.0)  # Assess global health every minute

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error assessing global health: {e}")
                await asyncio.sleep(60.0)

    async def _analyze_performance_trends(self) -> None:
        """Analyze performance trends and predict issues."""
        while not self._shutdown_event.is_set():
            try:
                # Analyze trends every 5 minutes
                await asyncio.sleep(300.0)

                with self._lock:
                    for cluster_id, health_metrics in self.cluster_health.items():
                        # Add current metrics to performance history
                        history = self._performance_history[cluster_id]
                        history.append(health_metrics.health_score)

                        # Analyze trends if we have enough data
                        if len(history) >= 10:
                            recent_scores = list(history)[-10:]
                            trend = (recent_scores[-1] - recent_scores[0]) / len(
                                recent_scores
                            )

                            # Alert on declining trends
                            if (
                                trend < -2.0
                            ):  # Declining by more than 2 points per measurement
                                logger.warning(
                                    f"Declining health trend detected for cluster {cluster_id}: "
                                    f"trend={trend:.2f} points per check"
                                )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error analyzing performance trends: {e}")
                await asyncio.sleep(300.0)

    def get_health_summary(self) -> FederationHealth:
        """Get comprehensive health summary."""
        with self._lock:
            cluster_health_info = {}

            for cluster_id, metrics in self.cluster_health.items():
                cluster_health_info[cluster_id] = ClusterHealthInfo(
                    status=metrics.status.value,
                    health_score=metrics.health_score,
                    consecutive_failures=metrics.consecutive_failures,
                    average_latency_ms=metrics.average_latency_ms,
                    cpu_usage_percent=metrics.cpu_usage_percent,
                    memory_usage_percent=metrics.memory_usage_percent,
                    last_check=metrics.last_health_check,
                )

            return FederationHealth(
                global_status=self.global_health.value,
                cluster_health=cluster_health_info,
                total_clusters=len(self.cluster_health),
                healthy_clusters=sum(
                    1
                    for m in self.cluster_health.values()
                    if m.status == HealthStatus.HEALTHY
                ),
                degraded_clusters=sum(
                    1
                    for m in self.cluster_health.values()
                    if m.status == HealthStatus.DEGRADED
                ),
                unhealthy_clusters=sum(
                    1
                    for m in self.cluster_health.values()
                    if m.status in [HealthStatus.UNHEALTHY, HealthStatus.CRITICAL]
                ),
            )


@dataclass(slots=True)
class FederationAutoRecovery:
    """
    Auto-recovery system for federation clusters.

    Implements various recovery strategies:
    - Automatic retry with exponential backoff
    - Circuit breaker coordination
    - Graceful degradation
    - Failover to backup clusters
    - Connection pool management
    """

    cluster_id: str
    health_monitor: FederationHealthMonitor
    retry_config: RetryConfiguration = field(default_factory=RetryConfiguration)

    # Recovery state
    recovery_strategies: dict[str, RecoveryStrategy] = field(default_factory=dict)
    recovery_tasks: set[asyncio.Task[Any]] = field(default_factory=set)

    # Recovery callbacks
    recovery_callbacks: list[Callable[[str, RecoveryStrategy, bool], None]] = field(
        default_factory=list
    )

    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)
    _lock: RLock = field(default_factory=RLock)

    async def start_auto_recovery(self) -> None:
        """Start auto-recovery monitoring."""
        logger.info(f"Starting auto-recovery for cluster {self.cluster_id}")

        # Register for health alerts
        self.health_monitor.add_alert_callback(self._handle_health_alert)

        # Start recovery coordination task
        recovery_task = asyncio.create_task(self._coordinate_recovery())
        self.recovery_tasks.add(recovery_task)

    async def stop_auto_recovery(self) -> None:
        """Stop auto-recovery monitoring."""
        logger.info("Stopping auto-recovery")
        self._shutdown_event.set()

        # Cancel recovery tasks
        for task in self.recovery_tasks:
            if not task.done():
                task.cancel()

        if self.recovery_tasks:
            await asyncio.gather(*self.recovery_tasks, return_exceptions=True)

        self.recovery_tasks.clear()

    def _handle_health_alert(
        self, cluster_id: str, status: HealthStatus, alert_data: AlertData
    ) -> None:
        """Handle health alerts and initiate recovery if needed."""
        logger.warning(f"Health alert for cluster {cluster_id}: {status}")

        # Determine recovery strategy based on status and alert data
        if status == HealthStatus.CRITICAL:
            strategy = RecoveryStrategy.CIRCUIT_BREAKER
        elif status == HealthStatus.UNHEALTHY:
            if alert_data.consecutive_failures > 5:
                strategy = RecoveryStrategy.FAILOVER
            else:
                strategy = RecoveryStrategy.EXPONENTIAL_BACKOFF
        elif status == HealthStatus.DEGRADED:
            strategy = RecoveryStrategy.GRACEFUL_DEGRADATION
        else:
            return  # No recovery needed

        # Set recovery strategy
        with self._lock:
            self.recovery_strategies[cluster_id] = strategy

        logger.info(f"Initiating {strategy.value} recovery for cluster {cluster_id}")

    async def _coordinate_recovery(self) -> None:
        """Coordinate recovery efforts for unhealthy clusters."""
        while not self._shutdown_event.is_set():
            try:
                # Check for clusters needing recovery
                clusters_to_recover = []
                with self._lock:
                    clusters_to_recover = list(self.recovery_strategies.items())

                # Execute recovery strategies
                for cluster_id, strategy in clusters_to_recover:
                    success = await self._execute_recovery_strategy(
                        cluster_id, strategy
                    )

                    # Notify callbacks
                    for callback in self.recovery_callbacks:
                        try:
                            callback(cluster_id, strategy, success)
                        except Exception as e:
                            logger.error(f"Error calling recovery callback: {e}")

                    # Remove from recovery list if successful
                    if success:
                        with self._lock:
                            self.recovery_strategies.pop(cluster_id, None)

                await asyncio.sleep(30.0)  # Check for recovery every 30 seconds

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in recovery coordination: {e}")
                await asyncio.sleep(30.0)

    async def _execute_recovery_strategy(
        self, cluster_id: str, strategy: RecoveryStrategy
    ) -> bool:
        """Execute specific recovery strategy for a cluster."""
        try:
            if strategy == RecoveryStrategy.IMMEDIATE_RETRY:
                return await self._immediate_retry_recovery(cluster_id)
            elif strategy == RecoveryStrategy.EXPONENTIAL_BACKOFF:
                return await self._exponential_backoff_recovery(cluster_id)
            elif strategy == RecoveryStrategy.CIRCUIT_BREAKER:
                return await self._circuit_breaker_recovery(cluster_id)
            elif strategy == RecoveryStrategy.GRACEFUL_DEGRADATION:
                return await self._graceful_degradation_recovery(cluster_id)
            elif strategy == RecoveryStrategy.FAILOVER:
                return await self._failover_recovery(cluster_id)
            else:
                logger.warning(f"Unknown recovery strategy: {strategy}")
                return False

        except Exception as e:
            logger.error(
                f"Error executing {strategy.value} recovery for {cluster_id}: {e}"
            )
            return False

    async def _immediate_retry_recovery(self, cluster_id: str) -> bool:
        """Attempt immediate retry recovery."""
        logger.info(f"Attempting immediate retry recovery for {cluster_id}")

        # Simulate immediate retry
        await asyncio.sleep(1.0)

        # Check if cluster is now healthy
        health_metrics = self.health_monitor.cluster_health.get(cluster_id)
        if health_metrics and health_metrics.status == HealthStatus.HEALTHY:
            logger.info(f"Immediate retry recovery successful for {cluster_id}")
            return True

        return False

    async def _exponential_backoff_recovery(self, cluster_id: str) -> bool:
        """Attempt recovery with exponential backoff."""
        logger.info(f"Attempting exponential backoff recovery for {cluster_id}")

        for attempt in range(self.retry_config.max_attempts):
            # Calculate delay with jitter
            delay = min(
                self.retry_config.initial_delay_seconds
                * (self.retry_config.backoff_multiplier**attempt),
                self.retry_config.max_delay_seconds,
            )

            jitter = random.uniform(
                -self.retry_config.jitter_factor, self.retry_config.jitter_factor
            )
            delay *= 1 + jitter

            logger.info(
                f"Recovery attempt {attempt + 1}/{self.retry_config.max_attempts} for {cluster_id}, waiting {delay:.2f}s"
            )
            await asyncio.sleep(delay)

            # Simulate recovery attempt
            # In real implementation, this would attempt to reconnect to the cluster
            if random.random() < 0.3:  # 30% chance of success per attempt
                logger.info(f"Exponential backoff recovery successful for {cluster_id}")
                return True

        logger.warning(
            f"Exponential backoff recovery failed for {cluster_id} after {self.retry_config.max_attempts} attempts"
        )
        return False

    async def _circuit_breaker_recovery(self, cluster_id: str) -> bool:
        """Coordinate circuit breaker recovery."""
        logger.info(f"Initiating circuit breaker recovery for {cluster_id}")

        # Circuit breaker recovery is handled by the circuit breaker itself
        # This just logs and waits for the circuit breaker to recover
        await asyncio.sleep(60.0)  # Wait for circuit breaker timeout

        # Check if recovery occurred
        health_metrics = self.health_monitor.cluster_health.get(cluster_id)
        if health_metrics and health_metrics.status in [
            HealthStatus.HEALTHY,
            HealthStatus.DEGRADED,
        ]:
            logger.info(f"Circuit breaker recovery successful for {cluster_id}")
            return True

        return False

    async def _graceful_degradation_recovery(self, cluster_id: str) -> bool:
        """Implement graceful degradation recovery."""
        logger.info(f"Implementing graceful degradation for {cluster_id}")

        # Graceful degradation: reduce load, disable non-critical features
        # For demo purposes, just wait and simulate recovery
        await asyncio.sleep(30.0)

        # Simulate partial recovery
        if random.random() < 0.7:  # 70% chance of partial recovery
            logger.info(f"Graceful degradation recovery successful for {cluster_id}")
            return True

        return False

    async def _failover_recovery(self, cluster_id: str) -> bool:
        """Implement failover recovery."""
        logger.info(f"Initiating failover recovery for {cluster_id}")

        # Failover: redirect traffic to backup clusters
        # For demo purposes, simulate failover process
        await asyncio.sleep(10.0)

        logger.info(f"Failover recovery completed for {cluster_id}")
        return True

    def add_recovery_callback(
        self, callback: Callable[[str, RecoveryStrategy, bool], None]
    ) -> None:
        """Add a callback for recovery events."""
        self.recovery_callbacks.append(callback)


@dataclass(slots=True)
class EnhancedFederationResilience:
    """
    Main class that coordinates all resilience features.

    Combines:
    - Health monitoring
    - Auto-recovery
    - Circuit breakers
    - Performance tracking
    - Alerting
    """

    cluster_id: str
    health_monitor: FederationHealthMonitor = field(init=False)
    auto_recovery: FederationAutoRecovery = field(init=False)
    circuit_breakers: dict[str, AdaptiveCircuitBreaker] = field(default_factory=dict)

    # Configuration
    health_config: HealthCheckConfiguration = field(
        default_factory=HealthCheckConfiguration
    )
    retry_config: RetryConfiguration = field(default_factory=RetryConfiguration)

    # State
    enabled: bool = False
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """Initialize resilience components."""
        self.health_monitor = FederationHealthMonitor(
            cluster_id=self.cluster_id,
            health_config=self.health_config,
            retry_config=self.retry_config,
        )

        self.auto_recovery = FederationAutoRecovery(
            cluster_id=self.cluster_id,
            health_monitor=self.health_monitor,
            retry_config=self.retry_config,
        )

    async def enable_resilience(self) -> None:
        """Enable all resilience features."""
        if self.enabled:
            logger.warning("Resilience features already enabled")
            return

        logger.info(f"Enabling federation resilience for cluster {self.cluster_id}")

        # Start health monitoring
        await self.health_monitor.start_monitoring()

        # Start auto-recovery
        await self.auto_recovery.start_auto_recovery()

        self.enabled = True
        logger.info("Federation resilience enabled successfully")

    async def disable_resilience(self) -> None:
        """Disable all resilience features."""
        if not self.enabled:
            return

        logger.info("Disabling federation resilience")

        # Stop auto-recovery
        await self.auto_recovery.stop_auto_recovery()

        # Stop health monitoring
        await self.health_monitor.stop_monitoring()

        self.enabled = False
        logger.info("Federation resilience disabled")

    def register_cluster(self, cluster_identity: ClusterIdentity) -> None:
        """Register a cluster for resilience monitoring."""
        with self._lock:
            self.health_monitor.register_cluster(cluster_identity)

            # Create adaptive circuit breaker for this cluster
            self.circuit_breakers[cluster_identity.cluster_id] = AdaptiveCircuitBreaker(
                failure_threshold=5, success_threshold=3, timeout_seconds=60.0
            )

    def unregister_cluster(self, cluster_id: str) -> None:
        """Unregister a cluster from resilience monitoring."""
        with self._lock:
            self.health_monitor.unregister_cluster(cluster_id)
            self.circuit_breakers.pop(cluster_id, None)

    def get_circuit_breaker(self, cluster_id: str) -> AdaptiveCircuitBreaker | None:
        """Get circuit breaker for a specific cluster."""
        return self.circuit_breakers.get(cluster_id)

    def get_resilience_summary(self) -> ResilienceMetrics:
        """Get comprehensive resilience metrics."""
        health_summary = self.health_monitor.get_health_summary()

        circuit_breaker_states = {}
        for cluster_id, cb in self.circuit_breakers.items():
            circuit_breaker_states[cluster_id] = CircuitBreakerState(
                state=cb.state,
                failure_count=cb.failure_count,
                success_count=cb.success_count,
                current_timeout=cb.current_timeout,
            )

        active_recovery_strategies = {}
        with self.auto_recovery._lock:
            active_recovery_strategies = {
                cluster_id: strategy.value
                for cluster_id, strategy in self.auto_recovery.recovery_strategies.items()
            }

        return ResilienceMetrics(
            enabled=self.enabled,
            health_summary=health_summary,
            circuit_breaker_states=circuit_breaker_states,
            active_recovery_strategies=active_recovery_strategies,
            total_registered_clusters=len(self.circuit_breakers),
        )
