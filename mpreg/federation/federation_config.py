"""
Federation Configuration System for MPREG.

This module provides well-encapsulated data structures and configuration
management for MPREG's federation system, addressing the implementation
gaps identified in comprehensive testing.

Key capabilities:
- Type-safe federation policy configuration
- Cross-federation connection management
- Cluster boundary enforcement
- Federation bridge configuration
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from mpreg.datastructures.type_aliases import (
    ClusterId,
    DurationSeconds,
    FeatureFlag,
    NodeId,
    Timestamp,
    UrlString,
)


class FederationMode(Enum):
    """Federation policy modes for cluster boundary management."""

    STRICT_ISOLATION = "strict_isolation"
    """Enforce strict cluster_id boundaries - reject cross-federation connections."""

    EXPLICIT_BRIDGING = "explicit_bridging"
    """Allow explicitly configured cross-federation connections."""

    PERMISSIVE_BRIDGING = "permissive_bridging"
    """Allow cross-federation connections with warnings."""


class ConnectionAuthorizationResult(Enum):
    """Result of federation connection authorization check."""

    AUTHORIZED = "authorized"
    """Connection is authorized to proceed."""

    REJECTED_CLUSTER_MISMATCH = "rejected_cluster_mismatch"
    """Connection rejected due to cluster_id mismatch."""

    REJECTED_NOT_ALLOWED = "rejected_not_allowed"
    """Connection rejected - not in allowed foreign clusters."""

    AUTHORIZED_WITH_WARNING = "authorized_with_warning"
    """Connection authorized but will generate warnings."""


@dataclass(frozen=True, slots=True)
class FederationBridgeConfig:
    """Configuration for explicit federation bridge connections."""

    local_cluster_id: ClusterId
    remote_cluster_id: ClusterId
    allowed_remote_nodes: frozenset[NodeId] = frozenset()
    connection_timeout_seconds: DurationSeconds = 30.0
    retry_attempts: int = 3
    health_check_interval_seconds: DurationSeconds = 60.0
    bridge_name: str = ""

    def __post_init__(self) -> None:
        """Validate bridge configuration."""
        if self.local_cluster_id == self.remote_cluster_id:
            raise ValueError("Bridge cannot connect cluster to itself")

        if not self.bridge_name:
            # Auto-generate bridge name if not provided
            object.__setattr__(
                self,
                "bridge_name",
                f"bridge_{self.local_cluster_id}_to_{self.remote_cluster_id}",
            )


@dataclass(frozen=True, slots=True)
class FederationSecurityPolicy:
    """Security policies for federation connections."""

    require_mutual_authentication: FeatureFlag = True
    allowed_functions_cross_federation: frozenset[str] = frozenset()
    blocked_functions_cross_federation: frozenset[str] = frozenset()
    max_cross_federation_requests_per_minute: int = 100
    require_encrypted_connections: FeatureFlag = True
    connection_rate_limit_per_cluster: int = 10

    def is_function_allowed_cross_federation(self, function_name: str) -> bool:
        """Check if function is allowed for cross-federation execution."""
        # If blocked list is specified and function is in it, deny
        if (
            self.blocked_functions_cross_federation
            and function_name in self.blocked_functions_cross_federation
        ):
            return False

        # If allowed list is specified, only allow functions in it
        if self.allowed_functions_cross_federation:
            return function_name in self.allowed_functions_cross_federation

        # If neither list is specified, allow all functions
        return True


@dataclass(frozen=True, slots=True)
class FederationConfig:
    """Comprehensive federation configuration for MPREG cluster nodes."""

    # Core federation policy
    federation_mode: FederationMode = FederationMode.STRICT_ISOLATION
    local_cluster_id: ClusterId = "default-cluster"

    # Cross-federation permissions
    allowed_foreign_cluster_ids: frozenset[ClusterId] = frozenset()
    explicit_bridges: frozenset[FederationBridgeConfig] = frozenset()

    # Connection management
    cross_federation_connection_timeout_seconds: DurationSeconds = 30.0
    connection_retry_attempts: int = 3
    connection_health_check_interval_seconds: DurationSeconds = 120.0

    # Security and access control
    security_policy: FederationSecurityPolicy = field(
        default_factory=FederationSecurityPolicy
    )

    # Monitoring and observability
    log_cross_federation_attempts: FeatureFlag = True
    log_connection_rejections: FeatureFlag = True
    emit_federation_metrics: FeatureFlag = True

    def is_cross_federation_connection_allowed(
        self, remote_cluster_id: ClusterId, remote_node_id: NodeId | None = None
    ) -> ConnectionAuthorizationResult:
        """Check if a cross-federation connection should be allowed."""

        # Same cluster is always allowed
        if remote_cluster_id == self.local_cluster_id:
            return ConnectionAuthorizationResult.AUTHORIZED

        match self.federation_mode:
            case FederationMode.STRICT_ISOLATION:
                return ConnectionAuthorizationResult.REJECTED_CLUSTER_MISMATCH

            case FederationMode.EXPLICIT_BRIDGING:
                # Check if there's an explicit bridge configuration
                for bridge in self.explicit_bridges:
                    if bridge.remote_cluster_id == remote_cluster_id:
                        # If specific nodes are configured, check node authorization
                        if bridge.allowed_remote_nodes and remote_node_id:
                            if remote_node_id in bridge.allowed_remote_nodes:
                                return ConnectionAuthorizationResult.AUTHORIZED
                            else:
                                return (
                                    ConnectionAuthorizationResult.REJECTED_NOT_ALLOWED
                                )
                        else:
                            # No specific node restrictions
                            return ConnectionAuthorizationResult.AUTHORIZED

                # Also check allowed foreign clusters list
                if remote_cluster_id in self.allowed_foreign_cluster_ids:
                    return ConnectionAuthorizationResult.AUTHORIZED

                return ConnectionAuthorizationResult.REJECTED_NOT_ALLOWED

            case FederationMode.PERMISSIVE_BRIDGING:
                # Allow all cross-federation connections but with warnings
                return ConnectionAuthorizationResult.AUTHORIZED_WITH_WARNING

    def get_bridge_config_for_cluster(
        self, remote_cluster_id: ClusterId
    ) -> FederationBridgeConfig | None:
        """Get bridge configuration for a specific remote cluster."""
        for bridge in self.explicit_bridges:
            if bridge.remote_cluster_id == remote_cluster_id:
                return bridge
        return None

    def is_federation_bridge_configured(self, remote_cluster_id: ClusterId) -> bool:
        """Check if explicit bridge is configured for remote cluster."""
        return self.get_bridge_config_for_cluster(remote_cluster_id) is not None


@dataclass(slots=True)
class FederationConnectionState:
    """Runtime state tracking for federation connections."""

    remote_cluster_id: ClusterId
    remote_node_id: NodeId
    remote_url: UrlString
    connection_established_at: Timestamp
    last_health_check_at: Timestamp
    authorization_result: ConnectionAuthorizationResult
    bridge_config: FederationBridgeConfig | None = None

    # Connection health metrics
    successful_requests: int = 0
    failed_requests: int = 0
    connection_errors: int = 0
    last_error_message: str = ""
    last_error_at: Timestamp = 0.0

    @property
    def is_healthy(self) -> bool:
        """Check if connection is considered healthy."""
        # Simple health check - no recent errors
        import time

        current_time = time.time()

        # Consider unhealthy if last error was within 60 seconds
        if self.last_error_at > 0 and (current_time - self.last_error_at) < 60.0:
            return False

        # Consider unhealthy if error rate is too high
        total_requests = self.successful_requests + self.failed_requests
        if total_requests > 10:
            error_rate = self.failed_requests / total_requests
            if error_rate > 0.5:  # More than 50% error rate
                return False

        return True

    @property
    def success_rate(self) -> float:
        """Calculate success rate for this connection."""
        total_requests = self.successful_requests + self.failed_requests
        if total_requests == 0:
            return 1.0
        return self.successful_requests / total_requests


@dataclass(slots=True)
class FederationManager:
    """Manager for federation configuration and connection state."""

    config: FederationConfig
    active_connections: dict[ClusterId, list[FederationConnectionState]] = field(
        default_factory=dict
    )
    connection_history: list[FederationConnectionState] = field(default_factory=list)

    def authorize_connection(
        self,
        remote_cluster_id: ClusterId,
        remote_node_id: NodeId,
        remote_url: UrlString,
    ) -> tuple[ConnectionAuthorizationResult, FederationBridgeConfig | None]:
        """Authorize and potentially track a federation connection."""

        # Check authorization
        auth_result = self.config.is_cross_federation_connection_allowed(
            remote_cluster_id, remote_node_id
        )

        # Get bridge config if applicable
        bridge_config = self.config.get_bridge_config_for_cluster(remote_cluster_id)

        # If authorized, we could track the connection state
        if auth_result in (
            ConnectionAuthorizationResult.AUTHORIZED,
            ConnectionAuthorizationResult.AUTHORIZED_WITH_WARNING,
        ):
            connection_state = FederationConnectionState(
                remote_cluster_id=remote_cluster_id,
                remote_node_id=remote_node_id,
                remote_url=remote_url,
                connection_established_at=self._current_timestamp(),
                last_health_check_at=self._current_timestamp(),
                authorization_result=auth_result,
                bridge_config=bridge_config,
            )

            # Track active connection
            if remote_cluster_id not in self.active_connections:
                self.active_connections[remote_cluster_id] = []
            self.active_connections[remote_cluster_id].append(connection_state)

            # Add to history
            self.connection_history.append(connection_state)

        return auth_result, bridge_config

    def remove_connection(
        self, remote_cluster_id: ClusterId, remote_node_id: NodeId
    ) -> bool:
        """Remove tracking for a federation connection."""
        if remote_cluster_id not in self.active_connections:
            return False

        connections = self.active_connections[remote_cluster_id]
        for i, conn in enumerate(connections):
            if conn.remote_node_id == remote_node_id:
                connections.pop(i)
                return True

        return False

    def get_federation_metrics(self) -> dict[str, int | float]:
        """Get federation connection metrics."""
        total_connections = sum(
            len(conns) for conns in self.active_connections.values()
        )
        healthy_connections = sum(
            1
            for conns in self.active_connections.values()
            for conn in conns
            if conn.is_healthy
        )

        total_requests = sum(
            conn.successful_requests + conn.failed_requests
            for conns in self.active_connections.values()
            for conn in conns
        )

        successful_requests = sum(
            conn.successful_requests
            for conns in self.active_connections.values()
            for conn in conns
        )

        return {
            "total_active_connections": total_connections,
            "healthy_connections": healthy_connections,
            "unhealthy_connections": total_connections - healthy_connections,
            "total_historical_connections": len(self.connection_history),
            "total_cross_federation_requests": total_requests,
            "successful_cross_federation_requests": successful_requests,
            "cross_federation_success_rate": (
                successful_requests / total_requests if total_requests > 0 else 1.0
            ),
            "active_foreign_clusters": len(self.active_connections),
        }

    def _current_timestamp(self) -> Timestamp:
        """Get current timestamp."""
        import time

        return time.time()


# Factory functions for common federation configurations


def create_strict_isolation_config(cluster_id: ClusterId) -> FederationConfig:
    """Create configuration for strict federation isolation."""
    return FederationConfig(
        federation_mode=FederationMode.STRICT_ISOLATION,
        local_cluster_id=cluster_id,
        log_cross_federation_attempts=True,
        log_connection_rejections=True,
    )


def create_explicit_bridging_config(
    cluster_id: ClusterId,
    allowed_clusters: set[ClusterId],
    bridges: set[FederationBridgeConfig] | None = None,
) -> FederationConfig:
    """Create configuration for explicit federation bridging."""
    return FederationConfig(
        federation_mode=FederationMode.EXPLICIT_BRIDGING,
        local_cluster_id=cluster_id,
        allowed_foreign_cluster_ids=frozenset(allowed_clusters),
        explicit_bridges=frozenset(bridges or set()),
        log_cross_federation_attempts=True,
        emit_federation_metrics=True,
    )


def create_permissive_bridging_config(cluster_id: ClusterId) -> FederationConfig:
    """Create configuration for permissive federation bridging (with warnings)."""
    return FederationConfig(
        federation_mode=FederationMode.PERMISSIVE_BRIDGING,
        local_cluster_id=cluster_id,
        log_cross_federation_attempts=True,
        emit_federation_metrics=True,
        security_policy=FederationSecurityPolicy(
            require_mutual_authentication=False,  # More permissive
            max_cross_federation_requests_per_minute=200,
        ),
    )


def create_development_config(cluster_id: ClusterId) -> FederationConfig:
    """Create development-friendly federation configuration."""
    return FederationConfig(
        federation_mode=FederationMode.PERMISSIVE_BRIDGING,
        local_cluster_id=cluster_id,
        cross_federation_connection_timeout_seconds=10.0,  # Faster timeouts
        connection_retry_attempts=1,  # Fewer retries
        log_cross_federation_attempts=True,
        log_connection_rejections=True,
        security_policy=FederationSecurityPolicy(
            require_mutual_authentication=False,
            require_encrypted_connections=False,  # Development mode
            max_cross_federation_requests_per_minute=1000,  # High limit for testing
        ),
    )
