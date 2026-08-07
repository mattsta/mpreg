"""
Federation Connection Manager for MPREG.

This module provides connection management and policy enforcement for
MPREG's federation system. It integrates with the server's connection
handling to enforce federation policies and track connection state.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from loguru import logger

from mpreg.datastructures.type_aliases import (
    ClusterId,
    NodeId,
    Timestamp,
    UrlString,
)
from mpreg.fabric.federation_config import (
    ConnectionAuthorizationResult,
    FederationBridgeConfig,
    FederationConfig,
    FederationManager,
)


@dataclass(frozen=True, slots=True)
class FederationConnectionAttempt:
    """Information about a federation connection attempt."""

    remote_cluster_id: ClusterId
    remote_node_id: NodeId
    remote_url: UrlString
    local_cluster_id: ClusterId
    local_node_id: NodeId
    attempt_timestamp: Timestamp
    authorization_result: ConnectionAuthorizationResult
    bridge_config: FederationBridgeConfig | None = None
    error_message: str = ""


@dataclass(frozen=True, slots=True)
class FederationConnectionDecision:
    """Decision result for a federation connection attempt."""

    allowed: bool
    authorization_result: ConnectionAuthorizationResult
    should_log_warning: bool
    should_log_rejection: bool
    error_message: str = ""
    bridge_config: FederationBridgeConfig | None = None

    @property
    def requires_special_handling(self) -> bool:
        """Check if connection requires special handling (warnings, etc)."""
        return (
            self.authorization_result
            == ConnectionAuthorizationResult.AUTHORIZED_WITH_WARNING
        )


class FederationConnectionManager:
    """Manager for federation connections with policy enforcement."""

    def __init__(self, federation_config: FederationConfig) -> None:
        """Initialize federation connection manager."""
        self.federation_manager = FederationManager(config=federation_config)
        self._connection_attempts: list[FederationConnectionAttempt] = []
        self._log = logger

    def evaluate_connection_attempt(
        self,
        remote_cluster_id: ClusterId,
        remote_node_id: NodeId,
        remote_url: UrlString,
        local_cluster_id: ClusterId,
        local_node_id: NodeId,
    ) -> FederationConnectionDecision:
        """Evaluate whether a federation connection should be allowed."""
        config = self.federation_manager.config

        # Get authorization result
        auth_result, bridge_config = self.federation_manager.authorize_connection(
            remote_cluster_id=remote_cluster_id,
            remote_node_id=remote_node_id,
            remote_url=remote_url,
        )

        # Record the attempt
        attempt = FederationConnectionAttempt(
            remote_cluster_id=remote_cluster_id,
            remote_node_id=remote_node_id,
            remote_url=remote_url,
            local_cluster_id=local_cluster_id,
            local_node_id=local_node_id,
            attempt_timestamp=time.time(),
            authorization_result=auth_result,
            bridge_config=bridge_config,
        )
        self._connection_attempts.append(attempt)

        # Create decision based on authorization result
        match auth_result:
            case ConnectionAuthorizationResult.AUTHORIZED:
                return FederationConnectionDecision(
                    allowed=True,
                    authorization_result=auth_result,
                    should_log_warning=False,
                    should_log_rejection=False,
                    bridge_config=bridge_config,
                )

            case ConnectionAuthorizationResult.AUTHORIZED_WITH_WARNING:
                return FederationConnectionDecision(
                    allowed=True,
                    authorization_result=auth_result,
                    should_log_warning=config.log_cross_federation_warnings,
                    should_log_rejection=False,
                    bridge_config=bridge_config,
                )

            case ConnectionAuthorizationResult.REJECTED_CLUSTER_MISMATCH:
                return FederationConnectionDecision(
                    allowed=False,
                    authorization_result=auth_result,
                    should_log_warning=False,
                    should_log_rejection=config.log_connection_rejections,
                    error_message=f"Connection rejected: cluster_id mismatch. Local: {local_cluster_id}, Remote: {remote_cluster_id}. Cross-federation connections not allowed in current federation mode.",
                )

            case ConnectionAuthorizationResult.REJECTED_NOT_ALLOWED:
                return FederationConnectionDecision(
                    allowed=False,
                    authorization_result=auth_result,
                    should_log_warning=False,
                    should_log_rejection=config.log_connection_rejections,
                    error_message=f"Connection rejected: cluster '{remote_cluster_id}' not in allowed foreign clusters list.",
                )

    def handle_connection_status(
        self,
        remote_cluster_id: ClusterId,
        remote_node_id: NodeId,
        remote_url: UrlString,
        local_cluster_id: ClusterId,
        local_node_id: NodeId,
        status_data: dict[str, Any],
    ) -> FederationConnectionDecision:
        """Handle connection metadata from remote federation node."""
        config = self.federation_manager.config

        decision = self.evaluate_connection_attempt(
            remote_cluster_id=remote_cluster_id,
            remote_node_id=remote_node_id,
            remote_url=remote_url,
            local_cluster_id=local_cluster_id,
            local_node_id=local_node_id,
        )

        # Log based on decision
        if (
            decision.authorization_result
            == ConnectionAuthorizationResult.AUTHORIZED_WITH_WARNING
        ):
            if decision.should_log_warning:
                self._log.warning(
                    "[{}] Cross-federation connection from {} (cluster: {}) authorized with warning. "
                    "Federation mode allows cross-federation connections but they may be unstable.",
                    local_node_id,
                    remote_node_id,
                    remote_cluster_id,
                )
            elif config.log_cross_federation_attempts:
                self._log.info(
                    "[{}] Cross-federation connection from {} (cluster: {}) authorized.",
                    local_node_id,
                    remote_node_id,
                    remote_cluster_id,
                )

        if decision.should_log_rejection:
            self._log.warning(
                "[{}] Rejected connection from {} (cluster: {}): {}",
                local_node_id,
                remote_node_id,
                remote_cluster_id,
                decision.error_message,
            )

        if (
            decision.allowed
            and decision.bridge_config
            and config.log_cross_federation_attempts
        ):
            self._log.info(
                "[{}] Authorized cross-federation connection from {} via bridge '{}'",
                local_node_id,
                remote_node_id,
                decision.bridge_config.bridge_name,
            )

        return decision

    def notify_connection_established(
        self,
        remote_cluster_id: ClusterId,
        remote_node_id: NodeId,
        remote_url: UrlString,
    ) -> None:
        """Notify manager that connection was successfully established."""
        # Find the connection state and update it
        if remote_cluster_id in self.federation_manager.active_connections:
            for conn_state in self.federation_manager.active_connections[
                remote_cluster_id
            ]:
                if conn_state.remote_node_id == remote_node_id:
                    conn_state.last_health_check_at = time.time()
                    break

    def notify_connection_closed(
        self, remote_cluster_id: ClusterId, remote_node_id: NodeId
    ) -> None:
        """Notify manager that connection was closed."""
        self.federation_manager.remove_connection(remote_cluster_id, remote_node_id)

    def notify_request_success(
        self, remote_cluster_id: ClusterId, remote_node_id: NodeId
    ) -> None:
        """Notify manager of successful cross-federation request."""
        if remote_cluster_id in self.federation_manager.active_connections:
            for conn_state in self.federation_manager.active_connections[
                remote_cluster_id
            ]:
                if conn_state.remote_node_id == remote_node_id:
                    conn_state.successful_requests += 1
                    break

    def notify_request_failure(
        self,
        remote_cluster_id: ClusterId,
        remote_node_id: NodeId,
        error_message: str = "",
    ) -> None:
        """Notify manager of failed cross-federation request."""
        if remote_cluster_id in self.federation_manager.active_connections:
            for conn_state in self.federation_manager.active_connections[
                remote_cluster_id
            ]:
                if conn_state.remote_node_id == remote_node_id:
                    conn_state.failed_requests += 1
                    conn_state.connection_errors += 1
                    conn_state.last_error_message = error_message
                    conn_state.last_error_at = time.time()
                    break

    def is_function_allowed_cross_federation(self, function_name: str) -> bool:
        """Check if function is allowed for cross-federation execution."""
        return self.federation_manager.config.security_policy.is_function_allowed_cross_federation(
            function_name
        )

    def get_federation_metrics(self) -> dict[str, Any]:
        """Get comprehensive federation metrics."""
        base_metrics = self.federation_manager.get_federation_metrics()

        # Add connection attempt metrics
        total_attempts = len(self._connection_attempts)
        authorized_attempts = sum(
            1
            for attempt in self._connection_attempts
            if attempt.authorization_result
            in (
                ConnectionAuthorizationResult.AUTHORIZED,
                ConnectionAuthorizationResult.AUTHORIZED_WITH_WARNING,
            )
        )
        rejected_attempts = total_attempts - authorized_attempts

        # Recent attempt metrics (last hour)
        current_time = time.time()
        one_hour_ago = current_time - 3600
        recent_attempts = [
            attempt
            for attempt in self._connection_attempts
            if attempt.attempt_timestamp > one_hour_ago
        ]

        federation_metrics = {
            **base_metrics,
            "total_connection_attempts": total_attempts,
            "authorized_connection_attempts": authorized_attempts,
            "rejected_connection_attempts": rejected_attempts,
            "recent_connection_attempts_last_hour": len(recent_attempts),
            "federation_mode": self.federation_manager.config.federation_mode.value,
            "local_cluster_id": self.federation_manager.config.local_cluster_id,
            "allowed_foreign_clusters": list(
                self.federation_manager.config.allowed_foreign_cluster_ids
            ),
            "explicit_bridges_configured": len(
                self.federation_manager.config.explicit_bridges
            ),
        }

        return federation_metrics

    def get_connection_health_summary(self) -> dict[str, Any]:
        """Get summary of connection health across all federations."""
        all_connections = []
        for connections in self.federation_manager.active_connections.values():
            all_connections.extend(connections)

        if not all_connections:
            return {
                "total_connections": 0,
                "healthy_connections": 0,
                "unhealthy_connections": 0,
                "average_success_rate": 1.0,
                "connections_by_cluster": {},
            }

        healthy_count = sum(1 for conn in all_connections if conn.is_healthy)

        success_rates = [
            conn.success_rate
            for conn in all_connections
            if conn.successful_requests + conn.failed_requests > 0
        ]
        avg_success_rate = (
            sum(success_rates) / len(success_rates) if success_rates else 1.0
        )

        connections_by_cluster = {}
        for (
            cluster_id,
            connections,
        ) in self.federation_manager.active_connections.items():
            connections_by_cluster[cluster_id] = {
                "total": len(connections),
                "healthy": sum(1 for conn in connections if conn.is_healthy),
                "average_success_rate": sum(conn.success_rate for conn in connections)
                / len(connections)
                if connections
                else 1.0,
            }

        return {
            "total_connections": len(all_connections),
            "healthy_connections": healthy_count,
            "unhealthy_connections": len(all_connections) - healthy_count,
            "average_success_rate": avg_success_rate,
            "connections_by_cluster": connections_by_cluster,
        }


# Integration helper functions


def create_federation_connection_manager(
    federation_config: FederationConfig,
) -> FederationConnectionManager:
    """Create a federation connection manager with the given configuration."""
    return FederationConnectionManager(federation_config)


def should_allow_cross_federation_connection(
    connection_manager: FederationConnectionManager,
    remote_cluster_id: ClusterId,
    remote_node_id: NodeId,
    remote_url: UrlString,
    local_cluster_id: ClusterId,
    local_node_id: NodeId,
    connection_data: dict[str, Any],
) -> tuple[bool, str]:
    """
    Helper function to determine if cross-federation connections should be allowed.

    Returns:
        tuple[bool, str]: (allowed, error_message_if_rejected)
    """
    decision = connection_manager.handle_connection_status(
        remote_cluster_id=remote_cluster_id,
        remote_node_id=remote_node_id,
        remote_url=remote_url,
        local_cluster_id=local_cluster_id,
        local_node_id=local_node_id,
        status_data=connection_data,
    )

    return decision.allowed, decision.error_message
