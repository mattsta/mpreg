"""
Tests for the new federation configuration system.

This module tests the well-encapsulated federation configuration
and connection management system that addresses the implementation
gaps identified in comprehensive testing.
"""

import pytest

from mpreg.federation.federation_config import (
    ConnectionAuthorizationResult,
    FederationBridgeConfig,
    FederationConfig,
    FederationMode,
    FederationSecurityPolicy,
    create_development_config,
    create_explicit_bridging_config,
    create_permissive_bridging_config,
    create_strict_isolation_config,
)
from mpreg.federation.federation_connection_manager import (
    FederationConnectionManager,
    should_allow_cross_federation_hello,
)


class TestFederationConfig:
    """Test federation configuration data structures."""

    def test_strict_isolation_config(self):
        """Test strict isolation configuration."""
        config = create_strict_isolation_config("test-cluster")

        assert config.federation_mode == FederationMode.STRICT_ISOLATION
        assert config.local_cluster_id == "test-cluster"
        assert len(config.allowed_foreign_cluster_ids) == 0
        assert len(config.explicit_bridges) == 0

        # Should reject all cross-federation connections
        result = config.is_cross_federation_connection_allowed("other-cluster")
        assert result == ConnectionAuthorizationResult.REJECTED_CLUSTER_MISMATCH

        # Should allow same-cluster connections
        result = config.is_cross_federation_connection_allowed("test-cluster")
        assert result == ConnectionAuthorizationResult.AUTHORIZED

    def test_explicit_bridging_config(self):
        """Test explicit bridging configuration."""
        allowed_clusters = {"partner-cluster", "staging-cluster"}
        bridge = FederationBridgeConfig(
            local_cluster_id="main-cluster",
            remote_cluster_id="partner-cluster",
            bridge_name="production-bridge",
        )

        config = create_explicit_bridging_config(
            "main-cluster", allowed_clusters, {bridge}
        )

        assert config.federation_mode == FederationMode.EXPLICIT_BRIDGING
        assert config.local_cluster_id == "main-cluster"
        assert config.allowed_foreign_cluster_ids == frozenset(allowed_clusters)

        # Should allow explicitly configured clusters
        result = config.is_cross_federation_connection_allowed("partner-cluster")
        assert result == ConnectionAuthorizationResult.AUTHORIZED

        result = config.is_cross_federation_connection_allowed("staging-cluster")
        assert result == ConnectionAuthorizationResult.AUTHORIZED

        # Should reject unconfigured clusters
        result = config.is_cross_federation_connection_allowed("unknown-cluster")
        assert result == ConnectionAuthorizationResult.REJECTED_NOT_ALLOWED

    def test_permissive_bridging_config(self):
        """Test permissive bridging configuration."""
        config = create_permissive_bridging_config("test-cluster")

        assert config.federation_mode == FederationMode.PERMISSIVE_BRIDGING

        # Should allow all cross-federation connections with warning
        result = config.is_cross_federation_connection_allowed("any-cluster")
        assert result == ConnectionAuthorizationResult.AUTHORIZED_WITH_WARNING

    def test_federation_bridge_config_validation(self):
        """Test federation bridge configuration validation."""
        # Valid bridge config
        bridge = FederationBridgeConfig(
            local_cluster_id="cluster-a", remote_cluster_id="cluster-b"
        )
        assert bridge.bridge_name == "bridge_cluster-a_to_cluster-b"

        # Should reject self-bridge
        with pytest.raises(ValueError, match="Bridge cannot connect cluster to itself"):
            FederationBridgeConfig(
                local_cluster_id="same-cluster", remote_cluster_id="same-cluster"
            )

    def test_federation_security_policy(self):
        """Test federation security policy functionality."""
        # Default policy allows all functions
        policy = FederationSecurityPolicy()
        assert policy.is_function_allowed_cross_federation("any_function")

        # Policy with allowed functions list
        policy = FederationSecurityPolicy(
            allowed_functions_cross_federation=frozenset(
                ["safe_function", "public_api"]
            )
        )
        assert policy.is_function_allowed_cross_federation("safe_function")
        assert not policy.is_function_allowed_cross_federation("internal_function")

        # Policy with blocked functions list
        policy = FederationSecurityPolicy(
            blocked_functions_cross_federation=frozenset(["dangerous_function"])
        )
        assert not policy.is_function_allowed_cross_federation("dangerous_function")
        assert policy.is_function_allowed_cross_federation("safe_function")


class TestFederationConnectionManager:
    """Test federation connection management."""

    def test_strict_isolation_connection_manager(self):
        """Test connection manager with strict isolation."""
        config = create_strict_isolation_config("main-cluster")
        manager = FederationConnectionManager(config)

        # Should allow same-cluster connections
        decision = manager.evaluate_connection_attempt(
            remote_cluster_id="main-cluster",
            remote_node_id="node-2",
            remote_url="ws://node2:8080",
            local_cluster_id="main-cluster",
            local_node_id="node-1",
        )
        assert decision.allowed
        assert not decision.should_log_warning
        assert not decision.should_log_rejection

        # Should reject cross-federation connections
        decision = manager.evaluate_connection_attempt(
            remote_cluster_id="other-cluster",
            remote_node_id="remote-node",
            remote_url="ws://remote:8080",
            local_cluster_id="main-cluster",
            local_node_id="node-1",
        )
        assert not decision.allowed
        assert not decision.should_log_warning
        assert decision.should_log_rejection
        assert "cluster_id mismatch" in decision.error_message

    def test_explicit_bridging_connection_manager(self):
        """Test connection manager with explicit bridging."""
        bridge = FederationBridgeConfig(
            local_cluster_id="main-cluster", remote_cluster_id="partner-cluster"
        )
        config = create_explicit_bridging_config(
            "main-cluster", {"partner-cluster"}, {bridge}
        )
        manager = FederationConnectionManager(config)

        # Should allow bridged connections
        decision = manager.evaluate_connection_attempt(
            remote_cluster_id="partner-cluster",
            remote_node_id="partner-node",
            remote_url="ws://partner:8080",
            local_cluster_id="main-cluster",
            local_node_id="main-node",
        )
        assert decision.allowed
        assert decision.bridge_config is not None
        assert decision.bridge_config.remote_cluster_id == "partner-cluster"

        # Should reject non-bridged connections
        decision = manager.evaluate_connection_attempt(
            remote_cluster_id="unknown-cluster",
            remote_node_id="unknown-node",
            remote_url="ws://unknown:8080",
            local_cluster_id="main-cluster",
            local_node_id="main-node",
        )
        assert not decision.allowed
        assert "not in allowed foreign clusters" in decision.error_message

    def test_permissive_bridging_connection_manager(self):
        """Test connection manager with permissive bridging."""
        config = create_permissive_bridging_config("main-cluster")
        manager = FederationConnectionManager(config)

        # Should allow all connections with warnings
        decision = manager.evaluate_connection_attempt(
            remote_cluster_id="any-cluster",
            remote_node_id="any-node",
            remote_url="ws://any:8080",
            local_cluster_id="main-cluster",
            local_node_id="main-node",
        )
        assert decision.allowed
        assert decision.should_log_warning
        assert not decision.should_log_rejection

    def test_connection_state_tracking(self):
        """Test connection state tracking and metrics."""
        config = create_permissive_bridging_config("main-cluster")
        manager = FederationConnectionManager(config)

        # Establish connection
        decision = manager.evaluate_connection_attempt(
            remote_cluster_id="partner-cluster",
            remote_node_id="partner-node",
            remote_url="ws://partner:8080",
            local_cluster_id="main-cluster",
            local_node_id="main-node",
        )
        assert decision.allowed

        # Notify connection established
        manager.notify_connection_established(
            "partner-cluster", "partner-node", "ws://partner:8080"
        )

        # Simulate successful requests
        manager.notify_request_success("partner-cluster", "partner-node")
        manager.notify_request_success("partner-cluster", "partner-node")

        # Simulate failed request
        manager.notify_request_failure("partner-cluster", "partner-node", "timeout")

        # Check metrics
        metrics = manager.get_federation_metrics()
        assert metrics["total_active_connections"] == 1
        assert metrics["active_foreign_clusters"] == 1
        assert metrics["total_cross_federation_requests"] == 3
        assert metrics["successful_cross_federation_requests"] == 2

        health_summary = manager.get_connection_health_summary()
        assert health_summary["total_connections"] == 1
        assert "partner-cluster" in health_summary["connections_by_cluster"]

    def test_function_authorization_cross_federation(self):
        """Test function authorization for cross-federation calls."""
        security_policy = FederationSecurityPolicy(
            allowed_functions_cross_federation=frozenset(["public_api", "health_check"])
        )
        config = FederationConfig(
            federation_mode=FederationMode.EXPLICIT_BRIDGING,
            local_cluster_id="main-cluster",
            allowed_foreign_cluster_ids=frozenset(["partner-cluster"]),
            security_policy=security_policy,
        )
        manager = FederationConnectionManager(config)

        # Should allow whitelisted functions
        assert manager.is_function_allowed_cross_federation("public_api")
        assert manager.is_function_allowed_cross_federation("health_check")

        # Should reject non-whitelisted functions
        assert not manager.is_function_allowed_cross_federation("internal_function")
        assert not manager.is_function_allowed_cross_federation("admin_api")

    def test_development_config(self):
        """Test development-friendly configuration."""
        config = create_development_config("dev-cluster")

        assert config.federation_mode == FederationMode.PERMISSIVE_BRIDGING
        assert config.cross_federation_connection_timeout_seconds == 10.0
        assert config.connection_retry_attempts == 1
        assert not config.security_policy.require_mutual_authentication
        assert not config.security_policy.require_encrypted_connections
        assert config.security_policy.max_cross_federation_requests_per_minute == 1000


class TestFederationIntegrationHelpers:
    """Test federation integration helper functions."""

    def test_should_allow_cross_federation_hello(self):
        """Test cross-federation HELLO authorization helper."""
        config = create_explicit_bridging_config("main-cluster", {"partner-cluster"})
        manager = FederationConnectionManager(config)

        # Should allow authorized cluster
        allowed, error = should_allow_cross_federation_hello(
            manager,
            remote_cluster_id="partner-cluster",
            remote_node_id="partner-node",
            remote_url="ws://partner:8080",
            local_cluster_id="main-cluster",
            local_node_id="main-node",
            hello_data={"functions": ["test_func"]},
        )
        assert allowed
        assert not error

        # Should reject unauthorized cluster
        allowed, error = should_allow_cross_federation_hello(
            manager,
            remote_cluster_id="unknown-cluster",
            remote_node_id="unknown-node",
            remote_url="ws://unknown:8080",
            local_cluster_id="main-cluster",
            local_node_id="main-node",
            hello_data={"functions": ["test_func"]},
        )
        assert not allowed
        assert "not in allowed foreign clusters" in error


class TestFederationConfigIntegration:
    """Test integration with MPREG settings."""

    def test_mpreg_settings_federation_integration(self):
        """Test that MPREGSettings properly integrates federation config."""
        from mpreg.core.config import MPREGSettings

        # Default settings should have strict isolation
        settings = MPREGSettings()
        assert settings.federation_config is not None
        assert (
            settings.federation_config.federation_mode
            == FederationMode.STRICT_ISOLATION
        )
        assert settings.federation_config.local_cluster_id == settings.cluster_id

        # Custom federation config should be used
        custom_config = create_permissive_bridging_config("custom-cluster")
        settings = MPREGSettings(
            cluster_id="custom-cluster", federation_config=custom_config
        )
        assert settings.federation_config is not None
        assert (
            settings.federation_config.federation_mode
            == FederationMode.PERMISSIVE_BRIDGING
        )
