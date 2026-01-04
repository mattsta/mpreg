"""
Tests for Federation Alerting and Notification System.

This module provides comprehensive tests for the federation alerting system,
including notification backends, routing rules, escalation policies, and
alert aggregation functionality.
"""

import time
from unittest.mock import AsyncMock, patch

import pytest

from mpreg.fabric.federation_alerting import (
    AlertRoutingRule,
    AlertSeverity,
    ConsoleNotificationBackend,
    EscalationLevel,
    EscalationPolicy,
    FederationAlertingService,
    NotificationBackend,
    NotificationChannel,
    NotificationTemplate,
    SlackNotificationBackend,
    WebhookNotificationBackend,
    create_basic_escalation_policy,
    create_cluster_routing_rule,
    create_console_channel,
    create_severity_routing_rule,
    create_slack_channel,
    create_webhook_channel,
)
from mpreg.fabric.performance_metrics import PerformanceAlert


class TestNotificationChannels:
    """Test notification channel configurations."""

    def test_console_channel_creation(self):
        """Test creating console notification channels."""
        channel = create_console_channel("dev-console", AlertSeverity.INFO)

        assert channel.channel_id == "dev-console"
        assert channel.backend == NotificationBackend.CONSOLE
        assert channel.min_severity == AlertSeverity.INFO
        assert channel.enabled is True
        assert channel.template_name == "default_console"

    def test_slack_channel_creation(self):
        """Test creating Slack notification channels."""
        webhook_url = "https://hooks.slack.com/services/test"
        channel = create_slack_channel(
            "prod-slack", webhook_url, AlertSeverity.CRITICAL
        )

        assert channel.channel_id == "prod-slack"
        assert channel.backend == NotificationBackend.SLACK
        assert channel.config["webhook_url"] == webhook_url
        assert channel.min_severity == AlertSeverity.CRITICAL
        assert channel.template_name == "default_slack"

    def test_webhook_channel_creation(self):
        """Test creating webhook notification channels."""
        webhook_url = "https://api.example.com/alerts"
        headers = {"Authorization": "Bearer token123"}

        channel = create_webhook_channel(
            "api-webhook", webhook_url, headers, AlertSeverity.CRITICAL
        )

        assert channel.channel_id == "api-webhook"
        assert channel.backend == NotificationBackend.WEBHOOK
        assert channel.config["webhook_url"] == webhook_url
        assert channel.config["headers"] == headers
        assert channel.min_severity == AlertSeverity.CRITICAL


class TestEscalationPolicies:
    """Test escalation policy configurations."""

    def test_basic_escalation_policy_creation(self):
        """Test creating basic escalation policies."""
        immediate_channels = ["console", "slack-dev"]
        escalated_channels = ["slack-oncall", "pagerduty"]

        policy = create_basic_escalation_policy(
            "basic-policy", immediate_channels, escalated_channels
        )

        assert policy.policy_id == "basic-policy"
        assert policy.name == "Basic Escalation - basic-policy"
        assert policy.escalation_levels[EscalationLevel.IMMEDIATE] == immediate_channels
        assert policy.escalation_levels[EscalationLevel.FIRST] == escalated_channels
        assert policy.escalation_levels[EscalationLevel.FINAL] == escalated_channels
        assert policy.default_channels == immediate_channels
        assert policy.enabled is True


class TestRoutingRules:
    """Test alert routing rule configurations."""

    def test_severity_routing_rule_creation(self):
        """Test creating severity-based routing rules."""
        rule = create_severity_routing_rule(
            "critical-alerts",
            ["critical", "error"],
            ["slack-oncall", "pagerduty"],
            priority=10,
        )

        assert rule.rule_id == "critical-alerts"
        assert rule.name == "Severity Routing - critical-alerts"
        assert rule.conditions["severity"] == ["critical", "error"]
        assert rule.target_channels == ["slack-oncall", "pagerduty"]
        assert rule.priority == 10
        assert rule.enabled is True

    def test_cluster_routing_rule_creation(self):
        """Test creating cluster-based routing rules."""
        rule = create_cluster_routing_rule(
            "production-clusters",
            "prod-.*",
            ["slack-prod", "webhook-monitor"],
            priority=20,
        )

        assert rule.rule_id == "production-clusters"
        assert rule.name == "Cluster Routing - production-clusters"
        assert rule.conditions["cluster_pattern"] == "prod-.*"
        assert rule.target_channels == ["slack-prod", "webhook-monitor"]
        assert rule.priority == 20


class TestNotificationBackends:
    """Test notification backend implementations."""

    @pytest.mark.asyncio
    async def test_console_backend_notification(self):
        """Test console backend notification delivery."""
        backend = ConsoleNotificationBackend()

        alert = PerformanceAlert(
            alert_id="test-alert",
            severity=AlertSeverity.WARNING,
            metric_name="latency",
            current_value=150.0,
            threshold_value=100.0,
            cluster_id="test-cluster",
            node_id=None,
            timestamp=time.time(),
            message="High latency detected",
        )

        channel = create_console_channel("test-console")

        # Capture print output
        with patch("builtins.print") as mock_print:
            delivery = await backend.send_notification(alert, channel)

        assert delivery.success is True
        assert delivery.backend == NotificationBackend.CONSOLE
        assert delivery.channel_id == "test-console"
        assert delivery.alert_id == "test-alert"
        assert delivery.response_code == 200

        # Check that print was called
        assert mock_print.called

    @pytest.mark.asyncio
    async def test_webhook_backend_notification(self):
        """Test webhook backend notification delivery."""
        backend = WebhookNotificationBackend()

        alert = PerformanceAlert(
            alert_id="test-alert",
            severity=AlertSeverity.CRITICAL,
            metric_name="error_rate",
            current_value=5.5,
            threshold_value=5.0,
            cluster_id="test-cluster",
            node_id=None,
            timestamp=time.time(),
            message="High error rate",
        )

        channel = create_webhook_channel(
            "test-webhook", "https://api.example.com/alerts"
        )

        # Mock HTTP response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(return_value="OK")

        with patch("aiohttp.ClientSession.post") as mock_post:
            mock_post.return_value.__aenter__.return_value = mock_response

            delivery = await backend.send_notification(alert, channel)

        assert delivery.success is True
        assert delivery.backend == NotificationBackend.WEBHOOK
        assert delivery.response_code == 200
        assert delivery.response_message
        assert "OK" in delivery.response_message

    @pytest.mark.asyncio
    async def test_slack_backend_notification(self):
        """Test Slack backend notification delivery."""
        backend = SlackNotificationBackend()

        alert = PerformanceAlert(
            alert_id="test-alert",
            severity=AlertSeverity.CRITICAL,
            metric_name="health_score",
            current_value=30.0,
            threshold_value=40.0,
            cluster_id="prod-cluster",
            node_id=None,
            timestamp=time.time(),
            message="Critical cluster health",
        )

        channel = create_slack_channel(
            "prod-slack", "https://hooks.slack.com/services/test"
        )

        # Mock HTTP response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(return_value="ok")

        with patch("aiohttp.ClientSession.post") as mock_post:
            mock_post.return_value.__aenter__.return_value = mock_response

            delivery = await backend.send_notification(alert, channel)

        assert delivery.success is True
        assert delivery.backend == NotificationBackend.SLACK
        assert delivery.response_code == 200

    @pytest.mark.asyncio
    async def test_backend_health_checks(self):
        """Test notification backend health checks."""
        console_backend = ConsoleNotificationBackend()
        webhook_backend = WebhookNotificationBackend()
        slack_backend = SlackNotificationBackend()

        assert await console_backend.health_check() is True
        assert await webhook_backend.health_check() is True
        assert await slack_backend.health_check() is True


class TestFederationAlertingService:
    """Test the main federation alerting service."""

    def test_service_initialization(self):
        """Test alerting service initialization."""
        service = FederationAlertingService()

        assert service.enabled is True
        assert service.aggregation_window_seconds == 300.0
        assert len(service.backends) >= 3  # Console, Webhook, Slack
        assert len(service.templates) >= 2  # Default console and slack templates
        assert len(service.channels) == 0
        assert len(service.routing_rules) == 0
        assert len(service.escalation_policies) == 0

    def test_add_notification_channels(self):
        """Test adding notification channels."""
        service = FederationAlertingService()

        console_channel = create_console_channel("dev-console")
        slack_channel = create_slack_channel(
            "prod-slack", "https://hooks.slack.com/test"
        )

        service.add_channel(console_channel)
        service.add_channel(slack_channel)

        assert len(service.channels) == 2
        assert "dev-console" in service.channels
        assert "prod-slack" in service.channels
        assert service.channels["dev-console"].backend == NotificationBackend.CONSOLE
        assert service.channels["prod-slack"].backend == NotificationBackend.SLACK

    def test_add_routing_rules(self):
        """Test adding and prioritizing routing rules."""
        service = FederationAlertingService()

        # Add rules with different priorities
        high_priority_rule = create_severity_routing_rule(
            "critical-rule", ["critical"], ["oncall"], priority=10
        )
        low_priority_rule = create_severity_routing_rule(
            "warning-rule", ["warning"], ["dev"], priority=100
        )
        medium_priority_rule = create_cluster_routing_rule(
            "prod-rule", "prod-.*", ["slack"], priority=50
        )

        service.add_routing_rule(low_priority_rule)
        service.add_routing_rule(high_priority_rule)
        service.add_routing_rule(medium_priority_rule)

        assert len(service.routing_rules) == 3

        # Check that rules are sorted by priority
        assert service.routing_rules[0].rule_id == "critical-rule"  # priority 10
        assert service.routing_rules[1].rule_id == "prod-rule"  # priority 50
        assert service.routing_rules[2].rule_id == "warning-rule"  # priority 100

    def test_add_escalation_policies(self):
        """Test adding escalation policies."""
        service = FederationAlertingService()

        policy = create_basic_escalation_policy(
            "prod-policy", ["slack-dev"], ["oncall", "manager"]
        )

        service.add_escalation_policy(policy)

        assert len(service.escalation_policies) == 1
        assert "prod-policy" in service.escalation_policies
        assert service.escalation_policies["prod-policy"].enabled is True

    @pytest.mark.asyncio
    async def test_alert_processing_basic(self):
        """Test basic alert processing and notification."""
        service = FederationAlertingService()

        # Set up channels and routing
        console_channel = create_console_channel("test-console")
        service.add_channel(console_channel)

        routing_rule = create_severity_routing_rule(
            "all-alerts", ["warning", "error", "critical"], ["test-console"]
        )
        service.add_routing_rule(routing_rule)

        # Create test alert
        alert = PerformanceAlert(
            alert_id="test-alert-001",
            severity=AlertSeverity.WARNING,
            metric_name="latency",
            current_value=120.0,
            threshold_value=100.0,
            cluster_id="test-cluster",
            node_id=None,
            timestamp=time.time(),
            message="Latency threshold exceeded",
        )

        # Process alert
        with patch("builtins.print"):  # Suppress console output
            await service.process_alert(alert)

        # Check that alert was stored
        assert alert.alert_id in service.active_alerts
        assert service.stats.total_sent >= 1
        assert len(service.delivery_history) >= 1

    @pytest.mark.asyncio
    async def test_alert_routing_rule_matching(self):
        """Test alert routing rule matching logic."""
        service = FederationAlertingService()

        # Set up multiple channels
        dev_channel = create_console_channel("dev", AlertSeverity.INFO)
        prod_channel = create_console_channel("prod", AlertSeverity.WARNING)
        critical_channel = create_console_channel("critical", AlertSeverity.CRITICAL)

        service.add_channel(dev_channel)
        service.add_channel(prod_channel)
        service.add_channel(critical_channel)

        # Set up routing rules
        dev_rule = create_cluster_routing_rule(
            "dev-rule", "dev-.*", ["dev"], priority=10
        )
        prod_rule = create_cluster_routing_rule(
            "prod-rule", "prod-.*", ["prod"], priority=20
        )
        critical_rule = create_severity_routing_rule(
            "critical-rule", ["critical"], ["critical"], priority=5
        )

        service.add_routing_rule(dev_rule)
        service.add_routing_rule(prod_rule)
        service.add_routing_rule(critical_rule)

        # Test dev cluster alert
        dev_alert = PerformanceAlert(
            alert_id="dev-alert",
            severity=AlertSeverity.WARNING,
            metric_name="cpu",
            current_value=75.0,
            threshold_value=70.0,
            cluster_id="dev-cluster-01",
            node_id=None,
            timestamp=time.time(),
            message="High CPU usage",
        )

        with patch("builtins.print"):
            await service.process_alert(dev_alert)

        # Test production cluster critical alert
        prod_critical_alert = PerformanceAlert(
            alert_id="prod-critical",
            severity=AlertSeverity.CRITICAL,
            metric_name="health_score",
            current_value=20.0,
            threshold_value=40.0,
            cluster_id="prod-cluster-01",
            node_id=None,
            timestamp=time.time(),
            message="Critical cluster health",
        )

        with patch("builtins.print"):
            await service.process_alert(prod_critical_alert)

        # Check that alerts were routed correctly
        assert len(service.active_alerts) == 2
        assert service.stats.total_sent >= 2  # At least 2 notifications sent

    @pytest.mark.asyncio
    async def test_alert_aggregation(self):
        """Test alert aggregation functionality."""
        service = FederationAlertingService()
        service.aggregation_window_seconds = 60.0  # 1 minute window

        # Set up basic routing
        channel = create_console_channel("test")
        service.add_channel(channel)

        rule = create_severity_routing_rule("all", ["warning"], ["test"])
        service.add_routing_rule(rule)

        # Create similar alerts that should be aggregated
        base_time = time.time()
        alerts = [
            PerformanceAlert(
                alert_id=f"alert-{i}",
                severity=AlertSeverity.WARNING,
                metric_name="latency",
                current_value=110.0 + i,
                threshold_value=100.0,
                cluster_id=f"cluster-{i}",
                node_id=None,
                timestamp=base_time + i,
                message=f"High latency in cluster-{i}",
            )
            for i in range(3)
        ]

        with patch("builtins.print"):
            # Process alerts in sequence
            for alert in alerts:
                await service.process_alert(alert)

        # Check if aggregation occurred
        assert len(service.aggregated_alerts) >= 1

        # Find the aggregated alert
        aggregated = next(iter(service.aggregated_alerts.values()))
        assert aggregated.alert_count >= 2
        assert aggregated.common_metric == "latency"
        assert len(aggregated.affected_clusters) >= 2

    @pytest.mark.asyncio
    async def test_rate_limiting(self):
        """Test notification rate limiting."""
        service = FederationAlertingService()

        # Create channel with very low rate limit
        channel = NotificationChannel(
            channel_id="rate-limited",
            backend=NotificationBackend.CONSOLE,
            config={},
            rate_limit_per_minute=2,  # Only 2 notifications per minute
        )
        service.add_channel(channel)

        rule = create_severity_routing_rule("all", ["warning"], ["rate-limited"])
        service.add_routing_rule(rule)

        # Try to send many alerts quickly
        alerts = [
            PerformanceAlert(
                alert_id=f"rate-limit-alert-{i}",
                severity=AlertSeverity.WARNING,
                metric_name=f"latency-{i}",  # Different metrics to avoid aggregation
                current_value=120.0 + i,  # Different values
                threshold_value=100.0,
                cluster_id=f"test-cluster-{i}",  # Different clusters to avoid aggregation
                node_id=None,
                timestamp=time.time() + i,  # Different timestamps
                message=f"Alert {i}",
            )
            for i in range(5)
        ]

        with patch("builtins.print"):
            for alert in alerts:
                await service.process_alert(alert)

        # Check that some alerts were rate limited
        assert service.stats.rate_limited > 0

    @pytest.mark.asyncio
    async def test_escalation_setup_and_processing(self):
        """Test alert escalation setup and processing."""
        service = FederationAlertingService()

        # Set up channels
        dev_channel = create_console_channel("dev")
        oncall_channel = create_console_channel("oncall")

        service.add_channel(dev_channel)
        service.add_channel(oncall_channel)

        # Set up escalation policy
        policy = EscalationPolicy(
            policy_id="test-escalation",
            name="Test Escalation",
            escalation_levels={
                EscalationLevel.FIRST: ["oncall"],
                EscalationLevel.FINAL: ["oncall"],
            },
            default_channels=["dev"],
        )
        service.add_escalation_policy(policy)

        # Set up routing rule with escalation
        rule = AlertRoutingRule(
            rule_id="escalating-rule",
            name="Escalating Rule",
            conditions={"severity": ["critical"]},
            target_channels=["dev"],
            escalation_policy_id="test-escalation",
        )
        service.add_routing_rule(rule)

        # Create critical alert
        alert = PerformanceAlert(
            alert_id="escalating-alert",
            severity=AlertSeverity.CRITICAL,
            metric_name="health_score",
            current_value=20.0,
            threshold_value=40.0,
            cluster_id="prod-cluster",
            node_id=None,
            timestamp=time.time(),
            message="Critical system failure",
        )

        with patch("builtins.print"):
            await service.process_alert(alert)

        # Check that escalation was set up
        assert alert.alert_id in service.escalations
        assert len(service.escalations[alert.alert_id]) > 0

    def test_alert_resolution(self):
        """Test alert resolution and cleanup."""
        service = FederationAlertingService()

        # Add an active alert
        alert = PerformanceAlert(
            alert_id="test-alert",
            severity=AlertSeverity.WARNING,
            metric_name="latency",
            current_value=120.0,
            threshold_value=100.0,
            cluster_id="test-cluster",
            node_id=None,
            timestamp=time.time(),
            message="Test alert",
        )

        service.active_alerts[alert.alert_id] = alert
        service.escalations[alert.alert_id] = {EscalationLevel.FIRST: time.time() + 300}

        # Resolve the alert
        resolved = service.resolve_alert(alert.alert_id)

        assert resolved is True
        assert alert.alert_id not in service.active_alerts
        assert alert.alert_id not in service.escalations

        # Try to resolve non-existent alert
        not_resolved = service.resolve_alert("non-existent")
        assert not_resolved is False

    def test_alerting_statistics(self):
        """Test alerting statistics collection."""
        service = FederationAlertingService()

        # Add some test data
        service.stats.total_sent = 100
        service.stats.successful_deliveries = 85
        service.stats.failed_deliveries = 15
        service.stats.rate_limited = 5
        service.stats.delivery_latency_ms.extend([10.0, 20.0, 15.0, 25.0, 12.0])

        # Add some channels and rules
        service.add_channel(create_console_channel("test1"))
        service.add_channel(
            create_slack_channel("test2", "https://hooks.slack.com/test")
        )
        service.add_routing_rule(
            create_severity_routing_rule("rule1", ["critical"], ["test1"])
        )

        stats = service.get_alerting_statistics()

        assert stats["enabled"] is True
        assert stats["configured_channels"] == 2
        assert stats["routing_rules"] == 1
        assert stats["delivery_stats"]["total_sent"] == 100
        assert stats["delivery_stats"]["successful_deliveries"] == 85
        assert stats["delivery_stats"]["failed_deliveries"] == 15
        assert stats["delivery_stats"]["success_rate_percent"] == 85.0
        assert (
            stats["delivery_stats"]["average_latency_ms"] == 16.4
        )  # (10+20+15+25+12)/5
        assert stats["delivery_stats"]["rate_limited"] == 5


class TestNotificationTemplates:
    """Test notification template functionality."""

    def test_template_creation_and_formatting(self):
        """Test creating and using notification templates."""
        template = NotificationTemplate(
            template_id="custom_template",
            name="Custom Alert Template",
            backend=NotificationBackend.SLACK,
            subject_template="🚨 {severity} Alert: {metric_name}",
            body_template="Cluster {cluster_id} has {metric_name} = {current_value} (threshold: {threshold_value})",
        )

        assert template.template_id == "custom_template"
        assert template.name == "Custom Alert Template"
        assert template.backend == NotificationBackend.SLACK
        assert "{severity}" in template.subject_template
        assert "{cluster_id}" in template.body_template

    @pytest.mark.asyncio
    async def test_template_usage_in_notifications(self):
        """Test using custom templates in notifications."""
        service = FederationAlertingService()

        # Create custom template
        custom_template = NotificationTemplate(
            template_id="test_template",
            name="Test Template",
            backend=NotificationBackend.CONSOLE,
            subject_template="TEST: {severity} - {metric_name}",
            body_template="Cluster: {cluster_id}, Value: {current_value}",
        )
        service.add_template(custom_template)

        # Create channel with custom template
        channel = NotificationChannel(
            channel_id="template-test",
            backend=NotificationBackend.CONSOLE,
            config={},
            template_name="test_template",
        )
        service.add_channel(channel)

        # Add routing rule
        rule = create_severity_routing_rule("all", ["warning"], ["template-test"])
        service.add_routing_rule(rule)

        # Create alert
        alert = PerformanceAlert(
            alert_id="template-alert",
            severity=AlertSeverity.WARNING,
            metric_name="cpu_usage",
            current_value=85.0,
            threshold_value=80.0,
            cluster_id="test-cluster",
            node_id=None,
            timestamp=time.time(),
            message="High CPU usage",
        )

        # Process alert and check template was used
        with patch("builtins.print") as mock_print:
            await service.process_alert(alert)

        # Verify template formatting was applied
        print_calls = [str(call) for call in mock_print.call_args_list]
        template_used = any("TEST: WARNING - cpu_usage" in call for call in print_calls)
        assert template_used


# Integration tests


@pytest.mark.asyncio
async def test_complete_alerting_workflow():
    """Test a complete end-to-end alerting workflow."""
    service = FederationAlertingService()

    # Set up comprehensive alerting configuration

    # 1. Create notification channels
    dev_console = create_console_channel("dev-console", AlertSeverity.INFO)
    prod_slack = create_slack_channel(
        "prod-slack", "https://hooks.slack.com/prod", AlertSeverity.WARNING
    )
    oncall_webhook = create_webhook_channel(
        "oncall-webhook",
        "https://api.oncall.com/alerts",
        {"Authorization": "Bearer secret"},
        AlertSeverity.CRITICAL,
    )

    service.add_channel(dev_console)
    service.add_channel(prod_slack)
    service.add_channel(oncall_webhook)

    # 2. Create escalation policy
    escalation_policy = EscalationPolicy(
        policy_id="prod-escalation",
        name="Production Escalation",
        escalation_levels={
            EscalationLevel.IMMEDIATE: ["prod-slack"],
            EscalationLevel.FIRST: ["oncall-webhook"],
            EscalationLevel.FINAL: ["oncall-webhook", "prod-slack"],
        },
        default_channels=["dev-console"],
    )
    service.add_escalation_policy(escalation_policy)

    # 3. Create routing rules
    dev_rule = create_cluster_routing_rule(
        "dev-routing", "dev-.*", ["dev-console"], priority=10
    )
    prod_warning_rule = AlertRoutingRule(
        rule_id="prod-warning",
        name="Production Warnings",
        conditions={"cluster_pattern": "prod-.*", "severity": ["warning", "error"]},
        target_channels=["prod-slack"],
        priority=20,
    )
    prod_critical_rule = AlertRoutingRule(
        rule_id="prod-critical",
        name="Production Critical",
        conditions={"cluster_pattern": "prod-.*", "severity": ["critical"]},
        target_channels=["oncall-webhook"],
        escalation_policy_id="prod-escalation",
        priority=5,
    )

    service.add_routing_rule(dev_rule)
    service.add_routing_rule(prod_warning_rule)
    service.add_routing_rule(prod_critical_rule)

    # 4. Create test alerts
    dev_alert = PerformanceAlert(
        alert_id="dev-001",
        severity=AlertSeverity.INFO,
        metric_name="memory_usage",
        current_value=70.0,
        threshold_value=80.0,
        cluster_id="dev-cluster-01",
        node_id=None,
        timestamp=time.time(),
        message="Memory usage within normal range",
    )

    prod_warning_alert = PerformanceAlert(
        alert_id="prod-warn-001",
        severity=AlertSeverity.WARNING,
        metric_name="latency",
        current_value=150.0,
        threshold_value=100.0,
        cluster_id="prod-cluster-01",
        node_id=None,
        timestamp=time.time(),
        message="Elevated latency detected",
    )

    prod_critical_alert = PerformanceAlert(
        alert_id="prod-crit-001",
        severity=AlertSeverity.CRITICAL,
        metric_name="health_score",
        current_value=25.0,
        threshold_value=40.0,
        cluster_id="prod-cluster-02",
        node_id=None,
        timestamp=time.time(),
        message="Critical cluster health failure",
    )

    # 5. Process alerts
    with patch("builtins.print"), patch("aiohttp.ClientSession.post") as mock_post:
        # Mock HTTP responses for Slack and webhook
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(return_value="OK")
        mock_post.return_value.__aenter__.return_value = mock_response

        await service.process_alert(dev_alert)
        await service.process_alert(prod_warning_alert)
        await service.process_alert(prod_critical_alert)

    # 6. Verify results
    assert len(service.active_alerts) == 3
    assert service.stats.total_sent >= 3

    # Check that critical alert has escalation set up
    assert prod_critical_alert.alert_id in service.escalations

    # Check routing worked correctly
    delivery_channels = {delivery.channel_id for delivery in service.delivery_history}
    assert "dev-console" in delivery_channels
    assert "prod-slack" in delivery_channels or "oncall-webhook" in delivery_channels

    # 7. Test alert resolution
    resolved = service.resolve_alert(prod_critical_alert.alert_id)
    assert resolved is True
    assert prod_critical_alert.alert_id not in service.active_alerts
    assert prod_critical_alert.alert_id not in service.escalations

    # 8. Check final statistics
    stats = service.get_alerting_statistics()
    assert stats["configured_channels"] == 3
    assert stats["routing_rules"] == 3
    assert stats["escalation_policies"] == 1
    assert stats["delivery_stats"]["total_sent"] >= 3
    assert stats["delivery_stats"]["success_rate_percent"] > 0


@pytest.mark.asyncio
async def test_alerting_service_resilience():
    """Test alerting service resilience to failures."""
    service = FederationAlertingService()

    # Create a channel that will fail
    failing_channel = NotificationChannel(
        channel_id="failing-webhook",
        backend=NotificationBackend.WEBHOOK,
        config={"webhook_url": "https://nonexistent.example.com/webhook"},
        enabled=True,
    )

    # Create a working channel
    working_channel = create_console_channel("console-backup")

    service.add_channel(failing_channel)
    service.add_channel(working_channel)

    # Route to both channels
    rule = create_severity_routing_rule(
        "redundant-routing", ["critical"], ["failing-webhook", "console-backup"]
    )
    service.add_routing_rule(rule)

    alert = PerformanceAlert(
        alert_id="resilience-test",
        severity=AlertSeverity.CRITICAL,
        metric_name="availability",
        current_value=95.0,
        threshold_value=99.0,
        cluster_id="test-cluster",
        node_id=None,
        timestamp=time.time(),
        message="Service availability degraded",
    )

    # Process alert - one channel should fail, one should succeed
    with patch("builtins.print"):
        await service.process_alert(alert)

    # Check that service continued working despite one failure
    assert len(service.delivery_history) >= 1
    assert service.stats.failed_deliveries >= 1  # Failing webhook
    assert service.stats.successful_deliveries >= 1  # Working console

    # Service should still be functional
    stats = service.get_alerting_statistics()
    assert stats["enabled"] is True
    assert len(service.active_alerts) >= 1
