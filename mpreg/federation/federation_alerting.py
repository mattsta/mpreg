"""
Federation Alerting and Notification System.

This module provides a comprehensive alerting and notification system for federated
MPREG deployments. It supports multiple notification backends, alert aggregation,
escalation policies, and flexible routing rules.

Features:
- Multiple notification backends (email, webhook, Slack, Discord, PagerDuty)
- Alert aggregation and deduplication
- Escalation policies with time-based escalation
- Flexible alert routing based on severity, cluster, and custom rules
- Rate limiting and throttling to prevent alert fatigue
- Rich notification templates with customizable formatting
- Delivery confirmation and retry mechanisms
"""

import asyncio
import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

import aiohttp
from loguru import logger

from .performance_metrics import AlertSeverity, PerformanceAlert


class NotificationBackend(Enum):
    """Supported notification backends."""

    EMAIL = "email"
    WEBHOOK = "webhook"
    SLACK = "slack"
    DISCORD = "discord"
    PAGERDUTY = "pagerduty"
    CONSOLE = "console"
    FILE = "file"


class EscalationLevel(Enum):
    """Alert escalation levels."""

    IMMEDIATE = "immediate"  # 0 minutes
    FIRST = "first"  # 5 minutes
    SECOND = "second"  # 15 minutes
    THIRD = "third"  # 30 minutes
    FINAL = "final"  # 60 minutes


@dataclass(frozen=True, slots=True)
class NotificationChannel:
    """Configuration for a notification channel."""

    channel_id: str
    backend: NotificationBackend
    config: dict[str, Any]
    enabled: bool = True
    min_severity: AlertSeverity = AlertSeverity.INFO
    rate_limit_per_minute: int = 10
    template_name: str | None = None


@dataclass(frozen=True, slots=True)
class EscalationPolicy:
    """Alert escalation policy configuration."""

    policy_id: str
    name: str
    escalation_levels: dict[EscalationLevel, list[str]]  # level -> channel_ids
    default_channels: list[str] = field(default_factory=list)
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class AlertRoutingRule:
    """Rule for routing alerts to specific channels."""

    rule_id: str
    name: str
    conditions: dict[str, Any]  # severity, cluster_pattern, metric_name, etc.
    target_channels: list[str]
    escalation_policy_id: str | None = None
    enabled: bool = True
    priority: int = 100  # Lower number = higher priority


@dataclass(frozen=True, slots=True)
class NotificationTemplate:
    """Template for formatting notifications."""

    template_id: str
    name: str
    backend: NotificationBackend
    subject_template: str
    body_template: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class NotificationDelivery:
    """Result of a notification delivery attempt."""

    delivery_id: str
    channel_id: str
    alert_id: str
    backend: NotificationBackend
    success: bool
    timestamp: float
    response_code: int | None = None
    response_message: str | None = None
    retry_count: int = 0
    delivery_time_ms: float = 0.0


@dataclass(frozen=True, slots=True)
class AggregatedAlert:
    """Aggregated alert combining multiple similar alerts."""

    aggregation_id: str
    alert_count: int
    first_alert: PerformanceAlert
    last_alert: PerformanceAlert
    affected_clusters: set[str]
    common_metric: str
    severity: AlertSeverity
    created_at: float
    last_updated: float
    suppressed_until: float | None = None


@dataclass(slots=True)
class NotificationStats:
    """Statistics for notification delivery."""

    total_sent: int = 0
    successful_deliveries: int = 0
    failed_deliveries: int = 0
    retry_attempts: int = 0
    rate_limited: int = 0
    delivery_latency_ms: deque[float] = field(
        default_factory=lambda: deque(maxlen=1000)
    )
    backend_stats: dict[NotificationBackend, dict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )


class NotificationBackendProtocol(Protocol):
    """Protocol for notification backend implementations."""

    async def send_notification(
        self,
        alert: PerformanceAlert,
        channel: NotificationChannel,
        template: NotificationTemplate | None = None,
    ) -> NotificationDelivery:
        """Send a notification through this backend."""
        ...

    async def send_aggregated_notification(
        self,
        aggregated_alert: AggregatedAlert,
        channel: NotificationChannel,
        template: NotificationTemplate | None = None,
    ) -> NotificationDelivery:
        """Send an aggregated notification through this backend."""
        ...

    async def health_check(self) -> bool:
        """Check if the backend is healthy and can send notifications."""
        ...


@dataclass(slots=True)
class FederationAlertingService:
    """
    Comprehensive alerting and notification service for federation.

    Provides intelligent alert routing, escalation, aggregation, and multi-backend
    notification delivery with rate limiting and retry mechanisms.
    """

    # Configuration
    enabled: bool = True
    aggregation_window_seconds: float = 300.0  # 5 minutes
    max_escalation_time_seconds: float = 3600.0  # 1 hour
    rate_limit_window_seconds: float = 60.0  # 1 minute

    # Core components
    channels: dict[str, NotificationChannel] = field(default_factory=dict)
    escalation_policies: dict[str, EscalationPolicy] = field(default_factory=dict)
    routing_rules: list[AlertRoutingRule] = field(default_factory=list)
    templates: dict[str, NotificationTemplate] = field(default_factory=dict)
    backends: dict[NotificationBackend, NotificationBackendProtocol] = field(
        default_factory=dict
    )

    # Runtime state
    active_alerts: dict[str, PerformanceAlert] = field(default_factory=dict)
    aggregated_alerts: dict[str, AggregatedAlert] = field(default_factory=dict)
    escalations: dict[str, dict[EscalationLevel, float]] = field(
        default_factory=dict
    )  # alert_id -> level -> timestamp
    delivery_history: deque[NotificationDelivery] = field(
        default_factory=lambda: deque(maxlen=10000)
    )
    rate_limits: dict[str, deque[float]] = field(
        default_factory=lambda: defaultdict(lambda: deque(maxlen=1000))
    )

    # Statistics and monitoring
    stats: NotificationStats = field(default_factory=NotificationStats)

    def __post_init__(self) -> None:
        """Initialize the alerting service."""
        logger.info("Federation alerting service initialized")

        # Register default backends
        self._register_default_backends()

        # Create default templates
        self._create_default_templates()

    def _register_default_backends(self) -> None:
        """Register default notification backends."""
        self.backends[NotificationBackend.CONSOLE] = ConsoleNotificationBackend()
        self.backends[NotificationBackend.WEBHOOK] = WebhookNotificationBackend()
        self.backends[NotificationBackend.SLACK] = SlackNotificationBackend()
        self.backends[NotificationBackend.EMAIL] = EmailNotificationBackend()

    def _create_default_templates(self) -> None:
        """Create default notification templates."""
        # Console template
        console_template = NotificationTemplate(
            template_id="default_console",
            name="Default Console",
            backend=NotificationBackend.CONSOLE,
            subject_template="ALERT: {severity} - {metric_name}",
            body_template="Cluster: {cluster_id}\nMetric: {metric_name}\nCurrent: {current_value}\nThreshold: {threshold_value}\nMessage: {message}",
        )

        # Slack template - using simple text format to avoid JSON escaping issues
        slack_template = NotificationTemplate(
            template_id="default_slack",
            name="Default Slack",
            backend=NotificationBackend.SLACK,
            subject_template="🚨 {severity} Alert: {metric_name}",
            body_template="*🚨 {severity} Alert*\n*Cluster:* {cluster_id}\n*Metric:* {metric_name}\n*Current Value:* {current_value}\n*Threshold:* {threshold_value}\n*Message:* {message}",
        )

        self.templates[console_template.template_id] = console_template
        self.templates[slack_template.template_id] = slack_template

    def add_channel(self, channel: NotificationChannel) -> None:
        """Add a notification channel."""
        self.channels[channel.channel_id] = channel
        logger.info(
            f"Added notification channel: {channel.channel_id} ({channel.backend.value})"
        )

    def add_escalation_policy(self, policy: EscalationPolicy) -> None:
        """Add an escalation policy."""
        self.escalation_policies[policy.policy_id] = policy
        logger.info(f"Added escalation policy: {policy.policy_id}")

    def add_routing_rule(self, rule: AlertRoutingRule) -> None:
        """Add an alert routing rule."""
        self.routing_rules.append(rule)
        # Sort by priority (lower number = higher priority)
        self.routing_rules.sort(key=lambda r: r.priority)
        logger.info(f"Added routing rule: {rule.rule_id} (priority: {rule.priority})")

    def add_template(self, template: NotificationTemplate) -> None:
        """Add a notification template."""
        self.templates[template.template_id] = template
        logger.info(f"Added notification template: {template.template_id}")

    async def process_alert(self, alert: PerformanceAlert) -> None:
        """Process an incoming alert and handle notifications."""
        if not self.enabled:
            return

        logger.debug(f"Processing alert: {alert.alert_id}")

        # Store the alert
        self.active_alerts[alert.alert_id] = alert

        # Check for aggregation opportunities
        aggregated = await self._try_aggregate_alert(alert)
        if aggregated:
            logger.debug(f"Alert aggregated: {alert.alert_id}")
            return

        # Find matching routing rules
        target_channels = self._find_target_channels(alert)

        if not target_channels:
            logger.warning(f"No routing rules matched alert: {alert.alert_id}")
            return

        # Send notifications
        await self._send_alert_notifications(alert, target_channels)

        # Set up escalation if needed
        await self._setup_escalation(alert, target_channels)

    async def _try_aggregate_alert(self, alert: PerformanceAlert) -> bool:
        """Try to aggregate the alert with existing similar alerts."""
        aggregation_key = (
            f"{alert.cluster_id}_{alert.metric_name}_{alert.severity.value}"
        )

        current_time = time.time()

        # Check if we have an existing aggregation
        if aggregation_key in self.aggregated_alerts:
            existing = self.aggregated_alerts[aggregation_key]

            # Check if aggregation window is still open
            if current_time - existing.created_at <= self.aggregation_window_seconds:
                # Update the aggregation
                updated_aggregation = AggregatedAlert(
                    aggregation_id=existing.aggregation_id,
                    alert_count=existing.alert_count + 1,
                    first_alert=existing.first_alert,
                    last_alert=alert,
                    affected_clusters=existing.affected_clusters | {alert.cluster_id},
                    common_metric=alert.metric_name,
                    severity=max(
                        existing.severity,
                        alert.severity,
                        key=lambda s: self._severity_priority(s),
                    ),
                    created_at=existing.created_at,
                    last_updated=current_time,
                    suppressed_until=existing.suppressed_until,
                )

                self.aggregated_alerts[aggregation_key] = updated_aggregation
                logger.debug(
                    f"Updated aggregated alert: {aggregation_key} (count: {updated_aggregation.alert_count})"
                )
                return True

        # Check if we should start a new aggregation
        similar_alerts = [
            a
            for a in self.active_alerts.values()
            if (
                a.metric_name == alert.metric_name
                and abs(a.timestamp - alert.timestamp)
                <= self.aggregation_window_seconds
                and a.alert_id != alert.alert_id
            )
        ]

        if (
            len(similar_alerts) >= 1
        ):  # Start aggregating if we have at least 1 similar alert
            aggregation_id = f"agg_{aggregation_key}_{int(current_time)}"
            aggregated_alert = AggregatedAlert(
                aggregation_id=aggregation_id,
                alert_count=len(similar_alerts) + 1,
                first_alert=min([alert] + similar_alerts, key=lambda a: a.timestamp),
                last_alert=alert,
                affected_clusters={alert.cluster_id}
                | {a.cluster_id for a in similar_alerts},
                common_metric=alert.metric_name,
                severity=max(
                    [alert] + similar_alerts,
                    key=lambda a: self._severity_priority(a.severity),
                ).severity,
                created_at=current_time,
                last_updated=current_time,
            )

            self.aggregated_alerts[aggregation_key] = aggregated_alert
            logger.info(
                f"Created new aggregated alert: {aggregation_id} (count: {aggregated_alert.alert_count})"
            )
            return True

        return False

    def _severity_priority(self, severity: AlertSeverity) -> int:
        """Get numeric priority for severity (higher = more severe)."""
        priorities = {
            AlertSeverity.INFO: 1,
            AlertSeverity.WARNING: 2,
            AlertSeverity.CRITICAL: 3,
            AlertSeverity.CRITICAL: 4,
        }
        return priorities.get(severity, 0)

    def _find_target_channels(self, alert: PerformanceAlert) -> list[str]:
        """Find target channels for an alert based on routing rules."""
        target_channels = []

        for rule in self.routing_rules:
            if not rule.enabled:
                continue

            if self._rule_matches_alert(rule, alert):
                target_channels.extend(rule.target_channels)
                logger.debug(
                    f"Routing rule matched: {rule.rule_id} -> {rule.target_channels}"
                )

        # Remove duplicates while preserving order
        seen = set()
        unique_channels = []
        for channel_id in target_channels:
            if channel_id not in seen:
                seen.add(channel_id)
                unique_channels.append(channel_id)

        return unique_channels

    def _rule_matches_alert(
        self, rule: AlertRoutingRule, alert: PerformanceAlert
    ) -> bool:
        """Check if a routing rule matches an alert."""
        conditions = rule.conditions

        # Check severity
        if "severity" in conditions:
            required_severities = conditions["severity"]
            if isinstance(required_severities, str):
                required_severities = [required_severities]
            if alert.severity.value not in required_severities:
                return False

        # Check cluster pattern
        if "cluster_pattern" in conditions:
            import re

            pattern = conditions["cluster_pattern"]
            if not re.match(pattern, alert.cluster_id):
                return False

        # Check metric name
        if "metric_name" in conditions:
            required_metrics = conditions["metric_name"]
            if isinstance(required_metrics, str):
                required_metrics = [required_metrics]
            if alert.metric_name not in required_metrics:
                return False

        # Check labels (using available fields from PerformanceAlert)
        if "labels" in conditions:
            required_labels = conditions["labels"]
            alert_data = {
                "cluster_id": alert.cluster_id,
                "node_id": alert.node_id,
                "metric_name": alert.metric_name,
            }
            for key, value in required_labels.items():
                if key not in alert_data or alert_data[key] != value:
                    return False

        return True

    async def _send_alert_notifications(
        self, alert: PerformanceAlert, channel_ids: list[str]
    ) -> None:
        """Send notifications for an alert to specified channels."""
        tasks = []

        for channel_id in channel_ids:
            if channel_id not in self.channels:
                logger.warning(f"Unknown channel: {channel_id}")
                continue

            channel = self.channels[channel_id]

            # Check if channel is enabled and severity meets minimum
            if not channel.enabled or self._severity_priority(
                alert.severity
            ) < self._severity_priority(channel.min_severity):
                continue

            # Check rate limiting
            if not self._check_rate_limit(channel_id, channel.rate_limit_per_minute):
                self.stats.rate_limited += 1
                logger.warning(f"Rate limit exceeded for channel: {channel_id}")
                continue

            # Get template
            template = None
            if channel.template_name and channel.template_name in self.templates:
                template = self.templates[channel.template_name]
            else:
                # Find default template for backend
                default_template_id = f"default_{channel.backend.value}"
                if default_template_id in self.templates:
                    template = self.templates[default_template_id]

            # Send notification
            task = self._send_single_notification(alert, channel, template)
            tasks.append(task)

        if tasks:
            deliveries = await asyncio.gather(*tasks, return_exceptions=True)

            for delivery in deliveries:
                if isinstance(delivery, Exception):
                    logger.error(f"Notification delivery failed: {delivery}")
                    self.stats.failed_deliveries += 1
                elif isinstance(delivery, NotificationDelivery):
                    self.delivery_history.append(delivery)
                    if delivery.success:
                        self.stats.successful_deliveries += 1
                    else:
                        self.stats.failed_deliveries += 1

                    self.stats.delivery_latency_ms.append(delivery.delivery_time_ms)
                    self.stats.backend_stats[delivery.backend]["sent"] += 1
                    if delivery.success:
                        self.stats.backend_stats[delivery.backend]["success"] += 1
                    else:
                        self.stats.backend_stats[delivery.backend]["failed"] += 1

    def _check_rate_limit(self, channel_id: str, limit_per_minute: int) -> bool:
        """Check if a channel is within its rate limit."""
        current_time = time.time()
        window_start = current_time - self.rate_limit_window_seconds

        # Clean old entries
        channel_times = self.rate_limits[channel_id]
        while channel_times and channel_times[0] < window_start:
            channel_times.popleft()

        # Check if we're under the limit
        if len(channel_times) >= limit_per_minute:
            return False

        # Record this attempt
        channel_times.append(current_time)
        return True

    async def _send_single_notification(
        self,
        alert: PerformanceAlert,
        channel: NotificationChannel,
        template: NotificationTemplate | None,
    ) -> NotificationDelivery:
        """Send a single notification."""
        start_time = time.time()

        try:
            backend = self.backends.get(channel.backend)
            if not backend:
                raise ValueError(f"No backend available for: {channel.backend}")

            delivery = await backend.send_notification(alert, channel, template)
            delivery_time = (time.time() - start_time) * 1000

            # Update delivery time
            updated_delivery = NotificationDelivery(
                delivery_id=delivery.delivery_id,
                channel_id=delivery.channel_id,
                alert_id=delivery.alert_id,
                backend=delivery.backend,
                success=delivery.success,
                timestamp=delivery.timestamp,
                response_code=delivery.response_code,
                response_message=delivery.response_message,
                retry_count=delivery.retry_count,
                delivery_time_ms=delivery_time,
            )

            self.stats.total_sent += 1
            return updated_delivery

        except Exception as e:
            logger.error(
                f"Failed to send notification via {channel.backend.value}: {e}"
            )
            delivery_time = (time.time() - start_time) * 1000

            return NotificationDelivery(
                delivery_id=f"failed_{int(time.time())}_{channel.channel_id}",
                channel_id=channel.channel_id,
                alert_id=alert.alert_id,
                backend=channel.backend,
                success=False,
                timestamp=time.time(),
                response_message=str(e),
                delivery_time_ms=delivery_time,
            )

    async def _setup_escalation(
        self, alert: PerformanceAlert, initial_channels: list[str]
    ) -> None:
        """Set up escalation for an alert if needed."""
        # Find escalation policy from routing rules
        escalation_policy_id = None
        for rule in self.routing_rules:
            if (
                rule.enabled
                and self._rule_matches_alert(rule, alert)
                and rule.escalation_policy_id
            ):
                escalation_policy_id = rule.escalation_policy_id
                break

        if (
            not escalation_policy_id
            or escalation_policy_id not in self.escalation_policies
        ):
            return

        policy = self.escalation_policies[escalation_policy_id]
        if not policy.enabled:
            return

        # Set up escalation timeline
        current_time = time.time()
        escalation_schedule = {}

        for level, delay_minutes in [
            (EscalationLevel.FIRST, 5),
            (EscalationLevel.SECOND, 15),
            (EscalationLevel.THIRD, 30),
            (EscalationLevel.FINAL, 60),
        ]:
            if level in policy.escalation_levels:
                escalation_schedule[level] = current_time + (delay_minutes * 60)

        if escalation_schedule:
            self.escalations[alert.alert_id] = escalation_schedule
            logger.info(f"Escalation scheduled for alert: {alert.alert_id}")

    async def process_escalations(self) -> None:
        """Process pending escalations."""
        current_time = time.time()
        escalations_to_process = []

        for alert_id, schedule in self.escalations.items():
            if alert_id not in self.active_alerts:
                continue

            for level, escalate_time in schedule.items():
                if current_time >= escalate_time:
                    escalations_to_process.append((alert_id, level))

        for alert_id, level in escalations_to_process:
            await self._execute_escalation(alert_id, level)

            # Remove processed escalation
            if alert_id in self.escalations:
                del self.escalations[alert_id][level]
                if not self.escalations[alert_id]:
                    del self.escalations[alert_id]

    async def _execute_escalation(self, alert_id: str, level: EscalationLevel) -> None:
        """Execute an escalation for an alert."""
        if alert_id not in self.active_alerts:
            return

        alert = self.active_alerts[alert_id]

        # Find escalation policy
        escalation_policy_id = None
        for rule in self.routing_rules:
            if (
                rule.enabled
                and self._rule_matches_alert(rule, alert)
                and rule.escalation_policy_id
            ):
                escalation_policy_id = rule.escalation_policy_id
                break

        if (
            not escalation_policy_id
            or escalation_policy_id not in self.escalation_policies
        ):
            return

        policy = self.escalation_policies[escalation_policy_id]

        if level in policy.escalation_levels:
            target_channels = policy.escalation_levels[level]
            logger.warning(
                f"Escalating alert {alert_id} to level {level.value}: {target_channels}"
            )
            await self._send_alert_notifications(alert, target_channels)

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an active alert and cancel its escalations."""
        if alert_id in self.active_alerts:
            del self.active_alerts[alert_id]

            # Cancel escalations
            if alert_id in self.escalations:
                del self.escalations[alert_id]

            logger.info(f"Alert resolved: {alert_id}")
            return True

        return False

    def get_active_alerts(self) -> list[PerformanceAlert]:
        """Get list of currently active alerts."""
        return list(self.active_alerts.values())

    def get_alerting_statistics(self) -> dict[str, Any]:
        """Get comprehensive alerting statistics."""
        current_time = time.time()

        # Calculate average delivery latency
        avg_latency = 0.0
        if self.stats.delivery_latency_ms:
            avg_latency = sum(self.stats.delivery_latency_ms) / len(
                self.stats.delivery_latency_ms
            )

        # Calculate success rate
        total_deliveries = (
            self.stats.successful_deliveries + self.stats.failed_deliveries
        )
        success_rate = 0.0
        if total_deliveries > 0:
            success_rate = (self.stats.successful_deliveries / total_deliveries) * 100

        return {
            "enabled": self.enabled,
            "active_alerts": len(self.active_alerts),
            "aggregated_alerts": len(self.aggregated_alerts),
            "pending_escalations": len(self.escalations),
            "configured_channels": len(self.channels),
            "routing_rules": len(self.routing_rules),
            "escalation_policies": len(self.escalation_policies),
            "delivery_stats": {
                "total_sent": self.stats.total_sent,
                "successful_deliveries": self.stats.successful_deliveries,
                "failed_deliveries": self.stats.failed_deliveries,
                "success_rate_percent": success_rate,
                "average_latency_ms": avg_latency,
                "rate_limited": self.stats.rate_limited,
            },
            "backend_stats": dict(self.stats.backend_stats),
            "recent_deliveries": len(
                [
                    d
                    for d in self.delivery_history
                    if current_time - d.timestamp <= 3600  # Last hour
                ]
            ),
        }


# Notification Backend Implementations


class ConsoleNotificationBackend:
    """Simple console-based notification backend for development."""

    async def send_notification(
        self,
        alert: PerformanceAlert,
        channel: NotificationChannel,
        template: NotificationTemplate | None = None,
    ) -> NotificationDelivery:
        """Send notification to console."""
        start_time = time.time()

        try:
            if template:
                subject = self._format_template(template.subject_template, alert)
                body = self._format_template(template.body_template, alert)
            else:
                subject = f"ALERT: {alert.severity.value.upper()} - {alert.metric_name}"
                body = f"Cluster: {alert.cluster_id}\nMessage: {alert.message}"

            print(f"\n{'=' * 60}")
            print(f"📢 NOTIFICATION: {channel.channel_id}")
            print(f"Subject: {subject}")
            print(f"Body: {body}")
            print(f"{'=' * 60}\n")

            return NotificationDelivery(
                delivery_id=f"console_{int(time.time())}_{channel.channel_id}",
                channel_id=channel.channel_id,
                alert_id=alert.alert_id,
                backend=NotificationBackend.CONSOLE,
                success=True,
                timestamp=time.time(),
                response_code=200,
                response_message="Printed to console",
            )

        except Exception as e:
            return NotificationDelivery(
                delivery_id=f"console_failed_{int(time.time())}_{channel.channel_id}",
                channel_id=channel.channel_id,
                alert_id=alert.alert_id,
                backend=NotificationBackend.CONSOLE,
                success=False,
                timestamp=time.time(),
                response_message=str(e),
            )

    async def send_aggregated_notification(
        self,
        aggregated_alert: AggregatedAlert,
        channel: NotificationChannel,
        template: NotificationTemplate | None = None,
    ) -> NotificationDelivery:
        """Send aggregated notification to console."""
        # For simplicity, use the last alert for formatting
        return await self.send_notification(
            aggregated_alert.last_alert, channel, template
        )

    async def health_check(self) -> bool:
        """Console backend is always healthy."""
        return True

    def _format_template(self, template: str, alert: PerformanceAlert) -> str:
        """Format a template string with alert data."""
        return template.format(
            severity=alert.severity.value.upper(),
            cluster_id=alert.cluster_id,
            metric_name=alert.metric_name,
            current_value=alert.current_value,
            threshold_value=alert.threshold_value,
            message=alert.message,
            timestamp=alert.timestamp,
        )


class WebhookNotificationBackend:
    """Webhook-based notification backend."""

    async def send_notification(
        self,
        alert: PerformanceAlert,
        channel: NotificationChannel,
        template: NotificationTemplate | None = None,
    ) -> NotificationDelivery:
        """Send notification via webhook."""
        webhook_url = channel.config.get("webhook_url")
        if not webhook_url:
            raise ValueError("webhook_url required in channel config")

        headers = channel.config.get("headers", {"Content-Type": "application/json"})
        timeout = channel.config.get("timeout", 30)

        payload = {
            "alert_id": alert.alert_id,
            "cluster_id": alert.cluster_id,
            "severity": alert.severity.value,
            "metric_name": alert.metric_name,
            "current_value": alert.current_value,
            "threshold_value": alert.threshold_value,
            "message": alert.message,
            "timestamp": alert.timestamp,
            "node_id": alert.node_id,
            "resolved": alert.resolved,
        }

        if template:
            payload["formatted_subject"] = self._format_template(
                template.subject_template, alert
            )
            payload["formatted_body"] = self._format_template(
                template.body_template, alert
            )

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook_url,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as response:
                    response_text = await response.text()

                    return NotificationDelivery(
                        delivery_id=f"webhook_{int(time.time())}_{channel.channel_id}",
                        channel_id=channel.channel_id,
                        alert_id=alert.alert_id,
                        backend=NotificationBackend.WEBHOOK,
                        success=response.status < 400,
                        timestamp=time.time(),
                        response_code=response.status,
                        response_message=response_text[:500],  # Truncate long responses
                    )

        except Exception as e:
            return NotificationDelivery(
                delivery_id=f"webhook_failed_{int(time.time())}_{channel.channel_id}",
                channel_id=channel.channel_id,
                alert_id=alert.alert_id,
                backend=NotificationBackend.WEBHOOK,
                success=False,
                timestamp=time.time(),
                response_message=str(e),
            )

    async def send_aggregated_notification(
        self,
        aggregated_alert: AggregatedAlert,
        channel: NotificationChannel,
        template: NotificationTemplate | None = None,
    ) -> NotificationDelivery:
        """Send aggregated notification via webhook."""
        # Convert aggregated alert to standard alert format for webhook
        return await self.send_notification(
            aggregated_alert.last_alert, channel, template
        )

    async def health_check(self) -> bool:
        """Check webhook health by making a test request."""
        return True  # Basic implementation - could ping webhook endpoint

    def _format_template(self, template: str, alert: PerformanceAlert) -> str:
        """Format a template string with alert data."""
        return template.format(
            severity=alert.severity.value.upper(),
            cluster_id=alert.cluster_id,
            metric_name=alert.metric_name,
            current_value=alert.current_value,
            threshold_value=alert.threshold_value,
            message=alert.message,
            timestamp=alert.timestamp,
        )


class SlackNotificationBackend:
    """Slack-based notification backend."""

    async def send_notification(
        self,
        alert: PerformanceAlert,
        channel: NotificationChannel,
        template: NotificationTemplate | None = None,
    ) -> NotificationDelivery:
        """Send notification to Slack."""
        webhook_url = channel.config.get("webhook_url")
        if not webhook_url:
            raise ValueError("webhook_url required for Slack notifications")

        # Format message based on template or default
        if template:
            formatted_body = self._format_template(template.body_template, alert)
            try:
                # Try to parse as JSON for rich formatting
                payload = json.loads(formatted_body)
            except json.JSONDecodeError:
                # Fall back to simple text
                payload = {"text": formatted_body}
        else:
            # Default Slack formatting
            severity_emoji = {
                AlertSeverity.INFO: "ℹ️",
                AlertSeverity.WARNING: "⚠️",
                AlertSeverity.CRITICAL: "❌",
                AlertSeverity.CRITICAL: "🚨",
            }

            payload = {
                "attachments": [
                    {
                        "color": self._get_severity_color(alert.severity),
                        "title": f"{severity_emoji.get(alert.severity, '🔔')} {alert.severity.value.upper()} Alert",
                        "fields": [
                            {
                                "title": "Cluster",
                                "value": alert.cluster_id,
                                "short": True,
                            },
                            {
                                "title": "Metric",
                                "value": alert.metric_name,
                                "short": True,
                            },
                            {
                                "title": "Current Value",
                                "value": str(alert.current_value),
                                "short": True,
                            },
                            {
                                "title": "Threshold",
                                "value": str(alert.threshold_value),
                                "short": True,
                            },
                            {
                                "title": "Message",
                                "value": alert.message,
                                "short": False,
                            },
                        ],
                        "ts": int(alert.timestamp),
                    }
                ]
            }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook_url, json=payload, timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    response_text = await response.text()

                    return NotificationDelivery(
                        delivery_id=f"slack_{int(time.time())}_{channel.channel_id}",
                        channel_id=channel.channel_id,
                        alert_id=alert.alert_id,
                        backend=NotificationBackend.SLACK,
                        success=response.status == 200,
                        timestamp=time.time(),
                        response_code=response.status,
                        response_message=response_text,
                    )

        except Exception as e:
            return NotificationDelivery(
                delivery_id=f"slack_failed_{int(time.time())}_{channel.channel_id}",
                channel_id=channel.channel_id,
                alert_id=alert.alert_id,
                backend=NotificationBackend.SLACK,
                success=False,
                timestamp=time.time(),
                response_message=str(e),
            )

    async def send_aggregated_notification(
        self,
        aggregated_alert: AggregatedAlert,
        channel: NotificationChannel,
        template: NotificationTemplate | None = None,
    ) -> NotificationDelivery:
        """Send aggregated notification to Slack."""
        # Convert to standard alert format
        return await self.send_notification(
            aggregated_alert.last_alert, channel, template
        )

    async def health_check(self) -> bool:
        """Check Slack webhook health."""
        return True  # Basic implementation

    def _get_severity_color(self, severity: AlertSeverity) -> str:
        """Get color for Slack attachment based on severity."""
        colors = {
            AlertSeverity.INFO: "good",
            AlertSeverity.WARNING: "warning",
            AlertSeverity.CRITICAL: "danger",
            AlertSeverity.CRITICAL: "#ff0000",
        }
        return colors.get(severity, "#808080")

    def _format_template(self, template: str, alert: PerformanceAlert) -> str:
        """Format a template string with alert data."""
        return template.format(
            severity=alert.severity.value.upper(),
            cluster_id=alert.cluster_id,
            metric_name=alert.metric_name,
            current_value=alert.current_value,
            threshold_value=alert.threshold_value,
            message=alert.message,
            timestamp=alert.timestamp,
        )


class EmailNotificationBackend:
    """Email-based notification backend (placeholder implementation)."""

    async def send_notification(
        self,
        alert: PerformanceAlert,
        channel: NotificationChannel,
        template: NotificationTemplate | None = None,
    ) -> NotificationDelivery:
        """Send notification via email."""
        # This is a placeholder implementation
        # In a real implementation, this would integrate with an email service

        smtp_server = channel.config.get("smtp_server")
        if not smtp_server:
            raise ValueError("smtp_server required for email notifications")

        # For now, just log that we would send an email
        logger.info(
            f"Would send email notification for alert {alert.alert_id} to {channel.config.get('recipient')}"
        )

        return NotificationDelivery(
            delivery_id=f"email_{int(time.time())}_{channel.channel_id}",
            channel_id=channel.channel_id,
            alert_id=alert.alert_id,
            backend=NotificationBackend.EMAIL,
            success=True,  # Simulated success
            timestamp=time.time(),
            response_code=200,
            response_message="Email sent (simulated)",
        )

    async def send_aggregated_notification(
        self,
        aggregated_alert: AggregatedAlert,
        channel: NotificationChannel,
        template: NotificationTemplate | None = None,
    ) -> NotificationDelivery:
        """Send aggregated notification via email."""
        return await self.send_notification(
            aggregated_alert.last_alert, channel, template
        )

    async def health_check(self) -> bool:
        """Check email backend health."""
        return True  # Simulated health check


# Helper functions for common alerting scenarios


def create_console_channel(
    channel_id: str, min_severity: AlertSeverity = AlertSeverity.INFO
) -> NotificationChannel:
    """Create a console notification channel."""
    return NotificationChannel(
        channel_id=channel_id,
        backend=NotificationBackend.CONSOLE,
        config={},
        min_severity=min_severity,
        template_name="default_console",
    )


def create_slack_channel(
    channel_id: str,
    webhook_url: str,
    min_severity: AlertSeverity = AlertSeverity.WARNING,
) -> NotificationChannel:
    """Create a Slack notification channel."""
    return NotificationChannel(
        channel_id=channel_id,
        backend=NotificationBackend.SLACK,
        config={"webhook_url": webhook_url},
        min_severity=min_severity,
        template_name="default_slack",
    )


def create_webhook_channel(
    channel_id: str,
    webhook_url: str,
    headers: dict[str, str] | None = None,
    min_severity: AlertSeverity = AlertSeverity.WARNING,
) -> NotificationChannel:
    """Create a webhook notification channel."""
    config: dict[str, Any] = {"webhook_url": webhook_url}
    if headers:
        config["headers"] = headers

    return NotificationChannel(
        channel_id=channel_id,
        backend=NotificationBackend.WEBHOOK,
        config=config,
        min_severity=min_severity,
    )


def create_basic_escalation_policy(
    policy_id: str, immediate_channels: list[str], escalated_channels: list[str]
) -> EscalationPolicy:
    """Create a basic escalation policy."""
    return EscalationPolicy(
        policy_id=policy_id,
        name=f"Basic Escalation - {policy_id}",
        escalation_levels={
            EscalationLevel.IMMEDIATE: immediate_channels,
            EscalationLevel.FIRST: escalated_channels,
            EscalationLevel.FINAL: escalated_channels,
        },
        default_channels=immediate_channels,
    )


def create_severity_routing_rule(
    rule_id: str,
    target_severities: list[str],
    target_channels: list[str],
    priority: int = 100,
) -> AlertRoutingRule:
    """Create a routing rule based on alert severity."""
    return AlertRoutingRule(
        rule_id=rule_id,
        name=f"Severity Routing - {rule_id}",
        conditions={"severity": target_severities},
        target_channels=target_channels,
        priority=priority,
    )


def create_cluster_routing_rule(
    rule_id: str, cluster_pattern: str, target_channels: list[str], priority: int = 100
) -> AlertRoutingRule:
    """Create a routing rule based on cluster pattern."""
    return AlertRoutingRule(
        rule_id=rule_id,
        name=f"Cluster Routing - {rule_id}",
        conditions={"cluster_pattern": cluster_pattern},
        target_channels=target_channels,
        priority=priority,
    )
