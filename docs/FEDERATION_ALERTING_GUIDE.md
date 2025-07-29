# Federation Alerting & Notification System Guide

The MPREG Federation Alerting System provides enterprise-grade notification capabilities with multiple backends, intelligent routing, escalation policies, and alert aggregation.

## Quick Start

### 1. Basic Setup

```python
from mpreg.federation.federation_alerting import (
    FederationAlertingService,
    create_console_channel,
    create_slack_channel,
    create_severity_routing_rule,
    AlertSeverity
)

# Create the alerting service
alerting = FederationAlertingService()

# Add notification channels
console_channel = create_console_channel("dev-console", AlertSeverity.INFO)
slack_channel = create_slack_channel("prod-slack", "https://hooks.slack.com/your-webhook", AlertSeverity.WARNING)

alerting.add_channel(console_channel)
alerting.add_channel(slack_channel)

# Add routing rules
dev_rule = create_severity_routing_rule("dev-alerts", ["info", "warning"], ["dev-console"])
prod_rule = create_severity_routing_rule("prod-alerts", ["error", "critical"], ["prod-slack"])

alerting.add_routing_rule(dev_rule)
alerting.add_routing_rule(prod_rule)
```

### 2. Integration with Performance Metrics

```python
from mpreg.federation.performance_metrics import PerformanceMetricsService, create_performance_metrics_service

# Create performance metrics service
metrics = create_performance_metrics_service(collection_interval=30.0)

# Connect alerting to metrics
async def alert_callback(alert):
    await alerting.process_alert(alert)

metrics.add_alert_callback(alert_callback)

# Start collecting metrics
await metrics.start_collection()
```

## Notification Backends

### Console Backend

Perfect for development and debugging:

```python
from mpreg.federation.federation_alerting import create_console_channel, AlertSeverity

channel = create_console_channel(
    channel_id="dev-console",
    min_severity=AlertSeverity.INFO  # Show all alerts
)
```

### Slack Backend

Rich integration with Slack workspaces:

```python
from mpreg.federation.federation_alerting import create_slack_channel

channel = create_slack_channel(
    channel_id="ops-slack",
    webhook_url="https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
    min_severity=AlertSeverity.WARNING
)
```

### Webhook Backend

Generic HTTP endpoint integration:

```python
from mpreg.federation.federation_alerting import create_webhook_channel

channel = create_webhook_channel(
    channel_id="monitoring-api",
    webhook_url="https://your-monitoring.com/alerts",
    headers={"Authorization": "Bearer your-token"},
    min_severity=AlertSeverity.ERROR
)
```

## Alert Routing Rules

### Severity-Based Routing

```python
from mpreg.federation.federation_alerting import create_severity_routing_rule

# Route critical alerts to multiple channels
critical_rule = create_severity_routing_rule(
    rule_id="critical-alerts",
    target_severities=["critical"],
    target_channels=["slack-oncall", "webhook-pager", "email-managers"],
    priority=10  # Higher priority (lower number)
)
```

### Cluster-Based Routing

```python
from mpreg.federation.federation_alerting import create_cluster_routing_rule

# Route production cluster alerts differently
prod_rule = create_cluster_routing_rule(
    rule_id="production-clusters",
    cluster_pattern="prod-.*",  # Regex pattern
    target_channels=["slack-prod", "webhook-ops"],
    priority=20
)
```

### Custom Routing Rules

```python
from mpreg.federation.federation_alerting import AlertRoutingRule

# Complex rule with multiple conditions
custom_rule = AlertRoutingRule(
    rule_id="database-critical",
    name="Database Critical Alerts",
    conditions={
        "severity": ["critical", "error"],
        "cluster_pattern": "db-.*",
        "metric_name": ["health_score", "error_rate"],
        "labels": {"service": "database"}
    },
    target_channels=["dba-oncall", "slack-database"],
    priority=5  # Highest priority
)
```

## Escalation Policies

### Basic Escalation

```python
from mpreg.federation.federation_alerting import (
    EscalationPolicy,
    EscalationLevel,
    create_basic_escalation_policy
)

# Simple escalation: dev team → oncall → management
policy = create_basic_escalation_policy(
    policy_id="standard-escalation",
    immediate_channels=["slack-dev"],
    escalated_channels=["slack-oncall", "email-managers"]
)

alerting.add_escalation_policy(policy)
```

### Advanced Escalation

```python
# Custom escalation with multiple levels
advanced_policy = EscalationPolicy(
    policy_id="critical-escalation",
    name="Critical System Escalation",
    escalation_levels={
        EscalationLevel.IMMEDIATE: ["slack-dev"],           # 0 minutes
        EscalationLevel.FIRST: ["slack-oncall"],            # 5 minutes
        EscalationLevel.SECOND: ["pager-oncall"],           # 15 minutes
        EscalationLevel.THIRD: ["email-managers"],          # 30 minutes
        EscalationLevel.FINAL: ["ceo-phone"]                # 60 minutes
    },
    default_channels=["slack-dev"]
)

# Link escalation to routing rule
critical_rule = AlertRoutingRule(
    rule_id="escalating-critical",
    name="Critical Alerts with Escalation",
    conditions={"severity": ["critical"]},
    target_channels=["slack-dev"],
    escalation_policy_id="critical-escalation",  # Link to policy
    priority=1
)
```

## Custom Notification Templates

### Slack Templates

```python
from mpreg.federation.federation_alerting import NotificationTemplate, NotificationBackend

slack_template = NotificationTemplate(
    template_id="custom_slack",
    name="Custom Slack Alert",
    backend=NotificationBackend.SLACK,
    subject_template="🚨 {severity} Alert: {metric_name}",
    body_template="""
*🚨 {severity} Alert in {cluster_id}*
📊 *Metric:* {metric_name}
📈 *Current Value:* {current_value}
🎯 *Threshold:* {threshold_value}
💬 *Details:* {message}
🕐 *Time:* {timestamp}
    """.strip()
)

alerting.add_template(slack_template)
```

### Webhook Templates

```python
webhook_template = NotificationTemplate(
    template_id="json_webhook",
    name="JSON Webhook Format",
    backend=NotificationBackend.WEBHOOK,
    subject_template="Alert: {severity}",
    body_template="""{
        "alert_type": "{severity}",
        "cluster": "{cluster_id}",
        "metric": "{metric_name}",
        "value": {current_value},
        "threshold": {threshold_value},
        "message": "{message}",
        "timestamp": {timestamp}
    }"""
)
```

## Complete Production Example

```python
import asyncio
from mpreg.federation.federation_alerting import *
from mpreg.federation.performance_metrics import *

async def setup_production_alerting():
    # 1. Create alerting service
    alerting = FederationAlertingService()

    # 2. Set up notification channels
    channels = [
        create_console_channel("dev-console", AlertSeverity.INFO),
        create_slack_channel("dev-slack", "https://hooks.slack.com/dev", AlertSeverity.WARNING),
        create_slack_channel("ops-slack", "https://hooks.slack.com/ops", AlertSeverity.ERROR),
        create_webhook_channel("pagerduty", "https://events.pagerduty.com/v2/enqueue",
                             {"Authorization": "Token token=your-pd-token"}, AlertSeverity.CRITICAL)
    ]

    for channel in channels:
        alerting.add_channel(channel)

    # 3. Set up escalation policies
    dev_escalation = create_basic_escalation_policy(
        "dev-escalation", ["dev-slack"], ["ops-slack"]
    )

    critical_escalation = EscalationPolicy(
        policy_id="critical-escalation",
        name="Critical Production Escalation",
        escalation_levels={
            EscalationLevel.IMMEDIATE: ["ops-slack"],
            EscalationLevel.FIRST: ["pagerduty"],
            EscalationLevel.FINAL: ["pagerduty", "ops-slack"]
        }
    )

    alerting.add_escalation_policy(dev_escalation)
    alerting.add_escalation_policy(critical_escalation)

    # 4. Set up intelligent routing
    routing_rules = [
        # Development clusters - low priority, basic escalation
        AlertRoutingRule(
            rule_id="dev-routing",
            name="Development Alerts",
            conditions={"cluster_pattern": "dev-.*"},
            target_channels=["dev-console", "dev-slack"],
            escalation_policy_id="dev-escalation",
            priority=100
        ),

        # Production warnings - ops team
        AlertRoutingRule(
            rule_id="prod-warnings",
            name="Production Warnings",
            conditions={
                "cluster_pattern": "prod-.*",
                "severity": ["warning", "error"]
            },
            target_channels=["ops-slack"],
            priority=20
        ),

        # Production critical - immediate escalation
        AlertRoutingRule(
            rule_id="prod-critical",
            name="Production Critical",
            conditions={
                "cluster_pattern": "prod-.*",
                "severity": ["critical"]
            },
            target_channels=["ops-slack"],
            escalation_policy_id="critical-escalation",
            priority=10
        ),

        # Database-specific alerts
        AlertRoutingRule(
            rule_id="database-alerts",
            name="Database Alerts",
            conditions={
                "cluster_pattern": ".*-db-.*",
                "metric_name": ["health_score", "error_rate", "latency"]
            },
            target_channels=["ops-slack", "pagerduty"],
            priority=5
        )
    ]

    for rule in routing_rules:
        alerting.add_routing_rule(rule)

    # 5. Set up performance metrics with alerting
    metrics = create_performance_metrics_service(
        collection_interval=30.0,
        custom_thresholds=create_production_thresholds()
    )

    # Connect metrics to alerting
    async def alert_handler(alert):
        await alerting.process_alert(alert)

        # Optional: Log alert for debugging
        print(f"Alert processed: {alert.alert_id} - {alert.message}")

    metrics.add_alert_callback(alert_handler)

    # 6. Start the system
    await metrics.start_collection()

    print("🚀 Production alerting system started!")
    print(f"📊 Metrics collection: {metrics.collection_interval}s interval")
    print(f"📢 Notification channels: {len(alerting.channels)}")
    print(f"🔀 Routing rules: {len(alerting.routing_rules)}")
    print(f"📈 Escalation policies: {len(alerting.escalation_policies)}")

    return alerting, metrics

# Run the setup
# alerting, metrics = await setup_production_alerting()
```

## Monitoring & Statistics

### Get Alerting Statistics

```python
# Get comprehensive alerting statistics
stats = alerting.get_alerting_statistics()

print(f"Active alerts: {stats['active_alerts']}")
print(f"Success rate: {stats['delivery_stats']['success_rate_percent']:.1f}%")
print(f"Average latency: {stats['delivery_stats']['average_latency_ms']:.1f}ms")
print(f"Rate limited: {stats['delivery_stats']['rate_limited']}")
```

### Manual Alert Management

```python
# Process a manual alert
from mpreg.federation.performance_metrics import PerformanceAlert

manual_alert = PerformanceAlert(
    alert_id="manual-001",
    cluster_id="prod-web-01",
    severity=AlertSeverity.WARNING,
    metric_name="cpu_usage",
    current_value=85.0,
    threshold_value=80.0,
    message="High CPU usage detected"
)

await alerting.process_alert(manual_alert)

# Resolve an alert
alerting.resolve_alert("manual-001")
```

### Check Active Alerts

```python
# Get all active alerts
active_alerts = alerting.get_active_alerts()
for alert in active_alerts:
    print(f"{alert.severity.value}: {alert.cluster_id} - {alert.message}")

# Check aggregated alerts
for agg_id, agg_alert in alerting.aggregated_alerts.items():
    print(f"Aggregated: {agg_alert.alert_count} alerts for {agg_alert.common_metric}")
```

## Best Practices

### 1. Start Simple

Begin with console notifications and basic routing, then add complexity:

```python
# Development setup
alerting = FederationAlertingService()
alerting.add_channel(create_console_channel("dev"))
alerting.add_routing_rule(create_severity_routing_rule("all", ["warning", "error", "critical"], ["dev"]))
```

### 2. Use Appropriate Severities

- `INFO`: Informational events, low-priority
- `WARNING`: Issues that need attention but aren't urgent
- `ERROR`: Problems that need immediate attention
- `CRITICAL`: System failures requiring immediate action

### 3. Configure Rate Limits

Prevent alert storms by setting appropriate rate limits:

```python
high_volume_channel = NotificationChannel(
    channel_id="metrics-webhook",
    backend=NotificationBackend.WEBHOOK,
    config={"webhook_url": "https://metrics.company.com/alerts"},
    rate_limit_per_minute=30,  # Allow up to 30 alerts per minute
    min_severity=AlertSeverity.WARNING
)
```

### 4. Test Your Setup

Always test your alerting configuration:

```python
# Create test alerts to verify routing
test_alert = PerformanceAlert(
    alert_id="test-routing",
    cluster_id="test-cluster",
    severity=AlertSeverity.WARNING,
    metric_name="test_metric",
    current_value=100.0,
    threshold_value=80.0,
    message="Test alert for routing verification"
)

await alerting.process_alert(test_alert)
# Check that it went to the expected channels
```

### 5. Monitor Your Monitoring

Keep track of alerting system health:

```python
# Regular health check
async def check_alerting_health():
    stats = alerting.get_alerting_statistics()

    if stats['delivery_stats']['success_rate_percent'] < 95:
        print("⚠️ Alert delivery success rate is low!")

    if stats['delivery_stats']['rate_limited'] > 100:
        print("⚠️ Many alerts are being rate limited!")

    if stats['active_alerts'] > 50:
        print("⚠️ High number of active alerts!")

# Run periodically
# await check_alerting_health()
```

## Troubleshooting

### Common Issues

1. **Alerts not routing**: Check routing rule conditions and priorities
2. **High rate limiting**: Increase rate limits or reduce alert frequency
3. **Webhook failures**: Verify URLs and authentication
4. **Missing escalations**: Ensure escalation policies are linked to routing rules

### Debug Mode

```python
# Enable debug logging
import logging
logging.getLogger("mpreg.federation.federation_alerting").setLevel(logging.DEBUG)

# Check delivery history
recent_deliveries = [d for d in alerting.delivery_history if time.time() - d.timestamp < 3600]
for delivery in recent_deliveries:
    print(f"{delivery.channel_id}: {'✅' if delivery.success else '❌'} {delivery.response_message}")
```

This alerting system provides enterprise-grade reliability and flexibility while maintaining simplicity for basic use cases. Start with the Quick Start section and gradually add complexity as needed.
