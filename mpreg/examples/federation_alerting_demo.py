#!/usr/bin/env python3
"""
Federation Alerting System Demo

This example demonstrates how to set up and use the federation alerting system
with performance metrics integration. It shows:

1. Setting up notification channels
2. Configuring routing rules and escalation policies
3. Integrating with performance metrics
4. Simulating alerts and monitoring responses

Run this example to see the alerting system in action!
"""

import asyncio
import time

from mpreg.federation.federation_alerting import (
    AlertRoutingRule,
    AlertSeverity,
    EscalationLevel,
    EscalationPolicy,
    FederationAlertingService,
    NotificationBackend,
    NotificationChannel,
    create_basic_escalation_policy,
)
from mpreg.federation.performance_metrics import (
    ClusterMetrics,
    PerformanceAlert,
    PerformanceMetricsService,
    create_performance_metrics_service,
    create_production_thresholds,
)

class DemoNotificationBackend:
    """Demo notification backend that logs to console with colors."""

    def __init__(self, name: str):
        self.name = name
        self.sent_count = 0

    async def send_notification(self, alert, channel, template=None):
        """Send a colorized demo notification."""
        self.sent_count += 1

        # Color codes for different severities
        colors = {
            AlertSeverity.INFO: "\033[94m",  # Blue
            AlertSeverity.WARNING: "\033[93m",  # Yellow
            AlertSeverity.CRITICAL: "\033[91m",  # Red
            AlertSeverity.CRITICAL: "\033[95m",  # Magenta
        }
        reset = "\033[0m"

        color = colors.get(alert.severity, "")

        print(f"\n{color}📢 {self.name.upper()} NOTIFICATION #{self.sent_count}{reset}")
        print(f"{color}┌─ Channel: {channel.channel_id}{reset}")
        print(f"{color}├─ Severity: {alert.severity.value.upper()}{reset}")
        print(f"{color}├─ Cluster: {alert.cluster_id}{reset}")
        print(f"{color}├─ Metric: {alert.metric_name}{reset}")
        print(
            f"{color}├─ Value: {alert.current_value} (threshold: {alert.threshold_value}){reset}"
        )
        print(f"{color}└─ Message: {alert.message}{reset}")

        from mpreg.federation.federation_alerting import NotificationDelivery

        return NotificationDelivery(
            delivery_id=f"demo_{int(time.time())}_{channel.channel_id}",
            channel_id=channel.channel_id,
            alert_id=alert.alert_id,
            backend=NotificationBackend.WEBHOOK,  # Using webhook as example
            success=True,
            timestamp=time.time(),
            response_code=200,
            response_message=f"Demo notification sent via {self.name}",
        )

    async def send_aggregated_notification(
        self, aggregated_alert, channel, template=None
    ):
        """Send aggregated notification."""
        return await self.send_notification(
            aggregated_alert.last_alert, channel, template
        )

    async def health_check(self):
        """Demo backend is always healthy."""
        return True

async def create_demo_alerting_service() -> FederationAlertingService:
    """Create a demo alerting service with multiple notification channels."""
    print("🚀 Setting up Federation Alerting Service...")

    alerting = FederationAlertingService()

    # Create demo backend
    demo_backend = DemoNotificationBackend("demo")
    alerting.backends[NotificationBackend.WEBHOOK] = demo_backend

    # 1. Set up notification channels
    print("📢 Adding notification channels...")

    channels = [
        NotificationChannel(
            channel_id="dev-console",
            backend=NotificationBackend.CONSOLE,
            config={},
            min_severity=AlertSeverity.INFO,
            template_name="default_console",
        ),
        NotificationChannel(
            channel_id="dev-slack",
            backend=NotificationBackend.WEBHOOK,  # Using demo backend
            config={"webhook_url": "https://hooks.slack.com/demo"},
            min_severity=AlertSeverity.WARNING,
            rate_limit_per_minute=5,
        ),
        NotificationChannel(
            channel_id="ops-team",
            backend=NotificationBackend.WEBHOOK,
            config={"webhook_url": "https://ops.company.com/alerts"},
            min_severity=AlertSeverity.CRITICAL,
            rate_limit_per_minute=10,
        ),
        NotificationChannel(
            channel_id="oncall-pager",
            backend=NotificationBackend.WEBHOOK,
            config={"webhook_url": "https://pager.company.com/critical"},
            min_severity=AlertSeverity.CRITICAL,
            rate_limit_per_minute=20,
        ),
    ]

    for channel in channels:
        alerting.add_channel(channel)
        print(f"  ✅ Added {channel.channel_id} ({channel.backend.value})")

    # 2. Set up escalation policies
    print("📈 Setting up escalation policies...")

    # Development escalation: dev team → ops team
    dev_policy = create_basic_escalation_policy(
        "dev-escalation",
        immediate_channels=["dev-console"],
        escalated_channels=["dev-slack"],
    )
    alerting.add_escalation_policy(dev_policy)
    print("  ✅ Added development escalation policy")

    # Production escalation: ops → oncall → multiple channels
    prod_policy = EscalationPolicy(
        policy_id="prod-escalation",
        name="Production Critical Escalation",
        escalation_levels={
            EscalationLevel.IMMEDIATE: ["ops-team"],
            EscalationLevel.FIRST: ["oncall-pager"],
            EscalationLevel.SECOND: ["oncall-pager", "ops-team"],
            EscalationLevel.FINAL: ["oncall-pager", "ops-team", "dev-slack"],
        },
        default_channels=["ops-team"],
    )
    alerting.add_escalation_policy(prod_policy)
    print("  ✅ Added production escalation policy")

    # 3. Set up intelligent routing rules
    print("🔀 Configuring routing rules...")

    routing_rules = [
        # Development clusters - route to dev channels with basic escalation
        AlertRoutingRule(
            rule_id="dev-routing",
            name="Development Cluster Routing",
            conditions={"cluster_pattern": "dev-.*"},
            target_channels=["dev-console", "dev-slack"],
            escalation_policy_id="dev-escalation",
            priority=100,
        ),
        # Production warnings - ops team only
        AlertRoutingRule(
            rule_id="prod-warnings",
            name="Production Warning Alerts",
            conditions={"cluster_pattern": "prod-.*", "severity": ["warning"]},
            target_channels=["ops-team"],
            priority=30,
        ),
        # Production errors - ops team with possible escalation
        AlertRoutingRule(
            rule_id="prod-errors",
            name="Production Error Alerts",
            conditions={"cluster_pattern": "prod-.*", "severity": ["error"]},
            target_channels=["ops-team"],
            priority=20,
        ),
        # Production critical - immediate escalation
        AlertRoutingRule(
            rule_id="prod-critical",
            name="Production Critical Alerts",
            conditions={"cluster_pattern": "prod-.*", "severity": ["critical"]},
            target_channels=["ops-team"],
            escalation_policy_id="prod-escalation",
            priority=10,
        ),
        # Database clusters - special handling
        AlertRoutingRule(
            rule_id="database-alerts",
            name="Database Cluster Alerts",
            conditions={
                "cluster_pattern": ".*-db$",
                "metric_name": ["health_score", "error_rate"],
            },
            target_channels=["ops-team", "oncall-pager"],
            priority=5,  # Highest priority
        ),
    ]

    for rule in routing_rules:
        alerting.add_routing_rule(rule)
        print(f"  ✅ Added {rule.name} (priority: {rule.priority})")

    print("\n✨ Alerting service configured with:")
    print(f"   📢 {len(alerting.channels)} notification channels")
    print(f"   🔀 {len(alerting.routing_rules)} routing rules")
    print(f"   📈 {len(alerting.escalation_policies)} escalation policies")

    return alerting

async def create_demo_metrics_service() -> PerformanceMetricsService:
    """Create a demo performance metrics service."""
    print("\n📊 Setting up Performance Metrics Service...")

    # Use production thresholds for realistic alerts
    thresholds = create_production_thresholds()

    metrics = create_performance_metrics_service(
        collection_interval=10.0,  # Fast collection for demo
        retention_hours=1,
        custom_thresholds=thresholds,
    )

    print("✅ Metrics service configured:")
    print(f"   ⏱️  Collection interval: {metrics.collection_interval}s")
    print(f"   🎯 Latency warning: {thresholds.latency_warning}ms")
    print(f"   🎯 Health score warning: {thresholds.health_score_warning}")
    print(f"   🎯 Error rate warning: {thresholds.error_rate_warning}%")

    return metrics

async def simulate_cluster_metrics(
    metrics_service: PerformanceMetricsService, scenario: str = "normal"
) -> None:
    """Simulate cluster metrics for different scenarios."""
    print(f"\n🎭 Simulating '{scenario}' scenario...")

    if scenario == "normal":
        # Normal operation - no alerts
        cluster_data = [
            ClusterMetrics(
                cluster_id="prod-web-01",
                cluster_name="Production Web 1",
                region="us-west-2",
                avg_latency_ms=45.0,
                throughput_rps=120.0,
                health_score=95.0,
                error_rate_percent=0.2,
            ),
            ClusterMetrics(
                cluster_id="dev-api-01",
                cluster_name="Development API 1",
                region="us-east-1",
                avg_latency_ms=80.0,
                throughput_rps=25.0,
                health_score=88.0,
                error_rate_percent=1.2,
            ),
        ]

    elif scenario == "degraded":
        # Some warnings and errors
        cluster_data = [
            ClusterMetrics(
                cluster_id="prod-web-01",
                cluster_name="Production Web 1",
                region="us-west-2",
                avg_latency_ms=180.0,
                throughput_rps=80.0,
                health_score=75.0,
                error_rate_percent=1.8,  # Warning levels
            ),
            ClusterMetrics(
                cluster_id="prod-db",
                cluster_name="Production Database",
                region="us-west-2",
                avg_latency_ms=220.0,
                throughput_rps=45.0,
                health_score=65.0,
                error_rate_percent=3.2,  # Error levels
            ),
            ClusterMetrics(
                cluster_id="dev-test-01",
                cluster_name="Development Test 1",
                region="us-east-1",
                avg_latency_ms=150.0,
                throughput_rps=15.0,
                health_score=82.0,
                error_rate_percent=2.5,
            ),
        ]

    elif scenario == "critical":
        # Critical system failures
        cluster_data = [
            ClusterMetrics(
                cluster_id="prod-web-01",
                cluster_name="Production Web 1",
                region="us-west-2",
                avg_latency_ms=520.0,
                throughput_rps=8.0,
                health_score=45.0,
                error_rate_percent=8.5,  # Critical levels
            ),
            ClusterMetrics(
                cluster_id="prod-db",
                cluster_name="Production Database",
                region="us-west-2",
                avg_latency_ms=780.0,
                throughput_rps=3.0,
                health_score=25.0,
                error_rate_percent=12.0,  # Critical
            ),
        ]

    else:
        raise ValueError(f"Unknown scenario: {scenario}")

    # Update metrics in the service
    for cluster in cluster_data:
        await metrics_service._update_cluster_metrics(cluster)
        print(
            f"  📊 {cluster.cluster_id}: latency={cluster.avg_latency_ms}ms, health={cluster.health_score}"
        )

    # Generate federation-wide metrics
    await metrics_service._generate_federation_metrics()

    # Check thresholds and trigger alerts
    await metrics_service._check_thresholds()

    print(f"  ✅ Scenario '{scenario}' metrics updated")

async def demonstrate_alerting_workflow():
    """Demonstrate the complete alerting workflow."""
    print("🎪 Federation Alerting System Demo")
    print("=" * 50)

    # 1. Set up services
    alerting = await create_demo_alerting_service()
    metrics = await create_demo_metrics_service()

    # 2. Connect metrics to alerting
    print("\n🔗 Connecting metrics to alerting...")

    def alert_handler(alert: PerformanceAlert):
        """Handle alerts from metrics service."""
        print(f"\n⚡ Alert triggered: {alert.alert_id}")
        print(f"   📍 Cluster: {alert.cluster_id}")
        print(f"   🚨 Severity: {alert.severity.value}")
        print(f"   📊 Metric: {alert.metric_name} = {alert.current_value}")

        # Process through alerting system (async)
        asyncio.create_task(alerting.process_alert(alert))

    metrics.add_alert_callback(alert_handler)
    print("✅ Alert callback registered")

    # 3. Demonstrate different scenarios
    scenarios = [
        ("normal", "All systems operating normally"),
        ("degraded", "Some performance issues detected"),
        ("critical", "Critical system failures!"),
    ]

    for scenario, description in scenarios:
        print("\n" + "=" * 60)
        print(f"🎬 SCENARIO: {scenario.upper()} - {description}")
        print("=" * 60)

        await simulate_cluster_metrics(metrics, scenario)

        # Wait a moment for alerts to process
        await asyncio.sleep(0.1)

        # Show alerting statistics
        stats = alerting.get_alerting_statistics()
        print("\n📈 Alerting Statistics:")
        print(f"   🔔 Active alerts: {stats['active_alerts']}")
        print(f"   📤 Total sent: {stats['delivery_stats']['total_sent']}")
        print(
            f"   ✅ Success rate: {stats['delivery_stats']['success_rate_percent']:.1f}%"
        )
        print(
            f"   ⚡ Avg latency: {stats['delivery_stats']['average_latency_ms']:.1f}ms"
        )

        if stats["active_alerts"] > 0:
            print("\n🚨 Active Alerts:")
            for alert in alerting.active_alerts.values():
                print(
                    f"   • {alert.severity.value}: {alert.cluster_id} - {alert.message}"
                )

        if scenario != "critical":  # Don't wait on last scenario
            print("\n⏳ Waiting 3 seconds before next scenario...")
            await asyncio.sleep(3)

    # 4. Demonstrate alert resolution
    print("\n" + "=" * 60)
    print("🔧 RESOLVING ALERTS")
    print("=" * 60)

    active_alerts = list(alerting.active_alerts.values())
    if active_alerts:
        for alert in active_alerts[:2]:  # Resolve first 2 alerts
            resolved = alerting.resolve_alert(alert.alert_id)
            if resolved:
                print(f"✅ Resolved alert: {alert.alert_id}")
            else:
                print(f"❌ Failed to resolve alert: {alert.alert_id}")

    # 5. Final statistics
    print("\n" + "=" * 60)
    print("📊 FINAL STATISTICS")
    print("=" * 60)

    final_stats = alerting.get_alerting_statistics()
    print(f"Notification Channels: {final_stats['configured_channels']}")
    print(f"Routing Rules: {final_stats['routing_rules']}")
    print(f"Escalation Policies: {final_stats['escalation_policies']}")
    print(f"Total Notifications Sent: {final_stats['delivery_stats']['total_sent']}")
    print(f"Success Rate: {final_stats['delivery_stats']['success_rate_percent']:.1f}%")
    print(f"Active Alerts Remaining: {final_stats['active_alerts']}")

    print("\n🎉 Demo completed successfully!")
    print("   💡 Check the output above to see how alerts were routed")
    print("   💡 Try modifying the thresholds or routing rules to experiment")

async def interactive_demo():
    """Run an interactive demo where users can trigger custom alerts."""
    print("🎮 Interactive Alerting Demo")
    print("=" * 40)

    alerting = await create_demo_alerting_service()

    print("\n📝 You can now create custom alerts!")
    print("   Example: cluster=prod-web-01, severity=critical, metric=health_score")

    while True:
        try:
            print("\n🔗 Enter alert details (or 'quit' to exit):")
            cluster_id = input("  Cluster ID: ").strip()
            if cluster_id.lower() == "quit":
                break

            severity_input = (
                input("  Severity (info/warning/error/critical): ").strip().lower()
            )
            severity_map = {
                "info": AlertSeverity.INFO,
                "warning": AlertSeverity.WARNING,
                "error": AlertSeverity.CRITICAL,
                "critical": AlertSeverity.CRITICAL,
            }
            severity = severity_map.get(severity_input, AlertSeverity.WARNING)

            metric_name = input("  Metric name: ").strip()
            current_value = float(input("  Current value: ").strip())
            threshold_value = float(input("  Threshold value: ").strip())
            message = input("  Alert message: ").strip()

            # Create and process the alert
            alert = PerformanceAlert(
                alert_id=f"interactive_{int(time.time())}",
                severity=severity,
                metric_name=metric_name,
                current_value=current_value,
                threshold_value=threshold_value,
                cluster_id=cluster_id,
                node_id=None,
                timestamp=time.time(),
                message=message or f"{metric_name} threshold exceeded",
            )

            print("\n⚡ Processing alert...")
            await alerting.process_alert(alert)

            # Show results
            stats = alerting.get_alerting_statistics()
            print(
                f"✅ Alert processed! Total sent: {stats['delivery_stats']['total_sent']}"
            )

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Error: {e}")

    print("\n👋 Interactive demo ended. Thanks for trying the alerting system!")

def main():
    """Main entry point for the demo."""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "interactive":
        asyncio.run(interactive_demo())
    else:
        asyncio.run(demonstrate_alerting_workflow())

if __name__ == "__main__":
    main()
