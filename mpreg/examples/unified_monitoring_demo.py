#!/usr/bin/env python3
"""
Unified Monitoring and Observability Demo.

This example demonstrates MPREG's unified monitoring system with ULID-based
end-to-end tracking across multiple systems. Shows cross-system correlation,
real-time performance analytics, and event timeline reconstruction.
"""

import asyncio
import time
from dataclasses import dataclass

from mpreg.core.monitoring.unified_monitoring import (
    EventType,
    HealthStatus,
    SystemPerformanceMetrics,
    SystemType,
    UnifiedSystemMonitor,
    create_unified_system_monitor,
)


@dataclass
class MockRPCRequest:
    """Mock RPC request for demo."""

    request_id: str
    command: str
    args: dict[str, str]


async def simulate_rpc_workflow(monitor: UnifiedSystemMonitor):
    """Simulate an RPC workflow that triggers multiple systems."""
    print("🚀 Starting RPC workflow simulation...")

    # Simulate RPC request with existing ID
    rpc_request = MockRPCRequest(
        request_id="rpc_user_login_12345",
        command="authenticate_user",
        args={"username": "alice", "password": "secret"},
    )

    # Record RPC start (uses existing request ID as tracking ID)
    tracking_id = await monitor.record_cross_system_event(
        correlation_id=f"workflow_{int(time.time())}",
        event_type=EventType.REQUEST_START,
        source_system=SystemType.RPC,
        existing_object_id=rpc_request.request_id,
        metadata={
            "command": rpc_request.command,
            "username": rpc_request.args["username"],
        },
    )

    print(f"📋 RPC workflow started with tracking ID: {tracking_id}")

    # Simulate RPC processing time
    await asyncio.sleep(0.1)

    # Simulate RPC triggering pub/sub notification
    await monitor.record_cross_system_event(
        correlation_id=f"workflow_{int(time.time())}",
        event_type=EventType.CROSS_SYSTEM_CORRELATION,
        source_system=SystemType.RPC,
        target_system=SystemType.PUBSUB,
        tracking_id=tracking_id,  # Use same tracking ID
        latency_ms=25.5,
        metadata={"event_type": "user_login", "topic": "auth.user.login"},
    )

    print("📢 Published login event to topic system")

    # Simulate pub/sub triggering cache update
    await monitor.record_cross_system_event(
        correlation_id=f"workflow_{int(time.time())}",
        event_type=EventType.CROSS_SYSTEM_CORRELATION,
        source_system=SystemType.PUBSUB,
        target_system=SystemType.CACHE,
        tracking_id=tracking_id,  # Same tracking ID
        latency_ms=15.0,
        metadata={"cache_operation": "update_user_session", "user": "alice"},
    )

    print("💾 Updated user session in cache")

    # Simulate cache triggering queue message
    await monitor.record_cross_system_event(
        correlation_id=f"workflow_{int(time.time())}",
        event_type=EventType.CROSS_SYSTEM_CORRELATION,
        source_system=SystemType.CACHE,
        target_system=SystemType.QUEUE,
        tracking_id=tracking_id,  # Same tracking ID
        latency_ms=8.0,
        metadata={"queue_message": "send_welcome_email", "user": "alice"},
    )

    print("📧 Queued welcome email message")

    # Complete RPC request
    await monitor.record_cross_system_event(
        correlation_id=f"workflow_{int(time.time())}",
        event_type=EventType.REQUEST_COMPLETE,
        source_system=SystemType.RPC,
        tracking_id=tracking_id,  # Same tracking ID
        latency_ms=150.0,  # Total RPC time
        success=True,
        metadata={"result": "authentication_successful"},
    )

    print("✅ RPC workflow completed successfully")
    return tracking_id


async def simulate_monitoring_data(monitor: UnifiedSystemMonitor):
    """Add some mock monitoring system interfaces for demo."""

    class MockSystemMonitor:
        def __init__(self, system_type: SystemType, base_rps: float = 100.0):
            self.system_type = system_type
            self.base_rps = base_rps

        async def get_system_metrics(self) -> SystemPerformanceMetrics:
            return SystemPerformanceMetrics(
                system_type=self.system_type,
                system_name=f"demo_{self.system_type.value}",
                requests_per_second=self.base_rps
                + (time.time() % 50),  # Vary over time
                average_latency_ms=20.0 + (time.time() % 30),  # Vary over time
                p95_latency_ms=45.0,
                p99_latency_ms=85.0,
                error_rate_percent=1.5,
                active_connections=15,
                total_operations_last_hour=int(self.base_rps * 3600),
                last_updated=time.time(),
            )

        async def get_health_status(self):
            # Simulate varying health
            score = 0.85 + (0.1 * (time.time() % 10) / 10)  # 0.85 to 0.95
            status = HealthStatus.HEALTHY if score > 0.9 else HealthStatus.DEGRADED
            return score, status

    # Attach mock monitors
    monitor.rpc_monitor = MockSystemMonitor(SystemType.RPC, 150.0)
    monitor.pubsub_monitor = MockSystemMonitor(SystemType.PUBSUB, 200.0)
    monitor.queue_monitor = MockSystemMonitor(SystemType.QUEUE, 80.0)
    monitor.cache_monitor = MockSystemMonitor(SystemType.CACHE, 300.0)
    monitor.federation_monitor = MockSystemMonitor(SystemType.FEDERATION, 50.0)
    monitor.transport_monitor = MockSystemMonitor(SystemType.TRANSPORT, 500.0)


async def demonstrate_monitoring_capabilities(monitor: UnifiedSystemMonitor):
    """Demonstrate various monitoring capabilities."""
    print("\n📊 Demonstrating monitoring capabilities...")

    # Get unified metrics
    metrics = await monitor.get_unified_metrics()

    print("\n🌐 Overall System Health:")
    print(f"  Health Score: {metrics.overall_health_score:.3f}")
    print(f"  Health Status: {metrics.overall_health_status.value}")
    print(
        f"  Systems: {metrics.systems_healthy} healthy, {metrics.systems_degraded} degraded"
    )
    print(f"  Total RPS: {metrics.total_requests_per_second:.1f}")
    print(f"  Average Latency: {metrics.average_cross_system_latency_ms:.1f}ms")
    print(f"  Active Correlations: {metrics.total_active_correlations}")

    print("\n📈 Cross-System Performance:")
    perf_summary = monitor.get_cross_system_performance_summary()
    for (source, target), perf_data in perf_summary.items():
        print(f"  {source.value} → {target.value}:")
        print(f"    Avg Latency: {perf_data['average_latency_ms']:.1f}ms")
        print(f"    P95 Latency: {perf_data['p95_latency_ms']:.1f}ms")
        print(f"    Total Interactions: {perf_data['total_interactions']}")

    print("\n🔍 Recent Events (last 10):")
    recent_events = monitor.get_recent_events(limit=10)
    for i, event in enumerate(recent_events[-10:], 1):
        target_info = f" → {event.target_system.value}" if event.target_system else ""
        success_icon = "✅" if event.success else "❌"
        print(
            f"  {i}. {success_icon} {event.source_system.value}{target_info} "
            f"({event.event_type.value}) [{event.tracking_id[:12]}...] "
            f"{event.latency_ms:.1f}ms"
        )


async def demonstrate_tracking_timeline(
    monitor: UnifiedSystemMonitor, tracking_id: str
):
    """Demonstrate end-to-end tracking timeline."""
    print(f"\n🔄 End-to-End Tracking Timeline for {tracking_id}:")

    timeline = monitor.get_tracking_timeline(tracking_id)

    total_latency = 0.0
    for i, event in enumerate(timeline, 1):
        target_info = f" → {event.target_system.value}" if event.target_system else ""
        success_icon = "✅" if event.success else "❌"
        timestamp = time.strftime("%H:%M:%S", time.localtime(event.timestamp))

        print(
            f"  {i}. [{timestamp}] {success_icon} {event.source_system.value}{target_info}"
        )
        print(f"     Event: {event.event_type.value}")
        print(f"     Latency: {event.latency_ms:.1f}ms")
        if event.metadata:
            key_metadata = {
                k: v for k, v in list(event.metadata.items())[:2]
            }  # Show first 2
            print(f"     Metadata: {key_metadata}")
        print()

        total_latency += event.latency_ms

    print(
        f"🏁 Total End-to-End Latency: {total_latency:.1f}ms across {len(timeline)} events"
    )


async def demonstrate_system_specific_tracking():
    """Demonstrate system-specific ULID generation."""
    print("\n🔖 System-Specific ULID Tracking Examples:")

    monitor = create_unified_system_monitor()

    # Generate system-specific tracking IDs
    for system_type in SystemType:
        tracking_id = monitor.generate_system_tracking_id(system_type)
        print(f"  {system_type.value:>12}: {tracking_id}")

    # Show custom namespace
    custom_tracking_id = monitor.generate_tracking_id(namespace_prefix="workflow")
    print(f"  {'workflow':>12}: {custom_tracking_id}")

    # Show existing ID preservation
    existing_id = "existing_request_abc123"
    preserved_id = monitor.generate_tracking_id(existing_id)
    print(f"  {'preserved':>12}: {preserved_id}")


async def run_demo():
    """Main async demo function."""
    print("🎯 MPREG Unified Monitoring and Observability Demo")
    print("=" * 60)

    # Create unified system monitor
    monitor = create_unified_system_monitor(
        metrics_collection_interval_ms=2000.0,  # Fast for demo
        health_check_interval_ms=3000.0,
    )

    # Add mock monitoring data
    await simulate_monitoring_data(monitor)

    # Start monitoring
    async with monitor:
        print("📡 Unified monitoring system started...")

        # Wait a moment for initial health checks
        await asyncio.sleep(1.0)

        # Simulate RPC workflow
        workflow_tracking_id = await simulate_rpc_workflow(monitor)

        # Wait a moment for metrics collection
        await asyncio.sleep(2.0)

        # Demonstrate monitoring capabilities
        await demonstrate_monitoring_capabilities(monitor)

        # Show end-to-end tracking
        await demonstrate_tracking_timeline(monitor, workflow_tracking_id)

        # Wait to see more background activity
        print("\n⏳ Collecting more monitoring data...")
        await asyncio.sleep(3.0)

        # Show final metrics
        final_metrics = await monitor.get_unified_metrics()
        print("\n📊 Final Metrics Collection:")
        print(f"  Collection Duration: {final_metrics.collection_duration_ms:.1f}ms")
        print(f"  Total Events in History: {len(monitor.event_history)}")
        print(f"  Total Correlation Chains: {len(monitor.correlation_chains)}")
        print(f"  Total Tracking Timelines: {len(monitor.tracking_id_to_events)}")
        print(f"  Active Tracking IDs: {len(monitor.active_tracking_ids)}")

    print("\n✨ Monitoring system stopped gracefully")

    # Demonstrate system-specific tracking
    await demonstrate_system_specific_tracking()

    print("\n🎉 Demo complete! Unified monitoring provides comprehensive")
    print("    observability across all MPREG systems with stable ULID")
    print("    tracking for end-to-end workflow visibility.")


def main():
    """Run the unified monitoring demo."""
    asyncio.run(run_demo())


if __name__ == "__main__":
    main()
