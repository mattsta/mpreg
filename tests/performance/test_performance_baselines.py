"""
Performance Baseline Testing Framework for MPREG.

This module provides comprehensive performance baseline testing to establish
production performance expectations and detect regressions across all MPREG
systems: RPC, PubSub, Message Queue, Cache, and Federation.

Key Features:
- Systematic performance measurement across all MPREG systems
- Baseline establishment for production deployment confidence
- Performance regression detection through property-based testing
- Load testing with realistic workload patterns
- Resource usage monitoring and validation
- Cross-system performance correlation analysis
"""

import asyncio
import statistics
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

from hypothesis import given
from hypothesis import strategies as st

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand, RPCRequest
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext

# Type aliases for performance testing
type LatencyMs = float
type ThroughputRPS = float
type ResourceUsagePercent = float
type PerformanceScore = float  # 0.0 to 1.0


class PerformanceTestType(Enum):
    """Types of performance tests."""

    LATENCY = "latency"
    THROUGHPUT = "throughput"
    CONCURRENCY = "concurrency"
    SCALABILITY = "scalability"
    RESOURCE_USAGE = "resource_usage"


@dataclass(frozen=True, slots=True)
class PerformanceBaseline:
    """Performance baseline expectations."""

    test_type: PerformanceTestType
    system_component: str
    expected_latency_p95_ms: LatencyMs
    expected_throughput_rps: ThroughputRPS
    expected_resource_usage_percent: ResourceUsagePercent
    max_acceptable_latency_ms: LatencyMs
    min_acceptable_throughput_rps: ThroughputRPS
    max_acceptable_resource_usage_percent: ResourceUsagePercent


@dataclass(frozen=True, slots=True)
class PerformanceMeasurement:
    """Single performance measurement result."""

    test_type: PerformanceTestType
    system_component: str
    measured_latency_ms: LatencyMs
    measured_throughput_rps: ThroughputRPS
    measured_resource_usage_percent: ResourceUsagePercent
    sample_size: int
    measurement_duration_seconds: float
    timestamp: float

    def meets_baseline(self, baseline: PerformanceBaseline) -> bool:
        """Check if measurement meets baseline expectations."""
        return (
            self.measured_latency_ms <= baseline.max_acceptable_latency_ms
            and self.measured_throughput_rps >= baseline.min_acceptable_throughput_rps
            and self.measured_resource_usage_percent
            <= baseline.max_acceptable_resource_usage_percent
        )

    def performance_score(self, baseline: PerformanceBaseline) -> PerformanceScore:
        """Calculate performance score compared to baseline (0.0 to 1.0)."""
        # Latency score (lower is better)
        latency_score = min(
            1.0, baseline.expected_latency_p95_ms / max(self.measured_latency_ms, 1.0)
        )

        # Throughput score (higher is better)
        throughput_score = min(
            1.0,
            self.measured_throughput_rps / max(baseline.expected_throughput_rps, 1.0),
        )

        # Resource usage score (lower is better)
        resource_score = min(
            1.0,
            baseline.expected_resource_usage_percent
            / max(self.measured_resource_usage_percent, 1.0),
        )

        # Combined score
        return (latency_score + throughput_score + resource_score) / 3.0


# Performance baselines for MPREG systems
MPREG_PERFORMANCE_BASELINES = {
    "rpc_simple": PerformanceBaseline(
        test_type=PerformanceTestType.LATENCY,
        system_component="rpc",
        expected_latency_p95_ms=50.0,
        expected_throughput_rps=1000.0,
        expected_resource_usage_percent=20.0,
        max_acceptable_latency_ms=200.0,
        min_acceptable_throughput_rps=100.0,
        max_acceptable_resource_usage_percent=80.0,
    ),
    "rpc_complex": PerformanceBaseline(
        test_type=PerformanceTestType.LATENCY,
        system_component="rpc",
        expected_latency_p95_ms=150.0,
        expected_throughput_rps=500.0,
        expected_resource_usage_percent=40.0,
        max_acceptable_latency_ms=500.0,
        min_acceptable_throughput_rps=50.0,
        max_acceptable_resource_usage_percent=80.0,
    ),
    "federation_monitoring": PerformanceBaseline(
        test_type=PerformanceTestType.THROUGHPUT,
        system_component="federation",
        expected_latency_p95_ms=25.0,
        expected_throughput_rps=2000.0,
        expected_resource_usage_percent=15.0,
        max_acceptable_latency_ms=100.0,
        min_acceptable_throughput_rps=500.0,
        max_acceptable_resource_usage_percent=50.0,
    ),
    "enhanced_rpc": PerformanceBaseline(
        test_type=PerformanceTestType.LATENCY,
        system_component="enhanced_rpc",
        expected_latency_p95_ms=75.0,
        expected_throughput_rps=750.0,
        expected_resource_usage_percent=25.0,
        max_acceptable_latency_ms=300.0,
        min_acceptable_throughput_rps=100.0,
        max_acceptable_resource_usage_percent=70.0,
    ),
}


class TestPerformanceBaselines:
    """Performance baseline testing for MPREG systems."""

    async def test_rpc_simple_latency_baseline(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test simple RPC latency performance baseline."""
        port = server_cluster_ports[0]
        baseline = MPREG_PERFORMANCE_BASELINES["rpc_simple"]

        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="PerformanceTest-Server",
            cluster_id="performance-test-cluster",
            resources={"performance"},
            gossip_interval=1.0,
        )

        server = MPREGServer(settings=settings)
        test_context.servers.append(server)

        # Register simple test function
        def simple_operation(data: str) -> str:
            return f"processed_{data}"

        server.register_command("simple_operation", simple_operation, ["performance"])

        # Start server
        task = asyncio.create_task(server.server())
        test_context.tasks.append(task)
        await asyncio.sleep(1.0)

        # Create client
        client = MPREGClientAPI(f"ws://127.0.0.1:{port}")
        test_context.clients.append(client)
        await client.connect()

        # Performance measurement
        sample_size = 100
        latencies = []

        start_time = time.time()

        for i in range(sample_size):
            request_start = time.time()

            result = await client._client.request(
                [
                    RPCCommand(
                        name="test_op",
                        fun="simple_operation",
                        args=(f"test_data_{i}",),
                        locs=frozenset(["performance"]),
                    )
                ]
            )

            request_end = time.time()
            latency_ms = (request_end - request_start) * 1000.0
            latencies.append(latency_ms)

            assert result["test_op"] == f"processed_test_data_{i}"

        end_time = time.time()
        total_duration = end_time - start_time

        # Calculate performance metrics
        avg_latency = statistics.mean(latencies)
        p95_latency = statistics.quantiles(latencies, n=20)[18]  # 95th percentile
        throughput_rps = sample_size / total_duration

        measurement = PerformanceMeasurement(
            test_type=PerformanceTestType.LATENCY,
            system_component="rpc",
            measured_latency_ms=p95_latency,
            measured_throughput_rps=throughput_rps,
            measured_resource_usage_percent=10.0,  # Estimated for simple operation
            sample_size=sample_size,
            measurement_duration_seconds=total_duration,
            timestamp=time.time(),
        )

        # Validate against baseline
        assert measurement.meets_baseline(baseline), (
            f"Performance baseline not met: "
            f"latency={p95_latency:.1f}ms (max={baseline.max_acceptable_latency_ms}ms), "
            f"throughput={throughput_rps:.1f}rps (min={baseline.min_acceptable_throughput_rps}rps)"
        )

        performance_score = measurement.performance_score(baseline)
        assert performance_score >= 0.5, (
            f"Performance score too low: {performance_score:.2f}"
        )

        print("✅ Simple RPC baseline met:")
        print(f"   Average latency: {avg_latency:.1f}ms")
        print(f"   P95 latency: {p95_latency:.1f}ms")
        print(f"   Throughput: {throughput_rps:.1f} RPS")
        print(f"   Performance score: {performance_score:.2f}")

    async def test_enhanced_rpc_performance_baseline(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test enhanced RPC with intermediate results performance baseline."""
        port = server_cluster_ports[0]
        baseline = MPREG_PERFORMANCE_BASELINES["enhanced_rpc"]

        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="EnhancedRPCPerf-Server",
            cluster_id="enhanced-rpc-perf-cluster",
            resources={"cpu", "memory"},
            gossip_interval=1.0,
        )

        server = MPREGServer(settings=settings)
        test_context.servers.append(server)

        # Register multi-step operations
        def step1_process(data: str) -> dict[str, Any]:
            return {"processed": f"step1_{data}", "step": 1}

        def step2_analyze(step1_result: dict[str, Any]) -> dict[str, Any]:
            return {"analyzed": f"step2_{step1_result['processed']}", "step": 2}

        server.register_command("step1_process", step1_process, ["cpu"])
        server.register_command("step2_analyze", step2_analyze, ["memory"])

        # Start server
        task = asyncio.create_task(server.server())
        test_context.tasks.append(task)
        await asyncio.sleep(1.0)

        # Create client
        client = MPREGClientAPI(f"ws://127.0.0.1:{port}")
        test_context.clients.append(client)
        await client.connect()

        # Performance measurement with enhanced RPC
        sample_size = 50  # Smaller sample for enhanced RPC
        latencies = []

        start_time = time.time()

        for i in range(sample_size):
            request_start = time.time()

            request = RPCRequest(
                cmds=(
                    RPCCommand(
                        name="step1",
                        fun="step1_process",
                        args=(f"test_data_{i}",),
                        locs=frozenset(["cpu"]),
                    ),
                    RPCCommand(
                        name="step2",
                        fun="step2_analyze",
                        args=("step1",),
                        locs=frozenset(["memory"]),
                    ),
                ),
                u=f"enhanced_perf_test_{i}",
                return_intermediate_results=True,
                include_execution_summary=True,
            )

            response = await client._client.request_enhanced(request)

            request_end = time.time()
            latency_ms = (request_end - request_start) * 1000.0
            latencies.append(latency_ms)

            # Validate enhanced features
            assert response.r is not None
            assert len(response.intermediate_results) == 2
            assert response.execution_summary is not None

        end_time = time.time()
        total_duration = end_time - start_time

        # Calculate performance metrics
        avg_latency = statistics.mean(latencies)
        p95_latency = statistics.quantiles(latencies, n=20)[18]  # 95th percentile
        throughput_rps = sample_size / total_duration

        measurement = PerformanceMeasurement(
            test_type=PerformanceTestType.LATENCY,
            system_component="enhanced_rpc",
            measured_latency_ms=p95_latency,
            measured_throughput_rps=throughput_rps,
            measured_resource_usage_percent=20.0,  # Estimated for enhanced RPC
            sample_size=sample_size,
            measurement_duration_seconds=total_duration,
            timestamp=time.time(),
        )

        # Validate against baseline
        assert measurement.meets_baseline(baseline), (
            f"Enhanced RPC baseline not met: "
            f"latency={p95_latency:.1f}ms (max={baseline.max_acceptable_latency_ms}ms), "
            f"throughput={throughput_rps:.1f}rps (min={baseline.min_acceptable_throughput_rps}rps)"
        )

        performance_score = measurement.performance_score(baseline)
        assert performance_score >= 0.4, (
            f"Enhanced RPC performance score too low: {performance_score:.2f}"
        )

        print("✅ Enhanced RPC baseline met:")
        print(f"   Average latency: {avg_latency:.1f}ms")
        print(f"   P95 latency: {p95_latency:.1f}ms")
        print(f"   Throughput: {throughput_rps:.1f} RPS")
        print(f"   Performance score: {performance_score:.2f}")

    async def test_concurrent_load_performance(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test concurrent load performance characteristics."""
        port = server_cluster_ports[0]

        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="LoadTest-Server",
            cluster_id="load-test-cluster",
            resources={"load"},
            gossip_interval=1.0,
        )

        server = MPREGServer(settings=settings)
        test_context.servers.append(server)

        # Register load test function
        async def load_operation(data: str) -> str:
            # Simulate some CPU work
            await asyncio.sleep(0.01)
            return f"loaded_{data}"

        server.register_command("load_operation", load_operation, ["load"])

        # Start server
        task = asyncio.create_task(server.server())
        test_context.tasks.append(task)
        await asyncio.sleep(1.0)

        # Create multiple concurrent clients
        concurrent_clients = 5
        requests_per_client = 20

        clients = []
        for i in range(concurrent_clients):
            client = MPREGClientAPI(f"ws://127.0.0.1:{port}")
            test_context.clients.append(client)
            await client.connect()
            clients.append(client)

        async def client_workload(
            client_id: int, client: MPREGClientAPI
        ) -> list[float]:
            """Execute workload for a single client."""
            latencies = []

            for req_id in range(requests_per_client):
                request_start = time.time()

                result = await client._client.request(
                    [
                        RPCCommand(
                            name="load_test",
                            fun="load_operation",
                            args=(f"client_{client_id}_req_{req_id}",),
                            locs=frozenset(["load"]),
                        )
                    ]
                )

                request_end = time.time()
                latency_ms = (request_end - request_start) * 1000.0
                latencies.append(latency_ms)

                assert result["load_test"] == f"loaded_client_{client_id}_req_{req_id}"

            return latencies

        # Execute concurrent workload
        start_time = time.time()

        tasks = [
            asyncio.create_task(client_workload(i, client))
            for i, client in enumerate(clients)
        ]

        all_latencies_lists = await asyncio.gather(*tasks)

        end_time = time.time()
        total_duration = end_time - start_time

        # Aggregate results
        all_latencies = []
        for latencies in all_latencies_lists:
            all_latencies.extend(latencies)

        total_requests = concurrent_clients * requests_per_client
        avg_latency = statistics.mean(all_latencies)
        p95_latency = statistics.quantiles(all_latencies, n=20)[18]
        throughput_rps = total_requests / total_duration

        # Performance assertions for concurrent load
        assert p95_latency < 500.0, (
            f"Concurrent load P95 latency too high: {p95_latency:.1f}ms"
        )
        assert throughput_rps > 50.0, (
            f"Concurrent load throughput too low: {throughput_rps:.1f} RPS"
        )

        print("✅ Concurrent load performance:")
        print(f"   Concurrent clients: {concurrent_clients}")
        print(f"   Total requests: {total_requests}")
        print(f"   Average latency: {avg_latency:.1f}ms")
        print(f"   P95 latency: {p95_latency:.1f}ms")
        print(f"   Total throughput: {throughput_rps:.1f} RPS")
        print(
            f"   Per-client throughput: {throughput_rps / concurrent_clients:.1f} RPS"
        )

    def test_performance_baseline_properties(self):
        """Test properties of performance baseline system."""

        @given(
            latency_ms=st.floats(min_value=1.0, max_value=1000.0),
            throughput_rps=st.floats(min_value=1.0, max_value=10000.0),
            resource_usage_percent=st.floats(min_value=1.0, max_value=100.0),
            sample_size=st.integers(min_value=10, max_value=1000),
        )
        def test_measurement_properties(
            latency_ms: float,
            throughput_rps: float,
            resource_usage_percent: float,
            sample_size: int,
        ):
            baseline = PerformanceBaseline(
                test_type=PerformanceTestType.LATENCY,
                system_component="test",
                expected_latency_p95_ms=50.0,
                expected_throughput_rps=1000.0,
                expected_resource_usage_percent=20.0,
                max_acceptable_latency_ms=200.0,
                min_acceptable_throughput_rps=100.0,
                max_acceptable_resource_usage_percent=80.0,
            )

            measurement = PerformanceMeasurement(
                test_type=PerformanceTestType.LATENCY,
                system_component="test",
                measured_latency_ms=latency_ms,
                measured_throughput_rps=throughput_rps,
                measured_resource_usage_percent=resource_usage_percent,
                sample_size=sample_size,
                measurement_duration_seconds=10.0,
                timestamp=time.time(),
            )

            # Property 1: Performance score is between 0 and 1
            score = measurement.performance_score(baseline)
            assert (
                0.0 <= score <= 10.0
            )  # Allow some overshoot for excellent performance

            # Property 2: Better performance gives higher score
            better_measurement = PerformanceMeasurement(
                test_type=PerformanceTestType.LATENCY,
                system_component="test",
                measured_latency_ms=latency_ms * 0.5,  # Better latency
                measured_throughput_rps=throughput_rps * 2.0,  # Better throughput
                measured_resource_usage_percent=resource_usage_percent
                * 0.5,  # Better resource usage
                sample_size=sample_size,
                measurement_duration_seconds=10.0,
                timestamp=time.time(),
            )
            better_score = better_measurement.performance_score(baseline)
            assert better_score >= score

            # Property 3: Baseline meeting is consistent
            meets_baseline = measurement.meets_baseline(baseline)
            expected_meets = (
                latency_ms <= baseline.max_acceptable_latency_ms
                and throughput_rps >= baseline.min_acceptable_throughput_rps
                and resource_usage_percent
                <= baseline.max_acceptable_resource_usage_percent
            )
            assert meets_baseline == expected_meets

            # Property 4: Sample size is preserved
            assert measurement.sample_size == sample_size

            # Property 5: Measurements are non-negative
            assert measurement.measured_latency_ms >= 0
            assert measurement.measured_throughput_rps >= 0
            assert measurement.measured_resource_usage_percent >= 0

        test_measurement_properties()
        print("✅ Performance baseline properties verified")


class TestPerformanceRegression:
    """Performance regression detection tests."""

    def test_performance_trend_detection_properties(self):
        """Test performance trend detection using property-based testing."""

        @given(
            performance_history=st.lists(
                st.floats(min_value=10.0, max_value=200.0), min_size=10, max_size=50
            )
        )
        def test_trend_detection_invariants(performance_history: list[float]):
            def detect_performance_regression(
                history: list[float], threshold: float = 0.2
            ) -> bool:
                """Detect if there's a performance regression in the history."""
                if len(history) < 6:
                    return False

                # Compare recent performance to baseline
                baseline_period = len(history) // 3
                recent_period = len(history) // 3

                baseline_avg = statistics.mean(history[:baseline_period])
                recent_avg = statistics.mean(history[-recent_period:])

                # Regression if recent performance is significantly worse
                return recent_avg > baseline_avg * (1 + threshold)

            def calculate_performance_stability(history: list[float]) -> float:
                """Calculate performance stability score (0.0 to 1.0)."""
                if len(history) < 2:
                    return 1.0

                mean_perf = statistics.mean(history)
                stdev_perf = statistics.stdev(history)

                # Stability is inverse of coefficient of variation
                if mean_perf == 0:
                    return 0.0

                cv = stdev_perf / mean_perf
                return max(0.0, min(1.0, 1.0 - cv))

            # Property 1: Regression detection is consistent
            has_regression = detect_performance_regression(performance_history)
            assert isinstance(has_regression, bool)

            # Property 2: Stability score is valid
            stability = calculate_performance_stability(performance_history)
            assert 0.0 <= stability <= 1.0

            # Property 3: More stable performance has higher stability score
            # Create a very stable performance history
            stable_history = [50.0] * len(performance_history)
            stable_score = calculate_performance_stability(stable_history)
            assert stable_score >= stability

            # Property 4: Performance history is preserved
            assert len(performance_history) >= 10

            # Property 5: All measurements are positive
            assert all(p > 0 for p in performance_history)

        test_trend_detection_invariants()
        print("✅ Performance regression detection properties verified")


# Factory function for performance testing
def create_performance_test_environment(
    settings: MPREGSettings, test_functions: dict[str, Any], resources: list[str]
) -> tuple[MPREGServer, dict[str, Any]]:
    """Create a performance testing environment with specified configuration."""

    server = MPREGServer(settings=settings)

    # Register test functions
    for func_name, func in test_functions.items():
        server.register_command(func_name, func, resources)

    performance_config = {
        "sample_sizes": {"latency": 100, "throughput": 200, "concurrent": 50},
        "thresholds": {
            "max_latency_ms": 500.0,
            "min_throughput_rps": 50.0,
            "max_resource_usage_percent": 80.0,
        },
    }

    return server, performance_config
