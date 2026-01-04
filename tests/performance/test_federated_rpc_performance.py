"""
Performance tests for MPREG's federated RPC system.

This module tests the performance characteristics and scalability
of the federated RPC announcement and routing system under load.

Key performance aspects tested:
- Announcement propagation latency and throughput
- Multi-hop routing performance
- Large-scale cluster scalability
- Concurrent execution performance
- Memory usage under load
"""

import asyncio
import gc
import time
from dataclasses import dataclass
from typing import Any

import psutil
import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand, RPCRequest
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext

pytestmark = pytest.mark.slow


class TestFederatedRPCPerformance:
    """Performance tests for federated RPC system."""

    async def test_announcement_propagation_latency(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Measure latency of federated RPC announcement propagation."""
        if len(server_cluster_ports) < 5:
            pytest.skip("Need at least 5 ports for latency test")

        ports = server_cluster_ports[:5]

        # Create auto-discovery gossip cluster (linear chain for auto-discovery test)
        settings_list = []
        for i, port in enumerate(ports):
            # Linear chain for auto-discovery: 0 ← 1 ← 2 ← 3 ← 4
            connect_to = f"ws://127.0.0.1:{ports[i - 1]}" if i > 0 else None

            settings_list.append(
                MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"Latency-Node-{i + 1}",
                    cluster_id="latency-test-cluster",
                    resources={f"latency-{i + 1}"},
                    peers=None,  # Auto-discovery via gossip
                    connect=connect_to,  # Linear chain initial connections
                    advertised_urls=None,
                    gossip_interval=0.5,  # Reasonable gossip interval
                )
            )

        servers = [MPREGServer(settings=s) for s in settings_list]
        test_context.servers.extend(servers)

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.3)

        # Wait for auto-discovery to complete
        await asyncio.sleep(3.0)

        # Measure announcement propagation time
        def test_function(data: str) -> str:
            return f"Latency test: {data}"

        start_time = time.time()
        servers[0].register_command("test_function", test_function, ["latency-1"])

        # Wait for function propagation across auto-discovered cluster
        await asyncio.sleep(5.0)

        client = MPREGClientAPI(f"ws://127.0.0.1:{ports[-1]}")  # Connect to last node
        test_context.clients.append(client)
        await client.connect()

        try:
            result = await client._client.request(
                [
                    RPCCommand(
                        name="latency_test",
                        fun="test_function",
                        args=("propagation_test",),
                        locs=frozenset(["latency-1"]),
                    )
                ]
            )

            end_time = time.time()
            propagation_time = (end_time - start_time) * 1000  # Convert to ms

            success = "latency_test" in result

            print("📊 Announcement propagation performance:")
            print(f"  Chain length: {len(ports)} nodes")
            print(f"  Propagation time: {propagation_time:.1f}ms")
            print(f"  Success: {'✅' if success else '❌'}")

            if success:
                print(f"  Per-hop latency: {propagation_time / (len(ports) - 1):.1f}ms")

            # Performance assertion: Should propagate within reasonable time
            # Auto-discovery + function propagation should complete reasonably quickly
            assert propagation_time < 12000, (
                f"Propagation took {propagation_time:.1f}ms, expected <12000ms"
            )
            assert success, "Function should be reachable after propagation"

        except Exception as e:
            print(f"❌ Latency test failed: {e}")
            pytest.fail(f"Latency test failed: {e}")

    async def test_concurrent_rpc_execution_performance(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test performance under concurrent RPC execution load."""
        if len(server_cluster_ports) < 4:
            pytest.skip("Need at least 4 ports for concurrent execution test")

        ports = server_cluster_ports[:4]

        # Create cluster with different processing capabilities
        settings_list = []
        for i, port in enumerate(ports):
            connect_to = f"ws://127.0.0.1:{ports[0]}" if i > 0 else None

            settings_list.append(
                MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"Concurrent-Node-{i + 1}",
                    cluster_id="concurrent-test-cluster",
                    resources={f"concurrent-{i + 1}"},
                    peers=None,
                    connect=connect_to,
                    advertised_urls=None,
                    gossip_interval=1.0,
                )
            )

        servers = [MPREGServer(settings=s) for s in settings_list]
        test_context.servers.extend(servers)

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.5)

        await asyncio.sleep(3.0)

        # Register functions with different performance characteristics
        async def fast_function(data: str) -> str:
            await asyncio.sleep(0.01)  # 10ms
            return f"Fast: {data}"

        async def medium_function(data: str) -> str:
            await asyncio.sleep(0.05)  # 50ms
            return f"Medium: {data}"

        async def slow_function(data: str) -> str:
            await asyncio.sleep(0.1)  # 100ms
            return f"Slow: {data}"

        def cpu_intensive_function(data: str) -> str:
            # CPU-intensive operation
            result = 0
            for i in range(100000):
                result += i * i
            return f"CPU: {data} -> {result}"

        servers[0].register_command("fast_function", fast_function, ["concurrent-1"])
        servers[1].register_command(
            "medium_function", medium_function, ["concurrent-2"]
        )
        servers[2].register_command("slow_function", slow_function, ["concurrent-3"])
        servers[3].register_command(
            "cpu_intensive_function", cpu_intensive_function, ["concurrent-4"]
        )

        # Allow propagation
        await asyncio.sleep(6.0)

        # Performance test with multiple concurrent clients
        num_concurrent_requests = 20
        clients = []

        for i in range(min(num_concurrent_requests, len(ports))):
            client = MPREGClientAPI(f"ws://127.0.0.1:{ports[i % len(ports)]}")
            clients.append(client)
            test_context.clients.append(client)
            await client.connect()

        @dataclass
        class PerformanceResult:
            client_id: int
            execution_time: float
            success: bool
            summary: Any

        # Create concurrent requests
        async def make_concurrent_request(
            client_id: int, client: MPREGClientAPI
        ) -> PerformanceResult:
            """Make a concurrent RPC request."""
            request = RPCRequest(
                cmds=(
                    RPCCommand(
                        name="fast_result",
                        fun="fast_function",
                        args=(f"concurrent_test_{client_id}",),
                        locs=frozenset(["concurrent-1"]),
                    ),
                    RPCCommand(
                        name="cpu_result",
                        fun="cpu_intensive_function",
                        args=(f"concurrent_test_{client_id}",),
                        locs=frozenset(["concurrent-4"]),
                    ),
                ),
                u=f"concurrent_request_{client_id}",
                return_intermediate_results=True,
                include_execution_summary=True,
            )

            start_time = time.time()
            response = await client._client.request_enhanced(request)
            end_time = time.time()

            return PerformanceResult(
                client_id=client_id,
                execution_time=(end_time - start_time) * 1000,
                success="cpu_result" in response.r,
                summary=response.execution_summary,
            )

        # Execute concurrent requests
        print(f"🚀 Starting {num_concurrent_requests} concurrent RPC requests...")
        start_time = time.time()

        tasks = []
        for i, client in enumerate(clients):
            task = asyncio.create_task(make_concurrent_request(i, client))  # type: ignore[arg-type]
            tasks.append(task)

        # Add more tasks if we have fewer clients than desired requests
        for i in range(len(clients), num_concurrent_requests):
            client = clients[i % len(clients)]
            task = asyncio.create_task(make_concurrent_request(i, client))  # type: ignore[arg-type]
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_time = (time.time() - start_time) * 1000

        # Analyze performance results
        successful_results: list[PerformanceResult] = []
        failed_count = 0

        for r in results:
            if isinstance(r, PerformanceResult) and r.success:
                successful_results.append(r)
            else:
                failed_count += 1

        execution_times: list[float] = [r.execution_time for r in successful_results]

        print("📊 Concurrent execution performance results:")
        print(f"  Total requests: {num_concurrent_requests}")
        print(f"  Successful: {len(successful_results)}")
        print(f"  Failed: {failed_count}")
        print(f"  Total time: {total_time:.1f}ms")
        print(
            f"  Throughput: {len(successful_results) / (total_time / 1000):.1f} requests/sec"
        )

        if execution_times:
            avg_time = sum(execution_times) / len(execution_times)
            min_time = min(execution_times)
            max_time = max(execution_times)

            print(f"  Average request time: {avg_time:.1f}ms")
            print(f"  Min request time: {min_time:.1f}ms")
            print(f"  Max request time: {max_time:.1f}ms")

        # Performance assertions
        success_rate = len(successful_results) / num_concurrent_requests
        assert success_rate >= 0.8, (
            f"Success rate {success_rate:.2f} below threshold 0.8"
        )

        throughput = len(successful_results) / (total_time / 1000)
        assert throughput >= 2.0, (
            f"Throughput {throughput:.1f} req/sec below threshold 2.0"
        )

    async def test_large_scale_cluster_performance(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test performance characteristics of large-scale federated clusters."""
        available_ports = len(server_cluster_ports)
        if available_ports < 8:
            pytest.skip(
                f"Need at least 8 ports for large-scale test, got {available_ports}"
            )

        # Use up to 12 nodes for large-scale test
        num_nodes = min(12, available_ports)
        ports = server_cluster_ports[:num_nodes]

        print(f"🏗️ Creating large-scale cluster with {num_nodes} nodes...")

        # Create mesh-like topology
        settings_list = []
        for i, port in enumerate(ports):
            # Connect each node to 2-3 other nodes for redundancy
            connect_targets = []
            if i > 0:
                connect_targets.append(f"ws://127.0.0.1:{ports[0]}")  # Connect to hub
            if i > 2:
                connect_targets.append(
                    f"ws://127.0.0.1:{ports[i - 2]}"
                )  # Connect to earlier node

            connect_to = connect_targets[0] if connect_targets else None

            settings_list.append(
                MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"Scale-Node-{i + 1}",
                    cluster_id="large-scale-cluster",
                    resources={f"scale-{i + 1}"},
                    peers=None,
                    connect=connect_to,
                    advertised_urls=None,
                    gossip_interval=1.0,
                )
            )

        servers = [MPREGServer(settings=s) for s in settings_list]
        test_context.servers.extend(servers)

        # Monitor memory usage
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Start servers with staggered timing
        start_time = time.time()
        for i, server in enumerate(servers):
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.4)  # Stagger startup

            if i % 5 == 0:  # Progress indicator
                print(f"  Started {i + 1}/{num_nodes} servers...")

        setup_time = (time.time() - start_time) * 1000
        print(f"✅ Cluster setup completed in {setup_time:.1f}ms")

        # Allow cluster formation
        await asyncio.sleep(8.0)

        # Register functions across the cluster
        def make_function(node_id: int):
            def scale_function(data: str) -> dict[str, Any]:
                return {
                    "node_id": node_id,
                    "processed_data": f"scale_result_{node_id}_{data}",
                    "timestamp": time.time(),
                    "cluster_size": num_nodes,
                }

            return scale_function

        function_registration_start = time.time()
        for i, server in enumerate(servers):
            func = make_function(i)
            server.register_command(f"scale_function_{i}", func, [f"scale-{i + 1}"])

        function_registration_time = (time.time() - function_registration_start) * 1000
        print(
            f"✅ Function registration completed in {function_registration_time:.1f}ms"
        )

        # Extended propagation time for large cluster
        await asyncio.sleep(15.0)

        # Test cross-cluster connectivity with multiple samples
        num_test_samples = min(20, num_nodes * 2)
        connectivity_tests = []

        test_start_time = time.time()

        for test_id in range(num_test_samples):
            source_node = test_id % num_nodes
            target_node = (test_id + num_nodes // 2) % num_nodes

            client = MPREGClientAPI(f"ws://127.0.0.1:{ports[source_node]}")
            test_context.clients.append(client)
            await client.connect()

            try:
                result = await client._client.request(
                    [
                        RPCCommand(
                            name="scale_test",
                            fun=f"scale_function_{target_node}",
                            args=(f"large_scale_test_{test_id}",),
                            locs=frozenset([f"scale-{target_node + 1}"]),
                        )
                    ]
                )

                success = "scale_test" in result
                connectivity_tests.append(success)

                if not success:
                    print(
                        f"❌ Test {test_id}: Node {source_node} → Node {target_node} FAILED"
                    )

            except Exception as e:
                connectivity_tests.append(False)
                print(
                    f"❌ Test {test_id}: Node {source_node} → Node {target_node} ERROR: {e}"
                )

        test_time = (time.time() - test_start_time) * 1000

        # Memory usage after testing
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_usage = final_memory - initial_memory

        # Performance analysis
        success_rate = sum(connectivity_tests) / len(connectivity_tests)

        print("📊 Large-scale cluster performance results:")
        print(f"  Cluster size: {num_nodes} nodes")
        print(f"  Setup time: {setup_time:.1f}ms")
        print(f"  Function registration time: {function_registration_time:.1f}ms")
        print(f"  Connectivity test time: {test_time:.1f}ms ({num_test_samples} tests)")
        print(
            f"  Success rate: {success_rate:.2%} ({sum(connectivity_tests)}/{len(connectivity_tests)})"
        )
        print(f"  Memory usage: {memory_usage:.1f}MB increase")
        print(f"  Average memory per node: {memory_usage / num_nodes:.2f}MB")

        # Performance assertions for large-scale cluster
        assert success_rate >= 0.7, (
            f"Large-scale success rate {success_rate:.2%} below threshold 70%"
        )
        assert memory_usage < num_nodes * 50, (
            f"Memory usage {memory_usage:.1f}MB too high for {num_nodes} nodes"
        )
        assert setup_time < num_nodes * 1000, (
            f"Setup time {setup_time:.1f}ms too slow for {num_nodes} nodes"
        )

    async def test_federated_rpc_memory_usage(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test memory usage characteristics under federated RPC load."""
        if len(server_cluster_ports) < 3:
            pytest.skip("Need at least 3 ports for memory usage test")

        ports = server_cluster_ports[:3]

        # Create test cluster
        settings_list = []
        for i, port in enumerate(ports):
            connect_to = f"ws://127.0.0.1:{ports[0]}" if i > 0 else None

            settings_list.append(
                MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"Memory-Node-{i + 1}",
                    cluster_id="memory-test-cluster",
                    resources={f"memory-{i + 1}"},
                    peers=None,
                    connect=connect_to,
                    advertised_urls=None,
                    gossip_interval=1.0,
                )
            )

        servers = [MPREGServer(settings=s) for s in settings_list]
        test_context.servers.extend(servers)

        # Monitor memory usage
        process = psutil.Process()
        baseline_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.5)

        await asyncio.sleep(2.0)
        startup_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Register many functions to test memory scaling
        num_functions = 100
        for server_idx, server in enumerate(servers):
            for func_idx in range(num_functions):

                def make_function(s_idx: int, f_idx: int):
                    def memory_function(data: str) -> str:
                        return f"Memory test S{s_idx}F{f_idx}: {data}"

                    return memory_function

                func = make_function(server_idx, func_idx)
                server.register_command(
                    f"memory_function_{server_idx}_{func_idx}",
                    func,
                    [f"memory-{server_idx + 1}"],
                )

        await asyncio.sleep(5.0)
        registration_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Create multiple clients and perform many requests
        num_requests = 200
        clients = []

        for i in range(len(ports)):
            client = MPREGClientAPI(f"ws://127.0.0.1:{ports[i]}")
            clients.append(client)
            test_context.clients.append(client)
            await client.connect()

        # Execute many requests to test memory behavior under load
        for request_batch in range(5):  # 5 batches of 40 requests each
            batch_tasks = []

            for i in range(40):
                client = clients[i % len(clients)]
                server_idx = i % len(servers)
                func_idx = i % num_functions

                task = asyncio.create_task(
                    client._client.request(
                        [
                            RPCCommand(
                                name="memory_test",
                                fun=f"memory_function_{server_idx}_{func_idx}",
                                args=(f"memory_load_test_{request_batch}_{i}",),
                                locs=frozenset([f"memory-{server_idx + 1}"]),
                            )
                        ]
                    )
                )
                batch_tasks.append(task)

            # Wait for batch completion
            await asyncio.gather(*batch_tasks, return_exceptions=True)

            # Check memory after each batch
            batch_memory = process.memory_info().rss / 1024 / 1024  # MB
            print(f"  Batch {request_batch + 1}: {batch_memory:.1f}MB")

            # Force garbage collection
            gc.collect()
            await asyncio.sleep(0.5)

        final_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Memory analysis
        startup_overhead = startup_memory - baseline_memory
        registration_overhead = registration_memory - startup_memory
        execution_overhead = final_memory - registration_memory
        total_overhead = final_memory - baseline_memory

        print("📊 Memory usage analysis:")
        print(f"  Baseline memory: {baseline_memory:.1f}MB")
        print(f"  After startup: {startup_memory:.1f}MB (+{startup_overhead:.1f}MB)")
        print(
            f"  After registration: {registration_memory:.1f}MB (+{registration_overhead:.1f}MB)"
        )
        print(f"  After execution: {final_memory:.1f}MB (+{execution_overhead:.1f}MB)")
        print(f"  Total overhead: {total_overhead:.1f}MB")
        print(f"  Functions registered: {len(servers) * num_functions}")
        print(
            f"  Memory per function: {registration_overhead / (len(servers) * num_functions):.3f}MB"
        )
        print(f"  Requests executed: {num_requests}")

        # Memory usage assertions
        assert startup_overhead < 100, (
            f"Startup memory overhead {startup_overhead:.1f}MB too high"
        )
        assert registration_overhead < 50, (
            f"Function registration overhead {registration_overhead:.1f}MB too high"
        )
        assert execution_overhead < 30, (
            f"Execution memory overhead {execution_overhead:.1f}MB too high"
        )
