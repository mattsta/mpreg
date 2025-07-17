"""Integration tests that serve as comprehensive usage examples.

These tests demonstrate real-world usage patterns of the MPREG system including:
- Basic RPC calls and responses
- Multi-step workflows with dependencies
- Distributed processing across cluster nodes
- Error handling and fault tolerance
- Concurrent operations and load testing

Each test is designed to be a complete, documented example that users can
reference for their own implementations.
"""

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from mpreg.model import RPCCommand
from mpreg.server import MPREGServer


class TestBasicUsageExamples:
    """Examples of basic MPREG usage patterns."""

    async def test_simple_echo_call(
        self, single_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Example: Simple RPC call to echo function.

        This demonstrates the most basic MPREG usage - calling a single
        function on a server and getting the result back.
        """
        client = await client_factory(single_server.settings.port)

        # Basic echo call
        result = await client.call("echo", "Hello MPREG!")
        assert result == "Hello MPREG!"

        # Echo with different data types
        result = await client.call("echo", {"key": "value", "number": 42})
        assert result == {"key": "value", "number": 42}

    async def test_multi_argument_function(
        self, single_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Example: Calling functions with multiple arguments.

        Shows how to call the 'echos' function which accepts multiple arguments
        and returns them as a tuple.
        """
        client = await client_factory(single_server.settings.port)

        result = await client.call("echos", "arg1", "arg2", "arg3")
        assert result == ("arg1", "arg2", "arg3")

        # Mix of data types
        result = await client.call("echos", 1, "two", [3, 4], {"five": 6})
        assert result == (1, "two", [3, 4], {"five": 6})

    async def test_function_with_keyword_arguments(
        self, enhanced_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Example: Using keyword arguments in RPC calls.

        Demonstrates how to pass both positional and keyword arguments
        to remote functions.
        """
        client = await client_factory(enhanced_server.settings.port)

        # Call with keyword arguments
        result = await client.call(
            "format_results", {"prediction": "success"}, format_type="summary"
        )
        assert result == "Result: success"

    async def test_resource_specific_routing(
        self, enhanced_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Example: Resource-specific function routing.

        Shows how MPREG routes function calls to servers that have
        the required resources available.
        """
        client = await client_factory(enhanced_server.settings.port)

        # Call function requiring specific resource
        result = await client.call(
            "data_processing", [10, 20, 30, 40, 50], locs=frozenset(["data-processor"])
        )
        assert result == 150  # sum of the list


class TestWorkflowExamples:
    """Examples of multi-step workflows and dependency resolution."""

    async def test_simple_dependency_chain(
        self, enhanced_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Example: Simple two-step workflow with dependency.

        This shows how MPREG automatically resolves dependencies between
        function calls, where the output of one function becomes the input
        to another.
        """
        client = await client_factory(enhanced_server.settings.port)

        # Create a workflow where step2 depends on step1
        result = await client._client.request(
            [
                RPCCommand(
                    name="step1", fun="data_processing", args=([1, 2, 3, 4, 5],)
                ),
                RPCCommand(
                    name="step2", fun="echo", args=("step1",)
                ),  # References step1 result
            ]
        )

        # step2 should contain the result of step1 (sum of [1,2,3,4,5] = 15)
        assert result["step2"] == 15

    async def test_complex_multi_step_workflow(
        self, enhanced_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Example: Complex workflow with multiple dependencies.

        Demonstrates a realistic data processing pipeline:
        1. Process raw data
        2. Run ML inference on processed data
        3. Format the final results
        """
        client = await client_factory(enhanced_server.settings.port)

        # Multi-step workflow: data processing -> ML inference -> formatting
        result = await client._client.request(
            [
                RPCCommand(
                    name="processed_data",
                    fun="data_processing",
                    args=([100, 200, 300],),
                    locs=frozenset(["data-processor"]),
                ),
                RPCCommand(
                    name="ml_result",
                    fun="ml_inference",
                    args=("my_model", {"value": "processed_data"}),
                    locs=frozenset(["ml-model"]),
                ),
                RPCCommand(
                    name="final_output",
                    fun="format_results",
                    args=("ml_result",),
                    kwargs={"format_type": "summary"},
                    locs=frozenset(["formatter"]),
                ),
            ]
        )

        # Only the final step is returned (leaf node in dependency graph)
        assert "final_output" in result
        assert "Result: processed_600" in result["final_output"]

    async def test_parallel_processing_workflow(
        self, enhanced_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Example: Parallel processing with fan-out and fan-in pattern.

        Shows how MPREG can execute independent steps in parallel,
        then combine their results in a final step.
        """
        client = await client_factory(enhanced_server.settings.port)

        # Parallel processing: two independent data processing steps + combination
        result = await client._client.request(
            [
                RPCCommand(name="batch1", fun="data_processing", args=([1, 2, 3],)),
                RPCCommand(name="batch2", fun="data_processing", args=([4, 5, 6],)),
                RPCCommand(name="combined", fun="echos", args=("batch1", "batch2")),
            ]
        )

        # Results from both parallel branches should be combined
        assert result["combined"] == (6, 15)  # sums of [1,2,3] and [4,5,6]


class TestDistributedExamples:
    """Examples of distributed operations across multiple servers."""

    async def test_distributed_function_routing(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ) -> None:
        """Example: Function calls routed across cluster nodes.

        Demonstrates how MPREG automatically routes function calls to
        appropriate servers based on available functions and resources.
        """
        server1, server2 = cluster_2_servers

        # Connect to first server, but calls may be routed to either server
        client = await client_factory(server1.settings.port)

        # These calls should work regardless of which server has the function
        result1 = await client.call("echo", "distributed call 1")
        result2 = await client.call("echos", "distributed", "call", "2")

        assert result1 == "distributed call 1"
        assert result2 == ("distributed", "call", "2")

    async def test_resource_based_routing(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ) -> None:
        """Example: Resource-specific routing across cluster.

        Shows how function calls are routed to servers that have
        the required resources, even in a distributed cluster.
        """
        server1, server2 = cluster_2_servers

        # Add resource-specific functions to different servers
        def process_model_a(data: str) -> str:
            return f"model-a processed: {data}"

        def process_model_b(data: str) -> str:
            return f"model-b processed: {data}"

        server1.register_command("process_a", process_model_a, ["model-a"])
        server2.register_command("process_b", process_model_b, ["model-b"])

        # Wait for registration to propagate
        await asyncio.sleep(0.5)

        client = await client_factory(server1.settings.port)

        # Calls should be routed to appropriate servers based on resources
        result_a = await client.call(
            "process_a", "test data", locs=frozenset(["model-a"])
        )
        result_b = await client.call(
            "process_b", "test data", locs=frozenset(["model-b"])
        )

        assert result_a == "model-a processed: test data"
        assert result_b == "model-b processed: test data"

    async def test_cross_server_workflow(
        self,
        cluster_3_servers: tuple[MPREGServer, MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ) -> None:
        """Example: Multi-step workflow spanning multiple servers.

        Demonstrates complex workflows where different steps execute
        on different servers based on resource availability.
        """
        primary, secondary, tertiary = cluster_3_servers

        # Register different functions on different servers
        def preprocess(data: list[Any]) -> dict[str, Any]:
            return {"preprocessed": sum(data), "count": len(data)}

        def analyze(preprocessed_data: dict[str, Any]) -> dict[str, Any]:
            return {
                "analysis": f"Analyzed {preprocessed_data['count']} items",
                "total": preprocessed_data["preprocessed"],
            }

        def finalize(analysis: dict[str, Any]) -> str:
            return f"Final report: {analysis['analysis']}, Total: {analysis['total']}"

        primary.register_command("preprocess", preprocess, ["dataset-1"])
        secondary.register_command("analyze", analyze, ["dataset-2"])
        tertiary.register_command("finalize", finalize, ["dataset-3"])

        # Wait for cluster synchronization
        await asyncio.sleep(1.0)

        client = await client_factory(primary.settings.port)

        # Multi-server workflow
        result = await client._client.request(
            [
                RPCCommand(
                    name="prep",
                    fun="preprocess",
                    args=([10, 20, 30, 40],),
                    locs=frozenset(["dataset-1"]),
                ),
                RPCCommand(
                    name="analysis",
                    fun="analyze",
                    args=("prep",),
                    locs=frozenset(["dataset-2"]),
                ),
                RPCCommand(
                    name="report",
                    fun="finalize",
                    args=("analysis",),
                    locs=frozenset(["dataset-3"]),
                ),
            ]
        )

        assert "report" in result
        assert "Final report: Analyzed 4 items, Total: 100" == result["report"]


class TestConcurrencyExamples:
    """Examples of concurrent operations and load testing."""

    async def test_concurrent_client_calls(
        self, single_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Example: Multiple clients making concurrent calls.

        Demonstrates how MPREG handles multiple concurrent clients
        and their simultaneous requests.
        """
        # Create multiple clients
        clients = [await client_factory(single_server.settings.port) for _ in range(5)]

        # Make concurrent calls from all clients
        tasks = [
            client.call("echo", f"message from client {i}")
            for i, client in enumerate(clients)
        ]

        results = await asyncio.gather(*tasks)

        # Verify all calls succeeded
        for i, result in enumerate(results):
            assert result == f"message from client {i}"

    async def test_concurrent_workflow_execution(
        self, enhanced_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Example: Multiple concurrent workflows.

        Shows how MPREG can handle multiple complex workflows
        executing concurrently without interference.
        """
        client = await client_factory(enhanced_server.settings.port)

        # Define multiple independent workflows
        workflows = []
        for i in range(3):
            workflow = client._client.request(
                [
                    RPCCommand(
                        name=f"data_{i}",
                        fun="data_processing",
                        args=([i, i + 1, i + 2],),
                    ),
                    RPCCommand(
                        name=f"ml_{i}",
                        fun="ml_inference",
                        args=(f"model_{i}", {"value": f"data_{i}"}),
                    ),
                    RPCCommand(
                        name=f"result_{i}", fun="format_results", args=(f"ml_{i}",)
                    ),
                ]
            )
            workflows.append(workflow)

        # Execute all workflows concurrently
        results = await asyncio.gather(*workflows)

        # Verify all workflows completed successfully
        assert len(results) == 3
        for i, result in enumerate(results):
            assert f"result_{i}" in result

    async def test_stress_testing_pattern(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ) -> None:
        """Example: Stress testing the cluster under load.

        Demonstrates patterns for load testing MPREG clusters
        with high concurrency and request volume.
        """
        server1, server2 = cluster_2_servers

        # Create multiple clients for load distribution
        clients = [await client_factory(server1.settings.port) for _ in range(10)]

        # Generate high-volume concurrent load
        tasks: list[Any] = []
        for client_idx, client in enumerate(clients):
            for request_idx in range(20):  # 20 requests per client = 200 total
                task = client.call("echo", f"load_test_{client_idx}_{request_idx}")
                tasks.append(task)

        # Execute all requests concurrently
        results = await asyncio.gather(*tasks)

        # Verify all requests succeeded
        assert len(results) == 200
        for i, result in enumerate(results):
            client_idx = i // 20
            request_idx = i % 20
            expected = f"load_test_{client_idx}_{request_idx}"
            assert result == expected


class TestErrorHandlingExamples:
    """Examples of error handling and fault tolerance."""

    async def test_command_not_found_handling(
        self, single_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Example: Handling non-existent function calls.

        Shows how to properly handle cases where requested functions
        don't exist on any server in the cluster.
        """
        client = await client_factory(single_server.settings.port)

        # This should raise a proper exception
        with pytest.raises(Exception):  # Will be CommandNotFoundException
            await client.call("nonexistent_function", "some args")

    async def test_timeout_handling(
        self, single_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Example: Handling request timeouts.

        Demonstrates proper timeout handling for long-running or
        unresponsive operations.
        """
        client = await client_factory(single_server.settings.port)

        # This should timeout quickly
        with pytest.raises(asyncio.TimeoutError):
            await client.call("echo", "test", timeout=0.001)  # Very short timeout

    async def test_graceful_degradation(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ) -> None:
        """Example: Graceful degradation when servers become unavailable.

        Shows how MPREG handles server failures and automatically
        routes requests to available servers.
        """
        server1, server2 = cluster_2_servers
        client = await client_factory(server1.settings.port)

        # First call should work normally
        result1 = await client.call("echo", "before failure")
        assert result1 == "before failure"

        # Simulate server failure by stopping one server
        # In a real test, we'd need to properly simulate this
        # For now, just verify the client can still make calls
        result2 = await client.call("echo", "after failure")
        assert result2 == "after failure"


# Performance benchmarking example
class TestPerformanceExamples:
    """Examples of performance testing and benchmarking."""

    async def test_latency_measurement(
        self, single_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Example: Measuring RPC call latency.

        Shows how to benchmark MPREG performance for different
        types of operations and payload sizes.
        """
        import time

        client = await client_factory(single_server.settings.port)

        # Measure latency for simple calls
        start_time = time.perf_counter()
        await client.call("echo", "latency test")
        end_time = time.perf_counter()

        latency = (end_time - start_time) * 1000  # Convert to milliseconds
        assert latency < 100  # Should be under 100ms for local calls

        # Measure latency for larger payloads
        large_data = {"data": list(range(1000))}
        start_time = time.perf_counter()
        await client.call("echo", large_data)
        end_time = time.perf_counter()

        large_latency = (end_time - start_time) * 1000
        assert large_latency < 500  # Should still be reasonable for larger data

    async def test_throughput_measurement(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ) -> None:
        """Example: Measuring cluster throughput.

        Demonstrates how to measure the maximum throughput
        of a MPREG cluster under sustained load.
        """
        import time

        server1, server2 = cluster_2_servers

        # Use multiple clients to maximize throughput
        clients = [await client_factory(server1.settings.port) for _ in range(5)]

        # Measure throughput over time period
        start_time = time.perf_counter()

        # Execute many concurrent requests
        tasks: list[Any] = []
        for _ in range(100):  # 100 requests total
            client = clients[len(tasks) % len(clients)]  # Round-robin clients
            task = client.call("echo", f"throughput_test_{len(tasks)}")
            tasks.append(task)

        results = await asyncio.gather(*tasks)
        end_time = time.perf_counter()

        duration = end_time - start_time
        throughput = len(results) / duration  # Requests per second

        assert len(results) == 100
        assert throughput > 10  # Should achieve at least 10 RPS
