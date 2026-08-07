"""
Tests for the enhanced RPC system with intermediate results.

This module tests the implementation of intermediate results and execution
summaries that improve debugging and monitoring of distributed multi-hop
dependency resolution pipelines.
"""

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import (
    RPCCommand,
    RPCExecutionSummary,
    RPCIntermediateResult,
    RPCRequest,
)
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext


class TestIntermediateResults:
    """Test intermediate results functionality."""

    async def test_basic_intermediate_results_capture(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test basic intermediate results capture during multi-level execution."""
        port = server_cluster_ports[0]

        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="IntermediateTest-Server",
            cluster_id="test-cluster",
            resources={"cpu", "memory", "storage"},
            gossip_interval=1.0,
        )

        server = MPREGServer(settings=settings)
        test_context.servers.append(server)

        # Register test functions with different resource requirements
        def step1_process(data: str) -> dict:
            return {"processed_data": f"step1_{data}", "step": 1}

        def step2_analyze(step1_result: dict) -> dict:
            return {
                "analysis": f"analyzed_{step1_result['processed_data']}",
                "step": 2,
                "previous_step": step1_result["step"],
            }

        def step3_format(step2_result: dict) -> str:
            return f"final_output: {step2_result['analysis']} (prev_step: {step2_result['previous_step']})"

        server.register_command("step1_process", step1_process, ["cpu"])
        server.register_command("step2_analyze", step2_analyze, ["memory"])
        server.register_command("step3_format", step3_format, ["storage"])

        # Start server
        task = asyncio.create_task(server.server())
        test_context.tasks.append(task)
        await asyncio.sleep(0.3)  # Reduced startup time

        # Create client and connect
        client = MPREGClientAPI(f"ws://127.0.0.1:{port}")
        test_context.clients.append(client)
        await client.connect()

        # Execute RPC with intermediate results enabled
        request = RPCRequest(
            cmds=(
                RPCCommand(
                    name="step1",
                    fun="step1_process",
                    args=("test_input",),
                    locs=frozenset(["cpu"]),
                ),
                RPCCommand(
                    name="step2",
                    fun="step2_analyze",
                    args=("step1",),
                    locs=frozenset(["memory"]),
                ),
                RPCCommand(
                    name="step3",
                    fun="step3_format",
                    args=("step2",),
                    locs=frozenset(["storage"]),
                ),
            ),
            u="test_intermediate_results",
            return_intermediate_results=True,
        )

        response = await client._client.request_enhanced(request)

        # Verify final result
        assert "step3" in response.r
        assert "final_output: analyzed_step1_test_input" in response.r["step3"]

        # Verify intermediate results were captured
        assert len(response.intermediate_results) == 3

        # Check Level 0 (step1)
        level0_result = response.intermediate_results[0]
        assert isinstance(level0_result, RPCIntermediateResult)
        assert level0_result.level_index == 0
        assert level0_result.completed_levels == 1
        assert level0_result.total_levels == 3
        assert (
            abs(level0_result.progress_percentage - 33.333333333333336) < 0.01
        )  # 1/3 * 100
        assert "step1" in level0_result.level_results
        assert level0_result.level_results["step1"]["step"] == 1

        # Check Level 1 (step2)
        level1_result = response.intermediate_results[1]
        assert level1_result.level_index == 1
        assert level1_result.completed_levels == 2
        assert (
            abs(level1_result.progress_percentage - 66.66666666666667) < 0.01
        )  # 2/3 * 100
        assert "step2" in level1_result.level_results
        assert level1_result.level_results["step2"]["step"] == 2
        # Verify accumulated results include previous steps
        assert "step1" in level1_result.accumulated_results
        assert "step2" in level1_result.accumulated_results

        # Check Level 2 (step3) - final level
        level2_result = response.intermediate_results[2]
        assert level2_result.level_index == 2
        assert level2_result.completed_levels == 3
        assert level2_result.total_levels == 3
        assert level2_result.is_final_level
        assert level2_result.progress_percentage == 100.0
        assert "step3" in level2_result.level_results
        # Verify all results are accumulated
        assert "step1" in level2_result.accumulated_results
        assert "step2" in level2_result.accumulated_results
        assert "step3" in level2_result.accumulated_results

        print("✅ Basic intermediate results capture working correctly")

    async def test_execution_summary_generation(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test execution summary generation with performance metrics."""
        port = server_cluster_ports[0]

        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="ExecutionSummary-Server",
            cluster_id="test-cluster",
            resources={"fast", "slow"},
            gossip_interval=1.0,
        )

        server = MPREGServer(settings=settings)
        test_context.servers.append(server)

        # Register functions with different execution times
        async def fast_function(data: str) -> str:
            await asyncio.sleep(0.002)  # 2ms - faster test
            return f"fast_{data}"

        async def slow_function(data: str) -> str:
            await asyncio.sleep(0.01)  # 10ms - faster test bottleneck
            return f"slow_{data}"

        async def medium_function(fast_result: str, slow_result: str) -> str:
            await asyncio.sleep(0.005)  # 5ms - faster test
            return f"combined_{fast_result}_{slow_result}"

        server.register_command("fast_function", fast_function, ["fast"])
        server.register_command("slow_function", slow_function, ["slow"])
        server.register_command("medium_function", medium_function, ["fast"])

        # Start server
        task = asyncio.create_task(server.server())
        test_context.tasks.append(task)
        await asyncio.sleep(0.3)  # Reduced startup time

        # Create client and connect
        client = MPREGClientAPI(f"ws://127.0.0.1:{port}")
        test_context.clients.append(client)
        await client.connect()

        # Execute RPC with execution summary enabled
        request = RPCRequest(
            cmds=(
                RPCCommand(
                    name="fast_task",
                    fun="fast_function",
                    args=("test",),
                    locs=frozenset(["fast"]),
                ),
                RPCCommand(
                    name="slow_task",
                    fun="slow_function",
                    args=("test",),
                    locs=frozenset(["slow"]),
                ),
                RPCCommand(
                    name="final_task",
                    fun="medium_function",
                    args=("fast_task", "slow_task"),
                    locs=frozenset(["fast"]),
                ),
            ),
            u="test_execution_summary",
            include_execution_summary=True,
            return_intermediate_results=True,
        )

        response = await client._client.request_enhanced(request)

        # Verify execution summary was generated
        assert response.execution_summary is not None
        summary = response.execution_summary
        assert isinstance(summary, RPCExecutionSummary)

        # Verify summary structure
        assert summary.request_id == "test_execution_summary"
        assert summary.total_levels == 2  # Level 0: fast+slow parallel, Level 1: final
        assert len(summary.level_execution_times) == 2
        assert summary.parallel_commands_executed == 3  # 2 parallel + 1 final
        assert summary.cross_cluster_hops == 0  # All local execution

        # Verify bottleneck detection
        # Level 0 should be slower due to slow_function (50ms vs 10ms)
        level0_time = summary.level_execution_times[0]
        level1_time = summary.level_execution_times[1]

        # Level 0 should take roughly 10ms (limited by slow_function)
        assert level0_time >= 8  # Allow some variance - 10ms -> 8ms
        assert level1_time >= 3  # Level 1 should take roughly 5ms

        # Bottleneck should be level 0 (the parallel execution with slow function)
        assert summary.bottleneck_level_index == 0 or level0_time > level1_time
        assert (
            summary.bottleneck_time_ms
            == summary.level_execution_times[summary.bottleneck_level_index]
        )

        # Total execution time should be sum of level times (plus overhead)
        assert summary.total_execution_time_ms >= (level0_time + level1_time)

        # Average should be calculated correctly
        expected_avg = sum(summary.level_execution_times) / len(
            summary.level_execution_times
        )
        assert (
            abs(summary.average_level_time_ms - expected_avg) < 1.0
        )  # Allow small float precision difference

        print("✅ Execution summary generation working correctly")
        print(f"   Total time: {summary.total_execution_time_ms:.1f}ms")
        print(
            f"   Bottleneck level: {summary.bottleneck_level_index} ({summary.bottleneck_time_ms:.1f}ms)"
        )
        print(f"   Average level time: {summary.average_level_time_ms:.1f}ms")

    async def test_backward_compatibility(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test that existing RPC requests work without intermediate results."""
        port = server_cluster_ports[0]

        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="BackwardCompat-Server",
            cluster_id="test-cluster",
            resources={"compute"},
            gossip_interval=1.0,
        )

        server = MPREGServer(settings=settings)
        test_context.servers.append(server)

        # Register simple test function
        def simple_function(data: str) -> str:
            return f"processed_{data}"

        server.register_command("simple_function", simple_function, ["compute"])

        # Start server
        task = asyncio.create_task(server.server())
        test_context.tasks.append(task)
        await asyncio.sleep(0.3)  # Reduced startup time

        # Create client and connect
        client = MPREGClientAPI(f"ws://127.0.0.1:{port}")
        test_context.clients.append(client)
        await client.connect()

        # Execute traditional RPC request (no intermediate results)
        result = await client._client.request(
            [
                RPCCommand(
                    name="simple_task",
                    fun="simple_function",
                    args=("backward_compat_test",),
                    locs=frozenset(["compute"]),
                )
            ]
        )

        # Verify traditional behavior still works
        assert "simple_task" in result
        assert result["simple_task"] == "processed_backward_compat_test"

        print(
            "✅ Backward compatibility maintained - traditional RPC requests work unchanged"
        )

    async def test_debug_mode_comprehensive(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test comprehensive debug mode with all features enabled."""
        port = server_cluster_ports[0]

        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="DebugMode-Server",
            cluster_id="test-cluster",
            resources={"debug"},
            gossip_interval=1.0,
        )

        server = MPREGServer(settings=settings)
        test_context.servers.append(server)

        # Register debug test functions
        def debug_step1(input_data: str) -> dict:
            return {"stage": "initial", "data": f"debug_{input_data}", "timestamp": 1}

        def debug_step2(step1_result: dict) -> dict:
            return {
                "stage": "processing",
                "processed": step1_result["data"],
                "timestamp": 2,
                "prev_timestamp": step1_result["timestamp"],
            }

        def debug_step3(step2_result: dict) -> str:
            return f"final_debug: {step2_result['processed']} (stages: {step2_result['prev_timestamp']} -> {step2_result['timestamp']})"

        server.register_command("debug_step1", debug_step1, ["debug"])
        server.register_command("debug_step2", debug_step2, ["debug"])
        server.register_command("debug_step3", debug_step3, ["debug"])

        # Start server
        task = asyncio.create_task(server.server())
        test_context.tasks.append(task)
        await asyncio.sleep(0.3)  # Reduced startup time

        # Create client and connect
        client = MPREGClientAPI(f"ws://127.0.0.1:{port}")
        test_context.clients.append(client)
        await client.connect()

        # Execute RPC with all debug features enabled
        request = RPCRequest(
            cmds=(
                RPCCommand(
                    name="debug1",
                    fun="debug_step1",
                    args=("comprehensive_test",),
                    locs=frozenset(["debug"]),
                ),
                RPCCommand(
                    name="debug2",
                    fun="debug_step2",
                    args=("debug1",),
                    locs=frozenset(["debug"]),
                ),
                RPCCommand(
                    name="debug3",
                    fun="debug_step3",
                    args=("debug2",),
                    locs=frozenset(["debug"]),
                ),
            ),
            u="comprehensive_debug_test",
            return_intermediate_results=True,
            include_execution_summary=True,
            debug_mode=True,
        )

        response = await client._client.request_enhanced(request)

        # Verify all debug features are present
        assert response.r is not None
        assert len(response.intermediate_results) == 3
        assert response.execution_summary is not None

        # Verify final result
        final_result = response.r["debug3"]
        assert "final_debug: debug_comprehensive_test" in final_result
        assert "stages: 1 -> 2" in final_result

        # Verify intermediate results show dependency resolution
        level1 = response.intermediate_results[1]
        assert level1.accumulated_results["debug1"]["stage"] == "initial"
        assert level1.level_results["debug2"]["prev_timestamp"] == 1

        # Verify execution summary shows comprehensive metrics
        summary = response.execution_summary
        assert summary.total_levels == 3
        assert summary.parallel_commands_executed == 3
        assert summary.total_execution_time_ms > 0

        print("✅ Comprehensive debug mode working correctly")
        print(f"   Captured {len(response.intermediate_results)} intermediate results")
        print(
            f"   Execution summary: {summary.total_execution_time_ms:.1f}ms total, {summary.parallel_commands_executed} commands"
        )


class TestIntermediateResultsIntegration:
    """Test integration with client API and enhanced features."""

    def test_intermediate_result_data_structure_properties(self):
        """Test the intermediate result data structure properties and methods."""
        import time

        # Create test intermediate result
        result = RPCIntermediateResult(
            request_id="test_request",
            level_index=1,
            level_results={"step2": "result2"},
            accumulated_results={"step1": "result1", "step2": "result2"},
            total_levels=3,
            completed_levels=2,
            timestamp=time.time(),
            execution_time_ms=150.5,
        )

        # Test properties
        assert not result.is_final_level  # 2 of 3 levels complete
        assert abs(result.progress_percentage - 66.66666666666667) < 0.001  # 2/3 * 100

        # Test final level
        final_result = RPCIntermediateResult(
            request_id="test_request",
            level_index=2,
            level_results={"step3": "result3"},
            accumulated_results={
                "step1": "result1",
                "step2": "result2",
                "step3": "result3",
            },
            total_levels=3,
            completed_levels=3,
            timestamp=time.time(),
            execution_time_ms=75.2,
        )

        assert final_result.is_final_level  # 3 of 3 levels complete
        assert final_result.progress_percentage == 100.0

        print("✅ Intermediate result data structure properties working correctly")

    def test_execution_summary_data_structure_properties(self):
        """Test the execution summary data structure properties and methods."""

        level_times = (50.0, 120.0, 30.0, 80.0)  # Level 1 is bottleneck (120ms)

        summary = RPCExecutionSummary(
            request_id="test_summary",
            total_execution_time_ms=300.0,
            total_levels=4,
            level_execution_times=level_times,
            bottleneck_level_index=1,  # Index of 120ms level
            average_level_time_ms=70.0,  # (50+120+30+80)/4
            parallel_commands_executed=8,
            cross_cluster_hops=2,
        )

        # Test bottleneck detection
        assert summary.bottleneck_time_ms == 120.0
        assert summary.bottleneck_level_index == 1

        # Verify all properties are accessible
        assert summary.total_execution_time_ms == 300.0
        assert summary.total_levels == 4
        assert len(summary.level_execution_times) == 4
        assert summary.average_level_time_ms == 70.0
        assert summary.parallel_commands_executed == 8
        assert summary.cross_cluster_hops == 2

        print("✅ Execution summary data structure properties working correctly")
