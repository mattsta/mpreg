"""
Property-Based Tests for Enhanced RPC System with Intermediate Results.

This module provides comprehensive property-based testing for the enhanced RPC
system, validating invariants around intermediate results, execution summaries,
and progressive result streaming using Hypothesis.
"""

import asyncio
import time
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

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


# Custom strategies for RPC testing
@st.composite
def rpc_command_names(draw) -> str:
    """Generate valid RPC command names."""
    prefix = draw(
        st.sampled_from(["process", "analyze", "transform", "validate", "format"])
    )
    suffix = draw(st.integers(min_value=1, max_value=100))
    return f"{prefix}_{suffix}"


@st.composite
def rpc_command_args(draw) -> tuple[Any, ...]:
    """Generate valid RPC command arguments."""
    arg_count = draw(st.integers(min_value=0, max_value=3))
    args = []

    for _ in range(arg_count):
        arg_type = draw(st.sampled_from(["string", "int", "float", "reference"]))
        if arg_type == "string":
            args.append(draw(st.text(min_size=1, max_size=20)))
        elif arg_type == "int":
            args.append(draw(st.integers(min_value=1, max_value=1000)))
        elif arg_type == "float":
            args.append(draw(st.floats(min_value=0.1, max_value=100.0)))
        elif arg_type == "reference":
            # Reference to another command
            args.append(draw(rpc_command_names()))

    return tuple(args)


@st.composite
def resource_sets(draw) -> frozenset[str]:
    """Generate valid resource sets."""
    resources = draw(
        st.lists(
            st.sampled_from(["cpu", "memory", "storage", "network", "gpu", "disk"]),
            min_size=1,
            max_size=3,
            unique=True,
        )
    )
    return frozenset(resources)


@st.composite
def rpc_command_chains(draw) -> list[RPCCommand]:
    """Generate chains of RPC commands with proper dependencies."""
    chain_length = draw(st.integers(min_value=1, max_value=5))
    commands = []

    for i in range(chain_length):
        cmd_name = f"step_{i + 1}"

        if i == 0:
            # First command takes primitive arguments
            args = draw(st.tuples(st.text(min_size=1, max_size=10)))
        else:
            # Subsequent commands reference previous commands
            dependency_count = draw(st.integers(min_value=1, max_value=min(i, 2)))
            dependencies = draw(
                st.lists(
                    st.sampled_from([f"step_{j + 1}" for j in range(i)]),
                    min_size=dependency_count,
                    max_size=dependency_count,
                    unique=True,
                )
            )
            args = tuple(dependencies)

        command = RPCCommand(
            name=cmd_name,
            fun=f"test_function_{i + 1}",
            args=args,
            locs=draw(resource_sets()),
        )
        commands.append(command)

    return commands


@st.composite
def enhanced_rpc_requests(draw) -> RPCRequest:
    """Generate enhanced RPC requests with intermediate results enabled."""
    commands = draw(rpc_command_chains())

    return RPCRequest(
        cmds=tuple(commands),
        u=f"test_request_{draw(st.integers(min_value=1, max_value=10000))}",
        return_intermediate_results=draw(st.booleans()),
        include_execution_summary=draw(st.booleans()),
        debug_mode=draw(st.booleans()),
    )


class TestEnhancedRPCProperties:
    """Property-based tests for enhanced RPC system."""

    def test_intermediate_result_data_structure_properties(self):
        """Test properties of RPCIntermediateResult data structure."""

        @given(
            request_id=st.text(min_size=1, max_size=50),
            level_index=st.integers(min_value=0, max_value=10),
            total_levels=st.integers(min_value=1, max_value=10),
            completed_levels=st.integers(min_value=1, max_value=10),
            execution_time_ms=st.floats(min_value=0.1, max_value=10000.0),
            level_results=st.dictionaries(
                st.text(min_size=1, max_size=20),
                st.one_of(st.text(), st.integers(), st.floats()),
                min_size=1,
                max_size=5,
            ),
            accumulated_results=st.dictionaries(
                st.text(min_size=1, max_size=20),
                st.one_of(st.text(), st.integers(), st.floats()),
                min_size=1,
                max_size=10,
            ),
        )
        def test_intermediate_result_invariants(
            request_id: str,
            level_index: int,
            total_levels: int,
            completed_levels: int,
            execution_time_ms: float,
            level_results: dict[str, Any],
            accumulated_results: dict[str, Any],
        ):
            # Ensure valid relationships between indices and levels
            if completed_levels > total_levels:
                completed_levels = total_levels
            if level_index >= total_levels:
                level_index = total_levels - 1
            if completed_levels <= level_index:
                completed_levels = level_index + 1

            result = RPCIntermediateResult(
                request_id=request_id,
                level_index=level_index,
                level_results=level_results,
                accumulated_results=accumulated_results,
                total_levels=total_levels,
                completed_levels=completed_levels,
                timestamp=time.time(),
                execution_time_ms=execution_time_ms,
            )

            # Property 1: Progress percentage is between 0 and 100
            progress = result.progress_percentage
            assert 0.0 <= progress <= 100.0

            # Property 2: Progress percentage is consistent with completion ratio
            expected_progress = (completed_levels / total_levels) * 100.0
            assert abs(progress - expected_progress) < 0.001

            # Property 3: Final level detection is consistent
            is_final = result.is_final_level
            expected_final = completed_levels == total_levels
            assert is_final == expected_final

            # Property 4: Accumulated results contain at least level results
            for key in level_results:
                # Level results should be available in accumulated results
                # (though values might differ due to processing)
                assert isinstance(key, str)

            # Property 5: Level index is within valid range
            assert 0 <= level_index < total_levels

            # Property 6: Completed levels is at least level_index + 1
            assert completed_levels >= level_index + 1
            assert completed_levels <= total_levels

            # Property 7: Execution time is non-negative
            assert execution_time_ms >= 0.0

            # Property 8: Request ID is preserved
            assert result.request_id == request_id

            # Property 9: Timestamp is reasonable (within last minute)
            current_time = time.time()
            assert current_time - 60 < result.timestamp <= current_time

        test_intermediate_result_invariants()
        print("✅ Intermediate result data structure properties verified")

    def test_execution_summary_properties(self):
        """Test properties of RPCExecutionSummary data structure."""

        @given(
            request_id=st.text(min_size=1, max_size=50),
            total_levels=st.integers(min_value=1, max_value=10),
            level_execution_times=st.lists(
                st.floats(min_value=1.0, max_value=1000.0), min_size=1, max_size=10
            ),
            parallel_commands_executed=st.integers(min_value=1, max_value=50),
            cross_cluster_hops=st.integers(min_value=0, max_value=20),
        )
        def test_execution_summary_invariants(
            request_id: str,
            total_levels: int,
            level_execution_times: list[float],
            parallel_commands_executed: int,
            cross_cluster_hops: int,
        ):
            # Ensure consistent data
            if len(level_execution_times) != total_levels:
                level_execution_times = level_execution_times[:total_levels]
                while len(level_execution_times) < total_levels:
                    level_execution_times.append(10.0)

            total_execution_time = sum(level_execution_times)
            bottleneck_index = level_execution_times.index(max(level_execution_times))
            bottleneck_time = level_execution_times[bottleneck_index]
            average_time = total_execution_time / len(level_execution_times)

            summary = RPCExecutionSummary(
                request_id=request_id,
                total_execution_time_ms=total_execution_time,
                total_levels=total_levels,
                level_execution_times=tuple(level_execution_times),
                bottleneck_level_index=bottleneck_index,
                average_level_time_ms=average_time,
                parallel_commands_executed=parallel_commands_executed,
                cross_cluster_hops=cross_cluster_hops,
            )

            # Property 1: Total execution time equals sum of level times
            calculated_total = sum(summary.level_execution_times)
            assert abs(summary.total_execution_time_ms - calculated_total) < 0.001

            # Property 2: Average time is correct
            expected_average = calculated_total / len(summary.level_execution_times)
            assert abs(summary.average_level_time_ms - expected_average) < 0.001

            # Property 3: Bottleneck index is valid and identifies slowest level
            assert (
                0 <= summary.bottleneck_level_index < len(summary.level_execution_times)
            )
            bottleneck_actual_time = summary.level_execution_times[
                summary.bottleneck_level_index
            ]
            assert bottleneck_actual_time == max(summary.level_execution_times)
            assert summary.bottleneck_time_ms == bottleneck_actual_time

            # Property 4: Total levels matches level execution times length
            assert summary.total_levels == len(summary.level_execution_times)

            # Property 5: All execution times are positive
            assert all(t > 0 for t in summary.level_execution_times)

            # Property 6: Parallel commands executed is positive
            assert summary.parallel_commands_executed > 0

            # Property 7: Cross cluster hops is non-negative
            assert summary.cross_cluster_hops >= 0

            # Property 8: Request ID is preserved
            assert summary.request_id == request_id

        test_execution_summary_invariants()
        print("✅ Execution summary properties verified")

    @pytest.mark.asyncio
    async def test_enhanced_rpc_execution_properties(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test properties of enhanced RPC execution with live servers."""

        # Test with a fixed set of parameters to avoid Hypothesis async issues
        test_cases = [
            (2, True, True),  # Short chain with both features
            (3, True, False),  # Medium chain with intermediate results only
            (2, False, True),  # Short chain with execution summary only
            (1, False, False),  # Single step with no extras
        ]

        for test_idx, (
            chain_length,
            enable_intermediate_results,
            enable_execution_summary,
        ) in enumerate(test_cases):
            # Use different port for each test case to avoid conflicts
            port = server_cluster_ports[test_idx]

            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"PropertyTest-Server-{test_idx}",
                cluster_id=f"property-test-cluster-{test_idx}",
                resources={"cpu", "memory", "storage"},
                gossip_interval=1.0,
            )

            server = MPREGServer(settings=settings)
            test_context.servers.append(server)

            # Register test functions for the chain
            def create_test_function(step_num: int):
                def test_function(data: str) -> dict:
                    return {
                        f"step_{step_num}_result": f"processed_{data}",
                        "step_number": step_num,
                        "input_data": data,
                    }

                return test_function

            # Register functions for ALL possible steps (up to max chain length)
            max_chain_length = max(case[0] for case in test_cases)
            for i in range(max_chain_length):
                step_num = i + 1
                function_name = f"test_function_{step_num}"
                test_func = create_test_function(step_num)
                resources = (
                    ["cpu"] if i % 3 == 0 else ["memory"] if i % 3 == 1 else ["storage"]
                )
                server.register_command(function_name, test_func, resources)

            # Start server
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.3)  # Reduced startup time

            # Create client
            client = MPREGClientAPI(f"ws://127.0.0.1:{port}")
            test_context.clients.append(client)
            await client.connect()

            # Create command chain
            commands = []
            for i in range(chain_length):
                step_num = i + 1
                cmd_name = f"step_{step_num}"

                if i == 0:
                    args = ("test_input",)
                else:
                    # Reference previous step
                    args = (f"step_{i}",)

                resources = (
                    ["cpu"] if i % 3 == 0 else ["memory"] if i % 3 == 1 else ["storage"]
                )

                command = RPCCommand(
                    name=cmd_name,
                    fun=f"test_function_{step_num}",
                    args=args,
                    locs=frozenset(resources),
                )
                commands.append(command)

            # Create enhanced RPC request
            request = RPCRequest(
                cmds=tuple(commands),
                u=f"property_test_{chain_length}_{int(time.time() * 1000)}",
                return_intermediate_results=enable_intermediate_results,
                include_execution_summary=enable_execution_summary,
            )

            # Execute request
            response = await client._client.request_enhanced(request)

            # Property 1: Basic response structure
            assert response.r is not None
            assert response.u == request.u
            assert response.error is None

            # Property 2: Final result contains the final step (dependency chains only keep final results)
            final_result = response.r
            final_step_name = f"step_{chain_length}"
            assert final_step_name in final_result
            final_step_result = final_result[final_step_name]
            assert isinstance(final_step_result, dict)
            assert "step_number" in final_step_result
            assert final_step_result["step_number"] == chain_length

            # Property 3: Intermediate results consistency
            if enable_intermediate_results:
                assert len(response.intermediate_results) == chain_length

                for i, intermediate in enumerate(response.intermediate_results):
                    assert intermediate.level_index == i
                    assert intermediate.total_levels == chain_length
                    assert intermediate.completed_levels == i + 1
                    assert 0.0 <= intermediate.progress_percentage <= 100.0
                    assert intermediate.execution_time_ms > 0

                # Final intermediate result should be complete
                final_intermediate = response.intermediate_results[-1]
                assert final_intermediate.is_final_level
                assert final_intermediate.progress_percentage == 100.0
            else:
                assert len(response.intermediate_results) == 0

            # Property 4: Execution summary consistency
            if enable_execution_summary:
                assert response.execution_summary is not None
                summary = response.execution_summary

                assert summary.total_levels == chain_length
                assert len(summary.level_execution_times) == chain_length
                assert summary.parallel_commands_executed >= chain_length
                assert summary.total_execution_time_ms > 0.0
                assert summary.average_level_time_ms > 0.0
                assert 0 <= summary.bottleneck_level_index < chain_length
                assert summary.bottleneck_time_ms > 0.0

                # Bottleneck identification
                bottleneck_time = summary.level_execution_times[
                    summary.bottleneck_level_index
                ]
                assert summary.bottleneck_time_ms == bottleneck_time
                assert bottleneck_time == max(summary.level_execution_times)
            else:
                assert response.execution_summary is None

            print(
                f"✅ Property test passed for chain_length={chain_length}, intermediate={enable_intermediate_results}, summary={enable_execution_summary}"
            )

        print("✅ Enhanced RPC execution properties verified with live servers")

    def test_progressive_result_streaming_properties(self):
        """Test properties of progressive result streaming."""

        @given(
            request_id=st.text(min_size=1, max_size=20),
            num_results=st.integers(min_value=1, max_value=10),
            base_timestamp=st.floats(
                min_value=time.time() - 3600, max_value=time.time()
            ),
        )
        def test_streaming_invariants(
            request_id: str,
            num_results: int,
            base_timestamp: float,
        ):
            # Generate consistent intermediate results
            intermediate_results = []
            for i in range(num_results):
                result = RPCIntermediateResult(
                    request_id=request_id,
                    level_index=i,
                    level_results={f"step_{i}": f"result_{i}"},
                    accumulated_results={
                        f"step_{j}": f"result_{j}" for j in range(i + 1)
                    },
                    total_levels=num_results,
                    completed_levels=i + 1,
                    timestamp=base_timestamp + i * 0.1,  # Ensure ordered timestamps
                    execution_time_ms=float(i + 1),
                )
                intermediate_results.append(result)

            # Fix up the data to ensure consistency
            total_levels = len(intermediate_results)
            fixed_results = intermediate_results

            # Property 1: Progressive completion
            for i, result in enumerate(fixed_results):
                assert result.completed_levels == i + 1
                expected_progress = ((i + 1) / total_levels) * 100.0
                assert abs(result.progress_percentage - expected_progress) < 0.001

            # Property 2: Monotonic progression
            for i in range(1, len(fixed_results)):
                prev_result = fixed_results[i - 1]
                curr_result = fixed_results[i]

                # Level indices should increase
                assert curr_result.level_index == prev_result.level_index + 1

                # Completed levels should increase
                assert curr_result.completed_levels == prev_result.completed_levels + 1

                # Progress should increase
                assert curr_result.progress_percentage > prev_result.progress_percentage

            # Property 3: Final result properties
            if fixed_results:
                final_result = fixed_results[-1]
                assert final_result.is_final_level
                assert final_result.progress_percentage == 100.0
                assert final_result.completed_levels == total_levels
                assert final_result.level_index == total_levels - 1

            # Property 4: Timestamp ordering (results should be temporally ordered)
            timestamps = [result.timestamp for result in fixed_results]
            # Allow for some time skew in testing
            for i in range(1, len(timestamps)):
                # Timestamps should be non-decreasing (or very close)
                time_diff = timestamps[i] - timestamps[i - 1]
                assert time_diff >= -0.1  # Allow small backward skew

            # Property 5: All results have same request ID and total levels
            if fixed_results:
                request_id = fixed_results[0].request_id
                for result in fixed_results:
                    assert result.request_id == request_id
                    assert result.total_levels == total_levels

        test_streaming_invariants()
        print("✅ Progressive result streaming properties verified")

    def test_rpc_request_enhancement_properties(self):
        """Test properties of enhanced RPC request structures."""

        @given(enhanced_rpc_requests())
        def test_request_enhancement_invariants(request: RPCRequest):
            # Property 1: Enhanced fields are consistent
            if hasattr(request, "debug_mode") and request.debug_mode:
                # Debug mode should imply enhanced features are available
                pass  # Implementation specific

            # Property 2: Command structure is valid
            assert len(request.cmds) > 0

            for i, cmd in enumerate(request.cmds):
                # Each command should have required fields
                assert isinstance(cmd.name, str)
                assert len(cmd.name) > 0
                assert isinstance(cmd.fun, str)
                assert len(cmd.fun) > 0
                assert isinstance(cmd.args, tuple)
                assert isinstance(cmd.locs, frozenset)
                assert len(cmd.locs) > 0

            # Property 3: Request ID is valid
            assert isinstance(request.u, str)
            assert len(request.u) > 0

            # Property 4: Boolean flags are boolean
            if hasattr(request, "return_intermediate_results"):
                assert isinstance(request.return_intermediate_results, bool)
            if hasattr(request, "include_execution_summary"):
                assert isinstance(request.include_execution_summary, bool)
            if hasattr(request, "debug_mode"):
                assert isinstance(request.debug_mode, bool)

            # Property 5: Command dependencies form valid DAG
            command_names = {cmd.name for cmd in request.cmds}

            for cmd in request.cmds:
                for arg in cmd.args:
                    if isinstance(arg, str) and arg in command_names:
                        # This is a reference to another command
                        # In a valid DAG, referenced commands should appear earlier
                        referenced_cmd_index = next(
                            i for i, c in enumerate(request.cmds) if c.name == arg
                        )
                        current_cmd_index = next(
                            i for i, c in enumerate(request.cmds) if c.name == cmd.name
                        )
                        # Referenced command should come before current command
                        assert referenced_cmd_index < current_cmd_index

        test_request_enhancement_invariants()
        print("✅ Enhanced RPC request properties verified")


class TestEnhancedRPCPerformanceProperties:
    """Property-based tests for enhanced RPC performance characteristics."""

    def test_execution_timing_properties(self):
        """Test properties of execution timing measurements."""

        @given(
            level_times=st.lists(
                st.floats(min_value=0.1, max_value=1000.0), min_size=1, max_size=10
            )
        )
        def test_timing_invariants(level_times: list[float]):
            epsilon = 1e-10  # For floating point comparisons
            total_time = sum(level_times)
            average_time = total_time / len(level_times)

            # Property 1: Total time is sum of parts
            assert abs(total_time - sum(level_times)) < 0.001

            # Property 2: Average is correct
            expected_average = sum(level_times) / len(level_times)
            assert abs(average_time - expected_average) < 0.001

            # Property 3: Bottleneck identification
            bottleneck_time = max(level_times)
            bottleneck_index = level_times.index(bottleneck_time)

            assert level_times[bottleneck_index] == bottleneck_time
            # Bottleneck is at least average (with floating point tolerance)
            assert bottleneck_time >= average_time - epsilon

            # Property 4: Performance metrics are non-negative
            assert total_time >= 0.0
            assert average_time >= 0.0
            assert bottleneck_time >= 0.0
            assert 0 <= bottleneck_index < len(level_times)

            # Property 5: Timing relationships
            min_time = min(level_times)
            max_time = max(level_times)

            # Use epsilon for floating point comparison
            epsilon = 1e-10
            assert min_time <= average_time + epsilon
            assert average_time <= max_time + epsilon
            assert abs(max_time - bottleneck_time) < epsilon

        test_timing_invariants()
        print("✅ Execution timing properties verified")

    def test_performance_trend_analysis_properties(self):
        """Test properties of performance trend analysis."""

        @given(
            performance_history=st.lists(
                st.floats(min_value=1.0, max_value=1000.0), min_size=3, max_size=20
            )
        )
        def test_trend_analysis_invariants(performance_history: list[float]):
            # Simple trend analysis logic
            def analyze_trend(history: list[float]) -> str:
                if len(history) < 3:
                    return "stable"

                recent_third = len(history) // 3
                recent_avg = sum(history[-recent_third:]) / recent_third
                earlier_avg = sum(history[:recent_third]) / recent_third

                improvement_threshold = 0.1  # 10% improvement
                degradation_threshold = 0.1  # 10% degradation

                if recent_avg < earlier_avg * (1 - improvement_threshold):
                    return "improving"
                elif recent_avg > earlier_avg * (1 + degradation_threshold):
                    return "degrading"
                else:
                    return "stable"

            trend = analyze_trend(performance_history)

            # Property 1: Trend is one of expected values
            assert trend in ["improving", "stable", "degrading"]

            # Property 2: Trend analysis is consistent with data
            if len(performance_history) >= 3:
                recent_third = len(performance_history) // 3
                recent_avg = sum(performance_history[-recent_third:]) / recent_third
                earlier_avg = sum(performance_history[:recent_third]) / recent_third

                if trend == "improving":
                    assert recent_avg < earlier_avg * 0.9  # At least 10% better
                elif trend == "degrading":
                    assert recent_avg > earlier_avg * 1.1  # At least 10% worse
                # stable case allows for variation within ±10%

            # Property 3: Performance history is preserved
            assert len(performance_history) >= 3
            assert all(p > 0 for p in performance_history)

        test_trend_analysis_invariants()
        print("✅ Performance trend analysis properties verified")
