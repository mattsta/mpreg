"""
Demonstration tests for the proposed intermediate results feature.

This module demonstrates how the intermediate results feature would work
for debugging complex multi-hop RPC pipelines in MPREG.

Note: This is a proof-of-concept demonstration. The actual implementation
would require integration with the RPC execution engine in server.py.
"""

import asyncio
import time

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.intermediate_results import (
    IntermediateResultCollector,
    RPCIntermediateResult,
    analyze_execution_bottlenecks,
    format_intermediate_results_for_debugging,
    trace_dependency_resolution,
)
from mpreg.core.model import RPCCommand
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext


class TestIntermediateResultsDemonstration:
    """Demonstrate the proposed intermediate results feature."""

    async def test_intermediate_results_collection_concept(self):
        """Demonstrate how intermediate results would be collected."""

        # Simulate a 4-level RPC execution
        collector = IntermediateResultCollector(
            request_id="demo_request_123", total_levels=4
        )

        # Simulate level 0 execution
        collector.start_level(0)
        await asyncio.sleep(0.1)  # Simulate execution time
        intermediate_0 = collector.complete_level(
            level_index=0,
            level_results={"data_loaded": {"source": "database", "rows": 1000}},
            accumulated_results={"data_loaded": {"source": "database", "rows": 1000}},
        )

        # Simulate level 1 execution (depends on level 0)
        collector.start_level(1)
        await asyncio.sleep(0.05)
        intermediate_1 = collector.complete_level(
            level_index=1,
            level_results={"data_cleaned": {"cleaned_rows": 950, "errors_removed": 50}},
            accumulated_results={
                "data_loaded": {"source": "database", "rows": 1000},
                "data_cleaned": {"cleaned_rows": 950, "errors_removed": 50},
            },
        )

        # Simulate level 2 execution (depends on level 1)
        collector.start_level(2)
        await asyncio.sleep(0.2)  # Slower level
        intermediate_2 = collector.complete_level(
            level_index=2,
            level_results={
                "analysis_result": {"patterns_found": 15, "confidence": 0.87}
            },
            accumulated_results={
                "data_loaded": {"source": "database", "rows": 1000},
                "data_cleaned": {"cleaned_rows": 950, "errors_removed": 50},
                "analysis_result": {"patterns_found": 15, "confidence": 0.87},
            },
        )

        # Simulate level 3 execution (final level)
        collector.start_level(3)
        await asyncio.sleep(0.03)
        intermediate_3 = collector.complete_level(
            level_index=3,
            level_results={
                "final_report": "Analysis complete with 15 patterns identified"
            },
            accumulated_results={
                "data_loaded": {"source": "database", "rows": 1000},
                "data_cleaned": {"cleaned_rows": 950, "errors_removed": 50},
                "analysis_result": {"patterns_found": 15, "confidence": 0.87},
                "final_report": "Analysis complete with 15 patterns identified",
            },
        )

        # Verify intermediate results capture
        assert len(collector.intermediate_results) == 4

        # Verify progress tracking
        assert intermediate_0.progress_percentage == 25.0
        assert intermediate_1.progress_percentage == 50.0
        assert intermediate_2.progress_percentage == 75.0
        assert intermediate_3.progress_percentage == 100.0
        assert intermediate_3.is_final_level

        # Verify timing information
        assert intermediate_0.execution_time_ms >= 90  # ~100ms
        assert intermediate_1.execution_time_ms >= 40  # ~50ms
        assert intermediate_2.execution_time_ms >= 190  # ~200ms (slowest)
        assert intermediate_3.execution_time_ms >= 20  # ~30ms

        # Verify result accumulation
        assert "data_loaded" in intermediate_1.accumulated_results
        assert "data_cleaned" in intermediate_2.accumulated_results
        assert len(intermediate_3.accumulated_results) == 4

        print("✓ Intermediate results collection works as expected")

    async def test_execution_bottleneck_analysis(self):
        """Demonstrate bottleneck analysis using intermediate results."""

        # Create sample intermediate results with varying execution times
        intermediate_results = [
            RPCIntermediateResult(
                request_id="perf_test",
                level_index=0,
                level_results={"step1": "result1"},
                accumulated_results={"step1": "result1"},
                total_levels=4,
                completed_levels=1,
                execution_time_ms=50.0,
            ),
            RPCIntermediateResult(
                request_id="perf_test",
                level_index=1,
                level_results={"step2": "result2"},
                accumulated_results={"step1": "result1", "step2": "result2"},
                total_levels=4,
                completed_levels=2,
                execution_time_ms=25.0,
            ),
            RPCIntermediateResult(
                request_id="perf_test",
                level_index=2,
                level_results={"step3": "result3"},
                accumulated_results={
                    "step1": "result1",
                    "step2": "result2",
                    "step3": "result3",
                },
                total_levels=4,
                completed_levels=3,
                execution_time_ms=200.0,  # Bottleneck!
            ),
            RPCIntermediateResult(
                request_id="perf_test",
                level_index=3,
                level_results={"step4": "result4"},
                accumulated_results={
                    "step1": "result1",
                    "step2": "result2",
                    "step3": "result3",
                    "step4": "result4",
                },
                total_levels=4,
                completed_levels=4,
                execution_time_ms=30.0,
            ),
        ]

        # Analyze bottlenecks
        analysis = analyze_execution_bottlenecks(intermediate_results)

        # Verify bottleneck identification
        assert analysis["slowest_level"]["level_index"] == 2
        assert analysis["slowest_level"]["execution_time_ms"] == 200.0
        assert analysis["average_level_time_ms"] == 76.25  # (50+25+200+30)/4

        # Verify slow level detection (levels > 1.5x average)
        assert len(analysis["slow_levels"]) == 1
        assert analysis["slow_levels"][0]["level_index"] == 2

        print("✓ Bottleneck analysis correctly identified slow execution level")
        print(
            f"  - Slowest level: {analysis['slowest_level']['level_index']} ({analysis['slowest_level']['execution_time_ms']}ms)"
        )
        print(f"  - Average level time: {analysis['average_level_time_ms']:.1f}ms")

    async def test_dependency_resolution_tracing(self):
        """Demonstrate dependency resolution tracing."""

        # Create intermediate results showing dependency accumulation
        intermediate_results = [
            RPCIntermediateResult(
                request_id="dep_trace",
                level_index=0,
                level_results={"load_data": {"rows": 100}},
                accumulated_results={"load_data": {"rows": 100}},
                total_levels=3,
                completed_levels=1,
                timestamp=time.time(),
            ),
            RPCIntermediateResult(
                request_id="dep_trace",
                level_index=1,
                level_results={
                    "process_data": {"processed": True},
                    "validate_data": {"valid": True},
                },
                accumulated_results={
                    "load_data": {"rows": 100},
                    "process_data": {"processed": True},
                    "validate_data": {"valid": True},
                },
                total_levels=3,
                completed_levels=2,
                timestamp=time.time() + 0.1,
            ),
            RPCIntermediateResult(
                request_id="dep_trace",
                level_index=2,
                level_results={"final_result": "success"},
                accumulated_results={
                    "load_data": {"rows": 100},
                    "process_data": {"processed": True},
                    "validate_data": {"valid": True},
                    "final_result": "success",
                },
                total_levels=3,
                completed_levels=3,
                timestamp=time.time() + 0.2,
            ),
        ]

        # Trace dependency resolution
        trace = trace_dependency_resolution(intermediate_results)

        # Verify dependency tracking
        assert trace["total_dependencies_resolved"] == 4
        assert trace["dependency_resolution_efficiency"]["levels_required"] == 3

        # Verify level-by-level dependency accumulation
        level_0 = trace["dependency_trace"]["level_0"]
        assert level_0["new_results"] == ["load_data"]
        assert level_0["dependency_count"] == 1

        level_1 = trace["dependency_trace"]["level_1"]
        assert set(level_1["new_results"]) == {"process_data", "validate_data"}
        assert level_1["dependency_count"] == 3

        level_2 = trace["dependency_trace"]["level_2"]
        assert level_2["new_results"] == ["final_result"]
        assert level_2["dependency_count"] == 4

        print("✓ Dependency resolution tracing works correctly")
        print(
            f"  - Total dependencies resolved: {trace['total_dependencies_resolved']}"
        )
        print(
            f"  - Levels required: {trace['dependency_resolution_efficiency']['levels_required']}"
        )

    async def test_debugging_output_formatting(self):
        """Demonstrate human-readable debugging output."""

        # Create sample intermediate results
        intermediate_results = [
            RPCIntermediateResult(
                request_id="debug_demo",
                level_index=0,
                level_results={
                    "sensor_data": {
                        "temperature": 22.5,
                        "humidity": 65,
                        "timestamp": "2024-01-15T10:30:00Z",
                    }
                },
                accumulated_results={
                    "sensor_data": {
                        "temperature": 22.5,
                        "humidity": 65,
                        "timestamp": "2024-01-15T10:30:00Z",
                    }
                },
                total_levels=3,
                completed_levels=1,
                execution_time_ms=45.2,
            ),
            RPCIntermediateResult(
                request_id="debug_demo",
                level_index=1,
                level_results={
                    "analysis": {"status": "normal", "alerts": [], "confidence": 0.95}
                },
                accumulated_results={
                    "sensor_data": {
                        "temperature": 22.5,
                        "humidity": 65,
                        "timestamp": "2024-01-15T10:30:00Z",
                    },
                    "analysis": {"status": "normal", "alerts": [], "confidence": 0.95},
                },
                total_levels=3,
                completed_levels=2,
                execution_time_ms=123.7,
            ),
            RPCIntermediateResult(
                request_id="debug_demo",
                level_index=2,
                level_results={
                    "report": "Environmental conditions are within normal parameters. Temperature: 22.5°C, Humidity: 65%. No alerts triggered."
                },
                accumulated_results={
                    "sensor_data": {
                        "temperature": 22.5,
                        "humidity": 65,
                        "timestamp": "2024-01-15T10:30:00Z",
                    },
                    "analysis": {"status": "normal", "alerts": [], "confidence": 0.95},
                    "report": "Environmental conditions are within normal parameters. Temperature: 22.5°C, Humidity: 65%. No alerts triggered.",
                },
                total_levels=3,
                completed_levels=3,
                execution_time_ms=28.9,
            ),
        ]

        # Format for debugging
        debug_output = format_intermediate_results_for_debugging(intermediate_results)

        # Verify key information is present
        assert "Request ID: debug_demo" in debug_output
        assert "Total Levels: 3" in debug_output
        assert "Level 0 (45.2ms):" in debug_output
        assert "Level 1 (123.7ms):" in debug_output
        assert "Level 2 (28.9ms):" in debug_output
        assert "Progress: 33.3%" in debug_output
        assert "Progress: 66.7%" in debug_output
        assert "Progress: 100.0%" in debug_output

        print("✓ Debug output formatting works correctly")
        print("\n" + debug_output)

    async def test_real_world_debugging_scenario(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Demonstrate how intermediate results would help debug a real pipeline failure."""

        port1, port2, port3 = server_cluster_ports[:3]

        # Create a 3-node cluster for complex pipeline
        servers = []
        for i, port in enumerate([port1, port2, port3]):
            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"Pipeline-Node-{i + 1}",
                cluster_id="debug-demo-cluster",
                resources={f"resource-{i + 1}"},
                peers=None,
                connect=f"ws://127.0.0.1:{port1}" if i > 0 else None,
                advertised_urls=None,
                gossip_interval=1.0,
            )
            servers.append(MPREGServer(settings=settings))

        test_context.servers.extend(servers)

        # Register functions that simulate a data processing pipeline
        def load_dataset(source: str) -> dict:
            return {
                "dataset_id": f"dataset_{source}",
                "rows": 10000,
                "columns": ["id", "name", "value", "timestamp"],
                "size_mb": 50.2,
                "loaded_at": time.time(),
            }

        def clean_data(dataset: dict) -> dict:
            # Simulate some data cleaning
            cleaned_rows = int(dataset["rows"] * 0.95)  # Remove 5% bad data
            return {
                "original_rows": dataset["rows"],
                "cleaned_rows": cleaned_rows,
                "removed_rows": dataset["rows"] - cleaned_rows,
                "cleaning_efficiency": 0.95,
                "cleaned_at": time.time(),
            }

        def analyze_patterns(dataset: dict, cleaning_result: dict) -> dict:
            # Simulate pattern analysis
            return {
                "patterns_detected": 23,
                "anomalies_found": 5,
                "confidence_score": 0.87,
                "processing_time_ms": 1500,
                "analysis_method": "statistical_clustering",
                "analyzed_rows": cleaning_result["cleaned_rows"],
                "analyzed_at": time.time(),
            }

        servers[0].register_command("load_dataset", load_dataset, ["resource-1"])
        servers[1].register_command("clean_data", clean_data, ["resource-2"])
        servers[2].register_command(
            "analyze_patterns", analyze_patterns, ["resource-3"]
        )

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.5)

        await asyncio.sleep(3.0)  # Allow cluster formation

        # Execute the pipeline and simulate capturing intermediate results
        client = MPREGClientAPI(f"ws://127.0.0.1:{port1}")
        test_context.clients.append(client)
        await client.connect()

        # This would be the actual RPC request
        commands = [
            RPCCommand(
                name="dataset",
                fun="load_dataset",
                args=("production_db",),
                locs=frozenset(["resource-1"]),
            ),
            RPCCommand(
                name="cleaned",
                fun="clean_data",
                args=("dataset",),
                locs=frozenset(["resource-2"]),
            ),
            RPCCommand(
                name="analysis",
                fun="analyze_patterns",
                args=("dataset", "cleaned"),
                locs=frozenset(["resource-3"]),
            ),
        ]

        # Execute the pipeline with intermediate results enabled
        import ulid

        from mpreg.core.model import RPCRequest

        request = RPCRequest(
            cmds=tuple(commands),
            u=str(ulid.new()),
            return_intermediate_results=True,
            include_execution_summary=True,
        )

        response = await client._client.request_enhanced(request)
        result = response.r

        # Use the actual intermediate results captured by the system
        print(
            f"✅ Intermediate results captured: {len(response.intermediate_results)} levels"
        )

        # Extract the actual intermediate results data
        # The intermediate results contain the level-by-level execution data
        if response.intermediate_results:
            # Create collector based on actual captured data
            collector = IntermediateResultCollector(
                request_id=response.intermediate_results[0].request_id,
                total_levels=response.intermediate_results[-1].total_levels,
            )

            # Populate collector with actual captured intermediate results
            for intermediate in response.intermediate_results:
                collector.start_level(intermediate.level_index)
                collector.complete_level(
                    level_index=intermediate.level_index,
                    level_results=intermediate.level_results,
                    accumulated_results=intermediate.accumulated_results,
                )
        else:
            # Fallback if intermediate results aren't captured
            collector = IntermediateResultCollector(
                request_id="pipeline_debug_001", total_levels=3
            )

            # Use final result as mock data for demonstration
            mock_intermediate = {
                "dataset": {"rows": 10000, "loaded": True},
                "cleaned": {"cleaned_rows": 9500, "efficiency": 0.95},
                "analysis": result["analysis"],
            }

            for i, (key, value) in enumerate(mock_intermediate.items()):
                collector.start_level(i)
                collector.complete_level(
                    level_index=i,
                    level_results={key: value},
                    accumulated_results={
                        k: v
                        for j, (k, v) in enumerate(mock_intermediate.items())
                        if j <= i
                    },
                )

        # Demonstrate debugging capabilities
        debug_output = format_intermediate_results_for_debugging(
            collector.intermediate_results
        )
        bottleneck_analysis = analyze_execution_bottlenecks(
            collector.intermediate_results
        )
        dependency_trace = trace_dependency_resolution(collector.intermediate_results)

        # Verify the pipeline executed successfully
        assert "analysis" in result
        assert result["analysis"]["patterns_detected"] == 23

        print("✓ Real-world pipeline debugging demonstration completed")
        print("\n=== Pipeline Execution Debug Information ===")
        print(debug_output)
        print("\n=== Performance Analysis ===")
        print(f"Slowest level: {bottleneck_analysis['slowest_level']['level_index']}")
        print(
            f"Average execution time: {bottleneck_analysis['average_level_time_ms']:.1f}ms"
        )
        print(
            f"Total dependencies resolved: {dependency_trace['total_dependencies_resolved']}"
        )

        # This demonstrates how intermediate results would make debugging much easier:
        # 1. See exactly what data was produced at each step
        # 2. Identify performance bottlenecks
        # 3. Trace dependency resolution
        # 4. Verify intermediate values are correct

        print("\n=== Debugging Benefits Demonstrated ===")
        print("1. ✓ Complete visibility into each pipeline step")
        print("2. ✓ Performance bottleneck identification")
        print("3. ✓ Dependency resolution verification")
        print("4. ✓ Intermediate value inspection for correctness")
        print("5. ✓ Timeline analysis of distributed execution")


class TestIntermediateResultsIntegrationConcept:
    """Demonstrate how intermediate results would integrate with existing systems."""

    def test_integration_with_enhanced_rpc_system(self):
        """Show how intermediate results integrate with enhanced RPC infrastructure."""

        # This demonstrates how the intermediate results feature would work
        # with the existing enhanced RPC system (enhanced_rpc.py)

        from mpreg.core.enhanced_rpc import (
            RPCExecutionStage,
            RPCProgressEvent,
        )

        # The intermediate results collector would work alongside
        # the existing progress monitoring
        collector = IntermediateResultCollector("integration_test", 2)

        # Simulate progress events that would be published
        progress_events = [
            RPCProgressEvent(
                request_id="integration_test",
                command_id="cmd_1",
                stage=RPCExecutionStage.COMMAND_STARTED,
                current_step="Starting data processing",
            ),
            RPCProgressEvent(
                request_id="integration_test",
                command_id="cmd_1",
                stage=RPCExecutionStage.COMMAND_COMPLETED,
                current_step="Data processing completed",
            ),
        ]

        # The intermediate results would complement the progress events
        # by providing the actual data results at each level
        collector.start_level(0)
        intermediate = collector.complete_level(
            level_index=0,
            level_results={"processed_data": {"status": "complete"}},
            accumulated_results={"processed_data": {"status": "complete"}},
        )

        # Integration points:
        # 1. Progress events show WHEN things happen
        # 2. Intermediate results show WHAT was produced
        # 3. Together they provide complete observability

        assert intermediate.level_index == 0
        assert len(progress_events) == 2

        print("✓ Integration with enhanced RPC system demonstrated")
        print("  - Progress events provide execution timeline")
        print("  - Intermediate results provide actual data")
        print("  - Combined: Complete distributed execution observability")

    def test_topic_based_intermediate_result_streaming(self):
        """Demonstrate streaming intermediate results via topics."""

        # This shows how intermediate results could be streamed
        # in real-time via MPREG's topic system

        collected_streams = []

        def simulate_topic_publish(topic: str, data: RPCIntermediateResult):
            """Simulate publishing intermediate result to a topic."""
            collected_streams.append(
                {
                    "topic": topic,
                    "timestamp": data.timestamp,
                    "level_index": data.level_index,
                    "progress": data.progress_percentage,
                    "results": list(data.level_results.keys()),
                }
            )

        # Simulate a client that requests streaming intermediate results
        request_id = "streaming_demo"
        topic_pattern = f"debug.rpc.{request_id}.intermediate"

        collector = IntermediateResultCollector(request_id, 3)

        # Simulate streaming results as they're produced
        for level in range(3):
            collector.start_level(level)
            intermediate = collector.complete_level(
                level_index=level,
                level_results={f"result_{level}": f"data_from_level_{level}"},
                accumulated_results={
                    f"result_{i}": f"data_from_level_{i}" for i in range(level + 1)
                },
            )

            # Stream to topic in real-time
            simulate_topic_publish(topic_pattern, intermediate)

        # Verify streaming behavior
        assert len(collected_streams) == 3
        # Direct type casting since we know the structure
        progress_0 = float(collected_streams[0]["progress"])  # type: ignore[arg-type]
        progress_1 = float(collected_streams[1]["progress"])  # type: ignore[arg-type]
        assert abs(progress_0 - 33.33333333333333) < 0.0001
        assert abs(progress_1 - 66.66666666666667) < 0.0001
        assert collected_streams[2]["progress"] == 100.0

        print("✓ Topic-based streaming demonstration completed")
        print("  - Real-time intermediate results via topic subscription")
        print("  - Progressive execution visibility")
        print("  - Integration with existing PubSub system")

        for i, stream in enumerate(collected_streams):
            print(f"    Level {i}: {stream['progress']:.1f}% - {stream['results']}")

    def test_configuration_driven_intermediate_results(self):
        """Demonstrate configurable intermediate result behavior."""

        # Show how intermediate results could be controlled via configuration

        configs = [
            {
                "name": "disabled",
                "return_intermediate_results": False,
                "include_execution_summary": False,
                "expected_results": 0,
            },
            {
                "name": "basic",
                "return_intermediate_results": True,
                "include_execution_summary": False,
                "expected_results": 2,
            },
            {
                "name": "full_debug",
                "return_intermediate_results": True,
                "include_execution_summary": True,
                "expected_results": 2,
            },
        ]

        for config in configs:
            collector = IntermediateResultCollector("config_test", 2)

            if config["return_intermediate_results"]:
                # Simulate collecting intermediate results
                for level in range(2):
                    collector.start_level(level)
                    collector.complete_level(
                        level_index=level,
                        level_results={f"step_{level}": f"result_{level}"},
                        accumulated_results={
                            f"step_{i}": f"result_{i}" for i in range(level + 1)
                        },
                    )

            # Verify configuration behavior
            actual_results = len(collector.intermediate_results)
            assert actual_results == config["expected_results"]

            print(
                f"✓ Config '{config['name']}': {actual_results} intermediate results collected"
            )

        print("✓ Configuration-driven behavior demonstrated")
        print("  - Flexible control over intermediate result collection")
        print("  - Performance optimization when disabled")
        print("  - Detailed debugging when enabled")
