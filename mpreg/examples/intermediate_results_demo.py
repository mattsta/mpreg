#!/usr/bin/env python3
"""
Demonstration of MPREG's Enhanced RPC System with Intermediate Results.

This script demonstrates the new intermediate results and execution summary
features that provide enhanced debugging and monitoring capabilities for
distributed multi-hop dependency resolution pipelines.
"""

import asyncio
import time

import ulid

# Import MPREG components
from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand, RPCRequest
from mpreg.server import MPREGServer


def display_execution_trace(response):
    """Display a formatted execution trace from intermediate results."""
    print("\n" + "=" * 50)
    print("🔍 RPC EXECUTION TRACE")
    print("=" * 50)

    if not response.intermediate_results:
        print("No intermediate results captured")
        return

    print(f"Request ID: {response.intermediate_results[0].request_id}")
    print(f"Total Levels: {response.intermediate_results[-1].total_levels}")
    print()

    for i, intermediate in enumerate(response.intermediate_results):
        print(
            f"Level {intermediate.level_index} ({intermediate.execution_time_ms:.1f}ms):"
        )
        print(f"  Progress: {intermediate.progress_percentage:.1f}%")
        print(f"  New Results: {list(intermediate.level_results.keys())}")
        print(f"  Total Available: {list(intermediate.accumulated_results.keys())}")
        print()

    if response.execution_summary:
        summary = response.execution_summary
        print("📊 EXECUTION SUMMARY:")
        print(f"  Total Time: {summary.total_execution_time_ms:.1f}ms")
        print(
            f"  Bottleneck Level: {summary.bottleneck_level_index} ({summary.bottleneck_time_ms:.1f}ms)"
        )
        print(f"  Average Level Time: {summary.average_level_time_ms:.1f}ms")
        print(f"  Commands Executed: {summary.parallel_commands_executed}")
        print(f"  Cross-Cluster Hops: {summary.cross_cluster_hops}")

    print("=" * 50)


async def demo_basic_intermediate_results():
    """Demonstrate basic intermediate results capture."""
    print("\n🎯 DEMO 1: Basic Intermediate Results Capture")
    print("-" * 40)

    # Setup server
    settings = MPREGSettings(
        host="127.0.0.1",
        port=8880,
        name="Demo-Server",
        cluster_id="demo-cluster",
        resources={"data", "compute", "output"},
        gossip_interval=1.0,
    )

    server = MPREGServer(settings=settings)

    # Register demo functions
    def load_data(source: str) -> dict:
        """Simulate data loading with some processing time."""
        time.sleep(0.1)  # Simulate I/O
        return {
            "data": f"loaded_from_{source}",
            "records": 1000,
            "timestamp": time.time(),
        }

    def process_data(data_result: dict) -> dict:
        """Simulate data processing."""
        time.sleep(0.2)  # Simulate computation
        return {
            "processed_data": f"processed_{data_result['data']}",
            "processed_records": data_result["records"] * 2,
            "processing_time": 0.2,
        }

    def generate_report(processed_result: dict) -> str:
        """Generate final report."""
        time.sleep(0.05)  # Simulate report generation
        return f"REPORT: {processed_result['processed_data']} with {processed_result['processed_records']} records"

    # Register functions with different resource requirements
    server.register_command("load_data", load_data, ["data"])
    server.register_command("process_data", process_data, ["compute"])
    server.register_command("generate_report", generate_report, ["output"])

    # Start server
    server_task = asyncio.create_task(server.server())
    await asyncio.sleep(1.0)  # Let server start

    try:
        # Create client
        client = MPREGClientAPI("ws://127.0.0.1:8880")
        await client.connect()

        # Execute RPC with intermediate results
        request = RPCRequest(
            cmds=(
                RPCCommand(
                    name="load_step",
                    fun="load_data",
                    args=("database",),
                    locs=frozenset(["data"]),
                ),
                RPCCommand(
                    name="process_step",
                    fun="process_data",
                    args=("load_step",),
                    locs=frozenset(["compute"]),
                ),
                RPCCommand(
                    name="report_step",
                    fun="generate_report",
                    args=("process_step",),
                    locs=frozenset(["output"]),
                ),
            ),
            u=str(ulid.new()),
            return_intermediate_results=True,
            include_execution_summary=True,
        )

        print("Executing 3-step pipeline with intermediate results tracking...")
        response = await client._client.request_enhanced(request)

        # Display results
        print(f"✅ Final Result: {response.r['report_step']}")
        display_execution_trace(response)

        await client.disconnect()

    finally:
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


async def demo_performance_analysis():
    """Demonstrate performance analysis with execution summaries."""
    print("\n⚡ DEMO 2: Performance Analysis with Execution Summaries")
    print("-" * 50)

    # Setup server with different performance characteristics
    settings = MPREGSettings(
        host="127.0.0.1",
        port=8881,
        name="Performance-Demo-Server",
        cluster_id="perf-cluster",
        resources={"fast", "slow", "medium"},
        gossip_interval=1.0,
    )

    server = MPREGServer(settings=settings)

    # Register functions with different performance characteristics
    async def fast_operation(data: str) -> str:
        """Fast operation - 10ms."""
        await asyncio.sleep(0.01)
        return f"fast_{data}"

    async def slow_operation(data: str) -> str:
        """Slow operation - 200ms (bottleneck)."""
        await asyncio.sleep(0.2)
        return f"slow_{data}"

    async def medium_operation(fast_result: str, slow_result: str) -> str:
        """Medium operation - 50ms, depends on both previous."""
        await asyncio.sleep(0.05)
        return f"combined_{fast_result}_{slow_result}"

    server.register_command("fast_operation", fast_operation, ["fast"])
    server.register_command("slow_operation", slow_operation, ["slow"])
    server.register_command("medium_operation", medium_operation, ["medium"])

    # Start server
    server_task = asyncio.create_task(server.server())
    await asyncio.sleep(1.0)

    try:
        # Create client
        client = MPREGClientAPI("ws://127.0.0.1:8881")
        await client.connect()

        # Execute performance test
        request = RPCRequest(
            cmds=(
                # These two run in parallel (Level 0)
                RPCCommand(
                    name="fast",
                    fun="fast_operation",
                    args=("test_data",),
                    locs=frozenset(["fast"]),
                ),
                RPCCommand(
                    name="slow",
                    fun="slow_operation",
                    args=("test_data",),
                    locs=frozenset(["slow"]),
                ),
                # This depends on both (Level 1)
                RPCCommand(
                    name="combined",
                    fun="medium_operation",
                    args=("fast", "slow"),
                    locs=frozenset(["medium"]),
                ),
            ),
            u=str(ulid.new()),
            return_intermediate_results=True,
            include_execution_summary=True,
            debug_mode=True,
        )

        print("Executing parallel pipeline with performance analysis...")
        start_time = time.time()
        response = await client._client.request_enhanced(request)
        total_time = (time.time() - start_time) * 1000

        print(f"✅ Final Result: {response.r['combined']}")
        print(f"📏 Client-measured total time: {total_time:.1f}ms")

        display_execution_trace(response)

        # Performance insights
        if response.execution_summary:
            summary = response.execution_summary
            print("\n🔬 PERFORMANCE INSIGHTS:")
            print(
                f"• Level 0 took {summary.level_execution_times[0]:.1f}ms (limited by slow_operation)"
            )
            print(
                f"• Level 1 took {summary.level_execution_times[1]:.1f}ms (medium_operation)"
            )
            print(
                f"• Bottleneck: Level {summary.bottleneck_level_index} is the slowest"
            )
            print(
                "• Optimization opportunity: The slow_operation is the limiting factor"
            )

        await client.disconnect()

    finally:
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


async def demo_dependency_debugging():
    """Demonstrate debugging complex dependencies."""
    print("\n🐛 DEMO 3: Complex Dependency Debugging")
    print("-" * 40)

    # Setup server
    settings = MPREGSettings(
        host="127.0.0.1",
        port=8882,
        name="Debug-Demo-Server",
        cluster_id="debug-cluster",
        resources={"input", "transform", "validate", "output"},
        gossip_interval=1.0,
    )

    server = MPREGServer(settings=settings)

    # Register functions that form a complex dependency graph
    def parse_input(raw_data: str) -> dict:
        """Parse raw input data."""
        return {
            "parsed": True,
            "items": raw_data.split(","),
            "count": len(raw_data.split(",")),
        }

    def transform_items(parsed_result: dict) -> dict:
        """Transform parsed items."""
        return {
            "transformed_items": [
                f"T({item.strip()})" for item in parsed_result["items"]
            ],
            "original_count": parsed_result["count"],
            "transformation": "uppercase",
        }

    def validate_data(parsed_result: dict, transformed_result: dict) -> dict:
        """Validate data integrity."""
        is_valid = parsed_result["count"] == len(
            transformed_result["transformed_items"]
        )
        return {
            "validation_passed": is_valid,
            "original_count": parsed_result["count"],
            "transformed_count": len(transformed_result["transformed_items"]),
            "integrity_check": "passed" if is_valid else "failed",
        }

    def generate_output(transformed_result: dict, validation_result: dict) -> str:
        """Generate final output."""
        if not validation_result["validation_passed"]:
            return "ERROR: Validation failed"

        return (
            f"OUTPUT: {', '.join(transformed_result['transformed_items'])} (validated)"
        )

    server.register_command("parse_input", parse_input, ["input"])
    server.register_command("transform_items", transform_items, ["transform"])
    server.register_command("validate_data", validate_data, ["validate"])
    server.register_command("generate_output", generate_output, ["output"])

    # Start server
    server_task = asyncio.create_task(server.server())
    await asyncio.sleep(1.0)

    try:
        # Create client
        client = MPREGClientAPI("ws://127.0.0.1:8882")
        await client.connect()

        # Execute complex dependency pipeline
        request = RPCRequest(
            cmds=(
                # Level 0: Parse input
                RPCCommand(
                    name="parsed",
                    fun="parse_input",
                    args=("apple, banana, cherry, date",),
                    locs=frozenset(["input"]),
                ),
                # Level 1: Transform (depends on parsed)
                RPCCommand(
                    name="transformed",
                    fun="transform_items",
                    args=("parsed",),
                    locs=frozenset(["transform"]),
                ),
                # Level 2: Validate (depends on both parsed and transformed)
                RPCCommand(
                    name="validated",
                    fun="validate_data",
                    args=("parsed", "transformed"),
                    locs=frozenset(["validate"]),
                ),
                # Level 3: Generate output (depends on transformed and validated)
                RPCCommand(
                    name="final",
                    fun="generate_output",
                    args=("transformed", "validated"),
                    locs=frozenset(["output"]),
                ),
            ),
            u=str(ulid.new()),
            return_intermediate_results=True,
            include_execution_summary=True,
            debug_mode=True,
        )

        print("Executing complex dependency pipeline...")
        response = await client._client.request_enhanced(request)

        print(f"✅ Final Result: {response.r['final']}")
        display_execution_trace(response)

        # Dependency analysis
        print("\n🔗 DEPENDENCY ANALYSIS:")
        for i, intermediate in enumerate(response.intermediate_results):
            new_deps = set(intermediate.level_results.keys())
            available_deps = set(intermediate.accumulated_results.keys())
            print(f"• Level {i}: Added {new_deps}, Total available: {available_deps}")

        await client.disconnect()

    finally:
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


async def main():
    """Run all demonstrations."""
    print("🚀 MPREG Enhanced RPC System - Intermediate Results Demo")
    print("=" * 60)
    print()
    print("This demonstration showcases MPREG's new intermediate results")
    print("and execution summary features for enhanced debugging and")
    print("monitoring of distributed multi-hop dependency pipelines.")
    print()

    try:
        await demo_basic_intermediate_results()
        await asyncio.sleep(2)

        await demo_performance_analysis()
        await asyncio.sleep(2)

        await demo_dependency_debugging()

        print("\n" + "=" * 60)
        print("✅ All demonstrations completed successfully!")
        print("🎉 Enhanced RPC system with intermediate results is working!")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n\n⚠️  Demo interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Demo failed with error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
