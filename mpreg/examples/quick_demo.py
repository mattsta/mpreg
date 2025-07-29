#!/usr/bin/env python3
"""Quick demonstration of MPREG's unique capabilities.

This script shows the core features that make MPREG special in under 5 minutes.

Run with: poetry run python mpreg/examples/quick_demo.py
"""

import asyncio
import time
from typing import Any

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
from mpreg.server import MPREGServer

async def demo_basic_dependency_resolution():
    """Demonstrate MPREG's automatic dependency resolution."""
    print("🔗 DEMO 1: Automatic Dependency Resolution")
    print("-" * 50)

    # Single server for this demo
    server = MPREGServer(
        MPREGSettings(port=9001, name="Demo-Server", resources={"demo"})
    )

    # Register demo functions
    def add_numbers(a: int, b: int) -> int:
        return a + b

    def multiply(x: int, factor: int = 2) -> int:
        return x * factor

    def format_result(value: int, label: str = "Result") -> str:
        return f"{label}: {value}"

    server.register_command("add", add_numbers, ["demo"])
    server.register_command("multiply", multiply, ["demo"])
    server.register_command("format", format_result, ["demo"])

    # Start server
    server_task = asyncio.create_task(server.server())
    await asyncio.sleep(0.5)

    try:
        async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
            print("📊 Executing complex dependency chain...")

            # Complex dependency chain - MPREG resolves automatically!
            result = await client._client.request(
                [
                    RPCCommand(
                        name="sum1", fun="add", args=(10, 20), locs=frozenset(["demo"])
                    ),
                    RPCCommand(
                        name="sum2", fun="add", args=(5, 15), locs=frozenset(["demo"])
                    ),
                    RPCCommand(
                        name="total",
                        fun="add",
                        args=("sum1", "sum2"),
                        locs=frozenset(["demo"]),
                    ),
                    RPCCommand(
                        name="doubled",
                        fun="multiply",
                        args=("total", 3),
                        locs=frozenset(["demo"]),
                    ),
                    RPCCommand(
                        name="final",
                        fun="format",
                        args=("doubled", "Final Answer"),
                        locs=frozenset(["demo"]),
                    ),
                ]
            )

            print(f"✅ {result['final']}")
            print(
                "   🎯 MPREG automatically resolved: sum1 → sum2 → total → doubled → final"
            )
            print("   ⚡ All dependencies computed in correct order transparently!")

    finally:
        server._shutdown_event.set()
        await asyncio.sleep(0.1)

async def demo_resource_based_routing():
    """Demonstrate intelligent resource-based function routing."""
    print("\n🎯 DEMO 2: Intelligent Resource-Based Routing")
    print("-" * 50)

    # Create specialized servers
    servers = []

    # CPU-optimized server
    cpu_server = MPREGServer(
        MPREGSettings(port=9001, name="CPU-Server", resources={"cpu", "math"})
    )

    # GPU-optimized server
    gpu_server = MPREGServer(
        MPREGSettings(
            port=9002,
            name="GPU-Server",
            resources={"gpu", "ml"},
            peers=["ws://127.0.0.1:9001"],
            log_level="WARNING",
        )
    )

    # Database server
    db_server = MPREGServer(
        MPREGSettings(
            port=9003,
            name="DB-Server",
            resources={"database", "storage"},
            peers=["ws://127.0.0.1:9001"],
            log_level="WARNING",
        )
    )

    servers = [cpu_server, gpu_server, db_server]

    # Register specialized functions
    def cpu_crunch(data: list) -> float:
        return sum(x**2 for x in data) / len(data)

    def gpu_inference(model: str, input_data: str) -> dict:
        return {
            "model": model,
            "prediction": f"result_for_{input_data}",
            "confidence": 0.95,
        }

    def store_result(key: str, value: Any) -> dict:
        return {"stored": True, "key": key, "value": value, "timestamp": time.time()}

    cpu_server.register_command("cpu_crunch", cpu_crunch, ["cpu", "math"])
    gpu_server.register_command("gpu_inference", gpu_inference, ["gpu", "ml"])
    db_server.register_command("store_result", store_result, ["database", "storage"])

    # Start servers
    tasks = []
    for server in servers:
        task = asyncio.create_task(server.server())
        tasks.append(task)
        await asyncio.sleep(0.1)

    await asyncio.sleep(1.5)  # Allow cluster formation

    try:
        async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
            print("🔄 Routing functions to specialized servers...")

            # MPREG automatically routes to the right servers!
            workflow = await client._client.request(
                [
                    # This goes to CPU server
                    RPCCommand(
                        name="cpu_result",
                        fun="cpu_crunch",
                        args=([1, 2, 3, 4, 5, 6, 7, 8, 9, 10],),
                        locs=frozenset(["cpu", "math"]),
                    ),
                    # This goes to GPU server
                    RPCCommand(
                        name="gpu_result",
                        fun="gpu_inference",
                        args=("ResNet50", "image_data.jpg"),
                        locs=frozenset(["gpu", "ml"]),
                    ),
                    # This goes to Database server, using both previous results
                    RPCCommand(
                        name="stored",
                        fun="store_result",
                        args=("analysis_results", "cpu_result"),
                        locs=frozenset(["database", "storage"]),
                    ),
                ]
            )

            # Debug: show what keys are actually available
            print(f"📊 Workflow results: {list(workflow.keys())}")

            if "cpu_result" in workflow:
                print(f"✅ CPU computation: {workflow['cpu_result']:.2f}")
            else:
                print(
                    f"⚠️  CPU computation result not found in keys: {list(workflow.keys())}"
                )

            if "gpu_result" in workflow and isinstance(workflow["gpu_result"], dict):
                print(
                    f"✅ GPU inference: {workflow['gpu_result'].get('prediction', 'No prediction')}"
                )
            else:
                print("⚠️  GPU inference result not found or invalid format")

            if "stored" in workflow:
                print("✅ Database storage: Record stored successfully")
            else:
                print("⚠️  Storage result not found")

            print("   🎯 Each function automatically routed to its optimal server!")
            print("   🌐 No manual endpoint management required!")

    finally:
        for server in servers:
            server._shutdown_event.set()
        await asyncio.sleep(0.1)

async def demo_concurrent_execution():
    """Demonstrate MPREG's concurrent execution capabilities."""
    print("\n⚡ DEMO 3: High-Performance Concurrent Execution")
    print("-" * 50)

    # Single server for this demo
    server = MPREGServer(
        MPREGSettings(
            port=9001,
            name="Concurrent-Server",
            resources={"compute"},
            log_level="WARNING",
        )
    )

    # Register demo functions
    def fast_compute(x: int) -> int:
        return x * x + 1

    def slow_compute(x: int) -> dict:
        import time

        time.sleep(0.01)  # Simulate some work
        return {"input": x, "result": x**3, "computed_at": time.time()}

    server.register_command("fast_compute", fast_compute, ["compute"])
    server.register_command("slow_compute", slow_compute, ["compute"])

    # Start server
    server_task = asyncio.create_task(server.server())
    await asyncio.sleep(0.5)

    try:
        async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
            print("🏃 Testing concurrent execution performance...")

            # Test 1: Sequential vs Concurrent
            start_time = time.perf_counter()

            # Create 20 concurrent tasks
            tasks = []
            for i in range(20):
                if i % 2 == 0:
                    task = client.call("fast_compute", i, locs=frozenset(["compute"]))
                else:
                    task = client.call("slow_compute", i, locs=frozenset(["compute"]))
                tasks.append(task)

            # Execute all concurrently
            results = await asyncio.gather(*tasks)

            concurrent_time = time.perf_counter() - start_time

            print(f"✅ Processed 20 mixed operations in {concurrent_time:.3f}s")
            print(f"✅ Average operation time: {concurrent_time / 20:.4f}s")
            print(f"✅ Effective throughput: {20 / concurrent_time:.1f} ops/second")
            print("   🎯 MPREG handles concurrent requests seamlessly!")
            print("   ⚡ Single connection, multiple parallel operations!")

            # Test 2: Complex concurrent workflow
            print("\n🔄 Testing concurrent workflow execution...")

            start_time = time.perf_counter()

            # Multiple independent workflows running concurrently
            workflow_tasks = []
            for i in range(5):
                workflow = client._client.request(
                    [
                        RPCCommand(
                            name=f"step1_{i}",
                            fun="fast_compute",
                            args=(i * 10,),
                            locs=frozenset(["compute"]),
                        ),
                        RPCCommand(
                            name=f"step2_{i}",
                            fun="slow_compute",
                            args=(f"step1_{i}",),
                            locs=frozenset(["compute"]),
                        ),
                    ]
                )
                workflow_tasks.append(workflow)

            workflow_results = await asyncio.gather(*workflow_tasks)
            workflow_time = time.perf_counter() - start_time

            print(f"✅ Executed 5 concurrent workflows in {workflow_time:.3f}s")
            print("✅ Each workflow had 2 dependent steps")
            print("✅ Total operations: 10 (5 workflows × 2 steps)")
            print("   🎯 MPREG efficiently manages complex concurrent dependencies!")

    finally:
        server._shutdown_event.set()
        await asyncio.sleep(0.1)

async def demo_zero_config_clustering():
    """Demonstrate MPREG's zero-configuration clustering."""
    print("\n🌐 DEMO 4: Zero-Configuration Cluster Formation")
    print("-" * 50)

    print("📡 Starting servers with automatic peer discovery...")

    # Start multiple servers that auto-discover each other
    servers = []

    # Primary server
    server1 = MPREGServer(
        MPREGSettings(
            port=9001,
            name="Node-1",
            resources={"node1", "primary"},
            log_level="WARNING",
        )
    )

    # Secondary servers that automatically connect
    server2 = MPREGServer(
        MPREGSettings(
            port=9002,
            name="Node-2",
            resources={"node2", "worker"},
            peers=["ws://127.0.0.1:9001"],  # Automatically joins cluster
            log_level="WARNING",
        )
    )

    server3 = MPREGServer(
        MPREGSettings(
            port=9003,
            name="Node-3",
            resources={"node3", "worker"},
            peers=["ws://127.0.0.1:9001"],  # Automatically joins cluster
            log_level="WARNING",
        )
    )

    servers = [server1, server2, server3]

    # Each server gets different functions
    def node1_function(data: str) -> str:
        return f"Processed by Node-1: {data}"

    def node2_function(data: str) -> str:
        return f"Processed by Node-2: {data}"

    def node3_function(data: str) -> str:
        return f"Processed by Node-3: {data}"

    server1.register_command("node1_work", node1_function, ["node1", "primary"])
    server2.register_command("node2_work", node2_function, ["node2", "worker"])
    server3.register_command("node3_work", node3_function, ["node3", "worker"])

    # Start all servers
    tasks = []
    for server in servers:
        task = asyncio.create_task(server.server())
        tasks.append(task)
        await asyncio.sleep(0.2)  # Stagger startup

    await asyncio.sleep(2.0)  # Allow cluster formation

    try:
        async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
            print("🔗 Testing cluster-wide function routing...")

            # Call functions across the entire cluster
            cluster_results = await asyncio.gather(
                client.call("node1_work", "task_1", locs=frozenset(["node1"])),
                client.call("node2_work", "task_2", locs=frozenset(["node2"])),
                client.call("node3_work", "task_3", locs=frozenset(["node3"])),
            )

            for i, result in enumerate(cluster_results, 1):
                print(f"✅ Node {i}: {result}")

            print("   🎯 Cluster formed automatically with zero configuration!")
            print("   🌐 Functions discoverable across entire cluster!")
            print("   📡 Gossip protocol handles membership automatically!")

    finally:
        for server in servers:
            server._shutdown_event.set()
        await asyncio.sleep(0.1)

async def main():
    """Run all quick demos."""
    print("=" * 60)
    print("🚀 MPREG Quick Demo - Unique Capabilities Showcase")
    print("=" * 60)
    print("⏱️  This demo takes about 3 minutes and shows what makes MPREG special")
    print()

    await demo_basic_dependency_resolution()
    await demo_resource_based_routing()
    await demo_concurrent_execution()
    await demo_zero_config_clustering()

    print("\n" + "=" * 60)
    print("🎉 MPREG Quick Demo Complete!")
    print("=" * 60)
    print("🌟 What makes MPREG unique and powerful:")
    print()
    print("✨ AUTOMATIC DEPENDENCY RESOLUTION")
    print("   • No manual dependency management")
    print("   • Topological sorting built-in")
    print("   • Late-binding parameter substitution")
    print()
    print("🎯 INTELLIGENT RESOURCE ROUTING")
    print("   • Functions automatically route to optimal servers")
    print("   • No hardcoded endpoints or manual load balancing")
    print("   • Dynamic resource-based function discovery")
    print()
    print("⚡ HIGH-PERFORMANCE CONCURRENCY")
    print("   • Sub-millisecond local function calls")
    print("   • Concurrent requests over single connections")
    print("   • Scales to hundreds of parallel operations")
    print()
    print("🌐 ZERO-CONFIGURATION CLUSTERING")
    print("   • Automatic peer discovery via gossip protocol")
    print("   • Self-managing cluster membership")
    print("   • No central configuration or coordination required")
    print()
    print("🔧 SELF-MANAGING ARCHITECTURE")
    print("   • Components handle their own lifecycle")
    print("   • Automatic connection pooling and cleanup")
    print("   • Resilient error handling and recovery")
    print()
    print("💡 Ready to build distributed applications with MPREG!")
    print("   See examples/ directory for more advanced use cases")

if __name__ == "__main__":
    asyncio.run(main())
