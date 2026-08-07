#!/usr/bin/env python3

import asyncio
import sys

sys.path.append(".")

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.model import RPCCommand


async def test_simple_analytics():
    print("=== SIMPLE ANALYTICS TEST ===")

    try:
        async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
            print("Testing simple analytics function...")

            # Test the analytics function directly without dependencies
            result = await client.call(
                "analytics", "test_dataset", ["metric1", "metric2"]
            )
            print(f"Simple analytics result: {result}")

    except Exception as e:
        print(f"❌ Simple analytics failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    return True


async def test_dependency_resolution():
    print("\n=== DEPENDENCY RESOLUTION TEST ===")

    try:
        async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
            print("Testing simple 2-step dependency...")

            # Test simple 2-step dependency
            result = await client._client.request(
                [
                    RPCCommand(
                        name="step1", fun="echo", args=("test_data",), locs=frozenset()
                    ),
                    RPCCommand(
                        name="step2",
                        fun="analytics",
                        args=("dataset", ["step1"]),
                        locs=frozenset(["analytics"]),
                    ),
                ]
            )

            print(f"2-step dependency result: {result}")

    except Exception as e:
        print(f"❌ 2-step dependency failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    return True


async def test_parallel_no_convergence():
    print("\n=== PARALLEL WITHOUT CONVERGENCE TEST ===")

    try:
        async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
            print("Testing parallel execution without convergence...")

            # Test just the three parallel branches without convergence
            result = await client._client.request(
                [
                    RPCCommand(
                        name="gpu_branch",
                        fun="run_inference",
                        args=("ModelA", "input_data"),
                        locs=frozenset(["gpu", "ml-models"]),
                    ),
                    RPCCommand(
                        name="cpu_branch",
                        fun="heavy_compute",
                        args=("input_data", 20),
                        locs=frozenset(["cpu-heavy"]),
                    ),
                    RPCCommand(
                        name="db_branch",
                        fun="query",
                        args=("SELECT * FROM data",),
                        locs=frozenset(["database"]),
                    ),
                ]
            )

            print(f"Parallel branches result: {result}")

    except Exception as e:
        print(f"❌ Parallel branches failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    return True


async def test_full_convergence():
    print("\n=== FULL CONVERGENCE TEST ===")

    try:
        async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
            print("Testing full parallel convergence...")

            # The full test with convergence
            result = await client._client.request(
                [
                    RPCCommand(
                        name="gpu_branch",
                        fun="run_inference",
                        args=("ModelA", "input_data"),
                        locs=frozenset(["gpu", "ml-models"]),
                    ),
                    RPCCommand(
                        name="cpu_branch",
                        fun="heavy_compute",
                        args=("input_data", 20),
                        locs=frozenset(["cpu-heavy"]),
                    ),
                    RPCCommand(
                        name="db_branch",
                        fun="query",
                        args=("SELECT * FROM data",),
                        locs=frozenset(["database"]),
                    ),
                    RPCCommand(
                        name="converged_analysis",
                        fun="analytics",
                        args=(
                            "convergence_test",
                            ["gpu_branch", "cpu_branch", "db_branch"],
                        ),
                        locs=frozenset(["analytics"]),
                    ),
                ]
            )

            print(f"✅ Full convergence result: {result}")

    except Exception as e:
        print(f"❌ Full convergence failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    return True


async def main():
    print("Connecting to existing 5-node cluster...")

    # Test in sequence to isolate the exact problem
    if not await test_simple_analytics():
        return

    if not await test_dependency_resolution():
        return

    if not await test_parallel_no_convergence():
        return

    await test_full_convergence()


if __name__ == "__main__":
    asyncio.run(main())
