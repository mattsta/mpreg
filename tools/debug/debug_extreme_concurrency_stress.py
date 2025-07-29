#!/usr/bin/env python3
"""
Stress test to reproduce and verify the fix for task destruction warnings under extreme concurrency.

This test simulates the conditions that cause "Task was destroyed but it is pending!" warnings.
"""

import asyncio
import sys
import warnings
from pathlib import Path

# Suppress warnings to see only task destruction warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from mpreg.server import MPREGServer, MPREGSettings


async def stress_test_server_cleanup(server_id: int):
    """Create a server, run it briefly, then shut it down."""

    # Create server with unique settings
    settings = MPREGSettings(
        host="127.0.0.1",
        port=9000 + server_id,
        name=f"stress-test-{server_id}",
        cluster_id=f"stress-cluster-{server_id}",
    )

    server = MPREGServer(settings)

    try:
        # Skip RPC registration to avoid conflicts - just start peer connection task
        peer_task = asyncio.create_task(server._manage_peer_connections())
        server._background_tasks.add(peer_task)
        peer_task.add_done_callback(server._background_tasks.discard)

        # Simulate very brief operation (extreme concurrency timing)
        await asyncio.sleep(0.05)  # Very short to stress the timing

        # Signal shutdown
        server.shutdown()

        # Stress test: very brief delay before cleanup (simulates test framework timing)
        await asyncio.sleep(0.01)

        # Perform shutdown_async - this should handle task cleanup properly
        await server.shutdown_async()

        # Return whether cleanup was successful
        return len(server._background_tasks) == 0

    except Exception as e:
        print(f"Server {server_id} failed: {e}")
        return False
    finally:
        # Ensure cleanup even if test fails
        try:
            server.shutdown()
            await server.shutdown_async()
        except TimeoutError:
            pass


async def run_extreme_concurrency_stress_test():
    """Run multiple servers concurrently to stress test cleanup."""

    print("=== EXTREME CONCURRENCY STRESS TEST ===")
    print()

    # Number of concurrent servers (simulating -n30 pytest-xdist)
    num_servers = 30

    print(f"Creating {num_servers} concurrent servers...")

    # Create tasks for all servers
    tasks = [
        asyncio.create_task(stress_test_server_cleanup(i)) for i in range(num_servers)
    ]

    print("Running stress test...")

    # Run all servers concurrently and collect results
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Analyze results
    successful_cleanups = sum(1 for r in results if r is True)
    failed_cleanups = sum(1 for r in results if r is False)
    exceptions = sum(1 for r in results if isinstance(r, Exception))

    print("Results:")
    print(f"  ✅ Successful cleanups: {successful_cleanups}/{num_servers}")
    print(f"  ❌ Failed cleanups: {failed_cleanups}/{num_servers}")
    print(f"  💥 Exceptions: {exceptions}/{num_servers}")

    if exceptions > 0:
        print("Exceptions encountered:")
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"  Server {i}: {result}")

    # Overall success if all cleanups worked
    overall_success = successful_cleanups == num_servers and exceptions == 0

    if overall_success:
        print("\n🎉 EXTREME CONCURRENCY STRESS TEST PASSED!")
        print("No task destruction warnings should have appeared above.")
    else:
        print("\n❌ STRESS TEST FAILED!")
        print(f"Only {successful_cleanups}/{num_servers} servers cleaned up properly")

    return overall_success


if __name__ == "__main__":
    try:
        success = asyncio.run(run_extreme_concurrency_stress_test())
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ TEST SUITE FAILED: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
