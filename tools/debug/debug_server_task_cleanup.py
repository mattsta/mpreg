#!/usr/bin/env python3
"""
Debug script to test server task cleanup and verify no "Task was destroyed but it is pending!" warnings.

This script tests the MPREGServer task management system to ensure proper cleanup during shutdown.
"""

import asyncio
import logging
import sys
import warnings
from pathlib import Path

# Suppress specific warnings during testing
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from mpreg.server import MPREGServer, MPREGSettings


async def test_server_task_cleanup():
    """Test that server tasks are properly cleaned up without warnings."""

    print("=== SERVER TASK CLEANUP TEST ===")
    print()

    # Create server settings for testing
    settings = MPREGSettings(
        host="127.0.0.1",
        port=8887,  # Use a different port to avoid conflicts
        name="cleanup-test-server",
        cluster_id="test-cluster",
    )

    server = MPREGServer(settings)

    try:
        print("1. Creating server...")

        # Start the server's core functionality without the blocking server() method
        print("2. Starting background tasks...")

        # Initialize the server components manually
        server._register_default_commands()
        server._discover_and_register_rpc_commands()

        # Start the peer connection manager task (this is what was causing issues)
        print("3. Starting peer connection manager...")
        peer_task = asyncio.create_task(server._manage_peer_connections())
        server._background_tasks.add(peer_task)
        peer_task.add_done_callback(server._background_tasks.discard)

        print(f"   - Background tasks started: {len(server._background_tasks)}")

        # Let it run briefly to simulate normal operation
        print("4. Letting server run for 0.5 seconds...")
        await asyncio.sleep(0.5)

        print(f"   - Background tasks running: {len(server._background_tasks)}")

        # Test graceful shutdown
        print("5. Testing graceful shutdown...")

        # Signal shutdown
        server.shutdown()

        # Wait a moment for shutdown signal to propagate
        await asyncio.sleep(0.1)

        # Perform async cleanup
        await server.shutdown_async()

        print("6. Checking task cleanup...")
        print(f"   - Remaining background tasks: {len(server._background_tasks)}")

        # Verify all tasks are cleaned up
        if len(server._background_tasks) == 0:
            print("✅ All background tasks properly cleaned up!")
        else:
            remaining_tasks = []
            for task in server._background_tasks:
                task_name = getattr(task, "__name__", "unnamed")
                coro_name = (
                    getattr(task.get_coro(), "__name__", "unknown")
                    if hasattr(task, "get_coro")
                    else "no_coro"
                )
                task_status = "done" if task.done() else "pending"
                remaining_tasks.append(f"{task_name}/{coro_name} ({task_status})")

            print(f"⚠️  {len(server._background_tasks)} tasks still tracked:")
            for task_info in remaining_tasks:
                print(f"     - {task_info}")

        print()
        print("✅ SERVER TASK CLEANUP TEST COMPLETED")

    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Ensure complete cleanup even if test fails
        try:
            server.shutdown()
            await server.shutdown_async()
        except Exception as cleanup_error:
            print(f"⚠️  Cleanup error: {cleanup_error}")


async def test_multiple_server_cycles():
    """Test multiple server start/stop cycles to detect task leaks."""

    print("\n=== MULTIPLE SERVER CYCLES TEST ===")
    print()

    for cycle in range(3):
        print(f"Cycle {cycle + 1}/3:")

        settings = MPREGSettings(
            host="127.0.0.1",
            port=8886 - cycle,  # Different ports for each cycle
            name=f"cycle-test-server-{cycle}",
            cluster_id=f"test-cluster-{cycle}",
        )

        server = MPREGServer(settings)

        try:
            # Start server components
            server._register_default_commands()
            server._discover_and_register_rpc_commands()

            # Start background tasks
            peer_task = asyncio.create_task(server._manage_peer_connections())
            server._background_tasks.add(peer_task)
            peer_task.add_done_callback(server._background_tasks.discard)

            # Brief operation
            await asyncio.sleep(0.2)

            # Shutdown
            server.shutdown()
            await server.shutdown_async()

            # Verify cleanup
            if len(server._background_tasks) == 0:
                print(f"  ✅ Cycle {cycle + 1}: Clean shutdown")
            else:
                print(
                    f"  ❌ Cycle {cycle + 1}: {len(server._background_tasks)} tasks remain"
                )

        except Exception as e:
            print(f"  ❌ Cycle {cycle + 1}: Error - {e}")

    print("\n✅ MULTIPLE CYCLES TEST COMPLETED")


if __name__ == "__main__":
    # Set up logging to catch task destruction warnings
    logging.basicConfig(level=logging.WARNING)

    # Run the tests
    try:
        asyncio.run(test_server_task_cleanup())
        asyncio.run(test_multiple_server_cycles())
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback

        traceback.print_exc()
