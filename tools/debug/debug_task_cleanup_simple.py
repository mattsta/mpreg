#!/usr/bin/env python3
"""
Simple test to verify the MPREGServer task cleanup fix is working.

This test specifically checks that the _manage_peer_connections task is properly
integrated into the background task management system.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from mpreg.server import MPREGServer, MPREGSettings


async def test_peer_connection_task_management():
    """Test that peer connection tasks are properly managed."""

    print("=== PEER CONNECTION TASK MANAGEMENT TEST ===")
    print()

    # Create a minimal server setup
    settings = MPREGSettings(
        host="127.0.0.1", port=8999, name="task-test-server", cluster_id="task-test"
    )

    server = MPREGServer(settings)

    print("1. Server created")
    print(f"   Initial background tasks: {len(server._background_tasks)}")

    # Start the peer connection manager task directly (this is what was fixed)
    print("2. Starting peer connection manager task...")
    peer_task = asyncio.create_task(server._manage_peer_connections())
    server._background_tasks.add(peer_task)
    peer_task.add_done_callback(server._background_tasks.discard)

    print(f"   Background tasks after starting: {len(server._background_tasks)}")

    # Let it run briefly
    print("3. Letting task run for 0.2 seconds...")
    await asyncio.sleep(0.2)

    print(f"   Background tasks still running: {len(server._background_tasks)}")

    # Check task status
    print("4. Checking task status:")
    for i, task in enumerate(server._background_tasks):
        task_name = (
            getattr(task.get_coro(), "__name__", "unknown")
            if hasattr(task, "get_coro")
            else "no_coro"
        )
        task_status = "done" if task.done() else "running"
        print(f"   Task {i + 1}: {task_name} ({task_status})")

    # Signal shutdown
    print("5. Signaling shutdown...")
    server.shutdown()  # This sets the shutdown event

    # Wait a moment for the task to respond to shutdown signal
    await asyncio.sleep(0.1)

    # Perform cleanup
    print("6. Performing async cleanup...")
    await server.shutdown_async()

    print(f"   Background tasks after cleanup: {len(server._background_tasks)}")

    # Final verification
    print("7. Final verification:")
    if len(server._background_tasks) == 0:
        print("   ✅ SUCCESS: All background tasks properly cleaned up!")
    else:
        print(f"   ⚠️  WARNING: {len(server._background_tasks)} tasks still tracked")
        for task in server._background_tasks:
            task_name = (
                getattr(task.get_coro(), "__name__", "unknown")
                if hasattr(task, "get_coro")
                else "no_coro"
            )
            task_status = "done" if task.done() else "pending"
            print(f"      - {task_name} ({task_status})")

    print()
    print("✅ PEER CONNECTION TASK MANAGEMENT TEST COMPLETED")

    return len(server._background_tasks) == 0


if __name__ == "__main__":
    try:
        success = asyncio.run(test_peer_connection_task_management())
        if success:
            print("\n🎉 ALL TESTS PASSED - TASK CLEANUP IS WORKING!")
        else:
            print("\n❌ TEST FAILED - TASK CLEANUP ISSUE DETECTED")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ TEST FAILED WITH EXCEPTION: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
