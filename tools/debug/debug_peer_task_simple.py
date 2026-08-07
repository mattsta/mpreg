#!/usr/bin/env python3
"""
Simple test specifically for the _manage_peer_connections task cleanup fix.

This test bypasses server initialization and directly tests the peer connection task.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from mpreg.server import MPREGServer, MPREGSettings


async def test_peer_connection_task_only():
    """Test specifically the _manage_peer_connections task lifecycle."""

    print("=== PEER CONNECTION TASK LIFECYCLE TEST ===")
    print()

    # Create minimal server
    settings = MPREGSettings(
        host="127.0.0.1", port=8999, name="peer-task-test", cluster_id="test"
    )

    server = MPREGServer(settings)

    print("1. Server created")
    print(f"   Initial background tasks: {len(server._background_tasks)}")

    # Start ONLY the peer connection manager task (skip RPC registration)
    print("2. Starting peer connection manager task...")
    peer_task = asyncio.create_task(server._manage_peer_connections())
    server._background_tasks.add(peer_task)
    peer_task.add_done_callback(server._background_tasks.discard)

    print(f"   Background tasks count: {len(server._background_tasks)}")

    # Let it run briefly
    print("3. Running task for 0.3 seconds...")
    await asyncio.sleep(0.3)

    # Check task is still running
    running_count = sum(1 for task in server._background_tasks if not task.done())
    print(f"   Running tasks: {running_count}/{len(server._background_tasks)}")

    # Test graceful shutdown
    print("4. Testing graceful shutdown...")

    # Signal shutdown first
    server.shutdown()  # Sets shutdown event

    # Brief delay to let task respond to shutdown signal
    await asyncio.sleep(0.1)

    # Now call shutdown_async which should wait for graceful completion
    await server.shutdown_async()

    # Check final state
    final_task_count = len(server._background_tasks)
    print(f"5. Final task count: {final_task_count}")

    if final_task_count == 0:
        print("✅ SUCCESS: Task completed gracefully without warnings!")
        return True
    else:
        print(f"⚠️  WARNING: {final_task_count} tasks still tracked")
        for task in server._background_tasks:
            task_name = getattr(task.get_coro(), "__name__", "unknown")
            task_status = "done" if task.done() else "pending"
            print(f"   - {task_name} ({task_status})")
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(test_peer_connection_task_only())
        if success:
            print("\n🎉 PEER TASK LIFECYCLE TEST PASSED!")
        else:
            print("\n❌ PEER TASK LIFECYCLE TEST FAILED!")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ TEST FAILED WITH EXCEPTION: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
