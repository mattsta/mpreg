#!/usr/bin/env python3
"""
Comprehensive test for all shutdown improvements.
"""

import asyncio
import sys

sys.path.insert(0, "/Users/matt/repos/mpreg")

from mpreg.datastructures.raft_task_manager import RaftTaskManager


async def test_comprehensive_shutdown():
    """Test comprehensive shutdown behavior."""

    print("=== COMPREHENSIVE SHUTDOWN TEST ===")

    # Test 1: Task Manager Force Cleanup
    print("\n1. Testing task manager force cleanup...")
    task_manager = RaftTaskManager("test_node")

    # Create several long-running tasks
    for i in range(5):
        await task_manager.create_task("core", f"long_task_{i}", asyncio.sleep, 10.0)
        await task_manager.create_task(
            "replication", f"repl_task_{i}", asyncio.sleep, 10.0
        )

    status = task_manager.get_task_status()
    print(f"   Created {status.active_tasks} active tasks")

    # Force cleanup all tasks
    cancelled = await task_manager.force_cleanup_all()
    print(f"   Force cancelled {cancelled} tasks")

    final_status = task_manager.get_task_status()
    print(f"   Final active tasks: {final_status.active_tasks}")
    assert final_status.active_tasks == 0

    # Test 2: Election Coordinator Lifecycle
    print("\n2. Testing election coordinator lifecycle...")

    from mpreg.datastructures.production_raft_implementation import ElectionCoordinator

    coordinators = []
    for i in range(3):
        task_mgr = RaftTaskManager(f"coord_node_{i}")
        coordinator = ElectionCoordinator()
        coordinator.task_manager = task_mgr

        # Mock election callback
        async def mock_callback():
            await asyncio.sleep(0.05)  # Simulate work

        # Start coordinator
        await coordinator.start_coordinator(f"coord_node_{i}", mock_callback)
        coordinators.append((coordinator, task_mgr))

    print(f"   Started {len(coordinators)} election coordinators")

    # Let them run briefly
    await asyncio.sleep(0.2)

    # Stop all coordinators
    for coordinator, task_mgr in coordinators:
        await coordinator.stop_coordinator()

        # Verify cleanup
        status = task_mgr.get_task_status()
        print(f"   Coordinator active tasks after stop: {status.active_tasks}")
        assert status.active_tasks == 0

    # Test 3: Stress Test - Rapid Start/Stop Cycles
    print("\n3. Testing rapid start/stop cycles...")

    for cycle in range(5):
        print(f"   Cycle {cycle + 1}/5")

        # Create task manager and coordinator
        stress_mgr = RaftTaskManager(f"stress_node_{cycle}")
        stress_coord = ElectionCoordinator()
        stress_coord.task_manager = stress_mgr

        async def stress_callback():
            await asyncio.sleep(0.01)

        # Rapid start/stop
        await stress_coord.start_coordinator(f"stress_node_{cycle}", stress_callback)
        await asyncio.sleep(0.05)  # Very brief run
        await stress_coord.stop_coordinator()

        # Verify complete cleanup
        final_status = stress_mgr.get_task_status()
        assert final_status.active_tasks == 0

    print("\n✅ All comprehensive shutdown tests passed!")


if __name__ == "__main__":
    asyncio.run(test_comprehensive_shutdown())
