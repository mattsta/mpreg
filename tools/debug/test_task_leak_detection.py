#!/usr/bin/env python3
"""
Test script to verify our improved task management and leak detection.
"""

import asyncio
import sys

sys.path.insert(0, "/Users/matt/repos/mpreg")

from mpreg.datastructures.raft_task_manager import RaftTaskManager, TaskState
from tests.conftest import AsyncTestContext


async def test_task_leak_detection():
    """Test the enhanced task management system."""

    print("=== TASK LEAK DETECTION TEST ===")

    # Test 1: Basic task lifecycle
    print("\n1. Testing basic task lifecycle...")
    task_manager = RaftTaskManager("test_node")

    # Create a simple task
    managed_task = await task_manager.create_task(
        "core", "test_task", asyncio.sleep, 0.1
    )

    print(f"   Created task: {managed_task.name} in state {managed_task.state.value}")
    assert managed_task.state == TaskState.RUNNING

    # Wait for task to complete
    await asyncio.sleep(0.2)

    # Check leak detection
    leaked = task_manager.detect_leaked_tasks()
    print(f"   Leaked tasks detected: {leaked}")

    # Cleanup finished tasks
    cleaned = task_manager.cleanup_finished_tasks()
    print(f"   Cleaned up {cleaned} finished tasks")

    # Test 2: Duplicate task prevention
    print("\n2. Testing duplicate task prevention...")

    # Create first task
    task1 = await task_manager.create_task(
        "core",
        "duplicate_test",
        asyncio.sleep,
        10,  # Long-running
    )
    print(f"   Created first task: {task1.state.value}")

    # Create duplicate (should cancel first)
    task2 = await task_manager.create_task("core", "duplicate_test", asyncio.sleep, 0.1)
    print(f"   Created duplicate task: {task2.state.value}")
    print(f"   First task state: {task1.state.value}")

    # Test 3: Graceful shutdown
    print("\n3. Testing graceful shutdown...")

    # Create multiple tasks
    for i in range(3):
        await task_manager.create_task(
            "maintenance", f"shutdown_test_{i}", asyncio.sleep, 1.0
        )

    status = task_manager.get_task_status()
    print(f"   Created {status.total_tasks} total tasks, {status.active_tasks} active")

    # Shutdown all tasks
    await task_manager.stop_all_tasks(timeout=2.0)

    final_status = task_manager.get_task_status()
    print(
        f"   After shutdown: {final_status.total_tasks} total, {final_status.active_tasks} active"
    )

    # Test 4: Test context leak detection
    print("\n4. Testing AsyncTestContext leak detection...")

    async with AsyncTestContext() as ctx:
        # Create a leaked task intentionally
        leaked_task = asyncio.create_task(asyncio.sleep(10), name="intentional_leak")

        # Context should detect and clean it up on exit
        print("   Created intentional leak, context will clean up...")

    print("✅ All tests completed successfully!")


if __name__ == "__main__":
    asyncio.run(test_task_leak_detection())
