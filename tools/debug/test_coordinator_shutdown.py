#!/usr/bin/env python3
"""
Test script to verify proper election coordinator shutdown.
"""

import asyncio
import sys

sys.path.insert(0, "/Users/matt/repos/mpreg")

from mpreg.datastructures.production_raft_implementation import ElectionCoordinator
from mpreg.datastructures.raft_task_manager import RaftTaskManager


async def test_coordinator_shutdown():
    """Test the enhanced coordinator shutdown system."""

    print("=== COORDINATOR SHUTDOWN TEST ===")

    # Test 1: Basic coordinator lifecycle
    print("\n1. Testing basic coordinator lifecycle...")
    task_manager = RaftTaskManager("test_node")
    coordinator = ElectionCoordinator()
    coordinator.task_manager = task_manager

    # Mock election callback
    election_count = 0

    async def mock_election_callback():
        nonlocal election_count
        election_count += 1
        print(f"   Mock election callback called ({election_count})")
        await asyncio.sleep(0.05)  # Simulate some work

    # Start coordinator
    await coordinator.start_coordinator("test_node", mock_election_callback)
    print("   Coordinator started")

    # Let it run for a bit
    await asyncio.sleep(0.3)
    print(f"   Election callbacks triggered: {election_count}")

    # Test graceful shutdown
    print("   Stopping coordinator...")
    await coordinator.stop_coordinator()
    print("   Coordinator stopped")

    # Verify task manager state
    status = task_manager.get_task_status()
    print(f"   Active tasks after shutdown: {status.active_tasks}")

    # Test 2: Force cleanup
    print("\n2. Testing force cleanup...")
    await task_manager.force_cleanup_all()

    final_status = task_manager.get_task_status()
    print(f"   Final active tasks: {final_status.active_tasks}")

    print("\n✅ All coordinator shutdown tests completed!")


if __name__ == "__main__":
    asyncio.run(test_coordinator_shutdown())
