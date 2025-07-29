"""
Unit tests for RaftTaskManager improvements.

Tests the enhanced task lifecycle management, leak detection,
and graceful shutdown capabilities.
"""

import asyncio

import pytest

from mpreg.datastructures.raft_task_manager import (
    ManagedTask,
    RaftTaskManager,
    TaskGroup,
    TaskState,
)


class TestManagedTask:
    """Test ManagedTask dataclass functionality."""

    def test_managed_task_creation(self):
        """Test basic ManagedTask creation and properties."""
        task = ManagedTask(name="test_task")

        assert task.name == "test_task"
        assert task.state == TaskState.CREATED
        assert task.task is None
        assert not task.is_active()
        assert task.get_runtime() == 0.0

    @pytest.mark.asyncio
    async def test_managed_task_active_state(self):
        """Test ManagedTask active state detection."""
        task = ManagedTask(name="test_task")

        # Test inactive without task
        assert not task.is_active()

        # Create a real asyncio task
        async def long_running_task():
            await asyncio.sleep(10)

        real_task = asyncio.create_task(long_running_task())
        task.task = real_task
        task.state = TaskState.RUNNING

        assert task.is_active()

        # Test inactive states
        task.state = TaskState.STOPPED
        assert not task.is_active()

        # Cancel the task and test
        real_task.cancel()
        await asyncio.sleep(0.01)  # Give time for cancellation
        task.state = TaskState.RUNNING
        assert not task.is_active()  # Should be inactive because task is done


class TestTaskGroup:
    """Test TaskGroup functionality."""

    def test_task_group_creation(self):
        """Test TaskGroup creation and basic operations."""
        group = TaskGroup("test_group")

        assert group.name == "test_group"
        assert len(group.tasks) == 0
        assert len(group.get_active_tasks()) == 0

    def test_task_group_add_task(self):
        """Test adding tasks to a group."""
        group = TaskGroup("test_group")
        task = ManagedTask(name="test_task")

        group.add_task("test_task", task)

        assert len(group.tasks) == 1
        assert "test_task" in group.tasks
        assert group.tasks["test_task"] == task

    @pytest.mark.asyncio
    async def test_task_group_get_active_tasks(self):
        """Test getting active tasks from a group."""
        group = TaskGroup("test_group")

        # Add an active task
        active_task = ManagedTask(name="active_task")

        async def long_running_task():
            await asyncio.sleep(10)

        real_task = asyncio.create_task(long_running_task())
        active_task.task = real_task
        active_task.state = TaskState.RUNNING

        # Add an inactive task
        inactive_task = ManagedTask(name="inactive_task")
        inactive_task.state = TaskState.STOPPED

        group.add_task("active", active_task)
        group.add_task("inactive", inactive_task)

        active_tasks = group.get_active_tasks()
        assert len(active_tasks) == 1
        assert active_tasks[0] == active_task

        # Cleanup
        real_task.cancel()

    def test_task_group_get_task_count(self):
        """Test task count by state."""
        group = TaskGroup("test_group")

        # Add tasks in different states
        states_to_test = [
            TaskState.CREATED,
            TaskState.RUNNING,
            TaskState.CANCELLING,
            TaskState.STOPPING,
            TaskState.STOPPED,
            TaskState.FAILED,
        ]

        for i, state in enumerate(states_to_test):
            task = ManagedTask(name=f"task_{i}")
            task.state = state
            group.add_task(f"task_{i}", task)

        counts = group.get_task_count()

        assert counts.created == 1
        assert counts.running == 1
        assert counts.cancelling == 1
        assert counts.stopping == 1
        assert counts.stopped == 1
        assert counts.failed == 1


class TestRaftTaskManager:
    """Test RaftTaskManager core functionality."""

    @pytest.fixture
    def task_manager(self):
        """Create a task manager for testing."""
        return RaftTaskManager("test_node")

    def test_task_manager_initialization(self, task_manager):
        """Test task manager initialization."""
        assert task_manager.node_id == "test_node"
        assert len(task_manager.task_groups) == 3  # core, replication, maintenance
        assert "core" in task_manager.task_groups
        assert "replication" in task_manager.task_groups
        assert "maintenance" in task_manager.task_groups
        assert not task_manager._shutdown_in_progress

    @pytest.mark.asyncio
    async def test_create_task(self, task_manager):
        """Test task creation."""

        async def dummy_coro():
            await asyncio.sleep(0.1)

        managed_task = await task_manager.create_task("core", "test_task", dummy_coro)

        assert managed_task.name == "test_task"
        assert managed_task.state == TaskState.RUNNING
        assert managed_task.task is not None
        assert managed_task.task.get_name() == "test_node_core_test_task"

        # Wait for task to complete
        await asyncio.sleep(0.2)
        assert managed_task.task.done()

    @pytest.mark.asyncio
    async def test_duplicate_task_prevention(self, task_manager):
        """Test that duplicate tasks are properly handled."""

        async def long_running_task():
            await asyncio.sleep(1.0)

        # Create first task
        task1 = await task_manager.create_task(
            "core", "duplicate_test", long_running_task
        )

        assert task1.state == TaskState.RUNNING

        # Create duplicate task (should cancel first)
        task2 = await task_manager.create_task(
            "core", "duplicate_test", long_running_task
        )

        assert task2.state == TaskState.RUNNING
        assert task1.state == TaskState.CANCELLING

        # Cleanup
        await task_manager.stop_all_tasks()

    @pytest.mark.asyncio
    async def test_graceful_task_cancellation(self, task_manager):
        """Test graceful task cancellation."""

        async def cancellable_task():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                # Simulate cleanup work
                await asyncio.sleep(0.1)
                raise

        managed_task = await task_manager.create_task(
            "core", "cancellable_task", cancellable_task
        )

        # Let task start
        await asyncio.sleep(0.1)
        assert managed_task.state == TaskState.RUNNING

        # Gracefully cancel it
        await task_manager._graceful_cancel_task(managed_task, timeout=1.0)

        assert managed_task.state == TaskState.STOPPED
        assert managed_task.stopped_at is not None

    @pytest.mark.asyncio
    async def test_stop_task_group(self, task_manager):
        """Test stopping a task group."""
        # Create multiple tasks in core group
        for i in range(3):
            await task_manager.create_task("core", f"task_{i}", asyncio.sleep, 1.0)

        status = task_manager.get_task_status()
        assert status.groups[0].active_count == 3

        # Stop the core group
        await task_manager.stop_task_group("core", timeout=1.0)

        # Check all tasks are stopped
        status = task_manager.get_task_status()
        core_group = next(g for g in status.groups if g.name == "core")
        assert core_group.active_count == 0

    @pytest.mark.asyncio
    async def test_stop_all_tasks(self, task_manager):
        """Test stopping all tasks across all groups."""
        # Create tasks in different groups
        await task_manager.create_task("core", "core_task", asyncio.sleep, 1.0)
        await task_manager.create_task("replication", "repl_task", asyncio.sleep, 1.0)
        await task_manager.create_task("maintenance", "maint_task", asyncio.sleep, 1.0)

        status = task_manager.get_task_status()
        assert status.active_tasks == 3

        # Stop all tasks
        await task_manager.stop_all_tasks(timeout=2.0)

        # Check all tasks are stopped
        status = task_manager.get_task_status()
        assert status.active_tasks == 0

    @pytest.mark.asyncio
    async def test_detect_leaked_tasks(self, task_manager):
        """Test leak detection for stale tasks."""
        # Create a task that appears to be leaked
        leaked_task = ManagedTask(name="leaked_task")

        async def completed_task():
            pass  # Task that completes immediately

        real_leaked_task = asyncio.create_task(completed_task())
        await real_leaked_task  # Wait for it to complete
        leaked_task.task = real_leaked_task  # Task is done but still tracked
        leaked_task.state = TaskState.CANCELLING  # But state shows cancelling

        task_manager.task_groups["core"].add_task("leaked_task", leaked_task)

        leaked_names = task_manager.detect_leaked_tasks()
        assert "core.leaked_task" in leaked_names

    @pytest.mark.asyncio
    async def test_cleanup_finished_tasks(self, task_manager):
        """Test cleanup of finished tasks."""
        # Add finished task
        finished_task = ManagedTask(name="finished_task")

        async def completed_task():
            pass  # Task that completes immediately

        real_finished_task = asyncio.create_task(completed_task())
        await real_finished_task  # Wait for it to complete
        finished_task.task = real_finished_task
        finished_task.state = TaskState.STOPPED

        # Add active task
        active_task = ManagedTask(name="active_task")

        async def long_running_task():
            await asyncio.sleep(10)

        real_active_task = asyncio.create_task(long_running_task())
        active_task.task = real_active_task
        active_task.state = TaskState.RUNNING

        task_manager.task_groups["core"].add_task("finished", finished_task)
        task_manager.task_groups["core"].add_task("active", active_task)

        # Should remove 1 finished task
        removed_count = task_manager.cleanup_finished_tasks()
        assert removed_count == 1
        assert "finished" not in task_manager.task_groups["core"].tasks
        assert "active" in task_manager.task_groups["core"].tasks

        # Cleanup
        real_active_task.cancel()

    @pytest.mark.asyncio
    async def test_force_cleanup_all(self, task_manager):
        """Test emergency force cleanup."""
        # Create some tasks
        await task_manager.create_task("core", "task1", asyncio.sleep, 10)
        await task_manager.create_task("replication", "task2", asyncio.sleep, 10)

        status = task_manager.get_task_status()
        assert status.active_tasks == 2

        # Force cleanup
        cancelled_count = await task_manager.force_cleanup_all()
        assert cancelled_count == 2

        # All task groups should be empty
        for group in task_manager.task_groups.values():
            assert len(group.tasks) == 0

    @pytest.mark.asyncio
    async def test_get_task_status(self, task_manager):
        """Test comprehensive task status reporting."""
        # Add tasks in different states
        task1 = ManagedTask(name="running_task")
        task1.state = TaskState.RUNNING

        async def long_running_task():
            await asyncio.sleep(10)

        real_task = asyncio.create_task(long_running_task())
        task1.task = real_task

        task2 = ManagedTask(name="stopped_task")
        task2.state = TaskState.STOPPED

        task_manager.task_groups["core"].add_task("running", task1)
        task_manager.task_groups["core"].add_task("stopped", task2)

        status = task_manager.get_task_status()

        assert status.node_id == "test_node"
        assert status.total_tasks == 2
        assert status.active_tasks == 1
        assert not status.shutdown_in_progress

        core_group = next(g for g in status.groups if g.name == "core")
        assert core_group.task_count == 2
        assert core_group.active_count == 1
        assert core_group.state_counts.running == 1
        assert core_group.state_counts.stopped == 1

        # Cleanup
        real_task.cancel()


class TestTaskManagerErrorHandling:
    """Test error handling in task manager."""

    @pytest.fixture
    def task_manager(self):
        """Create a task manager for testing."""
        return RaftTaskManager("test_node")

    @pytest.mark.asyncio
    async def test_graceful_cancel_task_timeout(self, task_manager):
        """Test graceful cancellation with timeout."""

        async def stubborn_task():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                # Simulate task that doesn't respond to cancellation quickly
                await asyncio.sleep(2.0)  # Longer than timeout
                raise

        managed_task = await task_manager.create_task(
            "core", "stubborn_task", stubborn_task
        )

        # Cancel with short timeout
        await task_manager._graceful_cancel_task(managed_task, timeout=0.5)

        # Should still be marked as stopped even though it timed out
        assert managed_task.state == TaskState.STOPPED

    @pytest.mark.asyncio
    async def test_graceful_cancel_task_exception(self, task_manager):
        """Test graceful cancellation when task raises unexpected exception."""

        async def failing_task():
            await asyncio.sleep(0.1)
            raise ValueError("Task failed")

        managed_task = await task_manager.create_task(
            "core", "failing_task", failing_task
        )

        # Wait for task to fail
        await asyncio.sleep(0.2)

        # Graceful cancel should handle already-failed task
        await task_manager._graceful_cancel_task(managed_task, timeout=1.0)

        assert managed_task.state == TaskState.STOPPED

    @pytest.mark.asyncio
    async def test_stop_nonexistent_group(self, task_manager):
        """Test stopping a group that doesn't exist."""
        # Should not raise exception
        await task_manager.stop_task_group("nonexistent", timeout=1.0)

    @pytest.mark.asyncio
    async def test_stop_empty_group(self, task_manager):
        """Test stopping an empty group."""
        # Should not raise exception
        await task_manager.stop_task_group("core", timeout=1.0)

        status = task_manager.get_task_status()
        core_group = next(g for g in status.groups if g.name == "core")
        assert core_group.active_count == 0
