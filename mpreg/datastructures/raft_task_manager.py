"""
Centralized Task Management for Production Raft Implementation.

This module provides a clean abstraction for managing all async tasks
in the Raft consensus algorithm, replacing scattered task state with
a centralized, lifecycle-aware task manager.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

task_log = logger


class TaskState(Enum):
    """States for managed tasks."""

    CREATED = "created"
    RUNNING = "running"
    CANCELLING = "cancelling"  # Task cancellation initiated
    STOPPING = "stopping"  # Graceful shutdown in progress
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass
class ManagedTask:
    """Container for a managed async task with lifecycle tracking."""

    name: str
    task: asyncio.Task | None = None
    state: TaskState = TaskState.CREATED
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    stopped_at: float | None = None
    failure_reason: BaseException | None = None

    def is_active(self) -> bool:
        """Check if task is actively running."""
        return (
            self.task is not None
            and not self.task.done()
            and self.state == TaskState.RUNNING
        )

    def get_runtime(self) -> float:
        """Get task runtime in seconds."""
        if self.started_at is None:
            return 0.0
        end_time = self.stopped_at or time.time()
        return end_time - self.started_at


@dataclass
class TaskStateCounts:
    """Count of tasks by state."""

    created: int = 0
    running: int = 0
    cancelling: int = 0
    stopping: int = 0
    stopped: int = 0
    failed: int = 0


@dataclass
class TaskInfo:
    """Information about a single task."""

    name: str
    state: str
    runtime: float
    is_active: bool


@dataclass
class TaskGroupStatus:
    """Status information for a task group."""

    name: str
    task_count: int
    active_count: int
    state_counts: TaskStateCounts
    tasks: list[TaskInfo]


@dataclass
class TaskManagerStatus:
    """Complete status of the task manager."""

    node_id: str
    shutdown_in_progress: bool
    total_tasks: int
    active_tasks: int
    groups: list[TaskGroupStatus]


@dataclass
class TaskGroup:
    """Group of related tasks that can be managed together."""

    name: str
    tasks: dict[str, ManagedTask] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

    def add_task(self, task_name: str, task: ManagedTask) -> None:
        """Add a task to this group."""
        self.tasks[task_name] = task

    def get_active_tasks(self) -> list[ManagedTask]:
        """Get all active tasks in this group."""
        return [task for task in self.tasks.values() if task.is_active()]

    def get_task_count(self) -> TaskStateCounts:
        """Get count of tasks by state."""
        counts = TaskStateCounts()
        for task in self.tasks.values():
            if task.state == TaskState.CREATED:
                counts.created += 1
            elif task.state == TaskState.RUNNING:
                counts.running += 1
            elif task.state == TaskState.CANCELLING:
                counts.cancelling += 1
            elif task.state == TaskState.STOPPING:
                counts.stopping += 1
            elif task.state == TaskState.STOPPED:
                counts.stopped += 1
            elif task.state == TaskState.FAILED:
                counts.failed += 1
        return counts


class RaftTaskManager:
    """
    Centralized task manager for Raft consensus implementation.

    Provides clean lifecycle management for all async tasks including:
    - Heartbeat/replication tasks
    - Election coordination
    - Log application
    - Background maintenance

    Replaces scattered task attributes with organized, observable state.
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.task_groups: dict[str, TaskGroup] = {}
        self.shutdown_timeout = 2.0
        self._shutdown_in_progress = False

        # Create standard task groups
        self._create_standard_groups()

    def _create_standard_groups(self) -> None:
        """Create standard task groups for Raft operations."""
        self.task_groups = {
            "core": TaskGroup("Core Raft Tasks"),  # heartbeat, election, apply
            "replication": TaskGroup("Log Replication"),  # per-follower replication
            "maintenance": TaskGroup("Background Maintenance"),  # cleanup, metrics
        }

    async def create_task(
        self,
        group_name: str,
        task_name: str,
        coro_func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> ManagedTask:
        """
        Create and register a new managed task.

        Args:
            group_name: Name of task group
            task_name: Unique name for this task
            coro_func: Coroutine function to execute
            *args, **kwargs: Arguments for coroutine function

        Returns:
            ManagedTask instance
        """
        if group_name not in self.task_groups:
            self.task_groups[group_name] = TaskGroup(group_name)

        # Cancel existing task with same name if it exists
        existing = self.task_groups[group_name].tasks.get(task_name)
        if existing and existing.is_active():
            task_log.warning(
                f"[{self.node_id}] Duplicate task {group_name}.{task_name} detected - cancelling existing"
            )
            # Use immediate cancel to avoid blocking task creation
            self._cancel_task_immediate(existing)
            # Small delay to allow cancellation to propagate
            await asyncio.sleep(0.01)

        # Create new task
        managed_task = ManagedTask(name=task_name)
        coro = coro_func(*args, **kwargs)
        try:
            managed_task.task = asyncio.create_task(
                coro, name=f"{self.node_id}_{group_name}_{task_name}"
            )
        except RuntimeError:
            # Avoid "coroutine was never awaited" when no loop is running.
            coro.close()
            raise
        managed_task.state = TaskState.RUNNING
        managed_task.started_at = time.time()

        def _on_task_done(task: asyncio.Task[Any]) -> None:
            if task.cancelled():
                if managed_task.state in (TaskState.RUNNING, TaskState.CANCELLING):
                    managed_task.state = TaskState.STOPPED
                managed_task.stopped_at = time.time()
                return
            exc = task.exception()
            if exc is not None:
                managed_task.failure_reason = exc
                managed_task.state = TaskState.FAILED
            else:
                managed_task.state = TaskState.STOPPED
            managed_task.stopped_at = time.time()

        managed_task.task.add_done_callback(_on_task_done)

        # Register with group
        self.task_groups[group_name].add_task(task_name, managed_task)

        task_log.debug(f"[{self.node_id}] Created task {group_name}.{task_name}")
        return managed_task

    async def stop_task_group(
        self, group_name: str, timeout: float | None = None
    ) -> None:
        """Stop all tasks in a specific group."""
        if group_name not in self.task_groups:
            return

        group = self.task_groups[group_name]
        active_tasks = group.get_active_tasks()

        if not active_tasks:
            return

        timeout = timeout or self.shutdown_timeout
        task_log.debug(
            f"[{self.node_id}] Stopping {len(active_tasks)} tasks in group {group_name}"
        )

        # Use graceful cancellation for better cleanup
        grace_timeout = (
            timeout / len(active_tasks) if len(active_tasks) > 1 else timeout
        )
        grace_timeout = min(grace_timeout, 0.5)  # Cap at 500ms per task

        for managed_task in active_tasks:
            try:
                await self._graceful_cancel_task(managed_task, timeout=grace_timeout)
            except Exception as e:
                task_log.warning(
                    f"[{self.node_id}] Error cancelling task {managed_task.name}: {e}"
                )
                # Fall back to immediate cancellation
                self._cancel_task_immediate(managed_task)
                managed_task.state = TaskState.STOPPED
                managed_task.stopped_at = time.time()

        task_log.debug(f"[{self.node_id}] Group {group_name} shutdown complete")

    async def stop_specific_task(
        self, group_name: str, task_name: str, timeout: float | None = None
    ) -> bool:
        """Stop a specific task by name within a group."""
        if group_name not in self.task_groups:
            task_log.debug(
                f"[{self.node_id}] Group {group_name} not found for task stop"
            )
            return False

        group = self.task_groups[group_name]
        if task_name not in group.tasks:
            task_log.debug(f"[{self.node_id}] Task {group_name}.{task_name} not found")
            return False

        managed_task = group.tasks[task_name]
        if not managed_task.is_active():
            task_log.debug(
                f"[{self.node_id}] Task {group_name}.{task_name} already inactive"
            )
            return True

        timeout = timeout or self.shutdown_timeout
        task_log.debug(f"[{self.node_id}] Stopping task {group_name}.{task_name}")

        try:
            await self._graceful_cancel_task(managed_task, timeout=timeout)
            task_log.debug(
                f"[{self.node_id}] Task {group_name}.{task_name} stopped successfully"
            )
            return True
        except Exception as e:
            task_log.warning(
                f"[{self.node_id}] Error stopping task {group_name}.{task_name}: {e}"
            )
            # Fall back to immediate cancellation
            self._cancel_task_immediate(managed_task)
            managed_task.state = TaskState.STOPPED
            managed_task.stopped_at = time.time()
            return True

    async def stop_all_tasks(self, timeout: float | None = None) -> None:
        """Stop all managed tasks across all groups."""
        if self._shutdown_in_progress:
            return

        self._shutdown_in_progress = True
        timeout = timeout or self.shutdown_timeout

        try:
            task_log.info(f"[{self.node_id}] Stopping all task groups")

            # Stop groups in order: maintenance -> replication -> core
            stop_order = ["maintenance", "replication", "core"]

            for group_name in stop_order:
                if group_name in self.task_groups:
                    await self.stop_task_group(
                        group_name, timeout=timeout / len(stop_order)
                    )

            # Stop any other groups
            for group_name in self.task_groups:
                if group_name not in stop_order:
                    await self.stop_task_group(group_name, timeout=0.5)

            task_log.info(f"[{self.node_id}] All tasks stopped")

        finally:
            self._shutdown_in_progress = False

    async def _graceful_cancel_task(
        self, managed_task: ManagedTask, timeout: float = 1.0
    ) -> None:
        """Cancel task with grace period for cleanup."""
        if not managed_task.task or managed_task.task.done():
            managed_task.state = TaskState.STOPPED
            managed_task.stopped_at = time.time()
            return

        task_log.debug(
            f"[{self.node_id}] Gracefully cancelling task {managed_task.name}"
        )
        managed_task.state = TaskState.CANCELLING
        managed_task.task.cancel()

        try:
            # Give task a chance to cleanup gracefully
            await asyncio.wait_for(managed_task.task, timeout=timeout)
        except TimeoutError, asyncio.CancelledError:
            # Expected - task was cancelled or took too long
            task_log.debug(f"[{self.node_id}] Task {managed_task.name} cancelled")
        except Exception as e:
            task_log.warning(
                f"[{self.node_id}] Task {managed_task.name} failed during cancellation: {e}"
            )
            managed_task.failure_reason = e
            managed_task.state = TaskState.FAILED
            return
        finally:
            if managed_task.state == TaskState.CANCELLING:
                managed_task.state = TaskState.STOPPED
            managed_task.stopped_at = time.time()

    def _cancel_task_immediate(self, managed_task: ManagedTask) -> None:
        """Cancel a task immediately without waiting."""
        if managed_task.task and not managed_task.task.done():
            managed_task.task.cancel()
            managed_task.state = TaskState.CANCELLING

    def get_task_status(self) -> TaskManagerStatus:
        """Get comprehensive status of all managed tasks."""
        total_tasks = 0
        active_tasks = 0
        group_statuses: list[TaskGroupStatus] = []

        for group_name, group in self.task_groups.items():
            group_active = group.get_active_tasks()
            group_counts = group.get_task_count()

            task_infos = [
                TaskInfo(
                    name=name,
                    state=task.state.value,
                    runtime=task.get_runtime(),
                    is_active=task.is_active(),
                )
                for name, task in group.tasks.items()
            ]

            group_status = TaskGroupStatus(
                name=group_name,
                task_count=len(group.tasks),
                active_count=len(group_active),
                state_counts=group_counts,
                tasks=task_infos,
            )
            group_statuses.append(group_status)

            total_tasks += len(group.tasks)
            active_tasks += len(group_active)

        return TaskManagerStatus(
            node_id=self.node_id,
            shutdown_in_progress=self._shutdown_in_progress,
            total_tasks=total_tasks,
            active_tasks=active_tasks,
            groups=group_statuses,
        )

    def cleanup_finished_tasks(self) -> int:
        """Remove finished tasks from tracking. Returns count of removed tasks."""
        removed_count = 0

        for group in self.task_groups.values():
            to_remove = []

            for task_name, managed_task in group.tasks.items():
                if (
                    managed_task.task
                    and managed_task.task.done()
                    and managed_task.state in (TaskState.STOPPED, TaskState.FAILED)
                ):
                    # Task is completely finished, safe to remove
                    to_remove.append(task_name)

            for task_name in to_remove:
                del group.tasks[task_name]
                removed_count += 1

        if removed_count > 0:
            task_log.debug(
                f"[{self.node_id}] Cleaned up {removed_count} finished tasks"
            )

        return removed_count

    def detect_leaked_tasks(self) -> list[str]:
        """Detect potentially leaked tasks that should have been cleaned up."""
        leaked_task_names = []

        for group_name, group in self.task_groups.items():
            for task_name, managed_task in group.tasks.items():
                # Tasks that have been cancelled but are still tracked
                if (
                    managed_task.state in (TaskState.CANCELLING, TaskState.STOPPING)
                    and managed_task.task
                    and managed_task.task.done()
                ):
                    leaked_task_names.append(f"{group_name}.{task_name}")

                # Tasks that have been running for a very long time (potential leak)
                elif (
                    managed_task.state == TaskState.RUNNING
                    and managed_task.get_runtime() > 300
                ):  # 5 minutes
                    leaked_task_names.append(f"{group_name}.{task_name} (long-running)")

        return leaked_task_names

    async def force_cleanup_all(self) -> int:
        """Emergency cleanup - cancel all tasks immediately and clean up tracking."""
        if self._shutdown_in_progress:
            return 0

        task_log.warning(
            f"[{self.node_id}] FORCE CLEANUP: Cancelling all tasks immediately"
        )
        cancelled_count = 0

        for group in self.task_groups.values():
            for managed_task in group.tasks.values():
                if managed_task.task and not managed_task.task.done():
                    managed_task.task.cancel()
                    managed_task.state = TaskState.STOPPED
                    managed_task.stopped_at = time.time()
                    cancelled_count += 1

        # Clear all tracking
        for group in self.task_groups.values():
            group.tasks.clear()

        return cancelled_count
