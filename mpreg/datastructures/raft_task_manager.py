"""
Centralized Task Management for Production Raft Implementation.

This module provides a clean abstraction for managing all async tasks
in the Raft consensus algorithm, replacing scattered task state with
a centralized, lifecycle-aware task manager.

Hard rule: never drop the last strong reference to an asyncio.Task while
it is still pending. Cancel **and await** (or wait_for) before discard.
Otherwise CPython emits ``Task was destroyed but it is pending!``.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from mpreg.core.errors import OPERATIONAL_EXCEPTIONS, log_caught_exception

task_log = logger

# Strong refs retained until each managed Task is fully done().
# asyncio only weakly tracks Tasks; dropping the last Python ref while a Task
# is still pending (including "cancelling" mid-gather) emits
# ``Task was destroyed but it is pending!``. This bag is the last line of
# defense when callers clear manager dicts, GC ProductionRaft, or abort stop()
# via wait_for timeout / CancelledError before drain completes.
_RETAINED_TASKS: set[asyncio.Task[Any]] = set()


def _consume_task_outcome(task: asyncio.Task[Any]) -> None:
    """Retrieve result/exception so GC never sees an unretrieved failure."""
    if not task.done():
        return
    with contextlib.suppress(asyncio.CancelledError, asyncio.InvalidStateError):
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            # Outcome consumed; do not re-raise (caller already finished).
            _ = type(exc)


def _retain_task(task: asyncio.Task[Any]) -> None:
    """Hold a strong ref until the task completes (idempotent)."""
    if task.done():
        _consume_task_outcome(task)
        return
    _RETAINED_TASKS.add(task)

    def _release(t: asyncio.Task[Any]) -> None:
        _RETAINED_TASKS.discard(t)
        _consume_task_outcome(t)

    task.add_done_callback(_release)


def retained_task_count() -> int:
    """Test/observability helper: number of tasks still retained."""
    # Drop any that finished without callback (should be rare).
    dead = [t for t in _RETAINED_TASKS if t.done()]
    for t in dead:
        _RETAINED_TASKS.discard(t)
    return len(_RETAINED_TASKS)


async def drain_retained_tasks(timeout: float = 2.0) -> int:
    """Cancel+await every retained unfinished task. Returns prior count."""
    pending = [t for t in list(_RETAINED_TASKS) if not t.done()]
    count = len(pending)
    if not pending:
        return 0
    current = asyncio.current_task()
    to_drain = [t for t in pending if t is not current]
    for t in to_drain:
        if not t.cancelled():
            t.cancel()
    if to_drain:
        try:
            await asyncio.wait_for(
                asyncio.gather(*to_drain, return_exceptions=True),
                timeout=timeout,
            )
        except TimeoutError, asyncio.CancelledError:
            for t in to_drain:
                if not t.done():
                    t.cancel()
            # Best-effort second drain; shield so outer cancel cannot abort.
            try:
                await asyncio.shield(asyncio.gather(*to_drain, return_exceptions=True))
            except asyncio.CancelledError:
                # Still try plain gather if shield itself was cancelled mid-flight.
                await asyncio.gather(*to_drain, return_exceptions=True)
    return count


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

    def is_unfinished(self) -> bool:
        """True while the underlying asyncio.Task has not completed."""
        return self.task is not None and not self.task.done()

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

    def get_unfinished_tasks(self) -> list[ManagedTask]:
        """Get all tasks whose asyncio.Task is not yet done."""
        return [task for task in self.tasks.values() if task.is_unfinished()]

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


async def _await_task_settled(
    task: asyncio.Task[Any], *, timeout: float | None = None
) -> None:
    """Await a task until done (cancel already requested or natural exit).

    Always retrieves the result/exception so the task is fully settled and
    never left pending when the last Python reference is dropped.
    Outer CancelledError (e.g. wait_for on stop) must not abort the drain.
    """
    if task.done():
        _consume_task_outcome(task)
        return

    async def _drain() -> None:
        await asyncio.gather(task, return_exceptions=True)

    try:
        if timeout is None:
            await _drain()
        else:
            await asyncio.wait_for(_drain(), timeout=timeout)
    except TimeoutError:
        # Still pending after timeout: cancel again and drain without timeout.
        if not task.done():
            task.cancel()
        try:
            await asyncio.shield(_drain())
        except asyncio.CancelledError:
            await _drain()
    except asyncio.CancelledError:
        # Caller task cancelled while draining — still drain child.
        if not task.done():
            task.cancel()
        try:
            await asyncio.shield(_drain())
        except asyncio.CancelledError:
            await _drain()
        raise


class RaftTaskManager:
    """
    Centralized task manager for Raft consensus implementation.

    Provides clean lifecycle management for all async tasks including:
    - Heartbeat/replication tasks
    - Election coordination
    - Log application
    - Background maintenance

    Replaces scattered task attributes with organized, observable state.

    Invariant: after any stop/cancel path returns, every managed
    ``asyncio.Task`` is ``done()``. No pending-destroy warnings.
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

        # Replace existing unfinished task: cancel **and await** before drop.
        existing = self.task_groups[group_name].tasks.get(task_name)
        if existing is not None and existing.is_unfinished():
            task_log.warning(
                f"[{self.node_id}] Duplicate task {group_name}.{task_name} "
                "detected - stopping existing before replace"
            )
            await self._graceful_cancel_task(existing, timeout=0.5)

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

        # Retain until done so clear()/GC/aborted stop cannot pending-destroy.
        _retain_task(managed_task.task)

        def _on_task_done(task: asyncio.Task[Any]) -> None:
            if task.cancelled():
                if managed_task.state in (
                    TaskState.RUNNING,
                    TaskState.CANCELLING,
                    TaskState.STOPPING,
                ):
                    managed_task.state = TaskState.STOPPED
                managed_task.stopped_at = time.time()
                return
            try:
                exc = task.exception()
            except asyncio.CancelledError:
                managed_task.state = TaskState.STOPPED
                managed_task.stopped_at = time.time()
                return
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
        unfinished = group.get_unfinished_tasks()

        if not unfinished:
            return

        timeout = timeout or self.shutdown_timeout
        task_log.debug(
            f"[{self.node_id}] Stopping {len(unfinished)} tasks in group {group_name}"
        )

        # Parallel cancel+await: cancel all first, then drain together so a
        # slow task does not serialize the whole group shutdown.
        current = asyncio.current_task()
        to_drain: list[asyncio.Task[Any]] = []
        for managed_task in unfinished:
            task = managed_task.task
            if task is None or task.done():
                managed_task.state = TaskState.STOPPED
                managed_task.stopped_at = time.time()
                continue
            if task is current:
                # Never cancel the running task from inside itself.
                managed_task.state = TaskState.STOPPING
                continue
            managed_task.state = TaskState.CANCELLING
            task.cancel()
            to_drain.append(task)

        if to_drain:

            async def _drain_group() -> None:
                await asyncio.gather(*to_drain, return_exceptions=True)

            try:
                await asyncio.wait_for(_drain_group(), timeout=timeout)
            except TimeoutError:
                # Force another cancel + unbounded drain (must settle).
                for task in to_drain:
                    if not task.done():
                        task.cancel()
                try:
                    await asyncio.shield(_drain_group())
                except asyncio.CancelledError:
                    await _drain_group()
            except asyncio.CancelledError:
                for task in to_drain:
                    if not task.done() and not task.cancelled():
                        task.cancel()
                try:
                    await asyncio.shield(_drain_group())
                except asyncio.CancelledError:
                    await _drain_group()
                raise

        for managed_task in unfinished:
            if managed_task.task is current and managed_task.is_unfinished():
                # Self-stop: leave STOPPING; caller loop must exit.
                continue
            if managed_task.state in (TaskState.CANCELLING, TaskState.RUNNING):
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
        if not managed_task.is_unfinished():
            managed_task.state = TaskState.STOPPED
            managed_task.stopped_at = time.time()
            return True

        # Do not cancel ourselves.
        if managed_task.task is asyncio.current_task():
            managed_task.state = TaskState.STOPPING
            task_log.debug(
                f"[{self.node_id}] Skipping self-cancel of {group_name}.{task_name}"
            )
            return True

        timeout = timeout or self.shutdown_timeout
        task_log.debug(f"[{self.node_id}] Stopping task {group_name}.{task_name}")

        try:
            await self._graceful_cancel_task(managed_task, timeout=timeout)
            return True
        except OPERATIONAL_EXCEPTIONS as e:
            log_caught_exception(
                task_log,
                f"[{self.node_id}] Error stopping task {group_name}.{task_name}",
                e,
                level="warning",
            )
            await self._force_settle_task(managed_task)
            return True

    async def stop_all_tasks(self, timeout: float | None = None) -> None:
        """Stop all managed tasks across all groups until every task is done."""
        if self._shutdown_in_progress:
            # Nested stop: still drain any unfinished work.
            await self._drain_all_unfinished(timeout=timeout or 0.5)
            return

        self._shutdown_in_progress = True
        timeout = timeout or self.shutdown_timeout

        try:
            task_log.info(f"[{self.node_id}] Stopping all task groups")

            # Stop groups in order: maintenance -> replication -> core
            stop_order = ["maintenance", "replication", "core"]
            per_group = max(0.1, timeout / max(len(stop_order), 1))

            for group_name in stop_order:
                if group_name in self.task_groups:
                    await self.stop_task_group(group_name, timeout=per_group)

            # Stop any other groups
            for group_name in self.task_groups:
                if group_name not in stop_order:
                    await self.stop_task_group(group_name, timeout=0.5)

            # Final invariant pass: nothing left pending.
            await self._drain_all_unfinished(timeout=max(0.2, timeout * 0.25))

            unfinished = self.count_unfinished_tasks()
            if unfinished:
                task_log.error(
                    f"[{self.node_id}] {unfinished} Raft tasks still unfinished "
                    "after stop_all_tasks — forcing settle"
                )
                await self._drain_all_unfinished(timeout=1.0)

            task_log.info(f"[{self.node_id}] All tasks stopped")

        finally:
            self._shutdown_in_progress = False

    def count_unfinished_tasks(self) -> int:
        """Count managed asyncio tasks that are not yet done."""
        return sum(
            1
            for group in self.task_groups.values()
            for managed in group.tasks.values()
            if managed.is_unfinished()
        )

    async def _drain_all_unfinished(self, *, timeout: float) -> None:
        """Cancel and await every unfinished managed task (except self).

        Uses ``asyncio.shield`` on the final drain so an outer
        ``wait_for(node.stop())`` CancelledError cannot abort settlement and
        leave heartbeat tasks in the ``cancelling`` state with no strong refs.
        """
        current = asyncio.current_task()
        pending: list[asyncio.Task[Any]] = []
        managed_list: list[ManagedTask] = []
        for group in self.task_groups.values():
            for managed in group.tasks.values():
                task = managed.task
                if task is None or task.done():
                    continue
                if task is current:
                    managed.state = TaskState.STOPPING
                    continue
                if not task.cancelled():
                    managed.state = TaskState.CANCELLING
                    task.cancel()
                pending.append(task)
                managed_list.append(managed)
        if not pending:
            return

        async def _gather_pending() -> None:
            await asyncio.gather(*pending, return_exceptions=True)

        try:
            await asyncio.wait_for(_gather_pending(), timeout=timeout)
        except TimeoutError:
            for task in pending:
                if not task.done():
                    task.cancel()
            try:
                await asyncio.shield(_gather_pending())
            except asyncio.CancelledError:
                await _gather_pending()
        except asyncio.CancelledError:
            # Stopper itself cancelled (e.g. wait_for on node.stop): still drain.
            for task in pending:
                if not task.done() and not task.cancelled():
                    task.cancel()
            try:
                await asyncio.shield(_gather_pending())
            except asyncio.CancelledError:
                await _gather_pending()
            raise
        for managed in managed_list:
            if managed.state in (
                TaskState.CANCELLING,
                TaskState.RUNNING,
                TaskState.STOPPING,
            ):
                if managed.task is not None and managed.task.done():
                    managed.state = TaskState.STOPPED
            managed.stopped_at = time.time()

    async def _graceful_cancel_task(
        self, managed_task: ManagedTask, timeout: float = 1.0
    ) -> None:
        """Cancel task and await settlement (no pending-destroy)."""
        task = managed_task.task
        if task is None or task.done():
            managed_task.state = TaskState.STOPPED
            managed_task.stopped_at = time.time()
            if task is not None:
                await _await_task_settled(task, timeout=0.0)
            return

        if task is asyncio.current_task():
            managed_task.state = TaskState.STOPPING
            return

        task_log.debug(
            f"[{self.node_id}] Gracefully cancelling task {managed_task.name}"
        )
        managed_task.state = TaskState.CANCELLING
        task.cancel()

        try:
            await _await_task_settled(task, timeout=timeout)
        except OPERATIONAL_EXCEPTIONS as e:
            log_caught_exception(
                task_log,
                f"[{self.node_id}] Task {managed_task.name} failed during cancellation",
                e,
                level="warning",
            )
            managed_task.failure_reason = e
            managed_task.state = TaskState.FAILED
            # Still must settle.
            await _await_task_settled(task, timeout=0.5)
            managed_task.stopped_at = time.time()
            return

        if managed_task.state in (TaskState.CANCELLING, TaskState.RUNNING):
            managed_task.state = TaskState.STOPPED
        managed_task.stopped_at = time.time()

    async def _force_settle_task(self, managed_task: ManagedTask) -> None:
        """Last-resort cancel+await for a single managed task."""
        task = managed_task.task
        if task is None:
            managed_task.state = TaskState.STOPPED
            managed_task.stopped_at = time.time()
            return
        if task is asyncio.current_task():
            managed_task.state = TaskState.STOPPING
            return
        if not task.done():
            task.cancel()
        await _await_task_settled(task, timeout=1.0)
        managed_task.state = TaskState.STOPPED
        managed_task.stopped_at = time.time()

    def _cancel_task_immediate(self, managed_task: ManagedTask) -> None:
        """Request cancel without awaiting.

        Prefer :meth:`_graceful_cancel_task`. Callers that use this **must**
        still await the task before dropping the last reference (e.g. via
        :meth:`_drain_all_unfinished`). Marked CANCELLING only.
        """
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
                # Unfinished tasks stuck in cancelling/stopping
                if (
                    managed_task.is_unfinished()
                    and managed_task.state
                    in (
                        TaskState.CANCELLING,
                        TaskState.STOPPING,
                    )
                    or (
                        managed_task.state in (TaskState.CANCELLING, TaskState.STOPPING)
                        and managed_task.task
                        and managed_task.task.done()
                    )
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
        """Emergency cleanup - cancel and **await** all tasks, then clear tracking.

        Clearing the manager dicts is safe for pending-destroy because every
        managed task is also held in ``_RETAINED_TASKS`` until ``done()``.
        """
        task_log.warning(
            f"[{self.node_id}] FORCE CLEANUP: Cancelling and settling all tasks"
        )
        unfinished_before = self.count_unfinished_tasks()
        await self._drain_all_unfinished(timeout=2.0)

        # Snapshot unfinished tasks before clear (retained bag still holds them).
        still_pending: list[asyncio.Task[Any]] = []
        for group in self.task_groups.values():
            still = [
                name
                for name, managed in group.tasks.items()
                if managed.is_unfinished()
                and managed.task is not asyncio.current_task()
            ]
            if still:
                task_log.error(
                    f"[{self.node_id}] force_cleanup still has unfinished: {still}"
                )
            for managed in group.tasks.values():
                if (
                    managed.task is not None
                    and not managed.task.done()
                    and managed.task is not asyncio.current_task()
                ):
                    still_pending.append(managed.task)
            group.tasks.clear()

        # Final settle of anything still cancelling (e.g. nested gather).
        if still_pending:
            for t in still_pending:
                if not t.done() and not t.cancelled():
                    t.cancel()
            await asyncio.gather(*still_pending, return_exceptions=True)

        return unfinished_before
