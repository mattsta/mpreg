"""
Task lifecycle management utilities for proper async cleanup.

This module provides utilities to manage background tasks and ensure proper cleanup
when objects are destroyed, preventing the "Task was destroyed but it is pending"
warnings and resource leaks.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any

from loguru import logger

task_log = logger


class TaskManager:
    """Manages background tasks with proper lifecycle cleanup."""

    def __init__(self, name: str = "TaskManager") -> None:
        self.name = name
        self.tasks: set[asyncio.Task[Any]] = set()
        self._shutdown_requested = False

    def create_task(
        self, coro: Coroutine[Any, Any, Any], name: str | None = None
    ) -> asyncio.Task[Any]:
        """Create and track a background task."""
        if self._shutdown_requested:
            coro.close()
            raise RuntimeError("Cannot create tasks after shutdown requested")

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            coro.close()
            raise

        task = loop.create_task(coro, name=name)
        self.tasks.add(task)

        # Clean up when task completes naturally
        task.add_done_callback(self._task_completed)

        task_log.debug(f"[{self.name}] Created task {task.get_name() or 'unnamed'}")
        return task

    def _task_completed(self, task: asyncio.Task[Any]) -> None:
        """Callback when a task completes naturally."""
        self.tasks.discard(task)

        # Log any exceptions
        if task.cancelled():
            task_log.debug(
                f"[{self.name}] Task {task.get_name() or 'unnamed'} was cancelled"
            )
        elif task.exception():
            task_log.error(
                f"[{self.name}] Task {task.get_name() or 'unnamed'} failed: {task.exception()}"
            )
        else:
            task_log.debug(
                f"[{self.name}] Task {task.get_name() or 'unnamed'} completed successfully"
            )

    async def shutdown(self, timeout: float = 5.0) -> None:
        """Shutdown all managed tasks gracefully."""
        if self._shutdown_requested:
            return

        self._shutdown_requested = True

        if not self.tasks:
            task_log.debug(f"[{self.name}] No tasks to shutdown")
            return

        task_log.info(f"[{self.name}] Shutting down {len(self.tasks)} background tasks")

        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        # Give tasks a moment to respond to cancellation
        await asyncio.sleep(0.1)

        # Wait for cancellation with timeout
        if self.tasks:
            try:
                # Use wait instead of gather to handle already-done tasks better
                pending_tasks = [task for task in self.tasks if not task.done()]
                if pending_tasks:
                    done, pending = await asyncio.wait(
                        pending_tasks,
                        timeout=timeout,
                        return_when=asyncio.ALL_COMPLETED,
                    )

                    # Force cancel any remaining pending tasks
                    for task in pending:
                        task.cancel()
                        task_log.warning(
                            f"[{self.name}] Force-cancelled task: {task.get_name()}"
                        )

                    # Wait a bit more for force-cancelled tasks to finish
                    if pending:
                        await asyncio.sleep(0.1)

            except Exception as e:
                task_log.warning(f"[{self.name}] Error during task shutdown: {e}")

        # Clear tasks regardless of whether they completed successfully
        self.tasks.clear()
        task_log.info(f"[{self.name}] Task shutdown complete")

    def cancel_all(self) -> None:
        """Synchronously cancel all tracked tasks without awaiting."""
        for task in list(self.tasks):
            if not task.done():
                task.cancel()
        self.tasks.clear()

    def __len__(self) -> int:
        """Return number of active tasks."""
        return len(self.tasks)

    def __bool__(self) -> bool:
        """Return True if there are active tasks."""
        return bool(self.tasks)


class ManagedObject:
    """Base class for objects that manage background tasks."""

    def __init__(self, name: str | None = None) -> None:
        self._task_manager = TaskManager(name or self.__class__.__name__)

    def create_task(
        self, coro: Coroutine[Any, Any, Any], name: str | None = None
    ) -> asyncio.Task[Any]:
        """Create a managed background task."""
        return self._task_manager.create_task(coro, name)

    async def shutdown(self) -> None:
        """Shutdown the object and all its background tasks."""
        await self._task_manager.shutdown()

    def __del__(self) -> None:
        """Best-effort cleanup to avoid pending task warnings at loop teardown."""
        if not hasattr(self, "_task_manager"):
            return
        task_manager = self._task_manager
        if not task_manager.tasks or task_manager._shutdown_requested:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not None and loop.is_running():
            loop.create_task(task_manager.shutdown())
        else:
            task_manager.cancel_all()
