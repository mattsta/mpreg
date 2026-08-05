"""Async helpers for the MPREG CLI (nested-loop safe).

F1: click handlers historically called ``asyncio.run`` which fails when the
caller already has a running event loop (e.g. curriculum apps using CliRunner
inside ``async def main``). ``run_coro`` offloads to a worker thread in that
case so nested invocation works without every command inventing its own
``asyncio.to_thread`` wrapper.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Coroutine
from typing import TypeVar

T = TypeVar("T")

def run_coro(coro: Coroutine[object, object, T]) -> T:
    """Run *coro* to completion, even if an event loop is already running.

    - No running loop → ``asyncio.run(coro)`` (normal CLI path).
    - Running loop → execute ``asyncio.run(coro)`` in a worker thread so the
      nested loop is isolated from the caller's loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    def _in_thread() -> T:
        return asyncio.run(coro)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(_in_thread).result()
