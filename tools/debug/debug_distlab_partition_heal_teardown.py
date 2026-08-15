"""Deep-dive: leftover task graphs after raft.partition_heal teardown.

Runs the DistLab raft partition/heal scenario repeatedly and, after each
run's teardown, inspects every still-pending asyncio task: name, state,
and the depth/shape of its ``fut_waiter`` chain (Task -> _GatheringFuture
-> child Task -> ...). A chain or cycle here is what explodes with
RecursionError inside ``asyncio.Runner`` teardown's ``_cancel_all_tasks``.

Usage:
    uv run python tools/debug/debug_distlab_partition_heal_teardown.py [rounds]
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any

from mpreg.testing.distlab.builtins import ensure_builtins
from mpreg.testing.distlab.registry import get_registry


def waiter_chain(task: asyncio.Task[Any], limit: int = 50) -> list[str]:
    """Describe the fut_waiter chain hanging off a task."""
    chain: list[str] = []
    seen: set[int] = set()
    obj: Any = task
    while obj is not None and len(chain) < limit:
        if id(obj) in seen:
            chain.append("<CYCLE>")
            break
        seen.add(id(obj))
        if isinstance(obj, asyncio.Task):
            chain.append(f"Task({obj.get_name()})")
            obj = getattr(obj, "_fut_waiter", None)
        elif type(obj).__name__ == "_GatheringFuture":
            children = getattr(obj, "_children", [])
            chain.append(f"Gather[{len(children)}]")
            pending = [c for c in children if not c.done()]
            obj = pending[0] if pending else None
        else:
            chain.append(type(obj).__name__)
            break
    return chain


async def run_once(round_no: int) -> int:
    ensure_builtins()
    r = await get_registry().run("raft.partition_heal")
    # Give done-callbacks a tick to fire before inspecting.
    await asyncio.sleep(0)
    current = asyncio.current_task()
    leftovers = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
    if leftovers:
        print(f"round {round_no}: ok={r.ok} LEFTOVER TASKS: {len(leftovers)}")
        for t in leftovers:
            chain = waiter_chain(t)
            print(f"  depth={len(chain)} :: {' -> '.join(chain)}")
    else:
        print(f"round {round_no}: ok={r.ok} clean")
    return len(leftovers)


def main() -> int:
    rounds = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    worst = 0
    for i in range(rounds):
        worst = max(worst, asyncio.run(run_once(i)))
    print(f"worst leftover count: {worst}")
    return 1 if worst else 0


if __name__ == "__main__":
    raise SystemExit(main())
