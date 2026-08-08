"""Hard gate: Raft stop never leaves asyncio tasks pending for GC destroy.

CPython emits ``Task was destroyed but it is pending!`` when the last strong
reference to a non-done Task is dropped. That is forbidden for Raft teardown
under any concurrency / xdist schedule.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import io
import re
import warnings
from contextlib import redirect_stderr

import pytest

from mpreg.datastructures.production_raft import RaftState
from mpreg.datastructures.production_raft_implementation import (
    ProductionRaft,
    RaftConfiguration,
)
from mpreg.datastructures.raft_storage_adapters import RaftStorageFactory
from mpreg.datastructures.raft_task_manager import RaftTaskManager, TaskState

_PENDING_DESTROY = re.compile(r"Task was destroyed but it is pending", re.IGNORECASE)


class _NullTransport:
    async def send_request_vote(self, *a, **k):
        return None

    async def send_append_entries(self, *a, **k):
        return None

    async def send_install_snapshot(self, *a, **k):
        return None


class _SM:
    async def apply_command(self, command, index):
        return command

    async def create_snapshot(self) -> bytes:
        return b"{}"

    async def restore_from_snapshot(self, snapshot_data: bytes) -> None:
        return None

    def get_current_state(self):
        return {}


async def _sleep_forever() -> None:
    """Cancellable background work used by lifecycle tests."""
    while True:
        await asyncio.sleep(0.05)


def _make_node(node_id: str, members: set[str]) -> ProductionRaft:
    return ProductionRaft(
        node_id=node_id,
        cluster_members=members,
        storage=RaftStorageFactory.create_memory_storage(node_id),
        transport=_NullTransport(),  # type: ignore[arg-type]
        state_machine=_SM(),  # type: ignore[arg-type]
        config=RaftConfiguration(
            election_timeout_min=0.08,
            election_timeout_max=0.12,
            heartbeat_interval=0.02,
            rpc_timeout=0.05,
        ),
    )


@pytest.mark.asyncio
async def test_stop_all_tasks_leaves_zero_unfinished() -> None:
    mgr = RaftTaskManager("n1")

    for i in range(5):
        await mgr.create_task("core", f"loop_{i}", _sleep_forever)
    await mgr.create_task("replication", "r1", _sleep_forever)
    await mgr.create_task("maintenance", "m1", _sleep_forever)

    assert mgr.count_unfinished_tasks() == 7
    await mgr.stop_all_tasks(timeout=1.0)
    assert mgr.count_unfinished_tasks() == 0


@pytest.mark.asyncio
async def test_duplicate_create_awaits_old_task_before_replace() -> None:
    mgr = RaftTaskManager("n1")
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def first() -> None:
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def second() -> None:
        await asyncio.sleep(30)

    await mgr.create_task("core", "slot", first)
    await started.wait()
    old = mgr.task_groups["core"].tasks["slot"].task
    assert old is not None and not old.done()

    await mgr.create_task("core", "slot", second)
    # Old task must be fully settled (not merely cancelled-and-orphaned).
    assert old.done()
    assert cancelled.is_set()
    new = mgr.task_groups["core"].tasks["slot"].task
    assert new is not None and new is not old and not new.done()

    await mgr.stop_all_tasks(timeout=1.0)
    assert mgr.count_unfinished_tasks() == 0


@pytest.mark.asyncio
async def test_force_cleanup_awaits_before_clearing_refs() -> None:
    mgr = RaftTaskManager("n1")

    tasks = []
    for i in range(4):
        mt = await mgr.create_task("core", f"t{i}", _sleep_forever)
        assert mt.task is not None
        tasks.append(mt.task)

    await mgr.force_cleanup_all()
    assert all(t.done() for t in tasks)
    assert mgr.count_unfinished_tasks() == 0


@pytest.mark.asyncio
async def test_no_pending_destroy_warning_on_manager_teardown() -> None:
    """Drop manager after stop; stderr must not contain pending-destroy."""
    buf = io.StringIO()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with redirect_stderr(buf):
            mgr = RaftTaskManager("warn-node")
            for i in range(8):
                await mgr.create_task("core", f"w{i}", _sleep_forever)
            await mgr.stop_all_tasks(timeout=1.0)
            del mgr
            gc.collect()
            await asyncio.sleep(0)  # let loop process destroys

    text = buf.getvalue()
    assert not _PENDING_DESTROY.search(text), text
    for w in caught:
        assert "pending" not in str(w.message).lower()


@pytest.mark.asyncio
async def test_production_raft_stop_no_pending_destroy() -> None:
    """Single-node leader stop must settle heartbeat/coordinator."""
    node = _make_node("solo", {"solo"})
    await node.start()
    for _ in range(100):
        if node.current_state == RaftState.LEADER:
            break
        await asyncio.sleep(0.02)
    assert node.current_state == RaftState.LEADER

    buf = io.StringIO()
    with redirect_stderr(buf):
        await node.stop()
        unfinished = node.task_manager.count_unfinished_tasks()
        del node
        gc.collect()
        await asyncio.sleep(0)

    assert unfinished == 0
    assert not _PENDING_DESTROY.search(buf.getvalue()), buf.getvalue()


@pytest.mark.asyncio
async def test_stop_specific_task_settles() -> None:
    mgr = RaftTaskManager("n1")

    mt = await mgr.create_task("core", "heartbeat", _sleep_forever)
    task = mt.task
    assert task is not None
    ok = await mgr.stop_specific_task("core", "heartbeat", timeout=1.0)
    assert ok
    assert task.done()
    assert mt.state in (TaskState.STOPPED, TaskState.FAILED, TaskState.CANCELLING)


@pytest.mark.asyncio
async def test_multi_node_start_stop_no_pending() -> None:
    """Three nodes with live coordinators; stop all; zero unfinished + no warn."""
    members = {"a", "b", "c"}
    nodes = [_make_node(name, members) for name in sorted(members)]
    for n in nodes:
        await n.start()

    await asyncio.sleep(0.35)

    buf = io.StringIO()
    with redirect_stderr(buf):
        for n in nodes:
            await n.stop()
        unfinished = sum(n.task_manager.count_unfinished_tasks() for n in nodes)
        del nodes
        gc.collect()
        await asyncio.sleep(0)

    assert unfinished == 0
    assert not _PENDING_DESTROY.search(buf.getvalue()), buf.getvalue()


class _SlowTransport(_NullTransport):
    """Transport that stalls AppendEntries so heartbeat sits in gather."""

    def __init__(self, delay: float = 0.3) -> None:
        self.delay = delay

    async def send_append_entries(self, *a, **k):
        await asyncio.sleep(self.delay)

    async def send_request_vote(self, *a, **k):
        await asyncio.sleep(0.02)


def _make_node_slow(node_id: str, members: set[str]) -> ProductionRaft:
    return ProductionRaft(
        node_id=node_id,
        cluster_members=members,
        storage=RaftStorageFactory.create_memory_storage(node_id),
        transport=_SlowTransport(0.25),  # type: ignore[arg-type]
        state_machine=_SM(),  # type: ignore[arg-type]
        config=RaftConfiguration(
            election_timeout_min=0.12,
            election_timeout_max=0.20,
            heartbeat_interval=0.02,
            rpc_timeout=0.5,
        ),
    )


@pytest.mark.asyncio
async def test_aborted_stop_via_wait_for_no_pending_destroy() -> None:
    """wait_for(stop, tiny) must not leave heartbeat pending-destroy.

    Historical flake: node_2_core_heartbeat stuck cancelling mid-gather when
    the stopper was itself cancelled; GC then emitted pending-destroy.
    """
    from mpreg.datastructures.raft_task_manager import (
        drain_retained_tasks,
        retained_task_count,
    )

    members = {"node_0", "node_1", "node_2"}
    nodes = [_make_node_slow(name, members) for name in sorted(members)]
    for n in nodes:
        await n.start()

    # Let a leader form and enter heartbeat gather against slow transport.
    for _ in range(150):
        if any(n.current_state == RaftState.LEADER for n in nodes):
            break
        await asyncio.sleep(0.02)
    await asyncio.sleep(0.08)  # heartbeat mid-replicate

    buf = io.StringIO()
    with redirect_stderr(buf):
        for n in nodes:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(n.stop(), timeout=0.001)
            # Retry full stop so tests don't leak; first abort is the race.
            with contextlib.suppress(Exception):
                await n.stop()
        # Even if manager dicts were cleared mid-cancel, retain bag holds refs.
        await drain_retained_tasks(timeout=2.0)
        del nodes
        gc.collect()
        await asyncio.sleep(0.05)
        gc.collect()

    text = buf.getvalue()
    assert not _PENDING_DESTROY.search(text), text
    assert retained_task_count() == 0


@pytest.mark.asyncio
async def test_force_cleanup_mid_heartbeat_gather_no_pending() -> None:
    """force_cleanup_all while heartbeat awaits slow gather must settle."""
    from mpreg.datastructures.raft_task_manager import retained_task_count

    members = {"solo_a", "solo_b"}
    nodes = [_make_node_slow(name, members) for name in sorted(members)]
    for n in nodes:
        await n.start()
    await asyncio.sleep(0.25)

    buf = io.StringIO()
    with redirect_stderr(buf):
        for n in nodes:
            await n.task_manager.force_cleanup_all()
        del nodes
        gc.collect()
        await asyncio.sleep(0.05)
        gc.collect()

    assert not _PENDING_DESTROY.search(buf.getvalue()), buf.getvalue()
    assert retained_task_count() == 0


@pytest.mark.asyncio
async def test_retain_bag_survives_manager_clear() -> None:
    """Dropping manager refs while tasks cancelling must not pending-destroy."""
    from mpreg.datastructures.raft_task_manager import (
        _RETAINED_TASKS,
        RaftTaskManager,
        drain_retained_tasks,
    )

    mgr = RaftTaskManager("retain-node")

    async def stuck() -> None:
        # Nested gather similar to heartbeat replication.
        await asyncio.gather(
            asyncio.sleep(30),
            asyncio.sleep(30),
        )

    tasks = []
    for i in range(6):
        mt = await mgr.create_task("core", f"hb_{i}", stuck)
        assert mt.task is not None
        tasks.append(mt.task)

    assert all(t in _RETAINED_TASKS for t in tasks)

    buf = io.StringIO()
    with redirect_stderr(buf):
        # Cancel without awaiting, then clear manager tracking (old bug path).
        for mt in list(mgr.task_groups["core"].tasks.values()):
            if mt.task and not mt.task.done():
                mt.task.cancel()
        mgr.task_groups["core"].tasks.clear()
        del mgr
        # Without retain bag this would pending-destroy on GC.
        gc.collect()
        await asyncio.sleep(0)  # let cancel progress
        # Drain retain bag explicitly (production stop does this via force).
        await drain_retained_tasks(timeout=2.0)
        gc.collect()
        await asyncio.sleep(0)

    assert all(t.done() for t in tasks)
    assert not _PENDING_DESTROY.search(buf.getvalue()), buf.getvalue()
