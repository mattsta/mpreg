"""B2: InstallSnapshot apply path (INV-C5)."""

from __future__ import annotations

import pytest

from mpreg.datastructures.production_raft import (
    InstallSnapshotRequest,
    RaftState,
)
from mpreg.datastructures.production_raft_implementation import (
    ProductionRaft,
    RaftConfiguration,
)
from mpreg.datastructures.raft_storage_adapters import RaftStorageFactory
from tests.test_production_raft_integration import TestableStateMachine


class _NullTransport:
    async def send_request_vote(self, target, request):  # type: ignore[no-untyped-def]
        return None

    async def send_append_entries(self, target, request):  # type: ignore[no-untyped-def]
        return None

    async def send_install_snapshot(self, target, request):  # type: ignore[no-untyped-def]
        return None


@pytest.mark.asyncio
async def test_install_snapshot_restores_state_machine() -> None:
    sm = TestableStateMachine()
    await sm.apply_command("k=1", 1)
    snap_bytes = await sm.create_snapshot()

    follower_sm = TestableStateMachine()
    storage = RaftStorageFactory.create_memory_storage("f1")
    node = ProductionRaft(
        node_id="f1",
        cluster_members={"f1", "l1"},
        storage=storage,
        transport=_NullTransport(),
        state_machine=follower_sm,
        config=RaftConfiguration(
            election_timeout_min=0.15,
            election_timeout_max=0.30,
            heartbeat_interval=0.025,
        ),
    )
    # Ensure RPC mixin path is available
    req = InstallSnapshotRequest(
        term=1,
        leader_id="l1",
        last_included_index=5,
        last_included_term=1,
        offset=0,
        data=snap_bytes,
        done=True,
    )
    # Follower should accept higher/equal term install
    node.persistent_state = node.persistent_state.__class__(
        current_term=1,
        voted_for=None,
        log_entries=[],
    )
    node.testing_set_state(RaftState.FOLLOWER)

    resp = await node.handle_install_snapshot(req)
    assert resp is not None
    assert resp.term >= 1
    assert resp.success is True
    # State machine restored
    assert follower_sm.state.get("k") == 1 or "k" in follower_sm.state
    assert node.volatile_state.last_applied >= 5


@pytest.mark.asyncio
async def test_install_snapshot_failure_returns_success_false() -> None:
    """COR-T10-01: apply/persist failure must not ACK success."""

    class _BoomSM(TestableStateMachine):
        async def restore_from_snapshot(self, data: bytes) -> None:  # type: ignore[override]
            raise RuntimeError("restore exploded")

    storage = RaftStorageFactory.create_memory_storage("f-fail")
    node = ProductionRaft(
        node_id="f-fail",
        cluster_members={"f-fail", "l1"},
        storage=storage,
        transport=_NullTransport(),
        state_machine=_BoomSM(),
        config=RaftConfiguration(
            election_timeout_min=0.15,
            election_timeout_max=0.30,
            heartbeat_interval=0.025,
        ),
    )
    node.persistent_state = node.persistent_state.__class__(
        current_term=1,
        voted_for=None,
        log_entries=[],
    )
    node.testing_set_state(RaftState.FOLLOWER)
    req = InstallSnapshotRequest(
        term=1,
        leader_id="l1",
        last_included_index=5,
        last_included_term=1,
        offset=0,
        data=b"not-a-valid-sm-blob",
        done=True,
    )
    resp = await node.handle_install_snapshot(req)
    assert resp.success is False
    assert resp.term >= 1
    # Leader must not treat this as catch-up complete.
    assert node.volatile_state.last_applied < 5 or node.volatile_state.last_applied == 0
