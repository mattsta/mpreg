"""B3: CONFIGURATION_CHANGE hard-fail (INV-C6)."""

from __future__ import annotations

import pytest

from mpreg.consensus import MembershipChangeNotSupported, ProductionRaft, RaftConfiguration
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
async def test_submit_configuration_change_raises() -> None:
    storage = RaftStorageFactory.create_memory_storage("m1")
    node = ProductionRaft(
        node_id="n1",
        cluster_members={"n1"},
        storage=storage,
        transport=_NullTransport(),
        state_machine=TestableStateMachine(),
        config=RaftConfiguration(
            election_timeout_min=0.15,
            election_timeout_max=0.30,
            heartbeat_interval=0.025,
        ),
    )
    with pytest.raises(MembershipChangeNotSupported):
        await node.submit_configuration_change({"n1", "n2"})
