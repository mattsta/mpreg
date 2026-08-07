"""E1 / INV-E1: mpreg.consensus facade surface."""

from __future__ import annotations

from mpreg import consensus
from mpreg.core import consensus_api


def test_facade_exports() -> None:
    assert consensus.ProductionRaft is not None
    assert consensus.RaftState is not None
    assert consensus.MembershipChangeNotSupported is not None
    assert callable(consensus.status_dict)


def test_consensus_api_reexports() -> None:
    assert consensus_api.ProductionRaft is consensus.ProductionRaft
    assert consensus_api.MembershipChangeNotSupported is (
        consensus.MembershipChangeNotSupported
    )


def test_status_dict_shape() -> None:
    class Fake:
        node_id = "n1"
        current_state = consensus.RaftState.FOLLOWER
        cluster_members = {"n1", "n2"}

        class PS:
            current_term = 3
            voted_for = None
            log_entries: list = []

        class VS:
            commit_index = 0
            last_applied = 0

        persistent_state = PS()
        volatile_state = VS()

    d = consensus.status_dict(Fake())  # type: ignore[arg-type]
    assert d["node_id"] == "n1"
    assert d["term"] == 3
    assert d["membership_change_supported"] is False
    assert "commit_index" in d
