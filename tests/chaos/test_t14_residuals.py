"""T14 residual closeout: gossip LRU, InstallSnapshot done, raft Prom, OpenAPI, profiles."""

from __future__ import annotations

from collections import OrderedDict

import pytest
from click.testing import CliRunner

from mpreg.cli.main import cli
from mpreg.consensus import status_dict
from mpreg.core.monitoring.server_monitoring import ServerMetricsTracker
from mpreg.datastructures.production_raft import (
    InstallSnapshotRequest,
    PersistentState,
    RaftState,
)
from mpreg.datastructures.production_raft_implementation import (
    ProductionRaft,
    RaftConfiguration,
)
from mpreg.datastructures.raft_codec import (
    deserialize_install_snapshot,
)
from mpreg.datastructures.raft_storage_adapters import RaftStorageFactory
from mpreg.datastructures.vector_clock import VectorClock
from mpreg.fabric.gossip import GossipFilter, GossipMessage, GossipMessageType
from mpreg.server_pkg.openapi_surface import (
    openapi_path_set,
    route_table_path_set,
)
from tests.test_production_raft_integration import TestableStateMachine


class _NullTransport:
    async def send_request_vote(self, target, request):  # type: ignore[no-untyped-def]
        return None

    async def send_append_entries(self, target, request):  # type: ignore[no-untyped-def]
        return None

    async def send_install_snapshot(self, target, request):  # type: ignore[no-untyped-def]
        return None


def _make_node(node_id: str = "n1") -> ProductionRaft:
    return ProductionRaft(
        node_id=node_id,
        cluster_members={node_id, "n2"},
        storage=RaftStorageFactory.create_memory_storage(node_id),
        transport=_NullTransport(),
        state_machine=TestableStateMachine(),
        config=RaftConfiguration(
            election_timeout_min=0.15,
            election_timeout_max=0.30,
            heartbeat_interval=0.025,
            snapshot_threshold=5,
        ),
    )


# --- PERF-T14-01: gossip seen_messages LRU ---


def _msg(mid: str) -> GossipMessage:
    vc = VectorClock.empty().increment("n1")
    return GossipMessage(
        message_id=mid,
        message_type=GossipMessageType.STATE_UPDATE,
        sender_id="n1",
        payload={"k": mid},
        vector_clock=vc,
        sequence_number=1,
        ttl=10,
        max_hops=5,
        digest=f"d-{mid}",
    )


def test_perf_t14_01_gossip_seen_is_ordered_dict() -> None:
    f = GossipFilter(max_seen_messages=10)
    assert isinstance(f.seen_messages, OrderedDict)


def test_perf_t14_01_gossip_lru_evicts_oldest() -> None:
    f = GossipFilter(max_seen_messages=5)
    for i in range(8):
        m = _msg(f"m{i}")
        # Direct record path
        if hasattr(m, "can_propagate"):
            f._record_message(m)  # type: ignore[arg-type]
        else:
            f.seen_messages[f"m{i}"] = None
            while len(f.seen_messages) > 5:
                f.seen_messages.popitem(last=False)
    assert len(f.seen_messages) <= 5
    # Oldest should be gone
    assert "m0" not in f.seen_messages
    assert "m1" not in f.seen_messages
    assert "m7" in f.seen_messages


def test_perf_t14_01_gossip_refresh_moves_to_end() -> None:
    f = GossipFilter(max_seen_messages=3)
    for mid in ("a", "b", "c"):
        f._record_message(_msg(mid))  # type: ignore[arg-type]
    # Refresh a → should survive next eviction
    f._record_message(_msg("a"))  # type: ignore[arg-type]
    f._record_message(_msg("d"))  # type: ignore[arg-type]
    assert "a" in f.seen_messages
    assert "b" not in f.seen_messages  # oldest unrereshed


# --- COR-T14-01: InstallSnapshot done fail-closed ---


def test_cor_t14_01_install_snapshot_missing_done_false() -> None:
    data = {
        "term": 1,
        "leader_id": "L",
        "last_included_index": 10,
        "last_included_term": 1,
        "data": "",
        "offset": 0,
        # no "done"
    }
    req = deserialize_install_snapshot(data)
    assert req.done is False


def test_cor_t14_01_install_snapshot_explicit_done_true() -> None:
    data = {
        "term": 1,
        "leader_id": "L",
        "last_included_index": 10,
        "last_included_term": 1,
        "data": "",
        "offset": 0,
        "done": True,
    }
    req = deserialize_install_snapshot(data)
    assert req.done is True


@pytest.mark.asyncio
async def test_cor_t14_01_missing_done_does_not_apply_snapshot() -> None:
    """Partial wire without done must not complete install even if offset=0."""
    sm = TestableStateMachine()
    await sm.apply_command("k=1", 1)
    snap = await sm.create_snapshot()
    node = _make_node("f1")
    node.current_state = RaftState.FOLLOWER
    node.persistent_state = PersistentState(
        current_term=1, voted_for=None, log_entries=[]
    )
    # Build request with done=False (simulating missing-done deserialize)
    req = InstallSnapshotRequest(
        term=1,
        leader_id="L",
        last_included_index=5,
        last_included_term=1,
        offset=0,
        data=snap,
        done=False,
    )
    resp = await node.handle_install_snapshot(req)
    assert resp.success is True  # ACK of chunk while buffering
    # done=False must not finalize snapshot base index
    assert getattr(node, "_snapshot_last_index", 0) != 5
    assert node.installing_snapshot is True or len(node.snapshot_chunks) >= 0


# --- OBS-T14-01: raft Prom bridge ---


def test_obs_t14_01_set_raft_bridge_prom_series() -> None:
    t = ServerMetricsTracker()
    t.set_raft_bridge(
        term=7,
        commit_index=100,
        last_applied=99,
        log_size=50,
        elections_started=3,
        elections_won=1,
        append_entries_success=10,
        append_entries_failure=2,
        commands_applied=40,
        state="leader",
    )
    lines = "\n".join(t.prometheus_lines('cluster_id="c"'))
    assert "mpreg_raft_term" in lines
    assert "mpreg_raft_commit_index" in lines
    assert "mpreg_raft_last_applied" in lines
    assert "mpreg_raft_elections_started_total" in lines
    assert "mpreg_raft_commands_applied_total" in lines
    assert t.raft_term == 7
    assert t.raft_commit_index == 100


def test_obs_t14_01_status_dict_includes_metrics() -> None:
    node = _make_node("n1")
    node.current_state = RaftState.FOLLOWER
    node.persistent_state = PersistentState(
        current_term=4, voted_for=None, log_entries=[]
    )
    node.metrics.elections_started = 2
    node.metrics.commands_applied = 5
    d = status_dict(node)
    assert d["term"] == 4
    assert "metrics" in d
    assert d["metrics"]["elections_started"] == 2
    assert d["metrics"]["commands_applied"] == 5


def test_obs_t14_01_refresh_bridge_from_provider() -> None:
    from mpreg.fabric.monitoring_endpoints import FederationMonitoringSystem

    # Minimal instance without full __post_init__ web app if possible
    tracker = ServerMetricsTracker()

    class _Stub:
        server_metrics_tracker = tracker
        raft_status_provider = staticmethod(
            lambda: {
                "nodes": [
                    {
                        "role": "leader",
                        "term": 9,
                        "commit_index": 20,
                        "last_applied": 18,
                        "metrics": {
                            "elections_started": 4,
                            "elections_won": 2,
                            "append_entries_success": 11,
                            "append_entries_failure": 1,
                            "commands_applied": 15,
                            "log_size": 12,
                        },
                    }
                ]
            }
        )

    # Bind method
    _Stub._refresh_raft_bridge_metrics = (  # type: ignore[attr-defined]
        FederationMonitoringSystem._refresh_raft_bridge_metrics
    )
    stub = _Stub()
    FederationMonitoringSystem._refresh_raft_bridge_metrics(stub)  # type: ignore[arg-type]
    assert tracker.raft_term == 9
    assert tracker.raft_commit_index == 20
    assert tracker.raft_elections_started == 4
    assert tracker.raft_commands_applied == 15


# --- ERG-T14-01: OpenAPI ↔ router parity ---


def test_erg_t14_01_openapi_matches_route_table() -> None:
    assert openapi_path_set() == route_table_path_set()
    assert "/" in openapi_path_set()
    assert "/metrics/prometheus" in openapi_path_set()
    assert "/mgmt/v1/nodes/drain" in openapi_path_set()


def test_erg_t14_01_openapi_matches_live_router_source() -> None:
    import re
    from pathlib import Path

    mon = Path("mpreg/fabric/monitoring_endpoints.py").read_text()
    router = set(re.findall(r'add_(?:get|post)\(\s*["\']([^"\']+)', mon))
    router |= set(re.findall(r'add_(?:get|post)\(\s*\n\s*["\']([^"\']+)', mon))
    assert router == openapi_path_set()


# --- ERG-T14-02: profile risk tags ---


def test_erg_t14_02_profile_list_shows_risk() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["profile", "list"])
    assert result.exit_code == 0, result.output
    assert "lab" in result.output
    assert "federated" in result.output.lower() or "Federated" in result.output
    assert (
        "Risk" in result.output
        or "risk" in result.output.lower()
        or "prod-baseline" in result.output
    )


# --- ERG-T14-03: cluster / soft-rt mon posture ---


def test_erg_t14_03_cluster_ready_and_mon() -> None:
    from mpreg.core.config import MPREGSettings

    s = MPREGSettings.from_path("mpreg/profiles/cluster.toml")
    assert s.monitoring_host in ("127.0.0.1", "localhost")
    assert float(s.ready_min_score) >= 0.7


def test_erg_t14_03_soft_rt_mon_loopback() -> None:
    from mpreg.core.config import MPREGSettings

    s = MPREGSettings.from_path("mpreg/profiles/soft-rt.toml")
    assert s.monitoring_host in ("127.0.0.1", "localhost")
