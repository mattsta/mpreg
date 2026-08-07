"""T15 residual closeout: ready gauge wiring, peer refuse close, mon posture."""

from __future__ import annotations

from types import SimpleNamespace

from click.testing import CliRunner

from mpreg.cli.main import cli
from mpreg.core.config import MPREGSettings
from mpreg.core.monitoring.server_monitoring import ServerMetricsTracker
from mpreg.datastructures.federated_types import FederatedRPCAnnouncement
from mpreg.fabric.monitoring_endpoints import FederationMonitoringSystem
from mpreg.server_pkg.drain_admission import should_refuse_for_drain


def test_obs_t15_01_ready_uses_server_metrics_tracker() -> None:
    """OBS-T15-01: /ready path must resolve server_metrics_tracker field."""
    tracker = ServerMetricsTracker()
    tracker.set_ready(True)
    assert tracker.node_ready == 1

    class _Stub:
        server_metrics_tracker = tracker
        metrics_tracker = None
        _metrics_tracker = None
        metrics_tracker_provider = None

    _Stub._resolve_server_metrics_tracker = (  # type: ignore[attr-defined]
        FederationMonitoringSystem._resolve_server_metrics_tracker
    )
    stub = _Stub()
    resolved = FederationMonitoringSystem._resolve_server_metrics_tracker(stub)  # type: ignore[arg-type]
    assert resolved is tracker
    if hasattr(resolved, "set_ready"):
        resolved.set_ready(False)
    assert tracker.node_ready == 0


def test_obs_t15_01_prom_drain_clears_ready_without_ready_hit() -> None:
    tracker = ServerMetricsTracker()
    tracker.set_ready(True)

    class _Stub:
        server_metrics_tracker = tracker
        draining_provider = staticmethod(lambda: True)

        def _resolve_server_metrics_tracker(self):  # type: ignore[no-untyped-def]
            return self.server_metrics_tracker

        def _refresh_raft_bridge_metrics(self) -> None:
            return None

    # Simulate the drain-refresh fragment from _build_prometheus_text
    stub = _Stub()
    tr = stub._resolve_server_metrics_tracker()
    draining = bool(stub.draining_provider())
    tr.set_draining(draining)
    assert tracker.node_draining == 1
    assert tracker.node_ready == 0


def test_cor_t15_01_track_inbound_peer_returns_bool() -> None:
    """COR-T15-01: refuse path returns False (callers close connection)."""
    from mpreg.core.config import MPREGSettings

    class _S:
        def __init__(self) -> None:
            self.settings = MPREGSettings(max_peer_connections=1)
            self.peer_connections = {"existing": object()}
            self._inbound_peer_connections: dict = {}
            self._metrics_tracker = ServerMetricsTracker()
            self.cluster = SimpleNamespace(
                local_url="ws://local",
                connection_event_bus=SimpleNamespace(publish=lambda e: None),
            )
            self._fabric_control_plane = None
            self._fabric_gossip_transport = None

        def _peer_connection_count(self) -> int:
            return len(self.peer_connections) + len(self._inbound_peer_connections)

        def _peer_connections_at_cap(self, *, reserving: bool = True) -> bool:
            max_p = int(self.settings.max_peer_connections or 0)
            if max_p <= 0:
                return False
            n = self._peer_connection_count()
            return n >= max_p if reserving else n > max_p

        def _note_peer_accept_reject(self, n: int = 1) -> None:
            self._metrics_tracker.record_peer_accept_reject(n)

        def _schedule_catalog_snapshot(self, peer_url: str) -> None:
            return None

    # Bind real method
    from mpreg.server import MPREGServer

    s = _S()
    # Call unbound method with our stand-in
    ok = MPREGServer._track_inbound_peer_connection(s, "ws://new-peer", object())  # type: ignore[arg-type]
    assert ok is False
    assert s._metrics_tracker.peer_accept_rejects == 1
    assert "ws://new-peer" not in s._inbound_peer_connections

    # Same url replace when under cap after clearing
    s.peer_connections.clear()
    s.settings = MPREGSettings(max_peer_connections=2)
    conn = object()
    ok2 = MPREGServer._track_inbound_peer_connection(s, "ws://p1", conn)  # type: ignore[arg-type]
    assert ok2 is True
    assert "ws://p1" in s._inbound_peer_connections


def test_cor_t15_02_should_process_doc_marks_api_only() -> None:
    doc = FederatedRPCAnnouncement.should_process.__doc__ or ""
    assert "API-only" in doc or "catalog gossip" in doc


def test_erg_t15_01_dev_and_discovery_mon_loopback() -> None:
    for path in (
        "mpreg/profiles/dev.toml",
        "mpreg/profiles/discovery-resolver.toml",
    ):
        s = MPREGSettings.from_path(path)
        assert s.monitoring_host in ("127.0.0.1", "localhost"), path


def test_erg_t15_01_config_check_warns_public_mon() -> None:
    import tempfile
    from pathlib import Path

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "open-mon.toml"
        p.write_text(
            """
[mpreg]
name = "x"
cluster_id = "c"
host = "0.0.0.0"
monitoring_enabled = true
monitoring_host = "0.0.0.0"
enable_default_cache = true
enable_default_queue = true
"""
        )
        result = runner.invoke(cli, ["config-check", str(p), "--format", "json"])
        assert result.exit_code == 0  # lab_ok
        assert (
            "non-loopback" in result.output or "monitoring_auth_token" in result.output
        )


def test_erg_t15_01_profile_list_includes_discovery_resolver() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["profile", "list"])
    assert result.exit_code == 0
    assert "discovery-resolver" in result.output


def test_t15_drain_still_deny_unknown() -> None:
    assert should_refuse_for_drain(draining=True, role="brand-new") is True
