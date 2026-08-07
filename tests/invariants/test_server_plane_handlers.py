"""E5: Peeled plane handlers are self-managing and importable."""

from __future__ import annotations

from mpreg.server_pkg.raft_handlers import RaftPlane
from mpreg.server_pkg.routing_handlers import RoutingPlane
from mpreg.server_pkg.rpc_handlers import RpcPlane


def test_raft_plane_status_unconfigured() -> None:
    plane = RaftPlane()
    st = plane.status()
    assert st["configured"] is False
    assert st["membership_change_supported"] is False


def test_raft_plane_register_requires_transport() -> None:
    plane = RaftPlane()
    try:
        plane.register_node(object())
        raise AssertionError("expected RuntimeError")
    except RuntimeError:
        pass


def test_routing_and_rpc_planes_smoke() -> None:
    rp = RoutingPlane()
    rp.record_unreachable(message_id="m", reason="no_fabric_path")
    assert rp.blackhole_stats()["blackhole_count"] == 1
    assert RpcPlane.hop_budget_exceeded("u").error.code == 1003
