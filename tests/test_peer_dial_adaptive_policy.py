from __future__ import annotations

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


def _make_server(*, peers: list[str] | None = None, port: int = 12345) -> MPREGServer:
    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name="PeerDialPolicyTest",
        cluster_id="peer-dial-policy-test",
        monitoring_enabled=False,
        peers=peers,
    )
    return MPREGServer(settings=settings)


def test_fast_connect_retry_cap_scales_down_with_cluster_size() -> None:
    server = _make_server()

    small = server._select_peer_connection_policy(
        fast_connect=True,
        peer_target_count=1,
        consecutive_failures=0,
        connected_ratio=1.0,
    )
    medium = server._select_peer_connection_policy(
        fast_connect=True,
        peer_target_count=9,
        consecutive_failures=0,
        connected_ratio=1.0,
    )
    sparse_large = server._select_peer_connection_policy(
        fast_connect=True,
        peer_target_count=25,
        consecutive_failures=0,
        connected_ratio=0.25,
    )
    sparse_very_large = server._select_peer_connection_policy(
        fast_connect=True,
        peer_target_count=49,
        consecutive_failures=0,
        connected_ratio=0.20,
    )

    assert small.max_retries >= 1
    assert medium.max_retries >= 1
    assert sparse_large.max_retries <= 1
    assert sparse_very_large.max_retries <= sparse_large.max_retries


def test_fast_connect_retry_cap_can_recover_after_failures() -> None:
    server = _make_server()

    large_sparse = server._select_peer_connection_policy(
        fast_connect=True,
        peer_target_count=25,
        consecutive_failures=8,
        connected_ratio=0.25,
    )
    large_healthy = server._select_peer_connection_policy(
        fast_connect=True,
        peer_target_count=25,
        consecutive_failures=8,
        connected_ratio=0.80,
    )

    assert large_sparse.max_retries <= 1
    assert large_healthy.max_retries >= large_sparse.max_retries
    assert large_healthy.max_retries <= 2


def test_peer_dial_parallelism_is_bounded_for_large_fabrics() -> None:
    server = _make_server()

    assert server._peer_dial_parallelism(1, connected_ratio=1.0) == 1
    assert server._peer_dial_parallelism(9, connected_ratio=1.0) == 3
    assert server._peer_dial_parallelism(16, connected_ratio=1.0) == 4
    assert server._peer_dial_parallelism(25, connected_ratio=0.4) <= 2
    assert server._peer_dial_parallelism(49, connected_ratio=0.3) == 1
    assert server._peer_dial_parallelism(49, connected_ratio=0.8) >= 2


def test_peer_dial_selection_spread_varies_by_node_identity() -> None:
    node_a = _make_server(port=12345)
    node_b = _make_server(port=12346)
    candidates = [f"ws://127.0.0.1:{20000 + index}" for index in range(20)]

    ordered_a = sorted(candidates, key=node_a._peer_dial_selection_spread)
    ordered_b = sorted(candidates, key=node_b._peer_dial_selection_spread)

    assert ordered_a[:8] != ordered_b[:8]


def test_peer_dial_backoff_base_scales_with_dial_pressure() -> None:
    server = _make_server()

    healthy = server._peer_dial_backoff_base_seconds(49, connected_ratio=0.9)
    sparse = server._peer_dial_backoff_base_seconds(49, connected_ratio=0.2)

    assert sparse > healthy
    assert sparse >= healthy * 1.5


def test_peer_reconcile_interval_scales_with_dial_pressure() -> None:
    server = _make_server()

    healthy = server._peer_reconcile_interval_seconds(49, connected_ratio=0.9)
    sparse = server._peer_reconcile_interval_seconds(49, connected_ratio=0.2)

    assert sparse > healthy
    assert sparse >= healthy * 1.5


def test_fabric_catalog_refresh_interval_scales_with_cluster_hint() -> None:
    small = _make_server()
    large = _make_server(
        peers=[f"ws://127.0.0.1:{20000 + index}" for index in range(49)]
    )

    small_interval = small._fabric_catalog_refresh_interval_seconds()
    large_interval = large._fabric_catalog_refresh_interval_seconds()

    assert small_interval >= 2.0
    assert large_interval > small_interval
    assert large_interval <= small.settings.fabric_catalog_ttl_seconds * 0.5
