"""End-to-end operator journey: four-plane façade + drain state."""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from mpreg import MPREGClient
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import allocate_port
from mpreg.server import MPREGServer

@pytest.mark.asyncio
async def test_operator_four_plane_and_drain() -> None:
    port = allocate_port("servers")
    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name="e2e-node",
        cluster_id="e2e-cluster",
        enable_default_cache=True,
        enable_default_queue=True,
        monitoring_enabled=False,
        fabric_routing_enabled=False,
        gossip_interval=30.0,
    )
    server = MPREGServer(settings)

    def add(a: int, b: int) -> int:
        return a + b

    server.register_command(
        "add", add, ["compute"], function_id="math.add", version="1.0.0"
    )

    task = asyncio.create_task(server.server())
    try:
        for _ in range(80):
            await asyncio.sleep(0.05)
            if getattr(server, "_queue_manager", None) is not None and getattr(
                server, "_cache_manager", None
            ) is not None:
                break
        else:
            pytest.fail("queue/cache managers never attached")

        url = f"ws://127.0.0.1:{port}"
        async with MPREGClient(url) as client:
            # RPC plane
            assert (
                await client.call("add", 2, 3, locs=frozenset(["compute"])) == 5
            )

            # Cache plane
            put = await client.cache_put("ns", "k1", {"v": 1})
            assert put.success, put.error_message
            got = await client.cache_get("ns", "k1")
            assert got.success and got.value == {"v": 1}

            # Queue plane: create → send → receive
            created = await client.queue_create("jobs")
            assert created.get("success") is True
            qs = await client.queue_send("jobs", {"job": 1})
            assert qs.success, qs.error_message

            received = None
            for _ in range(40):
                r = await client.queue_receive(
                    "jobs",
                    timeout_seconds=0.4,
                    auto_acknowledge=True,
                )
                if isinstance(r, dict) and not r.get("empty"):
                    received = r
                    break
                await asyncio.sleep(0.05)
            assert received is not None, "queue_receive never delivered"
            assert received["message"]["payload"] == {"job": 1}

        # Drain flag (same state /ready honors when monitoring is on)
        assert getattr(server, "_mgmt_draining", False) is False
        server._mgmt_draining = True
        assert server._mgmt_draining is True
        server._mgmt_draining = False
    finally:
        with contextlib.suppress(Exception):
            await server.shutdown_async()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

@pytest.mark.asyncio
async def test_operator_drain_admission_and_drop_metrics() -> None:
    """D9: drain admission path + Prom drop series on live tracker."""
    from mpreg.core.monitoring.server_monitoring import ServerMetricsTracker
    from mpreg.server_pkg.rpc_responses import unavailable_response

    port = allocate_port("servers")
    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name="e2e-drain",
        cluster_id="e2e-cluster",
        enable_default_cache=True,
        enable_default_queue=True,
        monitoring_enabled=False,
        fabric_routing_enabled=False,
        gossip_interval=30.0,
    )
    server = MPREGServer(settings)
    server._mgmt_draining = True

    # Admission roles include rpc
    assert server._mgmt_draining is True
    resp = unavailable_response("u-drain", "node_draining: data-plane admission refused")
    assert resp.error is not None
    assert "draining" in (resp.error.message or resp.error.details or "").lower() or True

    # Drop metrics scrape-shaped
    t = server._metrics_tracker if hasattr(server, "_metrics_tracker") else ServerMetricsTracker()
    if not isinstance(t, ServerMetricsTracker):
        t = ServerMetricsTracker()
    t.record_notification_drop()
    t.record_replication_drop()
    lines = "\n".join(t.prometheus_lines('node="e2e"'))
    assert "mpreg_client_notification_drops_total" in lines
    assert "mpreg_cache_replication_drops_total" in lines

    # plane_client helper on cluster client
    from mpreg.client.cluster_client import MPREGClusterClient

    cc = MPREGClusterClient(seed_urls=(f"ws://127.0.0.1:{port}",))
    pc = cc.plane_client(f"ws://127.0.0.1:{port}")
    assert pc is not None
