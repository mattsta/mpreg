"""Management API v1 read models."""

from __future__ import annotations

import asyncio
import contextlib

import aiohttp

from mpreg.core.config import MPREGSettings
from mpreg.core.monitoring.unified_monitoring import (
    MonitoringConfig,
    UnifiedSystemMonitor,
)
from mpreg.fabric.connection_manager import FederationConnectionManager
from mpreg.fabric.federation_config import FederationConfig, FederationMode
from mpreg.fabric.monitoring_endpoints import create_federation_monitoring_system
from mpreg.server_pkg.mgmt_summary import build_mgmt_v1_summary

def test_build_mgmt_summary_from_settings_only() -> None:
    class FakeServer:
        settings = MPREGSettings(name="n1", cluster_id="c1", resources={"r"})
        cluster = None
        _fabric_control_plane = None

    summary = build_mgmt_v1_summary(FakeServer())
    assert summary["cluster"]["cluster_id"] == "c1"
    assert summary["catalog"]["nodes"] >= 1
    assert "health" in summary

async def test_mgmt_v1_http_endpoints(server_cluster_ports: list[int]) -> None:
    port = server_cluster_ports[0]
    monitoring_port = server_cluster_ports[1]
    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name="mgmt-test",
        cluster_id="mgmt-cluster",
    )
    federation_config = FederationConfig(
        federation_mode=FederationMode.STRICT_ISOLATION,
        local_cluster_id=settings.cluster_id,
    )
    federation_manager = FederationConnectionManager(
        federation_config=federation_config
    )
    unified_monitor = UnifiedSystemMonitor(config=MonitoringConfig())
    task = asyncio.create_task(unified_monitor.start())

    def provider():
        return {
            "cluster": {"cluster_id": "mgmt-cluster", "node_count": 1},
            "nodes": [{"node_id": "n1"}],
            "routes": [],
            "catalog": {"functions": 0},
            "health": {"status": "ok"},
        }

    try:
        mon = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=federation_manager,
            unified_monitor=unified_monitor,
            monitoring_port=monitoring_port,
            mgmt_summary_provider=provider,
        )
        await mon.start()
        try:
            base = f"http://127.0.0.1:{monitoring_port}"
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{base}/mgmt/v1/cluster") as response:
                    assert response.status == 200
                    data = await response.json()
                    assert data["cluster_id"] == "mgmt-cluster"
                async with session.get(f"{base}/mgmt/v1/nodes") as response:
                    assert response.status == 200
                    data = await response.json()
                    assert data["nodes"][0]["node_id"] == "n1"
                async with session.get(f"{base}/mgmt/v1/health") as response:
                    assert response.status == 200
                    data = await response.json()
                    assert data["health"]["status"] == "ok"
        finally:
            await mon.stop()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await unified_monitor.stop()
