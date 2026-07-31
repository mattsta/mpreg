"""Prometheus exposition endpoint."""

from __future__ import annotations

import asyncio

import aiohttp

from mpreg.core.config import MPREGSettings
from mpreg.core.monitoring.unified_monitoring import MonitoringConfig, UnifiedSystemMonitor
from mpreg.fabric.connection_manager import FederationConnectionManager
from mpreg.fabric.federation_config import FederationConfig, FederationMode
from mpreg.fabric.monitoring_endpoints import create_federation_monitoring_system

async def test_prometheus_endpoint_text_format(
    server_cluster_ports: list[int],
) -> None:
    port = server_cluster_ports[0]
    monitoring_port = server_cluster_ports[1]
    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name="prom-test",
        cluster_id="prom-cluster",
        monitoring_enable_cors=False,
    )
    federation_config = FederationConfig(
        federation_mode=FederationMode.STRICT_ISOLATION,
        local_cluster_id=settings.cluster_id,
    )
    federation_manager = FederationConnectionManager(federation_config=federation_config)
    unified_monitor = UnifiedSystemMonitor(config=MonitoringConfig())
    task = asyncio.create_task(unified_monitor.start())
    try:
        mon = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=federation_manager,
            unified_monitor=unified_monitor,
            monitoring_port=monitoring_port,
            enable_cors=False,
        )
        await mon.start()
        try:
            url = f"http://127.0.0.1:{monitoring_port}/metrics/prometheus"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    assert response.status == 200
                    text = await response.text()
                    assert "mpreg_info" in text
                    assert "mpreg_monitoring_up" in text
                    assert "text/plain" in response.headers.get("Content-Type", "")
        finally:
            await mon.stop()
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await unified_monitor.stop()
