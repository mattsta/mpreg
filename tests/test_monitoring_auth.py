"""Monitoring bearer-token auth middleware."""

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


async def test_monitoring_requires_bearer_token(
    server_cluster_ports: list[int],
) -> None:
    port = server_cluster_ports[0]
    monitoring_port = server_cluster_ports[1]
    token = "secret-test-token"
    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name="auth-test",
        cluster_id="auth-cluster",
        monitoring_auth_token=token,
        monitoring_enable_cors=False,
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
    try:
        mon = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=federation_manager,
            unified_monitor=unified_monitor,
            monitoring_port=monitoring_port,
            enable_cors=False,
            auth_token=token,
        )
        await mon.start()
        try:
            base = f"http://127.0.0.1:{monitoring_port}"
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{base}/health") as response:
                    assert response.status == 401
                headers = {"Authorization": f"Bearer {token}"}
                async with session.get(f"{base}/health", headers=headers) as response:
                    assert response.status == 200
                headers2 = {"X-MPREG-Monitoring-Token": token}
                async with session.get(f"{base}/health", headers=headers2) as response:
                    assert response.status == 200
        finally:
            await mon.stop()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await unified_monitor.stop()
