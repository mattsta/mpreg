import asyncio

import aiohttp

from mpreg.core.config import MPREGSettings
from mpreg.core.monitoring.unified_monitoring import MonitoringConfig, UnifiedSystemMonitor
from mpreg.fabric.connection_manager import FederationConnectionManager
from mpreg.fabric.federation_config import FederationConfig, FederationMode
from mpreg.fabric.monitoring_endpoints import create_federation_monitoring_system

async def test_policy_dry_run_endpoint(server_cluster_ports: list[int]) -> None:
    port, monitoring_port = server_cluster_ports[:2]
    settings = MPREGSettings(
        host="127.0.0.1", port=port, name="pol", cluster_id="c-pol"
    )
    federation_config = FederationConfig(
        federation_mode=FederationMode.STRICT_ISOLATION,
        local_cluster_id=settings.cluster_id,
    )
    fm = FederationConnectionManager(federation_config=federation_config)
    um = UnifiedSystemMonitor(config=MonitoringConfig())
    task = asyncio.create_task(um.start())

    def provider(body: dict):
        return {
            "dry_run": True,
            "allowed": body.get("namespace") != "blocked",
            "namespace": body.get("namespace"),
            "reason": "test",
        }

    try:
        mon = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=fm,
            unified_monitor=um,
            monitoring_port=monitoring_port,
            policy_dry_run_provider=provider,
        )
        await mon.start()
        try:
            url = f"http://127.0.0.1:{monitoring_port}/mgmt/v1/policy/dry-run"
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json={"namespace": "ok"}) as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data["allowed"] is True
                async with session.post(url, json={"namespace": "blocked"}) as resp:
                    data = await resp.json()
                    assert data["allowed"] is False
        finally:
            await mon.stop()
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await um.stop()
