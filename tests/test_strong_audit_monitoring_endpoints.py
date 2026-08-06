"""Integration: /metrics/strong, /metrics/shared-audit, /mgmt/v1/strong, prom series."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import aiohttp
import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.monitoring.unified_monitoring import (
    MonitoringConfig,
    UnifiedSystemMonitor,
)
from mpreg.fabric.connection_manager import FederationConnectionManager
from mpreg.fabric.federation_config import FederationConfig, FederationMode
from mpreg.fabric.monitoring_endpoints import create_federation_monitoring_system
from mpreg.server_pkg.monitoring_metrics import (
    build_shared_audit_metrics,
    build_strong_metrics,
)
from tests.conftest import AsyncTestContext

@pytest.mark.asyncio
async def test_strong_and_audit_monitoring_routes(
    test_context: AsyncTestContext,
    server_cluster_ports: list[int],
) -> None:
    port = server_cluster_ports[0]
    monitoring_port = server_cluster_ports[1]

    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name="StrongAuditMon-Server",
        cluster_id="strong-audit-mon",
        resources={"monitor"},
        gossip_interval=1.0,
        cache_strong_enabled=False,
        mgmt_audit_shared_enabled=False,
    )
    federation_config = FederationConfig(
        federation_mode=FederationMode.STRICT_ISOLATION,
        local_cluster_id=settings.cluster_id,
    )
    federation_manager = FederationConnectionManager(
        federation_config=federation_config
    )
    unified_monitor = UnifiedSystemMonitor(config=MonitoringConfig())
    test_context.tasks.append(asyncio.create_task(unified_monitor.start()))

    fake = SimpleNamespace(
        settings=settings,
        _cache_manager=None,
        _strong_local_backend=None,
        _strong_pending_purge_task=None,
        _shared_audit_store=None,
        _shared_audit_replicator=None,
    )

    monitoring_system = create_federation_monitoring_system(
        settings=settings,
        federation_config=federation_config,
        federation_manager=federation_manager,
        unified_monitor=unified_monitor,
        monitoring_port=monitoring_port,
        strong_metrics_provider=lambda: build_strong_metrics(fake),
        shared_audit_metrics_provider=lambda: build_shared_audit_metrics(fake),
    )
    await monitoring_system.start()
    base = f"http://127.0.0.1:{monitoring_port}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/metrics/strong") as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["status"] == "ok"
                assert data["strong"]["health"] == "disabled"

            async with session.get(f"{base}/mgmt/v1/strong") as resp:
                assert resp.status == 200
                data = await resp.json()
                assert "strong" in data

            async with session.get(f"{base}/metrics/shared-audit") as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["status"] == "ok"
                assert data["shared_audit"]["status"] == "disabled"

            async with session.get(f"{base}/metrics/prometheus") as resp:
                assert resp.status == 200
                text = await resp.text()
                assert "mpreg_info" in text
                assert "mpreg_strong_enabled" in text
                assert "mpreg_shared_audit_enabled" in text

            async with session.get(f"{base}/endpoints") as resp:
                assert resp.status == 200
                data = await resp.json()
                paths = {e["path"] for e in data.get("endpoints", data.get("routes", []))}
                # endpoints payload shape varies; also accept openapi
                if not paths:
                    async with session.get(f"{base}/openapi.json") as oresp:
                        odoc = await oresp.json()
                        paths = set((odoc.get("paths") or {}).keys())
                assert "/metrics/strong" in paths
                assert "/metrics/shared-audit" in paths
                assert "/mgmt/v1/strong" in paths
    finally:
        await monitoring_system.stop()
