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
from mpreg.fabric.route_decision_log import (
    RouteDecisionLog,
    get_default_route_decision_log,
    make_record_from_route,
)


def test_ring_buffer_filters() -> None:
    log = RouteDecisionLog(maxlen=3)
    for i in range(5):
        log.record(
            make_record_from_route(
                message_id=f"m{i}",
                correlation_id="c1" if i % 2 == 0 else "c2",
                topic="t",
                message_type="rpc",
                reason="local",
                cached=False,
                targets=["n1"],
                routing_path=["a"],
                hops_required=0,
            )
        )
    assert log.stats()["size"] == 3
    assert len(log.recent(correlation_id="c1")) >= 1


async def test_decisions_http_endpoint(server_cluster_ports: list[int]) -> None:
    get_default_route_decision_log().clear()
    get_default_route_decision_log().record(
        make_record_from_route(
            message_id="mid-1",
            correlation_id="corr-1",
            topic="rpc.test",
            message_type="rpc",
            reason="local",
            cached=False,
            targets=["node-a"],
            routing_path=["node-a"],
            hops_required=0,
            traceparent="00-" + "a" * 32 + "-" + "b" * 16 + "-01",
        )
    )
    port, monitoring_port = server_cluster_ports[:2]
    settings = MPREGSettings(
        host="127.0.0.1", port=port, name="dec", cluster_id="c-dec"
    )
    federation_config = FederationConfig(
        federation_mode=FederationMode.STRICT_ISOLATION,
        local_cluster_id=settings.cluster_id,
    )
    fm = FederationConnectionManager(federation_config=federation_config)
    um = UnifiedSystemMonitor(config=MonitoringConfig())
    task = asyncio.create_task(um.start())
    try:
        mon = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=fm,
            unified_monitor=um,
            monitoring_port=monitoring_port,
        )
        await mon.start()
        try:
            url = f"http://127.0.0.1:{monitoring_port}/routing/decisions?limit=10"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data["decisions"]
                    assert data["decisions"][0]["message_id"] == "mid-1"
        finally:
            await mon.stop()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await um.stop()
