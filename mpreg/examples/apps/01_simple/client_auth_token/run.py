"""L1 client_auth_token — monitoring bearer + client auth_token wiring."""

from __future__ import annotations

import asyncio

import aiohttp

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.monitoring.unified_monitoring import (
    MonitoringConfig,
    UnifiedSystemMonitor,
)
from mpreg.core.port_allocator import port_range_context
from mpreg.core.transport.interfaces import SecurityConfig, TransportConfig
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
)
from mpreg.fabric.connection_manager import FederationConnectionManager
from mpreg.fabric.federation_config import FederationConfig, FederationMode
from mpreg.fabric.monitoring_endpoints import create_federation_monitoring_system
from mpreg.server import MPREGServer

async def main() -> None:
    with app_run(
        "client_auth_token",
        "Client Auth Token — bearer monitoring + client wiring",
        level="L1",
    ):
        token = "curriculum-demo-token"
        with port_range_context(2, "servers") as ports:
            ws_port, mon_port = ports[0], ports[1]
            settings = [
                MPREGSettings(
                    host="127.0.0.1",
                    port=ws_port,
                    name="Auth-Node",
                    cluster_id="auth-demo",
                    resources={"compute"},
                    log_level="WARNING",
                    gossip_interval=30.0,
                    monitoring_auth_token=token,
                    monitoring_enable_cors=False,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]

                def ping(msg: str) -> str:
                    return f"pong:{msg}"

                server.register_command("ping", ping, ["compute"])

                federation_config = FederationConfig(
                    federation_mode=FederationMode.STRICT_ISOLATION,
                    local_cluster_id="auth-demo",
                )
                federation_manager = FederationConnectionManager(
                    federation_config=federation_config
                )
                unified = UnifiedSystemMonitor(config=MonitoringConfig())
                mon_task = asyncio.create_task(unified.start())
                mon = create_federation_monitoring_system(
                    settings=settings[0],
                    federation_config=federation_config,
                    federation_manager=federation_manager,
                    unified_monitor=unified,
                    monitoring_port=mon_port,
                    enable_cors=False,
                    auth_token=token,
                )
                await mon.start()
                try:
                    base = f"http://127.0.0.1:{mon_port}"
                    url = f"ws://127.0.0.1:{ws_port}"

                    with scenario(
                        "monitoring rejects unauthenticated",
                        "client.auth",
                        "tx.security",
                    ):
                        async with aiohttp.ClientSession() as session:
                            async with session.get(f"{base}/health") as resp:
                                ensure(
                                    resp.status == 401,
                                    f"expected 401 without token got {resp.status}",
                                )
                        ok("GET /health → 401 without bearer")

                    with scenario(
                        "Bearer token unlocks health",
                        "client.auth",
                        "mon.health",
                    ):
                        headers = {"Authorization": f"Bearer {token}"}
                        async with aiohttp.ClientSession() as session:
                            async with session.get(
                                f"{base}/health", headers=headers
                            ) as resp:
                                ensure(
                                    resp.status == 200,
                                    f"bearer health expected 200 got {resp.status}",
                                )
                                body = await resp.text()
                        ok(f"Bearer health 200 body_len={len(body)}")

                    with scenario(
                        "X-MPREG-Monitoring-Token header",
                        "client.auth",
                    ):
                        headers = {"X-MPREG-Monitoring-Token": token}
                        async with aiohttp.ClientSession() as session:
                            async with session.get(
                                f"{base}/health", headers=headers
                            ) as resp:
                                ensure(
                                    resp.status == 200,
                                    f"header health expected 200 got {resp.status}",
                                )
                        ok("X-MPREG-Monitoring-Token accepted")

                    with scenario(
                        "client auth_token wiring still RPCs",
                        "client.auth",
                        "client.api",
                        "rpc.call",
                    ):
                        # Demo wires auth_token into TransportConfig.security —
                        # local WS servers do not require it for RPC; prove plumbing.
                        transport = TransportConfig(
                            security=SecurityConfig(auth_token=token)
                        )
                        async with MPREGClientAPI(
                            url, auth_token=token, transport_config=transport
                        ) as client:
                            ensure(client.auth_token == token, "auth_token not set")
                            out = await client.call(
                                "ping", "auth", locs=frozenset(["compute"])
                            )
                            ensure(out == "pong:auth", f"ping got {out!r}")
                        ok("MPREGClientAPI(auth_token=…) RPC ok")

                    with scenario(
                        "wrong token still 401",
                        "tx.security",
                        "client.auth",
                    ):
                        headers = {"Authorization": "Bearer wrong-token"}
                        async with aiohttp.ClientSession() as session:
                            async with session.get(
                                f"{base}/health", headers=headers
                            ) as resp:
                                ensure(
                                    resp.status == 401,
                                    f"wrong token expected 401 got {resp.status}",
                                )
                        ok("wrong bearer rejected")
                        step(
                            "non-claim: full mTLS client cert path needs local CA story"
                        )
                finally:
                    await mon.stop()
                    mon_task.cancel()
                    try:
                        await mon_task
                    except asyncio.CancelledError:
                        pass
                    await unified.stop()

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
