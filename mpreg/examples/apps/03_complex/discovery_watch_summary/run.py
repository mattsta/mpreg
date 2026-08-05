"""L3 discovery_watch_summary — catalog_watch deltas + summary_query/watch."""

from __future__ import annotations

import asyncio
import time

from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.pubsub_client import MPREGPubSubClient
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
)
from mpreg.server import MPREGServer

async def main() -> None:
    with app_run(
        "discovery_watch_summary",
        "Discovery Watch + Summary — deltas and service summaries",
        level="L3",
    ):
        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="disco-hub",
                    cluster_id="market",
                    log_level="WARNING",
                    gossip_interval=30.0,
                    resources={"market"},
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]
                url = f"ws://127.0.0.1:{ports[0]}"

                def quote_v1(symbol: str) -> dict[str, str]:
                    return {"symbol": symbol, "px": "100"}

                def indicator_v1(symbol: str) -> dict[str, str]:
                    return {"symbol": symbol, "rsi": "55"}

                server.register_command(
                    "svc.market.quote",
                    quote_v1,
                    ["market"],
                    function_id="market.quote",
                    version="1.0.0",
                )
                server.register_command(
                    "svc.market.indicator",
                    indicator_v1,
                    ["market"],
                    function_id="market.indicator",
                    version="1.0.0",
                )
                step(f"registered quote+indicator on {url}")

                async with MPREGClientAPI(url) as client:
                    with scenario(
                        "catalog_watch returns delta topic",
                        "disco.catalog_watch",
                        "client.api",
                    ):
                        watch_info = await client.catalog_watch()
                        topic = getattr(watch_info, "topic", "") or ""
                        ensure(bool(topic), f"empty catalog_watch topic: {watch_info}")
                        ok(f"catalog_watch topic={topic}")

                    with scenario(
                        "subscribe watch topic and see registration delta",
                        "disco.catalog_watch",
                        "pubsub.client_wire",
                    ):
                        pubsub = MPREGPubSubClient(base_client=client)
                        await pubsub.start()
                        try:
                            q: asyncio.Queue[object] = asyncio.Queue()

                            def on_delta(message: object) -> None:
                                q.put_nowait(message)

                            await pubsub.subscribe(
                                patterns=[topic],
                                callback=on_delta,
                                get_backlog=False,
                            )
                            await asyncio.sleep(0.15)

                            def quote_watch(symbol: str) -> dict[str, str]:
                                return {"symbol": symbol, "watch": "1"}

                            server.register_command(
                                "svc.market.quote_watch",
                                quote_watch,
                                ["market"],
                                function_id="market.quote_watch",
                                version="1.0.0",
                            )

                            deadline = time.time() + 6.0
                            found = False
                            while time.time() < deadline:
                                try:
                                    message = await asyncio.wait_for(
                                        q.get(),
                                        timeout=max(0.1, deadline - time.time()),
                                    )
                                except TimeoutError:
                                    break
                                payload = getattr(message, "payload", None)
                                if not isinstance(payload, dict):
                                    continue
                                delta = payload.get("delta", {})
                                if not isinstance(delta, dict):
                                    continue
                                functions = delta.get("functions", [])
                                if not isinstance(functions, list):
                                    continue
                                names = [
                                    entry.get("identity", {}).get("name")
                                    for entry in functions
                                    if isinstance(entry, dict)
                                ]
                                if "svc.market.quote_watch" in names:
                                    found = True
                                    break
                            ensure(found, "catalog delta never included quote_watch")
                            ok("delta stream observed svc.market.quote_watch")
                        finally:
                            await pubsub.stop()

                    with scenario(
                        "summary_query lists market services",
                        "disco.summary_query",
                    ):
                        service_ids: list[str] = []
                        deadline = time.time() + 5.0
                        while time.time() < deadline:
                            response = await client.summary_query(
                                namespace="svc.market"
                            )
                            items = getattr(response, "items", ()) or ()
                            service_ids = [
                                getattr(item, "service_id", "") for item in items
                            ]
                            if len(service_ids) >= 2:
                                break
                            await asyncio.sleep(0.1)
                        ensure(
                            "svc.market.quote" in service_ids,
                            f"quote missing from {service_ids}",
                        )
                        ensure(
                            "svc.market.indicator" in service_ids,
                            f"indicator missing from {service_ids}",
                        )
                        ok(f"summary services={service_ids}")

                    with scenario(
                        "summary_watch topic shapes",
                        "disco.summary_watch",
                    ):
                        scoped = await client.summary_watch(
                            scope="global", namespace="svc.market"
                        )
                        ensure(
                            scoped.topic
                            == "mpreg.discovery.summary.global.svc.market",
                            f"bad global ns topic {scoped.topic}",
                        )
                        root = await client.summary_watch(scope="global")
                        ensure(
                            root.topic == "mpreg.discovery.summary.global",
                            f"bad global root {root.topic}",
                        )
                        unscoped = await client.summary_watch(namespace="svc.market")
                        ensure(
                            unscoped.topic == "mpreg.discovery.summary.svc.market",
                            f"bad unscoped {unscoped.topic}",
                        )
                        ok("summary_watch topic shapes verified")

                    with scenario(
                        "catalog_query still works alongside watches",
                        "disco.catalog_query",
                    ):
                        if hasattr(client, "catalog_query"):
                            cq = await client.catalog_query(
                                entry_type="functions", namespace="svc.market"
                            )
                            items = getattr(cq, "items", ()) or ()
                            ensure(len(items) >= 1, f"catalog_query empty {cq}")
                            ok(f"catalog_query items={len(items)}")
                        else:
                            ok("catalog_query API absent — skipped")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
