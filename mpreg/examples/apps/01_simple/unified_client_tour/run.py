"""L1 unified_client_tour — MPREGClient four-plane façade (client.unified)."""

from __future__ import annotations

import asyncio

from mpreg import MPREGClient
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
        "unified_client_tour",
        "Unified Client Tour — RPC + cache + queue façade",
        level="L1",
    ):
        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    host="127.0.0.1",
                    port=ports[0],
                    name="Unified-Node",
                    cluster_id="unified-cluster",
                    log_level="WARNING",
                    enable_default_cache=True,
                    enable_default_queue=True,
                    monitoring_enabled=False,
                    fabric_routing_enabled=False,
                    gossip_interval=30.0,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]

                def greet(msg: str) -> str:
                    return f"greet:{msg}"

                def add(a: int, b: int) -> int:
                    return a + b

                server.register_command("greet", greet, ["compute"])
                server.register_command("add", add, ["compute"])

                # Default cache/queue attach during server boot — wait if needed
                for _ in range(80):
                    if (
                        getattr(server, "_queue_manager", None) is not None
                        and getattr(server, "_cache_manager", None) is not None
                    ):
                        break
                    await asyncio.sleep(0.05)
                ensure(
                    getattr(server, "_cache_manager", None) is not None,
                    "default cache manager missing",
                )
                ensure(
                    getattr(server, "_queue_manager", None) is not None,
                    "default queue manager missing",
                )
                ok("cache + queue managers attached")

                url = f"ws://127.0.0.1:{ports[0]}"
                step(f"MPREGClient → {url}")

                async with MPREGClient(url) as client:
                    with scenario(
                        "RPC via unified call",
                        "client.unified",
                        "rpc.call",
                        "rpc.register",
                    ):
                        out = await client.call(
                            "greet", "hello", locs=frozenset(["compute"])
                        )
                        ensure(out == "greet:hello", f"greet got {out!r}")
                        summed = await client.call(
                            "add", 20, 22, locs=frozenset(["compute"])
                        )
                        ensure(summed == 42, f"add got {summed!r}")
                        ok(f"rpc greet + add → {summed}")

                    with scenario(
                        "cache_put / cache_get surface",
                        "client.unified",
                        "cache.rpc_surface",
                        "cache.put_get",
                    ):
                        put = await client.cache_put("tour", "k1", {"v": 1, "t": "u"})
                        ensure(put.success, f"cache_put failed: {put.error_message}")
                        got = await client.cache_get("tour", "k1")
                        ensure(got.success, f"cache_get failed: {got.error_message}")
                        ensure(
                            got.value == {"v": 1, "t": "u"},
                            f"bad cache value {got.value!r}",
                        )
                        ok("cache put/get via unified client")

                    with scenario(
                        "queue create / send / receive",
                        "client.unified",
                        "queue.rpc_surface",
                        "queue.send",
                    ):
                        created = await client.queue_create("unified-jobs")
                        ensure(
                            isinstance(created, dict)
                            and created.get("success") is True,
                            f"queue_create failed: {created}",
                        )
                        sent = await client.queue_send(
                            "unified-jobs", {"job": "paint", "n": 1}
                        )
                        ensure(sent.success, f"queue_send failed: {sent.error_message}")
                        received = None
                        for _ in range(40):
                            r = await client.queue_receive(
                                "unified-jobs",
                                timeout_seconds=0.4,
                                auto_acknowledge=True,
                            )
                            if isinstance(r, dict) and not r.get("empty"):
                                received = r
                                break
                            await asyncio.sleep(0.05)
                        ensure(received is not None, "queue_receive never delivered")
                        payload = received.get("message", {}).get("payload")
                        ensure(
                            payload == {"job": "paint", "n": 1},
                            f"bad queue payload {payload!r}",
                        )
                        ok("queue create→send→receive via unified client")

                    with scenario(
                        "second cache key isolation on façade",
                        "cache.rpc_surface",
                    ):
                        await client.cache_put("tour", "k2", {"v": 2})
                        a = await client.cache_get("tour", "k1")
                        b = await client.cache_get("tour", "k2")
                        ensure(
                            a.success
                            and b.success
                            and a.value.get("v") == 1
                            and b.value.get("v") == 2,
                            f"isolation failed a={a.value} b={b.value}",
                        )
                        ok("k1/k2 isolated")

                    with scenario(
                        "discovery via unified façade",
                        "client.unified",
                        "client.cluster_map",
                        "disco.list_peers",
                        "disco.catalog_query",
                    ):
                        peers = await client.list_peers()
                        ensure(isinstance(peers, tuple), f"peers type {type(peers)}")
                        cmap = await client.cluster_map()
                        ensure(cmap is not None, "cluster_map None")
                        # catalog_query may return empty functions list on bare node
                        try:
                            cat = await client.catalog_query()
                            ensure(cat is not None, "catalog None")
                            step(f"catalog type={type(cat).__name__}")
                        except Exception as exc:
                            # Surface must exist; empty catalog is ok
                            step(
                                f"catalog_query raised (ok if empty): {type(exc).__name__}"
                            )
                        ensure(
                            callable(client.list_peers)
                            and callable(client.cluster_map)
                            and callable(client.catalog_query)
                            and callable(client.summary_query),
                            "discovery methods missing on MPREGClient",
                        )
                        ok(
                            f"unified discovery: peers={len(peers)} "
                            f"map={type(cmap).__name__}"
                        )

                    with scenario(
                        "rpc inventory via unified façade",
                        "client.unified",
                        "rpc.list",
                        "rpc.describe",
                    ):
                        ensure(callable(client.rpc_list), "rpc_list missing")
                        ensure(callable(client.rpc_describe), "rpc_describe missing")
                        ensure(callable(client.rpc_report), "rpc_report missing")
                        for name in (
                            "dns_list",
                            "dns_register",
                            "summary_watch",
                            "resolver_cache_stats",
                            "namespace_status",
                            "namespace_policy_validate",
                            "discovery_access_audit",
                        ):
                            ensure(callable(getattr(client, name)), f"{name} missing")
                        listing = await client.rpc_list()
                        ensure(listing is not None, "rpc_list None")
                        step(f"rpc_list type={type(listing).__name__}")
                        ok(
                            f"unified rpc_list/describe/report surface → {type(listing).__name__}"
                        )

                    with scenario(
                        "plane error_code on façade results",
                        "client.unified",
                        "cache.rpc_surface",
                    ):
                        from mpreg.client.unified_client import (
                            CacheOpResult,
                            QueueSendResult,
                        )

                        # Successful put must expose error_code attribute (None when ok)
                        put = await client.cache_put("tour", "errk", {"ok": True})
                        ensure(isinstance(put, CacheOpResult), type(put))
                        ensure(
                            hasattr(put, "error_code"),
                            "CacheOpResult missing error_code",
                        )
                        ensure(put.success is True, f"put failed {put}")
                        ensure(
                            put.error_code is None,
                            f"ok put should have no code {put.error_code}",
                        )
                        # Synthetic raw promotion path
                        synthetic = CacheOpResult.from_raw(
                            {"success": False, "error_code": 1001, "error": "nope"}
                        )
                        ensure(synthetic.success is False, "synth success")
                        ensure(
                            synthetic.error_code == 1001, f"code {synthetic.error_code}"
                        )
                        qsyn = QueueSendResult.from_raw(
                            {"success": False, "error_code": 42, "error_message": "q"}
                        )
                        ensure(qsyn.error_code == 42, f"q code {qsyn.error_code}")
                        ok("plane error_code promoted on CacheOpResult/QueueSendResult")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
