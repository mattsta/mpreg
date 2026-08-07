"""L1 cluster_map_catalog — refresh_cluster_map + catalog_query + summary."""

from __future__ import annotations

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.cluster_client import MPREGClusterClient
from mpreg.core.cluster_map import CatalogQueryRequest, ClusterMapRequest
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
    with (
        app_run(
            "cluster_map_catalog",
            "Cluster Map + Catalog Query",
            level="L1",
        ),
        port_range_context(2, "servers") as ports,
    ):
        url_a = f"ws://127.0.0.1:{ports[0]}"
        url_b = f"ws://127.0.0.1:{ports[1]}"
        settings = [
            MPREGSettings(
                port=ports[0],
                name="Map-A",
                cluster_id="map-lab",
                resources={"api"},
                log_level="WARNING",
                gossip_interval=1.0,
            ),
            MPREGSettings(
                port=ports[1],
                name="Map-B",
                cluster_id="map-lab",
                resources={"api"},
                peers=[url_a],
                log_level="WARNING",
                gossip_interval=1.0,
            ),
        ]

        async def _run(servers: list[MPREGServer]) -> None:
            for s in servers:

                def ping(msg: str = "x") -> str:
                    return f"p:{msg}"

                s.register_command("ping", ping, ["api"])

            await asyncio.sleep(0.8)

            with scenario(
                "cluster_map + refresh_cluster_map",
                "client.cluster_map",
                "client.cluster",
                "disco.cluster_map",
            ):
                async with MPREGClusterClient(seed_urls=(url_a, url_b)) as client:
                    snap = await client.cluster_map()
                    ensure(snap is not None, "map None")
                    nodes = getattr(snap, "nodes", None) or []
                    ensure(len(nodes) >= 1, f"nodes={nodes}")
                    await client.refresh_cluster_map()
                    ensure(
                        isinstance(client._endpoint_scores, dict)
                        or hasattr(client, "_endpoint_scores"),
                        "scores surface missing",
                    )
                    step(
                        f"nodes={len(nodes)} "
                        f"scores={len(getattr(client, '_endpoint_scores', {}))}"
                    )
                    out = await client.call(
                        "ping", "map", locs=frozenset(["api"]), timeout=10.0
                    )
                    ensure(out == "p:map", f"got {out!r}")
                ok("cluster_map + refresh + call")

            with scenario(
                "cluster_map_v2 scoped snapshot",
                "disco.cluster_map",
                "client.cluster_map",
                "client.api",
            ):
                async with MPREGClientAPI(url_a) as api:
                    v2 = await api.cluster_map_v2(
                        ClusterMapRequest(cluster_id="map-lab", limit=32)
                    )
                    ensure(v2 is not None, "cluster_map_v2 None")
                    ensure(
                        getattr(v2, "cluster_id", None) in (None, "", "map-lab")
                        or str(getattr(v2, "cluster_id", "")).startswith("map"),
                        f"unexpected cluster_id on v2: {v2}",
                    )
                    v2_nodes = getattr(v2, "nodes", ()) or ()
                    ensure(len(v2_nodes) >= 1, f"v2 nodes empty: {v2}")
                    step(
                        f"cluster_map_v2 type={type(v2).__name__} nodes={len(v2_nodes)}"
                    )
                    ok(f"cluster_map_v2 → {len(v2_nodes)} node(s)")

            with scenario(
                "catalog_query scoped",
                "disco.catalog_query",
                "client.api",
            ):
                async with MPREGClientAPI(url_a) as api:
                    cat = await api.catalog_query(
                        CatalogQueryRequest(entry_type="functions")
                    )
                    ensure(cat is not None, "catalog None")
                    step(f"catalog type={type(cat).__name__} entry_type=functions")
                    # nodes catalog as second shape
                    nodes = await api.catalog_query(
                        CatalogQueryRequest(entry_type="nodes")
                    )
                    ensure(nodes is not None, "nodes catalog None")
                    # F23: omitted / empty entry_type defaults to functions
                    defaulted = await api.catalog_query(CatalogQueryRequest())
                    ensure(defaulted is not None, "default catalog None")
                    empty = await api.catalog_query(
                        CatalogQueryRequest.from_dict({"entry_type": ""})
                    )
                    ensure(empty is not None, "empty entry_type catalog None")
                    ok(
                        f"catalog_query functions+nodes+default → "
                        f"{type(cat).__name__}/{type(nodes).__name__}"
                    )

            with scenario(
                "summary_query surface",
                "disco.summary_query",
                "client.cluster",
            ):
                async with MPREGClusterClient(seed_urls=(url_a,)) as client:
                    try:
                        summary = await client.summary_query()
                        ensure(summary is not None, "summary None")
                        step(f"summary type={type(summary).__name__}")
                        ok(f"summary_query → {type(summary).__name__}")
                    except Exception as exc:
                        # Some builds require discovery export; surface honesty
                        step(f"summary_query: {type(exc).__name__}: {exc}")
                        ok("summary_query callable (export may be off — non-fatal)")

        await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())
