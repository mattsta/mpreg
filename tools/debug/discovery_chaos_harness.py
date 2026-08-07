#!/usr/bin/env python3
"""
Discovery chaos harness to validate resilience during node restarts.

Run with:
  uv run python tools/debug/discovery_chaos_harness.py --help
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import random
import time
from collections.abc import Iterable
from dataclasses import dataclass

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import PortAllocator
from mpreg.datastructures.type_aliases import EndpointScope
from mpreg.server import MPREGServer


@dataclass(slots=True)
class ChaosConfig:
    feature_nodes: int
    duration_seconds: float
    restart_interval_seconds: float
    query_interval_seconds: float
    restart_resolver: bool
    summary_scope: EndpointScope | None
    seed: int


@dataclass(slots=True)
class NodeHandle:
    name: str
    url: str
    settings: MPREGSettings
    server: MPREGServer
    task: asyncio.Task[None]


@dataclass(slots=True)
class ChaosResults:
    query_count: int = 0
    error_count: int = 0
    restart_count: int = 0


def _register_market_functions(server: MPREGServer, node_name: str) -> None:
    def handler(payload: str) -> str:
        return f"{node_name}:{payload}"

    for name in (
        "svc.market.quote",
        "svc.market.indicator",
        "svc.market.chart",
        "svc.market.alerts",
        "svc.market.strategy",
    ):
        server.register_command(name, handler, ["market"])


async def _start_node(settings: MPREGSettings) -> NodeHandle:
    server = MPREGServer(settings=settings)
    if settings.name.startswith("feature-"):
        _register_market_functions(server, settings.name)
    task = asyncio.create_task(server.server())
    url = f"ws://127.0.0.1:{settings.port}"
    return NodeHandle(
        name=settings.name,
        url=url,
        settings=settings,
        server=server,
        task=task,
    )


async def _stop_node(handle: NodeHandle) -> None:
    handle.server.shutdown()
    with contextlib.suppress(TimeoutError):
        await handle.server.shutdown_async()
    handle.task.cancel()
    await asyncio.gather(handle.task, return_exceptions=True)


async def _query_loop(
    url: str,
    results: ChaosResults,
    *,
    interval_seconds: float,
    deadline: float,
    summary_scope: EndpointScope | None,
) -> None:
    async with MPREGClientAPI(url) as client:
        while time.monotonic() < deadline:
            results.query_count += 1
            try:
                await client.catalog_query(
                    entry_type="functions", namespace="svc.market"
                )
                if summary_scope:
                    await client.summary_query(
                        namespace="svc.market", scope=summary_scope
                    )
            except Exception:
                results.error_count += 1
            await asyncio.sleep(interval_seconds)


async def run_chaos(config: ChaosConfig) -> int:
    allocator = PortAllocator()
    ports = allocator.allocate_port_range(config.feature_nodes + 1, "testing")
    urls = [f"ws://127.0.0.1:{port}" for port in ports]
    cluster_id = "chaos-cluster"
    summary_scope = config.summary_scope or "global"

    feature_handles: list[NodeHandle] = []
    resolver_handle: NodeHandle | None = None
    bootstrap_url = urls[0]

    try:
        for i in range(config.feature_nodes):
            connect = bootstrap_url if i > 0 else None
            settings = MPREGSettings(
                host="127.0.0.1",
                port=ports[i],
                name=f"feature-{i}",
                cluster_id=cluster_id,
                connect=connect,
                resources={"market"},
                gossip_interval=0.2,
                discovery_summary_export_enabled=True,
                discovery_summary_export_interval_seconds=1.0,
                discovery_summary_export_scope=summary_scope,
                discovery_summary_export_include_unscoped=True,
            )
            feature_handles.append(await _start_node(settings))

        resolver_settings = MPREGSettings(
            host="127.0.0.1",
            port=ports[-1],
            name="resolver-0",
            cluster_id=cluster_id,
            connect=bootstrap_url,
            resources={"market"},
            gossip_interval=0.2,
            discovery_resolver_mode=True,
            discovery_summary_resolver_mode=True,
            discovery_summary_resolver_scopes=(summary_scope,),
        )
        resolver_handle = await _start_node(resolver_settings)

        await asyncio.sleep(2.5)

        results = ChaosResults()
        deadline = time.monotonic() + config.duration_seconds
        query_task = asyncio.create_task(
            _query_loop(
                resolver_handle.url,
                results,
                interval_seconds=config.query_interval_seconds,
                deadline=deadline,
                summary_scope=config.summary_scope,
            )
        )

        next_restart = time.monotonic() + config.restart_interval_seconds
        rng = random.Random(config.seed)
        while time.monotonic() < deadline:
            if time.monotonic() >= next_restart:
                targets: list[NodeHandle] = list(feature_handles)
                if config.restart_resolver and resolver_handle is not None:
                    targets.append(resolver_handle)
                if targets:
                    victim = rng.choice(targets)
                    print(f"Restarting {victim.name} at {victim.url}")
                    await _stop_node(victim)
                    replacement = await _start_node(victim.settings)
                    results.restart_count += 1
                    if resolver_handle and victim.name == resolver_handle.name:
                        resolver_handle = replacement
                    else:
                        for index, handle in enumerate(feature_handles):
                            if handle.name == victim.name:
                                feature_handles[index] = replacement
                                break
                next_restart += config.restart_interval_seconds
            await asyncio.sleep(0.2)

        await query_task
        print("Discovery chaos results")
        print(f"Queries: {results.query_count}")
        print(f"Errors: {results.error_count}")
        print(f"Restarts: {results.restart_count}")
        return 0
    finally:
        handles: Iterable[NodeHandle] = list(feature_handles)
        if resolver_handle is not None:
            handles = list(handles) + [resolver_handle]
        for handle in handles:
            await _stop_node(handle)
        for port in ports:
            allocator.release_port(port)


def _parse_args() -> ChaosConfig:
    parser = argparse.ArgumentParser(description="Discovery chaos harness")
    parser.add_argument("--feature-nodes", type=int, default=3)
    parser.add_argument("--duration", type=float, default=20.0)
    parser.add_argument("--restart-interval", type=float, default=5.0)
    parser.add_argument("--query-interval", type=float, default=1.0)
    parser.add_argument("--restart-resolver", action="store_true")
    parser.add_argument("--summary-scope", default="global")
    parser.add_argument("--seed", type=int, default=11)
    args = parser.parse_args()

    summary_scope: EndpointScope | None
    if args.summary_scope:
        summary_scope = str(args.summary_scope).strip().lower()
    else:
        summary_scope = None
    if summary_scope is not None and summary_scope not in {
        "local",
        "zone",
        "region",
        "global",
    }:
        raise ValueError(f"Unsupported summary scope: {summary_scope}")

    return ChaosConfig(
        feature_nodes=max(1, args.feature_nodes),
        duration_seconds=max(1.0, args.duration),
        restart_interval_seconds=max(0.5, args.restart_interval),
        query_interval_seconds=max(0.1, args.query_interval),
        restart_resolver=bool(args.restart_resolver),
        summary_scope=summary_scope,
        seed=args.seed,
    )


def main() -> int:
    config = _parse_args()
    return asyncio.run(run_chaos(config))


if __name__ == "__main__":
    raise SystemExit(main())
