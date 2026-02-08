#!/usr/bin/env python3
"""
Discovery load harness for catalog_query and summary_query.

Run with:
  uv run python tools/debug/discovery_load_harness.py --help
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import random
import time
from dataclasses import dataclass

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import PortAllocator
from mpreg.datastructures.type_aliases import EndpointScope
from mpreg.server import MPREGServer


@dataclass(slots=True)
class DiscoveryLoadConfig:
    feature_nodes: int
    resolver_nodes: int
    duration_seconds: float
    concurrency: int
    qps_per_client: float
    summary_scope: EndpointScope | None
    seed: int


@dataclass(slots=True)
class LoadSample:
    command: str
    latency_ms: float
    ok: bool
    error: str | None


@dataclass(slots=True)
class LoadReport:
    count: int
    errors: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


@dataclass(slots=True)
class ClusterHandle:
    servers: list[MPREGServer]
    tasks: list[asyncio.Task[None]]
    ports: list[int]
    allocator: PortAllocator
    resolver_url: str


def _percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = max(0, min(len(ordered) - 1, int(round(percent * (len(ordered) - 1)))))
    return ordered[index]


def _summarize_samples(samples: list[LoadSample]) -> dict[str, LoadReport]:
    grouped: dict[str, list[LoadSample]] = {}
    for sample in samples:
        grouped.setdefault(sample.command, []).append(sample)
    reports: dict[str, LoadReport] = {}
    for command, entries in grouped.items():
        latencies = [entry.latency_ms for entry in entries]
        errors = sum(1 for entry in entries if not entry.ok)
        reports[command] = LoadReport(
            count=len(entries),
            errors=errors,
            p50_ms=_percentile(latencies, 0.50),
            p95_ms=_percentile(latencies, 0.95),
            p99_ms=_percentile(latencies, 0.99),
            max_ms=max(latencies) if latencies else 0.0,
        )
    return reports


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


async def _start_cluster(config: DiscoveryLoadConfig) -> ClusterHandle:
    allocator = PortAllocator()
    total_nodes = config.feature_nodes + config.resolver_nodes
    ports = allocator.allocate_port_range(total_nodes, "testing")
    urls = [f"ws://127.0.0.1:{port}" for port in ports]
    cluster_id = "load-cluster"
    summary_scope = config.summary_scope or "global"

    servers: list[MPREGServer] = []
    tasks: list[asyncio.Task[None]] = []
    bootstrap_url = urls[0] if urls else ""

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
        server = MPREGServer(settings=settings)
        _register_market_functions(server, settings.name)
        task = asyncio.create_task(server.server())
        servers.append(server)
        tasks.append(task)

    for i in range(config.resolver_nodes):
        index = config.feature_nodes + i
        settings = MPREGSettings(
            host="127.0.0.1",
            port=ports[index],
            name=f"resolver-{i}",
            cluster_id=cluster_id,
            connect=bootstrap_url or None,
            resources={"market"},
            gossip_interval=0.2,
            discovery_resolver_mode=True,
            discovery_summary_resolver_mode=True,
            discovery_summary_resolver_scopes=(summary_scope,),
        )
        server = MPREGServer(settings=settings)
        task = asyncio.create_task(server.server())
        servers.append(server)
        tasks.append(task)

    await asyncio.sleep(2.5)
    resolver_url = (
        urls[config.feature_nodes] if config.resolver_nodes > 0 else bootstrap_url
    )
    return ClusterHandle(
        servers=servers,
        tasks=tasks,
        ports=ports,
        allocator=allocator,
        resolver_url=resolver_url,
    )


async def _shutdown_cluster(handle: ClusterHandle) -> None:
    for server in handle.servers:
        server.shutdown()
    for server in handle.servers:
        with contextlib.suppress(TimeoutError):
            await server.shutdown_async()
    for task in handle.tasks:
        task.cancel()
    if handle.tasks:
        await asyncio.gather(*handle.tasks, return_exceptions=True)
    for port in handle.ports:
        handle.allocator.release_port(port)


async def _run_client(
    url: str,
    *,
    duration_seconds: float,
    qps: float,
    rng: random.Random,
    summary_scope: EndpointScope | None,
    samples: list[LoadSample],
) -> None:
    interval = 1.0 / qps if qps > 0 else 0.0
    deadline = time.monotonic() + duration_seconds
    async with MPREGClientAPI(url) as client:
        while time.monotonic() < deadline:
            command = "catalog_query"
            if summary_scope and rng.random() < 0.5:
                command = "summary_query"
            start = time.perf_counter()
            error: str | None = None
            ok = True
            try:
                if command == "summary_query":
                    await client.summary_query(
                        namespace="svc.market", scope=summary_scope
                    )
                else:
                    await client.catalog_query(
                        entry_type="functions", namespace="svc.market"
                    )
            except Exception as exc:
                ok = False
                error = type(exc).__name__
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            samples.append(
                LoadSample(
                    command=command,
                    latency_ms=elapsed_ms,
                    ok=ok,
                    error=error,
                )
            )
            if interval > 0:
                remaining = interval - (time.perf_counter() - start)
                if remaining > 0:
                    await asyncio.sleep(remaining)


async def run_load(config: DiscoveryLoadConfig) -> int:
    cluster = await _start_cluster(config)
    rng = random.Random(config.seed)
    samples: list[LoadSample] = []
    try:
        tasks = [
            asyncio.create_task(
                _run_client(
                    cluster.resolver_url,
                    duration_seconds=config.duration_seconds,
                    qps=config.qps_per_client,
                    rng=rng,
                    summary_scope=config.summary_scope,
                    samples=samples,
                )
            )
            for _ in range(config.concurrency)
        ]
        await asyncio.gather(*tasks)
        reports = _summarize_samples(samples)
        print("Discovery load results")
        print(f"Resolver URL: {cluster.resolver_url}")
        for command, report in reports.items():
            print(
                f"{command}: count={report.count} errors={report.errors} "
                f"p50={report.p50_ms:.2f}ms p95={report.p95_ms:.2f}ms "
                f"p99={report.p99_ms:.2f}ms max={report.max_ms:.2f}ms"
            )
        return 0
    finally:
        await _shutdown_cluster(cluster)


def _parse_args() -> DiscoveryLoadConfig:
    parser = argparse.ArgumentParser(description="Discovery load harness")
    parser.add_argument("--feature-nodes", type=int, default=3)
    parser.add_argument("--resolver-nodes", type=int, default=1)
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--qps", type=float, default=5.0)
    parser.add_argument("--summary-scope", default="global")
    parser.add_argument("--seed", type=int, default=7)
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

    return DiscoveryLoadConfig(
        feature_nodes=max(1, args.feature_nodes),
        resolver_nodes=max(0, args.resolver_nodes),
        duration_seconds=max(1.0, args.duration),
        concurrency=max(1, args.concurrency),
        qps_per_client=max(0.1, args.qps),
        summary_scope=summary_scope,
        seed=args.seed,
    )


def main() -> int:
    config = _parse_args()
    return asyncio.run(run_load(config))


if __name__ == "__main__":
    raise SystemExit(main())
