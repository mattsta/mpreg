from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import get_port_allocator
from mpreg.server import MPREGServer

type Seconds = float


@dataclass(frozen=True, slots=True)
class HierarchicalTierConfig:
    name: str
    regions: int
    nodes_per_region: int


@dataclass(slots=True)
class HierarchicalRegionData:
    name: str
    servers: list[MPREGServer]
    ports: list[int]


@dataclass(slots=True)
class HierarchicalTierData:
    name: str
    regions: list[HierarchicalRegionData]
    coordinators: list[MPREGServer]


@dataclass(frozen=True, slots=True)
class PropagationSample:
    elapsed_seconds: Seconds
    nodes_with_function: int
    total_nodes: int
    success_rate: float


@dataclass(frozen=True, slots=True)
class ProbeConfig:
    initial_convergence_seconds: Seconds
    sample_interval_seconds: Seconds
    propagation_duration_seconds: Seconds
    post_fault_duration_seconds: Seconds
    failure_mode: str


@dataclass(frozen=True, slots=True)
class ProbeReport:
    generated_at_unix: float
    config: ProbeConfig
    total_nodes: int
    bridge_count: int
    propagation_samples: tuple[PropagationSample, ...]
    post_fault_samples: tuple[PropagationSample, ...]
    failed_coordinators: tuple[str, ...]
    propagation_peak: float
    post_fault_peak: float
    initial_function_holders: tuple[str, ...]
    initial_missing_nodes: tuple[str, ...]
    post_fault_function_holders: tuple[str, ...]
    post_fault_missing_nodes: tuple[str, ...]
    surviving_component_sizes: tuple[int, ...]
    initial_max_hops_from_global: int
    initial_unreachable_from_global: int
    post_fault_max_hops_from_global: int
    post_fault_unreachable_from_global: int


def _configure_logging() -> None:
    logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
    logging.getLogger("websockets.client").setLevel(logging.CRITICAL)


def _function_presence(
    *, servers: list[MPREGServer], function_name: str
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    holders: list[str] = []
    missing: list[str] = []
    for server in servers:
        node_name = server.settings.name
        if function_name in server.cluster.funtimes:
            holders.append(node_name)
        else:
            missing.append(node_name)
    return tuple(sorted(holders)), tuple(sorted(missing))


def _connected_component_sizes(servers: list[MPREGServer]) -> tuple[int, ...]:
    by_url = {server.cluster.local_url: server for server in servers}
    if not by_url:
        return tuple()
    remaining = set(by_url.keys())
    component_sizes: list[int] = []
    while remaining:
        start = remaining.pop()
        queue = [start]
        component = {start}
        while queue:
            node_url = queue.pop()
            server = by_url[node_url]
            peers = {
                peer_url
                for peer_url, connection in server._get_all_peer_connections().items()
                if connection.is_connected and peer_url in by_url
            }
            for peer_url in peers:
                if peer_url in component:
                    continue
                component.add(peer_url)
                remaining.discard(peer_url)
                queue.append(peer_url)
        component_sizes.append(len(component))
    return tuple(sorted(component_sizes, reverse=True))


def _hop_reachability(
    servers: list[MPREGServer], *, source_node_url: str
) -> tuple[int, int]:
    by_url = {server.cluster.local_url: server for server in servers}
    if source_node_url not in by_url:
        return 0, len(by_url)

    visited: dict[str, int] = {source_node_url: 0}
    queue = [source_node_url]
    while queue:
        current = queue.pop(0)
        current_hop = visited[current]
        server = by_url[current]
        peers = {
            peer_url
            for peer_url, connection in server._get_all_peer_connections().items()
            if connection.is_connected and peer_url in by_url
        }
        for peer_url in peers:
            if peer_url in visited:
                continue
            visited[peer_url] = current_hop + 1
            queue.append(peer_url)

    max_hops = max(visited.values()) if visited else 0
    unreachable = max(0, len(by_url) - len(visited))
    return max_hops, unreachable


async def _sample_function_propagation(
    *,
    servers: list[MPREGServer],
    function_name: str,
    duration_seconds: float,
    sample_interval_seconds: float,
) -> list[PropagationSample]:
    samples: list[PropagationSample] = []
    started_at = time.monotonic()
    while True:
        elapsed = time.monotonic() - started_at
        nodes_with_function = sum(
            1 for server in servers if function_name in server.cluster.funtimes
        )
        total_nodes = len(servers)
        success_rate = nodes_with_function / total_nodes if total_nodes > 0 else 0.0
        samples.append(
            PropagationSample(
                elapsed_seconds=elapsed,
                nodes_with_function=nodes_with_function,
                total_nodes=total_nodes,
                success_rate=success_rate,
            )
        )
        if elapsed >= duration_seconds:
            break
        await asyncio.sleep(sample_interval_seconds)
    return samples


async def _build_hierarchy(
    *,
    allocator,
    tier_configs: list[HierarchicalTierConfig],
) -> tuple[
    list[MPREGServer], list[HierarchicalTierData], list[asyncio.Task[None]], int
]:
    all_servers: list[MPREGServer] = []
    tier_data: list[HierarchicalTierData] = []
    tasks: list[asyncio.Task[None]] = []
    bridge_count = 0

    for tier_idx, tier_config in enumerate(tier_configs):
        tier_regions: list[HierarchicalRegionData] = []
        tier_servers: list[MPREGServer] = []
        for region_idx in range(tier_config.regions):
            region_name = f"{tier_config.name}-Region-{region_idx}"
            ports = allocator.allocate_port_range(
                tier_config.nodes_per_region, "research"
            )
            region_servers: list[MPREGServer] = []
            for node_idx, port in enumerate(ports):
                connect_to = f"ws://127.0.0.1:{ports[0]}" if node_idx > 0 else None
                settings = MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"{region_name}-Node-{node_idx}",
                    cluster_id="hierarchical-federation",
                    resources={
                        f"tier-{tier_idx}",
                        f"region-{region_idx}",
                        "balancing-resource",
                    },
                    peers=None,
                    connect=connect_to,
                    advertised_urls=None,
                    gossip_interval=0.4,
                    log_level="WARNING",
                    monitoring_enabled=False,
                )
                server = MPREGServer(settings=settings)
                region_servers.append(server)
                tier_servers.append(server)
                all_servers.append(server)
                task = asyncio.create_task(server.server())
                tasks.append(task)
                await asyncio.sleep(0.05)

            tier_regions.append(
                HierarchicalRegionData(
                    name=region_name, servers=region_servers, ports=ports
                )
            )

        tier_data.append(
            HierarchicalTierData(
                name=tier_config.name,
                regions=tier_regions,
                coordinators=[region.servers[0] for region in tier_regions],
            )
        )

    for tier_idx in range(len(tier_data) - 1):
        current_tier = tier_data[tier_idx]
        next_tier = tier_data[tier_idx + 1]
        for coordinator_index, current_coord in enumerate(current_tier.coordinators):
            target_index = coordinator_index % len(next_tier.coordinators)
            target = next_tier.coordinators[target_index]
            await current_coord._establish_peer_connection(
                f"ws://127.0.0.1:{target.settings.port}"
            )
            bridge_count += 1

    return all_servers, tier_data, tasks, bridge_count


async def _run_probe(
    *,
    config: ProbeConfig,
    output_json: Path | None,
) -> int:
    _configure_logging()
    allocator = get_port_allocator()
    tier_configs = [
        HierarchicalTierConfig(name="Local", regions=4, nodes_per_region=4),
        HierarchicalTierConfig(name="Regional", regions=2, nodes_per_region=3),
        HierarchicalTierConfig(name="Global", regions=1, nodes_per_region=2),
    ]
    all_servers: list[MPREGServer] = []
    tier_data: list[HierarchicalTierData] = []
    tasks: list[asyncio.Task[None]] = []

    try:
        all_servers, tier_data, tasks, bridge_count = await _build_hierarchy(
            allocator=allocator,
            tier_configs=tier_configs,
        )

        print(f"started_nodes={len(all_servers)} bridge_count={bridge_count}")
        print(f"initial_convergence_sleep={config.initial_convergence_seconds:.2f}s")
        await asyncio.sleep(config.initial_convergence_seconds)

        def hierarchical_test(data: str) -> str:
            return f"hierarchical:{data}"

        global_coordinator = tier_data[2].coordinators[0]
        global_coordinator.register_command(
            "hierarchical_test", hierarchical_test, ["balancing-resource"]
        )

        propagation_samples = await _sample_function_propagation(
            servers=all_servers,
            function_name="hierarchical_test",
            duration_seconds=config.propagation_duration_seconds,
            sample_interval_seconds=config.sample_interval_seconds,
        )
        global_source_url = global_coordinator.cluster.local_url
        initial_max_hops, initial_unreachable = _hop_reachability(
            all_servers,
            source_node_url=global_source_url,
        )

        failed_coordinators: list[MPREGServer] = []
        for tier in tier_data[:-1]:
            if len(tier.coordinators) <= 1:
                continue
            failed = tier.coordinators[1]
            failed_coordinators.append(failed)
            if config.failure_mode == "shutdown":
                await failed.shutdown_async()
            else:
                for connection in list(failed.peer_connections.values()):
                    with contextlib.suppress(Exception):
                        await connection.disconnect()

        await asyncio.sleep(0.5)

        def post_fault_test(data: str) -> str:
            return f"post-fault:{data}"

        global_coordinator.register_command(
            "post_fault_test", post_fault_test, ["balancing-resource"]
        )

        surviving_servers = [
            server for server in all_servers if server not in failed_coordinators
        ]
        post_fault_samples = await _sample_function_propagation(
            servers=surviving_servers,
            function_name="post_fault_test",
            duration_seconds=config.post_fault_duration_seconds,
            sample_interval_seconds=config.sample_interval_seconds,
        )

        propagation_peak = max(sample.success_rate for sample in propagation_samples)
        post_fault_peak = max(sample.success_rate for sample in post_fault_samples)
        initial_holders, initial_missing = _function_presence(
            servers=all_servers,
            function_name="hierarchical_test",
        )
        post_fault_holders, post_fault_missing = _function_presence(
            servers=surviving_servers,
            function_name="post_fault_test",
        )
        component_sizes = _connected_component_sizes(surviving_servers)
        post_fault_max_hops, post_fault_unreachable = _hop_reachability(
            surviving_servers,
            source_node_url=global_source_url,
        )

        print(f"propagation_peak={propagation_peak:.4f}")
        print(f"post_fault_peak={post_fault_peak:.4f}")
        print(f"surviving_component_sizes={component_sizes}")
        print(
            "global_hops "
            f"initial_max={initial_max_hops} "
            f"initial_unreachable={initial_unreachable} "
            f"post_fault_max={post_fault_max_hops} "
            f"post_fault_unreachable={post_fault_unreachable}"
        )
        print(
            f"initial_function_holders={len(initial_holders)}/{len(all_servers)} "
            f"post_fault_holders={len(post_fault_holders)}/{len(surviving_servers)}"
        )

        report = ProbeReport(
            generated_at_unix=time.time(),
            config=config,
            total_nodes=len(all_servers),
            bridge_count=bridge_count,
            propagation_samples=tuple(propagation_samples),
            post_fault_samples=tuple(post_fault_samples),
            failed_coordinators=tuple(
                server.settings.name for server in failed_coordinators
            ),
            propagation_peak=propagation_peak,
            post_fault_peak=post_fault_peak,
            initial_function_holders=initial_holders,
            initial_missing_nodes=initial_missing,
            post_fault_function_holders=post_fault_holders,
            post_fault_missing_nodes=post_fault_missing,
            surviving_component_sizes=component_sizes,
            initial_max_hops_from_global=initial_max_hops,
            initial_unreachable_from_global=initial_unreachable,
            post_fault_max_hops_from_global=post_fault_max_hops,
            post_fault_unreachable_from_global=post_fault_unreachable,
        )

        if output_json is not None:
            output_json.parent.mkdir(parents=True, exist_ok=True)
            output_json.write_text(
                json.dumps(asdict(report), indent=2, sort_keys=True),
                encoding="utf-8",
            )
            print(f"wrote_report={output_json}")

        return 0
    finally:
        await asyncio.gather(
            *(server.shutdown_async() for server in all_servers),
            return_exceptions=True,
        )
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for tier in tier_data:
            for region in tier.regions:
                for port in region.ports:
                    allocator.release_port(port)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe hierarchical federation propagation/fault timelines"
    )
    parser.add_argument(
        "--initial-convergence-seconds",
        type=float,
        default=3.0,
    )
    parser.add_argument(
        "--sample-interval-seconds",
        type=float,
        default=0.5,
    )
    parser.add_argument(
        "--propagation-duration-seconds",
        type=float,
        default=8.0,
    )
    parser.add_argument(
        "--post-fault-duration-seconds",
        type=float,
        default=8.0,
    )
    parser.add_argument(
        "--failure-mode",
        choices=("disconnect", "shutdown"),
        default="disconnect",
        help="How to simulate coordinator failure in lower tiers",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    output_json = Path(args.output_json).resolve() if args.output_json else None
    config = ProbeConfig(
        initial_convergence_seconds=max(0.0, float(args.initial_convergence_seconds)),
        sample_interval_seconds=max(0.1, float(args.sample_interval_seconds)),
        propagation_duration_seconds=max(0.1, float(args.propagation_duration_seconds)),
        post_fault_duration_seconds=max(0.1, float(args.post_fault_duration_seconds)),
        failure_mode=str(args.failure_mode),
    )
    return asyncio.run(_run_probe(config=config, output_json=output_json))


if __name__ == "__main__":
    raise SystemExit(main())
