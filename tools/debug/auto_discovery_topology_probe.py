#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import get_port_allocator
from mpreg.fabric.gossip import GossipMessageType
from mpreg.server import MPREGServer

type NodeCount = int
type Seconds = float
type TopologyName = str


@dataclass(frozen=True, slots=True)
class ProbeConfig:
    nodes: NodeCount
    topology: TopologyName
    duration_seconds: Seconds
    sample_interval_seconds: Seconds
    gossip_interval_seconds: Seconds
    log_level: str
    concurrency_factor: float
    peer_dial_diagnostics: bool


@dataclass(frozen=True, slots=True)
class ThresholdConfig:
    min_discovered_peers: int
    min_nodes_meeting_threshold: int
    discovery_timeout_seconds: Seconds


@dataclass(frozen=True, slots=True)
class ConvergenceSnapshot:
    elapsed_seconds: Seconds
    min_discovered_function_catalog: int
    avg_discovered_function_catalog: float
    max_discovered_function_catalog: int
    min_discovered_node_catalog: int
    avg_discovered_node_catalog: float
    max_discovered_node_catalog: int
    min_discovered_peer_directory: int
    avg_discovered_peer_directory: float
    max_discovered_peer_directory: int
    nodes_meeting_function_threshold: int
    nodes_meeting_node_threshold: int
    nodes_meeting_peer_directory_threshold: int
    min_active_connections: int
    avg_active_connections: float
    max_active_connections: int
    min_target_hint: int
    avg_target_hint: float
    max_target_hint: int
    min_desired_connections: int
    avg_desired_connections: float
    max_desired_connections: int
    min_connected_ratio: float
    avg_connected_ratio: float
    max_connected_ratio: float
    min_pending_messages: int
    avg_pending_messages: float
    max_pending_messages: int
    min_pending_catalog_messages: int
    avg_pending_catalog_messages: float
    max_pending_catalog_messages: int
    min_pending_catalog_unique_updates: int
    avg_pending_catalog_unique_updates: float
    max_pending_catalog_unique_updates: int
    min_pending_catalog_duplicate_updates: int
    avg_pending_catalog_duplicate_updates: float
    max_pending_catalog_duplicate_updates: int


@dataclass(frozen=True, slots=True)
class NodeDiscoveryDetails:
    node_id: str
    discovered_function_catalog: int
    discovered_node_catalog: int
    discovered_peer_directory: int
    missing_function_catalog: tuple[str, ...]
    active_connections: int
    target_hint: int
    desired_connections: int
    connected_ratio: float
    pending_messages: int
    pending_catalog_messages: int


@dataclass(frozen=True, slots=True)
class ProbeResult:
    converged: bool
    converged_at_seconds: Seconds | None
    threshold: ThresholdConfig
    final_snapshot: ConvergenceSnapshot
    snapshot_count: int
    nodes: tuple[NodeDiscoveryDetails, ...]


@dataclass(frozen=True, slots=True)
class ProbeReport:
    generated_at_unix: float
    config: ProbeConfig
    result: ProbeResult
    snapshots: tuple[ConvergenceSnapshot, ...]


def _configure_log_noise_suppression() -> None:
    logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
    logging.getLogger("websockets.client").setLevel(logging.CRITICAL)


def _build_threshold(config: ProbeConfig) -> ThresholdConfig:
    expected_peers = max(config.nodes - 1, 1)
    if config.nodes >= 30:
        min_discovered_peers = int(expected_peers * 0.8)
        min_nodes_meeting_threshold = int(config.nodes * 0.8)
        base_timeout = max(45.0, config.nodes * 1.0)
    elif config.nodes >= 10:
        min_discovered_peers = expected_peers
        min_nodes_meeting_threshold = config.nodes
        base_timeout = max(15.0, config.nodes * 1.0)
    else:
        min_discovered_peers = expected_peers
        min_nodes_meeting_threshold = config.nodes
        base_timeout = max(5.0, config.nodes * 0.5)
    return ThresholdConfig(
        min_discovered_peers=min_discovered_peers,
        min_nodes_meeting_threshold=min_nodes_meeting_threshold,
        discovery_timeout_seconds=base_timeout * config.concurrency_factor,
    )


def _connect_target(
    *,
    topology: TopologyName,
    node_index: int,
    ports: list[int],
) -> str | None:
    if topology == "LINEAR_CHAIN":
        if node_index == 0:
            return None
        return f"ws://127.0.0.1:{ports[node_index - 1]}"
    if topology == "STAR_HUB":
        if node_index == 0:
            return None
        return f"ws://127.0.0.1:{ports[0]}"
    if topology == "RING":
        if node_index == 0:
            return f"ws://127.0.0.1:{ports[-1]}"
        return f"ws://127.0.0.1:{ports[node_index - 1]}"
    if topology == "MULTI_HUB":
        hub_count = min(3, len(ports))
        if node_index < hub_count:
            if node_index == 0:
                return None
            return f"ws://127.0.0.1:{ports[node_index - 1]}"
        hub_port = ports[node_index % hub_count]
        return f"ws://127.0.0.1:{hub_port}"
    raise ValueError(f"Unsupported topology: {topology}")


def _node_settings(
    *,
    node_index: int,
    port: int,
    ports: list[int],
    config: ProbeConfig,
) -> MPREGSettings:
    connect_to = _connect_target(
        topology=config.topology,
        node_index=node_index,
        ports=ports,
    )
    return MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=f"AutoProbe-{config.topology}-{node_index}",
        cluster_id=f"probe-{config.topology.lower()}-{config.nodes}",
        resources={f"node-{node_index}", "probe-resource"},
        peers=None,
        connect=connect_to,
        advertised_urls=None,
        gossip_interval=config.gossip_interval_seconds,
        log_level=config.log_level,
        log_debug_scopes=("peer_dial",) if config.peer_dial_diagnostics else tuple(),
        monitoring_enabled=False,
    )


def _startup_pattern(node_count: int) -> tuple[int, float, float]:
    if node_count >= 50:
        return 5, 1.0, 0.5
    if node_count >= 20:
        return 8, 0.5, 0.3
    return node_count, 0.0, 0.1


async def _start_servers(
    *,
    servers: list[MPREGServer],
    tasks: list[asyncio.Task[None]],
) -> None:
    batch_size, batch_delay, per_node_delay = _startup_pattern(len(servers))
    print(
        f"startup=batch_size:{batch_size} batch_delay:{batch_delay} per_node_delay:{per_node_delay}"
    )
    for batch_start in range(0, len(servers), batch_size):
        batch_end = min(batch_start + batch_size, len(servers))
        print(
            f"  batch {batch_start // batch_size + 1}: nodes {batch_start}-{batch_end - 1}"
        )
        for server in servers[batch_start:batch_end]:
            tasks.append(asyncio.create_task(server.server()))
            await asyncio.sleep(per_node_delay)
        if batch_delay > 0 and batch_end < len(servers):
            await asyncio.sleep(batch_delay)


def _capture_snapshot(
    *,
    servers: list[MPREGServer],
    elapsed_seconds: Seconds,
    threshold: ThresholdConfig,
) -> ConvergenceSnapshot:
    function_counts: list[int] = []
    node_catalog_counts: list[int] = []
    peer_directory_counts: list[int] = []
    active_connection_counts: list[int] = []
    target_hints: list[int] = []
    desired_connections: list[int] = []
    connected_ratios: list[float] = []
    pending_messages: list[int] = []
    pending_catalog_messages: list[int] = []
    pending_catalog_unique_updates: list[int] = []
    pending_catalog_duplicate_updates: list[int] = []

    nodes_meeting_function_threshold = 0
    nodes_meeting_node_threshold = 0
    nodes_meeting_peer_directory_threshold = 0
    now = time.time()

    for server in servers:
        function_discovered = max(len(server.cluster.servers) - 1, 0)
        function_counts.append(function_discovered)
        if function_discovered >= threshold.min_discovered_peers:
            nodes_meeting_function_threshold += 1

        node_catalog_nodes: set[str] = set()
        if server._fabric_control_plane is not None:
            for entry in server._fabric_control_plane.index.catalog.nodes.entries(
                now=now
            ):
                node_catalog_nodes.add(entry.node_id)
        node_catalog_discovered = len(node_catalog_nodes - {server.cluster.local_url})
        node_catalog_counts.append(node_catalog_discovered)
        if node_catalog_discovered >= threshold.min_discovered_peers:
            nodes_meeting_node_threshold += 1

        peer_nodes: set[str] = set()
        if server._peer_directory is not None:
            peer_nodes = {node.node_id for node in server._peer_directory.nodes()}
        peer_directory_discovered = len(peer_nodes - {server.cluster.local_url})
        peer_directory_counts.append(peer_directory_discovered)
        if peer_directory_discovered >= threshold.min_discovered_peers:
            nodes_meeting_peer_directory_threshold += 1

        connected_count = sum(
            1
            for connection in server._get_all_peer_connections().values()
            if connection.is_connected
        )
        active_connection_counts.append(connected_count)

        target_hint = max(server._peer_dial_target_count_hint(), 1)
        target_hints.append(target_hint)
        connected_ratio = connected_count / max(target_hint, 1)
        connected_ratios.append(connected_ratio)
        desired_connections.append(
            server._peer_target_connection_count(target_hint, connected_ratio)
        )

        gossip = (
            server._fabric_control_plane.gossip
            if server._fabric_control_plane is not None
            else None
        )
        if gossip is None:
            pending_messages.append(0)
            pending_catalog_messages.append(0)
            pending_catalog_unique_updates.append(0)
            pending_catalog_duplicate_updates.append(0)
            continue
        pending_messages.append(len(gossip.pending_messages))
        catalog_updates = 0
        unique_updates: set[str] = set()
        for message in gossip.pending_messages:
            if message.message_type is not GossipMessageType.CATALOG_UPDATE:
                continue
            catalog_updates += 1
            payload = message.payload
            if isinstance(payload, dict):
                update_id = payload.get("update_id")
                if isinstance(update_id, str) and update_id:
                    unique_updates.add(update_id)
                    continue
            unique_updates.add(message.message_id)
        pending_catalog_messages.append(catalog_updates)
        unique_count = len(unique_updates)
        pending_catalog_unique_updates.append(unique_count)
        pending_catalog_duplicate_updates.append(max(catalog_updates - unique_count, 0))

    return ConvergenceSnapshot(
        elapsed_seconds=elapsed_seconds,
        min_discovered_function_catalog=min(function_counts),
        avg_discovered_function_catalog=sum(function_counts)
        / max(len(function_counts), 1),
        max_discovered_function_catalog=max(function_counts),
        min_discovered_node_catalog=min(node_catalog_counts),
        avg_discovered_node_catalog=sum(node_catalog_counts)
        / max(len(node_catalog_counts), 1),
        max_discovered_node_catalog=max(node_catalog_counts),
        min_discovered_peer_directory=min(peer_directory_counts),
        avg_discovered_peer_directory=sum(peer_directory_counts)
        / max(len(peer_directory_counts), 1),
        max_discovered_peer_directory=max(peer_directory_counts),
        nodes_meeting_function_threshold=nodes_meeting_function_threshold,
        nodes_meeting_node_threshold=nodes_meeting_node_threshold,
        nodes_meeting_peer_directory_threshold=nodes_meeting_peer_directory_threshold,
        min_active_connections=min(active_connection_counts),
        avg_active_connections=sum(active_connection_counts)
        / max(len(active_connection_counts), 1),
        max_active_connections=max(active_connection_counts),
        min_target_hint=min(target_hints),
        avg_target_hint=sum(target_hints) / max(len(target_hints), 1),
        max_target_hint=max(target_hints),
        min_desired_connections=min(desired_connections),
        avg_desired_connections=sum(desired_connections)
        / max(len(desired_connections), 1),
        max_desired_connections=max(desired_connections),
        min_connected_ratio=min(connected_ratios),
        avg_connected_ratio=sum(connected_ratios) / max(len(connected_ratios), 1),
        max_connected_ratio=max(connected_ratios),
        min_pending_messages=min(pending_messages),
        avg_pending_messages=sum(pending_messages) / max(len(pending_messages), 1),
        max_pending_messages=max(pending_messages),
        min_pending_catalog_messages=min(pending_catalog_messages),
        avg_pending_catalog_messages=sum(pending_catalog_messages)
        / max(len(pending_catalog_messages), 1),
        max_pending_catalog_messages=max(pending_catalog_messages),
        min_pending_catalog_unique_updates=min(pending_catalog_unique_updates),
        avg_pending_catalog_unique_updates=sum(pending_catalog_unique_updates)
        / max(len(pending_catalog_unique_updates), 1),
        max_pending_catalog_unique_updates=max(pending_catalog_unique_updates),
        min_pending_catalog_duplicate_updates=min(pending_catalog_duplicate_updates),
        avg_pending_catalog_duplicate_updates=sum(pending_catalog_duplicate_updates)
        / max(len(pending_catalog_duplicate_updates), 1),
        max_pending_catalog_duplicate_updates=max(pending_catalog_duplicate_updates),
    )


def _node_details(servers: list[MPREGServer]) -> tuple[NodeDiscoveryDetails, ...]:
    all_node_ids = sorted(server.cluster.local_url for server in servers)
    now = time.time()
    details: list[NodeDiscoveryDetails] = []
    for server in sorted(servers, key=lambda item: item.cluster.local_url):
        discovered_nodes = sorted(
            node_id
            for node_id in server.cluster.servers
            if node_id != server.cluster.local_url
        )
        node_catalog_nodes: set[str] = set()
        if server._fabric_control_plane is not None:
            for entry in server._fabric_control_plane.index.catalog.nodes.entries(
                now=now
            ):
                node_catalog_nodes.add(entry.node_id)
        peer_directory_nodes: set[str] = set()
        if server._peer_directory is not None:
            peer_directory_nodes = {
                node.node_id for node in server._peer_directory.nodes()
            }
        connected_count = sum(
            1
            for connection in server._get_all_peer_connections().values()
            if connection.is_connected
        )
        target_hint = max(server._peer_dial_target_count_hint(), 1)
        connected_ratio = connected_count / max(target_hint, 1)
        desired = server._peer_target_connection_count(target_hint, connected_ratio)

        pending_message_count = 0
        pending_catalog_count = 0
        gossip = (
            server._fabric_control_plane.gossip
            if server._fabric_control_plane is not None
            else None
        )
        if gossip is not None:
            pending_message_count = len(gossip.pending_messages)
            pending_catalog_count = sum(
                1
                for message in gossip.pending_messages
                if message.message_type is GossipMessageType.CATALOG_UPDATE
            )

        missing = tuple(
            node_id
            for node_id in all_node_ids
            if node_id != server.cluster.local_url and node_id not in discovered_nodes
        )
        details.append(
            NodeDiscoveryDetails(
                node_id=server.cluster.local_url,
                discovered_function_catalog=len(discovered_nodes),
                discovered_node_catalog=len(
                    node_catalog_nodes - {server.cluster.local_url}
                ),
                discovered_peer_directory=len(
                    peer_directory_nodes - {server.cluster.local_url}
                ),
                missing_function_catalog=missing,
                active_connections=connected_count,
                target_hint=target_hint,
                desired_connections=desired,
                connected_ratio=connected_ratio,
                pending_messages=pending_message_count,
                pending_catalog_messages=pending_catalog_count,
            )
        )
    return tuple(details)


async def _run_probe(config: ProbeConfig) -> ProbeReport:
    _configure_log_noise_suppression()
    threshold = _build_threshold(config)
    allocator = get_port_allocator()
    ports = allocator.allocate_port_range(config.nodes, "servers")
    servers: list[MPREGServer] = []
    tasks: list[asyncio.Task[None]] = []
    snapshots: list[ConvergenceSnapshot] = []
    converged_at: Seconds | None = None

    try:
        for node_index, port in enumerate(ports):
            servers.append(
                MPREGServer(
                    settings=_node_settings(
                        node_index=node_index,
                        port=port,
                        ports=ports,
                        config=config,
                    )
                )
            )
        print(
            f"Starting {config.nodes} servers topology={config.topology} "
            f"ports={ports[0]}..{ports[-1]}"
        )
        print(
            f"threshold={threshold.min_nodes_meeting_threshold}/{config.nodes} "
            f"nodes with >= {threshold.min_discovered_peers}/{config.nodes - 1} peers"
        )
        print(
            f"test_timeout={threshold.discovery_timeout_seconds:.1f}s "
            f"probe_duration={config.duration_seconds:.1f}s"
        )

        await _start_servers(servers=servers, tasks=tasks)
        started_at = time.monotonic()
        deadline = started_at + config.duration_seconds

        while time.monotonic() <= deadline:
            elapsed = time.monotonic() - started_at
            snapshot = _capture_snapshot(
                servers=servers,
                elapsed_seconds=elapsed,
                threshold=threshold,
            )
            snapshots.append(snapshot)
            print(
                f"[t={elapsed:6.1f}s] "
                f"func min/avg/max={snapshot.min_discovered_function_catalog}/"
                f"{snapshot.avg_discovered_function_catalog:.1f}/"
                f"{snapshot.max_discovered_function_catalog} "
                f"nodecat min/avg/max={snapshot.min_discovered_node_catalog}/"
                f"{snapshot.avg_discovered_node_catalog:.1f}/"
                f"{snapshot.max_discovered_node_catalog} "
                f"peerdir min/avg/max={snapshot.min_discovered_peer_directory}/"
                f"{snapshot.avg_discovered_peer_directory:.1f}/"
                f"{snapshot.max_discovered_peer_directory} "
                f"meet(func/nodecat/peerdir)="
                f"{snapshot.nodes_meeting_function_threshold}/"
                f"{snapshot.nodes_meeting_node_threshold}/"
                f"{snapshot.nodes_meeting_peer_directory_threshold} "
                f"active_conn min/avg/max={snapshot.min_active_connections}/"
                f"{snapshot.avg_active_connections:.1f}/"
                f"{snapshot.max_active_connections} "
                f"pending_catalog min/avg/max={snapshot.min_pending_catalog_messages}/"
                f"{snapshot.avg_pending_catalog_messages:.1f}/"
                f"{snapshot.max_pending_catalog_messages}"
            )
            if (
                snapshot.nodes_meeting_function_threshold
                >= threshold.min_nodes_meeting_threshold
                and converged_at is None
            ):
                converged_at = elapsed
                print(f"converged_at={converged_at:.1f}s")
            await asyncio.sleep(config.sample_interval_seconds)

        final_snapshot = snapshots[-1]
        details = _node_details(servers)
        result = ProbeResult(
            converged=converged_at is not None
            and converged_at <= threshold.discovery_timeout_seconds,
            converged_at_seconds=converged_at,
            threshold=threshold,
            final_snapshot=final_snapshot,
            snapshot_count=len(snapshots),
            nodes=details,
        )
        return ProbeReport(
            generated_at_unix=time.time(),
            config=config,
            result=result,
            snapshots=tuple(snapshots),
        )
    finally:
        print("Shutting down servers...")
        await asyncio.gather(
            *(server.shutdown_async() for server in servers),
            return_exceptions=True,
        )
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for port in ports:
            allocator.release_port(port)
        print("Cleanup complete.")


def _parse_args() -> tuple[ProbeConfig, Path | None]:
    parser = argparse.ArgumentParser(
        description="Topology-aware auto-discovery probe with structured convergence evidence"
    )
    parser.add_argument("--nodes", type=int, default=20)
    parser.add_argument(
        "--topology",
        type=str,
        default="STAR_HUB",
        choices=("LINEAR_CHAIN", "STAR_HUB", "RING", "MULTI_HUB"),
    )
    parser.add_argument("--duration-seconds", type=float, default=120.0)
    parser.add_argument("--sample-interval-seconds", type=float, default=5.0)
    parser.add_argument("--gossip-interval", type=float, default=1.0)
    parser.add_argument("--log-level", type=str, default="ERROR")
    parser.add_argument(
        "--concurrency-factor",
        type=float,
        default=2.0,
        help="Match test timeout scaling (2.0 under xdist)",
    )
    parser.add_argument("--peer-dial-diagnostics", action="store_true")
    parser.add_argument("--output-json", type=str, default="")
    args = parser.parse_args()

    config = ProbeConfig(
        nodes=max(2, int(args.nodes)),
        topology=str(args.topology),
        duration_seconds=max(5.0, float(args.duration_seconds)),
        sample_interval_seconds=max(0.2, float(args.sample_interval_seconds)),
        gossip_interval_seconds=max(0.1, float(args.gossip_interval)),
        log_level=str(args.log_level),
        concurrency_factor=max(0.1, float(args.concurrency_factor)),
        peer_dial_diagnostics=bool(args.peer_dial_diagnostics),
    )
    output_path = (
        Path(args.output_json).resolve() if str(args.output_json).strip() else None
    )
    return config, output_path


def main() -> int:
    config, output_path = _parse_args()
    report = asyncio.run(_run_probe(config))
    payload = asdict(report)

    print(
        f"probe_result converged={report.result.converged} "
        f"converged_at={report.result.converged_at_seconds} "
        f"snapshots={report.result.snapshot_count}"
    )
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        print(f"wrote_report={output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
