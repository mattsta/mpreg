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


@dataclass(frozen=True, slots=True)
class ConvergenceThreshold:
    min_discovered_peers: int
    min_nodes_meeting_threshold: int


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
    min_recent_messages: int
    avg_recent_messages: float
    max_recent_messages: int
    min_function_ttl_seconds: float
    avg_function_ttl_seconds: float
    max_function_ttl_seconds: float
    min_function_age_seconds: float
    avg_function_age_seconds: float
    max_function_age_seconds: float
    min_function_entry_count: int
    avg_function_entry_count: float
    max_function_entry_count: int
    min_gossip_interval: float
    avg_gossip_interval: float
    max_gossip_interval: float
    min_gossip_fanout: int
    avg_gossip_fanout: float
    max_gossip_fanout: int
    gossip_hop_limit: int
    min_reachable_within_hop_limit: int
    avg_reachable_within_hop_limit: float
    max_reachable_within_hop_limit: int
    min_reachable_total: int
    avg_reachable_total: float
    max_reachable_total: int


@dataclass(frozen=True, slots=True)
class NodeDiscoveryCount:
    node_id: str
    discovered_peers: int


@dataclass(frozen=True, slots=True)
class PeerFrequency:
    peer_id: str
    seen_by_nodes: int


@dataclass(frozen=True, slots=True)
class LocalFunctionCount:
    node_id: str
    count: int


@dataclass(frozen=True, slots=True)
class RefreshTaskState:
    node_id: str
    catalog_refresh_running: bool
    node_refresh_running: bool


@dataclass(frozen=True, slots=True)
class NodeDepartedCount:
    node_id: str
    departed_peer_count: int


@dataclass(frozen=True, slots=True)
class DepartedPeerMark:
    peer_id: str
    marked_by_nodes: int


@dataclass(frozen=True, slots=True)
class MissingPeersForNode:
    node_id: str
    missing_peers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FinalNodeMetrics:
    node_id: str
    discovered_function_catalog: int
    discovered_node_catalog: int
    discovered_peer_directory: int
    active_connections: int
    target_hint: int
    desired_connections: int
    connected_ratio: float
    pending_messages: int
    pending_catalog_messages: int


@dataclass(frozen=True, slots=True)
class DistributionSummary:
    lowest_nodes: tuple[NodeDiscoveryCount, ...]
    highest_nodes: tuple[NodeDiscoveryCount, ...]
    most_frequent_peers: tuple[PeerFrequency, ...]
    least_frequent_peers: tuple[PeerFrequency, ...]
    local_function_counts: tuple[LocalFunctionCount, ...]
    refresh_task_states: tuple[RefreshTaskState, ...]
    node_departed_counts: tuple[NodeDepartedCount, ...]
    most_marked_departed_peers: tuple[DepartedPeerMark, ...]
    missing_peers_for_lowest_nodes: tuple[MissingPeersForNode, ...]
    final_node_metrics: tuple[FinalNodeMetrics, ...]
    zero_local_function_nodes: tuple[str, ...]
    seed_dial_lines: tuple[str, ...]


def _build_settings(
    *,
    node_id: int,
    port: int,
    ports: list[int],
    gossip_interval: float,
    log_level: str,
    peer_dial_diagnostics: bool,
) -> MPREGSettings:
    num_hubs = min(3, len(ports))
    if node_id < num_hubs:
        connect_to = f"ws://127.0.0.1:{ports[node_id - 1]}" if node_id > 0 else None
    else:
        hub_port = ports[node_id % num_hubs]
        connect_to = f"ws://127.0.0.1:{hub_port}"

    return MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=f"AutoDiscover-Node-{node_id}",
        cluster_id=f"debug-multihub-{len(ports)}",
        resources={f"node-{node_id}", "debug-resource"},
        peers=None,
        connect=connect_to,
        advertised_urls=None,
        gossip_interval=gossip_interval,
        log_level=log_level,
        log_debug_scopes=("peer_dial",) if peer_dial_diagnostics else tuple(),
        monitoring_enabled=False,
        fabric_routing_enabled=False,
    )


def _configure_debug_logging() -> None:
    # Reduce websocket handshake-noise so convergence metrics stay visible.
    logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
    logging.getLogger("websockets.client").setLevel(logging.CRITICAL)


def _build_thresholds(node_count: int) -> ConvergenceThreshold:
    expected_peers = max(node_count - 1, 1)
    min_discovered_peers = int(expected_peers * 0.8)
    min_nodes_meeting_threshold = int(node_count * 0.8)
    return ConvergenceThreshold(
        min_discovered_peers=min_discovered_peers,
        min_nodes_meeting_threshold=min_nodes_meeting_threshold,
    )


def _capture_snapshot(
    *,
    servers: list[MPREGServer],
    elapsed_seconds: float,
    threshold: ConvergenceThreshold,
) -> ConvergenceSnapshot:
    function_catalog_discovered_counts: list[int] = []
    node_catalog_discovered_counts: list[int] = []
    peer_directory_discovered_counts: list[int] = []
    active_connections: list[int] = []
    target_hints: list[int] = []
    desired_connections: list[int] = []
    connected_ratios: list[float] = []
    pending_messages: list[int] = []
    pending_catalog_messages: list[int] = []
    pending_catalog_unique_updates: list[int] = []
    pending_catalog_duplicate_updates: list[int] = []
    recent_messages: list[int] = []
    function_ttl_seconds_values: list[float] = []
    function_age_seconds_values: list[float] = []
    function_entry_counts: list[int] = []
    gossip_intervals: list[float] = []
    gossip_fanouts: list[int] = []
    reachable_within_limit_counts: list[int] = []
    reachable_total_counts: list[int] = []
    nodes_meeting_function_threshold = 0
    nodes_meeting_node_threshold = 0
    nodes_meeting_peer_directory_threshold = 0
    now = time.time()
    hop_limit = 5

    node_ids = [server.cluster.local_url for server in servers]
    adjacency: dict[str, set[str]] = {node_id: set() for node_id in node_ids}
    for server in servers:
        source_id = server.cluster.local_url
        for peer_id, connection in server._get_all_peer_connections().items():
            if not connection.is_connected:
                continue
            if peer_id not in adjacency:
                continue
            adjacency[source_id].add(peer_id)
            adjacency[peer_id].add(source_id)

    def _reachable_count(start: str, max_hops: int | None) -> int:
        visited = {start}
        frontier = {start}
        hops = 0
        while frontier:
            if max_hops is not None and hops >= max_hops:
                break
            next_frontier: set[str] = set()
            for node_id in frontier:
                for peer_id in adjacency.get(node_id, set()):
                    if peer_id in visited:
                        continue
                    visited.add(peer_id)
                    next_frontier.add(peer_id)
            if not next_frontier:
                break
            frontier = next_frontier
            hops += 1
        return max(len(visited) - 1, 0)

    for server in servers:
        function_catalog_discovered = max(len(server.cluster.servers) - 1, 0)
        function_catalog_discovered_counts.append(function_catalog_discovered)
        if function_catalog_discovered >= threshold.min_discovered_peers:
            nodes_meeting_function_threshold += 1

        function_entries = tuple()
        if server._fabric_control_plane is not None:
            function_entries = (
                server._fabric_control_plane.index.catalog.functions.entries(now=now)
            )
        function_entry_counts.append(len(function_entries))
        if function_entries:
            ttl_values = [float(entry.ttl_seconds) for entry in function_entries]
            function_ttl_seconds_values.extend(ttl_values)
            ages = [
                max(0.0, now - float(entry.advertised_at)) for entry in function_entries
            ]
            function_age_seconds_values.extend(ages)

        node_catalog_nodes: set[str] = set()
        if server._fabric_control_plane is not None:
            entries = server._fabric_control_plane.index.catalog.nodes.entries(now=now)
            node_catalog_nodes = {entry.node_id for entry in entries}
        node_catalog_discovered = len(node_catalog_nodes - {server.cluster.local_url})
        node_catalog_discovered_counts.append(node_catalog_discovered)
        if node_catalog_discovered >= threshold.min_discovered_peers:
            nodes_meeting_node_threshold += 1

        peer_directory_nodes: set[str] = set()
        if server._peer_directory is not None:
            peer_directory_nodes = {
                node.node_id for node in server._peer_directory.nodes()
            }
        peer_directory_discovered = len(
            peer_directory_nodes - {server.cluster.local_url}
        )
        peer_directory_discovered_counts.append(peer_directory_discovered)
        if peer_directory_discovered >= threshold.min_discovered_peers:
            nodes_meeting_peer_directory_threshold += 1

        connection_count = sum(
            1
            for connection in server._get_all_peer_connections().values()
            if connection.is_connected
        )
        active_connections.append(connection_count)
        target_hint = max(server._peer_dial_target_count_hint(), 1)
        target_hints.append(target_hint)
        connected_ratio = connection_count / max(target_hint, 1)
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
            recent_messages.append(0)
            gossip_intervals.append(0.0)
            gossip_fanouts.append(0)
        else:
            pending_count = len(gossip.pending_messages)
            pending_messages.append(pending_count)
            catalog_update_count = 0
            unique_catalog_updates: set[str] = set()
            for message in gossip.pending_messages:
                if message.message_type is not GossipMessageType.CATALOG_UPDATE:
                    continue
                catalog_update_count += 1
                payload = message.payload
                if isinstance(payload, dict):
                    update_id = payload.get("update_id")
                    if isinstance(update_id, str) and update_id:
                        unique_catalog_updates.add(update_id)
                        continue
                unique_catalog_updates.add(message.message_id)
            pending_catalog_messages.append(catalog_update_count)
            unique_count = len(unique_catalog_updates)
            pending_catalog_unique_updates.append(unique_count)
            pending_catalog_duplicate_updates.append(
                max(catalog_update_count - unique_count, 0)
            )
            recent_messages.append(len(gossip.recent_messages))
            gossip_intervals.append(float(gossip.scheduler.adaptive_interval))
            gossip_fanouts.append(int(gossip.scheduler.fanout))

        node_id = server.cluster.local_url
        reachable_within_limit_counts.append(_reachable_count(node_id, hop_limit))
        reachable_total_counts.append(_reachable_count(node_id, None))

    ttl_values = function_ttl_seconds_values or [0.0]
    age_values = function_age_seconds_values or [0.0]
    entry_counts = function_entry_counts or [0]

    return ConvergenceSnapshot(
        elapsed_seconds=elapsed_seconds,
        min_discovered_function_catalog=min(function_catalog_discovered_counts),
        avg_discovered_function_catalog=(
            sum(function_catalog_discovered_counts)
            / max(len(function_catalog_discovered_counts), 1)
        ),
        max_discovered_function_catalog=max(function_catalog_discovered_counts),
        min_discovered_node_catalog=min(node_catalog_discovered_counts),
        avg_discovered_node_catalog=(
            sum(node_catalog_discovered_counts)
            / max(len(node_catalog_discovered_counts), 1)
        ),
        max_discovered_node_catalog=max(node_catalog_discovered_counts),
        min_discovered_peer_directory=min(peer_directory_discovered_counts),
        avg_discovered_peer_directory=(
            sum(peer_directory_discovered_counts)
            / max(len(peer_directory_discovered_counts), 1)
        ),
        max_discovered_peer_directory=max(peer_directory_discovered_counts),
        nodes_meeting_function_threshold=nodes_meeting_function_threshold,
        nodes_meeting_node_threshold=nodes_meeting_node_threshold,
        nodes_meeting_peer_directory_threshold=nodes_meeting_peer_directory_threshold,
        min_active_connections=min(active_connections),
        avg_active_connections=sum(active_connections)
        / max(len(active_connections), 1),
        max_active_connections=max(active_connections),
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
        avg_pending_catalog_messages=(
            sum(pending_catalog_messages) / max(len(pending_catalog_messages), 1)
        ),
        max_pending_catalog_messages=max(pending_catalog_messages),
        min_pending_catalog_unique_updates=min(pending_catalog_unique_updates),
        avg_pending_catalog_unique_updates=(
            sum(pending_catalog_unique_updates)
            / max(len(pending_catalog_unique_updates), 1)
        ),
        max_pending_catalog_unique_updates=max(pending_catalog_unique_updates),
        min_pending_catalog_duplicate_updates=min(pending_catalog_duplicate_updates),
        avg_pending_catalog_duplicate_updates=(
            sum(pending_catalog_duplicate_updates)
            / max(len(pending_catalog_duplicate_updates), 1)
        ),
        max_pending_catalog_duplicate_updates=max(pending_catalog_duplicate_updates),
        min_recent_messages=min(recent_messages),
        avg_recent_messages=sum(recent_messages) / max(len(recent_messages), 1),
        max_recent_messages=max(recent_messages),
        min_function_ttl_seconds=min(ttl_values),
        avg_function_ttl_seconds=sum(ttl_values) / max(len(ttl_values), 1),
        max_function_ttl_seconds=max(ttl_values),
        min_function_age_seconds=min(age_values),
        avg_function_age_seconds=sum(age_values) / max(len(age_values), 1),
        max_function_age_seconds=max(age_values),
        min_function_entry_count=min(entry_counts),
        avg_function_entry_count=sum(entry_counts) / max(len(entry_counts), 1),
        max_function_entry_count=max(entry_counts),
        min_gossip_interval=min(gossip_intervals),
        avg_gossip_interval=sum(gossip_intervals) / max(len(gossip_intervals), 1),
        max_gossip_interval=max(gossip_intervals),
        min_gossip_fanout=min(gossip_fanouts),
        avg_gossip_fanout=sum(gossip_fanouts) / max(len(gossip_fanouts), 1),
        max_gossip_fanout=max(gossip_fanouts),
        gossip_hop_limit=hop_limit,
        min_reachable_within_hop_limit=min(reachable_within_limit_counts),
        avg_reachable_within_hop_limit=(
            sum(reachable_within_limit_counts)
            / max(len(reachable_within_limit_counts), 1)
        ),
        max_reachable_within_hop_limit=max(reachable_within_limit_counts),
        min_reachable_total=min(reachable_total_counts),
        avg_reachable_total=sum(reachable_total_counts)
        / max(len(reachable_total_counts), 1),
        max_reachable_total=max(reachable_total_counts),
    )


def _capture_discovery_distribution(
    servers: list[MPREGServer],
) -> tuple[list[NodeDiscoveryCount], list[PeerFrequency]]:
    per_node_counts: list[NodeDiscoveryCount] = []
    peer_frequency: dict[str, int] = {}

    for server in servers:
        peer_nodes: set[str] = set()
        if server._peer_directory is not None:
            peer_nodes = {
                node.node_id
                for node in server._peer_directory.nodes()
                if node.node_id != server.cluster.local_url
            }
        per_node_counts.append(
            NodeDiscoveryCount(
                node_id=server.cluster.local_url,
                discovered_peers=len(peer_nodes),
            )
        )
        for peer_id in peer_nodes:
            peer_frequency[peer_id] = peer_frequency.get(peer_id, 0) + 1

    ordered_node_counts = sorted(
        per_node_counts,
        key=lambda entry: (entry.discovered_peers, entry.node_id),
    )
    ordered_peer_frequency = sorted(
        (
            PeerFrequency(peer_id=peer_id, seen_by_nodes=count)
            for peer_id, count in peer_frequency.items()
        ),
        key=lambda entry: (-entry.seen_by_nodes, entry.peer_id),
    )
    return ordered_node_counts, ordered_peer_frequency


def _print_distribution_summary(
    servers: list[MPREGServer], seed_targets: list[str | None]
) -> DistributionSummary:
    node_counts, peer_frequency = _capture_discovery_distribution(servers)
    local_function_counts: list[LocalFunctionCount] = []
    refresh_task_states: list[RefreshTaskState] = []
    node_departed_counts: list[NodeDepartedCount] = []
    departed_peer_frequency: dict[str, int] = {}
    seed_dial_lines: list[str] = []
    now = time.time()
    for server in servers:
        local_function_counts.append(
            LocalFunctionCount(
                node_id=server.cluster.local_url,
                count=len(server.registry.specs()),
            )
        )
        catalog_refresh_task = server._fabric_catalog_refresh_task
        node_refresh_task = server._fabric_node_refresh_task
        refresh_task_states.append(
            RefreshTaskState(
                node_id=server.cluster.local_url,
                catalog_refresh_running=(
                    catalog_refresh_task is not None and not catalog_refresh_task.done()
                ),
                node_refresh_running=(
                    node_refresh_task is not None and not node_refresh_task.done()
                ),
            )
        )
        active_departed_peers = [
            peer_id
            for peer_id, record in server._departed_peers.items()
            if not record.is_expired(now)
        ]
        node_departed_counts.append(
            NodeDepartedCount(
                node_id=server.cluster.local_url,
                departed_peer_count=len(active_departed_peers),
            )
        )
        for peer_id in active_departed_peers:
            departed_peer_frequency[peer_id] = (
                departed_peer_frequency.get(peer_id, 0) + 1
            )
    if not node_counts:
        return DistributionSummary(
            lowest_nodes=tuple(),
            highest_nodes=tuple(),
            most_frequent_peers=tuple(),
            least_frequent_peers=tuple(),
            local_function_counts=tuple(local_function_counts),
            refresh_task_states=tuple(refresh_task_states),
            node_departed_counts=tuple(node_departed_counts),
            most_marked_departed_peers=tuple(),
            missing_peers_for_lowest_nodes=tuple(),
            final_node_metrics=tuple(),
            zero_local_function_nodes=tuple(),
            seed_dial_lines=tuple(),
        )
    summary_size = min(10, len(node_counts))
    lowest_nodes = node_counts[:summary_size]
    highest_nodes = list(reversed(node_counts[-summary_size:]))
    node_ids = {server.cluster.local_url for server in servers}
    peer_ids_by_node: dict[str, set[str]] = {}
    for server in servers:
        peer_nodes: set[str] = set()
        if server._peer_directory is not None:
            peer_nodes = {
                node.node_id
                for node in server._peer_directory.nodes()
                if node.node_id != server.cluster.local_url
            }
        peer_ids_by_node[server.cluster.local_url] = peer_nodes
    print("Discovery distribution:")
    print("  Lowest node discovery counts:")
    for entry in lowest_nodes:
        print(f"    {entry.node_id} -> {entry.discovered_peers}")
    print("  Highest node discovery counts:")
    for entry in highest_nodes:
        print(f"    {entry.node_id} -> {entry.discovered_peers}")

    if peer_frequency:
        top_peers = peer_frequency[:summary_size]
        low_peers = sorted(
            peer_frequency, key=lambda entry: (entry.seen_by_nodes, entry.peer_id)
        )[:summary_size]
        departed_top = sorted(
            (
                DepartedPeerMark(peer_id=peer_id, marked_by_nodes=count)
                for peer_id, count in departed_peer_frequency.items()
            ),
            key=lambda entry: (-entry.marked_by_nodes, entry.peer_id),
        )
        print("  Most frequently discovered peers:")
        for entry in top_peers:
            print(f"    {entry.peer_id} seen_by={entry.seen_by_nodes}")
        print("  Least frequently discovered peers:")
        for entry in low_peers:
            print(f"    {entry.peer_id} seen_by={entry.seen_by_nodes}")
        if departed_top:
            print("  Most-marked departed peers:")
            for entry in departed_top[:summary_size]:
                print(f"    {entry.peer_id} marked_by={entry.marked_by_nodes}")
    else:
        low_peers = []
        departed_top = []

    if local_function_counts:
        function_counts = [entry.count for entry in local_function_counts]
        print(
            "  Local function spec counts: "
            f"min/avg/max={min(function_counts)}/"
            f"{(sum(function_counts) / len(function_counts)):.1f}/"
            f"{max(function_counts)}"
        )
        zero_spec_nodes = [
            entry.node_id for entry in local_function_counts if entry.count == 0
        ]
        if zero_spec_nodes:
            print("  Nodes with zero local function specs:")
            for node_id in zero_spec_nodes[:summary_size]:
                print(f"    {node_id}")
    else:
        zero_spec_nodes = []

    if seed_targets:
        node_index_by_url = {
            server.cluster.local_url: index for index, server in enumerate(servers)
        }
        now = time.time()
        print("  Seed dial state for lowest-discovery nodes:")
        for entry in lowest_nodes:
            node_index = node_index_by_url.get(entry.node_id)
            if node_index is None:
                continue
            seed_peer = seed_targets[node_index]
            if seed_peer is None:
                line = f"{entry.node_id} seed=None"
                print(f"    {line}")
                seed_dial_lines.append(line)
                continue
            server = servers[node_index]
            state = server._peer_dial_state.get(seed_peer)
            connection = server._get_all_peer_connections().get(seed_peer)
            is_connected = connection.is_connected if connection is not None else False
            if state is None:
                line = (
                    f"{entry.node_id} seed={seed_peer} "
                    f"state=missing connected={is_connected}"
                )
                print(f"    {line}")
                seed_dial_lines.append(line)
                continue
            line = (
                f"{entry.node_id} seed={seed_peer} connected={is_connected} "
                f"failures={state.consecutive_failures} "
                f"last_success_at={state.last_success_at} "
                f"next_attempt_in={max(state.next_attempt_at - now, 0.0):.2f}s"
            )
            print(f"    {line}")
            seed_dial_lines.append(line)

    missing_peers_for_lowest_nodes: list[MissingPeersForNode] = []
    print("  Missing peers for lowest-discovery nodes:")
    for entry in lowest_nodes:
        known_peers = peer_ids_by_node.get(entry.node_id, set())
        missing = sorted(node_ids - {entry.node_id} - known_peers)
        missing_peers_for_lowest_nodes.append(
            MissingPeersForNode(
                node_id=entry.node_id,
                missing_peers=tuple(missing),
            )
        )
        preview = ", ".join(missing[:10]) if missing else "<none>"
        print(f"    {entry.node_id} missing={len(missing)} [{preview}]")

    final_node_metrics: list[FinalNodeMetrics] = []
    for server in sorted(servers, key=lambda item: item.cluster.local_url):
        node_id = server.cluster.local_url
        function_catalog_discovered = max(len(server.cluster.servers) - 1, 0)
        node_catalog_discovered = 0
        if server._fabric_control_plane is not None:
            node_entries = server._fabric_control_plane.index.catalog.nodes.entries()
            node_catalog_discovered = len(
                {entry.node_id for entry in node_entries} - {node_id}
            )
        peer_directory_discovered = len(peer_ids_by_node.get(node_id, set()))
        active_connections = sum(
            1
            for connection in server._get_all_peer_connections().values()
            if connection.is_connected
        )
        target_hint = max(server._peer_dial_target_count_hint(), 1)
        connected_ratio = active_connections / max(target_hint, 1)
        desired_connections = server._peer_target_connection_count(
            target_hint, connected_ratio
        )
        gossip = (
            server._fabric_control_plane.gossip
            if server._fabric_control_plane is not None
            else None
        )
        pending_messages = len(gossip.pending_messages) if gossip is not None else 0
        pending_catalog_messages = 0
        if gossip is not None:
            pending_catalog_messages = sum(
                1
                for message in gossip.pending_messages
                if message.message_type is GossipMessageType.CATALOG_UPDATE
            )
        final_node_metrics.append(
            FinalNodeMetrics(
                node_id=node_id,
                discovered_function_catalog=function_catalog_discovered,
                discovered_node_catalog=node_catalog_discovered,
                discovered_peer_directory=peer_directory_discovered,
                active_connections=active_connections,
                target_hint=target_hint,
                desired_connections=desired_connections,
                connected_ratio=connected_ratio,
                pending_messages=pending_messages,
                pending_catalog_messages=pending_catalog_messages,
            )
        )

    return DistributionSummary(
        lowest_nodes=tuple(lowest_nodes),
        highest_nodes=tuple(highest_nodes),
        most_frequent_peers=tuple(peer_frequency[:summary_size])
        if peer_frequency
        else tuple(),
        least_frequent_peers=tuple(low_peers),
        local_function_counts=tuple(local_function_counts),
        refresh_task_states=tuple(refresh_task_states),
        node_departed_counts=tuple(node_departed_counts),
        most_marked_departed_peers=tuple(departed_top[:summary_size]),
        missing_peers_for_lowest_nodes=tuple(missing_peers_for_lowest_nodes),
        final_node_metrics=tuple(final_node_metrics),
        zero_local_function_nodes=tuple(
            entry.node_id for entry in local_function_counts if entry.count == 0
        ),
        seed_dial_lines=tuple(seed_dial_lines),
    )


def _write_json_report(
    *,
    output_path: Path,
    node_count: int,
    duration_seconds: float,
    sample_interval_seconds: float,
    gossip_interval: float,
    threshold: ConvergenceThreshold,
    snapshots: list[ConvergenceSnapshot],
    converged_at: float | None,
    distribution: DistributionSummary,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at_unix": time.time(),
        "config": {
            "nodes": node_count,
            "duration_seconds": duration_seconds,
            "sample_interval_seconds": sample_interval_seconds,
            "gossip_interval": gossip_interval,
            "threshold": asdict(threshold),
        },
        "result": {
            "converged": converged_at is not None,
            "converged_at_seconds": converged_at,
            "sample_count": len(snapshots),
        },
        "snapshots": [asdict(snapshot) for snapshot in snapshots],
        "distribution": asdict(distribution),
    }
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )


async def _run_debug(
    *,
    node_count: NodeCount,
    duration_seconds: Seconds,
    sample_interval_seconds: Seconds,
    gossip_interval: float,
    log_level: str,
    peer_dial_diagnostics: bool,
    output_json: Path | None,
) -> int:
    _configure_debug_logging()
    allocator = get_port_allocator()
    ports = allocator.allocate_port_range(node_count, "servers")
    servers: list[MPREGServer] = []
    tasks: list[asyncio.Task[None]] = []
    seed_targets: list[str | None] = []
    threshold = _build_thresholds(node_count)
    converged_at: float | None = None
    snapshots: list[ConvergenceSnapshot] = []
    task_failures_reported: set[int] = set()

    try:
        for i, port in enumerate(ports):
            settings = _build_settings(
                node_id=i,
                port=port,
                ports=ports,
                gossip_interval=gossip_interval,
                log_level=log_level,
                peer_dial_diagnostics=peer_dial_diagnostics,
            )
            servers.append(MPREGServer(settings=settings))
            seed_targets.append(settings.connect)

        print(f"Starting {node_count} servers on ports {ports[0]}..{ports[-1]}")
        print(
            f"Convergence target: >= {threshold.min_nodes_meeting_threshold}/{node_count} "
            f"nodes with >= {threshold.min_discovered_peers}/{max(node_count - 1, 1)} peers discovered"
        )

        if node_count >= 50:
            batch_size = 5
            intra_start_delay = 0.5
            inter_batch_delay = 1.0
        elif node_count >= 20:
            batch_size = 8
            intra_start_delay = 0.3
            inter_batch_delay = 0.5
        else:
            batch_size = node_count
            intra_start_delay = 0.1
            inter_batch_delay = 0.0

        for batch_start in range(0, node_count, batch_size):
            batch_end = min(batch_start + batch_size, node_count)
            print(
                f"  Batch {(batch_start // batch_size) + 1}: nodes {batch_start}-{batch_end - 1}"
            )
            for server in servers[batch_start:batch_end]:
                tasks.append(asyncio.create_task(server.server()))
                await asyncio.sleep(intra_start_delay)
            if inter_batch_delay > 0 and batch_end < node_count:
                await asyncio.sleep(inter_batch_delay)

        started_at = time.monotonic()
        while True:
            elapsed_seconds = time.monotonic() - started_at
            snapshot = _capture_snapshot(
                servers=servers,
                elapsed_seconds=elapsed_seconds,
                threshold=threshold,
            )
            snapshots.append(snapshot)
            failed_tasks = 0
            for index, task in enumerate(tasks):
                if not task.done():
                    continue
                failed_tasks += 1
                if index in task_failures_reported:
                    continue
                task_failures_reported.add(index)
                exc = task.exception()
                if exc is None:
                    print(f"  task[{index}] exited cleanly")
                else:
                    print(f"  task[{index}] FAILED with {type(exc).__name__}: {exc}")
            print(
                f"[t={snapshot.elapsed_seconds:6.1f}s] "
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
                f"{snapshot.avg_active_connections:.1f}/{snapshot.max_active_connections} "
                f"target_hint min/avg/max={snapshot.min_target_hint}/"
                f"{snapshot.avg_target_hint:.1f}/{snapshot.max_target_hint} "
                f"desired_conn min/avg/max={snapshot.min_desired_connections}/"
                f"{snapshot.avg_desired_connections:.1f}/{snapshot.max_desired_connections} "
                f"connected_ratio min/avg/max={snapshot.min_connected_ratio:.2f}/"
                f"{snapshot.avg_connected_ratio:.2f}/{snapshot.max_connected_ratio:.2f} "
                f"pending min/avg/max={snapshot.min_pending_messages}/"
                f"{snapshot.avg_pending_messages:.1f}/{snapshot.max_pending_messages} "
                f"pending_catalog min/avg/max={snapshot.min_pending_catalog_messages}/"
                f"{snapshot.avg_pending_catalog_messages:.1f}/"
                f"{snapshot.max_pending_catalog_messages} "
                f"pending_catalog_unique min/avg/max="
                f"{snapshot.min_pending_catalog_unique_updates}/"
                f"{snapshot.avg_pending_catalog_unique_updates:.1f}/"
                f"{snapshot.max_pending_catalog_unique_updates} "
                f"pending_catalog_dup min/avg/max="
                f"{snapshot.min_pending_catalog_duplicate_updates}/"
                f"{snapshot.avg_pending_catalog_duplicate_updates:.1f}/"
                f"{snapshot.max_pending_catalog_duplicate_updates} "
                f"recent min/avg/max={snapshot.min_recent_messages}/"
                f"{snapshot.avg_recent_messages:.1f}/{snapshot.max_recent_messages} "
                f"func_ttl min/avg/max={snapshot.min_function_ttl_seconds:.1f}/"
                f"{snapshot.avg_function_ttl_seconds:.1f}/{snapshot.max_function_ttl_seconds:.1f} "
                f"func_age min/avg/max={snapshot.min_function_age_seconds:.1f}/"
                f"{snapshot.avg_function_age_seconds:.1f}/{snapshot.max_function_age_seconds:.1f} "
                f"func_entries min/avg/max={snapshot.min_function_entry_count}/"
                f"{snapshot.avg_function_entry_count:.1f}/{snapshot.max_function_entry_count} "
                f"gossip_interval min/avg/max={snapshot.min_gossip_interval:.2f}/"
                f"{snapshot.avg_gossip_interval:.2f}/{snapshot.max_gossip_interval:.2f} "
                f"gossip_fanout min/avg/max={snapshot.min_gossip_fanout}/"
                f"{snapshot.avg_gossip_fanout:.1f}/{snapshot.max_gossip_fanout} "
                f"reach<=h{snapshot.gossip_hop_limit} min/avg/max="
                f"{snapshot.min_reachable_within_hop_limit}/"
                f"{snapshot.avg_reachable_within_hop_limit:.1f}/"
                f"{snapshot.max_reachable_within_hop_limit} "
                f"reach_total min/avg/max={snapshot.min_reachable_total}/"
                f"{snapshot.avg_reachable_total:.1f}/{snapshot.max_reachable_total} "
                f"failed_tasks={failed_tasks}/{len(tasks)}"
            )
            if (
                snapshot.nodes_meeting_function_threshold
                >= threshold.min_nodes_meeting_threshold
            ):
                converged_at = elapsed_seconds
                break
            if elapsed_seconds >= duration_seconds:
                break
            await asyncio.sleep(sample_interval_seconds)

        if converged_at is not None:
            print(f"CONVERGED in {converged_at:.1f}s")
            distribution = _print_distribution_summary(servers, seed_targets)
            if output_json is not None:
                _write_json_report(
                    output_path=output_json,
                    node_count=node_count,
                    duration_seconds=duration_seconds,
                    sample_interval_seconds=sample_interval_seconds,
                    gossip_interval=gossip_interval,
                    threshold=threshold,
                    snapshots=snapshots,
                    converged_at=converged_at,
                    distribution=distribution,
                )
                print(f"Wrote JSON report: {output_json}")
            return 0

        print(f"DID NOT CONVERGE within {duration_seconds:.1f}s")
        distribution = _print_distribution_summary(servers, seed_targets)
        if output_json is not None:
            _write_json_report(
                output_path=output_json,
                node_count=node_count,
                duration_seconds=duration_seconds,
                sample_interval_seconds=sample_interval_seconds,
                gossip_interval=gossip_interval,
                threshold=threshold,
                snapshots=snapshots,
                converged_at=converged_at,
                distribution=distribution,
            )
            print(f"Wrote JSON report: {output_json}")
        return 1
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deep-dive auto-discovery debugger for multi-hub topologies"
    )
    parser.add_argument("--nodes", type=int, default=50, help="Number of nodes")
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=140.0,
        help="Maximum observation time before declaring no convergence",
    )
    parser.add_argument(
        "--sample-interval-seconds",
        type=float,
        default=5.0,
        help="Metric sampling cadence",
    )
    parser.add_argument(
        "--gossip-interval",
        type=float,
        default=1.0,
        help="Server gossip interval setting",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="ERROR",
        help="Server log level",
    )
    parser.add_argument(
        "--peer-dial-diagnostics",
        action="store_true",
        help="Enable detailed peer dial diagnostics in server logs",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="",
        help="Optional path to write structured JSON report",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    output_json = (
        Path(args.output_json).resolve() if str(args.output_json).strip() else None
    )
    return asyncio.run(
        _run_debug(
            node_count=max(2, int(args.nodes)),
            duration_seconds=max(10.0, float(args.duration_seconds)),
            sample_interval_seconds=max(0.5, float(args.sample_interval_seconds)),
            gossip_interval=max(0.1, float(args.gossip_interval)),
            log_level=str(args.log_level),
            peer_dial_diagnostics=bool(args.peer_dial_diagnostics),
            output_json=output_json,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
