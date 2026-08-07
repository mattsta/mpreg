#!/usr/bin/env python3
"""Summarize structured auto-discovery trace reports."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

type Seconds = float


@dataclass(frozen=True, slots=True)
class ThresholdConfig:
    min_discovered_peers: int
    min_nodes_meeting_threshold: int


@dataclass(frozen=True, slots=True)
class SnapshotPoint:
    elapsed_seconds: Seconds
    nodes_meeting_function_threshold: int
    nodes_meeting_node_threshold: int
    nodes_meeting_peer_directory_threshold: int
    min_discovered_function_catalog: int
    avg_discovered_function_catalog: float
    min_discovered_node_catalog: int
    avg_discovered_node_catalog: float
    min_discovered_peer_directory: int
    avg_discovered_peer_directory: float
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
class ThresholdHitSummary:
    first_function_seconds: Seconds | None
    first_node_seconds: Seconds | None
    first_peer_directory_seconds: Seconds | None


def _as_snapshot(payload: dict[str, object]) -> SnapshotPoint:
    return SnapshotPoint(
        elapsed_seconds=float(payload["elapsed_seconds"]),
        nodes_meeting_function_threshold=int(
            payload["nodes_meeting_function_threshold"]
        ),
        nodes_meeting_node_threshold=int(payload["nodes_meeting_node_threshold"]),
        nodes_meeting_peer_directory_threshold=int(
            payload["nodes_meeting_peer_directory_threshold"]
        ),
        min_discovered_function_catalog=int(payload["min_discovered_function_catalog"]),
        avg_discovered_function_catalog=float(
            payload["avg_discovered_function_catalog"]
        ),
        min_discovered_node_catalog=int(payload["min_discovered_node_catalog"]),
        avg_discovered_node_catalog=float(payload["avg_discovered_node_catalog"]),
        min_discovered_peer_directory=int(payload["min_discovered_peer_directory"]),
        avg_discovered_peer_directory=float(payload["avg_discovered_peer_directory"]),
        min_pending_messages=int(payload.get("min_pending_messages", 0)),
        avg_pending_messages=float(payload.get("avg_pending_messages", 0.0)),
        max_pending_messages=int(payload.get("max_pending_messages", 0)),
        min_pending_catalog_messages=int(
            payload.get("min_pending_catalog_messages", 0)
        ),
        avg_pending_catalog_messages=float(
            payload.get("avg_pending_catalog_messages", 0.0)
        ),
        max_pending_catalog_messages=int(
            payload.get("max_pending_catalog_messages", 0)
        ),
        min_pending_catalog_unique_updates=int(
            payload.get("min_pending_catalog_unique_updates", 0)
        ),
        avg_pending_catalog_unique_updates=float(
            payload.get("avg_pending_catalog_unique_updates", 0.0)
        ),
        max_pending_catalog_unique_updates=int(
            payload.get("max_pending_catalog_unique_updates", 0)
        ),
        min_pending_catalog_duplicate_updates=int(
            payload.get("min_pending_catalog_duplicate_updates", 0)
        ),
        avg_pending_catalog_duplicate_updates=float(
            payload.get("avg_pending_catalog_duplicate_updates", 0.0)
        ),
        max_pending_catalog_duplicate_updates=int(
            payload.get("max_pending_catalog_duplicate_updates", 0)
        ),
        min_recent_messages=int(payload.get("min_recent_messages", 0)),
        avg_recent_messages=float(payload.get("avg_recent_messages", 0.0)),
        max_recent_messages=int(payload.get("max_recent_messages", 0)),
        min_gossip_interval=float(payload.get("min_gossip_interval", 0.0)),
        avg_gossip_interval=float(payload.get("avg_gossip_interval", 0.0)),
        max_gossip_interval=float(payload.get("max_gossip_interval", 0.0)),
        min_gossip_fanout=int(payload.get("min_gossip_fanout", 0)),
        avg_gossip_fanout=float(payload.get("avg_gossip_fanout", 0.0)),
        max_gossip_fanout=int(payload.get("max_gossip_fanout", 0)),
        gossip_hop_limit=int(payload.get("gossip_hop_limit", 0)),
        min_reachable_within_hop_limit=int(
            payload.get("min_reachable_within_hop_limit", 0)
        ),
        avg_reachable_within_hop_limit=float(
            payload.get("avg_reachable_within_hop_limit", 0.0)
        ),
        max_reachable_within_hop_limit=int(
            payload.get("max_reachable_within_hop_limit", 0)
        ),
        min_reachable_total=int(payload.get("min_reachable_total", 0)),
        avg_reachable_total=float(payload.get("avg_reachable_total", 0.0)),
        max_reachable_total=int(payload.get("max_reachable_total", 0)),
    )


def _first_threshold_hit(
    snapshots: list[SnapshotPoint],
    threshold: int,
    *,
    selector: str,
) -> Seconds | None:
    for snapshot in snapshots:
        if selector == "function":
            value = snapshot.nodes_meeting_function_threshold
        elif selector == "node":
            value = snapshot.nodes_meeting_node_threshold
        else:
            value = snapshot.nodes_meeting_peer_directory_threshold
        if value >= threshold:
            return snapshot.elapsed_seconds
    return None


def _summarize_threshold_hits(
    snapshots: list[SnapshotPoint], threshold: ThresholdConfig
) -> ThresholdHitSummary:
    return ThresholdHitSummary(
        first_function_seconds=_first_threshold_hit(
            snapshots, threshold.min_nodes_meeting_threshold, selector="function"
        ),
        first_node_seconds=_first_threshold_hit(
            snapshots, threshold.min_nodes_meeting_threshold, selector="node"
        ),
        first_peer_directory_seconds=_first_threshold_hit(
            snapshots, threshold.min_nodes_meeting_threshold, selector="peer"
        ),
    )


def _format_optional_seconds(value: Seconds | None) -> str:
    if value is None:
        return "never"
    return f"{value:.1f}s"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize auto-discovery structured trace JSON"
    )
    parser.add_argument("--report", required=True, help="Path to trace JSON report")
    args = parser.parse_args()

    report_path = Path(args.report).resolve()
    if not report_path.exists():
        print(f"Missing report: {report_path}")
        return 2

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    threshold_payload = payload["config"]["threshold"]
    threshold = ThresholdConfig(
        min_discovered_peers=int(threshold_payload["min_discovered_peers"]),
        min_nodes_meeting_threshold=int(
            threshold_payload["min_nodes_meeting_threshold"]
        ),
    )
    snapshots = [
        _as_snapshot(item)
        for item in payload.get("snapshots", [])
        if isinstance(item, dict)
    ]
    if not snapshots:
        print("No snapshots in report")
        return 2

    threshold_hits = _summarize_threshold_hits(snapshots, threshold)
    final = snapshots[-1]
    result = payload.get("result", {})

    print("Auto-discovery trace summary")
    print(f"report={report_path}")
    print(
        "threshold="
        f"{threshold.min_nodes_meeting_threshold} nodes with "
        f"{threshold.min_discovered_peers}+ peers"
    )
    print(
        "converged="
        f"{bool(result.get('converged', False))} "
        f"at={result.get('converged_at_seconds')}"
    )
    print(
        "first_hit_seconds="
        f"function:{_format_optional_seconds(threshold_hits.first_function_seconds)} "
        f"node:{_format_optional_seconds(threshold_hits.first_node_seconds)} "
        f"peerdir:{_format_optional_seconds(threshold_hits.first_peer_directory_seconds)}"
    )
    print(
        "final_nodes_meeting="
        f"function:{final.nodes_meeting_function_threshold} "
        f"node:{final.nodes_meeting_node_threshold} "
        f"peerdir:{final.nodes_meeting_peer_directory_threshold}"
    )
    print(
        "final_min_avg_discovered="
        f"function:{final.min_discovered_function_catalog}/{final.avg_discovered_function_catalog:.1f} "
        f"node:{final.min_discovered_node_catalog}/{final.avg_discovered_node_catalog:.1f} "
        f"peerdir:{final.min_discovered_peer_directory}/{final.avg_discovered_peer_directory:.1f}"
    )
    max_pending_snapshot = max(snapshots, key=lambda item: item.max_pending_messages)
    max_recent_snapshot = max(snapshots, key=lambda item: item.max_recent_messages)
    print(
        "final_pending_recent="
        f"pending:{final.min_pending_messages}/{final.avg_pending_messages:.1f}/{final.max_pending_messages} "
        f"recent:{final.min_recent_messages}/{final.avg_recent_messages:.1f}/{final.max_recent_messages}"
    )
    print(
        "final_pending_catalog="
        f"messages:{final.min_pending_catalog_messages}/{final.avg_pending_catalog_messages:.1f}/{final.max_pending_catalog_messages} "
        f"unique:{final.min_pending_catalog_unique_updates}/{final.avg_pending_catalog_unique_updates:.1f}/{final.max_pending_catalog_unique_updates} "
        f"dup:{final.min_pending_catalog_duplicate_updates}/{final.avg_pending_catalog_duplicate_updates:.1f}/{final.max_pending_catalog_duplicate_updates}"
    )
    max_pending_catalog_snapshot = max(
        snapshots, key=lambda item: item.max_pending_catalog_messages
    )
    print(
        "peak_pending_recent="
        f"pending:{max_pending_snapshot.max_pending_messages}@{max_pending_snapshot.elapsed_seconds:.1f}s "
        f"recent:{max_recent_snapshot.max_recent_messages}@{max_recent_snapshot.elapsed_seconds:.1f}s"
    )
    print(
        "peak_pending_catalog="
        f"messages:{max_pending_catalog_snapshot.max_pending_catalog_messages}"
        f"@{max_pending_catalog_snapshot.elapsed_seconds:.1f}s "
        f"dup:{max_pending_catalog_snapshot.max_pending_catalog_duplicate_updates}"
    )
    print(
        "final_gossip="
        f"interval:{final.min_gossip_interval:.2f}/{final.avg_gossip_interval:.2f}/{final.max_gossip_interval:.2f} "
        f"fanout:{final.min_gossip_fanout}/{final.avg_gossip_fanout:.1f}/{final.max_gossip_fanout}"
    )
    if final.gossip_hop_limit > 0:
        print(
            "final_reachability="
            f"within_h{final.gossip_hop_limit}:"
            f"{final.min_reachable_within_hop_limit}/"
            f"{final.avg_reachable_within_hop_limit:.1f}/"
            f"{final.max_reachable_within_hop_limit} "
            f"total:{final.min_reachable_total}/"
            f"{final.avg_reachable_total:.1f}/{final.max_reachable_total}"
        )
    distribution = payload.get("distribution", {})
    least_frequent_peers = distribution.get("least_frequent_peers", [])
    if isinstance(least_frequent_peers, list) and least_frequent_peers:
        least_summary = ", ".join(
            f"{item.get('peer_id')}:{item.get('seen_by_nodes')}"
            for item in least_frequent_peers[:5]
            if isinstance(item, dict)
        )
        print(f"least_frequent_peers={least_summary}")
    refresh_task_states = distribution.get("refresh_task_states", [])
    stopped_refresh_nodes: list[str] = []
    if isinstance(refresh_task_states, list):
        for item in refresh_task_states:
            if not isinstance(item, dict):
                continue
            catalog_running = bool(item.get("catalog_refresh_running", False))
            node_running = bool(item.get("node_refresh_running", False))
            if not catalog_running or not node_running:
                node_id = item.get("node_id")
                if isinstance(node_id, str):
                    stopped_refresh_nodes.append(node_id)
    print(f"refresh_task_health=stopped_nodes:{len(stopped_refresh_nodes)}")
    if stopped_refresh_nodes:
        preview = ", ".join(stopped_refresh_nodes[:10])
        print(f"refresh_task_stopped_preview={preview}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
