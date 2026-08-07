#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

type Seconds = float


@dataclass(frozen=True, slots=True)
class Snapshot:
    elapsed_seconds: Seconds
    nodes_meeting_function_threshold: int
    nodes_meeting_node_threshold: int
    nodes_meeting_peer_directory_threshold: int
    min_discovered_function_catalog: int
    avg_discovered_function_catalog: float
    max_discovered_function_catalog: int
    min_discovered_node_catalog: int
    avg_discovered_node_catalog: float
    max_discovered_node_catalog: int
    min_discovered_peer_directory: int
    avg_discovered_peer_directory: float
    max_discovered_peer_directory: int
    min_active_connections: int
    avg_active_connections: float
    max_active_connections: int
    min_pending_catalog_messages: int
    avg_pending_catalog_messages: float
    max_pending_catalog_messages: int


def _snapshot_from_payload(payload: dict[str, object]) -> Snapshot:
    return Snapshot(
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
        max_discovered_function_catalog=int(payload["max_discovered_function_catalog"]),
        min_discovered_node_catalog=int(payload["min_discovered_node_catalog"]),
        avg_discovered_node_catalog=float(payload["avg_discovered_node_catalog"]),
        max_discovered_node_catalog=int(payload["max_discovered_node_catalog"]),
        min_discovered_peer_directory=int(payload["min_discovered_peer_directory"]),
        avg_discovered_peer_directory=float(payload["avg_discovered_peer_directory"]),
        max_discovered_peer_directory=int(payload["max_discovered_peer_directory"]),
        min_active_connections=int(payload["min_active_connections"]),
        avg_active_connections=float(payload["avg_active_connections"]),
        max_active_connections=int(payload["max_active_connections"]),
        min_pending_catalog_messages=int(payload["min_pending_catalog_messages"]),
        avg_pending_catalog_messages=float(payload["avg_pending_catalog_messages"]),
        max_pending_catalog_messages=int(payload["max_pending_catalog_messages"]),
    )


def _nearest_before_timeout(
    snapshots: list[Snapshot], timeout_seconds: Seconds
) -> Snapshot | None:
    eligible = [
        snapshot
        for snapshot in snapshots
        if snapshot.elapsed_seconds <= timeout_seconds
    ]
    if not eligible:
        return None
    return eligible[-1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize topology probe convergence around test timeout"
    )
    parser.add_argument("--report", required=True, help="Path to probe JSON report")
    args = parser.parse_args()

    report_path = Path(args.report).resolve()
    if not report_path.exists():
        print(f"Missing report: {report_path}")
        return 2

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    threshold_payload = payload["result"]["threshold"]
    timeout_seconds = float(threshold_payload["discovery_timeout_seconds"])
    snapshots = [
        _snapshot_from_payload(item)
        for item in payload.get("snapshots", [])
        if isinstance(item, dict)
    ]
    if not snapshots:
        print("No snapshots in report")
        return 2

    at_timeout = _nearest_before_timeout(snapshots, timeout_seconds)
    final = snapshots[-1]
    nodes_payload = payload["result"].get("nodes", [])
    low_nodes = sorted(
        [item for item in nodes_payload if isinstance(item, dict)],
        key=lambda item: (
            int(item.get("discovered_function_catalog", 0)),
            str(item.get("node_id", "")),
        ),
    )[:8]

    print("Topology probe summary")
    print(f"report={report_path}")
    print(
        "config="
        f"nodes:{payload['config']['nodes']} "
        f"topology:{payload['config']['topology']} "
        f"gossip_interval:{payload['config']['gossip_interval_seconds']}"
    )
    print(
        "threshold="
        f"{threshold_payload['min_nodes_meeting_threshold']} nodes with "
        f"{threshold_payload['min_discovered_peers']}+ peers "
        f"within {timeout_seconds:.1f}s"
    )
    print(
        "result="
        f"converged:{payload['result']['converged']} "
        f"converged_at:{payload['result']['converged_at_seconds']}"
    )
    if at_timeout is not None:
        print(
            "at_timeout="
            f"t:{at_timeout.elapsed_seconds:.1f}s "
            f"meet(func/node/peerdir):"
            f"{at_timeout.nodes_meeting_function_threshold}/"
            f"{at_timeout.nodes_meeting_node_threshold}/"
            f"{at_timeout.nodes_meeting_peer_directory_threshold} "
            f"func_min_avg_max:{at_timeout.min_discovered_function_catalog}/"
            f"{at_timeout.avg_discovered_function_catalog:.1f}/"
            f"{at_timeout.max_discovered_function_catalog} "
            f"nodecat_min_avg_max:{at_timeout.min_discovered_node_catalog}/"
            f"{at_timeout.avg_discovered_node_catalog:.1f}/"
            f"{at_timeout.max_discovered_node_catalog} "
            f"pending_catalog_min_avg_max:{at_timeout.min_pending_catalog_messages}/"
            f"{at_timeout.avg_pending_catalog_messages:.1f}/"
            f"{at_timeout.max_pending_catalog_messages}"
        )
    print(
        "final="
        f"t:{final.elapsed_seconds:.1f}s "
        f"meet(func/node/peerdir):"
        f"{final.nodes_meeting_function_threshold}/"
        f"{final.nodes_meeting_node_threshold}/"
        f"{final.nodes_meeting_peer_directory_threshold} "
        f"func_min_avg_max:{final.min_discovered_function_catalog}/"
        f"{final.avg_discovered_function_catalog:.1f}/"
        f"{final.max_discovered_function_catalog} "
        f"pending_catalog_min_avg_max:{final.min_pending_catalog_messages}/"
        f"{final.avg_pending_catalog_messages:.1f}/"
        f"{final.max_pending_catalog_messages}"
    )
    if low_nodes:
        print("lowest_nodes_by_function_discovery=")
        for node in low_nodes:
            missing_peers = node.get("missing_function_catalog", [])
            missing_count = len(missing_peers) if isinstance(missing_peers, list) else 0
            print(
                f"  {node.get('node_id')} "
                f"func={node.get('discovered_function_catalog')} "
                f"nodecat={node.get('discovered_node_catalog')} "
                f"peerdir={node.get('discovered_peer_directory')} "
                f"active={node.get('active_connections')} "
                f"desired={node.get('desired_connections')} "
                f"pending_catalog={node.get('pending_catalog_messages')} "
                f"missing_count={missing_count}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
