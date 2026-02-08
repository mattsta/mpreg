#!/usr/bin/env python3
"""Analyze auto-discovery diagnostic logs emitted by DIAG_CONN/DIAG_DISCOVERY_LOOP."""

from __future__ import annotations

import argparse
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from statistics import median

type Seconds = float
type NodeUrl = str


@dataclass(frozen=True, slots=True)
class ConnectFailure:
    url: str
    attempt: int
    attempts_total: int
    timeout_seconds: Seconds
    elapsed_seconds: Seconds
    error: str


@dataclass(frozen=True, slots=True)
class ConnectSuccess:
    url: str
    attempt: int
    attempts_total: int
    timeout_seconds: Seconds
    elapsed_seconds: Seconds


@dataclass(frozen=True, slots=True)
class DiscoveryLoopSnapshot:
    node: NodeUrl
    targets: int
    connected: int
    desired: int
    due: int
    selected: int
    not_due: int
    connected_ratio: float
    discovery_ratio: float
    function_catalog_discovered: int
    node_catalog_discovered: int
    peer_directory_discovered: int
    pending_catalog_updates: int
    interval_seconds: Seconds


CONNECT_FAILURE_RE = re.compile(
    r"\[DIAG_CONN\] connect_failure url=(?P<url>\S+) "
    r"attempt=(?P<attempt>\d+)/(?P<attempts_total>\d+) "
    r"timeout=(?P<timeout>[0-9.]+)s elapsed=(?P<elapsed>[0-9.]+)s error=(?P<error>.*)$"
)

CONNECT_SUCCESS_RE = re.compile(
    r"\[DIAG_CONN\] connect_success url=(?P<url>\S+) "
    r"attempt=(?P<attempt>\d+)/(?P<attempts_total>\d+) "
    r"timeout=(?P<timeout>[0-9.]+)s elapsed=(?P<elapsed>[0-9.]+)s$"
)

DISCOVERY_LOOP_RE = re.compile(
    r"\[DIAG_DISCOVERY_LOOP\] node=(?P<node>\S+) name=\S+ targets=(?P<targets>\d+) "
    r"connected=(?P<connected>\d+) desired=(?P<desired>\d+) due=(?P<due>\d+) "
    r"selected=(?P<selected>\d+) not_due=(?P<not_due>\d+) "
    r"connected_ratio=(?P<connected_ratio>[0-9.]+) "
    r"discovery_ratio=(?P<discovery_ratio>[0-9.]+) "
    r"function_catalog_discovered=(?P<function_catalog_discovered>-?\d+) "
    r"node_catalog_discovered=(?P<node_catalog_discovered>-?\d+) "
    r"peer_directory_discovered=(?P<peer_directory_discovered>-?\d+) "
    r"pending_catalog_updates=(?P<pending_catalog_updates>\d+) "
    r"interval=(?P<interval>[0-9.]+)s$"
)


def _parse_args() -> Path:
    parser = argparse.ArgumentParser(description="Analyze discovery diagnostic log")
    parser.add_argument("--log", required=True, help="Path to pytest run log")
    args = parser.parse_args()
    return Path(str(args.log))


def _classify_failure(error: str) -> str:
    lower_error = error.lower()
    if "http 503" in lower_error:
        return "http_503"
    if "connection timeout" in lower_error:
        return "timeout"
    if "connect call failed" in lower_error:
        return "connect_refused"
    if "max reconnection attempts reached" in lower_error:
        return "max_retries"
    return "other"


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * p))))
    return ordered[index]


def main() -> int:
    log_path = _parse_args()
    if not log_path.exists():
        print(f"missing_log={log_path}")
        return 2

    failures: list[ConnectFailure] = []
    successes: list[ConnectSuccess] = []
    latest_discovery_by_node: dict[NodeUrl, DiscoveryLoopSnapshot] = {}

    for raw_line in log_path.read_text(encoding="utf-8").splitlines():
        failure_match = CONNECT_FAILURE_RE.search(raw_line)
        if failure_match:
            failures.append(
                ConnectFailure(
                    url=failure_match.group("url"),
                    attempt=int(failure_match.group("attempt")),
                    attempts_total=int(failure_match.group("attempts_total")),
                    timeout_seconds=float(failure_match.group("timeout")),
                    elapsed_seconds=float(failure_match.group("elapsed")),
                    error=failure_match.group("error"),
                )
            )
            continue

        success_match = CONNECT_SUCCESS_RE.search(raw_line)
        if success_match:
            successes.append(
                ConnectSuccess(
                    url=success_match.group("url"),
                    attempt=int(success_match.group("attempt")),
                    attempts_total=int(success_match.group("attempts_total")),
                    timeout_seconds=float(success_match.group("timeout")),
                    elapsed_seconds=float(success_match.group("elapsed")),
                )
            )
            continue

        discovery_match = DISCOVERY_LOOP_RE.search(raw_line)
        if discovery_match:
            snapshot = DiscoveryLoopSnapshot(
                node=discovery_match.group("node"),
                targets=int(discovery_match.group("targets")),
                connected=int(discovery_match.group("connected")),
                desired=int(discovery_match.group("desired")),
                due=int(discovery_match.group("due")),
                selected=int(discovery_match.group("selected")),
                not_due=int(discovery_match.group("not_due")),
                connected_ratio=float(discovery_match.group("connected_ratio")),
                discovery_ratio=float(discovery_match.group("discovery_ratio")),
                function_catalog_discovered=int(
                    discovery_match.group("function_catalog_discovered")
                ),
                node_catalog_discovered=int(
                    discovery_match.group("node_catalog_discovered")
                ),
                peer_directory_discovered=int(
                    discovery_match.group("peer_directory_discovered")
                ),
                pending_catalog_updates=int(
                    discovery_match.group("pending_catalog_updates")
                ),
                interval_seconds=float(discovery_match.group("interval")),
            )
            latest_discovery_by_node[snapshot.node] = snapshot

    failure_class_counts = Counter(_classify_failure(item.error) for item in failures)
    failure_url_counts = Counter(item.url for item in failures)
    failure_timeout_exceeded = sum(
        1 for item in failures if item.elapsed_seconds >= item.timeout_seconds
    )
    success_elapsed = [item.elapsed_seconds for item in successes]
    failure_elapsed = [item.elapsed_seconds for item in failures]
    latest_snapshots = list(latest_discovery_by_node.values())

    print(f"log={log_path}")
    print(f"connect_successes={len(successes)}")
    print(f"connect_failures={len(failures)}")
    print(
        "failure_classes="
        + ",".join(
            f"{failure_class}:{count}"
            for failure_class, count in failure_class_counts.most_common()
        )
    )
    print(f"failure_timeout_exceeded={failure_timeout_exceeded}/{len(failures)}")
    if success_elapsed:
        print(
            "success_elapsed_seconds="
            f"p50={median(success_elapsed):.3f} "
            f"p90={_percentile(success_elapsed, 0.90):.3f} "
            f"p99={_percentile(success_elapsed, 0.99):.3f} "
            f"max={max(success_elapsed):.3f}"
        )
    if failure_elapsed:
        print(
            "failure_elapsed_seconds="
            f"p50={median(failure_elapsed):.3f} "
            f"p90={_percentile(failure_elapsed, 0.90):.3f} "
            f"p99={_percentile(failure_elapsed, 0.99):.3f} "
            f"max={max(failure_elapsed):.3f}"
        )

    if failure_url_counts:
        print("top_failure_urls:")
        for url, count in failure_url_counts.most_common(8):
            print(f"  {url} -> {count}")

    if latest_snapshots:
        min_function_catalog = min(
            snapshot.function_catalog_discovered for snapshot in latest_snapshots
        )
        min_node_catalog = min(
            snapshot.node_catalog_discovered for snapshot in latest_snapshots
        )
        min_peer_directory = min(
            snapshot.peer_directory_discovered for snapshot in latest_snapshots
        )
        max_pending_catalog = max(
            snapshot.pending_catalog_updates for snapshot in latest_snapshots
        )
        low_connected_nodes = [
            snapshot
            for snapshot in sorted(
                latest_snapshots,
                key=lambda entry: (
                    entry.connected_ratio,
                    entry.function_catalog_discovered,
                    entry.node,
                ),
            )[:8]
        ]

        print(
            "latest_discovery_summary="
            f"nodes={len(latest_snapshots)} "
            f"min_function_catalog={min_function_catalog} "
            f"min_node_catalog={min_node_catalog} "
            f"min_peer_directory={min_peer_directory} "
            f"max_pending_catalog={max_pending_catalog}"
        )
        print("lowest_connected_nodes:")
        for snapshot in low_connected_nodes:
            print(
                "  "
                f"{snapshot.node} connected_ratio={snapshot.connected_ratio:.2f} "
                f"targets={snapshot.targets} connected={snapshot.connected} desired={snapshot.desired} "
                f"function_catalog={snapshot.function_catalog_discovered} "
                f"node_catalog={snapshot.node_catalog_discovered} "
                f"peer_directory={snapshot.peer_directory_discovered} "
                f"pending_catalog_updates={snapshot.pending_catalog_updates}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
