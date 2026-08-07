#!/usr/bin/env python3
"""Summarize peer-dial saturation loops from DIAG_PEER_DIAL logs."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

type NodeUrl = str
type NodeName = str
type TimestampText = str


@dataclass(frozen=True, slots=True)
class LoopEntry:
    timestamp: TimestampText
    node_url: NodeUrl
    node_name: NodeName
    targets: int
    desired_connected: int
    connected: int
    due: int
    selected: int


@dataclass(frozen=True, slots=True)
class SaturationSummary:
    total_entries: int
    active_entries: int
    saturated_entries: int
    saturated_node_count: int
    first_saturated_at: TimestampText | None
    last_saturated_at: TimestampText | None
    max_saturated_due: int
    mean_saturated_due: float


LOOP_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \| .* "
    r"\[DIAG_PEER_DIAL\] node=(?P<node>\S+) name=(?P<name>\S+) loop "
    r"targets=(?P<targets>\d+) .* desired_connected=(?P<desired>\d+) "
    r"connected=(?P<connected>\d+) due=(?P<due>\d+) selected=(?P<selected>\d+) "
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze DIAG_PEER_DIAL saturation patterns"
    )
    parser.add_argument("--log", required=True, help="Path to pytest log")
    parser.add_argument(
        "--min-targets",
        type=int,
        default=20,
        help="Ignore entries with fewer than this many targets",
    )
    parser.add_argument(
        "--top-nodes",
        type=int,
        default=10,
        help="Number of nodes to include in node saturation ranking",
    )
    return parser.parse_args()


def _load_entries(*, log_path: Path, min_targets: int) -> list[LoopEntry]:
    entries: list[LoopEntry] = []
    for raw_line in log_path.read_text(encoding="utf-8").splitlines():
        match = LOOP_PATTERN.search(raw_line)
        if not match:
            continue
        targets = int(match.group("targets"))
        if targets < min_targets:
            continue
        entries.append(
            LoopEntry(
                timestamp=str(match.group("timestamp")),
                node_url=str(match.group("node")),
                node_name=str(match.group("name")),
                targets=targets,
                desired_connected=int(match.group("desired")),
                connected=int(match.group("connected")),
                due=int(match.group("due")),
                selected=int(match.group("selected")),
            )
        )
    return entries


def _is_saturated(entry: LoopEntry) -> bool:
    return (
        entry.due > 0
        and entry.selected == 0
        and entry.connected >= entry.desired_connected
    )


def _summarize(entries: list[LoopEntry]) -> tuple[SaturationSummary, dict[str, int]]:
    active_entries = [entry for entry in entries if entry.selected > 0]
    saturated_entries = [entry for entry in entries if _is_saturated(entry)]
    by_node: dict[str, int] = {}
    for entry in saturated_entries:
        by_node[entry.node_name] = by_node.get(entry.node_name, 0) + 1

    saturated_due_values = [entry.due for entry in saturated_entries]
    mean_saturated_due = 0.0
    if saturated_due_values:
        mean_saturated_due = sum(saturated_due_values) / len(saturated_due_values)

    first_saturated_at = saturated_entries[0].timestamp if saturated_entries else None
    last_saturated_at = saturated_entries[-1].timestamp if saturated_entries else None

    summary = SaturationSummary(
        total_entries=len(entries),
        active_entries=len(active_entries),
        saturated_entries=len(saturated_entries),
        saturated_node_count=len(by_node),
        first_saturated_at=first_saturated_at,
        last_saturated_at=last_saturated_at,
        max_saturated_due=max(saturated_due_values) if saturated_due_values else 0,
        mean_saturated_due=mean_saturated_due,
    )
    return summary, by_node


def main() -> int:
    args = _parse_args()
    log_path = Path(str(args.log)).resolve()
    if not log_path.exists():
        print(f"missing_log={log_path}")
        return 2
    entries = _load_entries(
        log_path=log_path, min_targets=max(1, int(args.min_targets))
    )
    summary, by_node = _summarize(entries)

    print(f"log_path={log_path}")
    print(f"entries={summary.total_entries}")
    print(f"active_entries={summary.active_entries}")
    print(f"saturated_entries={summary.saturated_entries}")
    print(f"saturated_node_count={summary.saturated_node_count}")
    print(f"first_saturated_at={summary.first_saturated_at}")
    print(f"last_saturated_at={summary.last_saturated_at}")
    print(f"max_saturated_due={summary.max_saturated_due}")
    print(f"mean_saturated_due={summary.mean_saturated_due:.3f}")

    if by_node:
        ranked = sorted(by_node.items(), key=lambda item: (-item[1], item[0]))
        top_n = max(1, int(args.top_nodes))
        print("top_saturated_nodes:")
        for node_name, count in ranked[:top_n]:
            print(f"  {node_name}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
