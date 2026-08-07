#!/usr/bin/env python3
"""Summarize DIAG_PEER_DIAL loop telemetry from pytest logs."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

type NodeName = str
type TargetCount = int


@dataclass(frozen=True, slots=True)
class LoopDiag:
    node: str
    name: NodeName
    targets: TargetCount
    connected_ratio: float
    discovery_ratio: float
    pressure: float
    desired_connected: int
    connected: int
    due: int
    selected: int
    parallelism: int
    budget: int
    interval_seconds: float


LOOP_RE = re.compile(
    r"\[DIAG_PEER_DIAL\] node=(?P<node>\S+) name=(?P<name>\S+) loop .*?"
    r"targets=(?P<targets>\d+) .*?"
    r"connected_ratio=(?P<connected_ratio>\d+\.\d+) "
    r"(?:discovery_ratio=(?P<discovery_ratio>\d+\.\d+) )?"
    r"pressure=(?P<pressure>\d+\.\d+) .*?"
    r"desired_connected=(?P<desired_connected>\d+) .*?"
    r"connected=(?P<connected>\d+) due=(?P<due>\d+) selected=(?P<selected>\d+) "
    r"parallelism=(?P<parallelism>\d+) budget=(?P<budget>\d+).*?"
    r"interval=(?P<interval>\d+\.\d+)s"
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze DIAG_PEER_DIAL loop telemetry"
    )
    parser.add_argument("--log", required=True, help="Path to log file")
    parser.add_argument(
        "--name-prefix",
        default="AutoDiscover-Node-",
        help="Filter node name prefix",
    )
    parser.add_argument(
        "--min-targets",
        type=int,
        default=10,
        help="Minimum targets value to include in summary",
    )
    return parser.parse_args()


def _load_entries(
    *,
    log_path: Path,
    name_prefix: str,
    min_targets: int,
) -> list[LoopDiag]:
    entries: list[LoopDiag] = []
    for raw_line in log_path.read_text(encoding="utf-8").splitlines():
        match = LOOP_RE.search(raw_line)
        if not match:
            continue
        name = str(match.group("name"))
        targets = int(match.group("targets"))
        if name_prefix and not name.startswith(name_prefix):
            continue
        if targets < min_targets:
            continue
        entries.append(
            LoopDiag(
                node=str(match.group("node")),
                name=name,
                targets=targets,
                connected_ratio=float(match.group("connected_ratio")),
                discovery_ratio=float(match.group("discovery_ratio") or 0.0),
                pressure=float(match.group("pressure")),
                desired_connected=int(match.group("desired_connected")),
                connected=int(match.group("connected")),
                due=int(match.group("due")),
                selected=int(match.group("selected")),
                parallelism=int(match.group("parallelism")),
                budget=int(match.group("budget")),
                interval_seconds=float(match.group("interval")),
            )
        )
    return entries


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _print_summary(entries: list[LoopDiag]) -> None:
    if not entries:
        print("entries=0")
        return

    max_targets = max(entry.targets for entry in entries)
    min_ratio = min(entry.connected_ratio for entry in entries)
    max_due = max(entry.due for entry in entries)
    print(f"entries={len(entries)}")
    print(f"max_targets={max_targets}")
    print(f"min_connected_ratio={min_ratio:.3f}")
    print(f"max_due={max_due}")

    low_connectivity = [entry for entry in entries if entry.connected_ratio < 0.5]
    if low_connectivity:
        print(f"low_connectivity_entries={len(low_connectivity)}")
        print(
            f"low_connectivity_mean_parallelism={_mean([float(entry.parallelism) for entry in low_connectivity]):.3f}"
        )
        print(
            f"low_connectivity_mean_selected={_mean([float(entry.selected) for entry in low_connectivity]):.3f}"
        )
        print(
            f"low_connectivity_mean_due={_mean([float(entry.due) for entry in low_connectivity]):.3f}"
        )
    else:
        print("low_connectivity_entries=0")

    constrained = [
        entry for entry in entries if entry.due >= 5 and entry.parallelism <= 1
    ]
    print(f"constrained_entries_due_ge_5_parallelism_le_1={len(constrained)}")
    starvation = [entry for entry in entries if entry.due >= 10 and entry.selected <= 2]
    print(f"starvation_entries_due_ge_10_selected_le_2={len(starvation)}")

    by_targets: dict[int, list[LoopDiag]] = {}
    for entry in entries:
        by_targets.setdefault(entry.targets, []).append(entry)
    print("targets_summary:")
    for targets in sorted(by_targets.keys()):
        bucket = by_targets[targets]
        print(
            f"  targets={targets} count={len(bucket)} "
            f"mean_connected_ratio={_mean([entry.connected_ratio for entry in bucket]):.3f} "
            f"mean_parallelism={_mean([float(entry.parallelism) for entry in bucket]):.3f} "
            f"mean_due={_mean([float(entry.due) for entry in bucket]):.3f} "
            f"mean_selected={_mean([float(entry.selected) for entry in bucket]):.3f}"
        )

    latest_by_name: dict[str, LoopDiag] = {}
    for entry in entries:
        latest_by_name[entry.name] = entry
    print("latest_by_node:")
    latest_entries = sorted(
        latest_by_name.values(),
        key=lambda entry: (entry.connected_ratio, entry.due, entry.name),
    )
    for entry in latest_entries:
        print(
            f"  name={entry.name} targets={entry.targets} desired={entry.desired_connected} "
            f"connected={entry.connected} ratio={entry.connected_ratio:.3f} "
            f"discovery_ratio={entry.discovery_ratio:.3f} "
            f"due={entry.due} selected={entry.selected} parallelism={entry.parallelism}"
        )


def main() -> int:
    args = _parse_args()
    log_path = Path(str(args.log)).resolve()
    if not log_path.exists():
        print(f"missing_log={log_path}")
        return 2
    entries = _load_entries(
        log_path=log_path,
        name_prefix=str(args.name_prefix),
        min_targets=int(args.min_targets),
    )
    _print_summary(entries)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
