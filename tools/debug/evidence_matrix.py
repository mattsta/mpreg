#!/usr/bin/env python3
"""
Aggregate pytest_evidence_harness reports into a per-nodeid matrix.

Example:
  uv run python tools/debug/evidence_matrix.py \
    --report artifacts/debug/raft_focus_n3_single/20260207T020020Z/evidence_report.json \
    --report artifacts/debug/clean_perf_n3_single/20260207T020106Z/evidence_report.json \
    --report artifacts/debug/current_failures_n3_batch/20260207T021109Z/evidence_report.json
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path

type NodeId = str
type RunMode = str
type SessionId = str


@dataclass(frozen=True, slots=True)
class RunNodeOutcome:
    report_path: Path
    session_id: SessionId
    run_mode: RunMode
    run_index: int
    nodeid: NodeId
    status: str


@dataclass(slots=True)
class NodeAggregate:
    nodeid: NodeId
    pass_count: int = 0
    fail_count: int = 0
    timeout_count: int = 0

    def apply(self, status: str) -> None:
        if status == "PASS":
            self.pass_count += 1
            return
        if status == "TIMEOUT":
            self.timeout_count += 1
            return
        self.fail_count += 1


FAILED_NODE_RE = re.compile(r"^FAILED\s+([^\s]+)")


def _extract_failed_nodeids(pytest_failed_lines: list[str]) -> set[NodeId]:
    failed: set[NodeId] = set()
    for line in pytest_failed_lines:
        match = FAILED_NODE_RE.match(line.strip())
        if match:
            failed.add(match.group(1))
    return failed


def _session_id(report_path: Path) -> SessionId:
    # report path shape:
    # .../<session_id>/evidence_report.json
    return report_path.parent.name


def _load_outcomes(report_path: Path) -> list[RunNodeOutcome]:
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    run_mode = str(payload["config"]["run_mode"])
    session_id = _session_id(report_path)
    outcomes: list[RunNodeOutcome] = []
    for run in payload["runs"]:
        run_index = int(run["run_index"])
        targets = [str(value) for value in run["targets"]]
        timed_out = bool(run["timed_out"])
        exit_code = run["exit_code"]
        failed_lines = [str(line) for line in run["pytest_failed_lines"]]
        failed_nodeids = _extract_failed_nodeids(failed_lines)

        if run_mode == "single":
            status = "PASS" if (not timed_out and exit_code == 0) else "FAIL"
            if timed_out:
                status = "TIMEOUT"
            for nodeid in targets:
                outcomes.append(
                    RunNodeOutcome(
                        report_path=report_path,
                        session_id=session_id,
                        run_mode=run_mode,
                        run_index=run_index,
                        nodeid=nodeid,
                        status=status,
                    )
                )
            continue

        # Batch mode: identify explicit failed node IDs from pytest summary lines.
        if timed_out and not failed_nodeids:
            for nodeid in targets:
                outcomes.append(
                    RunNodeOutcome(
                        report_path=report_path,
                        session_id=session_id,
                        run_mode=run_mode,
                        run_index=run_index,
                        nodeid=nodeid,
                        status="TIMEOUT",
                    )
                )
            continue

        for nodeid in targets:
            status = "FAIL" if nodeid in failed_nodeids else "PASS"
            outcomes.append(
                RunNodeOutcome(
                    report_path=report_path,
                    session_id=session_id,
                    run_mode=run_mode,
                    run_index=run_index,
                    nodeid=nodeid,
                    status=status,
                )
            )
    return outcomes


def _print_summary(outcomes: list[RunNodeOutcome]) -> None:
    aggregates: dict[NodeId, NodeAggregate] = {}
    for item in outcomes:
        aggregate = aggregates.get(item.nodeid)
        if aggregate is None:
            aggregate = NodeAggregate(nodeid=item.nodeid)
            aggregates[item.nodeid] = aggregate
        aggregate.apply(item.status)

    print("Evidence matrix summary")
    print(f"reports={len({item.report_path for item in outcomes})}")
    print(f"outcomes={len(outcomes)}")
    print("nodeid,status_counts")
    for nodeid in sorted(aggregates.keys()):
        summary = aggregates[nodeid]
        print(
            f"{nodeid},pass={summary.pass_count},fail={summary.fail_count},timeout={summary.timeout_count}"
        )


def _print_detailed(outcomes: list[RunNodeOutcome]) -> None:
    print("Detailed outcomes")
    print("session,run_mode,run_index,status,nodeid")
    for item in sorted(
        outcomes,
        key=lambda value: (
            value.session_id,
            value.run_mode,
            value.run_index,
            value.nodeid,
        ),
    ):
        print(
            f"{item.session_id},{item.run_mode},{item.run_index},{item.status},{item.nodeid}"
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate pytest evidence reports into a per-nodeid matrix"
    )
    parser.add_argument(
        "--report",
        action="append",
        required=True,
        help="Path to evidence_report.json (repeatable)",
    )
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="Print per-run outcomes in addition to summary",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    report_paths = [Path(value).resolve() for value in args.report]
    outcomes: list[RunNodeOutcome] = []
    for report_path in report_paths:
        if not report_path.exists():
            print(f"Missing report: {report_path}")
            return 2
        outcomes.extend(_load_outcomes(report_path))

    if not outcomes:
        print("No outcomes loaded")
        return 2

    _print_summary(outcomes)
    if args.detailed:
        _print_detailed(outcomes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
