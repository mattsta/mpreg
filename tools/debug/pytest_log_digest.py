#!/usr/bin/env python3
"""Summarize pytest log files into deterministic failure evidence."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path

type NodeId = str


@dataclass(frozen=True, slots=True)
class PatternCounter:
    label: str
    regex: str


@dataclass(frozen=True, slots=True)
class PatternHit:
    label: str
    count: int


@dataclass(frozen=True, slots=True)
class FailureEntry:
    nodeid: NodeId
    detail: str


@dataclass(frozen=True, slots=True)
class DigestReport:
    log_path: str
    total_lines: int
    failed_count: int
    error_count: int
    failures: tuple[FailureEntry, ...]
    errors: tuple[FailureEntry, ...]
    summary_line: str | None
    pattern_hits: tuple[PatternHit, ...]


PATTERNS: tuple[PatternCounter, ...] = (
    PatternCounter("auto_discovery_timeout", r"Auto-discovery did not converge"),
    PatternCounter("raft_no_leader", r"No leader elected"),
    PatternCounter("stop_iteration", r"coroutine raised StopIteration"),
    PatternCounter("transport_connection_error", r"TransportConnectionError"),
    PatternCounter("assertion_error", r"AssertionError"),
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize pytest log failure evidence"
    )
    parser.add_argument("--log", required=True, help="Path to pytest log file")
    parser.add_argument(
        "--json-out",
        default=None,
        help="Optional path to write report JSON",
    )
    return parser.parse_args()


def _parse_result_line(line: str, prefix: str) -> FailureEntry | None:
    if not line.startswith(prefix):
        return None
    payload = line[len(prefix) :].strip()
    if not payload:
        return None
    if " - " in payload:
        nodeid, detail = payload.split(" - ", 1)
        return FailureEntry(nodeid=nodeid.strip(), detail=detail.strip())
    return FailureEntry(nodeid=payload, detail="")


def _digest_log(path: Path) -> DigestReport:
    failures: list[FailureEntry] = []
    errors: list[FailureEntry] = []
    summary_line: str | None = None
    total_lines = 0
    pattern_counts: dict[str, int] = {pattern.label: 0 for pattern in PATTERNS}

    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            total_lines += 1

            for pattern in PATTERNS:
                pattern_counts[pattern.label] += len(re.findall(pattern.regex, line))

            failure_entry = _parse_result_line(line.strip(), "FAILED ")
            if failure_entry is not None:
                failures.append(failure_entry)

            error_entry = _parse_result_line(line.strip(), "ERROR ")
            if error_entry is not None:
                errors.append(error_entry)

            compact = line.strip()
            if summary_line is None and re.match(
                r"^\d+ passed(?:, \d+ failed)?(?:, \d+ xfailed)?", compact
            ):
                summary_line = compact

    hits = tuple(
        PatternHit(label=pattern.label, count=pattern_counts[pattern.label])
        for pattern in PATTERNS
    )
    return DigestReport(
        log_path=str(path),
        total_lines=total_lines,
        failed_count=len(failures),
        error_count=len(errors),
        failures=tuple(failures),
        errors=tuple(errors),
        summary_line=summary_line,
        pattern_hits=hits,
    )


def _print_report(report: DigestReport) -> None:
    print(f"log_path={report.log_path}")
    print(f"total_lines={report.total_lines}")
    print(f"failed_count={report.failed_count}")
    print(f"error_count={report.error_count}")
    print(f"summary_line={report.summary_line}")
    print("pattern_hits:")
    for hit in report.pattern_hits:
        print(f"  - {hit.label}: {hit.count}")
    if report.failures:
        print("failures:")
        for failure in report.failures:
            print(f"  - {failure.nodeid} :: {failure.detail}")
    if report.errors:
        print("errors:")
        for error in report.errors:
            print(f"  - {error.nodeid} :: {error.detail}")


def main() -> int:
    args = _parse_args()
    log_path = Path(str(args.log)).resolve()
    if not log_path.exists():
        print(f"log not found: {log_path}")
        return 2
    report = _digest_log(log_path)
    _print_report(report)
    if args.json_out:
        out_path = Path(str(args.json_out)).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")
        print(f"json_out={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
