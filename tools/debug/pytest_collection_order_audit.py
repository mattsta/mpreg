#!/usr/bin/env python3
"""
Reusable pytest collection-order audit tool.

This script proves whether `pytest --collect-only` output changes when the same
files are presented in a different order.
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

type NodeId = str
type Seconds = float
type ExitCode = int


@dataclass(frozen=True, slots=True)
class AuditConfigSnapshot:
    files: tuple[str, ...]
    pytest_args: tuple[str, ...]
    output_dir: str


@dataclass(frozen=True, slots=True)
class CollectionRun:
    label: str
    command: tuple[str, ...]
    file_order: tuple[str, ...]
    exit_code: ExitCode
    duration_seconds: Seconds
    collected_nodeids: tuple[NodeId, ...]
    output_path: str


@dataclass(frozen=True, slots=True)
class CollectionDiff:
    only_in_forward: tuple[NodeId, ...]
    only_in_reverse: tuple[NodeId, ...]
    shared_count: int


@dataclass(frozen=True, slots=True)
class AuditReport:
    generated_at_utc: str
    config: AuditConfigSnapshot
    runs: tuple[CollectionRun, ...]
    diff: CollectionDiff


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_collected_nodeids(output_text: str) -> tuple[NodeId, ...]:
    nodeids: list[NodeId] = []
    for raw_line in output_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "::" not in line:
            continue
        if line.startswith("FAILED") or line.startswith("ERROR"):
            continue
        if line.startswith("tests/"):
            nodeids.append(line)
    return tuple(nodeids)


def _run_collection(
    *,
    label: str,
    file_order: tuple[str, ...],
    pytest_args: tuple[str, ...],
    session_dir: Path,
) -> CollectionRun:
    command = (
        "uv",
        "run",
        "pytest",
        "--collect-only",
        "-q",
        *pytest_args,
        *file_order,
    )
    started = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=_repo_root(),
        text=True,
        capture_output=True,
        check=False,
    )
    duration_seconds = time.monotonic() - started

    combined_output = completed.stdout
    if completed.stderr:
        if combined_output and not combined_output.endswith("\n"):
            combined_output += "\n"
        combined_output += completed.stderr

    output_path = session_dir / f"{label}.log"
    header = [
        f"# generated_at_utc: {_utc_now_iso()}",
        f"# label: {label}",
        f"# exit_code: {completed.returncode}",
        f"# duration_seconds: {duration_seconds:.3f}",
        f"# command: {' '.join(shlex.quote(part) for part in command)}",
        "",
    ]
    output_path.write_text("\n".join(header) + combined_output, encoding="utf-8")

    collected_nodeids = _parse_collected_nodeids(combined_output)
    return CollectionRun(
        label=label,
        command=command,
        file_order=file_order,
        exit_code=completed.returncode,
        duration_seconds=duration_seconds,
        collected_nodeids=collected_nodeids,
        output_path=str(output_path),
    )


def _build_report(
    *,
    files: tuple[str, ...],
    pytest_args: tuple[str, ...],
    output_dir: Path,
    runs: tuple[CollectionRun, ...],
) -> AuditReport:
    forward = runs[0]
    reverse = runs[1]
    forward_set = set(forward.collected_nodeids)
    reverse_set = set(reverse.collected_nodeids)
    only_in_forward = tuple(sorted(forward_set - reverse_set))
    only_in_reverse = tuple(sorted(reverse_set - forward_set))
    shared_count = len(forward_set & reverse_set)

    return AuditReport(
        generated_at_utc=_utc_now_iso(),
        config=AuditConfigSnapshot(
            files=files,
            pytest_args=pytest_args,
            output_dir=str(output_dir),
        ),
        runs=runs,
        diff=CollectionDiff(
            only_in_forward=only_in_forward,
            only_in_reverse=only_in_reverse,
            shared_count=shared_count,
        ),
    )


def _write_report(report: AuditReport, session_dir: Path) -> None:
    json_path = session_dir / "report.json"
    text_path = session_dir / "report.txt"

    json_path.write_text(
        json.dumps(asdict(report), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    forward = report.runs[0]
    reverse = report.runs[1]
    lines: list[str] = []
    lines.append("Pytest Collection Order Audit")
    lines.append(f"generated_at_utc: {report.generated_at_utc}")
    lines.append(f"output_dir: {report.config.output_dir}")
    lines.append(f"files: {', '.join(report.config.files)}")
    lines.append(
        "pytest_args: "
        + (
            ", ".join(report.config.pytest_args)
            if report.config.pytest_args
            else "(none)"
        )
    )
    lines.append(
        f"forward_count: {len(forward.collected_nodeids)} ({forward.output_path})"
    )
    lines.append(
        f"reverse_count: {len(reverse.collected_nodeids)} ({reverse.output_path})"
    )
    lines.append(f"shared_count: {report.diff.shared_count}")
    lines.append(f"only_in_forward: {len(report.diff.only_in_forward)}")
    for nodeid in report.diff.only_in_forward:
        lines.append(f"  - {nodeid}")
    lines.append(f"only_in_reverse: {len(report.diff.only_in_reverse)}")
    for nodeid in report.diff.only_in_reverse:
        lines.append(f"  - {nodeid}")
    text_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit pytest collection order sensitivity"
    )
    parser.add_argument(
        "--file",
        action="append",
        required=True,
        help="Test file to include in order audit (repeatable, at least two)",
    )
    parser.add_argument(
        "--pytest-arg",
        action="append",
        default=[],
        help="Additional pytest argument for collect-only run (repeatable)",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/debug/collection_order_audit",
        help="Directory for audit artifacts",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    files = tuple(str(value).strip() for value in args.file if str(value).strip())
    if len(files) < 2:
        raise SystemExit("At least two --file entries are required")

    pytest_args = tuple(str(value) for value in args.pytest_arg)
    output_dir = Path(str(args.output_dir)).resolve()
    session_dir = output_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    session_dir.mkdir(parents=True, exist_ok=True)

    forward_order = files
    reverse_order = tuple(reversed(files))

    forward_run = _run_collection(
        label="forward",
        file_order=forward_order,
        pytest_args=pytest_args,
        session_dir=session_dir,
    )
    reverse_run = _run_collection(
        label="reverse",
        file_order=reverse_order,
        pytest_args=pytest_args,
        session_dir=session_dir,
    )

    report = _build_report(
        files=files,
        pytest_args=pytest_args,
        output_dir=output_dir,
        runs=(forward_run, reverse_run),
    )
    _write_report(report, session_dir)

    print("Pytest collection order audit")
    print(f"session_dir={session_dir}")
    print(f"forward_count={len(forward_run.collected_nodeids)}")
    print(f"reverse_count={len(reverse_run.collected_nodeids)}")
    print(f"shared_count={report.diff.shared_count}")
    print(f"only_in_forward={len(report.diff.only_in_forward)}")
    print(f"only_in_reverse={len(report.diff.only_in_reverse)}")
    print(f"report_json={session_dir / 'report.json'}")
    print(f"report_text={session_dir / 'report.txt'}")

    has_diff = bool(report.diff.only_in_forward or report.diff.only_in_reverse)
    has_error = forward_run.exit_code != 0 or reverse_run.exit_code != 0
    if has_error:
        return 2
    return 1 if has_diff else 0


if __name__ == "__main__":
    raise SystemExit(main())
