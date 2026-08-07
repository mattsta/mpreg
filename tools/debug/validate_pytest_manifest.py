#!/usr/bin/env python3
"""Validate pytest node IDs declared in a debug manifest."""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

type NodeId = str


@dataclass(frozen=True, slots=True)
class ValidationRun:
    nodeid: NodeId
    command: tuple[str, ...]
    exit_code: int
    valid: bool
    output_excerpt: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ValidationReport:
    generated_at_utc: str
    manifest_path: str
    total_tests: int
    valid_tests: int
    invalid_tests: int
    runs: tuple[ValidationRun, ...]


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_manifest_tests(manifest_path: Path) -> tuple[NodeId, ...]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Manifest must be a JSON object")
    raw_tests = payload.get("tests", [])
    if not isinstance(raw_tests, list):
        raise ValueError("Manifest 'tests' must be a list")
    tests = tuple(str(nodeid) for nodeid in raw_tests if str(nodeid).strip())
    if not tests:
        raise ValueError("Manifest contains no tests")
    return tests


def _validate_nodeid(nodeid: NodeId) -> ValidationRun:
    command = ("uv", "run", "pytest", "--collect-only", "-q", nodeid)
    process = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    output_lines = tuple(line.rstrip() for line in process.stdout.splitlines()[:12])
    is_valid = process.returncode == 0
    return ValidationRun(
        nodeid=nodeid,
        command=command,
        exit_code=int(process.returncode),
        valid=is_valid,
        output_excerpt=output_lines,
    )


def _write_report(output_dir: Path, report: ValidationReport) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "manifest_validation_report.json"
    text_path = output_dir / "manifest_validation_report.txt"

    json_path.write_text(
        json.dumps(asdict(report), indent=2, sort_keys=True), encoding="utf-8"
    )

    lines: list[str] = []
    lines.append("Manifest Validation Report")
    lines.append(f"generated_at_utc: {report.generated_at_utc}")
    lines.append(f"manifest_path: {report.manifest_path}")
    lines.append(
        f"totals: total={report.total_tests} valid={report.valid_tests} invalid={report.invalid_tests}"
    )
    lines.append("runs:")
    for run in report.runs:
        status = "VALID" if run.valid else "INVALID"
        lines.append(f"  - status={status} exit={run.exit_code} nodeid={run.nodeid}")
        for excerpt in run.output_excerpt[:3]:
            lines.append(f"    out={excerpt}")
    text_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate pytest node IDs in a manifest"
    )
    parser.add_argument("--manifest", required=True, help="Path to manifest JSON file")
    parser.add_argument(
        "--output-dir",
        default="artifacts/debug/manifest_validation",
        help="Directory for validation reports",
    )
    args = parser.parse_args()

    manifest_path = Path(str(args.manifest)).resolve()
    tests = _load_manifest_tests(manifest_path)

    runs = tuple(_validate_nodeid(nodeid) for nodeid in tests)
    valid_count = sum(1 for run in runs if run.valid)
    report = ValidationReport(
        generated_at_utc=_utc_now_iso(),
        manifest_path=str(manifest_path),
        total_tests=len(runs),
        valid_tests=valid_count,
        invalid_tests=len(runs) - valid_count,
        runs=runs,
    )

    output_dir = Path(str(args.output_dir)).resolve() / datetime.now(UTC).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    _write_report(output_dir, report)

    print(f"report_json={output_dir / 'manifest_validation_report.json'}")
    print(f"report_text={output_dir / 'manifest_validation_report.txt'}")
    print(
        f"totals: total={report.total_tests} valid={report.valid_tests} invalid={report.invalid_tests}"
    )

    return 0 if report.invalid_tests == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
