#!/usr/bin/env python3
"""Repeat advanced topology load-balance test and extract run metrics."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

type Seconds = float
type Percent = float
type NodeId = str

DEFAULT_NODEID: NodeId = (
    "tests/test_advanced_topological_research.py::"
    "TestAdvancedTopologicalResearch::"
    "test_hierarchical_regional_federation_with_auto_balancing"
)


@dataclass(frozen=True, slots=True)
class ProbeConfig:
    nodeid: NodeId
    repeats: int
    timeout_seconds: Seconds
    output_dir: str
    enable_platform_diag: bool


@dataclass(frozen=True, slots=True)
class RunMetrics:
    load_balance_percent: Percent | None
    propagation_success_percent: Percent | None
    fault_tolerance_percent: Percent | None
    hierarchy_efficiency_percent: Percent | None


@dataclass(frozen=True, slots=True)
class RunResult:
    run_index: int
    exit_code: int | None
    timed_out: bool
    duration_seconds: Seconds
    log_path: str
    passed: bool
    metrics: RunMetrics
    failure_line: str | None


@dataclass(frozen=True, slots=True)
class ProbeReport:
    generated_at_utc: str
    config: ProbeConfig
    total_runs: int
    passed_runs: int
    failed_runs: int
    timeout_runs: int
    min_load_balance_percent: Percent | None
    max_load_balance_percent: Percent | None
    avg_load_balance_percent: Percent | None
    runs: tuple[RunResult, ...]


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_args() -> ProbeConfig:
    parser = argparse.ArgumentParser(
        description="Repeat a topology test and extract load-balance metrics"
    )
    parser.add_argument("--nodeid", default=DEFAULT_NODEID)
    parser.add_argument("--repeats", type=int, default=6)
    parser.add_argument("--timeout-seconds", type=float, default=900.0)
    parser.add_argument(
        "--output-dir",
        default="artifacts/debug/advanced_topology_load_balance_probe",
    )
    parser.add_argument(
        "--enable-platform-diag",
        action="store_true",
        help="Enable MPREG platform diagnostic env flags during each run",
    )
    args = parser.parse_args()
    return ProbeConfig(
        nodeid=str(args.nodeid),
        repeats=max(1, int(args.repeats)),
        timeout_seconds=max(30.0, float(args.timeout_seconds)),
        output_dir=str(Path(str(args.output_dir)).resolve()),
        enable_platform_diag=bool(args.enable_platform_diag),
    )


def _extract_percent(pattern: str, text: str) -> Percent | None:
    match = re.search(pattern, text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _extract_failure_line(text: str) -> str | None:
    for line in text.splitlines():
        stripped = line.strip()
        if "Poor load balancing:" in stripped:
            return stripped
        if stripped.startswith("FAILED "):
            return stripped
    return None


def _extract_metrics(text: str) -> RunMetrics:
    return RunMetrics(
        load_balance_percent=_extract_percent(r"Load balancing:\s+([0-9.]+)%", text),
        propagation_success_percent=_extract_percent(
            r"Hierarchical propagation:.*\(([0-9.]+)% success\)", text
        ),
        fault_tolerance_percent=_extract_percent(
            r"Fault tolerance:\s+([0-9.]+)%", text
        ),
        hierarchy_efficiency_percent=_extract_percent(
            r"Hierarchy efficiency:\s+([0-9.]+)%", text
        ),
    )


def _run_once(config: ProbeConfig, run_index: int, session_dir: Path) -> RunResult:
    log_path = session_dir / f"run_{run_index:03d}.log"
    cmd = ["uv", "run", "pytest", "-q", "-s", config.nodeid]

    env = os.environ.copy()
    if config.enable_platform_diag:
        env["MPREG_DEBUG_RAFT"] = "1"
        env["MPREG_DEBUG_GOSSIP_SCHED"] = "1"
        env["MPREG_DEBUG_PEER_DIAL_POLICY"] = "1"
        env["MPREG_DEBUG_SUMMARY_INGRESS"] = "1"

    started = time.monotonic()
    exit_code: int | None = None
    timed_out = False

    with log_path.open("w", encoding="utf-8") as handle:
        handle.write(f"# started_at_utc: {_utc_now_iso()}\n")
        handle.write(f"# run_index: {run_index}\n")
        handle.write(f"# nodeid: {config.nodeid}\n")
        handle.write(f"# command: {' '.join(cmd)}\n")
        handle.write("\n")
        handle.flush()

        proc = subprocess.Popen(
            cmd,
            cwd=Path(__file__).resolve().parents[2],
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            start_new_session=True,
        )
        try:
            proc.wait(timeout=config.timeout_seconds)
            exit_code = proc.returncode
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(proc.pid, 15)
            except ProcessLookupError:
                pass
            except Exception:
                pass
            try:
                proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, 9)
                except ProcessLookupError:
                    pass
                except Exception:
                    pass
            exit_code = proc.returncode

    duration = time.monotonic() - started
    text = log_path.read_text(encoding="utf-8", errors="replace")
    metrics = _extract_metrics(text)
    passed = (not timed_out) and exit_code == 0
    return RunResult(
        run_index=run_index,
        exit_code=exit_code,
        timed_out=timed_out,
        duration_seconds=duration,
        log_path=str(log_path),
        passed=passed,
        metrics=metrics,
        failure_line=_extract_failure_line(text),
    )


def _build_report(config: ProbeConfig, runs: tuple[RunResult, ...]) -> ProbeReport:
    load_values = [
        run.metrics.load_balance_percent
        for run in runs
        if run.metrics.load_balance_percent is not None
    ]
    min_load = min(load_values) if load_values else None
    max_load = max(load_values) if load_values else None
    avg_load = (sum(load_values) / len(load_values)) if load_values else None

    total_runs = len(runs)
    passed_runs = sum(1 for run in runs if run.passed)
    timeout_runs = sum(1 for run in runs if run.timed_out)
    failed_runs = total_runs - passed_runs

    return ProbeReport(
        generated_at_utc=_utc_now_iso(),
        config=config,
        total_runs=total_runs,
        passed_runs=passed_runs,
        failed_runs=failed_runs,
        timeout_runs=timeout_runs,
        min_load_balance_percent=min_load,
        max_load_balance_percent=max_load,
        avg_load_balance_percent=avg_load,
        runs=runs,
    )


def _write_report(report: ProbeReport, session_dir: Path) -> None:
    json_path = session_dir / "probe_report.json"
    text_path = session_dir / "probe_report.txt"

    json_path.write_text(
        json.dumps(asdict(report), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    lines: list[str] = []
    lines.append("Advanced Topology Load-Balance Probe")
    lines.append(f"generated_at_utc: {report.generated_at_utc}")
    lines.append(f"nodeid: {report.config.nodeid}")
    lines.append(
        "totals: "
        f"total={report.total_runs} "
        f"passed={report.passed_runs} "
        f"failed={report.failed_runs} "
        f"timed_out={report.timeout_runs}"
    )
    lines.append(
        "load_balance_percent: "
        f"min={report.min_load_balance_percent} "
        f"max={report.max_load_balance_percent} "
        f"avg={report.avg_load_balance_percent}"
    )
    lines.append("runs:")
    for run in report.runs:
        lines.append(
            f"  - run={run.run_index:03d} passed={run.passed} exit={run.exit_code} "
            f"timeout={run.timed_out} duration={run.duration_seconds:.2f}s "
            f"load_balance={run.metrics.load_balance_percent}"
        )
        lines.append(f"    log={run.log_path}")
        if run.failure_line:
            lines.append(f"    failure={run.failure_line}")

    text_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    config = _parse_args()
    session_dir = Path(config.output_dir) / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    session_dir.mkdir(parents=True, exist_ok=True)

    print(f"session_dir={session_dir}")
    print(f"nodeid={config.nodeid}")
    print(f"repeats={config.repeats} timeout_seconds={config.timeout_seconds}")
    if config.enable_platform_diag:
        print("platform_diag=enabled")

    run_results: list[RunResult] = []
    for run_index in range(1, config.repeats + 1):
        print(f"[run {run_index:03d}] start")
        result = _run_once(config, run_index, session_dir)
        run_results.append(result)
        print(
            f"[run {run_index:03d}] passed={result.passed} exit={result.exit_code} "
            f"timeout={result.timed_out} duration={result.duration_seconds:.2f}s "
            f"load_balance={result.metrics.load_balance_percent}"
        )
        if result.failure_line:
            print(f"[run {run_index:03d}] failure={result.failure_line}")
        print(f"[run {run_index:03d}] log={result.log_path}")

    report = _build_report(config, tuple(run_results))
    _write_report(report, session_dir)
    print(f"report_json={session_dir / 'probe_report.json'}")
    print(f"report_text={session_dir / 'probe_report.txt'}")
    print(
        f"summary passed={report.passed_runs}/{report.total_runs} "
        f"load_balance_min={report.min_load_balance_percent}"
    )
    return 0 if report.failed_runs == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
