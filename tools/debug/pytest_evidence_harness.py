#!/usr/bin/env python3
"""
Reusable deep-dive pytest evidence harness.

This runner executes targeted pytest node IDs with:
- per-run timeout enforcement (with process-group cleanup),
- deterministic artifact output (one log per run),
- structured evidence reports (JSON + text summary),
- failure signature counting for quick triage.

Examples:
  uv run python tools/debug/pytest_evidence_harness.py \
    --test tests/test_comprehensive_auto_discovery.py::TestAutoDiscoveryMediumClusters::test_20_node_star_hub_auto_discovery

  uv run python tools/debug/pytest_evidence_harness.py \
    --preset auto20-chain --repeat 3 --timeout 120

  uv run python tools/debug/pytest_evidence_harness.py \
    --manifest tools/debug/manifests/current_failure_audit.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import signal
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

type NodeId = str
type RunOrdinal = int
type Seconds = float
type ExitCode = int
type EnvKey = str
type EnvValue = str
type ManifestPath = str
type RunMode = str


@dataclass(frozen=True, slots=True)
class FailurePattern:
    label: str
    regex: str


@dataclass(slots=True)
class PatternHit:
    label: str
    count: int


@dataclass(frozen=True, slots=True)
class EnvOverride:
    key: EnvKey
    value: EnvValue


@dataclass(frozen=True, slots=True)
class HarnessConfigSnapshot:
    tests: tuple[NodeId, ...]
    run_mode: RunMode
    repeat_count: int
    timeout_seconds: Seconds
    output_dir: str
    pytest_args: tuple[str, ...]
    env_overrides: tuple[EnvOverride, ...]
    stop_on_failure: bool
    preset: str | None


@dataclass(slots=True)
class HarnessConfig:
    tests: tuple[NodeId, ...]
    run_mode: RunMode
    repeat_count: int
    timeout_seconds: Seconds
    output_dir: Path
    pytest_args: tuple[str, ...]
    env_overrides: tuple[EnvOverride, ...]
    stop_on_failure: bool
    preset: str | None

    def snapshot(self) -> HarnessConfigSnapshot:
        return HarnessConfigSnapshot(
            tests=self.tests,
            run_mode=self.run_mode,
            repeat_count=self.repeat_count,
            timeout_seconds=self.timeout_seconds,
            output_dir=str(self.output_dir),
            pytest_args=self.pytest_args,
            env_overrides=self.env_overrides,
            stop_on_failure=self.stop_on_failure,
            preset=self.preset,
        )


@dataclass(frozen=True, slots=True)
class HarnessManifest:
    tests: tuple[NodeId, ...]
    preset: str | None
    run_mode: RunMode
    repeat_count: int
    timeout_seconds: Seconds
    output_dir: str | None
    pytest_args: tuple[str, ...]
    env_overrides: tuple[EnvOverride, ...]
    stop_on_failure: bool


@dataclass(frozen=True, slots=True)
class RunSpec:
    run_index: RunOrdinal
    cycle_index: int
    label: str
    targets: tuple[NodeId, ...]
    timeout_seconds: Seconds


@dataclass(slots=True)
class RunResult:
    run_index: RunOrdinal
    cycle_index: int
    label: str
    targets: tuple[NodeId, ...]
    command: tuple[str, ...]
    exit_code: ExitCode | None
    timed_out: bool
    duration_seconds: Seconds
    started_at_utc: str
    log_path: str
    short_failure_line: str | None
    pytest_failed_lines: tuple[str, ...]
    pattern_hits: tuple[PatternHit, ...]

    @property
    def passed(self) -> bool:
        return not self.timed_out and self.exit_code == 0


@dataclass(slots=True)
class AggregateTotals:
    total_runs: int
    passed_runs: int
    failed_runs: int
    timed_out_runs: int


@dataclass(slots=True)
class PatternAggregate:
    label: str
    total_count: int


@dataclass(slots=True)
class EvidenceReport:
    generated_at_utc: str
    config: HarnessConfigSnapshot
    totals: AggregateTotals
    pattern_totals: tuple[PatternAggregate, ...]
    failed_nodeids: tuple[NodeId, ...]
    runs: tuple[RunResult, ...]


PATTERNS: tuple[FailurePattern, ...] = (
    FailurePattern("assertion_error", r"AssertionError"),
    FailurePattern("auto_discovery_timeout", r"Auto-discovery did not converge"),
    FailurePattern("leader_election_timeout", r"No leader elected"),
    FailurePattern("connection_refused", r"Connect call failed"),
    FailurePattern("connection_timeout", r"WebSocket connection timeout"),
    FailurePattern("max_retries_reached", r"Max reconnection attempts reached"),
    FailurePattern(
        "handshake_invalid", r"InvalidMessage: did not receive a valid HTTP request"
    ),
    FailurePattern("diag_peer_dial", r"\[DIAG_PEER_DIAL\]"),
    FailurePattern("diag_connection", r"\[DIAG_CONN\]"),
    FailurePattern("attribute_error", r"AttributeError"),
)

PRESET_TESTS: dict[str, tuple[NodeId, ...]] = {
    "auto20-only": (
        "tests/test_comprehensive_auto_discovery.py::TestAutoDiscoveryMediumClusters::test_20_node_star_hub_auto_discovery",
    ),
    "auto20-chain": (
        "tests/test_live_raft_integration.py::TestLiveRaftIntegration::test_live_cluster_size_performance[9]",
        "tests/test_live_raft_integration.py::TestLiveRaftIntegration::test_live_cluster_size_performance[11]",
        "tests/test_comprehensive_auto_discovery.py::TestAutoDiscoveryMediumClusters::test_20_node_star_hub_auto_discovery",
    ),
}

DEFAULT_REPEAT_COUNT = 1
DEFAULT_TIMEOUT_SECONDS = 120.0
DEFAULT_OUTPUT_DIR = "artifacts/debug/pytest_evidence"
DEFAULT_RUN_MODE = "single"
RUN_MODES: tuple[RunMode, ...] = ("single", "batch")


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _parse_env_entry(entry: str) -> EnvOverride:
    clean_entry = entry.strip()
    if "=" not in clean_entry:
        raise ValueError(f"--env must be KEY=VALUE, got: {clean_entry}")
    key, value = clean_entry.split("=", 1)
    clean_key = key.strip()
    if not clean_key:
        raise ValueError(f"--env key cannot be empty: {clean_entry}")
    return EnvOverride(key=clean_key, value=value)


def _parse_env_entries(entries: tuple[str, ...]) -> tuple[EnvOverride, ...]:
    env_overrides: list[EnvOverride] = []
    for raw_entry in entries:
        entry = raw_entry.strip()
        if not entry:
            continue
        env_overrides.append(_parse_env_entry(entry))
    return tuple(env_overrides)


def _resolve_tests(
    *, preset: str | None, tests: tuple[str, ...], source_label: str
) -> tuple[NodeId, ...]:
    selected_tests: list[NodeId] = []
    if preset:
        selected_tests.extend(PRESET_TESTS[preset])
    selected_tests.extend([nodeid for nodeid in tests if nodeid.strip()])
    if not selected_tests:
        raise ValueError(
            f"At least one test is required from {source_label} (--test/--preset or manifest)"
        )
    return tuple(selected_tests)


def _load_manifest(path: Path) -> HarnessManifest:
    if not path.exists():
        raise ValueError(f"Manifest file not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Manifest must be a JSON object")

    raw_preset = payload.get("preset")
    preset = str(raw_preset) if raw_preset is not None else None
    if preset is not None and preset not in PRESET_TESTS:
        allowed = ", ".join(sorted(PRESET_TESTS.keys()))
        raise ValueError(f"Invalid manifest preset '{preset}'. Allowed: {allowed}")

    raw_tests = payload.get("tests", [])
    if raw_tests is None:
        raw_tests = []
    if not isinstance(raw_tests, list):
        raise ValueError("Manifest 'tests' must be a list of node IDs")
    tests = tuple(str(nodeid) for nodeid in raw_tests if str(nodeid).strip())

    raw_run_mode = payload.get("run_mode", DEFAULT_RUN_MODE)
    run_mode = (
        str(raw_run_mode).strip() if raw_run_mode is not None else DEFAULT_RUN_MODE
    )
    if run_mode not in RUN_MODES:
        allowed = ", ".join(RUN_MODES)
        raise ValueError(f"Invalid manifest run_mode '{run_mode}'. Allowed: {allowed}")

    raw_repeat = payload.get(
        "repeat_count", payload.get("repeat", DEFAULT_REPEAT_COUNT)
    )
    repeat_count = max(1, int(raw_repeat))

    raw_timeout = payload.get(
        "timeout_seconds", payload.get("timeout", DEFAULT_TIMEOUT_SECONDS)
    )
    timeout_seconds = max(1.0, float(raw_timeout))

    raw_output_dir = payload.get("output_dir")
    output_dir = str(raw_output_dir) if raw_output_dir is not None else None

    raw_pytest_args = payload.get("pytest_args", [])
    if raw_pytest_args is None:
        raw_pytest_args = []
    if not isinstance(raw_pytest_args, list):
        raise ValueError("Manifest 'pytest_args' must be a list of strings")
    pytest_args = tuple(str(value) for value in raw_pytest_args)

    raw_env_entries = payload.get("env_overrides", payload.get("env", []))
    if raw_env_entries is None:
        raw_env_entries = []
    if not isinstance(raw_env_entries, list):
        raise ValueError("Manifest 'env_overrides' must be a list of KEY=VALUE strings")
    env_overrides = _parse_env_entries(tuple(str(value) for value in raw_env_entries))

    stop_on_failure = bool(payload.get("stop_on_failure", False))
    return HarnessManifest(
        tests=tests,
        preset=preset,
        run_mode=run_mode,
        repeat_count=repeat_count,
        timeout_seconds=timeout_seconds,
        output_dir=output_dir,
        pytest_args=pytest_args,
        env_overrides=env_overrides,
        stop_on_failure=stop_on_failure,
    )


def _config_from_manifest(manifest_path: Path) -> HarnessConfig:
    manifest = _load_manifest(manifest_path)
    tests = _resolve_tests(
        preset=manifest.preset,
        tests=manifest.tests,
        source_label=f"manifest {manifest_path}",
    )
    output_dir = (
        Path(manifest.output_dir).resolve()
        if manifest.output_dir
        else Path(DEFAULT_OUTPUT_DIR).resolve()
    )
    return HarnessConfig(
        tests=tests,
        run_mode=manifest.run_mode,
        repeat_count=manifest.repeat_count,
        timeout_seconds=manifest.timeout_seconds,
        output_dir=output_dir,
        pytest_args=manifest.pytest_args,
        env_overrides=manifest.env_overrides,
        stop_on_failure=manifest.stop_on_failure,
        preset=manifest.preset,
    )


def _sanitize_nodeid(nodeid: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", nodeid).strip("._")
    if not safe:
        return "run"
    if len(safe) > 110:
        return safe[:110]
    return safe


def _parse_failed_lines(log_text: str) -> tuple[str, ...]:
    lines: list[str] = []
    for raw_line in log_text.splitlines():
        line = raw_line.strip()
        if line.startswith("FAILED "):
            lines.append(line)
    return tuple(lines)


def _extract_short_failure_line(
    log_text: str, failed_lines: tuple[str, ...]
) -> str | None:
    priority_tokens = (
        "AssertionError:",
        "AttributeError:",
        "Auto-discovery did not converge",
        "No leader elected",
        "FAILED ",
    )
    for raw_line in log_text.splitlines():
        line = raw_line.strip()
        for token in priority_tokens:
            if token in line:
                return line
    if failed_lines:
        return failed_lines[0]
    return None


def _collect_pattern_hits(log_text: str) -> tuple[PatternHit, ...]:
    hits: list[PatternHit] = []
    for pattern in PATTERNS:
        count = len(re.findall(pattern.regex, log_text))
        hits.append(PatternHit(label=pattern.label, count=count))
    return tuple(hits)


def _terminate_process_group(
    process: subprocess.Popen[str],
    *,
    grace_seconds: float,
) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
    except ProcessLookupError:
        return
    except Exception:
        pass
    try:
        process.wait(timeout=grace_seconds)
        return
    except subprocess.TimeoutExpired:
        pass
    if process.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
    except ProcessLookupError:
        return
    except Exception:
        pass


def _run_single(spec: RunSpec, config: HarnessConfig, artifact_dir: Path) -> RunResult:
    started_at_utc = _utc_now_iso()
    run_slug = _sanitize_nodeid(spec.label)
    log_path = artifact_dir / f"run_{spec.run_index:03d}_{run_slug}.log"
    command = (
        "uv",
        "run",
        "pytest",
        "-q",
        "-s",
        *config.pytest_args,
        *spec.targets,
    )

    header = [
        f"# started_at_utc: {started_at_utc}",
        f"# run_index: {spec.run_index}",
        f"# cycle_index: {spec.cycle_index}",
        f"# label: {spec.label}",
        f"# targets: {','.join(spec.targets)}",
        f"# timeout_seconds: {spec.timeout_seconds}",
        f"# command: {' '.join(shlex.quote(part) for part in command)}",
        "",
    ]
    if config.env_overrides:
        for override in config.env_overrides:
            header.append(f"# env_override: {override.key}={override.value}")
        header.append("")

    start_mono = time.monotonic()
    timed_out = False
    exit_code: int | None = None

    with log_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(header))
        handle.flush()
        env = os.environ.copy()
        for override in config.env_overrides:
            env[override.key] = override.value
        process = subprocess.Popen(
            command,
            cwd=_repo_root(),
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=(os.name == "posix"),
            env=env,
        )
        try:
            process.wait(timeout=spec.timeout_seconds)
            exit_code = process.returncode
        except subprocess.TimeoutExpired:
            timed_out = True
            _terminate_process_group(process, grace_seconds=5.0)
            exit_code = process.returncode
        finally:
            handle.write("\n")
            handle.write(f"# timed_out: {timed_out}\n")
            handle.write(f"# exit_code: {exit_code}\n")
            handle.flush()

    duration_seconds = time.monotonic() - start_mono
    log_text = log_path.read_text(encoding="utf-8")
    failed_lines = _parse_failed_lines(log_text)
    short_failure_line = _extract_short_failure_line(log_text, failed_lines)
    pattern_hits = _collect_pattern_hits(log_text)

    return RunResult(
        run_index=spec.run_index,
        cycle_index=spec.cycle_index,
        label=spec.label,
        targets=spec.targets,
        command=command,
        exit_code=exit_code,
        timed_out=timed_out,
        duration_seconds=duration_seconds,
        started_at_utc=started_at_utc,
        log_path=str(log_path),
        short_failure_line=short_failure_line,
        pytest_failed_lines=failed_lines,
        pattern_hits=pattern_hits,
    )


def _aggregate_results(
    config: HarnessConfig, run_results: tuple[RunResult, ...]
) -> EvidenceReport:
    total_runs = len(run_results)
    passed_runs = sum(1 for run in run_results if run.passed)
    timed_out_runs = sum(1 for run in run_results if run.timed_out)
    failed_runs = total_runs - passed_runs

    pattern_totals: list[PatternAggregate] = []
    for pattern in PATTERNS:
        total_count = 0
        for run in run_results:
            for hit in run.pattern_hits:
                if hit.label == pattern.label:
                    total_count += hit.count
                    break
        pattern_totals.append(
            PatternAggregate(label=pattern.label, total_count=total_count)
        )

    failed_nodeids: list[NodeId] = []
    for run in run_results:
        if run.passed:
            continue
        for target in run.targets:
            if target not in failed_nodeids:
                failed_nodeids.append(target)

    return EvidenceReport(
        generated_at_utc=_utc_now_iso(),
        config=config.snapshot(),
        totals=AggregateTotals(
            total_runs=total_runs,
            passed_runs=passed_runs,
            failed_runs=failed_runs,
            timed_out_runs=timed_out_runs,
        ),
        pattern_totals=tuple(pattern_totals),
        failed_nodeids=tuple(failed_nodeids),
        runs=run_results,
    )


def _write_report_files(report: EvidenceReport, artifact_dir: Path) -> None:
    json_path = artifact_dir / "evidence_report.json"
    text_path = artifact_dir / "evidence_report.txt"

    json_path.write_text(
        json.dumps(asdict(report), indent=2, sort_keys=True), encoding="utf-8"
    )

    lines: list[str] = []
    lines.append("Pytest Evidence Report")
    lines.append(f"generated_at_utc: {report.generated_at_utc}")
    lines.append(f"output_dir: {report.config.output_dir}")
    lines.append(f"run_mode: {report.config.run_mode}")
    if report.config.env_overrides:
        lines.append(
            "env_overrides: "
            + ", ".join(
                f"{override.key}={override.value}"
                for override in report.config.env_overrides
            )
        )
    lines.append(
        f"totals: total={report.totals.total_runs} "
        f"passed={report.totals.passed_runs} "
        f"failed={report.totals.failed_runs} "
        f"timed_out={report.totals.timed_out_runs}"
    )
    lines.append("pattern_totals:")
    for pattern in report.pattern_totals:
        lines.append(f"  - {pattern.label}: {pattern.total_count}")
    lines.append("runs:")
    for run in report.runs:
        status = "PASS" if run.passed else "FAIL"
        if run.timed_out:
            status = "TIMEOUT"
        lines.append(
            f"  - run={run.run_index:03d} cycle={run.cycle_index:02d} status={status} "
            f"duration={run.duration_seconds:.2f}s label={run.label}"
        )
        lines.append(f"    targets={','.join(run.targets)}")
        lines.append(f"    log={run.log_path}")
        if run.short_failure_line:
            lines.append(f"    failure={run.short_failure_line}")
        if run.pytest_failed_lines:
            lines.append(f"    pytest_failed={run.pytest_failed_lines[0]}")
    text_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_run_specs(config: HarnessConfig) -> tuple[RunSpec, ...]:
    specs: list[RunSpec] = []
    run_index: RunOrdinal = 1
    for cycle_index in range(1, config.repeat_count + 1):
        if config.run_mode == "single":
            for nodeid in config.tests:
                specs.append(
                    RunSpec(
                        run_index=run_index,
                        cycle_index=cycle_index,
                        label=nodeid,
                        targets=(nodeid,),
                        timeout_seconds=config.timeout_seconds,
                    )
                )
                run_index += 1
            continue

        if config.run_mode == "batch":
            specs.append(
                RunSpec(
                    run_index=run_index,
                    cycle_index=cycle_index,
                    label=f"batch_cycle_{cycle_index}",
                    targets=config.tests,
                    timeout_seconds=config.timeout_seconds,
                )
            )
            run_index += 1
            continue

        raise ValueError(f"Unsupported run_mode: {config.run_mode}")
    return tuple(specs)


def _parse_args() -> HarnessConfig:
    parser = argparse.ArgumentParser(
        description="Reusable deep-dive pytest evidence harness"
    )
    parser.add_argument(
        "--manifest",
        default=None,
        help="Path to a JSON manifest file describing the run configuration",
    )
    parser.add_argument(
        "--test",
        action="append",
        default=[],
        help="Pytest node id (repeat this flag for multiple tests)",
    )
    parser.add_argument(
        "--preset",
        choices=sorted(PRESET_TESTS.keys()),
        default=None,
        help="Named test sequence preset",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=DEFAULT_REPEAT_COUNT,
        help="How many cycles to run over the full test list",
    )
    parser.add_argument(
        "--run-mode",
        choices=sorted(RUN_MODES),
        default=DEFAULT_RUN_MODE,
        help="Execution mode: single (each test separately) or batch (all tests together per cycle)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Per-run timeout in seconds",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for logs and reports",
    )
    parser.add_argument(
        "--pytest-arg",
        action="append",
        default=[],
        help="Extra pytest argument (repeatable)",
    )
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        help="Environment override KEY=VALUE (repeatable)",
    )
    parser.add_argument(
        "--stop-on-failure",
        action="store_true",
        help="Stop execution after first failing or timed-out run",
    )
    args = parser.parse_args()

    manifest_path: ManifestPath | None = (
        str(args.manifest).strip() if args.manifest is not None else None
    )
    if manifest_path:
        has_cli_tests = bool(args.test or args.preset)
        has_repeat_override = int(args.repeat) != DEFAULT_REPEAT_COUNT
        has_timeout_override = float(args.timeout) != DEFAULT_TIMEOUT_SECONDS
        has_output_override = str(args.output_dir) != DEFAULT_OUTPUT_DIR
        has_run_mode_override = str(args.run_mode) != DEFAULT_RUN_MODE
        has_pytest_args = bool(args.pytest_arg)
        has_env_args = bool(args.env)
        if (
            has_cli_tests
            or has_repeat_override
            or has_timeout_override
            or has_output_override
            or has_run_mode_override
            or has_pytest_args
            or has_env_args
            or bool(args.stop_on_failure)
        ):
            raise ValueError(
                "--manifest cannot be combined with --test/--preset/--repeat/--timeout/"
                "--output-dir/--run-mode/--pytest-arg/--env/--stop-on-failure"
            )
        return _config_from_manifest(Path(manifest_path).resolve())

    selected_tests = _resolve_tests(
        preset=args.preset,
        tests=tuple(str(nodeid) for nodeid in args.test),
        source_label="CLI",
    )
    repeat_count = max(1, int(args.repeat))
    timeout_seconds = max(1.0, float(args.timeout))
    output_dir = Path(str(args.output_dir)).resolve()
    pytest_args = tuple(str(value) for value in args.pytest_arg)
    env_overrides = _parse_env_entries(tuple(str(value) for value in args.env))

    return HarnessConfig(
        tests=selected_tests,
        run_mode=str(args.run_mode),
        repeat_count=repeat_count,
        timeout_seconds=timeout_seconds,
        output_dir=output_dir,
        pytest_args=pytest_args,
        env_overrides=env_overrides,
        stop_on_failure=bool(args.stop_on_failure),
        preset=args.preset,
    )


def main() -> int:
    try:
        config = _parse_args()
    except ValueError as exc:
        print(f"Argument error: {exc}")
        return 2

    session_dir = config.output_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    session_dir.mkdir(parents=True, exist_ok=True)

    print("Pytest evidence harness")
    print(f"session_dir={session_dir}")
    print(f"tests={len(config.tests)} repeat={config.repeat_count}")
    print(f"run_mode={config.run_mode}")
    print(f"timeout_seconds={config.timeout_seconds}")
    if config.env_overrides:
        print(
            "env_overrides="
            + ",".join(
                f"{override.key}={override.value}" for override in config.env_overrides
            )
        )

    run_specs = _build_run_specs(config)
    run_results: list[RunResult] = []

    for spec in run_specs:
        print(
            f"[run {spec.run_index:03d}] cycle={spec.cycle_index} "
            f"label={spec.label} targets={len(spec.targets)}"
        )
        result = _run_single(spec, config, session_dir)
        run_results.append(result)
        status = "PASS" if result.passed else "FAIL"
        if result.timed_out:
            status = "TIMEOUT"
        print(
            f"[run {spec.run_index:03d}] status={status} "
            f"exit={result.exit_code} duration={result.duration_seconds:.2f}s"
        )
        if result.short_failure_line:
            print(f"[run {spec.run_index:03d}] failure={result.short_failure_line}")
        print(f"[run {spec.run_index:03d}] log={result.log_path}")
        if config.stop_on_failure and not result.passed:
            print("Stopping early due to --stop-on-failure")
            break

    report = _aggregate_results(config, tuple(run_results))
    _write_report_files(report, session_dir)
    print(f"report_json={session_dir / 'evidence_report.json'}")
    print(f"report_text={session_dir / 'evidence_report.txt'}")
    print(
        "totals: "
        f"total={report.totals.total_runs} "
        f"passed={report.totals.passed_runs} "
        f"failed={report.totals.failed_runs} "
        f"timed_out={report.totals.timed_out_runs}"
    )
    return 0 if report.totals.failed_runs == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
