#!/usr/bin/env python3
"""Run pytest from a manifest with stall detection and stack sampling evidence."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import signal
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

type Pid = int
type NodeId = str
type Seconds = float
type RunMode = str
type EnvKey = str
type EnvValue = str


@dataclass(frozen=True, slots=True)
class EnvOverride:
    key: EnvKey
    value: EnvValue


@dataclass(frozen=True, slots=True)
class ManifestConfig:
    run_full_suite: bool
    tests: tuple[NodeId, ...]
    pytest_args: tuple[str, ...]
    timeout_seconds: Seconds
    run_mode: RunMode
    env_overrides: tuple[EnvOverride, ...]


@dataclass(frozen=True, slots=True)
class WatchdogConfig:
    manifest_path: str
    output_dir: str
    stall_seconds: Seconds
    poll_interval_seconds: Seconds
    sample_seconds: Seconds
    timeout_seconds: Seconds | None


@dataclass(frozen=True, slots=True)
class WorkerCpuSnapshot:
    pid: Pid
    ppid: Pid
    cpu_percent: float
    command: str


@dataclass(frozen=True, slots=True)
class StallCapture:
    captured_at_utc: str
    stalled_for_seconds: Seconds
    log_path: str
    ps_snapshot_path: str
    sample_path: str | None
    hottest_worker_pid: Pid | None
    worker_cpu_snapshots: tuple[WorkerCpuSnapshot, ...]


@dataclass(frozen=True, slots=True)
class RunReport:
    generated_at_utc: str
    config: WatchdogConfig
    command: tuple[str, ...]
    log_path: str
    exit_code: int | None
    timed_out: bool
    stalled: bool
    duration_seconds: Seconds
    stall_capture: StallCapture | None


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _parse_args() -> WatchdogConfig:
    parser = argparse.ArgumentParser(
        description="Run pytest with watchdog and capture stall evidence"
    )
    parser.add_argument("--manifest", required=True, help="Path to manifest JSON")
    parser.add_argument(
        "--output-dir",
        default="artifacts/debug/pytest_stall_watchdog",
        help="Base output directory for watchdog artifacts",
    )
    parser.add_argument(
        "--stall-seconds",
        type=float,
        default=90.0,
        help="No-log-growth threshold before capturing stall evidence",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=5.0,
        help="Polling interval for liveness checks",
    )
    parser.add_argument(
        "--sample-seconds",
        type=float,
        default=5.0,
        help="Duration passed to /usr/bin/sample for worker stack capture",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=0.0,
        help="Optional hard timeout for entire pytest run (0 disables)",
    )
    args = parser.parse_args()
    timeout_value = float(args.timeout_seconds)
    return WatchdogConfig(
        manifest_path=str(Path(str(args.manifest)).resolve()),
        output_dir=str(Path(str(args.output_dir)).resolve()),
        stall_seconds=max(10.0, float(args.stall_seconds)),
        poll_interval_seconds=max(0.5, float(args.poll_interval_seconds)),
        sample_seconds=max(1.0, float(args.sample_seconds)),
        timeout_seconds=None if timeout_value <= 0.0 else timeout_value,
    )


def _parse_env_overrides(raw_entries: object) -> tuple[EnvOverride, ...]:
    if raw_entries is None:
        return tuple()
    if not isinstance(raw_entries, list):
        raise ValueError("Manifest 'env_overrides' must be a list of KEY=VALUE strings")
    overrides: list[EnvOverride] = []
    for raw_entry in raw_entries:
        entry = str(raw_entry).strip()
        if not entry:
            continue
        if "=" not in entry:
            raise ValueError(f"Invalid env_overrides entry (need KEY=VALUE): {entry}")
        key, value = entry.split("=", 1)
        clean_key = key.strip()
        if not clean_key:
            raise ValueError(f"Invalid env_overrides entry (empty key): {entry}")
        overrides.append(EnvOverride(key=clean_key, value=value))
    return tuple(overrides)


def _load_manifest(path: Path) -> ManifestConfig:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Manifest must be a JSON object")

    run_full_suite = bool(payload.get("run_full_suite", False))
    raw_tests = payload.get("tests", [])
    if not isinstance(raw_tests, list):
        raise ValueError("Manifest 'tests' must be a list")
    tests = tuple(str(nodeid) for nodeid in raw_tests if str(nodeid).strip())
    if not run_full_suite and not tests:
        raise ValueError("Manifest includes no tests and run_full_suite is false")

    raw_pytest_args = payload.get("pytest_args", [])
    if raw_pytest_args is None:
        raw_pytest_args = []
    if not isinstance(raw_pytest_args, list):
        raise ValueError("Manifest 'pytest_args' must be a list")
    pytest_args = tuple(str(value) for value in raw_pytest_args)

    raw_timeout = payload.get("timeout_seconds", 0.0)
    timeout_seconds = max(0.0, float(raw_timeout))

    raw_mode = payload.get("run_mode", "batch")
    run_mode = str(raw_mode).strip() or "batch"
    if run_mode not in {"batch", "single"}:
        raise ValueError("Manifest run_mode must be batch or single")
    env_overrides = _parse_env_overrides(payload.get("env_overrides", []))

    return ManifestConfig(
        run_full_suite=run_full_suite,
        tests=tests,
        pytest_args=pytest_args,
        timeout_seconds=timeout_seconds,
        run_mode=run_mode,
        env_overrides=env_overrides,
    )


def _build_command(manifest: ManifestConfig) -> tuple[str, ...]:
    if manifest.run_mode != "batch":
        raise ValueError("Watchdog currently supports only batch manifests")
    if manifest.run_full_suite:
        return ("uv", "run", "pytest", "-q", "-s", *manifest.pytest_args)
    return ("uv", "run", "pytest", "-q", "-s", *manifest.pytest_args, *manifest.tests)


def _read_process_table() -> tuple[tuple[Pid, Pid], ...]:
    table_raw = subprocess.run(
        ("ps", "-axo", "pid=,ppid="),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    rows: list[tuple[Pid, Pid]] = []
    for line in table_raw.stdout.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            rows.append((int(parts[0]), int(parts[1])))
        except ValueError:
            continue
    return tuple(rows)


def _descendants(root_pid: Pid) -> tuple[Pid, ...]:
    table = _read_process_table()
    children_by_parent: dict[Pid, list[Pid]] = {}
    for pid, ppid in table:
        children_by_parent.setdefault(ppid, []).append(pid)

    seen: set[Pid] = {root_pid}
    pending: list[Pid] = [root_pid]
    while pending:
        parent = pending.pop()
        for child in children_by_parent.get(parent, []):
            if child in seen:
                continue
            seen.add(child)
            pending.append(child)
    return tuple(sorted(seen))


def _cpu_snapshot(pids: tuple[Pid, ...]) -> tuple[WorkerCpuSnapshot, ...]:
    if not pids:
        return tuple()
    pid_arg = ",".join(str(pid) for pid in pids)
    ps_raw = subprocess.run(
        ("ps", "-p", pid_arg, "-o", "pid=,ppid=,%cpu=,command="),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    snapshots: list[WorkerCpuSnapshot] = []
    for line in ps_raw.stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        parts = stripped.split(maxsplit=3)
        if len(parts) < 4:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
            cpu = float(parts[2])
        except ValueError:
            continue
        command = parts[3]
        snapshots.append(
            WorkerCpuSnapshot(
                pid=pid,
                ppid=ppid,
                cpu_percent=cpu,
                command=command,
            )
        )
    return tuple(snapshots)


def _write_ps_snapshot(path: Path) -> None:
    ps_snapshot = subprocess.run(
        ("ps", "-axo", "pid,ppid,%cpu,%mem,state,etime,command"),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    path.write_text(ps_snapshot.stdout, encoding="utf-8")


def _sample_process(pid: Pid, sample_seconds: Seconds, output_path: Path) -> bool:
    sample_cmd = (
        "/usr/bin/sample",
        str(pid),
        str(int(sample_seconds)),
        "-file",
        str(output_path),
    )
    run = subprocess.run(
        sample_cmd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if run.returncode == 0 and output_path.exists():
        return True
    output_path.write_text(run.stdout, encoding="utf-8")
    return False


def _terminate_group(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except Exception:
        pass
    try:
        process.wait(timeout=5.0)
        return
    except subprocess.TimeoutExpired:
        pass
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except Exception:
        pass


def _run_watchdog(config: WatchdogConfig, manifest: ManifestConfig) -> RunReport:
    session_dir = Path(config.output_dir) / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    session_dir.mkdir(parents=True, exist_ok=True)

    command = _build_command(manifest)
    log_path = session_dir / "pytest_watchdog.log"
    report_path = session_dir / "watchdog_report.json"

    header = [
        f"# started_at_utc: {_utc_now_iso()}",
        f"# manifest: {config.manifest_path}",
        f"# command: {' '.join(shlex.quote(part) for part in command)}",
        "",
    ]
    if manifest.env_overrides:
        for override in manifest.env_overrides:
            header.append(f"# env_override: {override.key}={override.value}")
        header.append("")

    last_growth_mono = time.monotonic()
    started_mono = last_growth_mono
    last_size = 0
    stalled = False
    timed_out = False
    stall_capture: StallCapture | None = None

    with log_path.open("w", encoding="utf-8") as log_handle:
        log_handle.write("\n".join(header))
        log_handle.flush()

        process = subprocess.Popen(
            command,
            cwd=_repo_root(),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
            env={
                **os.environ,
                **{override.key: override.value for override in manifest.env_overrides},
            },
        )

        while True:
            return_code = process.poll()
            now_mono = time.monotonic()

            current_size = log_path.stat().st_size if log_path.exists() else 0
            if current_size > last_size:
                last_growth_mono = now_mono
                last_size = current_size

            if return_code is not None:
                break

            if config.timeout_seconds is not None:
                if (now_mono - started_mono) >= config.timeout_seconds:
                    timed_out = True
                    _terminate_group(process)
                    break

            stalled_for = now_mono - last_growth_mono
            if stalled_for >= config.stall_seconds:
                pids = _descendants(process.pid)
                snapshots = _cpu_snapshot(pids)
                hottest_pid: Pid | None = None
                hottest_cpu = -1.0
                for snapshot in snapshots:
                    if "python" not in snapshot.command:
                        continue
                    if snapshot.cpu_percent > hottest_cpu:
                        hottest_cpu = snapshot.cpu_percent
                        hottest_pid = snapshot.pid

                ps_snapshot_path = session_dir / "stall_ps_snapshot.txt"
                _write_ps_snapshot(ps_snapshot_path)

                sample_path: Path | None = None
                if hottest_pid is not None:
                    sample_path = session_dir / f"stall_sample_pid_{hottest_pid}.txt"
                    _sample_process(hottest_pid, config.sample_seconds, sample_path)

                stalled = True
                stall_capture = StallCapture(
                    captured_at_utc=_utc_now_iso(),
                    stalled_for_seconds=stalled_for,
                    log_path=str(log_path),
                    ps_snapshot_path=str(ps_snapshot_path),
                    sample_path=str(sample_path) if sample_path is not None else None,
                    hottest_worker_pid=hottest_pid,
                    worker_cpu_snapshots=snapshots,
                )
                _terminate_group(process)
                break

            time.sleep(config.poll_interval_seconds)

        exit_code = process.returncode

    duration = time.monotonic() - started_mono
    report = RunReport(
        generated_at_utc=_utc_now_iso(),
        config=config,
        command=command,
        log_path=str(log_path),
        exit_code=exit_code,
        timed_out=timed_out,
        stalled=stalled,
        duration_seconds=duration,
        stall_capture=stall_capture,
    )
    report_path.write_text(
        json.dumps(asdict(report), indent=2, sort_keys=True), encoding="utf-8"
    )

    print(f"session_dir={session_dir}")
    print(f"log_path={log_path}")
    print(f"report_path={report_path}")
    print(f"exit_code={exit_code} timed_out={timed_out} stalled={stalled}")
    if stall_capture is not None:
        print(f"stall_ps_snapshot={stall_capture.ps_snapshot_path}")
        if stall_capture.sample_path is not None:
            print(f"stall_sample={stall_capture.sample_path}")
    return report


def main() -> int:
    config = _parse_args()
    manifest = _load_manifest(Path(config.manifest_path))
    report = _run_watchdog(config, manifest)
    if report.stalled or report.timed_out:
        return 2
    if report.exit_code is None:
        return 2
    return int(report.exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
