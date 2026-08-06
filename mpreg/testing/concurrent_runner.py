"""Encapsulated high-concurrency pytest runner.

Raises open-file limits, runs pytest-xdist, and attaches a HangWatchdog that
profiles stalled workers with py-spy — all in Python, no shell scripts.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from mpreg.testing.hang_observe import (
    DEFAULT_PROFILE_DIR,
    DEFAULT_STATE_DIR,
    HangProfiler,
    HangStateDir,
    HangWatchdog,
    enable_faulthandler,
)
from mpreg.testing.resource_limits import NoFileLimit, raise_open_file_limit

@dataclass(slots=True)
class ConcurrentSuiteResult:
    exit_code: int
    log_path: Path
    junit_path: Path
    duration_seconds: float
    open_files: NoFileLimit
    stall_dumps: list[Path] = field(default_factory=list)
    summary_line: str = ""

@dataclass(slots=True)
class ConcurrentSuiteRunner:
    """Run the full test suite under pytest-xdist with hang observability."""

    workers: int = 16
    root: Path = field(default_factory=lambda: Path.cwd())
    log_path: Path | None = None
    junit_path: Path | None = None
    state_dir: Path = field(default_factory=lambda: DEFAULT_STATE_DIR)
    profile_dir: Path = field(default_factory=lambda: DEFAULT_PROFILE_DIR)
    stall_seconds: float = 90.0
    open_file_target: int = 1_048_576
    extra_pytest_args: list[str] = field(default_factory=list)
    use_sudo_for_pyspy: bool = True

    def run(self) -> ConcurrentSuiteResult:
        root = self.root.resolve()
        workers = self.workers
        log_path = self.log_path or (root / ".local" / f"full-suite-n{workers}.log")
        junit_path = self.junit_path or (root / ".local" / f"full-suite-n{workers}.xml")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        junit_path.parent.mkdir(parents=True, exist_ok=True)
        self.profile_dir.mkdir(parents=True, exist_ok=True)

        limits = raise_open_file_limit(self.open_file_target)
        enable_faulthandler()

        state = HangStateDir(self.state_dir)
        state.clear()
        os.environ["MPREG_TEST_STATE_DIR"] = str(state.root.resolve())
        os.environ["PYTHONFAULTHANDLER"] = "1"

        cmd = [
            "uv",
            "run",
            "pytest",
            "-n",
            str(workers),
            "-q",
            "--tb=line",
            f"--junitxml={junit_path}",
            *self.extra_pytest_args,
        ]

        started = time.monotonic()
        stall_dumps: list[Path] = []
        summary = ""

        with log_path.open("w", encoding="utf-8") as log_f:
            log_f.write(
                f"# concurrent suite workers={workers} ulimit_nofile={limits.soft}/{limits.hard}\n"
            )
            log_f.flush()
            env = os.environ.copy()
            proc = subprocess.Popen(
                cmd,
                cwd=str(root),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
            )

            assert proc.stdout is not None

            # Mirror pytest output into log + this process stdout
            def _pump() -> None:
                for raw in iter(proc.stdout.readline, b""):
                    text = raw.decode("utf-8", errors="replace")
                    log_f.write(text)
                    log_f.flush()
                    sys.stdout.write(text)
                    sys.stdout.flush()

            pump = threading.Thread(target=_pump, name="pytest-pump", daemon=True)
            pump.start()

            watchdog = HangWatchdog(
                progress_path=log_path,
                state=state,
                profiler=HangProfiler(
                    profile_dir=self.profile_dir,
                    use_sudo_for_pyspy=self.use_sudo_for_pyspy,
                ),
                stall_seconds=self.stall_seconds,
                controller_pid=proc.pid,
            )
            # Watchdog in a thread so we can join pump cleanly
            dump_box: list[Path] = []

            def _watch() -> None:
                dump_box.extend(watchdog.run_until(proc))

            watcher = threading.Thread(target=_watch, name="hang-watchdog", daemon=True)
            watcher.start()

            rc = proc.wait()
            pump.join(timeout=30)
            watcher.join(timeout=self.stall_seconds + 30)
            stall_dumps = list(dump_box)

        duration = time.monotonic() - started
        try:
            tail = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            for line in reversed(tail):
                if "passed" in line or "failed" in line or "error" in line:
                    summary = line.strip()
                    break
        except OSError:
            summary = ""

        return ConcurrentSuiteResult(
            exit_code=int(rc),
            log_path=log_path,
            junit_path=junit_path,
            duration_seconds=duration,
            open_files=limits,
            stall_dumps=stall_dumps,
            summary_line=summary,
        )

def main(argv: list[str] | None = None) -> int:
    """CLI: ``python -m mpreg.testing.concurrent_runner``."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run MPREG tests under high concurrency with hang profiling."
    )
    parser.add_argument("-n", "--workers", type=int, default=16)
    parser.add_argument("--stall-seconds", type=float, default=90.0)
    parser.add_argument("--open-files", type=int, default=1_048_576)
    parser.add_argument("--log", type=Path, default=None)
    parser.add_argument("--junit", type=Path, default=None)
    parser.add_argument("--profile-dir", type=Path, default=DEFAULT_PROFILE_DIR)
    parser.add_argument("--no-sudo-pyspy", action="store_true")
    parser.add_argument("pytest_args", nargs="*", help="Extra args passed to pytest")
    args = parser.parse_args(argv)

    runner = ConcurrentSuiteRunner(
        workers=args.workers,
        log_path=args.log,
        junit_path=args.junit,
        profile_dir=args.profile_dir,
        stall_seconds=args.stall_seconds,
        open_file_target=args.open_files,
        extra_pytest_args=list(args.pytest_args),
        use_sudo_for_pyspy=not args.no_sudo_pyspy,
    )
    result = runner.run()
    print(
        f"\n# done exit={result.exit_code} duration={result.duration_seconds:.1f}s "
        f"nofile={result.open_files.soft}/{result.open_files.hard} "
        f"stalls={len(result.stall_dumps)}"
    )
    if result.summary_line:
        print(result.summary_line)
    for dump in result.stall_dumps:
        print(f"# stall dump: {dump}")
    return result.exit_code

if __name__ == "__main__":
    raise SystemExit(main())
