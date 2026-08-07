"""Hang observability for concurrent pytest runs.

Workers write breadcrumbs (pid + current nodeid). A HangWatchdog monitors
pytest progress and, on stall, dumps Python stacks via py-spy (sudo on macOS)
plus native samples — no shell scripts required.
"""

from __future__ import annotations

import contextlib
import faulthandler
import os
import signal
import subprocess
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

DEFAULT_STATE_DIR = Path(".local/test-state")
DEFAULT_PROFILE_DIR = Path(".local/hang-profile")
DEFAULT_PYSPY = Path(
    os.environ.get(
        "MPREG_PYSPY",
        str(Path.home() / ".pyenv/versions/3.12.2/bin/py-spy"),
    )
)


@dataclass(frozen=True, slots=True)
class WorkerBreadcrumb:
    worker: str
    pid: int | None
    nodeid: str | None
    path: Path


class HangStateDir:
    """Per-worker current-test breadcrumbs for hang correlation."""

    def __init__(self, root: Path | None = None) -> None:
        env = os.environ.get("MPREG_TEST_STATE_DIR")
        self.root = Path(env) if env else (root or DEFAULT_STATE_DIR)

    def ensure(self) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        return self.root

    def clear(self) -> None:
        self.ensure()
        for path in self.root.glob("*.current"):
            path.unlink(missing_ok=True)
        for path in self.root.glob("*.pid"):
            path.unlink(missing_ok=True)

    @property
    def worker_id(self) -> str:
        return os.environ.get("PYTEST_XDIST_WORKER", "gw-main")

    def write_pid(self, pid: int | None = None) -> Path:
        self.ensure()
        path = self.root / f"{self.worker_id}.pid"
        path.write_text(str(pid if pid is not None else os.getpid()), encoding="utf-8")
        return path

    def write_current(self, nodeid: str, pid: int | None = None) -> Path:
        self.ensure()
        path = self.root / f"{self.worker_id}.current"
        path.write_text(
            f"pid={pid if pid is not None else os.getpid()}\nnodeid={nodeid}\n",
            encoding="utf-8",
        )
        return path

    def clear_current(self) -> None:
        path = self.root / f"{self.worker_id}.current"
        path.unlink(missing_ok=True)

    def breadcrumbs(self) -> list[WorkerBreadcrumb]:
        if not self.root.exists():
            return []
        out: list[WorkerBreadcrumb] = []
        for path in sorted(self.root.glob("*.current")):
            worker = path.stem
            pid: int | None = None
            nodeid: str | None = None
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                continue
            for line in text.splitlines():
                if line.startswith("pid="):
                    try:
                        pid = int(line.split("=", 1)[1].strip())
                    except ValueError:
                        pid = None
                elif line.startswith("nodeid="):
                    nodeid = line.split("=", 1)[1].strip() or None
            out.append(
                WorkerBreadcrumb(worker=worker, pid=pid, nodeid=nodeid, path=path)
            )
        return out

    def worker_pids(self) -> list[int]:
        pids: list[int] = []
        if not self.root.exists():
            return pids
        for path in self.root.glob("*.pid"):
            try:
                pids.append(int(path.read_text(encoding="utf-8").strip()))
            except OSError, ValueError:
                continue
        for crumb in self.breadcrumbs():
            if crumb.pid is not None:
                pids.append(crumb.pid)
        # unique preserve order
        seen: set[int] = set()
        uniq: list[int] = []
        for pid in pids:
            if pid in seen:
                continue
            seen.add(pid)
            uniq.append(pid)
        return uniq


def enable_faulthandler(stream: TextIO | None = None) -> None:
    """Enable all-thread dumps; register SIGUSR1 for on-demand stacks.

    On SIGUSR1, stacks go to both stderr and ``{state}/gwN.stack`` so a
    HangProfiler can collect Python frames without py-spy (needed on 3.14+
    until py-spy supports that interpreter).
    """
    target = stream or sys.stderr
    try:
        faulthandler.enable(file=target, all_threads=True)
    except Exception:
        return
    if not hasattr(signal, "SIGUSR1"):
        return

    state = HangStateDir()

    def _dump_to_state(signum: int, frame: object) -> None:
        try:
            state.ensure()
            stack_path = state.root / f"{state.worker_id}.stack"
            with stack_path.open("w", encoding="utf-8") as fh:
                fh.write(f"pid={os.getpid()} worker={state.worker_id}\n")
                faulthandler.dump_traceback(file=fh, all_threads=True)
            # Also mirror to stderr for live suite logs.
            faulthandler.dump_traceback(file=target, all_threads=True)
        except Exception:
            with contextlib.suppress(Exception):
                faulthandler.dump_traceback(file=target, all_threads=True)

    try:
        signal.signal(signal.SIGUSR1, _dump_to_state)
    except Exception:
        with contextlib.suppress(Exception):
            faulthandler.register(signal.SIGUSR1, file=target, all_threads=True)


def install_pytest_hang_hooks() -> None:
    """Side-effect import target: hooks live in tests/conftest via thin wrappers."""
    enable_faulthandler()
    HangStateDir().write_pid()


@dataclass(slots=True)
class ProcessCpuSnapshot:
    pid: int
    cpu_percent: float
    rss_kb: int
    command: str = ""


class ProcessSampler:
    """Minimal process listing without shelling out to complex pipelines."""

    @staticmethod
    def snapshot(pids: Iterable[int]) -> list[ProcessCpuSnapshot]:
        out: list[ProcessCpuSnapshot] = []
        for pid in pids:
            try:
                completed = subprocess.run(
                    ["ps", "-o", "pid=,pcpu=,rss=,command=", "-p", str(pid)],
                    check=False,
                    capture_output=True,
                    text=True,
                )
            except OSError:
                continue
            line = (completed.stdout or "").strip()
            if not line:
                continue
            parts = line.split(None, 3)
            if len(parts) < 3:
                continue
            try:
                out.append(
                    ProcessCpuSnapshot(
                        pid=int(parts[0]),
                        cpu_percent=float(parts[1]),
                        rss_kb=int(parts[2]),
                        command=parts[3] if len(parts) > 3 else "",
                    )
                )
            except ValueError:
                continue
        out.sort(key=lambda s: s.cpu_percent, reverse=True)
        return out

    @staticmethod
    def children_of(pid: int) -> list[int]:
        try:
            completed = subprocess.run(
                ["pgrep", "-P", str(pid)],
                check=False,
                capture_output=True,
                text=True,
            )
        except OSError:
            return []
        pids: list[int] = []
        for line in (completed.stdout or "").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                pids.append(int(line))
            except ValueError:
                continue
        return pids


@dataclass(slots=True)
class HangProfiler:
    """Dump Python + native stacks for suspected hung workers."""

    profile_dir: Path = field(default_factory=lambda: DEFAULT_PROFILE_DIR)
    pyspy_path: Path = field(default_factory=lambda: DEFAULT_PYSPY)
    use_sudo_for_pyspy: bool = True

    def dump(
        self,
        *,
        label: str,
        pids: list[int],
        breadcrumbs: list[WorkerBreadcrumb] | None = None,
        top_n: int = 6,
        record_hot_seconds: float = 5.0,
        hot_cpu_threshold: float = 40.0,
    ) -> Path:
        stamp = time.strftime("%Y%m%d-%H%M%S")
        out = self.profile_dir / f"{label}-{stamp}"
        out.mkdir(parents=True, exist_ok=True)
        log_path = out / "watchdog.log"
        lines: list[str] = [f"hang dump at {stamp}", f"pids_in={pids}"]
        if breadcrumbs:
            lines.append("breadcrumbs:")
            for crumb in breadcrumbs:
                lines.append(f"  {crumb.worker} pid={crumb.pid} nodeid={crumb.nodeid}")
                with contextlib.suppress(OSError):
                    (out / f"{crumb.worker}.current").write_text(
                        crumb.path.read_text(encoding="utf-8"), encoding="utf-8"
                    )

        snaps = ProcessSampler.snapshot(pids)[:top_n]
        lines.append("ranked:")
        state = HangStateDir()
        for snap in snaps:
            lines.append(
                f"  pid={snap.pid} cpu={snap.cpu_percent} rss_kb={snap.rss_kb} cmd={snap.command[:120]}"
            )
            self._profile_one(
                out,
                snap,
                record_hot_seconds=record_hot_seconds,
                hot_cpu_threshold=hot_cpu_threshold,
                lines=lines,
                state=state,
            )

        # Collect any remaining stack breadcrumbs
        for stack_path in state.root.glob("*.stack"):
            dest = out / stack_path.name
            try:
                if not dest.exists():
                    dest.write_text(
                        stack_path.read_text(encoding="utf-8", errors="replace"),
                        encoding="utf-8",
                    )
            except OSError:
                pass

        log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return out

    def _profile_one(
        self,
        out: Path,
        snap: ProcessCpuSnapshot,
        *,
        record_hot_seconds: float,
        hot_cpu_threshold: float,
        lines: list[str],
        state: HangStateDir | None = None,
    ) -> None:
        pid = snap.pid
        # Primary path on Python 3.14+: faulthandler via SIGUSR1 → state/*.stack
        try:
            os.kill(pid, signal.SIGUSR1)
            lines.append(f"  sent SIGUSR1 to {pid}")
            time.sleep(0.35)  # allow handler to flush .stack
        except OSError as exc:
            lines.append(f"  SIGUSR1 {pid} failed: {exc}")

        if state is not None:
            for stack_path in state.root.glob("*.stack"):
                try:
                    text = stack_path.read_text(encoding="utf-8", errors="replace")
                except OSError:
                    continue
                if f"pid={pid}" not in text and f"pid={pid}\n" not in text:
                    # still copy all stacks once; match by mtime affinity below
                    pass
                dest = out / f"faulthandler-{stack_path.stem}-{pid}.txt"
                # Prefer stacks that mention this pid
                if f"pid={pid}" in text.splitlines()[0:3] or f"pid={pid}" in text[:80]:
                    dest = out / f"faulthandler-{pid}.txt"
                    dest.write_text(text, encoding="utf-8")
                    lines.append(
                        f"  faulthandler stack → {dest.name} ({len(text)} bytes)"
                    )

        # Optional py-spy (unsupported on CPython 3.14 as of py-spy 0.4.0)
        if self.pyspy_path.is_file():
            dump_path = out / f"pyspy-{pid}.txt"
            cmd = [str(self.pyspy_path), "dump", "--pid", str(pid)]
            if self.use_sudo_for_pyspy:
                cmd = ["sudo", "-n", *cmd]
            rc = self._run(cmd, dump_path, out / f"pyspy-{pid}.err")
            err = ""
            with contextlib.suppress(OSError):
                err = (out / f"pyspy-{pid}.err").read_text(encoding="utf-8")[:200]
            lines.append(f"  py-spy dump pid={pid} rc={rc} {err!r}")
            if (
                rc == 0
                and snap.cpu_percent >= hot_cpu_threshold
                and record_hot_seconds > 0
            ):
                svg = out / f"pyspy-{pid}.svg"
                rec = [
                    str(self.pyspy_path),
                    "record",
                    "-d",
                    str(int(record_hot_seconds)),
                    "-o",
                    str(svg),
                    "--pid",
                    str(pid),
                ]
                if self.use_sudo_for_pyspy:
                    rec = ["sudo", "-n", *rec]
                rc = self._run(
                    rec,
                    out / f"pyspy-record-{pid}.out",
                    out / f"pyspy-record-{pid}.err",
                )
                lines.append(f"  py-spy record pid={pid} rc={rc}")

        sample_out = out / f"sample-{pid}.txt"
        self._run(
            ["sample", str(pid), "2", "-file", str(sample_out)],
            out / f"sample-{pid}.run.out",
            out / f"sample-{pid}.run.err",
        )
        lsof_out = out / f"lsof-{pid}.txt"
        try:
            completed = subprocess.run(
                ["lsof", "-nP", "-p", str(pid)],
                check=False,
                capture_output=True,
                text=True,
            )
            tcp = [
                ln
                for ln in (completed.stdout or "").splitlines()
                if "TCP" in ln or "UDP" in ln
            ]
            lsof_out.write_text("\n".join(tcp[:80]) + "\n", encoding="utf-8")
        except OSError as exc:
            lines.append(f"  lsof failed: {exc}")

    @staticmethod
    def _run(cmd: list[str], stdout_path: Path, stderr_path: Path) -> int:
        try:
            completed = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
            )
        except OSError as exc:
            stderr_path.write_text(str(exc), encoding="utf-8")
            return 127
        stdout_path.write_text(completed.stdout or "", encoding="utf-8")
        stderr_path.write_text(completed.stderr or "", encoding="utf-8")
        return int(completed.returncode)


@dataclass(slots=True)
class HangWatchdog:
    """Poll a progress artifact; on stall, run HangProfiler."""

    progress_path: Path
    state: HangStateDir
    profiler: HangProfiler
    stall_seconds: float = 90.0
    poll_seconds: float = 15.0
    controller_pid: int | None = None

    def run_until(self, proc: subprocess.Popen[bytes]) -> list[Path]:
        """Block until ``proc`` exits; return paths of any stall dumps.

        Progress is log growth **or** breadcrumb nodeid changes. Long tests
        that keep updating ``*.current`` are not treated as hangs.
        """
        dumps: list[Path] = []
        last_size = -1
        last_crumbs = ""
        stall = 0.0
        while proc.poll() is None:
            time.sleep(self.poll_seconds)
            try:
                size = (
                    self.progress_path.stat().st_size
                    if self.progress_path.exists()
                    else 0
                )
            except OSError:
                size = 0
            crumbs = "|".join(
                f"{c.worker}:{c.nodeid}" for c in self.state.breadcrumbs()
            )
            if size > last_size or crumbs != last_crumbs:
                last_size = max(last_size, size)
                last_crumbs = crumbs
                stall = 0.0
                continue
            stall += self.poll_seconds
            if stall < self.stall_seconds:
                continue
            # Only dump if some worker still has a current test (else teardown).
            if not self.state.breadcrumbs():
                stall = 0.0
                continue
            pids = self._candidate_pids(proc.pid)
            dump = self.profiler.dump(
                label="stall",
                pids=pids,
                breadcrumbs=self.state.breadcrumbs(),
            )
            dumps.append(dump)
            stall = 0.0
            try:
                last_size = (
                    self.progress_path.stat().st_size
                    if self.progress_path.exists()
                    else 0
                )
            except OSError:
                last_size = 0
            last_crumbs = "|".join(
                f"{c.worker}:{c.nodeid}" for c in self.state.breadcrumbs()
            )
        return dumps

    def _candidate_pids(self, root_pid: int) -> list[int]:
        pids = list(self.state.worker_pids())
        if self.controller_pid:
            pids.extend(ProcessSampler.children_of(self.controller_pid))
            pids.append(self.controller_pid)
        pids.extend(ProcessSampler.children_of(root_pid))
        pids.append(root_pid)
        seen: set[int] = set()
        uniq: list[int] = []
        for pid in pids:
            if pid in seen:
                continue
            seen.add(pid)
            uniq.append(pid)
        return uniq
