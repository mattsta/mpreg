"""Shared runtime helpers for curriculum example apps."""

from __future__ import annotations

import asyncio
import sys
import time
import traceback
from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, TypeVar

from mpreg.core.config import MPREGSettings
from mpreg.examples.showcase_utils import ServerHandle, run_with_servers, start_servers, stop_servers
from mpreg.server import MPREGServer

T = TypeVar("T")

class ExampleFailed(RuntimeError):
    """Raised when an example assertion or invariant fails."""

def ensure(condition: bool, message: str) -> None:
    """Assert a demo invariant; raise ExampleFailed on failure.

    When inside :func:`app_run`, increments scenario ensure counters.
    """
    if _ACTIVE_STATS is not None:
        _ACTIVE_STATS.note_ensure()
    if not condition:
        raise ExampleFailed(message)

def banner(title: str, *, level: str = "", app_id: str = "") -> None:
    """Print a consistent section banner for CLI output."""
    bits = [p for p in (level, app_id, title) if p]
    line = " · ".join(bits) if bits else title
    print()
    print("=" * 64)
    print(line)
    print("=" * 64)

def step(msg: str) -> None:
    print(f"  → {msg}")

def ok(msg: str) -> None:
    print(f"  ✓ {msg}")

def feature(feature_id: str, detail: str = "") -> None:
    """Announce a catalog feature being exercised (see FEATURE_CATALOG.md)."""
    suffix = f" — {detail}" if detail else ""
    print(f"  ◆ feature:{feature_id}{suffix}")

@dataclass
class ScenarioStats:
    """Accumulated scenario/ensure counts for an app run."""

    scenarios: list[str] = field(default_factory=list)
    ensures: int = 0

    def note_ensure(self) -> None:
        self.ensures += 1

_ACTIVE_STATS: ScenarioStats | None = None

def ensure_counted(condition: bool, message: str) -> None:
    """Alias for :func:`ensure` (counters are built into ensure under app_run)."""
    ensure(condition, message)

@contextmanager
def scenario(name: str, *feature_ids: str) -> Iterator[None]:
    """Label a multi-step API drill inside an app.

    Usage::

        with scenario("idempotent create", "rpc.call", "cache.put_get"):
            ...
            ensure(...)
    """
    global _ACTIVE_STATS
    print()
    print(f"  ┌─ scenario: {name}")
    for fid in feature_ids:
        feature(fid)
    started = time.monotonic()
    if _ACTIVE_STATS is not None:
        _ACTIVE_STATS.scenarios.append(name)
    try:
        yield
    except Exception:
        print(f"  └─ scenario FAILED: {name}")
        raise
    else:
        elapsed = time.monotonic() - started
        print(f"  └─ scenario ok: {name} ({elapsed:.2f}s)")

@contextmanager
def app_run(app_id: str, title: str, *, level: str = "") -> Iterator[ScenarioStats]:
    """Top-level banner + scenario stats for a curriculum app."""
    global _ACTIVE_STATS
    stats = ScenarioStats()
    prev = _ACTIVE_STATS
    _ACTIVE_STATS = stats
    banner(title, level=level, app_id=app_id)
    try:
        yield stats
    finally:
        _ACTIVE_STATS = prev
        if stats.scenarios:
            ok(
                f"{app_id}: {len(stats.scenarios)} scenario(s), "
                f"~{stats.ensures} counted ensures"
            )

@dataclass(frozen=True, slots=True)
class RunReport:
    app_id: str
    ok: bool
    duration_s: float
    error: str | None = None

async def run_with_timeout(
    coro: Awaitable[T],
    *,
    timeout_s: float | None = None,
) -> T:
    if timeout_s is None or timeout_s <= 0:
        return await coro
    return await asyncio.wait_for(coro, timeout=timeout_s)

async def wait_until(
    predicate: Callable[[], bool | Awaitable[bool]],
    *,
    timeout_s: float = 8.0,
    interval_s: float = 0.05,
    what: str = "condition",
) -> None:
    """Poll until predicate is true or fail with ExampleFailed."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        result = predicate()
        if asyncio.iscoroutine(result) or asyncio.isfuture(result):
            result = await result  # type: ignore[assignment]
        if result:
            return
        await asyncio.sleep(interval_s)
    raise ExampleFailed(f"Timed out waiting for {what} ({timeout_s:.1f}s)")

async def run_app_main(
    app_id: str,
    main: Callable[[], Awaitable[None]],
    *,
    timeout_s: float | None = 120.0,
) -> RunReport:
    """Execute an app main and capture a structured report."""
    started = time.monotonic()
    try:
        await run_with_timeout(main(), timeout_s=timeout_s)
        return RunReport(app_id=app_id, ok=True, duration_s=time.monotonic() - started)
    except ExampleFailed as exc:
        return RunReport(
            app_id=app_id,
            ok=False,
            duration_s=time.monotonic() - started,
            error=str(exc),
        )
    except Exception as exc:
        tb = traceback.format_exc(limit=8)
        return RunReport(
            app_id=app_id,
            ok=False,
            duration_s=time.monotonic() - started,
            error=f"{type(exc).__name__}: {exc}\n{tb}",
        )

def exit_from_report(report: RunReport) -> None:
    if report.ok:
        ok(f"{report.app_id} completed in {report.duration_s:.2f}s")
        raise SystemExit(0)
    print(f"  ✗ {report.app_id} FAILED in {report.duration_s:.2f}s", file=sys.stderr)
    if report.error:
        print(report.error, file=sys.stderr)
    raise SystemExit(1)

# Re-export lifecycle helpers for app authors
__all__ = [
    "ExampleFailed",
    "RunReport",
    "ScenarioStats",
    "ServerHandle",
    "app_run",
    "banner",
    "ensure",
    "ensure_counted",
    "exit_from_report",
    "feature",
    "ok",
    "run_app_main",
    "run_with_servers",
    "run_with_timeout",
    "scenario",
    "start_servers",
    "step",
    "stop_servers",
    "wait_until",
    "MPREGSettings",
    "MPREGServer",
]
