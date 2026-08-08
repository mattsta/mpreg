"""Scenario runner: generators + clients + nemesis + checkers."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from mpreg.testing.distlab.checker import Checker, CompositeChecker
from mpreg.testing.distlab.history import History
from mpreg.testing.distlab.models import CheckResult, ScenarioResult
from mpreg.testing.distlab.nemesis import Nemesis


class SystemUnderTest(Protocol):
    """Minimal SUT surface for scenarios."""

    def snapshot_state(self) -> Any:
        """State object consumed by checkers."""
        ...


ClientFn = Callable[[History, int], Awaitable[None]]
SetupFn = Callable[[], Awaitable[Any] | Any]
TeardownFn = Callable[[Any], Awaitable[None] | None]


@dataclass(slots=True)
class Scenario:
    """Self-describing, auto-managing distributed test scenario.

    Lifecycle:
      1. setup() → sut
      2. optional nemesis.start()
      3. run N client coroutines concurrently (and/or sequential generator)
      4. nemesis.stop() + heal
      5. checkers on history + sut.snapshot_state()
      6. teardown(sut)
    """

    name: str
    clients: Sequence[ClientFn] = ()
    checker: Checker | None = None
    nemesis: Nemesis | None = None
    setup: SetupFn | None = None
    teardown: TeardownFn | None = None
    # Optional sequential body after clients (e.g. soak loop)
    body: Callable[[History, Any], Awaitable[None]] | None = None
    history: History = field(default_factory=History)
    concurrency: int = 0  # 0 = len(clients)
    meta: dict[str, Any] = field(default_factory=dict)
    # If True, raise AssertionError on check failure
    strict: bool = True

    async def run(self) -> ScenarioResult:
        t0 = time.time()
        sut: Any = None
        if self.setup is not None:
            maybe = self.setup()
            sut = await maybe if asyncio.iscoroutine(maybe) else maybe

        # Bind history onto nemesis
        if self.nemesis is not None:
            self.nemesis.history = self.history
            self.nemesis.start()

        try:
            if self.clients:
                n = self.concurrency or len(self.clients)
                # Round-robin client fns if concurrency > len
                fns = list(self.clients)
                tasks = [
                    asyncio.create_task(fns[i % len(fns)](self.history, i))
                    for i in range(n)
                ]
                await asyncio.gather(*tasks)

            if self.body is not None:
                await self.body(self.history, sut)
        finally:
            if self.nemesis is not None:
                await self.nemesis.stop()

        state = None
        if sut is not None and hasattr(sut, "snapshot_state"):
            state = sut.snapshot_state()
        elif sut is not None:
            state = sut

        checker = self.checker or CompositeChecker(name="empty", checkers=[])
        check = checker.check(self.history, state=state)
        duration = time.time() - t0
        meta = dict(self.meta)
        # T17: operator/debug taxonomy from history (not WAN SLA)
        with contextlib.suppress(Exception):
            meta.setdefault("error_codes", self.history.error_code_counts())
            meta.setdefault("outcomes", self.history.outcome_counts())
        result = ScenarioResult(
            name=self.name,
            ok=check.ok,
            duration_s=duration,
            history_len=len(self.history),
            check=check,
            nemesis_actions=self.nemesis.action_count if self.nemesis else 0,
            meta=meta,
        )

        if self.teardown is not None:
            maybe = self.teardown(sut)
            if asyncio.iscoroutine(maybe):
                await maybe

        if self.strict and not result.ok:
            result.raise_if_failed()
        return result


@dataclass(slots=True)
class ScenarioSuite:
    """Named collection of scenarios (pluggable ecosystem entry)."""

    name: str
    scenarios: list[Scenario] = field(default_factory=list)

    def add(self, scenario: Scenario) -> None:
        self.scenarios.append(scenario)

    async def run_all(self, *, stop_on_fail: bool = True) -> list[ScenarioResult]:
        results: list[ScenarioResult] = []
        for sc in self.scenarios:
            # Fresh history per scenario if shared accidentally
            if len(sc.history) > 0 and sc.history is self.scenarios[0].history:
                sc.history = History()
            try:
                r = await sc.run()
            except AssertionError:
                if stop_on_fail:
                    raise
                r = ScenarioResult(
                    name=sc.name,
                    ok=False,
                    duration_s=0.0,
                    history_len=len(sc.history),
                    check=CheckResult(
                        name="raised",
                        ok=False,
                    ),
                )
            results.append(r)
            if stop_on_fail and not r.ok:
                break
        return results
