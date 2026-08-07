"""Nemesis: scheduled fault injection against a system under test.

Integrates with :class:`mpreg.testing.faults.FaultInjector` and optional
system-specific hooks (partition mesh links, crash backends, malice modes).
"""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol

from mpreg.testing.distlab.history import History
from mpreg.testing.distlab.models import OpKind, OpStatus
from mpreg.testing.faults import FaultInjector


class NemesisAction(StrEnum):
    PARTITION_MAJORITY = "partition_majority"
    PARTITION_ONE = "partition_one"
    HEAL = "heal"
    CRASH_ONE = "crash_one"
    RECOVER_ALL = "recover_all"
    DROP_RATE = "drop_rate"
    DELAY = "delay"
    CLEAR_RATES = "clear_rates"
    CUSTOM = "custom"


class NemesisTarget(Protocol):
    """System hooks the nemesis can call."""

    def node_ids(self) -> Sequence[str]: ...

    def apply_partition_groups(self, groups: Sequence[set[str]]) -> None: ...

    def heal_network(self) -> None: ...

    def crash_node(self, node_id: str) -> None: ...

    def recover_node(self, node_id: str) -> None: ...

    def set_drop_rate(self, rate: float) -> None: ...

    def set_delay(self, seconds: float) -> None: ...

    def clear_fault_rates(self) -> None: ...


@dataclass(slots=True)
class NullNemesisTarget:
    """No-op target for unit tests of the scheduler itself."""

    nodes: list[str] = field(default_factory=lambda: ["n0", "n1", "n2"])
    log: list[str] = field(default_factory=list)

    def node_ids(self) -> Sequence[str]:
        return list(self.nodes)

    def apply_partition_groups(self, groups: Sequence[set[str]]) -> None:
        self.log.append(f"partition:{groups}")

    def heal_network(self) -> None:
        self.log.append("heal")

    def crash_node(self, node_id: str) -> None:
        self.log.append(f"crash:{node_id}")

    def recover_node(self, node_id: str) -> None:
        self.log.append(f"recover:{node_id}")

    def set_drop_rate(self, rate: float) -> None:
        self.log.append(f"drop:{rate}")

    def set_delay(self, seconds: float) -> None:
        self.log.append(f"delay:{seconds}")

    def clear_fault_rates(self) -> None:
        self.log.append("clear_rates")


@dataclass(slots=True)
class FaultInjectorNemesisTarget:
    """Bridge nemesis actions onto a shared FaultInjector (+ optional hooks)."""

    injector: FaultInjector
    nodes: list[str]
    on_partition: Callable[[Sequence[set[str]]], None] | None = None
    on_heal: Callable[[], None] | None = None
    on_crash: Callable[[str], None] | None = None
    on_recover: Callable[[str], None] | None = None
    on_drop_rate: Callable[[float], None] | None = None
    on_delay: Callable[[float], None] | None = None

    def node_ids(self) -> Sequence[str]:
        return list(self.nodes)

    def apply_partition_groups(self, groups: Sequence[set[str]]) -> None:
        self.injector.partition(*groups)
        if self.on_partition:
            self.on_partition(groups)

    def heal_network(self) -> None:
        self.injector.heal()
        if self.on_heal:
            self.on_heal()

    def crash_node(self, node_id: str) -> None:
        self.injector.crash(node_id)
        if self.on_crash:
            self.on_crash(node_id)

    def recover_node(self, node_id: str) -> None:
        self.injector.recover(node_id)
        if self.on_recover:
            self.on_recover(node_id)

    def set_drop_rate(self, rate: float) -> None:
        self.injector.control_drop_rate = rate
        self.injector.data_drop_rate = rate
        if self.on_drop_rate:
            self.on_drop_rate(rate)

    def set_delay(self, seconds: float) -> None:
        self.injector.control_delay_seconds = seconds
        self.injector.data_delay_seconds = seconds
        if self.on_delay:
            self.on_delay(seconds)

    def clear_fault_rates(self) -> None:
        self.injector.control_drop_rate = 0.0
        self.injector.data_drop_rate = 0.0
        self.injector.control_delay_seconds = 0.0
        self.injector.data_delay_seconds = 0.0
        self.injector.duplicate_rate = 0.0


@dataclass(slots=True)
class Nemesis:
    """Periodic random (or scripted) fault schedule."""

    target: NemesisTarget
    history: History | None = None
    seed: int = 0
    interval_s: float = 0.15
    actions: list[NemesisAction] = field(
        default_factory=lambda: [
            NemesisAction.PARTITION_ONE,
            NemesisAction.HEAL,
            NemesisAction.DROP_RATE,
            NemesisAction.CLEAR_RATES,
            NemesisAction.DELAY,
        ]
    )
    process_name: str = "nemesis"
    _rng: random.Random = field(init=False, repr=False)
    _task: asyncio.Task[None] | None = field(default=None, repr=False)
    _stopped: bool = False
    action_count: int = 0
    custom_handler: Callable[[NemesisAction], Awaitable[None] | None] | None = None

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)

    def _record(self, action: NemesisAction, **meta: Any) -> None:
        self.action_count += 1
        if self.history is None:
            return
        self.history.append(
            process=self.process_name,
            kind=OpKind.FAULT if action is not NemesisAction.HEAL else OpKind.HEAL,
            status=OpStatus.OK,
            meta={"action": str(action), **meta},
        )

    def step_once(self, action: NemesisAction | None = None) -> NemesisAction:
        act = action or self._rng.choice(list(self.actions))
        nodes = list(self.target.node_ids())
        if act is NemesisAction.PARTITION_MAJORITY and len(nodes) >= 3:
            # Isolate one node from the majority component
            victim = self._rng.choice(nodes)
            majority = set(nodes) - {victim}
            self.target.apply_partition_groups([{victim}, majority])
            self._record(act, victim=victim)
        elif act is NemesisAction.PARTITION_ONE and len(nodes) >= 2:
            a, b = self._rng.sample(nodes, 2)
            rest = set(nodes) - {a, b}
            groups = [{a}, {b, *rest}] if rest else [{a}, {b}]
            self.target.apply_partition_groups(groups)
            self._record(act, a=a, b=b)
        elif act is NemesisAction.HEAL:
            self.target.heal_network()
            self._record(act)
        elif act is NemesisAction.CRASH_ONE and nodes:
            n = self._rng.choice(nodes)
            self.target.crash_node(n)
            self._record(act, node=n)
        elif act is NemesisAction.RECOVER_ALL:
            for n in nodes:
                self.target.recover_node(n)
            self._record(act)
        elif act is NemesisAction.DROP_RATE:
            rate = 0.1 + self._rng.random() * 0.4
            self.target.set_drop_rate(rate)
            self._record(act, rate=rate)
        elif act is NemesisAction.DELAY:
            d = 0.01 + self._rng.random() * 0.08
            self.target.set_delay(d)
            self._record(act, delay=d)
        elif act is NemesisAction.CLEAR_RATES:
            self.target.clear_fault_rates()
            self._record(act)
        elif act is NemesisAction.CUSTOM and self.custom_handler:
            maybe = self.custom_handler(act)
            if asyncio.iscoroutine(maybe) or isinstance(maybe, Awaitable):
                # sync step_once cannot await — schedule not supported here
                pass
            self._record(act)
        else:
            # Fallback heal
            self.target.heal_network()
            self._record(NemesisAction.HEAL)
            act = NemesisAction.HEAL
        return act

    def start(self) -> None:
        if self._task is not None:
            return
        self._stopped = False

        async def _loop() -> None:
            while not self._stopped:
                try:
                    self.step_once()
                except Exception:  # noqa: BLE001
                    pass
                await asyncio.sleep(self.interval_s)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._task = loop.create_task(_loop(), name="distlab-nemesis")

    async def stop(self) -> None:
        self._stopped = True
        t = self._task
        self._task = None
        if t is not None:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
        # Always heal on stop so residual checks see a connected world
        try:
            self.target.heal_network()
            self.target.clear_fault_rates()
            for n in self.target.node_ids():
                self.target.recover_node(n)
        except Exception:  # noqa: BLE001
            pass
