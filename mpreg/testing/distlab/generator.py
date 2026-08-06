"""Workload generators for DistLab scenarios (pluggable client work)."""

from __future__ import annotations

import random
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from mpreg.testing.distlab.history import History

class WorkItem(Protocol):
    """One unit of client work."""

    async def run(self, history: History, idx: int) -> None: ...

ClientFn = Callable[[History, int], Awaitable[None]]

@dataclass(slots=True)
class SequentialPuts:
    """N sequential puts on one logical key round-robin origins."""

    sut: Any  # StrongSUT
    n: int
    logical_key: str = "gen"
    origins: Sequence[str] | None = None
    value_fn: Callable[[int], Any] = field(default=lambda i: i)

    def as_body(self) -> Callable[[History, Any], Awaitable[None]]:
        async def body(history: History, sut: Any) -> None:
            s = sut if sut is not None else self.sut
            origins = list(self.origins or s.peer_ids)
            for i in range(self.n):
                origin = origins[i % len(origins)]
                await s.put(
                    history,
                    process=f"seq-{i}",
                    origin=origin,
                    logical_key=self.logical_key,
                    value=self.value_fn(i),
                    op_id=f"seq-{self.logical_key}-{i}",
                )

        return body

@dataclass(slots=True)
class ConcurrentPuts:
    """Build client callables for concurrent puts."""

    sut: Any
    n_clients: int
    logical_key: str = "cgen"
    multi_key: bool = False
    value_fn: Callable[[int], Any] = field(default=lambda i: i)

    def as_clients(self) -> list[ClientFn]:
        sut = self.sut
        key = self.logical_key
        multi = self.multi_key
        vfn = self.value_fn

        async def make(history: History, idx: int) -> None:
            lk = f"{key}-{idx}" if multi else key
            origin = sut.peer_ids[idx % len(sut.peer_ids)]
            await sut.put(
                history,
                process=f"c-{idx}",
                origin=origin,
                logical_key=lk,
                value=vfn(idx),
                op_id=f"cgen-{lk}-{idx}",
            )

        return [make] * self.n_clients

@dataclass(slots=True)
class AuditBurst:
    """Publish N audit events round-robin origins."""

    sut: Any
    n: int
    prefix: str = "g"

    def as_body(self) -> Callable[[History, Any], Awaitable[None]]:
        async def body(history: History, sut: Any) -> None:
            s = sut if sut is not None else self.sut
            for i in range(self.n):
                origin = s.node_ids[i % len(s.node_ids)]
                await s.publish(
                    history,
                    process=f"ap-{i}",
                    origin=origin,
                    event=f"{self.prefix}{i}",
                )
            await s.flush_all()
            await s.reconcile_all()

        return body

@dataclass(slots=True)
class RandomFaultPlan:
    """Seedable list of fault action names for scripted nemesis."""

    seed: int = 0
    n: int = 10
    actions: tuple[str, ...] = (
        "partition_one",
        "heal",
        "drop_rate",
        "clear_rates",
        "delay",
    )
    _rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)

    def plan(self) -> list[str]:
        return [self._rng.choice(self.actions) for _ in range(self.n)]
