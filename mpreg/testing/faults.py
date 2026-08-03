"""Controllable fault injection for L2 simulated-network proofs.

Separates control-plane vs data-plane drop/delay so routing and Raft scenarios
can stress one plane without silently breaking the other.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Hashable

class FaultKind(StrEnum):
    """Kinds of injectable faults."""

    DROP = "drop"
    DELAY = "delay"
    DUPLICATE = "duplicate"
    REORDER = "reorder"
    PARTITION = "partition"
    CLOCK_SKEW = "clock_skew"
    CRASH = "crash"

@dataclass(frozen=True, slots=True)
class NetworkView:
    """Snapshot of partition membership and per-node clock skew."""

    partitions: tuple[frozenset[str], ...] = ()
    clock_skew_seconds: dict[str, float] = field(default_factory=dict)
    crashed: frozenset[str] = frozenset()

    def can_communicate(self, source: str, target: str) -> bool:
        if source in self.crashed or target in self.crashed:
            return False
        if not self.partitions:
            return True
        source_part = target_part = None
        for part in self.partitions:
            if source in part:
                source_part = part
            if target in part:
                target_part = part
        if source_part is None and target_part is None:
            # Nodes outside all partition sets remain fully connected.
            return True
        return source_part is not None and source_part == target_part

    def now_for(self, node_id: str, wall: float | None = None) -> float:
        base = time.time() if wall is None else wall
        return base + float(self.clock_skew_seconds.get(node_id, 0.0))

@dataclass(slots=True)
class FaultInjector:
    """Mutable fault schedule used by oracles and chaos tests."""

    seed: int = 0
    control_drop_rate: float = 0.0
    data_drop_rate: float = 0.0
    control_delay_seconds: float = 0.0
    data_delay_seconds: float = 0.0
    duplicate_rate: float = 0.0
    _partitions: list[frozenset[str]] = field(default_factory=list)
    _clock_skew: dict[str, float] = field(default_factory=dict)
    _crashed: set[str] = field(default_factory=set)
    _rng: random.Random = field(init=False, repr=False)
    decisions: list[dict[str, object]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)

    def view(self) -> NetworkView:
        return NetworkView(
            partitions=tuple(self._partitions),
            clock_skew_seconds=dict(self._clock_skew),
            crashed=frozenset(self._crashed),
        )

    def partition(self, *groups: set[str] | frozenset[str]) -> None:
        """Replace partition map with the given disjoint groups."""
        self._partitions = [frozenset(g) for g in groups]
        self._record("partition", groups=[sorted(g) for g in self._partitions])

    def heal(self) -> None:
        self._partitions.clear()
        self._record("heal")

    def crash(self, node_id: str) -> None:
        self._crashed.add(node_id)
        self._record("crash", node_id=node_id)

    def recover(self, node_id: str) -> None:
        self._crashed.discard(node_id)
        self._record("recover", node_id=node_id)

    def set_clock_skew(self, node_id: str, skew_seconds: float) -> None:
        self._clock_skew[node_id] = float(skew_seconds)
        self._record("clock_skew", node_id=node_id, skew=skew_seconds)

    def clear_clock_skew(self) -> None:
        self._clock_skew.clear()
        self._record("clear_clock_skew")

    def can_deliver(
        self,
        source: str,
        target: str,
        *,
        plane: str = "control",
    ) -> bool:
        """Return False if the message should be dropped."""
        view = self.view()
        if not view.can_communicate(source, target):
            self._record(
                "drop_partition", source=source, target=target, plane=plane
            )
            return False
        rate = self.control_drop_rate if plane == "control" else self.data_drop_rate
        if rate > 0 and self._rng.random() < rate:
            self._record("drop_random", source=source, target=target, plane=plane)
            return False
        return True

    def delay_for(self, *, plane: str = "control") -> float:
        base = (
            self.control_delay_seconds
            if plane == "control"
            else self.data_delay_seconds
        )
        if base <= 0:
            return 0.0
        # Jitter 50–150% of configured delay.
        return base * (0.5 + self._rng.random())

    def should_duplicate(self) -> bool:
        return self.duplicate_rate > 0 and self._rng.random() < self.duplicate_rate

    def should_reorder(self, buffer_len: int) -> bool:
        return buffer_len >= 2 and self._rng.random() < 0.3

    def _record(self, kind: str, **fields: object) -> None:
        entry: dict[str, object] = {"kind": kind, "t": time.time()}
        entry.update(fields)
        self.decisions.append(entry)
        # Cap history for long soaks.
        if len(self.decisions) > 10_000:
            del self.decisions[:5_000]

def assert_no_routing_loop(path_hops: tuple[str, ...] | list[str]) -> None:
    """INV-R2 helper: path hops must be unique."""
    hops = list(path_hops)
    if len(hops) != len(set(hops)):
        raise AssertionError(f"routing loop in path: {hops}")

def assert_at_most_one_leader(leaders_by_term: dict[int, set[str]]) -> None:
    """INV-C1 helper."""
    for term, leaders in leaders_by_term.items():
        if len(leaders) > 1:
            raise AssertionError(
                f"election safety violated term={term} leaders={sorted(leaders)}"
            )

def stable_hash_key(value: Hashable) -> str:
    """Deterministic string key for ECMP rotation tests."""
    return str(value)
