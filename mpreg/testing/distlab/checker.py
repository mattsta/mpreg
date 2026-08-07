"""Pluggable checkers over DistLab histories and system snapshots.

These are **bounded** local checkers — not Elle/Jepsen full linearizability.
Each checker declares what it proves and what it does not.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from mpreg.testing.distlab.history import History
from mpreg.testing.distlab.models import (
    CheckResult,
    CheckViolation,
    OpKind,
    OpStatus,
)

class Checker(Protocol):
    """Self-describing checker protocol."""

    name: str

    def check(self, history: History, *, state: Any = None) -> CheckResult: ...

@dataclass(slots=True)
class CompositeChecker:
    """Run multiple checkers; fail if any errors."""

    name: str = "composite"
    checkers: list[Checker] = field(default_factory=list)

    def check(self, history: History, *, state: Any = None) -> CheckResult:
        ok = True
        violations: list[CheckViolation] = []
        stats: dict[str, Any] = {"sub": []}
        for c in self.checkers:
            r = c.check(history, state=state)
            stats["sub"].append(r.to_dict())
            if not r.ok:
                ok = False
            violations.extend(r.violations)
            for k, v in r.stats.items():
                stats[f"{c.name}.{k}"] = v
        return CheckResult(name=self.name, ok=ok, violations=violations, stats=stats)

@dataclass(slots=True)
class ResidualFreeChecker:
    """Failed PUTs must not leave ``op_id`` visible in state snapshot.

    ``state`` must expose ``visible_op_ids(key) -> set[str]`` and optionally
    ``pending_count() -> int``.
    """

    name: str = "residual_free"

    def check(self, history: History, *, state: Any = None) -> CheckResult:
        violations: list[CheckViolation] = []
        failed = history.failed_puts()
        if state is None:
            return CheckResult(
                name=self.name,
                ok=True,
                stats={"skipped": "no_state", "failed_puts": len(failed)},
            )
        pending = 0
        if hasattr(state, "pending_count"):
            pending = int(state.pending_count())
            if pending > 0:
                violations.append(
                    CheckViolation(
                        checker=self.name,
                        message=f"pending slots remain: {pending}",
                        evidence={"pending": pending},
                    )
                )
        for ev in failed:
            if not ev.op_id or not ev.key:
                continue
            if not hasattr(state, "visible_op_ids"):
                continue
            visible = set(state.visible_op_ids(ev.key))
            if ev.op_id in visible:
                violations.append(
                    CheckViolation(
                        checker=self.name,
                        message=(
                            f"failed op_id {ev.op_id} still visible on key {ev.key}"
                        ),
                        evidence={
                            "op_id": ev.op_id,
                            "key": ev.key,
                            "visible": sorted(visible),
                        },
                    )
                )
        return CheckResult(
            name=self.name,
            ok=not violations,
            violations=violations,
            stats={"failed_puts": len(failed), "pending": pending},
        )

@dataclass(slots=True)
class ReplicaAgreementChecker:
    """Non-null replica views agree on (value, op_id).

    Majority-quorum systems may leave some replicas empty; that is **not** a
    divergence. Two different non-null values for the same key is.
    """

    name: str = "replica_agreement"
    require_all: bool = False

    def check(self, history: History, *, state: Any = None) -> CheckResult:
        violations: list[CheckViolation] = []
        if state is None or not hasattr(state, "replica_views"):
            return CheckResult(
                name=self.name, ok=True, stats={"skipped": "no_state"}
            )
        keys = sorted({e.key for e in history.snapshot() if e.key})
        for key in keys:
            views = state.replica_views(key)  # dict[node -> (value, op_id)|None]
            vals = list(views.values())
            if not vals:
                continue
            if self.require_all:
                first = vals[0]
                for v in vals[1:]:
                    if v != first:
                        violations.append(
                            CheckViolation(
                                checker=self.name,
                                message=f"replicas diverged on key {key}",
                                evidence={
                                    "views": {
                                        k: _safe(v) for k, v in views.items()
                                    }
                                },
                            )
                        )
                        break
            else:
                present = [v for v in vals if v is not None]
                if len(present) >= 2:
                    first = present[0]
                    if any(v != first for v in present[1:]):
                        violations.append(
                            CheckViolation(
                                checker=self.name,
                                message=f"non-null replicas diverged on key {key}",
                                evidence={
                                    "views": {
                                        k: _safe(v) for k, v in views.items()
                                    }
                                },
                            )
                        )
        return CheckResult(
            name=self.name,
            ok=not violations,
            violations=violations,
            stats={"keys": len(keys)},
        )

@dataclass(slots=True)
class LWWRegisterChecker:
    """Bounded single-key LWW register check (not full linearizability).

    Rules:
    - Final visible value (if any) must come from some successful PUT.
    - Failed PUT op_ids must not be the final visible op_id.
    - If any successful PUT and state present, value must be non-empty OR
      all successes were overwritten (still final from a success).
    """

    name: str = "lww_register"
    key: str | None = None

    def check(self, history: History, *, state: Any = None) -> CheckResult:
        violations: list[CheckViolation] = []
        keys = (
            [self.key]
            if self.key
            else sorted({e.key for e in history.snapshot() if e.key and e.kind is OpKind.PUT})
        )
        stats: dict[str, Any] = {"keys_checked": 0}
        for key in keys:
            if key is None:
                continue
            stats["keys_checked"] = int(stats["keys_checked"]) + 1
            ok_puts = history.successful_puts(key)
            fail_puts = history.failed_puts(key)
            fail_ops = {e.op_id for e in fail_puts if e.op_id}
            ok_ops = {e.op_id for e in ok_puts if e.op_id}

            if state is None or not hasattr(state, "final_op_id"):
                continue
            final_op = state.final_op_id(key)
            final_val = (
                state.final_value(key) if hasattr(state, "final_value") else None
            )
            if final_op is None:
                if ok_puts:
                    # All successes overwritten only if concurrent higher won —
                    # if final empty with successes, violation unless multi-replica
                    # partial (we require agreement checker separately).
                    violations.append(
                        CheckViolation(
                            checker=self.name,
                            message=f"successful puts but no final value for {key}",
                            evidence={"ok_ops": sorted(ok_ops)},
                        )
                    )
                continue
            if final_op in fail_ops:
                violations.append(
                    CheckViolation(
                        checker=self.name,
                        message=f"final op_id {final_op} was a failed put",
                        evidence={"key": key, "final_op": final_op},
                    )
                )
            if final_op not in ok_ops:
                violations.append(
                    CheckViolation(
                        checker=self.name,
                        message=f"final op_id {final_op} not among successful puts",
                        evidence={
                            "key": key,
                            "final_op": final_op,
                            "ok_ops": sorted(x for x in ok_ops if x),
                        },
                    )
                )
            # Value match if we can find the ok event
            for e in ok_puts:
                if e.op_id == final_op and e.value is not None and final_val is not None:
                    if e.value != final_val:
                        violations.append(
                            CheckViolation(
                                checker=self.name,
                                message=f"final value mismatch for op {final_op}",
                                evidence={
                                    "expected": _safe(e.value),
                                    "actual": _safe(final_val),
                                },
                            )
                        )
        return CheckResult(
            name=self.name,
            ok=not violations,
            violations=violations,
            stats=stats,
        )

@dataclass(slots=True)
class GSetConvergenceChecker:
    """Shared-audit style: every published eligible id appears in all snapshots.

    ``state`` must expose ``gset_ids_by_node() -> dict[str, set[str]]``.
    History OK AUDIT_PUBLISH events contribute expected ids via op_id or value.
    """

    name: str = "gset_convergence"
    min_ids: int = 0

    def check(self, history: History, *, state: Any = None) -> CheckResult:
        violations: list[CheckViolation] = []
        expected: set[str] = set()
        for e in history.snapshot():
            if e.kind is OpKind.AUDIT_PUBLISH and e.status is OpStatus.OK:
                # gossip_eligible=false stays origin-local — not in convergence set
                if e.meta.get("eligible") is False:
                    continue
                eid = e.op_id or (str(e.value) if e.value is not None else None)
                if eid:
                    expected.add(eid)
        if state is None or not hasattr(state, "gset_ids_by_node"):
            return CheckResult(
                name=self.name,
                ok=True,
                stats={"skipped": "no_state", "expected": len(expected)},
            )
        by_node: dict[str, set[str]] = state.gset_ids_by_node()
        if not by_node:
            if expected or self.min_ids:
                violations.append(
                    CheckViolation(
                        checker=self.name,
                        message="no node snapshots for gset",
                    )
                )
        else:
            for node, ids in by_node.items():
                missing = expected - ids
                if missing:
                    violations.append(
                        CheckViolation(
                            checker=self.name,
                            message=f"node {node} missing {len(missing)} ids",
                            evidence={
                                "node": node,
                                "missing_sample": sorted(missing)[:8],
                            },
                        )
                    )
            # All nodes equal
            sets = list(by_node.values())
            if sets and any(s != sets[0] for s in sets[1:]):
                violations.append(
                    CheckViolation(
                        checker=self.name,
                        message="nodes disagree on gset membership",
                        evidence={
                            n: sorted(ids)[:12] for n, ids in by_node.items()
                        },
                    )
                )
            if self.min_ids and sets and len(sets[0]) < self.min_ids:
                violations.append(
                    CheckViolation(
                        checker=self.name,
                        message=f"gset size {len(sets[0])} < min_ids {self.min_ids}",
                    )
                )
        return CheckResult(
            name=self.name,
            ok=not violations,
            violations=violations,
            stats={"expected": len(expected), "nodes": len(by_node) if state else 0},
        )

@dataclass(slots=True)
class NoOpenInvokeChecker:
    """Every INVOKE must have a terminal event (no hung clients)."""

    name: str = "no_open_invoke"

    def check(self, history: History, *, state: Any = None) -> CheckResult:
        violations: list[CheckViolation] = []
        for inv, term in history.pairs():
            if term is None:
                violations.append(
                    CheckViolation(
                        checker=self.name,
                        message=f"open invoke process={inv.process} kind={inv.kind}",
                        evidence=inv.to_dict(),
                    )
                )
        return CheckResult(
            name=self.name,
            ok=not violations,
            violations=violations,
            stats={"pairs": len(history.pairs())},
        )

@dataclass(slots=True)
class CallableChecker:
    """Wrap a plain function as a checker (plugin hook)."""

    name: str
    fn: Callable[[History, Any], CheckResult]

    def check(self, history: History, *, state: Any = None) -> CheckResult:
        return self.fn(history, state)

def default_strong_checkers(*, key: str | None = None) -> CompositeChecker:
    return CompositeChecker(
        name="strong_default",
        checkers=[
            NoOpenInvokeChecker(),
            ResidualFreeChecker(),
            ReplicaAgreementChecker(),
            LWWRegisterChecker(key=key),
        ],
    )

def default_audit_checkers(*, min_ids: int = 0) -> CompositeChecker:
    return CompositeChecker(
        name="audit_default",
        checkers=[
            NoOpenInvokeChecker(),
            GSetConvergenceChecker(min_ids=min_ids),
        ],
    )

def _safe(v: Any) -> Any:
    try:
        hash(v)
        return v
    except TypeError:
        return repr(v)
