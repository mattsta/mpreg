"""Core event and result models for the distributed testing lab.

DistLab is a **first-party** product surface under ``mpreg.testing.distlab``.
It is Jepsen-inspired (history + checker + nemesis) but **not** a Jepsen port:
no Elle, no WAN generator, no external JVM. Scope is honest and documented.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

class OpKind(StrEnum):
    """Client-visible operation kinds recorded in a history."""

    # Generic KV
    PUT = "put"
    GET = "get"
    DELETE = "delete"
    # Shared audit
    AUDIT_PUBLISH = "audit_publish"
    AUDIT_SNAPSHOT = "audit_snapshot"
    # Control
    FAULT = "fault"
    HEAL = "heal"
    BARRIER = "barrier"
    CUSTOM = "custom"

class OpStatus(StrEnum):
    """Terminal status of a history event (Jepsen-style invoke/ok/fail/info)."""

    INVOKE = "invoke"
    OK = "ok"
    FAIL = "fail"
    # Indeterminate (timeout / crash mid-op) — checker must not treat as success
    INFO = "info"

@dataclass(frozen=True, slots=True)
class HistoryEvent:
    """One atomic observation in a linear history log."""

    index: int
    process: str
    kind: OpKind
    status: OpStatus
    wall_time: float
    key: str | None = None
    value: Any = None
    op_id: str | None = None
    error_code: int | None = None
    error_message: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "process": self.process,
            "kind": str(self.kind),
            "status": str(self.status),
            "wall_time": self.wall_time,
            "key": self.key,
            "value": self.value,
            "op_id": self.op_id,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "meta": dict(self.meta),
        }

@dataclass(frozen=True, slots=True)
class CheckViolation:
    """A single checker finding."""

    checker: str
    message: str
    severity: str = "error"  # error | warn | info
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "checker": self.checker,
            "message": self.message,
            "severity": self.severity,
            "evidence": dict(self.evidence),
        }

@dataclass(slots=True)
class CheckResult:
    """Aggregate result of one or more checkers."""

    name: str
    ok: bool
    violations: list[CheckViolation] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)

    def merge(self, other: CheckResult) -> CheckResult:
        return CheckResult(
            name=f"{self.name}+{other.name}",
            ok=self.ok and other.ok,
            violations=[*self.violations, *other.violations],
            stats={**self.stats, **{f"{other.name}.{k}": v for k, v in other.stats.items()}},
        )

    def raise_if_failed(self) -> None:
        if self.ok:
            return
        msgs = "; ".join(v.message for v in self.violations[:8])
        raise AssertionError(f"distlab check {self.name!r} failed: {msgs}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "ok": self.ok,
            "violations": [v.to_dict() for v in self.violations],
            "stats": dict(self.stats),
        }

@dataclass(slots=True)
class ScenarioResult:
    """Outcome of running a named scenario."""

    name: str
    ok: bool
    duration_s: float
    history_len: int
    check: CheckResult
    nemesis_actions: int = 0
    meta: dict[str, Any] = field(default_factory=dict)
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def raise_if_failed(self) -> None:
        if not self.ok:
            self.check.raise_if_failed()
            raise AssertionError(f"scenario {self.name!r} failed")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "ok": self.ok,
            "duration_s": self.duration_s,
            "history_len": self.history_len,
            "check": self.check.to_dict(),
            "nemesis_actions": self.nemesis_actions,
            "meta": dict(self.meta),
            "run_id": self.run_id,
        }

def wall_now() -> float:
    return time.time()
