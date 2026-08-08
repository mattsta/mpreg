"""MPREG Distributed Testing Lab (DistLab) — first-party product.

Jepsen-inspired history + checker + nemesis + scenario runner, packaged inside
the platform so the platform can test itself. Honest scope:

* **Is:** append-only histories, pluggable checkers, fault nemesis, STRONG,
  shared-audit, and ProductionRaft adapters, scenario suites, generators,
  registry, live helpers, FaultInjector integration.
* **Is not:** Elle linearizability, WAN geo generators, JVM Jepsen port,
  Byzantine fault tolerance proofs, kernel/iptables partitions.

Import::

    from mpreg.testing.distlab import (
        History, Scenario, Nemesis, default_strong_checkers, StrongSUT,
        default_raft_checkers, RaftSUT, get_registry, ensure_builtins,
    )

CLI (pyproject entry point only — never ``python -m``)::

    uv run mpreg distlab list
    uv run mpreg distlab catalog
    uv run mpreg distlab run strong.happy_3
    uv run mpreg distlab run raft.elect_3
"""

from __future__ import annotations

from mpreg.testing.distlab.checker import (
    CallableChecker,
    Checker,
    CompositeChecker,
    GSetConvergenceChecker,
    LWWRegisterChecker,
    NoOpenInvokeChecker,
    RaftSmAgreementChecker,
    ReplicaAgreementChecker,
    ResidualFreeChecker,
    UniqueLeaderChecker,
    default_audit_checkers,
    default_raft_checkers,
    default_strong_checkers,
)
from mpreg.testing.distlab.generator import (
    AuditBurst,
    ConcurrentPuts,
    RandomFaultPlan,
    SequentialPuts,
)
from mpreg.testing.distlab.history import History
from mpreg.testing.distlab.models import (
    CheckResult,
    CheckViolation,
    HistoryEvent,
    OpKind,
    OpStatus,
    ScenarioResult,
)
from mpreg.testing.distlab.nemesis import (
    FaultInjectorNemesisTarget,
    Nemesis,
    NemesisAction,
    NullNemesisTarget,
)
from mpreg.testing.distlab.registry import (
    DEFAULT_REGISTRY,
    ScenarioRegistry,
    get_registry,
)
from mpreg.testing.distlab.scenario import Scenario, ScenarioSuite
from mpreg.testing.distlab.sli import (
    DEFAULT_STRONG_SOAK_BUDGET,
    SliBudget,
    WallTimer,
    percentile,
    summarize_latencies_ms,
)

__all__ = [
    "DEFAULT_REGISTRY",
    "DEFAULT_STRONG_SOAK_BUDGET",
    "AuditBurst",
    "AuditSUT",
    "CallableChecker",
    "CheckResult",
    "CheckViolation",
    "Checker",
    "CompositeChecker",
    "ConcurrentPuts",
    "FaultInjectorNemesisTarget",
    "GSetConvergenceChecker",
    "History",
    "HistoryEvent",
    "LWWRegisterChecker",
    "LiveStrongSUT",
    "Nemesis",
    "NemesisAction",
    "NoOpenInvokeChecker",
    "NullNemesisTarget",
    "OpKind",
    "OpStatus",
    "RaftSUT",
    "RaftSmAgreementChecker",
    "RandomFaultPlan",
    "ReplicaAgreementChecker",
    "ResidualFreeChecker",
    "Scenario",
    "ScenarioRegistry",
    "ScenarioResult",
    "ScenarioSuite",
    "SequentialPuts",
    "SliBudget",
    "StrongSUT",
    "UniqueLeaderChecker",
    "WallTimer",
    "default_audit_checkers",
    "default_raft_checkers",
    "default_strong_checkers",
    "ensure_builtins",
    "get_registry",
    "percentile",
    "register_builtins",
    "summarize_latencies_ms",
]


def __getattr__(name: str) -> object:
    if name == "StrongSUT":
        from mpreg.testing.distlab.adapters.strong import StrongSUT

        return StrongSUT
    if name == "AuditSUT":
        from mpreg.testing.distlab.adapters.audit import AuditSUT

        return AuditSUT
    if name == "RaftSUT":
        from mpreg.testing.distlab.adapters.raft import RaftSUT

        return RaftSUT
    if name == "LiveStrongSUT":
        from mpreg.testing.distlab.live import LiveStrongSUT

        return LiveStrongSUT
    if name in ("ensure_builtins", "register_builtins"):
        from mpreg.testing.distlab import builtins as _b

        return getattr(_b, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
