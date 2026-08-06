"""MPREG Distributed Testing Lab (DistLab) — first-party product.

Jepsen-inspired history + checker + nemesis + scenario runner, packaged inside
the platform so the platform can test itself. Honest scope:

* **Is:** append-only histories, pluggable checkers, fault nemesis, STRONG and
  shared-audit adapters, scenario suites, FaultInjector integration.
* **Is not:** Elle linearizability, WAN geo generators, JVM Jepsen port,
  Byzantine fault tolerance proofs, kernel/iptables partitions.

Import::

    from mpreg.testing.distlab import (
        History, Scenario, Nemesis, default_strong_checkers, StrongSUT,
    )
"""

from __future__ import annotations

from mpreg.testing.distlab.checker import (
    CallableChecker,
    Checker,
    CompositeChecker,
    GSetConvergenceChecker,
    LWWRegisterChecker,
    NoOpenInvokeChecker,
    ReplicaAgreementChecker,
    ResidualFreeChecker,
    default_audit_checkers,
    default_strong_checkers,
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
from mpreg.testing.distlab.scenario import Scenario, ScenarioSuite

__all__ = [
    "AuditSUT",
    "CallableChecker",
    "CheckResult",
    "CheckViolation",
    "Checker",
    "CompositeChecker",
    "FaultInjectorNemesisTarget",
    "GSetConvergenceChecker",
    "History",
    "HistoryEvent",
    "LWWRegisterChecker",
    "Nemesis",
    "NemesisAction",
    "NoOpenInvokeChecker",
    "NullNemesisTarget",
    "OpKind",
    "OpStatus",
    "ReplicaAgreementChecker",
    "ResidualFreeChecker",
    "Scenario",
    "ScenarioResult",
    "ScenarioSuite",
    "StrongSUT",
    "default_audit_checkers",
    "default_strong_checkers",
]

def __getattr__(name: str) -> object:
    if name == "StrongSUT":
        from mpreg.testing.distlab.adapters.strong import StrongSUT

        return StrongSUT
    if name == "AuditSUT":
        from mpreg.testing.distlab.adapters.audit import AuditSUT

        return AuditSUT
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
