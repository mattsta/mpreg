"""Shared test harness primitives for distributed-systems validation.

Product code may import these only from tests and chaos scripts. The modules
are packaged so invariant suites and optional live chaos share one fault model.
"""

from mpreg.testing.faults import FaultInjector, FaultKind, NetworkView
from mpreg.testing.oracles import (
    RaftOracle,
    RoutingOracle,
    RpcOracle,
    RpcStreamEvent,
)

__all__ = [
    "FaultInjector",
    "FaultKind",
    "NetworkView",
    "RaftOracle",
    "RoutingOracle",
    "RpcOracle",
    "RpcStreamEvent",
]
