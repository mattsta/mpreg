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

# Concurrent runner is intentionally NOT imported here at module level;
# use the entry point: ``uv run mpreg test concurrent``.

__all__ = [
    "ConcurrentSuiteResult",
    "ConcurrentSuiteRunner",
    "FaultInjector",
    "FaultKind",
    "HangProfiler",
    "HangStateDir",
    "HangWatchdog",
    "NetworkView",
    "NoFileLimit",
    "RaftOracle",
    "RoutingOracle",
    "RpcOracle",
    "RpcStreamEvent",
    "raise_open_file_limit",
    # DistLab (first-party distributed testing lab)
    "distlab",
]

def __getattr__(name: str) -> object:
    """Lazy exports for concurrent/hang infrastructure and DistLab."""
    if name in {"ConcurrentSuiteResult", "ConcurrentSuiteRunner"}:
        from mpreg.testing import concurrent_runner as _cr

        return getattr(_cr, name)
    if name in {"HangProfiler", "HangStateDir", "HangWatchdog"}:
        from mpreg.testing import hang_observe as _ho

        return getattr(_ho, name)
    if name in {"NoFileLimit", "raise_open_file_limit"}:
        from mpreg.testing import resource_limits as _rl

        return getattr(_rl, name)
    if name == "distlab":
        from mpreg.testing import distlab as _dl

        return _dl
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
