"""Server composition helpers extracted from the MPREGServer façade.

The runtime entry point remains :class:`mpreg.server.MPREGServer`. Helpers in
this package reduce god-module growth without changing public behavior.

Modules
-------
- ``types`` — shared server dataclasses (stats, departed peers, catalog adapters)
- ``peer_dial`` — dial state types and pure connection/backoff policy math
- ``monitoring_metrics`` — DNS/persistence metric payload builders
- ``discovery_metrics`` — discovery summary/cache/policy/lag metric builders
- ``mgmt_summary`` — normalized ``/mgmt/v1`` read models
- ``consensus.md`` — canonical consensus API matrix (docs)
"""

from __future__ import annotations

from .discovery_metrics import (
    build_discovery_cache_metrics,
    build_discovery_lag_metrics,
    build_discovery_policy_metrics,
    build_discovery_summary_metrics,
)
from .mgmt_summary import build_mgmt_v1_summary
from .monitoring_metrics import build_dns_metrics, build_persistence_snapshot_metrics
from .peer_dial import (
    PeerDialBackoff,
    PeerDialConnectionPolicy,
    PeerDialDiagnosticSnapshot,
    PeerDialLoopSnapshot,
    PeerDialState,
    backoff_base_seconds,
    backoff_cap_seconds,
    dial_exploration_slots,
    dial_parallelism,
    dial_pressure,
    dial_pressure_from_counts,
    reconcile_interval_seconds,
    select_peer_connection_policy,
    selection_spread,
    spread_fraction,
    spread_fraction_for_url,
    target_connection_count,
)
from .types import (
    CatalogDeltaObserverAdapter,
    CatalogSnapshotDispatchState,
    CommandExecutionResult,
    DepartedPeer,
    InternalDiscoverySubscriptionAnnouncer,
    MessageStats,
    RemoteCommandStats,
)

__all__ = [
    "build_mgmt_v1_summary",
    "build_persistence_snapshot_metrics",
    "build_dns_metrics",
    "build_discovery_summary_metrics",
    "build_discovery_cache_metrics",
    "build_discovery_policy_metrics",
    "build_discovery_lag_metrics",
    "PeerDialBackoff",
    "PeerDialConnectionPolicy",
    "PeerDialDiagnosticSnapshot",
    "PeerDialLoopSnapshot",
    "PeerDialState",
    "backoff_base_seconds",
    "backoff_cap_seconds",
    "dial_exploration_slots",
    "dial_parallelism",
    "dial_pressure",
    "dial_pressure_from_counts",
    "reconcile_interval_seconds",
    "select_peer_connection_policy",
    "selection_spread",
    "spread_fraction",
    "spread_fraction_for_url",
    "target_connection_count",
    "CatalogDeltaObserverAdapter",
    "CatalogSnapshotDispatchState",
    "CommandExecutionResult",
    "DepartedPeer",
    "InternalDiscoverySubscriptionAnnouncer",
    "MessageStats",
    "RemoteCommandStats",
]

# openapi_surface — machine-readable HTTP contract
