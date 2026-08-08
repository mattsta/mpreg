"""Shared server-side dataclasses extracted from the MPREGServer module."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from mpreg.core.connection_events import ConnectionEvent
from mpreg.core.model import GoodbyeReason
from mpreg.fabric.catalog_delta import RoutingCatalogDelta


@dataclass(frozen=True, slots=True)
class InternalDiscoverySubscriptionAnnouncer:
    schedule: Callable[[Any], None]
    announce: Callable[[], Any]

    def on_connection_established(self, event: ConnectionEvent) -> None:
        self.schedule(self.announce())

    def on_connection_lost(self, event: ConnectionEvent) -> None:
        return


@dataclass(slots=True)
class MessageStats:
    total_processed: int = 0
    rpc_responses_skipped: int = 0
    server_messages: int = 0
    other_messages: int = 0


@dataclass(slots=True)
class RemoteCommandStats:
    total: int = 0
    last_command: str | None = None

    def record(self, command: str) -> None:
        self.total += 1
        self.last_command = command


@dataclass(frozen=True, slots=True)
class DepartedPeer:
    node_url: str
    instance_id: str
    cluster_id: str
    reason: GoodbyeReason
    departed_at: float
    ttl_seconds: float

    def is_expired(self, now: float | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.departed_at + self.ttl_seconds)


@dataclass(slots=True)
class CatalogSnapshotDispatchState:
    """Track coalesced catalog snapshot dispatch across rapid update bursts.

    Counters ``payload_builds`` / ``to_dict_calls`` / ``wire_encodes`` /
    ``bytes_sends`` exist so tests can prove serialize-once-send-many:
    one flush batch must build the catalog body once, not once per peer.
    """

    pending_peers: set[str] = field(default_factory=set)
    flush_task: asyncio.Task[None] | None = None
    enqueued_events: int = 0
    flush_batches: int = 0
    peers_flushed: int = 0
    # Serialize-once instrumentation (tests + diag).
    payload_builds: int = 0
    to_dict_calls: int = 0
    wire_encodes: int = 0
    bytes_sends: int = 0
    last_wire_bytes: int = 0
    last_update_id: str | None = None
    last_generation: int | None = None


@dataclass(frozen=True, slots=True)
class CommandExecutionResult:
    name: str
    value: Any


@dataclass(slots=True)
class CatalogDeltaObserverAdapter:
    publish: Callable[[RoutingCatalogDelta, dict[str, int]], None]

    def on_catalog_delta(
        self, delta: RoutingCatalogDelta, counts: dict[str, int]
    ) -> None:
        self.publish(delta, counts)
