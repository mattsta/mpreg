"""Adapter endpoint registry for transport auto-allocation."""

from __future__ import annotations

import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from mpreg.datastructures.type_aliases import PortNumber

from .defaults import ConnectionType
from .interfaces import TransportProtocol


@dataclass(frozen=True, slots=True)
class ProtocolPortAssignment:
    """Assigned port details for a transport protocol."""

    protocol: TransportProtocol
    host: str
    port: PortNumber
    connection_type: ConnectionType

    @property
    def endpoint(self) -> str:
        """Return the fully-qualified endpoint URL."""
        return f"{self.protocol.value}://{self.host}:{self.port}"


type ProtocolPortAssignmentCallback = Callable[
    [ProtocolPortAssignment], None | Awaitable[None]
]


@dataclass(slots=True)
class AdapterEndpointRegistry:
    """Thread-safe registry of adapter endpoint assignments."""

    _lock: threading.Lock = field(default_factory=threading.Lock)
    _assignments: dict[
        ConnectionType, dict[TransportProtocol, ProtocolPortAssignment]
    ] = field(default_factory=dict)
    _last_updated: float = 0.0

    def record(self, assignment: ProtocolPortAssignment) -> None:
        """Record or update an assignment."""
        with self._lock:
            self._assignments.setdefault(assignment.connection_type, {})[
                assignment.protocol
            ] = assignment
            self._last_updated = time.time()

    def release(self, assignment: ProtocolPortAssignment) -> None:
        """Remove an assignment if present."""
        with self._lock:
            entries = self._assignments.get(assignment.connection_type)
            if not entries:
                return
            entries.pop(assignment.protocol, None)
            if not entries:
                self._assignments.pop(assignment.connection_type, None)
            self._last_updated = time.time()

    def snapshot(self) -> dict[str, dict[str, str]]:
        """Return a lightweight snapshot of endpoints."""
        with self._lock:
            return {
                connection_type.value: {
                    protocol.value: assignment.endpoint
                    for protocol, assignment in assignments.items()
                }
                for connection_type, assignments in self._assignments.items()
            }

    def assignments(self) -> tuple[ProtocolPortAssignment, ...]:
        """Return all current assignments."""
        with self._lock:
            return tuple(
                assignment
                for assignments in self._assignments.values()
                for assignment in assignments.values()
            )

    def detailed_snapshot(self) -> dict[str, Any]:
        """Return a detailed snapshot including host/port metadata."""
        with self._lock:
            return {
                "last_updated": self._last_updated,
                "assignments": [
                    {
                        "connection_type": assignment.connection_type.value,
                        "protocol": assignment.protocol.value,
                        "host": assignment.host,
                        "port": assignment.port,
                        "endpoint": assignment.endpoint,
                    }
                    for assignments in self._assignments.values()
                    for assignment in assignments.values()
                ],
            }

    def get_endpoint(
        self, connection_type: ConnectionType, protocol: TransportProtocol
    ) -> str | None:
        """Return a single endpoint by connection type + protocol."""
        with self._lock:
            assignment = self._assignments.get(connection_type, {}).get(protocol)
            return assignment.endpoint if assignment else None

    def clear(self) -> None:
        """Clear all recorded assignments."""
        with self._lock:
            self._assignments.clear()
            self._last_updated = time.time()


_endpoint_registry: AdapterEndpointRegistry | None = None


def get_adapter_endpoint_registry() -> AdapterEndpointRegistry:
    """Return the shared adapter endpoint registry."""
    global _endpoint_registry
    if _endpoint_registry is None:
        _endpoint_registry = AdapterEndpointRegistry()
    return _endpoint_registry
