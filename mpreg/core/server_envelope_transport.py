"""Shared server transport for envelope-based peer messaging."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pydantic import BaseModel

from mpreg.core.serialization import JsonSerializer
from mpreg.core.transport.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    create_circuit_breaker,
)
from mpreg.datastructures.type_aliases import NodeId

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mpreg.core.connection import Connection
    from mpreg.server import MPREGServer


@dataclass(slots=True)
class ServerEnvelopeTransport:
    """Deliver serialized envelopes over MPREG server peer connections."""

    server: MPREGServer
    serializer: JsonSerializer
    circuit_breaker_config: CircuitBreakerConfig | None = field(
        default_factory=CircuitBreakerConfig
    )
    _circuit_breakers: dict[NodeId, CircuitBreaker] = field(
        default_factory=dict, init=False
    )

    def peer_ids(self, *, exclude: NodeId | None = None) -> tuple[NodeId, ...]:
        peers = []
        for peer_id, connection in self._active_connections().items():
            if peer_id == exclude:
                continue
            if connection.is_connected:
                peers.append(peer_id)
        return tuple(sorted(peers))

    async def send_envelope(self, peer_id: NodeId, envelope: BaseModel) -> bool:
        breaker = self._circuit_breaker_for(peer_id)
        if breaker and not breaker.can_execute():
            return False

        connection = self._active_connections().get(peer_id)
        if connection is None or not connection.is_connected:
            if breaker:
                breaker.record_failure()
            return False

        data = self.serializer.serialize(envelope.model_dump())
        try:
            await connection.send(data)
        except Exception:
            if breaker:
                breaker.record_failure()
            return False
        if breaker:
            breaker.record_success()
        return True

    def _active_connections(self) -> dict[str, Connection]:
        return {
            url: conn
            for url, conn in self.server._get_all_peer_connections().items()
            if conn.is_connected
        }

    def _circuit_breaker_for(self, peer_id: NodeId) -> CircuitBreaker | None:
        if self.circuit_breaker_config is None:
            return None
        breaker = self._circuit_breakers.get(peer_id)
        if breaker is None:
            breaker = create_circuit_breaker(
                peer_id,
                failure_threshold=self.circuit_breaker_config.failure_threshold,
                recovery_timeout_ms=self.circuit_breaker_config.recovery_timeout_ms,
                success_threshold=self.circuit_breaker_config.success_threshold,
            )
            self._circuit_breakers[peer_id] = breaker
        return breaker
