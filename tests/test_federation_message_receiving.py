"""Fabric message transport tests."""

import pytest

from mpreg.core.model import FabricMessageEnvelope
from mpreg.core.serialization import JsonSerializer
from mpreg.core.server_envelope_transport import ServerEnvelopeTransport
from mpreg.core.transport.circuit_breaker import (
    CircuitBreakerConfig,
    CircuitBreakerState,
)


class _StubConnection:
    def __init__(self) -> None:
        self.is_connected = True
        self.sent: list[bytes] = []

    async def send(self, data: bytes) -> None:
        self.sent.append(data)


class _StubServer:
    def __init__(self, connections: dict[str, _StubConnection]) -> None:
        self._connections = connections

    def _get_all_peer_connections(self) -> dict[str, _StubConnection]:
        return self._connections


@pytest.mark.asyncio
async def test_server_envelope_transport_sends() -> None:
    connection = _StubConnection()
    server = _StubServer({"ws://peer": connection})
    transport = ServerEnvelopeTransport(
        server=server,
        serializer=JsonSerializer(),
    )

    envelope = FabricMessageEnvelope(payload={"type": "ping"})
    sent = await transport.send_envelope("ws://peer", envelope)

    assert sent is True
    assert connection.sent


def test_server_envelope_transport_peer_ids() -> None:
    server = _StubServer({"ws://b": _StubConnection(), "ws://a": _StubConnection()})
    transport = ServerEnvelopeTransport(server=server, serializer=JsonSerializer())

    assert transport.peer_ids() == ("ws://a", "ws://b")
    assert transport.peer_ids(exclude="ws://a") == ("ws://b",)


class _FailingConnection:
    def __init__(self) -> None:
        self.is_connected = True

    async def send(self, data: bytes) -> None:
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_server_envelope_transport_circuit_breaker_opens() -> None:
    connection = _FailingConnection()
    server = _StubServer({"ws://peer": connection})
    transport = ServerEnvelopeTransport(
        server=server,
        serializer=JsonSerializer(),
        circuit_breaker_config=CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout_ms=60000.0,
            success_threshold=1,
        ),
    )

    envelope = FabricMessageEnvelope(payload={"type": "ping"})
    sent = await transport.send_envelope("ws://peer", envelope)

    assert sent is False
    breaker = transport._circuit_breakers["ws://peer"]
    assert breaker.state is CircuitBreakerState.OPEN
    assert await transport.send_envelope("ws://peer", envelope) is False
