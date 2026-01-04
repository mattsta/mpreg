"""Tests for auto-allocated port handling in MultiProtocolAdapter."""

from __future__ import annotations

import asyncio
import socket

import pytest

from mpreg.core.transport.adapter_registry import get_adapter_endpoint_registry
from mpreg.core.transport.factory import (
    MultiProtocolAdapter,
    MultiProtocolAdapterConfig,
)
from mpreg.core.transport.interfaces import TransportProtocol


def _port_available(port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("127.0.0.1", port))
            return True
    except OSError:
        return False


async def _wait_for_port_free(port: int, attempts: int = 20) -> bool:
    for _ in range(attempts):
        if _port_available(port):
            return True
        await asyncio.sleep(0.05)
    return False


@pytest.mark.asyncio
async def test_multi_protocol_adapter_auto_port_assignment() -> None:
    """Ensure auto-allocated ports invoke the assignment callback."""
    registry = get_adapter_endpoint_registry()
    registry.clear()
    assigned = []

    def _callback(assignment) -> None:
        assigned.append(assignment)

    config = MultiProtocolAdapterConfig(
        base_port=0,
        port_category="testing",
        port_assignment_callback=_callback,
    )
    adapter = MultiProtocolAdapter(config)

    try:
        await adapter.start([TransportProtocol.WEBSOCKET])
        assert assigned, "Expected port assignment callback to run"
        assignment = assigned[0]
        assert assignment.protocol == TransportProtocol.WEBSOCKET
        assert assignment.port > 0
        assert assignment.endpoint.startswith("ws://")
        snapshot = registry.snapshot()
        assert "client" in snapshot
        assert "ws" in snapshot["client"]
    finally:
        await adapter.stop()


@pytest.mark.asyncio
async def test_remove_protocol_releases_auto_port() -> None:
    """Ensure remove_protocol releases auto-assigned ports."""
    registry = get_adapter_endpoint_registry()
    registry.clear()
    config = MultiProtocolAdapterConfig(base_port=0, port_category="testing")
    adapter = MultiProtocolAdapter(config)

    try:
        await adapter.start([TransportProtocol.WEBSOCKET])
        port = adapter._protocol_ports.get(TransportProtocol.WEBSOCKET)
        assert port is not None

        await adapter.remove_protocol(TransportProtocol.WEBSOCKET)
        assert TransportProtocol.WEBSOCKET not in adapter.active_protocols
        assert TransportProtocol.WEBSOCKET not in adapter._protocol_ports
        assert await _wait_for_port_free(port)
        snapshot = registry.snapshot()
        assert snapshot.get("client", {}).get("ws") is None
    finally:
        await adapter.stop()
