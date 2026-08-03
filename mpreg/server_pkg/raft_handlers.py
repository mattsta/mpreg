"""Raft plane handlers peeled from the server composition root.

Keeps FabricRaftTransport lifecycle and operator status out of ``server.py``
business methods while remaining a thin adapter over product types.
"""

from __future__ import annotations

from typing import Any

from mpreg.consensus import status_dict

class RaftPlane:
    """Owns registered Raft nodes and fabric transport hooks for one server."""

    def __init__(self) -> None:
        self.transport: Any | None = None
        self._nodes: list[Any] = []

    def bind_transport(self, transport: Any) -> None:
        self.transport = transport

    def register_node(self, node: Any) -> None:
        if self.transport is None:
            raise RuntimeError("Fabric raft transport is not initialized")
        self.transport.register_node(node)
        if node not in self._nodes:
            self._nodes.append(node)

    def status(self) -> dict[str, Any]:
        nodes = [status_dict(n) for n in self._nodes]
        return {
            "configured": self.transport is not None,
            "registered": len(nodes),
            "nodes": nodes,
            "membership_change_supported": False,
            "snapshot_supported": True,
        }

    async def handle_message(
        self, message: Any, *, source_peer_url: str | None
    ) -> bool:
        if self.transport is None:
            return False
        return bool(
            await self.transport.handle_message(
                message, source_peer_url=source_peer_url
            )
        )
