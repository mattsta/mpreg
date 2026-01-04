"""Forwarding metadata for fabric pub/sub routing."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from mpreg.datastructures.type_aliases import HopCount, NodeId

FABRIC_PUBSUB_FORWARDING_KEY = "fabric_forwarding"


@dataclass(frozen=True, slots=True)
class PubSubForwardingMetadata:
    origin_node: NodeId
    routing_path: tuple[NodeId, ...] = ()
    max_hops: HopCount | None = None

    @property
    def hop_count(self) -> int:
        if not self.routing_path:
            return 0
        return max(0, len(self.routing_path) - 1)

    def has_visited(self, node_id: NodeId) -> bool:
        return node_id in self.routing_path

    def can_forward(self) -> bool:
        if self.max_hops is None:
            return True
        return self.hop_count < self.max_hops

    def with_hop(
        self, node_id: NodeId, *, max_hops: HopCount | None = None
    ) -> PubSubForwardingMetadata:
        if self.routing_path and self.routing_path[-1] == node_id:
            path = self.routing_path
        else:
            path = (*self.routing_path, node_id)
        return PubSubForwardingMetadata(
            origin_node=self.origin_node,
            routing_path=path,
            max_hops=self.max_hops if max_hops is None else max_hops,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "origin_node": self.origin_node,
            "routing_path": list(self.routing_path),
            "max_hops": self.max_hops,
        }

    def with_headers(self, headers: Mapping[str, Any]) -> dict[str, Any]:
        updated = dict(headers)
        updated[FABRIC_PUBSUB_FORWARDING_KEY] = self.to_dict()
        return updated

    @classmethod
    def from_headers(
        cls, headers: Mapping[str, Any]
    ) -> PubSubForwardingMetadata | None:
        payload = headers.get(FABRIC_PUBSUB_FORWARDING_KEY)
        if not isinstance(payload, Mapping):
            return None
        origin_node = str(payload.get("origin_node", ""))
        if not origin_node:
            return None
        raw_path = payload.get("routing_path", ())
        if isinstance(raw_path, (list, tuple)):
            path = tuple(str(item) for item in raw_path)
        else:
            path = ()
        max_hops = payload.get("max_hops")
        parsed_max_hops: HopCount | None = None
        if max_hops is not None:
            try:
                parsed_max_hops = int(max_hops)
            except (TypeError, ValueError):
                parsed_max_hops = None
        return cls(
            origin_node=origin_node,
            routing_path=path,
            max_hops=parsed_max_hops,
        )
