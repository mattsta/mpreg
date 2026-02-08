from __future__ import annotations

import json
from dataclasses import dataclass

from mpreg.datastructures.rpc_spec import RpcSpec
from mpreg.datastructures.type_aliases import NamespaceName


@dataclass(frozen=True, slots=True)
class RpcSpecSharePolicy:
    """Policy for attaching full RPC specs to catalog gossip entries."""

    mode: str = "summary"
    namespaces: tuple[NamespaceName, ...] = ()
    max_bytes: int | None = None

    def include_spec(self, spec: RpcSpec) -> bool:
        if self.mode != "full":
            return False
        if self.namespaces:
            namespace_match = any(
                _namespace_filter_matches(prefix, spec.namespace)
                or _namespace_filter_matches(prefix, spec.identity.name)
                for prefix in self.namespaces
            )
            if not namespace_match:
                return False
        if self.max_bytes is not None:
            payload = spec.to_dict()
            encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
            if len(encoded.encode("utf-8")) > self.max_bytes:
                return False
        return True


def _namespace_filter_matches(namespace_filter: str, value: str) -> bool:
    if not namespace_filter:
        return True
    if not value:
        return False
    if value == namespace_filter:
        return True
    return value.startswith(f"{namespace_filter}.")
