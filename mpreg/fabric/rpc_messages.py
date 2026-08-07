"""Typed payloads for fabric RPC messages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from mpreg.datastructures.type_aliases import (
    ClusterId,
    FunctionId,
    FunctionName,
    HopCount,
    JsonDict,
    NodeId,
    RequestId,
    VersionConstraintSpec,
)

FABRIC_RPC_REQUEST_KIND = "rpc-request"
FABRIC_RPC_RESPONSE_KIND = "rpc-response"


def _as_tuple(value: object) -> tuple[Any, ...]:
    if isinstance(value, tuple):
        return value
    if isinstance(value, list):
        return tuple(value)
    return ()


def _as_str_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    return ()


def _as_kwargs(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return {str(key): val for key, val in value.items()}
    return {}


def _as_optional_str(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _as_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float, str)):
        try:
            return int(value)
        except TypeError, ValueError:
            return None
    return None


@dataclass(frozen=True, slots=True)
class FabricRPCRequest:
    request_id: RequestId
    command: FunctionName
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    resources: tuple[str, ...] = field(default_factory=tuple)
    function_id: FunctionId | None = None
    version_constraint: VersionConstraintSpec | None = None
    target_cluster: ClusterId | None = None
    target_node: NodeId | None = None
    reply_to: NodeId = ""
    federation_path: tuple[ClusterId, ...] = field(default_factory=tuple)
    federation_remaining_hops: HopCount | None = None

    def to_dict(self) -> JsonDict:
        return {
            "kind": FABRIC_RPC_REQUEST_KIND,
            "request_id": self.request_id,
            "command": self.command,
            "args": list(self.args),
            "kwargs": dict(self.kwargs),
            "resources": list(self.resources),
            "function_id": self.function_id,
            "version_constraint": self.version_constraint,
            "target_cluster": self.target_cluster,
            "target_node": self.target_node,
            "reply_to": self.reply_to,
            "federation_path": list(self.federation_path),
            "federation_remaining_hops": self.federation_remaining_hops,
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> FabricRPCRequest:
        return cls(
            request_id=str(payload.get("request_id", "")),
            command=str(payload.get("command", "")),
            args=_as_tuple(payload.get("args", ())),
            kwargs=_as_kwargs(payload.get("kwargs", {})),
            resources=_as_str_tuple(payload.get("resources", ())),
            function_id=_as_optional_str(payload.get("function_id")),
            version_constraint=_as_optional_str(payload.get("version_constraint")),
            target_cluster=_as_optional_str(payload.get("target_cluster")),
            target_node=_as_optional_str(payload.get("target_node")),
            reply_to=str(payload.get("reply_to", "")),
            federation_path=_as_str_tuple(payload.get("federation_path", ())),
            federation_remaining_hops=_as_optional_int(
                payload.get("federation_remaining_hops")
            ),
        )


@dataclass(frozen=True, slots=True)
class FabricRPCResponse:
    request_id: RequestId
    success: bool
    result: Any | None = None
    error: dict[str, Any] | None = None
    responder: NodeId = ""
    reply_to: NodeId = ""
    reply_cluster: ClusterId | None = None
    routing_path: tuple[NodeId, ...] = field(default_factory=tuple)

    def to_dict(self) -> JsonDict:
        return {
            "kind": FABRIC_RPC_RESPONSE_KIND,
            "request_id": self.request_id,
            "success": bool(self.success),
            "result": self.result,
            "error": dict(self.error) if self.error else None,
            "responder": self.responder,
            "reply_to": self.reply_to,
            "reply_cluster": self.reply_cluster,
            "routing_path": list(self.routing_path),
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> FabricRPCResponse:
        raw_error = payload.get("error")
        return cls(
            request_id=str(payload.get("request_id", "")),
            success=bool(payload.get("success", False)),
            result=payload.get("result"),
            error=dict(raw_error) if isinstance(raw_error, dict) else None,
            responder=str(payload.get("responder", "")),
            reply_to=str(payload.get("reply_to", "")),
            reply_cluster=_as_optional_str(payload.get("reply_cluster")),
            routing_path=_as_str_tuple(payload.get("routing_path", ())),
        )
