from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from mpreg.core.native_codec import canonical_dumps, dumps, loads


class Serializer(ABC):
    """Abstract base class for data serialization."""

    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        """Serializes data into bytes."""

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """Deserializes bytes into data."""


@dataclass(slots=True)
class JsonSerializer(Serializer):
    """Wire serializer via the process :mod:`native_codec` backend (orjson).

    All hot-path encode/decode should go through this type or
    :func:`mpreg.core.native_codec.dumps` so a future msgpack/simdjson backend
    can be swapped without rewriting call sites.
    """

    def serialize(self, data: Any) -> bytes:
        """Serializes data to JSON bytes using the active native backend."""
        return dumps(data)

    def serialize_canonical(self, data: Any) -> bytes:
        """Key-sorted encode for HMAC / content digests."""
        return canonical_dumps(data)

    def serialize_model(self, model: Any) -> bytes:
        """Serialize a pydantic-like model in one hop (PERF-03).

        Prefer this over ``serialize(model.model_dump())`` on hot paths so
        frozenset conversion and dump share a single native pass when possible.
        """
        if hasattr(model, "model_dump") and callable(model.model_dump):
            return self.serialize(model.model_dump())
        return self.serialize(model)

    def deserialize(self, data: bytes) -> Any:
        """Deserializes JSON bytes to data using the active native backend."""
        return loads(data)
