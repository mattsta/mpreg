from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import orjson

class Serializer(ABC):
    """Abstract base class for data serialization."""

    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        """Serializes data into bytes."""
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """Deserializes bytes into data."""
        pass

@dataclass(slots=True)
class JsonSerializer(Serializer):
    """Serializer implementation using orjson for JSON serialization."""

    def serialize(self, data: Any) -> bytes:
        """Serializes data to JSON bytes using orjson."""

        # orjson can't serialize frozenset directly, convert to list
        def default(obj: Any) -> Any:
            if isinstance(obj, frozenset):
                return list(obj)
            # PERF-03: accept pydantic / objects with model_dump without a
            # prior materialize step on the call site.
            dump = getattr(obj, "model_dump", None)
            if callable(dump):
                return dump()
            raise TypeError

        return orjson.dumps(data, default=default)

    def serialize_model(self, model: Any) -> bytes:
        """Serialize a pydantic-like model in one hop (PERF-03).

        Prefer this over ``serialize(model.model_dump())`` on hot paths so
        frozenset conversion and dump share a single orjson pass when possible.
        """
        if hasattr(model, "model_dump") and callable(model.model_dump):
            return self.serialize(model.model_dump())
        return self.serialize(model)

    def deserialize(self, data: bytes) -> Any:
        """Deserializes JSON bytes to data using orjson."""
        return orjson.loads(data)
