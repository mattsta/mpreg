from abc import ABC, abstractmethod
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


class JsonSerializer(Serializer):
    """Serializer implementation using orjson for JSON serialization."""

    def serialize(self, data: Any) -> bytes:
        """Serializes data to JSON bytes using orjson."""

        # orjson can't serialize frozenset directly, convert to list
        def default(obj: Any) -> Any:
            if isinstance(obj, frozenset):
                return list(obj)
            raise TypeError

        return orjson.dumps(data, default=default)

    def deserialize(self, data: bytes) -> Any:
        """Deserializes JSON bytes to data using orjson."""
        return orjson.loads(data)
