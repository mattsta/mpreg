from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from loguru import logger

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mpreg.core.persistence.backend import SQLitePersistenceBackend


class KeyValueStore(Protocol):
    """Key/value persistence interface with optional TTL support."""

    async def get(self, key: str) -> bytes | None: ...

    async def put(
        self, key: str, value: bytes, *, expires_at: float | None = None
    ) -> None: ...

    async def delete(self, key: str) -> None: ...

    async def list_prefix(self, prefix: str) -> list[tuple[str, bytes]]: ...

    async def close(self) -> None: ...


@dataclass(slots=True)
class MemoryKeyValueStore:
    """In-memory key/value store for development and tests."""

    _entries: dict[str, tuple[bytes, float | None]] = field(default_factory=dict)

    async def get(self, key: str) -> bytes | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at is not None and time.time() >= expires_at:
            self._entries.pop(key, None)
            return None
        return value

    async def put(
        self, key: str, value: bytes, *, expires_at: float | None = None
    ) -> None:
        self._entries[key] = (value, expires_at)

    async def delete(self, key: str) -> None:
        self._entries.pop(key, None)

    async def list_prefix(self, prefix: str) -> list[tuple[str, bytes]]:
        now = time.time()
        items: list[tuple[str, bytes]] = []
        expired: list[str] = []
        for key, (value, expires_at) in self._entries.items():
            if expires_at is not None and now >= expires_at:
                expired.append(key)
                continue
            if key.startswith(prefix):
                items.append((key, value))
        for key in expired:
            self._entries.pop(key, None)
        return items

    async def close(self) -> None:
        self._entries.clear()


@dataclass(slots=True)
class SQLiteKeyValueStore:
    """SQLite-backed key/value store."""

    backend: SQLitePersistenceBackend
    namespace: str

    async def get(self, key: str) -> bytes | None:
        row = await self.backend.fetch_one(
            "SELECT value, expires_at FROM kv_store WHERE namespace=? AND key=?",
            (self.namespace, key),
        )
        if row is None:
            return None
        value, expires_at = row
        if expires_at is not None and time.time() >= float(expires_at):
            await self.delete(key)
            return None
        return value

    async def put(
        self, key: str, value: bytes, *, expires_at: float | None = None
    ) -> None:
        await self.backend.execute(
            "INSERT OR REPLACE INTO kv_store (namespace, key, value, expires_at)"
            " VALUES (?, ?, ?, ?)",
            (self.namespace, key, value, expires_at),
        )

    async def delete(self, key: str) -> None:
        await self.backend.execute(
            "DELETE FROM kv_store WHERE namespace=? AND key=?",
            (self.namespace, key),
        )

    async def list_prefix(self, prefix: str) -> list[tuple[str, bytes]]:
        rows = await self.backend.fetch_all(
            "SELECT key, value, expires_at FROM kv_store WHERE namespace=? AND key LIKE ?",
            (self.namespace, f"{prefix}%"),
        )
        now = time.time()
        items: list[tuple[str, bytes]] = []
        expired: list[str] = []
        for key, value, expires_at in rows:
            if expires_at is not None and now >= float(expires_at):
                expired.append(key)
                continue
            items.append((key, value))
        for key in expired:
            await self.delete(key)
        return items

    async def close(self) -> None:
        logger.debug("SQLiteKeyValueStore close: namespace={}", self.namespace)
