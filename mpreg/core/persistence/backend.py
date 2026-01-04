from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .kv_store import KeyValueStore, MemoryKeyValueStore, SQLiteKeyValueStore
from .queue_store import MemoryQueueStore, QueueStore, SQLiteQueueStore


class PersistenceBackend(Protocol):
    """Backend protocol for persistence storage."""

    async def open(self) -> None: ...

    async def close(self) -> None: ...

    def key_value_store(self, namespace: str) -> KeyValueStore: ...

    def queue_store(self, namespace: str, queue_name: str) -> QueueStore: ...

    async def list_queue_names(self, namespace: str) -> list[str]: ...


@dataclass(slots=True)
class MemoryPersistenceBackend:
    """In-memory persistence backend."""

    _kv_stores: dict[str, MemoryKeyValueStore] = field(default_factory=dict)
    _queue_stores: dict[tuple[str, str], MemoryQueueStore] = field(default_factory=dict)

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        self._kv_stores.clear()
        self._queue_stores.clear()

    def key_value_store(self, namespace: str) -> KeyValueStore:
        store = self._kv_stores.get(namespace)
        if store is None:
            store = MemoryKeyValueStore()
            self._kv_stores[namespace] = store
        return store

    def queue_store(self, namespace: str, queue_name: str) -> QueueStore:
        key = (namespace, queue_name)
        store = self._queue_stores.get(key)
        if store is None:
            store = MemoryQueueStore(namespace=namespace, queue_name=queue_name)
            self._queue_stores[key] = store
        return store

    async def list_queue_names(self, namespace: str) -> list[str]:
        return sorted(
            queue_name
            for (store_namespace, queue_name) in self._queue_stores
            if store_namespace == namespace
        )


@dataclass(slots=True)
class SQLitePersistenceBackend:
    """SQLite persistence backend with WAL support."""

    db_path: Path
    wal_mode: bool = True
    synchronous_mode: str = "NORMAL"
    foreign_keys: bool = True
    _conn: sqlite3.Connection | None = field(init=False, default=None)
    _lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)

    async def open(self) -> None:
        if self._conn is not None:
            return
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        await self.execute(
            "PRAGMA journal_mode=WAL" if self.wal_mode else "PRAGMA journal_mode=DELETE"
        )
        await self.execute(f"PRAGMA synchronous={self.synchronous_mode}")
        await self.execute(
            "PRAGMA foreign_keys=ON" if self.foreign_keys else "PRAGMA foreign_keys=OFF"
        )
        await self._initialize_schema()

    async def close(self) -> None:
        if self._conn is None:
            return
        self._conn.close()
        self._conn = None

    def key_value_store(self, namespace: str) -> KeyValueStore:
        return SQLiteKeyValueStore(backend=self, namespace=namespace)

    def queue_store(self, namespace: str, queue_name: str) -> QueueStore:
        return SQLiteQueueStore(
            backend=self, namespace=namespace, queue_name=queue_name
        )

    async def list_queue_names(self, namespace: str) -> list[str]:
        rows = await self.fetch_all(
            "SELECT DISTINCT queue_name FROM queue_meta WHERE namespace=?",
            (namespace,),
        )
        return sorted(row[0] for row in rows)

    async def _initialize_schema(self) -> None:
        await self.execute(
            """
            CREATE TABLE IF NOT EXISTS kv_store (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                value BLOB NOT NULL,
                expires_at REAL,
                PRIMARY KEY (namespace, key)
            )
            """
        )
        await self.execute(
            """
            CREATE TABLE IF NOT EXISTS queue_meta (
                namespace TEXT NOT NULL,
                queue_name TEXT NOT NULL,
                queue_config BLOB,
                next_seq INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (namespace, queue_name)
            )
            """
        )
        await self.execute(
            """
            CREATE TABLE IF NOT EXISTS queue_messages (
                namespace TEXT NOT NULL,
                queue_name TEXT NOT NULL,
                message_id TEXT NOT NULL,
                status TEXT NOT NULL,
                seq INTEGER,
                message BLOB NOT NULL,
                in_flight BLOB,
                updated_at REAL NOT NULL,
                PRIMARY KEY (namespace, queue_name, message_id)
            )
            """
        )
        await self.execute(
            """
            CREATE TABLE IF NOT EXISTS queue_fingerprints (
                namespace TEXT NOT NULL,
                queue_name TEXT NOT NULL,
                timestamp REAL NOT NULL,
                fingerprint TEXT NOT NULL
            )
            """
        )

    async def execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        await self._execute_sync(query, params)

    async def fetch_one(
        self, query: str, params: tuple[Any, ...] = ()
    ) -> tuple[Any, ...] | None:
        rows = await self._fetch_sync(query, params, fetch_one=True)
        return rows

    async def fetch_all(
        self, query: str, params: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        rows = await self._fetch_sync(query, params, fetch_one=False)
        return rows

    async def _execute_sync(self, query: str, params: tuple[Any, ...]) -> None:
        if self._conn is None:
            raise RuntimeError("SQLite backend not opened")
        async with self._lock:
            await asyncio.to_thread(self._execute_blocking, query, params)

    def _execute_blocking(self, query: str, params: tuple[Any, ...]) -> None:
        if self._conn is None:
            return
        cursor = self._conn.cursor()
        cursor.execute(query, params)
        self._conn.commit()
        cursor.close()

    async def _fetch_sync(
        self, query: str, params: tuple[Any, ...], *, fetch_one: bool
    ) -> Any:
        if self._conn is None:
            raise RuntimeError("SQLite backend not opened")
        async with self._lock:
            return await asyncio.to_thread(
                self._fetch_blocking, query, params, fetch_one
            )

    def _fetch_blocking(
        self, query: str, params: tuple[Any, ...], fetch_one: bool
    ) -> Any:
        if self._conn is None:
            return None
        cursor = self._conn.cursor()
        cursor.execute(query, params)
        if fetch_one:
            row = cursor.fetchone()
            cursor.close()
            return row
        rows = cursor.fetchall()
        cursor.close()
        return rows
