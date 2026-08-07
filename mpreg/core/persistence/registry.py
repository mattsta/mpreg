from __future__ import annotations

from dataclasses import dataclass, field

from loguru import logger

from .backend import (
    MemoryPersistenceBackend,
    PersistenceBackend,
    SQLitePersistenceBackend,
)
from .config import PersistenceConfig, PersistenceMode
from .kv_store import KeyValueStore
from .queue_store import QueueStore


@dataclass(slots=True)
class PersistenceRegistry:
    """Registry that provides unified persistence stores."""

    config: PersistenceConfig
    namespace: str = "mpreg"
    _backend: PersistenceBackend | None = field(init=False, default=None)
    _opened: bool = field(init=False, default=False)

    async def open(self) -> None:
        if self._backend is None:
            self._backend = self._create_backend()
        await self._backend.open()
        self._opened = True

    async def close(self) -> None:
        if self._backend is None:
            return
        await self._backend.close()
        self._backend = None
        self._opened = False

    def key_value_store(self, name: str) -> KeyValueStore:
        backend = self._ensure_backend()
        namespace = f"{self.namespace}:{name}"
        return backend.key_value_store(namespace)

    def queue_store(self, queue_name: str) -> QueueStore:
        backend = self._ensure_backend()
        namespace = f"{self.namespace}:queues"
        return backend.queue_store(namespace, queue_name)

    async def list_queue_names(self) -> list[str]:
        backend = self._ensure_backend()
        namespace = f"{self.namespace}:queues"
        return await backend.list_queue_names(namespace)

    def _ensure_backend(self) -> PersistenceBackend:
        if self._backend is None:
            self._backend = self._create_backend()
        if not self._opened:
            raise RuntimeError("PersistenceRegistry.open() must be awaited before use")
        return self._backend

    def _create_backend(self) -> PersistenceBackend:
        if self.config.mode is PersistenceMode.SQLITE:
            logger.info(
                "Initializing SQLite persistence backend at {}",
                self.config.sqlite_path(),
            )
            return SQLitePersistenceBackend(
                db_path=self.config.sqlite_path(),
                wal_mode=self.config.sqlite_wal,
                synchronous_mode=self.config.sqlite_synchronous,
                foreign_keys=self.config.sqlite_foreign_keys,
            )
        logger.info("Initializing in-memory persistence backend")
        return MemoryPersistenceBackend()
