from __future__ import annotations

from .config import PersistenceConfig, PersistenceMode

__all__ = [
    "PersistenceConfig",
    "PersistenceMode",
    "PersistenceBackend",
    "MemoryPersistenceBackend",
    "SQLitePersistenceBackend",
    "KeyValueStore",
    "MemoryKeyValueStore",
    "SQLiteKeyValueStore",
    "QueueStore",
    "QueuePersistenceState",
    "PersistenceRegistry",
    "CacheL2Store",
]


def __getattr__(name: str):
    if name in {
        "PersistenceBackend",
        "MemoryPersistenceBackend",
        "SQLitePersistenceBackend",
    }:
        from .backend import (
            MemoryPersistenceBackend,
            PersistenceBackend,
            SQLitePersistenceBackend,
        )

        return {
            "PersistenceBackend": PersistenceBackend,
            "MemoryPersistenceBackend": MemoryPersistenceBackend,
            "SQLitePersistenceBackend": SQLitePersistenceBackend,
        }[name]
    if name in {"KeyValueStore", "MemoryKeyValueStore", "SQLiteKeyValueStore"}:
        from .kv_store import KeyValueStore, MemoryKeyValueStore, SQLiteKeyValueStore

        return {
            "KeyValueStore": KeyValueStore,
            "MemoryKeyValueStore": MemoryKeyValueStore,
            "SQLiteKeyValueStore": SQLiteKeyValueStore,
        }[name]
    if name in {"QueueStore", "QueuePersistenceState"}:
        from .queue_store import QueuePersistenceState, QueueStore

        return {
            "QueueStore": QueueStore,
            "QueuePersistenceState": QueuePersistenceState,
        }[name]
    if name == "PersistenceRegistry":
        from .registry import PersistenceRegistry

        return PersistenceRegistry
    if name == "CacheL2Store":
        from .cache_store import CacheL2Store

        return CacheL2Store
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
