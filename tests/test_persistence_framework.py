import asyncio
import time

import pytest

from mpreg.core.global_cache import (
    CacheLevel,
    CacheMetadata,
    CacheOptions,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.core.message_queue import DeliveryGuarantee, MessageQueue, QueueConfiguration
from mpreg.core.persistence.backend import SQLitePersistenceBackend
from mpreg.core.persistence.config import PersistenceConfig, PersistenceMode
from mpreg.core.persistence.kv_store import MemoryKeyValueStore
from mpreg.core.persistence.registry import PersistenceRegistry


@pytest.mark.asyncio
async def test_memory_kv_store_ttl() -> None:
    store = MemoryKeyValueStore()
    await store.put("key", b"value", expires_at=time.time() + 0.1)
    assert await store.get("key") == b"value"
    await asyncio.sleep(0.2)
    assert await store.get("key") is None


@pytest.mark.asyncio
async def test_sqlite_kv_store_roundtrip(tmp_path) -> None:
    backend = SQLitePersistenceBackend(db_path=tmp_path / "kv.sqlite")
    await backend.open()
    store = backend.key_value_store("test")
    await store.put("alpha", b"beta")
    assert await store.get("alpha") == b"beta"
    await backend.close()


@pytest.mark.asyncio
async def test_queue_persistence_roundtrip(tmp_path) -> None:
    backend = SQLitePersistenceBackend(db_path=tmp_path / "queue.sqlite")
    await backend.open()
    store = backend.queue_store("mpreg:queues", "jobs")
    config = QueueConfiguration(name="jobs")
    await store.save_config(config)

    queue = MessageQueue(config, queue_store=store, autostart=False)
    result = await queue.send_message(
        "jobs.created", {"job": "run"}, DeliveryGuarantee.AT_LEAST_ONCE
    )
    assert result.success
    await queue.shutdown()

    queue_restarted = MessageQueue(config, queue_store=store, autostart=False)
    await queue_restarted.restore_from_store()
    assert len(queue_restarted.pending_messages) == 1
    await queue_restarted.shutdown()
    await backend.close()


@pytest.mark.asyncio
async def test_cache_l2_persistence_roundtrip(tmp_path) -> None:
    registry = PersistenceRegistry(
        PersistenceConfig(mode=PersistenceMode.SQLITE, data_dir=tmp_path)
    )
    await registry.open()

    cache_config = GlobalCacheConfiguration(
        enable_l2_persistent=True,
        enable_l3_distributed=False,
        enable_l4_federation=False,
        local_cluster_id="cache-test",
    )
    cache = GlobalCacheManager(cache_config, persistence_registry=registry)
    key = GlobalCacheKey.from_data("cache.ns", {"item": "x"})
    await cache.put(
        key,
        {"value": 42},
        CacheMetadata(ttl_seconds=60.0),
        options=CacheOptions(cache_levels=frozenset([CacheLevel.L2])),
    )
    await cache.shutdown()

    cache_restarted = GlobalCacheManager(cache_config, persistence_registry=registry)
    result = await cache_restarted.get(
        key, options=CacheOptions(cache_levels=frozenset([CacheLevel.L2]))
    )
    assert result.success
    await cache_restarted.shutdown()
    await registry.close()
