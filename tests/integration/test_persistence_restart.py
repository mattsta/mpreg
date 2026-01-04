import asyncio

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.global_cache import (
    CacheLevel,
    CacheMetadata,
    CacheOptions,
    GlobalCacheKey,
)
from mpreg.core.persistence.config import PersistenceConfig, PersistenceMode
from mpreg.server import MPREGServer
from tests.test_helpers import TestPortManager, wait_for_condition


async def _start_server(
    port: int,
    data_dir,
    *,
    enable_cache: bool,
    enable_queue: bool,
    name: str,
) -> tuple[MPREGServer, asyncio.Task[None]]:
    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=name,
        cluster_id="persist-test",
        resources={"cache", "queue"},
        monitoring_enabled=False,
        enable_default_cache=enable_cache,
        enable_default_queue=enable_queue,
        persistence_config=PersistenceConfig(
            mode=PersistenceMode.SQLITE,
            data_dir=data_dir,
        ),
    )
    server = MPREGServer(settings)
    task = asyncio.create_task(server.server())
    await asyncio.sleep(1.0)
    return server, task


async def _stop_server(server: MPREGServer, task: asyncio.Task[None]) -> None:
    await server.shutdown_async()
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except TimeoutError:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_persistent_queue_restores_after_restart(tmp_path) -> None:
    data_dir = tmp_path / "queue"
    with TestPortManager() as ports:
        server_a, task_a = await _start_server(
            ports.get_server_port(),
            data_dir,
            enable_cache=False,
            enable_queue=True,
            name="Persist-Queue-A",
        )
        assert server_a._queue_manager is not None
        await server_a._queue_manager.create_queue("jobs")
        assert "jobs" in server_a._queue_manager.list_queues()
        await _stop_server(server_a, task_a)

        server_b, task_b = await _start_server(
            ports.get_server_port(),
            data_dir,
            enable_cache=False,
            enable_queue=True,
            name="Persist-Queue-B",
        )

        await wait_for_condition(
            lambda: "jobs" in server_b._queue_manager.list_queues(),
            timeout=5.0,
            interval=0.1,
            error_message="Persisted queue not restored after restart",
        )
        await _stop_server(server_b, task_b)


@pytest.mark.asyncio
async def test_persistent_cache_l2_restore_after_restart(tmp_path) -> None:
    data_dir = tmp_path / "cache"
    with TestPortManager() as ports:
        server_a, task_a = await _start_server(
            ports.get_server_port(),
            data_dir,
            enable_cache=True,
            enable_queue=False,
            name="Persist-Cache-A",
        )
        assert server_a._cache_manager is not None
        key = GlobalCacheKey.from_data("persist.cache", {"item": "x"})
        await server_a._cache_manager.put(
            key,
            {"status": "ready"},
            CacheMetadata(ttl_seconds=300.0),
            options=CacheOptions(cache_levels=frozenset([CacheLevel.L2])),
        )
        await _stop_server(server_a, task_a)

        server_b, task_b = await _start_server(
            ports.get_server_port(),
            data_dir,
            enable_cache=True,
            enable_queue=False,
            name="Persist-Cache-B",
        )
        result = await server_b._cache_manager.get(
            key, options=CacheOptions(cache_levels=frozenset([CacheLevel.L2]))
        )
        assert result.success is True
        await _stop_server(server_b, task_b)
