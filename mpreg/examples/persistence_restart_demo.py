from __future__ import annotations

#!/usr/bin/env python3
"""Demo: unified persistence across queue + cache after restart."""

import asyncio
import tempfile
from pathlib import Path

from mpreg.core.config import MPREGSettings
from mpreg.core.global_cache import (
    CacheLevel,
    CacheMetadata,
    CacheOptions,
    GlobalCacheKey,
)
from mpreg.core.persistence.config import PersistenceConfig, PersistenceMode
from mpreg.core.port_allocator import port_context
from mpreg.server import MPREGServer


async def _start_server(
    port: int, data_dir: Path, name: str
) -> tuple[MPREGServer, asyncio.Task[None]]:
    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=name,
        cluster_id="persist-demo",
        resources={"cache", "queue"},
        monitoring_enabled=False,
        enable_default_cache=True,
        enable_default_queue=True,
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


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        data_dir = Path(tmp_dir)

        with port_context("servers") as port:
            server, task = await _start_server(port, data_dir, "Persist-First")
            await server._queue_manager.create_queue("jobs")

            key = GlobalCacheKey.from_data("demo.cache", {"job": "alpha"})
            await server._cache_manager.put(
                key,
                {"status": "ready"},
                CacheMetadata(ttl_seconds=300.0),
                options=CacheOptions(cache_levels=frozenset([CacheLevel.L2])),
            )
            await _stop_server(server, task)

        with port_context("servers") as port:
            server, task = await _start_server(port, data_dir, "Persist-Restart")
            print("Queues after restart:", server._queue_manager.list_queues())

            result = await server._cache_manager.get(
                key, options=CacheOptions(cache_levels=frozenset([CacheLevel.L2]))
            )
            print("Cache hit after restart:", result.success)
            await _stop_server(server, task)


if __name__ == "__main__":
    asyncio.run(main())
