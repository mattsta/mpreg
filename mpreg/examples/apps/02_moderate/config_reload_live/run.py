"""L2 config_reload_live — SQLite cache+queue survive restart (pers.*)."""

from __future__ import annotations

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
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.server import MPREGServer


async def _start(
    port: int, data_dir: Path, name: str
) -> tuple[MPREGServer, asyncio.Task[None]]:
    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=name,
        cluster_id="config-reload",
        resources={"cache", "queue"},
        monitoring_enabled=False,
        enable_default_cache=True,
        enable_default_queue=True,
        log_level="WARNING",
        persistence_config=PersistenceConfig(
            mode=PersistenceMode.SQLITE,
            data_dir=data_dir,
        ),
    )
    server = MPREGServer(settings)
    task = asyncio.create_task(server.server())
    await asyncio.sleep(1.0)
    return server, task


async def _stop(server: MPREGServer, task: asyncio.Task[None]) -> None:
    await server.shutdown_async()
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except TimeoutError:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def main() -> None:
    with (
        app_run(
            "config_reload_live",
            "Config Reload — SQLite restart durability",
            level="L2",
        ),
        tempfile.TemporaryDirectory() as tmp,
    ):
        data_dir = Path(tmp)
        key = GlobalCacheKey.from_data("config.reload", {"k": "theme"})

        with (
            scenario(
                "primary write cache L2 + queue",
                "pers.sqlite_kv",
                "pers.sqlite_queue",
                "cache.l2",
                "queue.create",
            ),
            port_context("servers") as port,
        ):
            step("start primary, write cache + queue")
            server, task = await _start(port, data_dir, "Config-Primary")
            try:
                ensure(server._queue_manager is not None, "no queue manager")
                ensure(server._cache_manager is not None, "no cache manager")
                await server._queue_manager.create_queue("jobs")
                await server._cache_manager.put(
                    key,
                    {"theme": "dark", "version": 1},
                    CacheMetadata(ttl_seconds=600.0),
                    options=CacheOptions(cache_levels=frozenset([CacheLevel.L2])),
                )
                # prove L2 read on same process
                hit = await server._cache_manager.get(
                    key,
                    options=CacheOptions(cache_levels=frozenset([CacheLevel.L2])),
                )
                ensure(
                    hit.success and hit.entry is not None,
                    "L2 miss before restart",
                )
                ensure(
                    hit.entry.value.get("theme") == "dark",
                    f"bad pre {hit.entry.value}",
                )
                ok(f"primary wrote theme={hit.entry.value}")
            finally:
                await _stop(server, task)

        with (
            scenario(
                "restart same data_dir restores state",
                "pers.restart",
                "cache.l2",
                "queue.create",
            ),
            port_context("servers") as port,
        ):
            step("restart on same data_dir")
            server, task = await _start(port, data_dir, "Config-Restart")
            try:
                ensure(server._queue_manager is not None, "no queue after restart")
                ensure(server._cache_manager is not None, "no cache after restart")
                queues = server._queue_manager.list_queues()
                ensure("jobs" in queues, f"queue missing after restart: {queues}")
                hit = await server._cache_manager.get(
                    key,
                    options=CacheOptions(cache_levels=frozenset([CacheLevel.L2])),
                )
                ensure(
                    hit.success and hit.entry is not None,
                    "cache miss after restart",
                )
                ensure(
                    hit.entry.value.get("theme") == "dark",
                    f"bad {hit.entry.value}",
                )
                ensure(
                    hit.entry.value.get("version") == 1,
                    f"version {hit.entry.value}",
                )
                ok(f"reload live queues={queues} cache={hit.entry.value}")
                step("non-claim: not multi-node shared storage; single data_dir demo")
            finally:
                await _stop(server, task)


if __name__ == "__main__":
    asyncio.run(main())
