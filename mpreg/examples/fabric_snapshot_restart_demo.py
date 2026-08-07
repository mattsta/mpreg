#!/usr/bin/env python3
"""Demo: fabric catalog + route key snapshots across restart."""

from __future__ import annotations

import asyncio
import tempfile
import time
from dataclasses import replace
from pathlib import Path

from mpreg.core.config import MPREGSettings
from mpreg.core.persistence.config import PersistenceConfig, PersistenceMode
from mpreg.core.port_allocator import port_range_context
from mpreg.fabric.federation_config import create_permissive_bridging_config
from mpreg.fabric.route_keys import RouteKeyRegistry
from mpreg.server import MPREGServer


async def _start_server(
    port: int,
    name: str,
    cluster_id: str,
    *,
    connect: str | None = None,
    persistence_dir: Path | None = None,
    route_keys: RouteKeyRegistry | None = None,
) -> tuple[MPREGServer, asyncio.Task[None]]:
    persistence_config = None
    if persistence_dir is not None:
        persistence_config = PersistenceConfig(
            mode=PersistenceMode.SQLITE,
            data_dir=persistence_dir,
        )
    federation_config = replace(
        create_permissive_bridging_config(cluster_id),
        log_cross_federation_attempts=False,
        log_cross_federation_warnings=False,
    )
    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=name,
        cluster_id=cluster_id,
        connect=connect,
        federation_config=federation_config,
        monitoring_enabled=False,
        enable_default_cache=False,
        enable_default_queue=False,
        persistence_config=persistence_config,
        fabric_route_key_registry=route_keys,
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


async def _wait_for_function(
    server: MPREGServer, function_name: str, *, timeout: float = 5.0
) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        catalog = server._fabric_control_plane.catalog
        if any(
            entry.identity.name == function_name
            for entry in catalog.functions.entries()
        ):
            return True
        await asyncio.sleep(0.1)
    return False


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        data_dir = Path(tmp_dir)
        with port_range_context(2, "servers") as ports:
            local_port, remote_port = ports
            remote_url = f"ws://127.0.0.1:{remote_port}"

            remote_server, remote_task = await _start_server(
                remote_port,
                "Fabric-Remote",
                "cluster-remote",
            )

            route_keys = RouteKeyRegistry()
            local_server, local_task = await _start_server(
                local_port,
                "Fabric-Local",
                "cluster-local",
                connect=remote_url,
                persistence_dir=data_dir,
                route_keys=route_keys,
            )

            def remote_echo() -> str:
                return "ok"

            remote_server.register_command("demo.remote", remote_echo, [])
            await _wait_for_function(local_server, "demo.remote")

            route_keys.register_key(
                cluster_id="cluster-remote",
                public_key=b"demo-route-key",
            )

            await _stop_server(local_server, local_task)
            await _stop_server(remote_server, remote_task)

            restored_keys = RouteKeyRegistry()
            restored_server, restored_task = await _start_server(
                local_port,
                "Fabric-Restart",
                "cluster-local",
                persistence_dir=data_dir,
                route_keys=restored_keys,
            )
            restored = await _wait_for_function(
                restored_server, "demo.remote", timeout=2.0
            )
            print("Remote function restored:", restored)
            print(
                "Route keys restored:",
                bool(restored_keys.resolve_public_keys("cluster-remote")),
            )
            await _stop_server(restored_server, restored_task)


if __name__ == "__main__":
    asyncio.run(main())
