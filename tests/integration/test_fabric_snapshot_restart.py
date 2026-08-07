import asyncio
import time

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.persistence.config import PersistenceConfig, PersistenceMode
from mpreg.datastructures.function_identity import FunctionIdentity, SemanticVersion
from mpreg.fabric.catalog import FunctionEndpoint
from mpreg.fabric.route_keys import RouteKeyRegistry
from mpreg.server import MPREGServer
from tests.test_helpers import TestPortManager, wait_for_condition


async def _start_server(
    port: int,
    data_dir,
    *,
    name: str,
    route_key_registry: RouteKeyRegistry,
) -> tuple[MPREGServer, asyncio.Task[None]]:
    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=name,
        cluster_id="fabric-snapshot",
        monitoring_enabled=False,
        enable_default_cache=False,
        enable_default_queue=False,
        persistence_config=PersistenceConfig(
            mode=PersistenceMode.SQLITE,
            data_dir=data_dir,
        ),
        fabric_route_key_registry=route_key_registry,
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
async def test_fabric_snapshot_restores_catalog_and_route_keys(tmp_path) -> None:
    data_dir = tmp_path / "fabric"
    with TestPortManager() as ports:
        route_keys_a = RouteKeyRegistry()
        server_a, task_a = await _start_server(
            ports.get_server_port(),
            data_dir,
            name="Fabric-Snapshot-A",
            route_key_registry=route_keys_a,
        )
        assert server_a._fabric_control_plane is not None
        identity = FunctionIdentity(
            name="snapshot.remote",
            function_id="snapshot.remote",
            version=SemanticVersion.parse("1.0.0"),
        )
        remote_endpoint = FunctionEndpoint(
            identity=identity,
            resources=frozenset({"cpu"}),
            node_id="node-remote",
            cluster_id="cluster-remote",
            advertised_at=time.time(),
            ttl_seconds=300.0,
        )
        server_a._fabric_control_plane.catalog.functions.register(
            remote_endpoint, now=time.time()
        )
        route_keys_a.register_key(
            cluster_id="cluster-remote",
            public_key=b"snapshot-route-key",
            now=time.time(),
        )
        await _stop_server(server_a, task_a)

        route_keys_b = RouteKeyRegistry()
        server_b, task_b = await _start_server(
            ports.get_server_port(),
            data_dir,
            name="Fabric-Snapshot-B",
            route_key_registry=route_keys_b,
        )
        assert server_b._fabric_control_plane is not None

        def _has_remote_function() -> bool:
            entries = server_b._fabric_control_plane.catalog.functions.entries()
            return any(
                entry.identity.name == "snapshot.remote"
                and entry.cluster_id == "cluster-remote"
                for entry in entries
            )

        await wait_for_condition(
            _has_remote_function,
            timeout=5.0,
            interval=0.1,
            error_message="Remote function not restored from fabric snapshot",
        )
        await wait_for_condition(
            lambda: bool(
                route_keys_b.resolve_public_keys("cluster-remote", now=time.time())
            ),
            timeout=5.0,
            interval=0.1,
            error_message="Route keys not restored from fabric snapshot",
        )
        await _stop_server(server_b, task_b)
