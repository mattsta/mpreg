#!/usr/bin/env python3
"""Probe connectivity behavior for federated RPC memory-load scenario."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import get_port_allocator
from mpreg.server import MPREGServer
from tests.test_helpers import wait_for_condition

type NodeIndex = int


@dataclass(slots=True)
class NodeProbeSnapshot:
    node_index: NodeIndex
    port: int
    task_done: bool
    task_exception: str | None
    listener_ready: bool
    discovered_peers: int
    connected_peers: int
    registered_functions: int
    pending_catalog_updates: int


@dataclass(slots=True)
class ConnectAttemptResult:
    node_index: NodeIndex
    port: int
    attempt: int
    success: bool
    duration_seconds: float
    error: str | None


def _build_settings(ports: list[int]) -> list[MPREGSettings]:
    settings_list: list[MPREGSettings] = []
    for index, port in enumerate(ports):
        connect_to = f"ws://127.0.0.1:{ports[0]}" if index > 0 else None
        settings_list.append(
            MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"Probe-Memory-Node-{index + 1}",
                cluster_id="probe-memory-test-cluster",
                resources={f"probe-memory-{index + 1}"},
                peers=None,
                connect=connect_to,
                advertised_urls=None,
                gossip_interval=1.0,
            )
        )
    return settings_list


def _make_memory_function(server_index: int, function_index: int):
    def memory_function(data: str) -> str:
        return f"Probe S{server_index}F{function_index}: {data}"

    return memory_function


def _snapshot_server(
    server: MPREGServer,
    task: asyncio.Task[None],
    *,
    node_index: int,
    port: int,
) -> NodeProbeSnapshot:
    task_exception: str | None = None
    if task.done():
        try:
            exception = task.exception()
            if exception is not None:
                task_exception = repr(exception)
        except Exception as exc:
            task_exception = repr(exc)

    pending_catalog_updates = -1
    if server._fabric_control_plane is not None:
        from mpreg.fabric.gossip import GossipMessageType

        pending_catalog_updates = sum(
            1
            for message in server._fabric_control_plane.gossip.pending_messages
            if message.message_type is GossipMessageType.CATALOG_UPDATE
        )

    connected_peers = sum(
        1
        for connection in server._get_all_peer_connections().values()
        if connection.is_connected
    )

    return NodeProbeSnapshot(
        node_index=node_index,
        port=port,
        task_done=task.done(),
        task_exception=task_exception,
        listener_ready=server._transport_listener is not None,
        discovered_peers=max(len(server.cluster.servers) - 1, 0),
        connected_peers=connected_peers,
        registered_functions=len(server.cluster.funtimes),
        pending_catalog_updates=pending_catalog_updates,
    )


async def _attempt_connect(
    *,
    node_index: int,
    port: int,
    attempt: int,
) -> ConnectAttemptResult:
    url = f"ws://127.0.0.1:{port}"
    client = MPREGClientAPI(url)
    started = time.monotonic()
    try:
        await client.connect()
        await client.disconnect()
        return ConnectAttemptResult(
            node_index=node_index,
            port=port,
            attempt=attempt,
            success=True,
            duration_seconds=time.monotonic() - started,
            error=None,
        )
    except Exception as exc:
        return ConnectAttemptResult(
            node_index=node_index,
            port=port,
            attempt=attempt,
            success=False,
            duration_seconds=time.monotonic() - started,
            error=repr(exc),
        )


async def _attempt_connect_reuse_client(
    *,
    node_index: int,
    port: int,
    attempt: int,
    client: MPREGClientAPI,
) -> ConnectAttemptResult:
    started = time.monotonic()
    try:
        await client.connect()
        await client.disconnect()
        return ConnectAttemptResult(
            node_index=node_index,
            port=port,
            attempt=attempt,
            success=True,
            duration_seconds=time.monotonic() - started,
            error=None,
        )
    except Exception as exc:
        return ConnectAttemptResult(
            node_index=node_index,
            port=port,
            attempt=attempt,
            success=False,
            duration_seconds=time.monotonic() - started,
            error=repr(exc),
        )


async def run_probe(*, num_functions: int = 100, connect_attempts: int = 3) -> None:
    allocator = get_port_allocator()
    ports = allocator.allocate_port_range(3, "servers")

    servers: list[MPREGServer] = []
    tasks: list[asyncio.Task[None]] = []

    try:
        settings_list = _build_settings(ports)
        servers = [MPREGServer(settings=settings) for settings in settings_list]

        print(f"Probe ports: {ports}")
        for server in servers:
            task = asyncio.create_task(server.server())
            tasks.append(task)
            await asyncio.sleep(0.5)

        await wait_for_condition(
            lambda: all(server._transport_listener is not None for server in servers),
            timeout=15.0,
            interval=0.1,
            error_message="Servers failed to start listeners during probe",
        )
        await asyncio.sleep(1.0)

        print("Registering probe functions...")
        for server_index, server in enumerate(servers):
            for function_index in range(num_functions):
                function = _make_memory_function(server_index, function_index)
                server.register_command(
                    f"probe_memory_function_{server_index}_{function_index}",
                    function,
                    [f"probe-memory-{server_index + 1}"],
                )

        await asyncio.sleep(5.0)

        print("\nNode snapshots before client connect attempts:")
        snapshots = [
            _snapshot_server(
                server,
                tasks[index],
                node_index=index,
                port=ports[index],
            )
            for index, server in enumerate(servers)
        ]
        for snapshot in snapshots:
            print(snapshot)

        print("\nSingle-node retry sequence (node 0 only):")
        single_node_client = MPREGClientAPI(f"ws://127.0.0.1:{ports[0]}")
        for attempt in range(1, connect_attempts + 1):
            result = await _attempt_connect_reuse_client(
                node_index=0,
                port=ports[0],
                attempt=attempt,
                client=single_node_client,
            )
            print(result)
            await asyncio.sleep(0.5)

        print("\nClient connect attempts:")
        for attempt in range(1, connect_attempts + 1):
            for index, port in enumerate(ports):
                result = await _attempt_connect(
                    node_index=index,
                    port=port,
                    attempt=attempt,
                )
                print(result)
            await asyncio.sleep(0.5)

        print("\nReused-client attempts (node 0):")
        reused_client = MPREGClientAPI(f"ws://127.0.0.1:{ports[0]}")
        for attempt in range(1, connect_attempts + 1):
            result = await _attempt_connect_reuse_client(
                node_index=0,
                port=ports[0],
                attempt=attempt,
                client=reused_client,
            )
            print(result)
            await asyncio.sleep(0.5)
    finally:
        await asyncio.gather(
            *(server.shutdown_async() for server in servers),
            return_exceptions=True,
        )
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        for port in ports:
            allocator.release_port(port)


if __name__ == "__main__":
    asyncio.run(run_probe())
