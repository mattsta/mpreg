from __future__ import annotations

import asyncio
import socket
import traceback

from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import get_port_allocator
from mpreg.server import MPREGServer


def build_settings(node_id: int, port: int, hub_port: int) -> MPREGSettings:
    connect_to = f"ws://127.0.0.1:{hub_port}" if node_id > 0 else None
    return MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=f"AutoDiscover-Node-{node_id}",
        cluster_id="debug-autodiscovery-20",
        resources={f"node-{node_id}", "debug-resource"},
        peers=None,
        connect=connect_to,
        advertised_urls=None,
        gossip_interval=1.0,
        log_level="ERROR",
        monitoring_enabled=False,
        fabric_routing_enabled=False,
    )


async def main() -> None:
    allocator = get_port_allocator()
    ports = allocator.allocate_port_range(20, "servers")
    servers: list[MPREGServer] = []
    tasks: list[asyncio.Task[None]] = []
    seen_failures: set[int] = set()

    try:
        hub_port = ports[0]
        for i, port in enumerate(ports):
            server = MPREGServer(settings=build_settings(i, port, hub_port))
            servers.append(server)

        print(f"Starting {len(servers)} servers on ports: {ports}")

        batch_size = 8
        batch_delay = 0.5
        for batch_start in range(0, len(servers), batch_size):
            batch_end = min(batch_start + batch_size, len(servers))
            batch = servers[batch_start:batch_end]
            print(
                f"Starting batch {batch_start // batch_size + 1}: {batch_start}-{batch_end - 1}"
            )
            for server in batch:
                task = asyncio.create_task(server.server())
                tasks.append(task)
                await asyncio.sleep(0.3)
            if batch_end < len(servers):
                await asyncio.sleep(batch_delay)

        print("Monitoring task health for 30s...")
        for tick in range(60):
            await asyncio.sleep(0.5)
            for idx, task in enumerate(tasks):
                if idx in seen_failures:
                    continue
                if task.done():
                    seen_failures.add(idx)
                    exc = task.exception()
                    if exc is None:
                        print(f"Task {idx} exited cleanly")
                    else:
                        print(f"Task {idx} failed with {type(exc).__name__}: {exc}")
                        traceback.print_exception(exc)
            if tick % 10 == 0:
                alive = sum(0 if task.done() else 1 for task in tasks)
                print(f"Tick {tick:02d}: {alive}/{len(tasks)} tasks alive")
                for probe_idx in (0, 12):
                    listener = servers[probe_idx]._transport_listener
                    listening = bool(listener and listener._listening)
                    print(f"  Node {probe_idx} listener active={listening}")
                    probe_port = ports[probe_idx]
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(0.3)
                    try:
                        sock.connect(("127.0.0.1", probe_port))
                    except Exception as exc:
                        print(
                            f"  Node {probe_idx} tcp connect failed on {probe_port}: {exc}"
                        )
                    else:
                        print(
                            f"  Node {probe_idx} tcp connect succeeded on {probe_port}"
                        )
                    finally:
                        sock.close()
                task0_frames = tasks[0].get_stack(limit=3)
                if task0_frames:
                    top = task0_frames[-1]
                    print(
                        f"  Node0 task frame: {top.f_code.co_filename}:{top.f_lineno}"
                    )

            if tick in {20, 40, 59}:
                discovered_counts = [
                    max(len(server.cluster.servers) - 1, 0) for server in servers
                ]
                print(
                    f"  Discovery snapshot tick {tick}: min={min(discovered_counts)} "
                    f"max={max(discovered_counts)} avg={sum(discovered_counts) / len(discovered_counts):.2f}"
                )

    finally:
        print("Shutting down servers...")
        await asyncio.gather(
            *(server.shutdown_async() for server in servers),
            return_exceptions=True,
        )
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for port in ports:
            allocator.release_port(port)
        print("Cleanup complete.")


if __name__ == "__main__":
    asyncio.run(main())
