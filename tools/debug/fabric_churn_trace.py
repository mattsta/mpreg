from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import get_port_allocator
from mpreg.server import MPREGServer

type Port = int
type Seconds = float


@dataclass(frozen=True, slots=True)
class ChurnSnapshot:
    elapsed_seconds: Seconds
    peer_count: int
    directory_count: int
    departed_in_peer_list: tuple[str, ...]
    departed_in_directory: tuple[str, ...]
    peer_urls_sample: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ChurnResult:
    success: bool
    expected_peer_count: int
    last_peer_count: int
    departed_urls: tuple[str, ...]
    snapshots: tuple[ChurnSnapshot, ...]


async def _start_cluster(
    *,
    ports: list[Port],
    cluster_id: str,
    servers: list[MPREGServer],
    tasks: list[asyncio.Task[None]],
    connect_url: str | None = None,
    node_offset: int = 0,
    gossip_interval: float = 0.3,
) -> list[MPREGServer]:
    created: list[MPREGServer] = []
    hub_url = connect_url or f"ws://127.0.0.1:{ports[0]}"

    for index, port in enumerate(ports):
        connect = connect_url or (hub_url if index > 0 else None)
        node_id = node_offset + index
        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name=f"{cluster_id}-node-{node_id}",
            cluster_id=cluster_id,
            resources={"compute"},
            connect=connect,
            gossip_interval=gossip_interval,
            fabric_catalog_ttl_seconds=15.0,
            fabric_route_ttl_seconds=10.0,
            log_level="INFO",
        )
        server = MPREGServer(settings=settings)
        servers.append(server)
        created.append(server)
        tasks.append(asyncio.create_task(server.server()))
        await asyncio.sleep(0.05)

    await asyncio.sleep(1.0)
    return created


def _directory_urls(server: MPREGServer) -> set[str]:
    if server._peer_directory is None:
        return set()
    return {
        node.node_id
        for node in server._peer_directory.nodes()
        if node.node_id != server.cluster.local_url
    }


async def _run_trace(
    *,
    initial_size: int,
    replacements: int,
    timeout_seconds: float,
    sample_interval_seconds: float,
    output_json: Path | None,
) -> int:
    allocator = get_port_allocator()
    total_ports = initial_size + replacements
    ports = allocator.allocate_port_range(total_ports, "servers")
    servers: list[MPREGServer] = []
    tasks: list[asyncio.Task[None]] = []
    snapshots: list[ChurnSnapshot] = []

    try:
        initial_ports = ports[:initial_size]
        replacement_ports = ports[initial_size:]
        cluster_id = "debug-churn-cluster"

        initial_servers = await _start_cluster(
            ports=initial_ports,
            cluster_id=cluster_id,
            servers=servers,
            tasks=tasks,
        )
        hub_url = f"ws://127.0.0.1:{initial_ports[0]}"
        hub_server = initial_servers[0]

        async with MPREGClientAPI(hub_url) as client:
            # Initial settle
            await asyncio.sleep(1.5)

            departing = initial_servers[-replacements:]
            departed_urls = tuple(
                sorted(f"ws://127.0.0.1:{server.settings.port}" for server in departing)
            )
            departed_set = set(departed_urls)
            print(f"Departing nodes: {departed_urls}")

            for server in departing:
                await server.shutdown_async()

            await asyncio.sleep(0.5)
            await _start_cluster(
                ports=replacement_ports,
                cluster_id=cluster_id,
                servers=servers,
                tasks=tasks,
                connect_url=hub_url,
                node_offset=initial_size,
            )

            expected_peer_count = initial_size - 1
            started_at = time.monotonic()
            success = False
            last_peer_count = -1

            while True:
                elapsed_seconds = time.monotonic() - started_at
                peers = await client.list_peers()
                peer_urls = {peer.url for peer in peers}
                directory_urls = _directory_urls(hub_server)
                departed_in_peers = tuple(sorted(peer_urls & departed_set))
                departed_in_directory = tuple(sorted(directory_urls & departed_set))
                last_peer_count = len(peer_urls)
                snapshot = ChurnSnapshot(
                    elapsed_seconds=elapsed_seconds,
                    peer_count=len(peer_urls),
                    directory_count=len(directory_urls),
                    departed_in_peer_list=departed_in_peers,
                    departed_in_directory=departed_in_directory,
                    peer_urls_sample=tuple(sorted(peer_urls)[:10]),
                )
                snapshots.append(snapshot)
                print(
                    f"[t={elapsed_seconds:5.1f}s] peers={snapshot.peer_count} "
                    f"directory={snapshot.directory_count} "
                    f"departed(peer_list)={len(departed_in_peers)} "
                    f"departed(directory)={len(departed_in_directory)}"
                )
                if (
                    len(peer_urls) == expected_peer_count
                    and not departed_in_peers
                    and not departed_in_directory
                ):
                    success = True
                    break
                if elapsed_seconds >= timeout_seconds:
                    break
                await asyncio.sleep(sample_interval_seconds)

            result = ChurnResult(
                success=success,
                expected_peer_count=expected_peer_count,
                last_peer_count=last_peer_count,
                departed_urls=departed_urls,
                snapshots=tuple(snapshots),
            )
            if output_json is not None:
                output_json.parent.mkdir(parents=True, exist_ok=True)
                output_json.write_text(
                    json.dumps(asdict(result), indent=2, sort_keys=True),
                    encoding="utf-8",
                )
                print(f"Wrote JSON report: {output_json}")
            print(
                f"Result: success={result.success} expected={result.expected_peer_count} "
                f"last={result.last_peer_count}"
            )
            return 0 if success else 1
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trace peer-directory behavior during churn recovery."
    )
    parser.add_argument("--initial-size", type=int, default=12)
    parser.add_argument("--replacements", type=int, default=2)
    parser.add_argument("--timeout-seconds", type=float, default=12.0)
    parser.add_argument("--sample-interval-seconds", type=float, default=0.2)
    parser.add_argument(
        "--output-json",
        type=str,
        default="",
        help="Optional path to write structured JSON report.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    output_json = (
        Path(args.output_json).resolve() if str(args.output_json).strip() else None
    )
    return asyncio.run(
        _run_trace(
            initial_size=max(3, int(args.initial_size)),
            replacements=max(1, int(args.replacements)),
            timeout_seconds=max(1.0, float(args.timeout_seconds)),
            sample_interval_seconds=max(0.05, float(args.sample_interval_seconds)),
            output_json=output_json,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
