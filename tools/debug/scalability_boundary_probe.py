from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import get_port_allocator
from mpreg.server import MPREGServer

type NodeCount = int
type Seconds = float


@dataclass(frozen=True, slots=True)
class ProbeConfig:
    sizes: tuple[NodeCount, ...]
    startup_delay_seconds: Seconds
    startup_wait_factor: Seconds
    baseline_propagation_factor: Seconds
    extended_propagation_timeout_seconds: Seconds
    sample_interval_seconds: Seconds
    gossip_interval_seconds: Seconds
    log_level: str


@dataclass(frozen=True, slots=True)
class ConvergenceSample:
    elapsed_seconds: Seconds
    success_rate: float
    discovered_nodes: int
    total_connections: int
    efficiency: float


@dataclass(frozen=True, slots=True)
class SizeProbeResult:
    size: NodeCount
    setup_time_seconds: Seconds
    baseline_wait_seconds: Seconds
    baseline_success_rate: float
    baseline_connections: int
    baseline_efficiency: float
    success_at_70_seconds: Seconds | None
    success_at_100_seconds: Seconds | None
    final_success_rate: float
    final_connections: int
    final_efficiency: float
    converged_to_70: bool
    converged_to_100: bool
    sample_count: int


def _build_settings(
    *,
    node_index: int,
    port: int,
    ports: list[int],
    size: int,
    gossip_interval_seconds: float,
    log_level: str,
) -> MPREGSettings:
    connect_to = f"ws://127.0.0.1:{ports[0]}" if node_index > 0 else None
    return MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=f"scale-node-{node_index}",
        cluster_id=f"scale-probe-{size}",
        resources={f"scale-resource-{node_index}"},
        peers=None,
        connect=connect_to,
        advertised_urls=None,
        gossip_interval=gossip_interval_seconds,
        log_level=log_level,
        monitoring_enabled=False,
    )


def _snapshot(servers: list[MPREGServer]) -> tuple[float, int, float]:
    if not servers:
        return 0.0, 0, 0.0
    nodes_with_function = sum(
        1 for server in servers if "scale_test" in server.cluster.funtimes
    )
    success_rate = nodes_with_function / len(servers)
    total_connections = sum(len(server.peer_connections) for server in servers)
    theoretical_max = len(servers) * max(len(servers) - 1, 0)
    efficiency = total_connections / theoretical_max if theoretical_max > 0 else 0.0
    return success_rate, total_connections, efficiency


async def _shutdown_servers(
    servers: list[MPREGServer], tasks: list[asyncio.Task[None]]
) -> None:
    await asyncio.gather(
        *(server.shutdown_async() for server in servers),
        return_exceptions=True,
    )
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _run_size_probe(config: ProbeConfig, size: int) -> SizeProbeResult:
    allocator = get_port_allocator()
    ports = allocator.allocate_port_range(size, "research")
    servers: list[MPREGServer] = []
    tasks: list[asyncio.Task[None]] = []
    samples: list[ConvergenceSample] = []

    started_at = time.monotonic()
    try:
        for node_index, port in enumerate(ports):
            settings = _build_settings(
                node_index=node_index,
                port=port,
                ports=ports,
                size=size,
                gossip_interval_seconds=config.gossip_interval_seconds,
                log_level=config.log_level,
            )
            server = MPREGServer(settings=settings)
            servers.append(server)
            task = asyncio.create_task(server.server())
            tasks.append(task)
            await asyncio.sleep(config.startup_delay_seconds)

        startup_wait_seconds = max(2.0, size * config.startup_wait_factor)
        await asyncio.sleep(startup_wait_seconds)
        setup_time_seconds = time.monotonic() - started_at

        def scale_test_function(data: str) -> str:
            return f"scale-probe:{data}"

        propagation_start = time.monotonic()
        servers[0].register_command(
            "scale_test", scale_test_function, ["scale-resource"]
        )

        baseline_wait_seconds = max(1.0, size * config.baseline_propagation_factor)
        await asyncio.sleep(baseline_wait_seconds)
        baseline_success_rate, baseline_connections, baseline_efficiency = _snapshot(
            servers
        )

        success_at_70_seconds: float | None = (
            baseline_wait_seconds if baseline_success_rate >= 0.7 else None
        )
        success_at_100_seconds: float | None = (
            baseline_wait_seconds if baseline_success_rate >= 1.0 else None
        )

        deadline = propagation_start + max(
            config.extended_propagation_timeout_seconds, baseline_wait_seconds
        )
        while time.monotonic() < deadline:
            success_rate, total_connections, efficiency = _snapshot(servers)
            elapsed = time.monotonic() - propagation_start
            samples.append(
                ConvergenceSample(
                    elapsed_seconds=elapsed,
                    success_rate=success_rate,
                    discovered_nodes=int(round(success_rate * size)),
                    total_connections=total_connections,
                    efficiency=efficiency,
                )
            )

            if success_at_70_seconds is None and success_rate >= 0.7:
                success_at_70_seconds = elapsed
            if success_at_100_seconds is None and success_rate >= 1.0:
                success_at_100_seconds = elapsed
            if success_at_70_seconds is not None and success_at_100_seconds is not None:
                break
            await asyncio.sleep(config.sample_interval_seconds)

        final_success_rate, final_connections, final_efficiency = _snapshot(servers)
        return SizeProbeResult(
            size=size,
            setup_time_seconds=setup_time_seconds,
            baseline_wait_seconds=baseline_wait_seconds,
            baseline_success_rate=baseline_success_rate,
            baseline_connections=baseline_connections,
            baseline_efficiency=baseline_efficiency,
            success_at_70_seconds=success_at_70_seconds,
            success_at_100_seconds=success_at_100_seconds,
            final_success_rate=final_success_rate,
            final_connections=final_connections,
            final_efficiency=final_efficiency,
            converged_to_70=success_at_70_seconds is not None,
            converged_to_100=success_at_100_seconds is not None,
            sample_count=len(samples),
        )
    finally:
        await _shutdown_servers(servers, tasks)
        for port in ports:
            allocator.release_port(port)


def _parse_sizes(raw_sizes: str) -> tuple[int, ...]:
    values: list[int] = []
    for raw in raw_sizes.split(","):
        text = raw.strip()
        if not text:
            continue
        values.append(max(2, int(text)))
    if not values:
        raise ValueError("At least one size is required")
    return tuple(values)


def _parse_args() -> tuple[ProbeConfig, Path | None]:
    parser = argparse.ArgumentParser(
        description="Probe scalability-boundary convergence with baseline vs extended waits"
    )
    parser.add_argument(
        "--sizes",
        default="5,8,12,16",
        help="Comma-separated cluster sizes",
    )
    parser.add_argument(
        "--startup-delay-seconds",
        type=float,
        default=0.05,
        help="Delay between server starts (matches test default)",
    )
    parser.add_argument(
        "--startup-wait-factor",
        type=float,
        default=0.15,
        help="Startup wait factor: max(2.0, size * factor)",
    )
    parser.add_argument(
        "--baseline-propagation-factor",
        type=float,
        default=0.05,
        help="Baseline propagation wait factor: max(1.0, size * factor)",
    )
    parser.add_argument(
        "--extended-propagation-timeout-seconds",
        type=float,
        default=12.0,
        help="Extended propagation observation window after baseline wait",
    )
    parser.add_argument(
        "--sample-interval-seconds",
        type=float,
        default=0.25,
        help="Sampling interval for convergence tracking",
    )
    parser.add_argument(
        "--gossip-interval-seconds",
        type=float,
        default=0.5,
        help="Server gossip interval",
    )
    parser.add_argument(
        "--log-level",
        default="ERROR",
        help="Server log level",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional output JSON path for results",
    )
    args = parser.parse_args()

    config = ProbeConfig(
        sizes=_parse_sizes(str(args.sizes)),
        startup_delay_seconds=max(0.0, float(args.startup_delay_seconds)),
        startup_wait_factor=max(0.01, float(args.startup_wait_factor)),
        baseline_propagation_factor=max(0.01, float(args.baseline_propagation_factor)),
        extended_propagation_timeout_seconds=max(
            1.0, float(args.extended_propagation_timeout_seconds)
        ),
        sample_interval_seconds=max(0.05, float(args.sample_interval_seconds)),
        gossip_interval_seconds=max(0.1, float(args.gossip_interval_seconds)),
        log_level=str(args.log_level),
    )
    output_path = (
        Path(args.output_json).resolve() if str(args.output_json).strip() else None
    )
    return config, output_path


async def _run_probe(config: ProbeConfig) -> list[SizeProbeResult]:
    results: list[SizeProbeResult] = []
    print("Scalability boundary probe")
    print(f"sizes={config.sizes}")
    print(
        "baseline_wait=max(1.0, size*"
        f"{config.baseline_propagation_factor:.3f}) "
        f"extended_timeout={config.extended_propagation_timeout_seconds:.2f}s"
    )
    for size in config.sizes:
        print(f"\n[size={size}] probing...")
        result = await _run_size_probe(config, size)
        results.append(result)
        print(
            f"[size={size}] baseline_success={result.baseline_success_rate:.2%} "
            f"final_success={result.final_success_rate:.2%} "
            f"success_at_70={result.success_at_70_seconds} "
            f"success_at_100={result.success_at_100_seconds} "
            f"connections={result.final_connections} "
            f"efficiency={result.final_efficiency:.2%}"
        )
    return results


def main() -> int:
    config, output_path = _parse_args()
    results = asyncio.run(_run_probe(config))
    payload = {
        "generated_at_unix": time.time(),
        "config": asdict(config),
        "results": [asdict(result) for result in results],
    }
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        print(f"\nWrote JSON report: {output_path}")

    if all(result.converged_to_70 for result in results):
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
