from __future__ import annotations

#!/usr/bin/env python3
"""
Main CLI Entry Point for MPREG Fabric Federation Management.

Provides command-line interface for comprehensive fabric federation operations:
- Cluster discovery, registration, and management
- Health monitoring and alerting
- Performance metrics and analytics
- Configuration validation and deployment
- Backup and recovery operations
"""

import asyncio
import json
import os
import sys
import time
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import aiohttp
import click
from rich.console import Console
from rich.table import Table

from mpreg import __version__
from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.logging import configure_logging
from mpreg.core.persistence.config import PersistenceConfig, PersistenceMode
from mpreg.core.port_allocator import allocate_port
from mpreg.server import MPREGServer

from .federation_cli import FederationCLI

console = Console()


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = "DEBUG" if verbose else "INFO"
    configure_logging(level, colorize=True)


@click.group()
@click.version_option(__version__, prog_name="mpreg")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.pass_context
def cli(ctx, verbose: bool):
    """
    MPREG Fabric Federation Management CLI.

    Comprehensive tools for managing fabric-enabled MPREG clusters including
    discovery, registration, health monitoring, and deployment automation.
    """
    setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


@cli.group()
def client():
    """Client commands for interacting with MPREG servers."""
    pass


@client.command("call")
@click.argument("fun")
@click.argument("args", nargs=-1)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option(
    "--locs",
    multiple=True,
    help="Resource locations to target (can be repeated)",
)
@click.option("--timeout", type=float, default=None, help="RPC timeout in seconds")
def call(
    fun: str,
    args: tuple[str, ...],
    url: str | None,
    locs: tuple[str, ...],
    timeout: float | None,
):
    """Call a remote RPC function."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    def _parse_arg(value: str) -> Any:
        try:
            return json.loads(value)
        except Exception:
            return value

    async def _call():
        async with MPREGClientAPI(url) as client:
            parsed_args = tuple(_parse_arg(arg) for arg in args)
            result = await client.call(
                fun,
                *parsed_args,
                locs=frozenset(locs) if locs else None,
                timeout=timeout,
            )
            console.print(result)

    asyncio.run(_call())


@client.command("list-peers")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
def list_peers(url: str | None):
    """List known peers in the cluster."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _list():
        async with MPREGClientAPI(url) as client:
            peers = await client.list_peers()
            console.print(peers)

    asyncio.run(_list())


@cli.group()
def server():
    """Server management commands."""
    pass


@server.command("start")
@click.option("--host", default="127.0.0.1", help="Host to bind")
@click.option(
    "--port",
    default=None,
    type=int,
    help="Port to bind (auto-allocate when omitted)",
)
@click.option("--name", default="MPREG Server", help="Server name")
@click.option("--resource", "resources", multiple=True, help="Server resource tag")
@click.option("--peer", "peers", multiple=True, help="Static peer URL")
@click.option("--connect", default=None, help="Peer URL to connect on startup")
@click.option("--cluster-id", default="default-cluster", help="Cluster ID")
@click.option(
    "--advertised-url",
    "advertised_urls",
    multiple=True,
    help="Advertised URL for inbound connections",
)
@click.option("--enable-cache", is_flag=True, help="Enable default cache stack")
@click.option("--enable-queue", is_flag=True, help="Enable default queue manager")
@click.option(
    "--enable-cache-federation",
    is_flag=True,
    help="Enable L4 fabric cache federation replication",
)
@click.option("--cache-region", default="local", help="Cache region name")
@click.option("--cache-latitude", type=float, default=0.0, help="Cache latitude")
@click.option("--cache-longitude", type=float, default=0.0, help="Cache longitude")
@click.option("--cache-capacity-mb", type=int, default=512, help="Cache capacity in MB")
@click.option(
    "--monitoring-port",
    type=int,
    default=None,
    help="Monitoring HTTP port (auto-allocate when omitted)",
)
@click.option(
    "--monitoring-host",
    default=None,
    help="Monitoring HTTP host (defaults to server host)",
)
@click.option(
    "--monitoring/--no-monitoring",
    default=True,
    help="Enable or disable monitoring endpoints",
)
@click.option(
    "--monitoring-cors/--no-monitoring-cors",
    default=True,
    help="Enable or disable CORS for monitoring endpoints",
)
@click.option(
    "--persistence-mode",
    type=click.Choice(["off", "memory", "sqlite"]),
    default="off",
    help="Enable unified persistence (memory or sqlite)",
)
@click.option(
    "--persistence-dir",
    type=click.Path(),
    default=None,
    help="Persistence data directory (sqlite)",
)
@click.option(
    "--persistence-sqlite-filename",
    type=str,
    default="mpreg.sqlite",
    help="SQLite filename for persistence backend",
)
def start_server(
    host: str,
    port: int | None,
    name: str,
    resources: tuple[str, ...],
    peers: tuple[str, ...],
    connect: str | None,
    cluster_id: str,
    advertised_urls: tuple[str, ...],
    enable_cache: bool,
    enable_queue: bool,
    enable_cache_federation: bool,
    cache_region: str,
    cache_latitude: float,
    cache_longitude: float,
    cache_capacity_mb: int,
    monitoring_port: int | None,
    monitoring_host: str | None,
    monitoring: bool,
    monitoring_cors: bool,
    persistence_mode: str,
    persistence_dir: str | None,
    persistence_sqlite_filename: str,
):
    """Start an MPREG server."""

    async def _start():
        selected_port = port if port not in (None, 0) else allocate_port("servers")
        selected_monitoring_port = monitoring_port
        if monitoring and selected_monitoring_port in (None, 0):
            selected_monitoring_port = allocate_port("monitoring")

        server_url = f"ws://{host}:{selected_port}"
        console.print(f"Server endpoint: {server_url}")
        console.print(f"MPREG_URL={server_url}")
        if monitoring:
            monitor_host = monitoring_host or host
            monitor_url = f"http://{monitor_host}:{selected_monitoring_port}"
            console.print(f"Monitoring endpoint: {monitor_url}")
            console.print(f"MPREG_MONITORING_URL={monitor_url}")

        persistence_config = None
        if persistence_mode != "off":
            data_dir = Path(persistence_dir) if persistence_dir else None
            persistence_config = PersistenceConfig(
                mode=PersistenceMode(persistence_mode),
                data_dir=data_dir or PersistenceConfig().data_dir,
                sqlite_filename=persistence_sqlite_filename,
            )

        settings = MPREGSettings(
            host=host,
            port=selected_port,
            name=name,
            resources=set(resources) if resources else None,
            peers=list(peers) if peers else None,
            connect=connect,
            cluster_id=cluster_id,
            advertised_urls=tuple(advertised_urls) if advertised_urls else None,
            gossip_interval=5.0,
            log_level="INFO",
            enable_default_cache=enable_cache,
            enable_default_queue=enable_queue,
            enable_cache_federation=enable_cache_federation,
            cache_region=cache_region,
            cache_latitude=cache_latitude,
            cache_longitude=cache_longitude,
            cache_capacity_mb=cache_capacity_mb,
            monitoring_port=selected_monitoring_port,
            monitoring_host=monitoring_host,
            monitoring_enabled=monitoring,
            monitoring_enable_cors=monitoring_cors,
            persistence_config=persistence_config,
        )
        server_instance = MPREGServer(settings=settings)
        await server_instance.server()

    asyncio.run(_start())


@server.command("start-config")
@click.argument("settings_path", type=click.Path(exists=True))
def start_config(settings_path: str) -> None:
    """Start an MPREG server from a JSON or TOML settings file."""

    async def _start() -> None:
        settings = MPREGSettings.from_path(settings_path)
        server_instance = MPREGServer(settings=settings)
        await server_instance.server()

    asyncio.run(_start())


@cli.group()
def demo():
    """Run tiered MPREG demos."""
    pass


@demo.command("tier1")
@click.argument(
    "system",
    type=click.Choice(["rpc", "pubsub", "queue", "cache", "federation", "monitoring"]),
)
def demo_tier1(system: str) -> None:
    """Run a tier 1 single-system demo."""
    from mpreg.examples.tier1_single_system_full import SYSTEMS

    asyncio.run(SYSTEMS[system]())


@demo.command("tier2")
def demo_tier2() -> None:
    """Run the tier 2 integration demos."""
    from mpreg.examples.tier2_integrations import main as tier2_main

    asyncio.run(tier2_main())


@demo.command("tier3")
def demo_tier3() -> None:
    """Run the tier 3 full-system demo."""
    from mpreg.examples.tier3_full_system_expansion import main as tier3_main

    asyncio.run(tier3_main())


@demo.command("all")
def demo_all() -> None:
    """Run tier 1 (RPC), tier 2, and tier 3 demos."""
    from mpreg.examples.tier1_single_system_full import demo_rpc
    from mpreg.examples.tier2_integrations import main as tier2_main
    from mpreg.examples.tier3_full_system_expansion import main as tier3_main

    async def _run():
        await demo_rpc()
        await tier2_main()
        await tier3_main()

    asyncio.run(_run())


@cli.command()
@click.option(
    "--config", "-c", type=click.Path(exists=True), help="Configuration file path"
)
@click.option(
    "--output",
    "-o",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
def discover(config: str | None, output: str):
    """Discover available fabric clusters."""

    async def _discover():
        federation_cli = FederationCLI()
        clusters = await federation_cli.discover_clusters(config)

        if output == "table":
            federation_cli.display_cluster_list(clusters)
        elif output == "json":
            import json

            # Convert dataclasses to dict for JSON serialization
            clusters_dict = [
                {
                    "cluster_id": cluster.cluster_id,
                    "cluster_name": cluster.cluster_name,
                    "region": cluster.region,
                    "bridge_url": cluster.bridge_url,
                    "server_url": cluster.server_url,
                    "status": cluster.status,
                    "health": cluster.health,
                    "discovery_source": cluster.discovery_source,
                    "health_score": cluster.health_score,
                }
                for cluster in clusters
            ]
            console.print(json.dumps(clusters_dict, indent=2))

    asyncio.run(_discover())


@cli.command()
@click.argument("cluster_id")
@click.argument("cluster_name")
@click.argument("region")
@click.argument("server_url")
@click.argument("bridge_url")
@click.option("--no-resilience", is_flag=True, help="Disable resilience monitoring")
def register(
    cluster_id: str,
    cluster_name: str,
    region: str,
    server_url: str,
    bridge_url: str,
    no_resilience: bool,
):
    """Register a new federation cluster."""

    async def _register():
        federation_cli = FederationCLI()
        success = await federation_cli.register_cluster(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            region=region,
            server_url=server_url,
            bridge_url=bridge_url,
            enable_resilience=not no_resilience,
        )

        if not success:
            sys.exit(1)

    asyncio.run(_register())


@cli.command()
@click.argument("cluster_id")
def unregister(cluster_id: str):
    """Unregister a federation cluster."""

    async def _unregister():
        federation_cli = FederationCLI()
        success = await federation_cli.unregister_cluster(cluster_id)

        if not success:
            sys.exit(1)

    asyncio.run(_unregister())


@cli.command()
@click.option("--cluster", "-c", help="Specific cluster ID to check")
@click.option(
    "--output",
    "-o",
    type=click.Choice(["report", "json"]),
    default="report",
    help="Output format",
)
def health(cluster: str | None, output: str):
    """Check cluster health status."""

    async def _health():
        federation_cli = FederationCLI()
        health_results = await federation_cli.check_cluster_health(cluster)

        if output == "report":
            federation_cli.display_health_report(health_results)
        elif output == "json":
            import json

            # Convert dataclasses to dict for JSON serialization
            serializable_results: dict[str, dict[str, Any]] = {}
            for cluster_id, result in health_results.items():
                serializable_results[cluster_id] = {}
                for key, value in result.items():
                    if is_dataclass(value) and not isinstance(value, type):
                        # Convert dataclass instance to dict
                        serializable_results[cluster_id][key] = asdict(value)
                    else:
                        serializable_results[cluster_id][key] = value
            console.print(json.dumps(serializable_results, indent=2, default=str))

    asyncio.run(_health())


@cli.command()
@click.option("--cluster", "-c", help="Specific cluster ID to show metrics for")
def metrics(cluster: str | None):
    """Display performance metrics for clusters."""

    async def _metrics():
        federation_cli = FederationCLI()
        await federation_cli.show_metrics(cluster)

    asyncio.run(_metrics())


@cli.command()
@click.argument("output_path", type=click.Path())
def generate_config(output_path: str):
    """Generate a federation configuration template."""
    federation_cli = FederationCLI()
    federation_cli.generate_config_template(output_path)


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
def validate_config(config_path: str):
    """Validate federation configuration file."""

    async def _validate():
        federation_cli = FederationCLI()
        is_valid = await federation_cli.validate_config(config_path)

        if not is_valid:
            sys.exit(1)

    asyncio.run(_validate())


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option(
    "--dry-run", is_flag=True, help="Validate configuration without deploying"
)
def deploy(config_path: str, dry_run: bool):
    """Deploy federation clusters from configuration file."""

    async def _deploy():
        federation_cli = FederationCLI()

        if dry_run:
            console.print(
                "[bold blue]🔍 Running deployment validation (dry-run mode)[/bold blue]"
            )
            is_valid = await federation_cli.validate_config(config_path)
            if is_valid:
                console.print(
                    "[green]✅ Configuration is valid and ready for deployment[/green]"
                )
            else:
                console.print("[red]❌ Configuration validation failed[/red]")
                sys.exit(1)
        else:
            success = await federation_cli.deploy_from_config(config_path)
            if not success:
                sys.exit(1)

    asyncio.run(_deploy())


@cli.command()
def topology():
    """Display federation topology."""
    federation_cli = FederationCLI()
    federation_cli.display_topology()


@cli.command()
@click.option("--force", is_flag=True, help="Force cleanup without confirmation")
def cleanup(force: bool):
    """Clean up all federation resources."""

    async def _cleanup():
        federation_cli = FederationCLI()

        if not force:
            if not click.confirm(
                "Are you sure you want to clean up all federation resources?"
            ):
                console.print("[yellow]⚠️ Cleanup cancelled[/yellow]")
                return

        await federation_cli.cleanup_all()

    asyncio.run(_cleanup())


@cli.group()
def monitor():
    """Fabric federation monitoring commands."""
    pass


@monitor.command()
@click.option(
    "--interval", "-i", type=int, default=30, help="Health check interval in seconds"
)
@click.option("--clusters", "-c", multiple=True, help="Specific clusters to monitor")
@click.option(
    "--summary/--detail",
    default=True,
    help="Use summary or detailed endpoint (monitoring URL mode only)",
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def health_watch(
    interval: int, clusters: tuple[str, ...], summary: bool, url: str | None
):
    """Continuously monitor cluster health."""

    async def _health_watch():
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if monitoring_url:
            base_url = monitoring_url.rstrip("/")
            console.print(
                f"[bold green]🔍 Starting monitoring endpoint health watch (interval: {interval}s)[/bold green]"
            )
            try:
                while True:
                    console.clear()
                    console.print(
                        f"[bold blue]🏥 Fabric Health Monitor - {time.time():.0f}[/bold blue]"
                    )
                    async with aiohttp.ClientSession() as session:
                        if clusters:
                            for cluster_id in clusters:
                                endpoint = f"{base_url}/health/clusters/{cluster_id}"
                                async with session.get(endpoint) as response:
                                    payload = await response.json()
                                    console.print_json(data=payload)
                        else:
                            endpoint = (
                                f"{base_url}/health/summary"
                                if summary
                                else f"{base_url}/health"
                            )
                            async with session.get(endpoint) as response:
                                payload = await response.json()
                                console.print_json(data=payload)
                    await asyncio.sleep(interval)
            except KeyboardInterrupt:
                console.print("\n[yellow]⚠️ Health monitoring stopped[/yellow]")
            return

        federation_cli = FederationCLI()
        console.print(
            f"[bold green]🔍 Starting health monitoring (interval: {interval}s)[/bold green]"
        )

        try:
            while True:
                console.clear()
                console.print(
                    f"[bold blue]🏥 Fabric Health Monitor - {time.time():.0f}[/bold blue]"
                )

                if clusters:
                    for cluster_id in clusters:
                        health_results = await federation_cli.check_cluster_health(
                            cluster_id
                        )
                        federation_cli.display_health_report(health_results)
                else:
                    health_results = await federation_cli.check_cluster_health(None)
                    federation_cli.display_health_report(health_results)

                await asyncio.sleep(interval)

        except KeyboardInterrupt:
            console.print("\n[yellow]⚠️ Health monitoring stopped[/yellow]")

    asyncio.run(_health_watch())


@monitor.command("health")
@click.option(
    "--cluster",
    "-c",
    default=None,
    help="Specific cluster ID to query",
)
@click.option(
    "--summary/--detail",
    default=True,
    help="Use summary or detailed endpoint (monitoring URL mode only)",
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def health_endpoint(cluster: str | None, summary: bool, url: str | None) -> None:
    """Fetch health status from the monitoring endpoint."""

    async def _health_endpoint() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        base_url = monitoring_url.rstrip("/")
        if cluster:
            endpoint = f"{base_url}/health/clusters/{cluster}"
        else:
            endpoint = f"{base_url}/health/summary" if summary else f"{base_url}/health"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                if response.status != 200:
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

    asyncio.run(_health_endpoint())


@monitor.command()
@click.option(
    "--interval", "-i", type=int, default=60, help="Metrics update interval in seconds"
)
@click.option("--clusters", "-c", multiple=True, help="Specific clusters to monitor")
@click.option(
    "--system",
    type=click.Choice(
        ["unified", "rpc", "pubsub", "queue", "cache", "transport", "federation"],
        case_sensitive=False,
    ),
    default="unified",
    help="Metrics system to query (monitoring URL mode only)",
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def metrics_watch(
    interval: int, clusters: tuple[str, ...], system: str, url: str | None
):
    """Continuously monitor cluster metrics."""

    async def _metrics_watch():
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if monitoring_url:
            endpoint_map = {
                "unified": "/metrics/unified",
                "rpc": "/metrics/rpc",
                "pubsub": "/metrics/pubsub",
                "queue": "/metrics/queue",
                "cache": "/metrics/cache",
                "transport": "/metrics/transport",
                "federation": "/metrics",
            }
            endpoint = f"{monitoring_url.rstrip('/')}{endpoint_map[system]}"
            console.print(
                f"[bold green]📊 Starting monitoring endpoint metrics watch (interval: {interval}s)[/bold green]"
            )
            try:
                while True:
                    console.clear()
                    console.print(
                        f"[bold blue]📊 Fabric Metrics Monitor - {time.time():.0f}[/bold blue]"
                    )
                    async with aiohttp.ClientSession() as session:
                        async with session.get(endpoint) as response:
                            payload = await response.json()
                            console.print_json(data=payload)
                    await asyncio.sleep(interval)
            except KeyboardInterrupt:
                console.print("\n[yellow]⚠️ Metrics monitoring stopped[/yellow]")
            return

        federation_cli = FederationCLI()
        console.print(
            f"[bold green]📊 Starting metrics monitoring (interval: {interval}s)[/bold green]"
        )

        try:
            while True:
                console.clear()
                console.print(
                    f"[bold blue]📊 Fabric Metrics Monitor - {time.time():.0f}[/bold blue]"
                )

                if clusters:
                    for cluster_id in clusters:
                        await federation_cli.show_metrics(cluster_id)
                else:
                    await federation_cli.show_metrics(None)

                await asyncio.sleep(interval)

        except KeyboardInterrupt:
            console.print("\n[yellow]⚠️ Metrics monitoring stopped[/yellow]")

    asyncio.run(_metrics_watch())


@monitor.command("status")
@click.option(
    "--cluster",
    "-c",
    default=None,
    help="Specific cluster ID for health details",
)
@click.option(
    "--output",
    "-o",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def status(cluster: str | None, output: str, url: str | None) -> None:
    """Show a compact admin status summary from monitoring endpoints."""

    def _format_timestamp(raw: Any) -> str:
        if not raw:
            return "n/a"
        try:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(raw)))
        except Exception:
            return str(raw)

    def _summarize_transport(snapshot_payload: dict[str, Any]) -> dict[str, int]:
        snapshots = snapshot_payload.get("transport_health_snapshots", {}) or {}
        if not isinstance(snapshots, dict):
            return {"total": 0, "healthy": 0, "degraded": 0}
        healthy = 0
        degraded = 0
        for entry in snapshots.values():
            score = entry.get("overall_health_score", 0.0)
            try:
                score_val = float(score)
            except Exception:
                score_val = 0.0
            if score_val >= 0.9:
                healthy += 1
            elif score_val >= 0.7:
                degraded += 1
        total = len(snapshots)
        return {"total": total, "healthy": healthy, "degraded": degraded}

    async def _status() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        base_url = monitoring_url.rstrip("/")
        health_endpoint = (
            f"{base_url}/health/clusters/{cluster}"
            if cluster
            else f"{base_url}/health/summary"
        )
        endpoints = {
            "health": health_endpoint,
            "persistence": f"{base_url}/metrics/persistence",
            "transport": f"{base_url}/metrics/transport",
            "unified": f"{base_url}/metrics/unified",
        }
        results: dict[str, Any] = {}
        async with aiohttp.ClientSession() as session:
            for key, endpoint in endpoints.items():
                async with session.get(endpoint) as response:
                    payload = await response.json()
                    results[key] = {
                        "status_code": response.status,
                        "payload": payload,
                    }

        if output == "json":
            console.print_json(data=results)
            return

        table = Table(title="MPREG Monitoring Status")
        table.add_column("Section")
        table.add_column("Value")

        health_payload = results.get("health", {}).get("payload", {})
        if cluster:
            cluster_info = health_payload.get("cluster", {})
            table.add_row(
                "Health",
                f"{cluster_info.get('status', 'unknown')} (cluster={cluster})",
            )
        else:
            summary = health_payload.get("health_summary", {})
            overall = summary.get("overall", {})
            table.add_row(
                "Health",
                f"{overall.get('status', 'unknown')} "
                f"score={overall.get('health_score', 'n/a')}",
            )
            table.add_row(
                "Clusters",
                f"{summary.get('cluster_counts', {}).get('total', 'n/a')} total",
            )

        persistence_payload = results.get("persistence", {}).get("payload", {})
        persistence = persistence_payload.get("persistence_snapshots", {})
        if persistence.get("enabled"):
            table.add_row(
                "Persistence",
                f"{persistence.get('mode', 'n/a')} dir={persistence.get('data_dir', 'n/a')}",
            )
            fabric = persistence.get("fabric", {})
            table.add_row(
                "Snapshots",
                f"saved={_format_timestamp(fabric.get('snapshot_last_saved_at'))} "
                f"restored={_format_timestamp(fabric.get('snapshot_last_restored_at'))}",
            )
        else:
            table.add_row("Persistence", "disabled")

        transport_payload = results.get("transport", {}).get("payload", {})
        transport_summary = _summarize_transport(transport_payload)
        table.add_row(
            "Transport",
            f"{transport_summary['total']} endpoints "
            f"(healthy={transport_summary['healthy']}, degraded={transport_summary['degraded']})",
        )

        unified_payload = results.get("unified", {}).get("payload", {})
        unified_metrics = unified_payload.get("unified_metrics", {})
        table.add_row(
            "Unified",
            f"health={unified_metrics.get('overall_health_status', 'n/a')} "
            f"score={unified_metrics.get('overall_health_score', 'n/a')}",
        )

        console.print(table)

    asyncio.run(_status())


@monitor.command("route-trace")
@click.option(
    "--destination",
    "-d",
    required=True,
    help="Destination cluster ID to trace",
)
@click.option(
    "--avoid",
    "-a",
    multiple=True,
    help="Clusters to avoid (can be repeated)",
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def route_trace(destination: str, avoid: tuple[str, ...], url: str | None) -> None:
    """Fetch a route selection trace from the monitoring endpoint."""

    async def _route_trace() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        params = {"destination": destination}
        if avoid:
            params["avoid"] = ",".join(avoid)
        endpoint = f"{monitoring_url.rstrip('/')}/routing/trace"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint, params=params) as response:
                payload = await response.json()
                if response.status != 200:
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

    asyncio.run(_route_trace())


@monitor.command("link-state")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def link_state(url: str | None) -> None:
    """Fetch link-state routing status from the monitoring endpoint."""

    async def _link_state() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint = f"{monitoring_url.rstrip('/')}/routing/link-state"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                if response.status != 200:
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

    asyncio.run(_link_state())


@monitor.command("transport-endpoints")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def transport_endpoints(url: str | None) -> None:
    """Fetch transport endpoint assignments from monitoring."""

    async def _transport_endpoints() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint = f"{monitoring_url.rstrip('/')}/transport/endpoints"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                if response.status != 200:
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

    asyncio.run(_transport_endpoints())


@monitor.command("metrics")
@click.option(
    "--system",
    type=click.Choice(
        ["unified", "rpc", "pubsub", "queue", "cache", "transport", "federation"],
        case_sensitive=False,
    ),
    default="unified",
    help="Metrics system to query",
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def metrics(system: str, url: str | None) -> None:
    """Fetch one-shot metrics from the monitoring endpoint."""

    async def _metrics() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint_map = {
            "unified": "/metrics/unified",
            "rpc": "/metrics/rpc",
            "pubsub": "/metrics/pubsub",
            "queue": "/metrics/queue",
            "cache": "/metrics/cache",
            "transport": "/metrics/transport",
            "federation": "/metrics",
        }
        endpoint = f"{monitoring_url.rstrip('/')}{endpoint_map[system]}"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                if response.status != 200:
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

    asyncio.run(_metrics())


@monitor.command("persistence")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def persistence(url: str | None) -> None:
    """Fetch persistence snapshot status from monitoring."""

    async def _persistence() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint = f"{monitoring_url.rstrip('/')}/metrics/persistence"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                if response.status != 200:
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

    asyncio.run(_persistence())


@monitor.command("persistence-watch")
@click.option(
    "--interval", "-i", type=int, default=30, help="Polling interval in seconds"
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def persistence_watch(interval: int, url: str | None) -> None:
    """Continuously monitor persistence snapshot status."""

    async def _persistence_watch() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint = f"{monitoring_url.rstrip('/')}/metrics/persistence"
        try:
            while True:
                console.clear()
                console.print(
                    f"[bold blue]💾 Persistence Snapshots - {time.time():.0f}[/bold blue]"
                )
                async with aiohttp.ClientSession() as session:
                    async with session.get(endpoint) as response:
                        payload = await response.json()
                        console.print_json(data=payload)
                await asyncio.sleep(interval)
        except KeyboardInterrupt:
            console.print("\n[yellow]⚠️ Persistence monitoring stopped[/yellow]")

    asyncio.run(_persistence_watch())


@monitor.command("endpoints")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def endpoints(url: str | None) -> None:
    """List available monitoring endpoints."""

    async def _endpoints() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint = f"{monitoring_url.rstrip('/')}/endpoints"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                if response.status != 200:
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

    asyncio.run(_endpoints())


@cli.group()
def auto_discovery():
    """Auto-discovery management commands."""
    pass


@auto_discovery.command()
@click.option(
    "--config", "-c", type=click.Path(exists=True), help="Discovery configuration file"
)
@click.option(
    "--output",
    "-o",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
def run(config: str | None, output: str):
    """Run auto-discovery to find fabric clusters."""

    async def _auto_discover():
        federation_cli = FederationCLI()
        clusters = await federation_cli.discover_clusters(config)

        if output == "table":
            federation_cli.display_cluster_list(clusters)
        elif output == "json":
            import json

            clusters_dict = [
                {
                    "cluster_id": cluster.cluster_id,
                    "cluster_name": cluster.cluster_name,
                    "region": cluster.region,
                    "bridge_url": cluster.bridge_url,
                    "server_url": cluster.server_url,
                    "status": cluster.status,
                    "health": cluster.health,
                    "discovery_source": cluster.discovery_source,
                    "health_score": cluster.health_score,
                }
                for cluster in clusters
            ]
            console.print(json.dumps(clusters_dict, indent=2))

    asyncio.run(_auto_discover())


@auto_discovery.command()
@click.argument("output_path", type=click.Path())
def generate(output_path: str):
    """Generate auto-discovery configuration template."""
    discovery_config = {
        "auto_discovery": {
            "enabled": True,
            "backends": [
                {
                    "protocol": "static_config",
                    "config_path": "/etc/mpreg/clusters.json",
                    "discovery_interval": 60.0,
                },
                {
                    "protocol": "consul",
                    "host": "localhost",
                    "port": 8500,
                    "service_name": "mpreg-federation",
                    "datacenter": "dc1",
                    "discovery_interval": 30.0,
                },
                {
                    "protocol": "dns_srv",
                    "domain": "mpreg.local",
                    "service": "_mpreg._tcp",
                    "discovery_interval": 120.0,
                },
                {
                    "protocol": "http_endpoint",
                    "discovery_url": "https://discovery.example.com/clusters",
                    "discovery_interval": 60.0,
                    "registration_ttl": 120.0,
                },
            ],
        }
    }

    import json
    from pathlib import Path

    output_file = Path(output_path)
    with open(output_file, "w") as f:
        json.dump(discovery_config, f, indent=2)

    console.print(
        f"[green]✅ Auto-discovery configuration generated: {output_path}[/green]"
    )


@cli.group()
def config():
    """Configuration management commands."""
    pass


@config.command()
@click.argument(
    "template_name", type=click.Choice(["basic", "production", "development"])
)
@click.argument("output_path", type=click.Path())
def template(template_name: str, output_path: str):
    """Generate specific configuration templates."""
    federation_cli = FederationCLI()

    # For now, use the same template - could be extended for different templates
    federation_cli.generate_config_template(output_path)
    console.print(
        f"[green]✅ Generated {template_name} template: {output_path}[/green]"
    )


@config.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--key", help="Specific configuration key to show")
def show(config_path: str, key: str | None):
    """Show configuration file contents."""
    import json

    from rich.panel import Panel
    from rich.syntax import Syntax

    try:
        with open(config_path) as f:
            config_data = json.load(f)

        if key:
            # Show specific key
            if key in config_data:
                console.print(json.dumps(config_data[key], indent=2))
            else:
                console.print(f"[red]❌ Key '{key}' not found in configuration[/red]")
        else:
            # Show entire configuration
            config_syntax = Syntax(
                json.dumps(config_data, indent=2),
                "json",
                theme="monokai",
                line_numbers=True,
            )

            panel = Panel(
                config_syntax,
                title=f"🔧 Configuration: {config_path}",
                border_style="blue",
            )
            console.print(panel)

    except Exception as e:
        console.print(f"[red]❌ Error reading configuration: {e}[/red]")


def main():
    """Main CLI entry point."""
    try:
        cli()
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ Operation cancelled by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]❌ Unexpected error: {e}[/red]")
        if "--verbose" in sys.argv or "-v" in sys.argv:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
