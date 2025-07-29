#!/usr/bin/env python3
"""
Main CLI Entry Point for MPREG Federation Management.

Provides command-line interface for comprehensive federation operations:
- Cluster discovery, registration, and management
- Health monitoring and alerting
- Performance metrics and analytics
- Configuration validation and deployment
- Backup and recovery operations
"""

import asyncio
import sys
from dataclasses import asdict, is_dataclass
from typing import Any

import click
from loguru import logger
from rich.console import Console

from .federation_cli import FederationCLI

console = Console()


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = "DEBUG" if verbose else "INFO"
    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.pass_context
def cli(ctx, verbose: bool):
    """
    MPREG Federation Management CLI.

    Comprehensive tools for managing federated MPREG clusters including
    discovery, registration, health monitoring, and deployment automation.
    """
    setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


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
    """Discover available federation clusters."""

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
    """Federation monitoring commands."""
    pass


@monitor.command()
@click.option(
    "--interval", "-i", type=int, default=30, help="Health check interval in seconds"
)
@click.option("--clusters", "-c", multiple=True, help="Specific clusters to monitor")
def health_watch(interval: int, clusters: tuple[str, ...]):
    """Continuously monitor cluster health."""

    async def _health_watch():
        federation_cli = FederationCLI()
        console.print(
            f"[bold green]🔍 Starting health monitoring (interval: {interval}s)[/bold green]"
        )

        try:
            while True:
                console.clear()
                console.print(
                    f"[bold blue]🏥 Federation Health Monitor - {asyncio.get_event_loop().time():.0f}[/bold blue]"
                )

                for cluster_id in clusters or []:
                    health_results = await federation_cli.check_cluster_health(
                        cluster_id
                    )
                    federation_cli.display_health_report(health_results)

                await asyncio.sleep(interval)

        except KeyboardInterrupt:
            console.print("\n[yellow]⚠️ Health monitoring stopped[/yellow]")

    asyncio.run(_health_watch())


@monitor.command()
@click.option(
    "--interval", "-i", type=int, default=60, help="Metrics update interval in seconds"
)
@click.option("--clusters", "-c", multiple=True, help="Specific clusters to monitor")
def metrics_watch(interval: int, clusters: tuple[str, ...]):
    """Continuously monitor cluster metrics."""

    async def _metrics_watch():
        federation_cli = FederationCLI()
        console.print(
            f"[bold green]📊 Starting metrics monitoring (interval: {interval}s)[/bold green]"
        )

        try:
            while True:
                console.clear()
                console.print(
                    f"[bold blue]📊 Federation Metrics Monitor - {asyncio.get_event_loop().time():.0f}[/bold blue]"
                )

                for cluster_id in clusters or []:
                    await federation_cli.show_metrics(cluster_id)

                await asyncio.sleep(interval)

        except KeyboardInterrupt:
            console.print("\n[yellow]⚠️ Metrics monitoring stopped[/yellow]")

    asyncio.run(_metrics_watch())


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
    """Run auto-discovery to find federation clusters."""

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
