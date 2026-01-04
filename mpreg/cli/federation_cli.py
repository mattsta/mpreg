"""
Fabric Federation Management CLI for MPREG.

Provides comprehensive command-line interface for managing fabric federated clusters:
- Cluster discovery and registration
- Health monitoring and alerting
- Performance metrics and analytics
- Configuration validation
- Deployment automation
- Backup and recovery operations
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.syntax import Syntax
from rich.table import Table
from rich.tree import Tree

from ..core.statistics import CLIDiscoveredCluster
from ..fabric.auto_discovery import (
    AutoDiscoveryService,
    create_auto_discovery_service,
    create_consul_discovery_config,
    create_dns_discovery_config,
    create_http_discovery_config,
    create_static_discovery_config,
)
from ..fabric.federation_optimized import ClusterIdentity
from ..fabric.federation_resilience import (
    EnhancedFederationResilience,
    HealthCheckConfiguration,
    RetryConfiguration,
)

console = Console()

@dataclass(slots=True)
class ClusterRegistration:
    """Registered cluster metadata for fabric-based federation tooling."""

    cluster_identity: ClusterIdentity
    server_url: str
    registered_at: float = field(default_factory=time.time)

class FederationCLI:
    """
    Comprehensive CLI for MPREG fabric federation management.

    Provides tools for:
    - Cluster lifecycle management
    - Health monitoring and diagnostics
    - Performance analysis
    - Configuration validation
    - Automated deployment
    """

    def __init__(self) -> None:
        self.console = Console()
        self.clusters: dict[str, ClusterRegistration] = {}
        self.resilience_systems: dict[str, EnhancedFederationResilience] = {}
        self.auto_discovery_service: AutoDiscoveryService | None = None

    async def discover_clusters(
        self, config_path: str | None = None
    ) -> list[CLIDiscoveredCluster]:
        """Discover available fabric clusters using auto-discovery."""
        self.console.print("[bold blue]🔍 Discovering fabric clusters...[/bold blue]")

        discovered_clusters: list[CLIDiscoveredCluster] = []

        if config_path:
            # Load from configuration file
            config_file = Path(config_path)
            if config_file.exists():
                with open(config_file) as f:
                    config_data = json.load(f)

                # Check if auto-discovery is configured
                auto_discovery_config = config_data.get("auto_discovery", {})
                if auto_discovery_config.get("enabled", False):
                    discovery_results = await self._run_auto_discovery(
                        auto_discovery_config
                    )
                    discovered_clusters.extend(discovery_results)

                # Also include static clusters from config
                static_clusters = config_data.get("clusters", [])
                for cluster_config in static_clusters:
                    discovered_clusters.append(
                        CLIDiscoveredCluster.from_config_dict(cluster_config)
                    )
        else:
            # Use default auto-discovery with multiple backends
            discovery_configs = [
                create_static_discovery_config("/tmp/mpreg-clusters.json"),
                create_dns_discovery_config("mpreg.local"),
            ]

            service = await create_auto_discovery_service(discovery_configs)

            # Run discovery once
            for backend in service.discovery_backends.values():
                try:
                    clusters = await backend.discover_clusters()
                    for cluster in clusters:
                        discovered_clusters.append(
                            CLIDiscoveredCluster.from_discovered_cluster(cluster)
                        )
                except Exception as e:
                    self.console.print(f"[yellow]⚠️ Discovery error: {e}[/yellow]")

        if not discovered_clusters:
            self.console.print("[yellow]⚠️ No clusters discovered[/yellow]")

        return discovered_clusters

    async def _run_auto_discovery(
        self, auto_discovery_config: dict[str, Any]
    ) -> list[CLIDiscoveredCluster]:
        """Run auto-discovery based on configuration."""
        discovery_configs = []

        # Parse discovery backend configurations
        backends = auto_discovery_config.get("backends", [])
        for backend_config in backends:
            protocol = backend_config.get("protocol")

            if protocol == "static_config":
                config = create_static_discovery_config(
                    config_path=backend_config.get(
                        "config_path", "/tmp/mpreg-clusters.json"
                    ),
                    discovery_interval=backend_config.get("discovery_interval", 60.0),
                )
                discovery_configs.append(config)
            elif protocol == "consul":
                config = create_consul_discovery_config(
                    consul_host=backend_config.get("host", "localhost"),
                    consul_port=backend_config.get("port", 8500),
                    service_name=backend_config.get("service_name", "mpreg-federation"),
                    datacenter=backend_config.get("datacenter", "dc1"),
                    discovery_interval=backend_config.get("discovery_interval", 30.0),
                )
                discovery_configs.append(config)
            elif protocol == "http_endpoint":
                config = create_http_discovery_config(
                    discovery_url=backend_config.get("discovery_url", ""),
                    discovery_interval=backend_config.get("discovery_interval", 60.0),
                    registration_ttl=backend_config.get("registration_ttl", 120.0),
                )
                discovery_configs.append(config)
            elif protocol == "dns_srv":
                config = create_dns_discovery_config(
                    domain=backend_config.get("domain", "mpreg.local"),
                    service=backend_config.get("service", "_mpreg._tcp"),
                    discovery_interval=backend_config.get("discovery_interval", 120.0),
                )
                discovery_configs.append(config)

        if not discovery_configs:
            return []

        # Create and run discovery service
        service = await create_auto_discovery_service(discovery_configs)
        discovered_clusters: list[CLIDiscoveredCluster] = []

        for backend in service.discovery_backends.values():
            try:
                clusters = await backend.discover_clusters()
                for cluster in clusters:
                    discovered_clusters.append(
                        CLIDiscoveredCluster.from_discovered_cluster(cluster)
                    )
            except Exception as e:
                self.console.print(f"[yellow]⚠️ Auto-discovery error: {e}[/yellow]")

        return discovered_clusters

    def display_cluster_list(self, clusters: list[CLIDiscoveredCluster]) -> None:
        """Display discovered clusters in a rich table."""
        table = Table(title="🌍 Fabric Clusters")

        table.add_column("Cluster ID", style="cyan", no_wrap=True)
        table.add_column("Name", style="magenta")
        table.add_column("Region", style="green")
        table.add_column("Status", justify="center")
        table.add_column("Health", justify="center")
        table.add_column("Source", style="yellow", no_wrap=True)
        table.add_column("Bridge URL", style="blue")

        for cluster in clusters:
            # Style status and health with colors
            status_style = {
                "active": "[green]🟢 Active[/green]",
                "inactive": "[red]🔴 Inactive[/red]",
                "maintenance": "[yellow]🟡 Maintenance[/yellow]",
            }.get(cluster.status, f"[dim]{cluster.status}[/dim]")

            health_style = {
                "healthy": "[green]✅ Healthy[/green]",
                "degraded": "[yellow]⚠️ Degraded[/yellow]",
                "unhealthy": "[red]❌ Unhealthy[/red]",
                "unknown": "[dim]❓ Unknown[/dim]",
            }.get(cluster.health, f"[dim]{cluster.health}[/dim]")

            # Discovery source with icons
            source_icons = {
                "static_config": "📁 Static",
                "consul": "🔍 Consul",
                "dns_srv": "🌐 DNS",
                "http_endpoint": "🔗 HTTP",
                "config": "📋 Config",
                "manual": "✏️ Manual",
            }
            source_display = source_icons.get(
                cluster.discovery_source, f"❓ {cluster.discovery_source}"
            )

            table.add_row(
                cluster.cluster_id,
                cluster.cluster_name,
                cluster.region,
                status_style,
                health_style,
                source_display,
                cluster.bridge_url,
            )

        self.console.print(table)

    async def register_cluster(
        self,
        cluster_id: str,
        cluster_name: str,
        region: str,
        server_url: str,
        bridge_url: str,
        enable_resilience: bool = True,
    ) -> bool:
        """Register a new fabric federation cluster."""
        self.console.print(
            f"[bold green]📝 Registering cluster: {cluster_id}[/bold green]"
        )

        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
            ) as progress:
                task = progress.add_task("Connecting to cluster...", total=None)

                # Create cluster identity
                cluster_identity = ClusterIdentity(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    region=region,
                    bridge_url=bridge_url,
                    public_key_hash=f"hash_{cluster_id}",
                    created_at=time.time(),
                )

                progress.update(task, description="Registering fabric cluster...")

                self.clusters[cluster_id] = ClusterRegistration(
                    cluster_identity=cluster_identity,
                    server_url=server_url,
                )

                if enable_resilience:
                    progress.update(
                        task, description="Setting up resilience monitoring..."
                    )

                    # Setup resilience system
                    resilience = EnhancedFederationResilience(
                        cluster_id=cluster_id,
                        health_config=HealthCheckConfiguration(),
                        retry_config=RetryConfiguration(),
                    )

                    resilience.register_cluster(cluster_identity)
                    await resilience.enable_resilience()

                    self.resilience_systems[cluster_id] = resilience

                progress.update(task, description="Registration complete!")

            self.console.print(
                f"[green]✅ Successfully registered cluster: {cluster_id}[/green]"
            )
            return True

        except Exception as e:
            self.console.print(
                f"[red]❌ Failed to register cluster {cluster_id}: {e}[/red]"
            )
            return False

    async def unregister_cluster(self, cluster_id: str) -> bool:
        """Unregister a fabric federation cluster."""
        self.console.print(
            f"[bold yellow]🗑️ Unregistering cluster: {cluster_id}[/bold yellow]"
        )

        try:
            # Disable resilience if enabled
            if cluster_id in self.resilience_systems:
                await self.resilience_systems[cluster_id].disable_resilience()
                del self.resilience_systems[cluster_id]

            # Disable federation
            if cluster_id in self.clusters:
                del self.clusters[cluster_id]

            self.console.print(
                f"[green]✅ Successfully unregistered cluster: {cluster_id}[/green]"
            )
            return True

        except Exception as e:
            self.console.print(
                f"[red]❌ Failed to unregister cluster {cluster_id}: {e}[/red]"
            )
            return False

    async def check_cluster_health(
        self, cluster_id: str | None = None
    ) -> dict[str, Any]:
        """Check health of one or all clusters."""
        if cluster_id:
            clusters_to_check = [cluster_id] if cluster_id in self.clusters else []
        else:
            clusters_to_check = list(self.clusters.keys())

        if not clusters_to_check:
            self.console.print(
                "[yellow]⚠️ No clusters available for health check[/yellow]"
            )
            return {}

        health_results = {}

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console,
        ) as progress:
            for cluster_id in clusters_to_check:
                task = progress.add_task(
                    f"Checking health: {cluster_id}...", total=None
                )

                try:
                    if cluster_id in self.resilience_systems:
                        resilience = self.resilience_systems[cluster_id]
                        health_summary = resilience.health_monitor.get_health_summary()
                        resilience_metrics = resilience.get_resilience_summary()

                        health_results[cluster_id] = {
                            "health_summary": health_summary,
                            "resilience_metrics": resilience_metrics,
                            "status": "healthy"
                            if health_summary.healthy_clusters > 0
                            else "unhealthy",
                        }
                    else:
                        # Basic registration-only health
                        cluster = self.clusters[cluster_id]
                        health_results[cluster_id] = {
                            "status": "registered",
                            "server_url": cluster.server_url,
                            "bridge_url": cluster.cluster_identity.bridge_url,
                            "region": cluster.cluster_identity.region,
                        }

                    progress.update(task, description=f"✅ {cluster_id} checked")

                except Exception as e:
                    health_results[cluster_id] = {
                        "status": "error",
                        "error": str(e),
                    }
                    progress.update(task, description=f"❌ {cluster_id} failed")

        return health_results

    def display_health_report(self, health_results: dict[str, Any]) -> None:
        """Display cluster health in a comprehensive report."""
        for cluster_id, result in health_results.items():
            # Create panel for each cluster
            if result.get("status") == "error":
                panel_content = f"[red]❌ Error: {result['error']}[/red]"
                panel_style = "red"
            elif "health_summary" in result:
                # Detailed health with resilience
                health = result["health_summary"]
                resilience = result["resilience_metrics"]

                panel_content = f"""
[bold]Global Status:[/bold] {health.global_status}
[bold]Total Clusters:[/bold] {health.total_clusters}
[bold]Healthy:[/bold] [green]{health.healthy_clusters}[/green]
[bold]Degraded:[/bold] [yellow]{health.degraded_clusters}[/yellow]
[bold]Unhealthy:[/bold] [red]{health.unhealthy_clusters}[/red]

[bold]Resilience Enabled:[/bold] {"✅" if resilience.enabled else "❌"}
[bold]Circuit Breakers:[/bold] {len(resilience.circuit_breaker_states)}
[bold]Active Recovery:[/bold] {len(resilience.active_recovery_strategies)}
"""
                panel_style = "green" if health.healthy_clusters > 0 else "red"
            else:
                panel_content = f"""
[bold]Status:[/bold] {result.get("status", "unknown")}
[bold]Server:[/bold] {result.get("server_url", "n/a")}
[bold]Bridge:[/bold] {result.get("bridge_url", "n/a")}
[bold]Region:[/bold] {result.get("region", "n/a")}
"""
                panel_style = "yellow"

            panel = Panel(
                panel_content.strip(),
                title=f"🏥 {cluster_id} Health",
                border_style=panel_style,
            )
            self.console.print(panel)

    async def show_metrics(self, cluster_id: str | None = None) -> None:
        """Display performance metrics for clusters."""
        clusters_to_show = (
            [cluster_id]
            if cluster_id and cluster_id in self.clusters
            else list(self.clusters.keys())
        )

        if not clusters_to_show:
            self.console.print("[yellow]⚠️ No clusters available for metrics[/yellow]")
            return

        self.console.print(
            "[yellow]⚠️ Metrics collection requires live fabric telemetry; "
            "no runtime clusters are attached in the CLI registry.[/yellow]"
        )

    def generate_config_template(self, output_path: str) -> None:
        """Generate a fabric federation configuration template."""
        config_template = {
            "version": "1.0",
            "federation": {
                "enabled": True,
                "auto_discovery": True,
                "health_check_interval": 30,
                "resilience": {
                    "circuit_breaker": {
                        "failure_threshold": 5,
                        "success_threshold": 3,
                        "timeout_seconds": 60,
                    },
                    "retry_policy": {
                        "max_attempts": 3,
                        "initial_delay_seconds": 1.0,
                        "backoff_multiplier": 2.0,
                    },
                },
            },
            "clusters": [
                {
                    "cluster_id": "production-primary",
                    "cluster_name": "Production Primary Cluster",
                    "region": "us-west-2",
                    "server_url": "ws://cluster-primary.example.com:<port>",
                    "bridge_url": "ws://federation-primary.example.com:<port>",
                    "priority": 1,
                    "resources": ["compute", "storage", "networking"],
                    "tags": {"environment": "production", "tier": "primary"},
                },
                {
                    "cluster_id": "production-secondary",
                    "cluster_name": "Production Secondary Cluster",
                    "region": "eu-central-1",
                    "server_url": "ws://cluster-secondary.example.com:<port>",
                    "bridge_url": "ws://federation-secondary.example.com:<port>",
                    "priority": 2,
                    "resources": ["compute", "storage"],
                    "tags": {"environment": "production", "tier": "secondary"},
                },
            ],
            "monitoring": {
                "enabled": True,
                "metrics_port": 0,
                "alert_endpoints": [
                    "http://prometheus.monitoring.svc.cluster.local:9093/api/v1/alerts"
                ],
            },
        }

        output_file = Path(output_path)
        with open(output_file, "w") as f:
            json.dump(config_template, f, indent=2)

        self.console.print(
            f"[green]✅ Configuration template generated: {output_path}[/green]"
        )

        # Display the template
        with open(output_file) as f:
            config_syntax = Syntax(f.read(), "json", theme="monokai", line_numbers=True)

        panel = Panel(
            config_syntax,
            title="🔧 Fabric Federation Configuration Template",
            border_style="blue",
        )
        self.console.print(panel)

    async def validate_config(self, config_path: str) -> bool:
        """Validate fabric federation configuration file."""
        self.console.print(
            f"[bold blue]🔍 Validating configuration: {config_path}[/bold blue]"
        )

        config_file = Path(config_path)
        if not config_file.exists():
            self.console.print(
                f"[red]❌ Configuration file not found: {config_path}[/red]"
            )
            return False

        try:
            with open(config_file) as f:
                config = json.load(f)

            validation_results = []

            # Validate required top-level fields
            required_fields = ["version", "federation", "clusters"]
            for field in required_fields:
                if field in config:
                    validation_results.append(
                        ("✅", f"Required field '{field}' present")
                    )
                else:
                    validation_results.append(
                        ("❌", f"Missing required field: '{field}'")
                    )

            # Validate clusters
            if "clusters" in config:
                clusters = config["clusters"]
                if isinstance(clusters, list) and len(clusters) > 0:
                    validation_results.append(
                        ("✅", f"Found {len(clusters)} cluster configurations")
                    )

                    for i, cluster in enumerate(clusters):
                        cluster_required = [
                            "cluster_id",
                            "cluster_name",
                            "region",
                            "server_url",
                            "bridge_url",
                        ]
                        for field in cluster_required:
                            if field in cluster:
                                validation_results.append(
                                    ("✅", f"Cluster {i}: '{field}' present")
                                )
                            else:
                                validation_results.append(
                                    ("❌", f"Cluster {i}: Missing '{field}'")
                                )
                else:
                    validation_results.append(("❌", "No valid clusters configured"))

            # Display validation results
            table = Table(title="🔍 Configuration Validation Results")
            table.add_column("Status", width=6)
            table.add_column("Validation Check")

            all_passed = True
            for status, check in validation_results:
                if status == "❌":
                    all_passed = False
                table.add_row(status, check)

            self.console.print(table)

            if all_passed:
                self.console.print("[green]✅ Configuration validation passed![/green]")
            else:
                self.console.print("[red]❌ Configuration validation failed![/red]")

            return all_passed

        except json.JSONDecodeError as e:
            self.console.print(f"[red]❌ Invalid JSON in configuration file: {e}[/red]")
            return False
        except Exception as e:
            self.console.print(f"[red]❌ Error validating configuration: {e}[/red]")
            return False

    async def deploy_from_config(self, config_path: str) -> bool:
        """Deploy fabric federation clusters from configuration file."""
        if not await self.validate_config(config_path):
            self.console.print(
                "[red]❌ Configuration validation failed, aborting deployment[/red]"
            )
            return False

        self.console.print(
            f"[bold green]🚀 Deploying fabric federation from: {config_path}[/bold green]"
        )

        try:
            with open(config_path) as f:
                config = json.load(f)

            clusters = config.get("clusters", [])
            federation_config = config.get("federation", {})
            enable_resilience = federation_config.get("resilience", {}).get(
                "enabled", True
            )

            deployment_results = []

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
            ) as progress:
                for cluster in clusters:
                    cluster_id = cluster["cluster_id"]
                    task = progress.add_task(f"Deploying {cluster_id}...", total=None)

                    try:
                        success = await self.register_cluster(
                            cluster_id=cluster_id,
                            cluster_name=cluster["cluster_name"],
                            region=cluster["region"],
                            server_url=cluster["server_url"],
                            bridge_url=cluster["bridge_url"],
                            enable_resilience=enable_resilience,
                        )

                        deployment_results.append((cluster_id, success))
                        progress.update(
                            task,
                            description=f"{'✅' if success else '❌'} {cluster_id}",
                        )

                    except Exception as e:
                        deployment_results.append((cluster_id, False))
                        progress.update(
                            task, description=f"❌ {cluster_id} failed: {e}"
                        )

            # Display deployment summary
            table = Table(title="🚀 Deployment Summary")
            table.add_column("Cluster ID", style="cyan")
            table.add_column("Status", justify="center")

            successful_deployments = 0
            for cluster_id, success in deployment_results:
                status = (
                    "[green]✅ Success[/green]" if success else "[red]❌ Failed[/red]"
                )
                table.add_row(cluster_id, status)
                if success:
                    successful_deployments += 1

            self.console.print(table)

            if successful_deployments == len(deployment_results):
                self.console.print(
                    "[green]🎉 All clusters deployed successfully![/green]"
                )
                return True
            else:
                self.console.print(
                    f"[yellow]⚠️ {successful_deployments}/{len(deployment_results)} clusters deployed successfully[/yellow]"
                )
                return False

        except Exception as e:
            self.console.print(f"[red]❌ Deployment failed: {e}[/red]")
            return False

    def display_topology(self) -> None:
        """Display fabric federation topology as a tree."""
        if not self.clusters:
            self.console.print("[yellow]⚠️ No clusters registered[/yellow]")
            return

        tree = Tree("🌍 Fabric Federation Topology")

        for cluster_id, cluster in self.clusters.items():
            cluster_node = tree.add(f"🟢 {cluster_id}")

            cluster_node.add(f"📍 Region: {cluster.cluster_identity.region}")
            cluster_node.add(f"🔗 Server: {cluster.server_url}")
            cluster_node.add(f"🌉 Bridge: {cluster.cluster_identity.bridge_url}")

            # Add resilience info if available
            if cluster_id in self.resilience_systems:
                resilience = self.resilience_systems[cluster_id]
                resilience_node = cluster_node.add("🛡️ Resilience")
                resilience_node.add(f"Enabled: {'✅' if resilience.enabled else '❌'}")
                resilience_node.add(
                    f"Circuit Breakers: {len(resilience.circuit_breakers)}"
                )

                # Add health status
                health = resilience.health_monitor.get_health_summary()
                health_node = resilience_node.add("🏥 Health")
                health_node.add(f"Status: {health.global_status}")
                health_node.add(f"Healthy: {health.healthy_clusters}")
                health_node.add(f"Unhealthy: {health.unhealthy_clusters}")

        self.console.print(tree)

    async def cleanup_all(self) -> None:
        """Clean up all registered clusters and resources."""
        if not self.clusters and not self.resilience_systems:
            self.console.print("[yellow]⚠️ No resources to clean up[/yellow]")
            return

        self.console.print(
            "[bold yellow]🧹 Cleaning up all fabric federation resources...[/bold yellow]"
        )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console,
        ) as progress:
            # Cleanup resilience systems
            for cluster_id in list(self.resilience_systems.keys()):
                task = progress.add_task(
                    f"Disabling resilience: {cluster_id}...", total=None
                )
                try:
                    await self.resilience_systems[cluster_id].disable_resilience()
                    del self.resilience_systems[cluster_id]
                    progress.update(
                        task, description=f"✅ {cluster_id} resilience disabled"
                    )
                except Exception as e:
                    progress.update(
                        task, description=f"❌ {cluster_id} resilience error: {e}"
                    )

            # Cleanup clusters
            for cluster_id in list(self.clusters.keys()):
                task = progress.add_task(
                    f"Disabling cluster: {cluster_id}...", total=None
                )
                try:
                    del self.clusters[cluster_id]
                    progress.update(task, description=f"✅ {cluster_id} disabled")
                except Exception as e:
                    progress.update(task, description=f"❌ {cluster_id} error: {e}")

        self.console.print("[green]✅ Cleanup completed[/green]")
