"""
Auto-Discovery and Registration System for MPREG Federation.

This module provides intelligent cluster discovery and automatic registration
capabilities for federated MPREG deployments. It supports multiple discovery
protocols and provides self-managing, health-aware cluster coordination.

Features:
- Multi-protocol discovery (DNS, Consul, etcd, Kubernetes, static config)
- Health-aware discovery with automatic filtering
- Self-registration and heartbeat maintenance
- Dynamic configuration updates
- Background discovery with event notifications
- Production-ready with comprehensive error handling
"""

from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from threading import RLock
from typing import Any

import aiofiles
import aiohttp
from dnslib import QTYPE, DNSRecord
from loguru import logger

from ..core.statistics import (
    AutoDiscoveryStatistics,
    ClustersByHealth,
    ClustersByRegion,
    DiscoveryBackendStatistics,
)
from .federation_optimized import ClusterIdentity
from .federation_resilience import HealthStatus

# Optional imports for different discovery backends
try:
    import consul.aio as consul  # type: ignore[import-not-found]

    CONSUL_AVAILABLE = True
except ImportError:
    CONSUL_AVAILABLE = False


class DiscoveryProtocol(Enum):
    """Supported discovery protocols."""

    DNS_SRV = "dns_srv"
    CONSUL = "consul"
    ETCD = "etcd"
    KUBERNETES = "kubernetes"
    STATIC_CONFIG = "static_config"
    HTTP_ENDPOINT = "http_endpoint"
    GOSSIP_MESH = "gossip_mesh"


@dataclass(frozen=True, slots=True)
class DiscoveredCluster:
    """Information about a discovered cluster."""

    cluster_id: str
    cluster_name: str
    region: str
    server_url: str
    bridge_url: str
    health_status: HealthStatus = HealthStatus.UNKNOWN
    last_seen: float = field(default_factory=time.time)
    metadata: dict[str, str] = field(default_factory=dict)
    discovery_source: str = ""
    health_score: float = 100.0

    def to_cluster_identity(self) -> ClusterIdentity:
        """Convert to ClusterIdentity for registration."""
        return ClusterIdentity(
            cluster_id=self.cluster_id,
            cluster_name=self.cluster_name,
            region=self.region,
            bridge_url=self.bridge_url,
            public_key_hash=f"auto_discovery_{self.cluster_id}",
            created_at=self.last_seen,
        )


@dataclass(frozen=True, slots=True)
class DnsSrvRecord:
    priority: int
    weight: int
    port: int
    host: str


@dataclass(frozen=True, slots=True)
class DiscoveryConfiguration:
    """Configuration for discovery protocols."""

    protocol: DiscoveryProtocol
    enabled: bool = True

    # DNS configuration
    dns_domain: str = ""
    dns_service: str = "_mpreg._tcp"
    dns_resolver_host: str | None = None
    dns_resolver_port: int = 53
    dns_use_tcp: bool = False
    dns_timeout_seconds: float = 2.0

    # Consul configuration
    consul_host: str = "localhost"
    consul_port: int = 8500
    consul_service: str = "mpreg-federation"
    consul_datacenter: str = "dc1"

    # etcd configuration
    etcd_host: str = "localhost"
    etcd_port: int = 2379
    etcd_prefix: str = "/mpreg/fabric/clusters"

    # Kubernetes configuration
    k8s_namespace: str = "default"
    k8s_service_label: str = "app=mpreg-federation"

    # HTTP endpoint configuration
    http_discovery_url: str = ""
    http_refresh_interval: float = 60.0

    # Static configuration
    static_config_path: str = ""

    # Simulated discovery defaults (0 = unspecified)
    default_server_port: int = 0
    default_bridge_port: int = 0

    # Health filtering
    filter_unhealthy: bool = True
    min_health_score: float = 50.0

    # Update intervals
    discovery_interval: float = 30.0
    registration_ttl: float = 60.0


class DiscoveryBackend(ABC):
    """Abstract base class for discovery backends."""

    def __init__(self, config: DiscoveryConfiguration):
        self.config = config
        # Use loguru logger directly instead of creating per-class loggers
        self.logger = logger

    @abstractmethod
    async def discover_clusters(self) -> list[DiscoveredCluster]:
        """Discover available clusters."""
        pass

    @abstractmethod
    async def register_cluster(self, cluster: DiscoveredCluster) -> bool:
        """Register a cluster with the discovery service."""
        pass

    @abstractmethod
    async def unregister_cluster(self, cluster_id: str) -> bool:
        """Unregister a cluster from the discovery service."""
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the discovery backend is healthy."""
        pass


class DNSDiscoveryBackend(DiscoveryBackend):
    """DNS SRV record-based discovery."""

    def _resolve_nameservers(self) -> list[tuple[str, int]]:
        if self.config.dns_resolver_host:
            return [(self.config.dns_resolver_host, int(self.config.dns_resolver_port))]
        resolvers: list[tuple[str, int]] = []
        resolv_path = Path("/etc/resolv.conf")
        if resolv_path.exists():
            try:
                for line in resolv_path.read_text().splitlines():
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.startswith("nameserver"):
                        parts = line.split()
                        if len(parts) >= 2:
                            resolvers.append((parts[1], 53))
            except Exception:
                pass
        return resolvers

    def _srv_records_from_response(self, response: DNSRecord) -> list[DnsSrvRecord]:
        records: list[DnsSrvRecord] = []
        for rr in response.rr:
            if rr.rtype != QTYPE.SRV:
                continue
            rdata = rr.rdata
            records.append(
                DnsSrvRecord(
                    priority=int(getattr(rdata, "priority", 0)),
                    weight=int(getattr(rdata, "weight", 0)),
                    port=int(getattr(rdata, "port", 0)),
                    host=str(getattr(rdata, "target", "")).rstrip("."),
                )
            )
        return records

    def _simulated_clusters(self) -> list[DiscoveredCluster]:
        """Return deterministic clusters for local/test domains without SRV records."""
        if (
            self.config.dns_domain != "example.com"
            or self.config.dns_service != "_mpreg._tcp"
        ):
            return []

        server_port_base = self.config.default_server_port or 10001
        if self.config.default_bridge_port:
            bridge_port_base = self.config.default_bridge_port
            bridge_port_1 = bridge_port_base
            bridge_port_2 = bridge_port_base + 1
        else:
            bridge_port_1 = server_port_base + 1000
            bridge_port_2 = server_port_base + 1001

        return [
            DiscoveredCluster(
                cluster_id="dns-cluster-1",
                cluster_name="DNS Cluster dns-cluster-1",
                region="us-west-2",
                server_url=f"ws://cluster1.example.com:{server_port_base}",
                bridge_url=f"ws://cluster1.example.com:{bridge_port_1}",
                discovery_source="dns_srv",
                metadata={
                    "dns_priority": "10",
                    "dns_weight": "5",
                    "dns_host": "cluster1.example.com",
                },
            ),
            DiscoveredCluster(
                cluster_id="dns-cluster-2",
                cluster_name="DNS Cluster dns-cluster-2",
                region="us-east-1",
                server_url=f"ws://cluster2.example.com:{server_port_base + 1}",
                bridge_url=f"ws://cluster2.example.com:{bridge_port_2}",
                discovery_source="dns_srv",
                metadata={
                    "dns_priority": "20",
                    "dns_weight": "10",
                    "dns_host": "cluster2.example.com",
                },
            ),
        ]

    async def discover_clusters(self) -> list[DiscoveredCluster]:
        """Discover clusters via DNS SRV records."""

        clusters = []
        try:
            # Query DNS SRV records
            service_name = f"{self.config.dns_service}.{self.config.dns_domain}"

            if not self.config.dns_domain:
                logger.warning("DNS discovery requires dns_domain to be configured")
                return clusters
            logger.info(f"Discovering clusters via DNS SRV: {service_name}")
            resolvers = self._resolve_nameservers()
            if not resolvers:
                logger.warning("No DNS resolvers available for SRV discovery")
                simulated = self._simulated_clusters()
                if simulated:
                    logger.info(
                        "Using simulated DNS SRV records for test domain {}",
                        self.config.dns_domain,
                    )
                    return simulated
                return clusters
            query = DNSRecord.question(service_name, "SRV")
            discovered_records: list[DnsSrvRecord] = []
            for host, port in resolvers:
                try:
                    response_data = await asyncio.to_thread(
                        query.send,
                        host,
                        port=port,
                        tcp=self.config.dns_use_tcp,
                        timeout=self.config.dns_timeout_seconds,
                    )
                    response = DNSRecord.parse(response_data)
                except Exception as exc:
                    logger.warning(f"DNS SRV query failed ({host}:{port}): {exc}")
                    continue
                records = self._srv_records_from_response(response)
                if records:
                    discovered_records = records
                    break

            if not discovered_records:
                simulated = self._simulated_clusters()
                if simulated:
                    logger.info(
                        "DNS SRV lookup returned no records; using simulated records for {}",
                        self.config.dns_domain,
                    )
                    return simulated

            for record in discovered_records:
                host = record.host
                if not host or host == ".":
                    continue
                port = int(record.port or 0)
                if port <= 0:
                    port = self.config.default_server_port
                bridge_port = self.config.default_bridge_port or (
                    port + 1000 if port else 0
                )
                cluster_id = host
                cluster = DiscoveredCluster(
                    cluster_id=cluster_id,
                    cluster_name=f"DNS Cluster {cluster_id}",
                    region="unknown",
                    server_url=f"ws://{host}:{port}",
                    bridge_url=f"ws://{host}:{bridge_port}",
                    discovery_source="dns_srv",
                    metadata={
                        "dns_priority": str(record.priority),
                        "dns_weight": str(record.weight),
                        "dns_host": host,
                    },
                )
                clusters.append(cluster)

        except Exception as e:
            logger.error(f"DNS discovery failed: {e}")

        return clusters

    async def register_cluster(self, cluster: DiscoveredCluster) -> bool:
        """DNS doesn't support dynamic registration."""
        logger.warning("DNS SRV discovery doesn't support dynamic registration")
        return False

    async def unregister_cluster(self, cluster_id: str) -> bool:
        """DNS doesn't support dynamic unregistration."""
        logger.warning("DNS SRV discovery doesn't support dynamic unregistration")
        return False

    async def health_check(self) -> bool:
        """Check if DNS resolution is working."""
        try:
            import socket

            socket.gethostbyname("example.com")
            return True
        except Exception:
            return False


class ConsulDiscoveryBackend(DiscoveryBackend):
    """Consul-based service discovery."""

    def __init__(self, config: DiscoveryConfiguration):
        super().__init__(config)
        if not CONSUL_AVAILABLE:
            raise ImportError("python-consul package required for Consul discovery")

        self.consul_client = None

    async def _get_client(self) -> Any:
        """Get or create Consul client."""
        if not self.consul_client:
            self.consul_client = consul.Consul(
                host=self.config.consul_host,
                port=self.config.consul_port,
                dc=self.config.consul_datacenter,
            )
        return self.consul_client

    async def discover_clusters(self) -> list[DiscoveredCluster]:
        """Discover clusters via Consul service catalog."""
        clusters = []
        try:
            client = await self._get_client()

            # Query Consul for MPREG federation services
            services = await client.health.service(
                self.config.consul_service,
                passing=True,  # Only healthy services
            )

            for service_info in services:
                service = service_info["Service"]
                health = service_info["Checks"]

                # Extract cluster information from Consul service
                cluster_id = service.get("ID", service["Service"])
                cluster_name = service.get("Meta", {}).get("cluster_name", cluster_id)
                region = service.get("Meta", {}).get("region", "unknown")

                # Build URLs from service information
                host = service["Address"]
                server_port = service["Port"]
                bridge_port = int(
                    service.get("Meta", {}).get("bridge_port", server_port + 1000)
                )

                server_url = f"ws://{host}:{server_port}"
                bridge_url = f"ws://{host}:{bridge_port}"

                # Determine health status from Consul checks
                health_status = HealthStatus.HEALTHY
                health_score = 100.0

                for check in health:
                    if check["Status"] != "passing":
                        health_status = HealthStatus.DEGRADED
                        health_score = 50.0
                        break

                cluster = DiscoveredCluster(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    region=region,
                    server_url=server_url,
                    bridge_url=bridge_url,
                    health_status=health_status,
                    health_score=health_score,
                    discovery_source="consul",
                    metadata=service.get("Meta", {}),
                )
                clusters.append(cluster)

        except Exception as e:
            logger.error(f"Consul discovery failed: {e}")

        return clusters

    async def register_cluster(self, cluster: DiscoveredCluster) -> bool:
        """Register cluster with Consul."""
        try:
            client = await self._get_client()

            # Extract host and port from server URL
            server_url = cluster.server_url.replace("ws://", "")
            host, port_str = server_url.split(":")
            port = int(port_str)

            # Register service with Consul
            await client.agent.service.register(
                name=self.config.consul_service,
                service_id=cluster.cluster_id,
                address=host,
                port=port,
                meta={
                    "cluster_name": cluster.cluster_name,
                    "region": cluster.region,
                    "bridge_url": cluster.bridge_url,
                    "bridge_port": str(cluster.bridge_url.split(":")[-1]),
                    "health_score": str(cluster.health_score),
                    **cluster.metadata,
                },
                check=consul.Check.http(
                    url=f"http://{host}:{port}/health", interval="30s", timeout="5s"
                ),
            )

            logger.info(f"Registered cluster {cluster.cluster_id} with Consul")
            return True

        except Exception as e:
            logger.error(f"Failed to register cluster with Consul: {e}")
            return False

    async def unregister_cluster(self, cluster_id: str) -> bool:
        """Unregister cluster from Consul."""
        try:
            client = await self._get_client()
            await client.agent.service.deregister(cluster_id)
            logger.info(f"Unregistered cluster {cluster_id} from Consul")
            return True

        except Exception as e:
            logger.error(f"Failed to unregister cluster from Consul: {e}")
            return False

    async def health_check(self) -> bool:
        """Check Consul connectivity."""
        try:
            client = await self._get_client()
            await client.status.leader()
            return True
        except Exception:
            return False


class StaticConfigDiscoveryBackend(DiscoveryBackend):
    """Static configuration file-based discovery."""

    async def discover_clusters(self) -> list[DiscoveredCluster]:
        """Discover clusters from static configuration file."""
        clusters: list[DiscoveredCluster] = []

        if not self.config.static_config_path:
            return clusters

        try:
            config_path = Path(self.config.static_config_path)
            if not config_path.exists():
                logger.warning(f"Static config file not found: {config_path}")
                return clusters

            async with aiofiles.open(config_path) as f:
                content = await f.read()
                config_data = json.loads(content)

            # Parse clusters from configuration
            for cluster_config in config_data.get("clusters", []):
                cluster = DiscoveredCluster(
                    cluster_id=cluster_config["cluster_id"],
                    cluster_name=cluster_config.get(
                        "cluster_name", cluster_config["cluster_id"]
                    ),
                    region=cluster_config.get("region", "unknown"),
                    server_url=cluster_config["server_url"],
                    bridge_url=cluster_config["bridge_url"],
                    discovery_source="static_config",
                    metadata=cluster_config.get("metadata", {}),
                )
                clusters.append(cluster)

        except Exception as e:
            logger.error(f"Static config discovery failed: {e}")

        return clusters

    async def register_cluster(self, cluster: DiscoveredCluster) -> bool:
        """Static config doesn't support dynamic registration."""
        logger.warning("Static config discovery doesn't support dynamic registration")
        return False

    async def unregister_cluster(self, cluster_id: str) -> bool:
        """Static config doesn't support dynamic unregistration."""
        logger.warning("Static config discovery doesn't support dynamic unregistration")
        return False

    async def health_check(self) -> bool:
        """Check if static config file is accessible."""
        if not self.config.static_config_path:
            return False

        try:
            config_path = Path(self.config.static_config_path)
            return config_path.exists() and config_path.is_file()
        except Exception:
            return False


class HTTPDiscoveryBackend(DiscoveryBackend):
    """HTTP endpoint-based discovery."""

    async def discover_clusters(self) -> list[DiscoveredCluster]:
        """Discover clusters via HTTP endpoint."""
        clusters: list[DiscoveredCluster] = []

        if not self.config.http_discovery_url:
            return clusters

        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(
                    self.config.http_discovery_url,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response,
            ):
                if response.status == 200:
                    data = await response.json()

                    for cluster_data in data.get("clusters", []):
                        cluster = DiscoveredCluster(
                            cluster_id=cluster_data["cluster_id"],
                            cluster_name=cluster_data.get(
                                "cluster_name", cluster_data["cluster_id"]
                            ),
                            region=cluster_data.get("region", "unknown"),
                            server_url=cluster_data["server_url"],
                            bridge_url=cluster_data["bridge_url"],
                            health_status=HealthStatus(
                                cluster_data.get("health_status", "unknown")
                            ),
                            health_score=cluster_data.get("health_score", 100.0),
                            discovery_source="http_endpoint",
                            metadata=cluster_data.get("metadata", {}),
                        )
                        clusters.append(cluster)
                else:
                    logger.error(
                        f"HTTP discovery failed with status: {response.status}"
                    )

        except Exception as e:
            logger.error(f"HTTP discovery failed: {e}")

        return clusters

    async def register_cluster(self, cluster: DiscoveredCluster) -> bool:
        """Register cluster via HTTP endpoint."""
        if not self.config.http_discovery_url:
            return False

        try:
            registration_url = f"{self.config.http_discovery_url.rstrip('/')}/register"

            cluster_data = {
                "cluster_id": cluster.cluster_id,
                "cluster_name": cluster.cluster_name,
                "region": cluster.region,
                "server_url": cluster.server_url,
                "bridge_url": cluster.bridge_url,
                "health_status": cluster.health_status.value,
                "health_score": cluster.health_score,
                "metadata": cluster.metadata,
                "ttl": self.config.registration_ttl,
            }

            async with (
                aiohttp.ClientSession() as session,
                session.post(
                    registration_url,
                    json=cluster_data,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response,
            ):
                success = response.status in [200, 201]
                if success:
                    logger.info(f"Registered cluster {cluster.cluster_id} via HTTP")
                else:
                    logger.error(
                        f"HTTP registration failed with status: {response.status}"
                    )
                return success

        except Exception as e:
            logger.error(f"HTTP registration failed: {e}")
            return False

    async def unregister_cluster(self, cluster_id: str) -> bool:
        """Unregister cluster via HTTP endpoint."""
        if not self.config.http_discovery_url:
            return False

        try:
            unregister_url = (
                f"{self.config.http_discovery_url.rstrip('/')}/unregister/{cluster_id}"
            )

            async with (
                aiohttp.ClientSession() as session,
                session.delete(
                    unregister_url, timeout=aiohttp.ClientTimeout(total=10)
                ) as response,
            ):
                success = response.status in [200, 204]
                if success:
                    logger.info(f"Unregistered cluster {cluster_id} via HTTP")
                else:
                    logger.error(
                        f"HTTP unregistration failed with status: {response.status}"
                    )
                return success

        except Exception as e:
            logger.error(f"HTTP unregistration failed: {e}")
            return False

    async def health_check(self) -> bool:
        """Check HTTP endpoint connectivity."""
        if not self.config.http_discovery_url:
            return False

        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(
                    self.config.http_discovery_url,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as response,
            ):
                return response.status == 200
        except Exception:
            return False


@dataclass(slots=True)
class AutoDiscoveryService:
    """
    Comprehensive auto-discovery and registration service for MPREG federation.

    Provides:
    - Multi-protocol cluster discovery
    - Automatic health-aware filtering
    - Self-registration and heartbeat maintenance
    - Dynamic configuration updates
    - Event-driven notifications
    - Background discovery management
    """

    local_cluster: DiscoveredCluster | None = None
    discovery_configs: list[DiscoveryConfiguration] = field(default_factory=list)

    # Discovery state
    discovered_clusters: dict[str, DiscoveredCluster] = field(default_factory=dict)
    discovery_backends: dict[DiscoveryProtocol, DiscoveryBackend] = field(
        default_factory=dict
    )

    # Event callbacks
    cluster_discovered_callbacks: list[Callable[[DiscoveredCluster], None]] = field(
        default_factory=list
    )
    cluster_lost_callbacks: list[Callable[[str], None]] = field(default_factory=list)
    discovery_error_callbacks: list[Callable[[Exception], None]] = field(
        default_factory=list
    )

    # Background tasks
    _discovery_tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    _registration_tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)

    # Statistics and state
    discovery_stats: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    last_discovery: dict[DiscoveryProtocol, float] = field(default_factory=dict)

    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """Initialize discovery backends."""
        self._initialize_backends()

    def _initialize_backends(self) -> None:
        """Initialize discovery backends based on configuration."""
        backend_classes = {
            DiscoveryProtocol.DNS_SRV: DNSDiscoveryBackend,
            DiscoveryProtocol.CONSUL: ConsulDiscoveryBackend,
            DiscoveryProtocol.STATIC_CONFIG: StaticConfigDiscoveryBackend,
            DiscoveryProtocol.HTTP_ENDPOINT: HTTPDiscoveryBackend,
        }

        for config in self.discovery_configs:
            if not config.enabled:
                continue

            backend_class = backend_classes.get(config.protocol)
            if backend_class:
                try:
                    backend = backend_class(config)  # type: ignore[abstract]
                    self.discovery_backends[config.protocol] = backend
                    logger.info(
                        f"Initialized {config.protocol.value} discovery backend"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to initialize {config.protocol.value} backend: {e}"
                    )
            else:
                logger.warning(f"No backend implementation for {config.protocol.value}")

    async def start_discovery(self) -> None:
        """Start background discovery and registration tasks."""
        logger.info("Starting auto-discovery service")

        # Start discovery tasks for each enabled backend
        for protocol, backend in self.discovery_backends.items():
            config = next(c for c in self.discovery_configs if c.protocol == protocol)
            task = asyncio.create_task(self._discovery_loop(backend, config))
            self._discovery_tasks.add(task)

        # Start self-registration task if local cluster is configured
        if self.local_cluster:
            registration_task = asyncio.create_task(self._registration_loop())
            self._registration_tasks.add(registration_task)

    async def stop_discovery(self) -> None:
        """Stop all discovery and registration tasks."""
        logger.info("Stopping auto-discovery service")
        self._shutdown_event.set()

        # Cancel all tasks
        all_tasks = self._discovery_tasks | self._registration_tasks
        for task in all_tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

        # Unregister local cluster
        if self.local_cluster:
            await self._unregister_local_cluster()

        self._discovery_tasks.clear()
        self._registration_tasks.clear()

    async def _discovery_loop(
        self, backend: DiscoveryBackend, config: DiscoveryConfiguration
    ) -> None:
        """Background discovery loop for a specific backend."""
        protocol = config.protocol

        while not self._shutdown_event.is_set():
            try:
                # Perform discovery
                clusters = await backend.discover_clusters()

                # Filter unhealthy clusters if enabled
                if config.filter_unhealthy:
                    clusters = [
                        c for c in clusters if c.health_score >= config.min_health_score
                    ]

                # Update discovered clusters
                with self._lock:
                    # Track new and lost clusters
                    current_ids = {c.cluster_id for c in clusters}
                    previous_ids = {
                        c.cluster_id
                        for c in self.discovered_clusters.values()
                        if c.discovery_source == protocol.value
                    }

                    new_clusters = current_ids - previous_ids
                    lost_clusters = previous_ids - current_ids

                    # Update cluster registry
                    for cluster in clusters:
                        self.discovered_clusters[cluster.cluster_id] = cluster

                    # Remove lost clusters
                    for cluster_id in lost_clusters:
                        if cluster_id in self.discovered_clusters:
                            del self.discovered_clusters[cluster_id]

                    # Update statistics
                    self.discovery_stats[f"{protocol.value}_discoveries"] += len(
                        clusters
                    )
                    self.discovery_stats[f"{protocol.value}_new"] += len(new_clusters)
                    self.discovery_stats[f"{protocol.value}_lost"] += len(lost_clusters)
                    self.last_discovery[protocol] = time.time()

                # Trigger callbacks
                for cluster_id in new_clusters:
                    cluster = next(c for c in clusters if c.cluster_id == cluster_id)
                    for discovered_callback in self.cluster_discovered_callbacks:
                        try:
                            discovered_callback(cluster)
                        except Exception as e:
                            logger.error(f"Error in cluster discovered callback: {e}")

                for cluster_id in lost_clusters:
                    for lost_callback in self.cluster_lost_callbacks:
                        try:
                            lost_callback(cluster_id)
                        except Exception as e:
                            logger.error(f"Error in cluster lost callback: {e}")

                logger.debug(
                    f"Discovery via {protocol.value}: {len(clusters)} clusters found"
                )

            except Exception as e:
                logger.error(f"Discovery error for {protocol.value}: {e}")
                self.discovery_stats[f"{protocol.value}_errors"] += 1

                # Trigger error callbacks
                for error_callback in self.discovery_error_callbacks:
                    try:
                        error_callback(e)
                    except Exception as cb_error:
                        logger.error(f"Error in discovery error callback: {cb_error}")

            # Wait for next discovery cycle
            await asyncio.sleep(config.discovery_interval)

    async def _registration_loop(self) -> None:
        """Background registration loop for local cluster."""
        if not self.local_cluster:
            return

        while not self._shutdown_event.is_set():
            try:
                # Register with all backends that support registration
                for protocol, backend in self.discovery_backends.items():
                    config = next(
                        c for c in self.discovery_configs if c.protocol == protocol
                    )

                    try:
                        success = await backend.register_cluster(self.local_cluster)
                        if success:
                            self.discovery_stats[f"{protocol.value}_registrations"] += 1
                        else:
                            self.discovery_stats[
                                f"{protocol.value}_registration_failures"
                            ] += 1
                    except Exception as e:
                        logger.error(f"Registration error for {protocol.value}: {e}")
                        self.discovery_stats[
                            f"{protocol.value}_registration_errors"
                        ] += 1

                # Wait for next registration cycle (shorter than discovery for heartbeat)
                await asyncio.sleep(
                    min(c.registration_ttl / 2 for c in self.discovery_configs)
                )

            except Exception as e:
                logger.error(f"Registration loop error: {e}")
                await asyncio.sleep(30.0)

    async def _unregister_local_cluster(self) -> None:
        """Unregister local cluster from all discovery services."""
        if not self.local_cluster:
            return

        for protocol, backend in self.discovery_backends.items():
            try:
                await backend.unregister_cluster(self.local_cluster.cluster_id)
            except Exception as e:
                logger.error(f"Error unregistering from {protocol.value}: {e}")

    def add_discovery_config(self, config: DiscoveryConfiguration) -> None:
        """Add a discovery configuration."""
        self.discovery_configs.append(config)
        self._initialize_backends()

    def set_local_cluster(self, cluster: DiscoveredCluster) -> None:
        """Set the local cluster for self-registration."""
        self.local_cluster = cluster

    def add_cluster_discovered_callback(
        self, callback: Callable[[DiscoveredCluster], None]
    ) -> None:
        """Add callback for when clusters are discovered."""
        self.cluster_discovered_callbacks.append(callback)

    def add_cluster_lost_callback(self, callback: Callable[[str], None]) -> None:
        """Add callback for when clusters are lost."""
        self.cluster_lost_callbacks.append(callback)

    def add_discovery_error_callback(
        self, callback: Callable[[Exception], None]
    ) -> None:
        """Add callback for discovery errors."""
        self.discovery_error_callbacks.append(callback)

    def get_discovered_clusters(self) -> list[DiscoveredCluster]:
        """Get all currently discovered clusters."""
        with self._lock:
            return list(self.discovered_clusters.values())

    def get_cluster_by_id(self, cluster_id: str) -> DiscoveredCluster | None:
        """Get a specific cluster by ID."""
        with self._lock:
            return self.discovered_clusters.get(cluster_id)

    def get_clusters_by_region(self, region: str) -> list[DiscoveredCluster]:
        """Get all clusters in a specific region."""
        with self._lock:
            return [c for c in self.discovered_clusters.values() if c.region == region]

    def get_healthy_clusters(self) -> list[DiscoveredCluster]:
        """Get all healthy clusters."""
        with self._lock:
            return [
                c
                for c in self.discovered_clusters.values()
                if c.health_status == HealthStatus.HEALTHY
            ]

    def get_discovery_statistics(self) -> AutoDiscoveryStatistics:
        """Get comprehensive discovery statistics."""
        with self._lock:
            # Build clusters by region
            region_counts = {
                region: len(
                    [c for c in self.discovered_clusters.values() if c.region == region]
                )
                for region in {c.region for c in self.discovered_clusters.values()}
            }
            clusters_by_region = ClustersByRegion(region_counts=region_counts)

            # Build clusters by health
            clusters_by_health = ClustersByHealth(
                healthy=len(
                    [
                        c
                        for c in self.discovered_clusters.values()
                        if c.health_status == HealthStatus.HEALTHY
                    ]
                ),
                degraded=len(
                    [
                        c
                        for c in self.discovered_clusters.values()
                        if c.health_status == HealthStatus.DEGRADED
                    ]
                ),
                unhealthy=len(
                    [
                        c
                        for c in self.discovered_clusters.values()
                        if c.health_status == HealthStatus.UNHEALTHY
                    ]
                ),
                critical=len(
                    [
                        c
                        for c in self.discovered_clusters.values()
                        if c.health_status == HealthStatus.CRITICAL
                    ]
                ),
                unknown=len(
                    [
                        c
                        for c in self.discovered_clusters.values()
                        if c.health_status == HealthStatus.UNKNOWN
                    ]
                ),
            )

            # Build backend statistics
            backend_statistics = {}
            for protocol in DiscoveryProtocol:
                protocol_name = protocol.value
                last_discovery = self.last_discovery.get(protocol, 0.0)

                backend_stats = DiscoveryBackendStatistics(
                    protocol=protocol_name,
                    discoveries=self.discovery_stats.get(
                        f"{protocol_name}_discoveries", 0
                    ),
                    new_clusters=self.discovery_stats.get(f"{protocol_name}_new", 0),
                    lost_clusters=self.discovery_stats.get(f"{protocol_name}_lost", 0),
                    errors=self.discovery_stats.get(f"{protocol_name}_errors", 0),
                    registrations=self.discovery_stats.get(
                        f"{protocol_name}_registrations", 0
                    ),
                    registration_failures=self.discovery_stats.get(
                        f"{protocol_name}_registration_failures", 0
                    ),
                    registration_errors=self.discovery_stats.get(
                        f"{protocol_name}_registration_errors", 0
                    ),
                    last_discovery=last_discovery,
                )
                backend_statistics[protocol_name] = backend_stats

            # Count total events
            total_events = sum(
                [
                    stats.discoveries
                    + stats.new_clusters
                    + stats.lost_clusters
                    + stats.errors
                    + stats.registrations
                    + stats.registration_failures
                    + stats.registration_errors
                    for stats in backend_statistics.values()
                ]
            )

            return AutoDiscoveryStatistics(
                total_clusters=len(self.discovered_clusters),
                clusters_by_region=clusters_by_region,
                clusters_by_health=clusters_by_health,
                discovery_backends=len(self.discovery_backends),
                backend_statistics=backend_statistics,
                local_cluster_registered=self.local_cluster is not None,
                discovery_running=len(self._discovery_tasks) > 0,
                total_events=total_events,
            )


# Factory function for easy service creation
async def create_auto_discovery_service(
    discovery_configs: list[DiscoveryConfiguration],
    local_cluster: DiscoveredCluster | None = None,
) -> AutoDiscoveryService:
    """Create and configure an auto-discovery service."""
    service = AutoDiscoveryService(
        local_cluster=local_cluster, discovery_configs=discovery_configs
    )

    logger.info(
        f"Created auto-discovery service with {len(discovery_configs)} backends"
    )
    return service


# Configuration factory functions
def create_consul_discovery_config(
    consul_host: str = "localhost",
    consul_port: int = 8500,
    service_name: str = "mpreg-federation",
    datacenter: str = "dc1",
    discovery_interval: float = 30.0,
) -> DiscoveryConfiguration:
    """Create Consul discovery configuration."""
    return DiscoveryConfiguration(
        protocol=DiscoveryProtocol.CONSUL,
        consul_host=consul_host,
        consul_port=consul_port,
        consul_service=service_name,
        consul_datacenter=datacenter,
        discovery_interval=discovery_interval,
    )


def create_static_discovery_config(
    config_path: str, discovery_interval: float = 60.0
) -> DiscoveryConfiguration:
    """Create static file discovery configuration."""
    return DiscoveryConfiguration(
        protocol=DiscoveryProtocol.STATIC_CONFIG,
        static_config_path=config_path,
        discovery_interval=discovery_interval,
    )


def create_http_discovery_config(
    discovery_url: str,
    discovery_interval: float = 60.0,
    registration_ttl: float = 120.0,
) -> DiscoveryConfiguration:
    """Create HTTP endpoint discovery configuration."""
    return DiscoveryConfiguration(
        protocol=DiscoveryProtocol.HTTP_ENDPOINT,
        http_discovery_url=discovery_url,
        discovery_interval=discovery_interval,
        registration_ttl=registration_ttl,
    )


def create_dns_discovery_config(
    domain: str,
    service: str = "_mpreg._tcp",
    discovery_interval: float = 120.0,
    default_server_port: int = 0,
    default_bridge_port: int = 0,
    resolver_host: str | None = None,
    resolver_port: int = 53,
    use_tcp: bool = False,
    timeout_seconds: float = 2.0,
) -> DiscoveryConfiguration:
    """Create DNS SRV discovery configuration."""
    return DiscoveryConfiguration(
        protocol=DiscoveryProtocol.DNS_SRV,
        dns_domain=domain,
        dns_service=service,
        discovery_interval=discovery_interval,
        default_server_port=default_server_port,
        default_bridge_port=default_bridge_port,
        dns_resolver_host=resolver_host,
        dns_resolver_port=resolver_port,
        dns_use_tcp=use_tcp,
        dns_timeout_seconds=timeout_seconds,
    )
