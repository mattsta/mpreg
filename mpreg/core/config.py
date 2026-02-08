from __future__ import annotations

import json
import tomllib
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

from mpreg.core.discovery_tenant import DiscoveryTenantCredential
from mpreg.core.namespace_policy import NamespacePolicyRule
from mpreg.core.persistence.config import PersistenceConfig, PersistenceMode
from mpreg.datastructures.type_aliases import (
    AreaId,
    ClusterId,
    DurationSeconds,
    EndpointScope,
    PortAssignmentCallback,
    PortNumber,
    TenantId,
)
from mpreg.fabric.federation_config import (
    FederationConfig,
    create_strict_isolation_config,
)
from mpreg.fabric.link_state import LinkStateAreaPolicy, LinkStateMode
from mpreg.fabric.route_control import RoutePolicy
from mpreg.fabric.route_keys import RouteKeyProvider, RouteKeyRegistry
from mpreg.fabric.route_policy_directory import RoutePolicyDirectory
from mpreg.fabric.route_security import RouteAnnouncementSigner, RouteSecurityConfig


@dataclass(slots=True)
class MPREGSettings:
    """MPREG server configuration settings."""

    host: str = "127.0.0.1"
    # When None or 0, the server auto-allocates a free port.
    port: PortNumber | None = None
    name: str = "NO NAME PROVIDED"
    resources: set[str] | None = None
    peers: list[str] | None = None
    connect: str | None = None
    cluster_id: ClusterId = "default-cluster"
    advertised_urls: tuple[str, ...] | None = None
    gossip_interval: DurationSeconds = 5.0
    goodbye_reconnect_grace_seconds: DurationSeconds = 5.0
    log_level: str = "INFO"
    log_debug_scopes: tuple[str, ...] = ()

    # Monitoring configuration
    monitoring_enabled: bool = True
    monitoring_port: PortNumber | None = None
    monitoring_host: str | None = None
    monitoring_enable_cors: bool = True
    on_port_assigned: PortAssignmentCallback | None = None
    on_monitoring_port_assigned: PortAssignmentCallback | None = None

    # Federation configuration
    federation_config: FederationConfig | None = None

    # Cache/system defaults
    enable_default_cache: bool = False
    enable_default_queue: bool = False
    enable_cache_federation: bool = False
    cache_region: str = "local"
    cache_latitude: float = 0.0
    cache_longitude: float = 0.0
    cache_capacity_mb: int = 512
    persistence_config: PersistenceConfig | None = None

    # Fabric routing configuration
    fabric_routing_enabled: bool = True
    fabric_routing_max_hops: int = 5
    fabric_catalog_ttl_seconds: float = 120.0
    fabric_route_ttl_seconds: float = 30.0
    fabric_route_announce_interval_seconds: float = 10.0
    fabric_link_state_mode: LinkStateMode = LinkStateMode.DISABLED
    fabric_link_state_ttl_seconds: DurationSeconds = 30.0
    fabric_link_state_announce_interval_seconds: DurationSeconds = 10.0
    fabric_link_state_ecmp_paths: int = 1
    fabric_link_state_area: AreaId | None = None
    fabric_link_state_area_policy: LinkStateAreaPolicy | None = None
    fabric_route_policy: RoutePolicy | None = None
    fabric_route_export_policy: RoutePolicy | None = None
    fabric_route_neighbor_policies: RoutePolicyDirectory | None = None
    fabric_route_export_neighbor_policies: RoutePolicyDirectory | None = None
    fabric_route_security_config: RouteSecurityConfig | None = None
    fabric_route_signer: RouteAnnouncementSigner | None = None
    fabric_route_key_registry: RouteKeyRegistry | None = None
    fabric_route_key_provider: RouteKeyProvider | None = None
    fabric_route_key_refresh_interval_seconds: DurationSeconds = 60.0
    fabric_route_key_ttl_seconds: DurationSeconds = 120.0
    fabric_route_key_announce_interval_seconds: DurationSeconds = 60.0
    fabric_raft_request_timeout_seconds: DurationSeconds = 1.0

    # RPC spec gossip configuration
    rpc_spec_gossip_mode: str = "summary"
    rpc_spec_gossip_namespaces: tuple[str, ...] = ()
    rpc_spec_gossip_max_bytes: int | None = None

    # Discovery resolver configuration
    discovery_resolver_mode: bool = False
    discovery_resolver_namespaces: tuple[str, ...] = ()
    discovery_resolver_seed_on_start: bool = True
    discovery_resolver_prune_interval_seconds: DurationSeconds = 30.0
    discovery_resolver_resync_interval_seconds: DurationSeconds = 0.0
    discovery_resolver_query_cache_enabled: bool = True
    discovery_resolver_query_ttl_seconds: DurationSeconds = 10.0
    discovery_resolver_query_stale_seconds: DurationSeconds = 20.0
    discovery_resolver_query_negative_ttl_seconds: DurationSeconds = 5.0
    discovery_resolver_query_cache_max_entries: int = 1000

    # Discovery summary resolver configuration
    discovery_summary_resolver_mode: bool = False
    discovery_summary_resolver_namespaces: tuple[str, ...] = ()
    discovery_summary_resolver_prune_interval_seconds: DurationSeconds = 30.0
    discovery_summary_resolver_scopes: tuple[EndpointScope, ...] = ()

    # Discovery namespace policy configuration
    discovery_policy_enabled: bool = False
    discovery_policy_default_allow: bool = True
    discovery_policy_rules: tuple[NamespacePolicyRule, ...] = ()
    discovery_tenant_mode: bool = False
    discovery_tenant_allow_request_override: bool = True
    discovery_tenant_default_id: TenantId | None = None
    discovery_tenant_header: str | None = None
    discovery_tenant_credentials: tuple[DiscoveryTenantCredential, ...] = ()
    discovery_access_audit_max_entries: int = 500

    # Discovery summary export configuration
    discovery_summary_export_enabled: bool = False
    discovery_summary_export_interval_seconds: DurationSeconds = 30.0
    discovery_summary_export_namespaces: tuple[str, ...] = ()
    discovery_summary_export_scope: EndpointScope | None = "global"
    discovery_summary_export_include_unscoped: bool = True
    discovery_summary_export_hold_down_seconds: DurationSeconds = 0.0
    discovery_summary_export_store_forward_seconds: DurationSeconds = 0.0
    discovery_summary_export_store_forward_max_messages: int = 100

    # Discovery backpressure configuration
    discovery_rate_limit_requests_per_minute: int = 0
    discovery_rate_limit_window_seconds: DurationSeconds = 60.0
    discovery_rate_limit_max_keys: int = 2000

    # DNS interoperability gateway configuration
    dns_gateway_enabled: bool = False
    dns_listen_host: str | None = None
    dns_udp_port: PortNumber | None = None
    dns_tcp_port: PortNumber | None = None
    dns_zones: tuple[str, ...] = ("mpreg",)
    dns_min_ttl_seconds: DurationSeconds = 1.0
    dns_max_ttl_seconds: DurationSeconds = 60.0
    dns_allow_external_names: bool = False
    dns_viewer_cluster_id: ClusterId | None = None
    dns_viewer_tenant_id: TenantId | None = None

    def __post_init__(self) -> None:
        """Initialize default federation configuration if not provided."""
        if self.federation_config is None:
            # Create strict isolation by default for security
            object.__setattr__(
                self,
                "federation_config",
                create_strict_isolation_config(self.cluster_id),
            )
        if isinstance(self.fabric_link_state_mode, str):
            try:
                object.__setattr__(
                    self,
                    "fabric_link_state_mode",
                    LinkStateMode(self.fabric_link_state_mode),
                )
            except ValueError:
                object.__setattr__(
                    self,
                    "fabric_link_state_mode",
                    LinkStateMode.DISABLED,
                )
        if self.discovery_policy_rules:
            normalized_rules: list[NamespacePolicyRule] = []
            for rule in self.discovery_policy_rules:
                if isinstance(rule, NamespacePolicyRule):
                    normalized_rules.append(rule)
                    continue
                if isinstance(rule, dict):
                    normalized_rules.append(NamespacePolicyRule.from_dict(rule))
                    continue
                raise ValueError(
                    f"Unsupported discovery_policy_rules entry: {type(rule).__name__}"
                )
            self.discovery_policy_rules = tuple(normalized_rules)
        if self.discovery_summary_export_scope is not None:
            scope = str(self.discovery_summary_export_scope).strip().lower()
            if not scope:
                self.discovery_summary_export_scope = None
            else:
                alias = "zone" if scope == "cluster" else scope
                if alias not in {"local", "zone", "region", "global"}:
                    raise ValueError(
                        f"Unsupported discovery_summary_export_scope: {scope}"
                    )
                self.discovery_summary_export_scope = alias
        if self.discovery_summary_resolver_scopes:
            normalized_scopes: list[str] = []
            for scope in self.discovery_summary_resolver_scopes:
                value = str(scope).strip().lower()
                if not value:
                    continue
                alias = "zone" if value == "cluster" else value
                if alias not in {"local", "zone", "region", "global"}:
                    raise ValueError(
                        f"Unsupported discovery_summary_resolver_scope: {scope}"
                    )
                normalized_scopes.append(alias)
            self.discovery_summary_resolver_scopes = tuple(normalized_scopes)
        if self.dns_zones:
            normalized_zones = [
                str(zone).strip().strip(".").lower()
                for zone in self.dns_zones
                if str(zone).strip().strip(".")
            ]
            self.dns_zones = tuple(dict.fromkeys(normalized_zones))
        if self.dns_min_ttl_seconds < 1:
            self.dns_min_ttl_seconds = 1.0
        if self.dns_max_ttl_seconds < self.dns_min_ttl_seconds:
            self.dns_max_ttl_seconds = max(
                float(self.dns_min_ttl_seconds), float(self.dns_max_ttl_seconds)
            )
        if self.discovery_access_audit_max_entries < 0:
            self.discovery_access_audit_max_entries = 0
        if self.discovery_rate_limit_requests_per_minute < 0:
            self.discovery_rate_limit_requests_per_minute = 0
        if self.discovery_rate_limit_window_seconds < 0:
            self.discovery_rate_limit_window_seconds = 0.0
        if self.discovery_tenant_header is not None:
            header = str(self.discovery_tenant_header).strip().lower()
            self.discovery_tenant_header = header or None
        if self.discovery_tenant_credentials:
            normalized_credentials: list[DiscoveryTenantCredential] = []
            for entry in self.discovery_tenant_credentials:
                if isinstance(entry, DiscoveryTenantCredential):
                    normalized_credentials.append(entry)
                    continue
                if isinstance(entry, dict):
                    normalized_credentials.append(
                        DiscoveryTenantCredential.from_dict(entry)
                    )
                    continue
                raise ValueError(
                    "Unsupported discovery_tenant_credentials entry: "
                    f"{type(entry).__name__}"
                )
            self.discovery_tenant_credentials = tuple(normalized_credentials)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> MPREGSettings:
        data = dict(payload)
        persistence_payload = data.pop("persistence_config", None)
        persistence_config = cls._parse_persistence_config(persistence_payload)
        if persistence_payload is not None or persistence_config is not None:
            data["persistence_config"] = persistence_config

        field_names = {field.name for field in fields(cls)}
        cleaned = {key: value for key, value in data.items() if key in field_names}
        cleaned = cls._normalize_collections(cleaned)
        return cls(**cleaned)

    @classmethod
    def from_json(cls, path: str | Path) -> MPREGSettings:
        payload = json.loads(Path(path).read_text())
        if isinstance(payload, dict) and isinstance(payload.get("mpreg"), dict):
            payload = payload["mpreg"]
        if not isinstance(payload, dict):
            raise ValueError("Settings JSON must be an object")
        return cls.from_dict(payload)

    @classmethod
    def from_toml(cls, path: str | Path) -> MPREGSettings:
        payload = tomllib.loads(Path(path).read_text())
        if isinstance(payload, dict) and isinstance(payload.get("mpreg"), dict):
            payload = payload["mpreg"]
        if not isinstance(payload, dict):
            raise ValueError("Settings TOML must be a table")
        return cls.from_dict(payload)

    @classmethod
    def from_path(cls, path: str | Path) -> MPREGSettings:
        path = Path(path)
        suffix = path.suffix.lower()
        if suffix in {".toml", ".tml"}:
            return cls.from_toml(path)
        if suffix == ".json":
            return cls.from_json(path)
        raise ValueError(f"Unsupported settings file extension: {suffix}")

    @staticmethod
    def _parse_persistence_config(
        payload: Any,
    ) -> PersistenceConfig | None:
        if payload is None:
            return None
        if isinstance(payload, PersistenceConfig):
            return payload
        if not isinstance(payload, dict):
            raise ValueError("persistence_config must be a mapping")

        mode_value = payload.get("mode", PersistenceMode.MEMORY.value)
        mode = (
            mode_value
            if isinstance(mode_value, PersistenceMode)
            else PersistenceMode(mode_value)
        )
        data_dir = payload.get("data_dir")
        if data_dir is not None and not isinstance(data_dir, Path):
            data_dir = Path(data_dir)

        return PersistenceConfig(
            mode=mode,
            data_dir=data_dir or Path("/tmp/mpreg_data"),
            sqlite_filename=str(payload.get("sqlite_filename", "mpreg.sqlite")),
            sqlite_wal=bool(payload.get("sqlite_wal", True)),
            sqlite_synchronous=str(payload.get("sqlite_synchronous", "NORMAL")),
            sqlite_foreign_keys=bool(payload.get("sqlite_foreign_keys", True)),
        )

    @staticmethod
    def _normalize_collections(data: dict[str, Any]) -> dict[str, Any]:
        resources = data.get("resources")
        if resources is not None and not isinstance(resources, set):
            if isinstance(resources, str):
                data["resources"] = {resources}
            else:
                data["resources"] = set(resources)

        peers = data.get("peers")
        if peers is not None and not isinstance(peers, list):
            if isinstance(peers, str):
                data["peers"] = [peers]
            else:
                data["peers"] = list(peers)

        advertised = data.get("advertised_urls")
        if advertised is not None and not isinstance(advertised, tuple):
            if isinstance(advertised, str):
                data["advertised_urls"] = (advertised,)
            else:
                data["advertised_urls"] = tuple(advertised)

        scopes = data.get("log_debug_scopes")
        if scopes is not None and not isinstance(scopes, tuple):
            if isinstance(scopes, str):
                data["log_debug_scopes"] = (scopes,)
            else:
                data["log_debug_scopes"] = tuple(scopes)

        gossip_namespaces = data.get("rpc_spec_gossip_namespaces")
        if gossip_namespaces is not None and not isinstance(gossip_namespaces, tuple):
            if isinstance(gossip_namespaces, str):
                data["rpc_spec_gossip_namespaces"] = (gossip_namespaces,)
            else:
                data["rpc_spec_gossip_namespaces"] = tuple(gossip_namespaces)

        resolver_namespaces = data.get("discovery_resolver_namespaces")
        if resolver_namespaces is not None and not isinstance(
            resolver_namespaces, tuple
        ):
            if isinstance(resolver_namespaces, str):
                data["discovery_resolver_namespaces"] = (resolver_namespaces,)
            else:
                data["discovery_resolver_namespaces"] = tuple(resolver_namespaces)

        summary_resolver_namespaces = data.get("discovery_summary_resolver_namespaces")
        if summary_resolver_namespaces is not None and not isinstance(
            summary_resolver_namespaces, tuple
        ):
            if isinstance(summary_resolver_namespaces, str):
                data["discovery_summary_resolver_namespaces"] = (
                    summary_resolver_namespaces,
                )
            else:
                data["discovery_summary_resolver_namespaces"] = tuple(
                    summary_resolver_namespaces
                )

        summary_resolver_scopes = data.get("discovery_summary_resolver_scopes")
        if summary_resolver_scopes is not None and not isinstance(
            summary_resolver_scopes, tuple
        ):
            if isinstance(summary_resolver_scopes, str):
                data["discovery_summary_resolver_scopes"] = (summary_resolver_scopes,)
            else:
                data["discovery_summary_resolver_scopes"] = tuple(
                    summary_resolver_scopes
                )

        policy_rules = data.get("discovery_policy_rules")
        if policy_rules is not None and not isinstance(policy_rules, tuple):
            if isinstance(policy_rules, dict):
                policy_rules = (policy_rules,)
            else:
                policy_rules = tuple(policy_rules)
            data["discovery_policy_rules"] = policy_rules

        tenant_credentials = data.get("discovery_tenant_credentials")
        if tenant_credentials is not None and not isinstance(tenant_credentials, tuple):
            if isinstance(tenant_credentials, dict):
                tenant_credentials = (tenant_credentials,)
            else:
                tenant_credentials = tuple(tenant_credentials)
            data["discovery_tenant_credentials"] = tenant_credentials

        summary_namespaces = data.get("discovery_summary_export_namespaces")
        if summary_namespaces is not None and not isinstance(summary_namespaces, tuple):
            if isinstance(summary_namespaces, str):
                data["discovery_summary_export_namespaces"] = (summary_namespaces,)
            else:
                data["discovery_summary_export_namespaces"] = tuple(summary_namespaces)

        return data
