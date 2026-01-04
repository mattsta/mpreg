from __future__ import annotations

import json
import tomllib
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

from mpreg.core.persistence.config import PersistenceConfig, PersistenceMode
from mpreg.datastructures.type_aliases import (
    AreaId,
    ClusterId,
    DurationSeconds,
    PortAssignmentCallback,
    PortNumber,
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

        return data
