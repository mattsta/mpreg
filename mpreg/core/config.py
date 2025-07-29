from dataclasses import dataclass

from mpreg.datastructures.type_aliases import ClusterId, DurationSeconds
from mpreg.federation.federation_config import (
    FederationConfig,
    create_strict_isolation_config,
)


@dataclass(slots=True)
class MPREGSettings:
    """MPREG server configuration settings."""

    host: str = "127.0.0.1"
    port: int = 6666
    name: str = "NO NAME PROVIDED"
    resources: set[str] | None = None
    peers: list[str] | None = None
    connect: str | None = None
    cluster_id: ClusterId = "default-cluster"
    advertised_urls: tuple[str, ...] | None = None
    gossip_interval: DurationSeconds = 5.0
    log_level: str = "INFO"

    # Federation configuration
    federation_config: FederationConfig | None = None

    def __post_init__(self) -> None:
        """Initialize default federation configuration if not provided."""
        if self.federation_config is None:
            # Create strict isolation by default for security
            object.__setattr__(
                self,
                "federation_config",
                create_strict_isolation_config(self.cluster_id),
            )
