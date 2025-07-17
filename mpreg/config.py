from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MPREGSettings(BaseSettings):
    """MPREG server configuration settings."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    host: str = Field(
        "127.0.0.1", description="The host address for the server to listen on."
    )
    port: int = Field(6666, description="The port for the server to listen on.")
    name: str = Field(
        "NO NAME PROVIDED",
        description="A human-readable name for this server instance.",
    )
    resources: set[str] | None = Field(
        None,
        description="A set of resource keys provided by this server (e.g., dataset-A, printer-C).",
    )
    peers: list[str] | None = Field(
        None, description="A list of static peer URLs to initially connect to."
    )

    connect: str | None = Field(
        None,
        description="The URL of another server to connect to on startup (for client role).",
    )
    cluster_id: str = Field(
        "default-cluster",
        description="A unique identifier for the cluster this server belongs to.",
    )
    advertised_urls: tuple[str, ...] | None = Field(
        None,
        description="List of URLs that this server advertises to other peers for inbound connections.",
    )
    gossip_interval: float = Field(
        5.0, description="Interval in seconds for sending gossip messages."
    )
