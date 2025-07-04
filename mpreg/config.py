from typing import List, Optional, Set

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MPREGSettings(BaseSettings):
    """MPREG server configuration settings."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    host: str = Field("127.0.0.1", description="The host address for the server to listen on.")
    port: int = Field(6666, description="The port for the server to listen on.")
    name: str = Field("NO NAME PROVIDED", description="A human-readable name for this server instance.")
    resources: Optional[Set[str]] = Field(None, description="A set of resource keys provided by this server (e.g., dataset-A, printer-C).")
    peers: Optional[List[str]] = Field(None, description="A list of static peer URLs to initially connect to.")
    funs: Optional[dict] = Field(None, description="A mapping of RPC names to callable functions provided by this server.")
    connect: Optional[str] = Field(None, description="The URL of another server to connect to on startup (for client role).")
    cluster_id: str = Field("default-cluster", description="A unique identifier for the cluster this server belongs to.")
