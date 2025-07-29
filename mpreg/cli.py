import pprint as pp
from dataclasses import dataclass
from typing import Any

from jsonargparse import CLI
from loguru import logger

from mpreg.cli.main import main as federation_main
from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


@dataclass(slots=True)
class MPREGCLI:
    """MPREG Command Line Interface for managing servers and interacting with the cluster."""

    url: str = "ws://127.0.0.1:6666/"

    async def _get_client_api(self) -> MPREGClientAPI:
        """Helper to get a connected MPREGClientAPI instance."""
        client_api = MPREGClientAPI(url=self.url)
        await client_api.connect()
        return client_api

    async def call(
        self,
        fun: str,
        *args: Any,
        locs: frozenset[str] | None = None,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> None:
        """Calls an RPC function on the MPREG cluster.

        Args:
            fun: The name of the RPC function to call.
            *args: Positional arguments for the RPC function.
            locs: Optional set of resource locations where the command can be executed.
            timeout: Optional timeout in seconds for the RPC call.
            **kwargs: Keyword arguments for the RPC function.
        """
        async with await self._get_client_api() as client_api:
            try:
                result = await client_api.call(
                    fun, *args, locs=locs, timeout=timeout, **kwargs
                )
                logger.info("RPC Call Result: {}", pp.pformat(result))
            except Exception as e:
                logger.error("RPC Call Failed: {}", e)

    async def list_peers(self) -> None:
        """Lists all known peers in the MPREG cluster and their capabilities."""
        # This command needs to be implemented by querying the cluster for its known peers.
        # For now, this is a placeholder.
        logger.info(
            "Listing peers is not yet implemented. This will require a dedicated RPC on the server."
        )

    async def start_server(
        self,
        host: str = "127.0.0.1",
        port: int = 6666,
        name: str = "MPREG Server",
        resources: set[str] | None = None,
        peers: list[str] | None = None,
        connect: str | None = None,
        cluster_id: str = "default-cluster",
        advertised_urls: tuple[str, ...] | None = None,
    ) -> None:
        """Starts an MPREG server instance.

        Args:
            host: The host address for the server to listen on.
            port: The port for the server to listen on.
            name: A human-readable name for this server instance.
            resources: A list of resource keys provided by this server.
            peers: A list of static peer URLs to initially connect to.
            connect: The URL of another server to connect to on startup.
            cluster_id: A unique identifier for the cluster this server belongs to.
            advertised_urls: List of URLs that this server advertises to other peers.
        """
        settings = MPREGSettings(
            host=host,
            port=port,
            name=name,
            resources=resources,
            peers=peers,
            connect=connect,
            cluster_id=cluster_id,
            advertised_urls=advertised_urls,
            gossip_interval=5.0,  # Add missing required field
            log_level="INFO",
        )
        server_instance = MPREGServer(settings=settings)
        await server_instance.server()

    def federation(self) -> None:
        """Access federation management CLI commands.

        Launches the comprehensive federation management interface with
        commands for cluster discovery, registration, health monitoring,
        and deployment automation.
        """
        logger.info("Launching federation management CLI...")
        federation_main()


def main() -> None:
    CLI(MPREGCLI, as_dict=False)  # type: ignore[no-untyped-call]


if __name__ == "__main__":
    main()
