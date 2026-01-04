from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from ..core.model import CommandNotFoundException, MPREGException, RPCCommand
from .client import Client

client_api_log = logger


@dataclass(slots=True)
class MPREGClientAPI:
    """A high-level client API for interacting with the MPREG cluster."""

    url: str
    full_log: bool = True

    # Fields assigned in __post_init__
    _client: Client = field(init=False)  # Needs special initialization in __post_init__
    _connected: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        """Initialize client and connection state."""
        self._client = Client(url=self.url, full_log=self.full_log)

    async def connect(self) -> None:
        """Establishes a connection to the MPREG server."""
        if not self._connected:
            await self._client.connect()
            self._connected = True
            client_api_log.info("Connected to MPREG server at {}", self._client.url)

    async def disconnect(self) -> None:
        """Closes the connection to the MPREG server."""
        if self._connected:
            await self._client.disconnect()
            self._connected = False

    async def call(
        self,
        fun: str,
        *args: Any,
        locs: frozenset[str] | None = None,
        function_id: str | None = None,
        version_constraint: str | None = None,
        target_cluster: str | None = None,
        routing_topic: str | None = None,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """Calls an RPC function on the MPREG cluster.

        Args:
            fun: The name of the RPC function to call.
            *args: Positional arguments for the RPC function.
            locs: Optional set of resource locations where the command can be executed.
            target_cluster: Optional target cluster for federated routing.
            routing_topic: Optional unified routing topic for policy-based routing.
            timeout: Optional timeout in seconds for the RPC call.
            **kwargs: Keyword arguments for the RPC function.

        Returns:
            The result of the RPC function call.

        Raises:
            CommandNotFoundException: If the specified function is not found on any available server.
            asyncio.TimeoutError: If the RPC call times out.
            Exception: For other RPC errors returned by the server.
        """
        if not self._connected:
            await self.connect()

        command = RPCCommand(
            name=fun,  # Using fun as name for simplicity in this API
            fun=fun,
            args=tuple(args),
            locs=locs or frozenset(),
            function_id=function_id,
            version_constraint=version_constraint,
            target_cluster=target_cluster,
            routing_topic=routing_topic,
            kwargs=kwargs,
        )
        try:
            result = await self._client.request(cmds=[command], timeout=timeout)
            # For single command calls, extract the result directly
            if isinstance(result, dict) and len(result) == 1:
                return list(result.values())[0]
            return result
        except CommandNotFoundException as e:
            raise e
        except MPREGException as e:
            client_api_log.error(
                "RPC Call Failed: {}: {}", e.rpc_error.code, e.rpc_error.message
            )
            raise e
        except Exception as e:
            client_api_log.error("RPC Call Failed: {}", e)
            raise

    async def list_peers(self) -> list[dict[str, Any]]:
        """Return the cluster's current peer list."""
        result = await self.call("list_peers")
        if not isinstance(result, list):
            raise TypeError(f"Expected peer list response, got {type(result).__name__}")
        return result

    async def __aenter__(self) -> MPREGClientAPI:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.disconnect()
