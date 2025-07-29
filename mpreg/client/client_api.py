from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from ..core.model import CommandNotFoundException, MPREGException, RPCCommand
from .client import Client


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
            logger.info("Connected to MPREG server at {}", self._client.url)

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
        timeout: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """Calls an RPC function on the MPREG cluster.

        Args:
            fun: The name of the RPC function to call.
            *args: Positional arguments for the RPC function.
            locs: Optional set of resource locations where the command can be executed.
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
            logger.error(
                "RPC Call Failed: {}: {}", e.rpc_error.code, e.rpc_error.message
            )
            raise e
        except Exception as e:
            logger.error("RPC Call Failed: {}", e)
            raise

    async def __aenter__(self) -> "MPREGClientAPI":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.disconnect()
