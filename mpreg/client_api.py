from typing import Any

from loguru import logger

from .client import Client
from .model import CommandNotFoundError, RPCCommand


class MPREGClientAPI:
    """A high-level client API for interacting with the MPREG cluster."""

    def __init__(self, url: str, full_log: bool = True):
        """Initializes the MPREGClientAPI.

        Args:
            url: The URL of the MPREG server to connect to.
            full_log: Whether to enable full logging for low-level client communication.
        """
        self._client = Client(url=url, full_log=full_log)
        self._connected = False

    async def connect(self) -> None:
        """Establishes a connection to the MPREG server."""
        if not self._connected:
            await self._client.connect()
            self._connected = True
            logger.info("Connected to MPREG server at {}", self._client.url)

    async def disconnect(self) -> None:
        """Closes the connection to the MPREG server."""
        if self._connected:
            # The underlying client's connect method is an async for loop,
            # so there's no explicit disconnect. We just stop the loop.
            # For a more robust solution, the Client class would need a disconnect method.
            logger.warning(
                "MPREGClientAPI does not support explicit disconnect yet. This will be implemented in future refactoring."
            )
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
            CommandNotFoundError: If the specified function is not found on any available server.
            asyncio.TimeoutError: If the RPC call times out.
            Exception: For other RPC errors returned by the server.
        """
        if not self._connected:
            await self.connect()

        command = RPCCommand(
            name=fun,  # Using fun as name for simplicity in this API
            fun=fun,
            args=tuple(args),
            locs=locs if locs is not None else frozenset(),
            kwargs=kwargs,
        )
        try:
            result = await self._client.request(cmds=[command], timeout=timeout)
            return result
        except Exception as e:
            # Re-raise specific RPC errors as Python exceptions for better user experience.
            if isinstance(e, Exception) and "RPC Error" in str(e):
                # Attempt to parse the structured error from the message
                # This is a temporary parsing, ideally the client.request would return structured error
                # or raise specific exceptions directly.
                if "Command not found" in str(e):
                    raise CommandNotFoundError(command_name=fun)
            raise

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
