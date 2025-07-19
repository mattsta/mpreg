"""Simple test client for an empty MPREG Cluster.

Every MPREG server instance in a cluster has "echo" and "echos" commands by default,
where "echo" accepts 1 argument and "echos" accepts a list of arguments, so we can always
test a cluster with echo commands for the processing/resolution/connection logic."""

import asyncio
import pprint as pp
from dataclasses import dataclass, field
from typing import Any

import ulid
import websockets
import websockets.client
from loguru import logger

from .model import MPREGException, RPCCommand, RPCRequest, RPCResponse
from .serialization import JsonSerializer

try:
    loop = asyncio.get_running_loop()
    loop.set_task_factory(asyncio.eager_task_factory)
except RuntimeError:
    # No event loop running, this is fine for imports
    pass


@dataclass
class Client:
    # websocket URL like: ws://127.0.0.1:7773
    url: str

    # optionally disable big log printing to get more accurate timing measurements
    full_log: bool = True

    websocket: websockets.client.WebSocketClientProtocol | None = field(
        default=None, init=False
    )
    serializer: JsonSerializer = field(default_factory=JsonSerializer, init=False)
    _pending_requests: dict[str, asyncio.Future[RPCResponse]] = field(
        default_factory=dict, init=False
    )
    _listener_task: asyncio.Task[None] | None = field(default=None, init=False)

    async def request(
        self, cmds: list[RPCCommand], timeout: float | None = None
    ) -> Any:
        """Sends an RPC request to the server and waits for a response.

        Args:
            cmds: A list of RPCCommand objects representing the commands to execute.
            timeout: Optional timeout in seconds for the request.

        Returns:
            The result of the RPC call.

        Raises:
            asyncio.TimeoutError: If the request times out.
            Exception: For other RPC errors returned by the server.
        """
        req = RPCRequest(cmds=tuple(cmds), u=str(ulid.new()))

        send = req.model_dump_json()

        if self.full_log:
            logger.info("====================== NEW REQUEST ======================")
            logger.info("[{}] Sending:\n{}", req.u, pp.pformat(req.model_dump()))

        if self.websocket is None:
            raise ConnectionError("WebSocket connection is not established.")

        # Create a future for this request's response
        response_future: asyncio.Future[RPCResponse] = asyncio.Future()
        self._pending_requests[req.u] = response_future

        try:
            await self.websocket.send(send)

            # Wait for the response with timeout
            response = await asyncio.wait_for(response_future, timeout=timeout)
        except TimeoutError:
            logger.error("[{}] Request timed out after {} seconds.", req.u, timeout)
            raise
        finally:
            # Clean up the pending request
            self._pending_requests.pop(req.u, None)

        if self.full_log:
            logger.info(
                "[{}] Result:\n{}", response.u, pp.pformat(response.model_dump())
            )

        assert req.u == response.u

        if response.error:
            logger.error(
                "RPC Error: {}: {}", response.error.code, response.error.message
            )
            raise MPREGException(rpc_error=response.error)

        return response.r

    async def _listen_for_responses(self) -> None:
        """Listen for incoming responses and route them to the correct pending request."""
        if not self.websocket:
            return

        try:
            async for raw_message in self.websocket:
                try:
                    response = RPCResponse.model_validate(
                        self.serializer.deserialize(
                            raw_message.encode("utf-8")
                            if isinstance(raw_message, str)
                            else raw_message
                        )
                    )

                    # Route the response to the correct pending request
                    if response.u in self._pending_requests:
                        future = self._pending_requests[response.u]
                        if not future.done():
                            future.set_result(response)
                    else:
                        logger.warning(
                            "Received response for unknown request: {}", response.u
                        )

                except Exception as e:
                    logger.error("Failed to process response: {}", e)

        except Exception as e:
            logger.error("Error in response listener: {}", e)
        finally:
            # Cancel all pending requests
            for future in self._pending_requests.values():
                if not future.done():
                    future.cancel()

    async def connect(self) -> None:
        self.websocket = await websockets.connect(self.url, user_agent_header=None)
        # Start the response listener
        self._listener_task = asyncio.create_task(self._listen_for_responses())

    async def disconnect(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        if self.websocket:
            await self.websocket.close()
        # Cancel any remaining pending requests
        for future in self._pending_requests.values():
            if not future.done():
                future.cancel()
        self._pending_requests.clear()
        self.websocket = None


@logger.catch
def cmd() -> None:
    import jsonargparse

    jsonargparse.CLI(Client)  # type: ignore[no-untyped-call]
