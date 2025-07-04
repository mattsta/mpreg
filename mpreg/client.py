"""Simple test client for an empty MPREG Cluster.

Every MPREG server instance in a cluster has "echo" and "echos" commands by default,
where "echo" accepts 1 argument and "echos" accepts a list of arguments, so we can always
test a cluster with echo commands for the processing/resolution/connection logic."""

import asyncio
import pprint as pp
import sys
from dataclasses import dataclass, field
from typing import Any

import ulid
import websockets
import websockets.client
from loguru import logger

from .model import RPCCommand, RPCRequest, RPCResponse
from .serialization import JsonSerializer

if sys.version_info >= (3, 12):
    asyncio.get_event_loop().set_task_factory(asyncio.eager_task_factory)


class Request(RPCRequest):
    # This class now inherits from RPCRequest in mpreg.model
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
        req = Request(cmds=tuple(cmds), u=str(ulid.new()))

        send = req.model_dump_json()

        if self.full_log:
            logger.info("====================== NEW REQUEST ======================")
            logger.info("[{}] Sending:\n{}", req.u, pp.pformat(req.model_dump()))

        await self.websocket.send(send)

        try:
            raw_response = await asyncio.wait_for(
                self.websocket.recv(), timeout=timeout
            )
        except TimeoutError:
            logger.error("[{}] Request timed out after {} seconds.", req.u, timeout)
            raise

        response = RPCResponse.model_validate(self.serializer.deserialize(raw_response))

        if self.full_log:
            logger.info(
                "[{}] Result:\n{}", response.u, pp.pformat(response.model_dump())
            )

        assert req.u == response.u

        if response.error:
            logger.error(
                "RPC Error: {}: {}", response.error.code, response.error.message
            )
            raise Exception(f"RPC Error: {response.error.message}")

        return response.r

    async def connect(self):
        async for websocket in websockets.connect(self.url, user_agent_header=None):
            self.websocket = websocket
            # The client will now be used by MPREGClientAPI, so no direct examples here.
            return

    def run(self):
        try:
            asyncio.run(self.connect())
        except KeyboardInterrupt:
            logger.warning("EXIT REQUEST CONFIRMED")


@logger.catch
def cmd():
    import jsonargparse

    jsonargparse.CLI(Client)
