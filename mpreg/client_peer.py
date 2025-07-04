import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import ulid
import websockets.client
from loguru import logger

from .connection import Connection
from .model import RPCServerRequest, RPCServerHello, RPCInternalRequest, RPCInternalAnswer
from .serialization import JsonSerializer
from .registry import CommandRegistry


@dataclass
class MPREGClient:
    """Encapsulates the client-side logic for connecting to and communicating with other MPREG servers."""

    url: str
    registry: CommandRegistry
    serializer: JsonSerializer
    local_funs: Tuple[str, ...]
    local_resources: FrozenSet[str]
    # TODO: This should be a list of connections to multiple peers.
    peer_connection: Optional[Connection] = field(default=None, init=False)

    async def connect(self) -> None:
        """Establishes a connection to the remote MPREG server and handles message exchange.

        This method connects to the peer, sends a HELLO message advertising
        its own server's capabilities (functions and resources), and then
        enters a loop to process incoming messages from the peer.
        """
        self.peer_connection = Connection(url=self.url)
        await self.peer_connection.connect()

        # Register OURSELF with the global echo target.
        # We send a RPCServerHello message with our capabilities.
        # For now, we hardcode 'echo' and 'echos' and 'local', 'test' resources.
        # In a more advanced implementation, this would come from the server's actual registry and resources.
        await self.peer_connection.send(
            self.serializer.serialize(
                RPCServerRequest(
                    server=RPCServerHello(
                        funs=self.local_funs,
                        locs=self.local_resources,
                    ),
                    u=str(ulid.new())
                ).model_dump()
            )
        )

        # This is the processing loop for US AS A CLUSTER CLIENT.
        # Here we RECEIVE messages from OTHER servers for LOCAL PROCESSING then REPLYING TO THE UPSTREAM.
        try:
            async for msg in self.peer_connection.websocket:
                parsed_msg = self.serializer.deserialize(msg)
                if False:
                    logger.info(
                        "[{} :: {}] Cluster message: {}",
                        self.url,
                        parsed_msg.get("u", "[no id]"),
                        parsed_msg,
                    )
                match parsed_msg.get("role"):
                    case "server":
                        server_request = RPCServerRequest.model_validate(parsed_msg)
                        logger.info(
                            "[{}:{}] Server status: {}",
                            *self.peer_connection.websocket.remote_address,
                            server_request.model_dump_json(),
                        )
                    case "internal-rpc":
                        # FORWARDED REQUEST from ANOTHER SERVER in MID-RPC mode.
                        # We know this request is FOR US since it was sent TO US directly.
                        internal_rpc = RPCInternalRequest.model_validate(parsed_msg)
                        command = internal_rpc.command
                        args = internal_rpc.args
                        u = internal_rpc.u

                        # Generate RESULT PAYLOAD
                        answer_payload = self.registry.get(command)(*args)
                        response_model = RPCInternalAnswer(answer=answer_payload, u=u)

                        # logger.info("[{}] Generated answer: {}", u, answer_payload)

                        # SEND RESULT PAYLOAD back UPSTREAM
                        await self.peer_connection.send(self.serializer.serialize(response_model.model_dump()))
        except (
            websockets.ConnectionClosedError,
            websockets.ConnectionClosedOK,
        ):
            # Note: we just need to catch here since the `websockets.connect()` we're inside
            #       is already an infinite generator for connection retries...
            logger.info("Server disconnected... reconnecting...")
        except Exception as e:
            logger.error("[{}] Error in peer connection: {}", self.url, e)
            await asyncio.sleep(1) # Wait before retrying to connect
