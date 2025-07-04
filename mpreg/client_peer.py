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
    gossip_interval: float = field(default=5.0, description="Interval in seconds for sending gossip messages.")
    cluster_id: str = Field(description="The ID of the cluster this client belongs to.")
    # TODO: This should be a list of connections to multiple peers.
    peer_connection: Optional[Connection] = field(default=None, init=False)
    _gossip_task: Optional[asyncio.Task] = field(default=None, init=False)

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
                        cluster_id=self.cluster_id
                    ),
                    u=str(ulid.new())
                ).model_dump()
            )
        )

        # Start the gossip task after successful connection.
        self._gossip_task = asyncio.create_task(self._gossip_loop())

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
                        kwargs = internal_rpc.kwargs
                        u = internal_rpc.u

                        # Generate RESULT PAYLOAD
                        answer_payload = self.registry.get(command)(*args, **kwargs)
                        response_model = RPCInternalAnswer(answer=answer_payload, u=u)

                        # logger.info("[{}] Generated answer: {}", u, answer_payload)

                        # SEND RESULT PAYLOAD back UPSTREAM
                        await self.peer_connection.send(self.serializer.serialize(response_model.model_dump()))
                    case "gossip":
                        # Process incoming gossip message.
                        gossip_message = GossipMessage.model_validate(parsed_msg)
                        # In a real scenario, this would update the local cluster state.
                        logger.info("[{}] Received gossip message: {}", self.url, gossip_message.model_dump_json())
                    case _:
                        logger.error("[{}] Unknown message role: {}", self.url, parsed_msg.get("role"))
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

    async def _gossip_loop(self) -> None:
        """Periodically sends gossip messages to the connected peer."""
        while True:
            try:
                # Create a GossipMessage with information about known peers.
                # For now, we'll just send our own info as a single peer.
                gossip_message = GossipMessage(
                    peers=(PeerInfo(
                        url=self.url,
                        funs=self.local_funs,
                        locs=self.local_resources,
                        last_seen=time.time(),
                        cluster_id=self.cluster_id
                    ),),
                    u=str(ulid.new()),
                    cluster_id=self.cluster_id
                )
                await self.peer_connection.send(self.serializer.serialize(gossip_message.model_dump()))
                logger.info("[{}] Sent gossip message.", self.url)
            except Exception as e:
                logger.error("[{}] Error sending gossip message: {}", self.url, e)
            await asyncio.sleep(self.gossip_interval)
