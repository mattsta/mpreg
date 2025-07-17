import asyncio
from dataclasses import dataclass, field

import ulid
import websockets.client
from loguru import logger
from pydantic import Field

from .connection import Connection
from .model import (
    GossipMessage,
    PeerInfo,
    RPCInternalAnswer,
    RPCInternalRequest,
    RPCServerHello,
    RPCServerRequest,
)
from .registry import CommandRegistry
from .serialization import JsonSerializer


@dataclass
class MPREGClient:
    """Encapsulates the client-side logic for connecting to and communicating with other MPREG servers."""

    url: str
    registry: CommandRegistry
    serializer: JsonSerializer
    local_funs: tuple[str, ...]
    local_resources: frozenset[str]
    cluster_id: str = Field(description="The ID of the cluster this client belongs to.")
    local_advertised_urls: tuple[str, ...] = Field(
        default_factory=tuple, description="URLs that this client's server advertises."
    )
    cluster_peers_info: dict[str, PeerInfo] = Field(
        description="Reference to the cluster's peers_info for gossip."
    )
    gossip_interval: float = field(
        default=5.0,
        metadata={"description": "Interval in seconds for sending gossip messages."},
    )
    # TODO: This should be a list of connections to multiple peers.
    peer_connection: Connection | None = field(default=None, init=False)
    _gossip_task: asyncio.Task[None] | None = field(default=None, init=False)

    async def connect(self) -> None:
        """Establishes a connection to the remote MPREG server and handles message exchange.

        This method attempts to connect to the peer using its advertised URLs.
        It tries each URL until a successful connection is established.
        Once connected, it sends a HELLO message advertising its own server's
        capabilities (functions and resources), and then enters a loop to
        process incoming messages from the peer.
        """
        connected = False
        for url in self.local_advertised_urls:
            try:
                self.peer_connection = Connection(url=url)
                await self.peer_connection.connect()
                connected = True
                break
            except Exception as e:
                logger.warning(
                    "[{}] Failed to connect to advertised URL {}: {}", self.url, url, e
                )
                self.peer_connection = None

        if not connected:
            raise ConnectionError(
                f"Failed to connect to any advertised URL for {self.url}"
            )

        # Register OURSELF with the global echo target.
        # We send a RPCServerHello message with our capabilities.
        # In a more advanced implementation, this would come from the server's actual registry and resources.
        if self.peer_connection is not None:
            await self.peer_connection.send(
                self.serializer.serialize(
                    RPCServerRequest(
                        server=RPCServerHello(
                            funs=self.local_funs,
                            locs=tuple(self.local_resources),
                            cluster_id=self.cluster_id,
                            advertised_urls=self.local_advertised_urls,
                        ),
                        u=str(ulid.new()),
                    ).model_dump()
                )
            )

        # Start the gossip task after successful connection.
        self._gossip_task = asyncio.create_task(self._gossip_loop())

        # This is the processing loop for US AS A CLUSTER CLIENT.
        # Here we RECEIVE messages from OTHER servers for LOCAL PROCESSING then REPLYING TO THE UPSTREAM.
        try:
            if self.peer_connection is None or self.peer_connection.websocket is None:
                raise ConnectionError("Peer connection not established.")
            async for msg in self.peer_connection.websocket:
                if not isinstance(msg, bytes):
                    logger.warning("[{}] Received non-bytes message: {}", self.url, msg)
                    continue
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
                        if self.peer_connection is not None:
                            await self.peer_connection.send(
                                self.serializer.serialize(response_model.model_dump())
                            )
                    case "gossip":
                        # Process incoming gossip message.
                        gossip_message = GossipMessage.model_validate(parsed_msg)
                        # In a real scenario, this would update the local cluster state.
                        logger.info(
                            "[{}] Received gossip message: {}",
                            self.url,
                            gossip_message.model_dump_json(),
                        )
                    case _:
                        logger.error(
                            "[{}] Unknown message role: {}",
                            self.url,
                            parsed_msg.get("role"),
                        )
        except (
            websockets.ConnectionClosedError,
            websockets.ConnectionClosedOK,
        ):
            # Note: we just need to catch here since the `websockets.connect()` we're inside
            #       is already an infinite generator for connection retries...
            logger.info("Server disconnected... reconnecting...")
        except Exception as e:
            logger.error("[{}] Error in peer connection: {}", self.url, e)
            await asyncio.sleep(1)  # Wait before retrying to connect

    async def _gossip_loop(self) -> None:
        """Periodically sends gossip messages to the connected peer."""
        while True:
            try:
                # Create a GossipMessage with information about all known healthy peers.
                # This propagates the cluster state to the connected peer.
                gossip_message = GossipMessage(
                    peers=tuple(self.cluster_peers_info.values()),
                    u=str(ulid.new()),
                    cluster_id=self.cluster_id,
                )
                if self.peer_connection is not None:
                    await self.peer_connection.send(
                        self.serializer.serialize(gossip_message.model_dump())
                    )
                logger.info(
                    "[{}] Sent gossip message with {} peers.",
                    self.url,
                    len(gossip_message.peers),
                )
            except Exception as e:
                logger.error("[{}] Error sending gossip message: {}", self.url, e)
            await asyncio.sleep(self.gossip_interval)
