from typing import Any, Tuple, FrozenSet, Literal, Union

from pydantic import BaseModel, Field


class RPCCommand(BaseModel):
    """
    Represents a single, immutable command within an RPC request.

    This is a standardized data transfer object used for communication
    between the client and server.
    """

    name: str = Field(description="The name of the command to execute.")
    fun: str = Field(description="The name of the function to call on the target server.")
    args: Tuple[Any, ...] = Field(default_factory=tuple, description="Positional arguments for the function call.")
    locs: FrozenSet[str] = Field(default_factory=frozenset, description="Resource locations where this command can be executed.")


class RPCRequest(BaseModel):
    """
    Represents a full RPC request from a client to the server.
    """

    role: Literal["rpc"] = "rpc"
    cmds: Tuple[RPCCommand, ...] = Field(description="A tuple of RPC commands to be executed.")
    u: str = Field(description="A unique identifier for this request.")


class RPCInternalRequest(BaseModel):
    """
    Represents an internal RPC request forwarded between servers.
    """

    role: Literal["internal-rpc"] = "internal-rpc"
    command: str = Field(description="The name of the command to execute.")
    args: Tuple[Any, ...] = Field(description="Positional arguments for the function call.")
    results: dict = Field(description="Intermediate results from previous RPC steps.")
    u: str = Field(description="A unique identifier for this internal request.")


class RPCInternalAnswer(BaseModel):
    """
    Represents an answer to an internal RPC request.
    """

    role: Literal["internal-answer"] = "internal-answer"
    answer: Any = Field(description="The result of the internal RPC call.")
    u: str = Field(description="The unique identifier of the internal request this is answering.")


class RPCServerHello(BaseModel):
    """
    Represents a server's hello message, advertising its capabilities.
    """

    what: Literal["HELLO"] = "HELLO"
    funs: Tuple[str, ...] = Field(description="Functions provided by this server.")
    locs: Tuple[str, ...] = Field(description="Locations/resources associated with this server.")


class RPCServerGoodbye(BaseModel):
    """
    Represents a server's goodbye message.
    """

    what: Literal["GOODBYE"] = "GOODBYE"


class RPCServerStatus(BaseModel):
    """
    Represents a server's status update message.
    """

    what: Literal["STATUS"] = "STATUS"


RPCServerMessage = Union[RPCServerHello, RPCServerGoodbye, RPCServerStatus]


class PeerInfo(BaseModel):
    """Information about a peer in the cluster."""
    url: str = Field(description="The URL of the peer.")
    funs: Tuple[str, ...] = Field(description="Functions provided by this peer.")
    locs: FrozenSet[str] = Field(description="Locations/resources associated with this peer.")
    last_seen: float = Field(description="Timestamp of when this peer was last seen alive.")


class GossipMessage(BaseModel):
    """A message exchanged during the gossip protocol."""
    role: Literal["gossip"] = "gossip"
    peers: Tuple[PeerInfo, ...] = Field(description="Information about known peers.")
    u: str = Field(description="A unique identifier for this gossip message.")


class RPCServerRequest(BaseModel):
    """
    Represents a server-to-server communication request.
    """

    role: Literal["server"] = "server"
    server: RPCServerMessage = Field(description="The server message payload.")
    u: str = Field(description="A unique identifier for this server request.")


class RPCResponse(BaseModel):
    """
    Represents a generic RPC response.
    """

    r: Any = Field(None, description="The result of the RPC call, if successful.")
    e: str = Field(None, description="Error message, if the RPC failed.")
    state: str = Field(None, description="The state of the RPC when the error occurred.")
    u: str = Field(description="The unique identifier of the request this is responding to.")


RPCMessage = Union[RPCRequest, RPCInternalRequest, RPCInternalAnswer, RPCServerRequest, GossipMessage, RPCResponse]