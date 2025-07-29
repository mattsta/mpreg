import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field

from mpreg.datastructures.federated_types import (
    AdvertisedURLs,
    AnnouncementID,
    FederatedPropagationInfo,
    FunctionNames,
    HopCount,
    MaxHops,
    ResourceNames,
    ServerCapabilities,
)
from mpreg.datastructures.type_aliases import (
    ClusterID,
    DurationMilliseconds,
    NodeURL,
    RequestId,
    Timestamp,
)


class RPCCommand(BaseModel):
    """
    Represents a single, immutable command within an RPC request.

    This is a standardized data transfer object used for communication
    between the client and server.
    """

    name: str = Field(description="The name of the command to execute.")
    fun: str = Field(
        description="The name of the function to call on the target server."
    )
    args: tuple[Any, ...] = Field(
        default_factory=tuple, description="Positional arguments for the function call."
    )
    kwargs: dict[str, Any] = Field(
        default_factory=dict, description="Keyword arguments for the function call."
    )
    locs: frozenset[str] = Field(
        default_factory=frozenset,
        description="Resource locations where this command can be executed.",
    )


class RPCRequest(BaseModel):
    """
    Represents a full RPC request from a client to the server.

    Now supports enhanced debugging features including intermediate results
    and execution summaries for improved observability.
    """

    role: Literal["rpc"] = "rpc"
    cmds: tuple[RPCCommand, ...] = Field(
        description="A tuple of RPC commands to be executed."
    )
    u: str = Field(description="A unique identifier for this request.")

    # Enhanced debugging features (optional, backward compatible)
    return_intermediate_results: bool = Field(
        default=False,
        description="Stream intermediate results after each execution level",
    )
    intermediate_result_callback_topic: str | None = Field(
        default=None, description="Optional topic to publish intermediate results to"
    )
    include_execution_summary: bool = Field(
        default=False, description="Include detailed execution summary in response"
    )
    debug_mode: bool = Field(
        default=False, description="Enable verbose debugging information"
    )


class RPCInternalRequest(BaseModel):
    """
    Represents an internal RPC request forwarded between servers.
    """

    role: Literal["internal-rpc"] = "internal-rpc"
    command: str = Field(description="The name of the command to execute.")
    args: tuple[Any, ...] = Field(
        description="Positional arguments for the function call."
    )
    kwargs: dict[str, Any] = Field(
        default_factory=dict, description="Keyword arguments for the function call."
    )
    results: dict[str, Any] = Field(
        description="Intermediate results from previous RPC steps."
    )
    u: str = Field(description="A unique identifier for this internal request.")


class RPCInternalAnswer(BaseModel):
    """
    Represents an answer to an internal RPC request.
    """

    role: Literal["internal-answer"] = "internal-answer"
    answer: Any = Field(description="The result of the internal RPC call.")
    u: str = Field(
        description="The unique identifier of the internal request this is answering."
    )


class RPCServerHello(BaseModel):
    """
    Represents a server's hello message, advertising its capabilities.

    Now uses properly typed federated RPC fields for better type safety
    and semantic clarity.
    """

    what: Literal["HELLO"] = "HELLO"
    funs: FunctionNames = Field(description="Functions provided by this server.")
    locs: ResourceNames = Field(
        description="Locations/resources associated with this server."
    )
    cluster_id: ClusterID = Field(
        description="The ID of the cluster this server belongs to."
    )
    advertised_urls: AdvertisedURLs = Field(
        default_factory=tuple,
        description="List of URLs that this server advertises for inbound connections.",
    )
    # Federated propagation fields (now properly typed)
    hop_count: HopCount = Field(
        default=0, description="Number of hops from original source"
    )
    max_hops: MaxHops = Field(
        default=3, description="Maximum hops before dropping message"
    )
    announcement_id: AnnouncementID = Field(
        default="", description="Unique ID for deduplication"
    )
    original_source: NodeURL = Field(
        default="", description="Original server that announced the function"
    )

    @classmethod
    def create_from_capabilities(
        cls,
        capabilities: ServerCapabilities,
        propagation: FederatedPropagationInfo | None = None,
    ) -> "RPCServerHello":
        """Create RPCServerHello from structured types."""
        if propagation is None:
            propagation = FederatedPropagationInfo.create_initial("")

        return cls(
            funs=capabilities.functions,
            locs=capabilities.resources,
            cluster_id=capabilities.cluster_id,
            advertised_urls=capabilities.advertised_urls,
            hop_count=propagation.hop_count,
            max_hops=propagation.max_hops,
            announcement_id=propagation.announcement_id,
            original_source=propagation.original_source,
        )

    @property
    def capabilities(self) -> ServerCapabilities:
        """Extract server capabilities from this hello message."""
        return ServerCapabilities(
            functions=self.funs,
            resources=self.locs,
            cluster_id=self.cluster_id,
            advertised_urls=self.advertised_urls,
        )

    @property
    def propagation_info(self) -> FederatedPropagationInfo:
        """Extract federated propagation info from this hello message."""
        return FederatedPropagationInfo(
            hop_count=self.hop_count,
            max_hops=self.max_hops,
            announcement_id=self.announcement_id,
            original_source=self.original_source,
        )

    @property
    def can_forward(self) -> bool:
        """Check if this hello message can be forwarded to another hop."""
        return self.propagation_info.can_forward

    @property
    def is_federated(self) -> bool:
        """Check if this is a federated announcement (hop_count > 0)."""
        return self.propagation_info.is_federated


class GoodbyeReason(Enum):
    """Reasons for node departure."""

    GRACEFUL_SHUTDOWN = "graceful_shutdown"
    MAINTENANCE = "maintenance"
    CLUSTER_REBALANCE = "cluster_rebalance"
    MANUAL_REMOVAL = "manual_removal"


class RPCServerGoodbye(BaseModel):
    """
    Server goodbye message for graceful departure from cluster.

    This message is sent by a node before it leaves the cluster to ensure
    proper cleanup and prevent connection retry storms.
    """

    what: Literal["GOODBYE"] = "GOODBYE"
    departing_node_url: str = Field(description="URL of the departing node")
    cluster_id: str = Field(description="Cluster the node is leaving")
    reason: GoodbyeReason = Field(description="Reason for departure")
    timestamp: Timestamp = Field(
        default_factory=time.time, description="When this goodbye was sent"
    )


class RPCServerStatus(BaseModel):
    """
    Represents a server's status update message.
    """

    what: Literal["STATUS"] = "STATUS"


class RPCServerHelloAck(BaseModel):
    """
    Acknowledgment for a HELLO message, confirming function registration.

    This message is sent in response to a HELLO to confirm that the receiving
    server has successfully added the announced functions to its function map.
    """

    what: Literal["HELLO_ACK"] = "HELLO_ACK"
    original_announcement_id: AnnouncementID = Field(
        description="ID of the original HELLO announcement being acknowledged"
    )
    acknowledged_functions: FunctionNames = Field(
        description="Functions successfully added to function map"
    )
    acknowledging_server: NodeURL = Field(
        description="URL of the server sending this acknowledgment"
    )
    timestamp: Timestamp = Field(
        default_factory=time.time, description="When this acknowledgment was sent"
    )


class RPCServerFunctionMapRequest(BaseModel):
    """
    Request for a server's current function map for verification.

    Used to verify that remote servers have the expected functions in their
    function maps, ensuring consistency across the federation.
    """

    what: Literal["FUNCTION_MAP_REQUEST"] = "FUNCTION_MAP_REQUEST"
    requesting_server: NodeURL = Field(
        description="URL of the server requesting the function map"
    )


class RPCServerFunctionMapResponse(BaseModel):
    """
    Response containing a server's current function map.

    Provides the complete function map for verification purposes.
    """

    what: Literal["FUNCTION_MAP_RESPONSE"] = "FUNCTION_MAP_RESPONSE"
    responding_server: NodeURL = Field(
        description="URL of the server providing the function map"
    )
    function_map: dict[str, frozenset[str]] = Field(
        description="Complete function map: function_name -> set of server URLs that provide it"
    )
    resources_map: dict[str, frozenset[str]] = Field(
        description="Complete resources map: resource_name -> set of server URLs that have it"
    )
    timestamp: Timestamp = Field(
        default_factory=time.time, description="When this function map was generated"
    )


type RPCServerMessage = (
    RPCServerHello
    | RPCServerGoodbye
    | RPCServerStatus
    | RPCServerHelloAck
    | RPCServerFunctionMapRequest
    | RPCServerFunctionMapResponse
)


class PeerInfo(BaseModel):
    """Information about a peer in the cluster."""

    url: str = Field(description="The URL of the peer.")
    funs: tuple[str, ...] = Field(description="Functions provided by this peer.")
    locs: frozenset[str] = Field(
        description="Locations/resources associated with this peer."
    )
    last_seen: float = Field(
        description="Timestamp of when this peer was last seen alive."
    )
    cluster_id: str = Field(description="The ID of the cluster this peer belongs to.")
    advertised_urls: tuple[str, ...] = Field(
        default_factory=tuple,
        description="List of URLs that this peer advertises for inbound connections.",
    )
    # NEW: Logical clock for proper distributed ordering
    logical_clock: dict[str, int] = Field(
        default_factory=dict,
        description="Vector clock for logical ordering of peer updates",
    )


class GossipMessage(BaseModel):
    """A message exchanged during the gossip protocol."""

    role: Literal["gossip"] = "gossip"
    peers: tuple[PeerInfo, ...] = Field(description="Information about known peers.")
    u: str = Field(description="A unique identifier for this gossip message.")
    cluster_id: str = Field(
        description="The ID of the cluster this gossip message originates from."
    )


class RPCServerRequest(BaseModel):
    """
    Represents a server-to-server communication request.
    """

    role: Literal["server"] = "server"
    server: RPCServerMessage = Field(description="The server message payload.")
    u: str = Field(description="A unique identifier for this server request.")


class RPCError(BaseModel):
    """Base model for structured RPC errors."""

    code: int = Field(description="A numeric error code.")
    message: str = Field(description="A human-readable error message.")
    details: Any | None = Field(
        default=None, description="Optional additional details about the error."
    )


class RPCResponse(BaseModel):
    """
    Represents a generic RPC response.

    Now supports enhanced debugging information including intermediate results
    and execution summaries for improved observability.
    """

    r: Any = Field(
        default=None, description="The result of the RPC call, if successful."
    )
    error: RPCError | None = Field(
        default=None, description="Structured error information, if the RPC failed."
    )
    u: str = Field(
        description="The unique identifier of the request this is responding to."
    )

    # Enhanced debugging fields (optional, backward compatible)
    # Using forward references to avoid circular dependency issues
    intermediate_results: tuple["RPCIntermediateResult", ...] = Field(
        default_factory=tuple,
        description="Intermediate results from each execution level",
    )
    execution_summary: "RPCExecutionSummary | None" = Field(
        default=None, description="Summary of execution performance and steps"
    )


# ============================================================================
# INTERMEDIATE RESULTS DATA STRUCTURES
# ============================================================================


@dataclass(frozen=True, slots=True)
class RPCIntermediateResult:
    """Intermediate result from RPC execution level completion."""

    request_id: RequestId
    level_index: int
    level_results: dict[str, Any]  # Results from this specific level
    accumulated_results: dict[str, Any]  # All results available so far
    total_levels: int
    completed_levels: int
    timestamp: Timestamp
    execution_time_ms: DurationMilliseconds

    @property
    def is_final_level(self) -> bool:
        """Check if this is the final execution level."""
        return self.completed_levels == self.total_levels

    @property
    def progress_percentage(self) -> float:
        """Calculate execution progress as percentage (0.0 to 100.0)."""
        return (self.completed_levels / self.total_levels) * 100.0


@dataclass(frozen=True, slots=True)
class RPCExecutionSummary:
    """Summary of RPC execution performance and characteristics."""

    request_id: RequestId
    total_execution_time_ms: DurationMilliseconds
    total_levels: int
    level_execution_times: tuple[DurationMilliseconds, ...]
    bottleneck_level_index: int  # Level that took the longest
    average_level_time_ms: DurationMilliseconds
    parallel_commands_executed: int
    cross_cluster_hops: int

    @property
    def bottleneck_time_ms(self) -> DurationMilliseconds:
        """Get execution time of the bottleneck level."""
        return self.level_execution_times[self.bottleneck_level_index]


# ============================================================================
# PUB/SUB MESSAGE TYPES
# ============================================================================


class PubSubMessage(BaseModel):
    """
    Represents a publish/subscribe message for topic-based messaging.
    """

    topic: str = Field(description="The topic this message is published to.")
    payload: Any = Field(description="The message payload/data.")
    timestamp: float = Field(description="Unix timestamp when message was created.")
    message_id: str = Field(description="Unique identifier for this message.")
    publisher: str = Field(description="Identifier of the publishing client/server.")
    headers: dict[str, Any] = Field(
        default_factory=dict, description="Optional message headers/metadata."
    )
    # Federation routing fields
    routing_path: list[str] | None = Field(
        default=None, description="Federation routing path for multi-hop messages."
    )
    current_hop: int | None = Field(
        default=None, description="Current hop index in the routing path."
    )


class TopicPattern(BaseModel):
    """
    Represents a topic pattern for subscription matching.

    Supports AMQP-style wildcards:
    - '*' matches exactly one word/segment
    - '#' matches zero or more words/segments

    Examples:
    - 'user.*.login' matches 'user.123.login' but not 'user.123.profile.login'
    - 'market.#' matches 'market.crypto.btc' and 'market.stocks'
    """

    pattern: str = Field(description="The topic pattern with wildcards.")
    exact_match: bool = Field(
        default=False, description="Whether this is an exact match (no wildcards)."
    )


class PubSubSubscription(BaseModel):
    """
    Represents a subscription to one or more topic patterns.
    """

    subscription_id: str = Field(description="Unique identifier for this subscription.")
    patterns: tuple[TopicPattern, ...] = Field(
        description="Topic patterns this subscription matches."
    )
    subscriber: str = Field(description="Identifier of the subscribing client/server.")
    created_at: float = Field(
        description="Unix timestamp when subscription was created."
    )
    get_backlog: bool = Field(
        default=True, description="Whether to receive recent message backlog."
    )
    backlog_seconds: int = Field(
        default=300, description="How many seconds of backlog to receive."
    )


class PubSubPublish(BaseModel):
    """
    Represents a publish operation request.
    """

    role: Literal["pubsub-publish"] = "pubsub-publish"
    message: PubSubMessage = Field(description="The message to publish.")
    u: str = Field(description="Unique identifier for this publish request.")


class PubSubSubscribe(BaseModel):
    """
    Represents a subscription request.
    """

    role: Literal["pubsub-subscribe"] = "pubsub-subscribe"
    subscription: PubSubSubscription = Field(description="The subscription details.")
    u: str = Field(description="Unique identifier for this subscribe request.")


class PubSubUnsubscribe(BaseModel):
    """
    Represents an unsubscribe request.
    """

    role: Literal["pubsub-unsubscribe"] = "pubsub-unsubscribe"
    subscription_id: str = Field(description="ID of subscription to cancel.")
    u: str = Field(description="Unique identifier for this unsubscribe request.")


class PubSubNotification(BaseModel):
    """
    Represents a message notification sent to subscribers.
    """

    role: Literal["pubsub-notification"] = "pubsub-notification"
    message: PubSubMessage = Field(description="The message being delivered.")
    subscription_id: str = Field(description="ID of subscription this matches.")
    u: str = Field(description="Unique identifier for this notification.")


class PubSubAck(BaseModel):
    """
    Represents an acknowledgment for pub/sub operations.
    """

    role: Literal["pubsub-ack"] = "pubsub-ack"
    operation_id: str = Field(description="ID of the operation being acknowledged.")
    success: bool = Field(description="Whether the operation succeeded.")
    error: str | None = Field(default=None, description="Error message if failed.")
    u: str = Field(description="Unique identifier for this ack.")


class TopicAdvertisement(BaseModel):
    """
    Represents topic subscription advertisements in gossip messages.
    """

    server_url: str = Field(description="URL of the server with these subscriptions.")
    topics: tuple[str, ...] = Field(description="Active topic patterns on this server.")
    subscriber_count: int = Field(description="Number of subscribers on this server.")
    last_activity: float = Field(description="Last message activity timestamp.")


class PubSubGossip(BaseModel):
    """
    Extended gossip message that includes topic routing information.
    """

    role: Literal["pubsub-gossip"] = "pubsub-gossip"
    peers: tuple[PeerInfo, ...] = Field(description="Information about known peers.")
    topics: tuple[TopicAdvertisement, ...] = Field(
        description="Topic subscription advertisements."
    )
    u: str = Field(description="Unique identifier for this gossip message.")
    cluster_id: str = Field(description="Cluster ID this gossip originates from.")


class ConsensusProposalMessage(BaseModel):
    """
    Consensus proposal message for distributed state changes.
    """

    role: Literal["consensus-proposal"] = "consensus-proposal"
    proposal_id: str = Field(description="Unique identifier for the proposal.")
    proposer_node_id: str = Field(description="ID of the node making the proposal.")
    state_key: str = Field(description="Key of the state being changed.")
    proposed_value: dict[str, Any] = Field(description="The proposed new state value.")
    required_votes: int = Field(description="Number of votes required for consensus.")
    consensus_deadline: float = Field(description="Deadline for reaching consensus.")
    u: str = Field(description="Unique identifier for this message.")
    cluster_id: str = Field(description="Cluster ID this proposal originates from.")


class ConsensusVoteMessage(BaseModel):
    """
    Consensus vote message for distributed state changes.
    """

    role: Literal["consensus-vote"] = "consensus-vote"
    proposal_id: str = Field(description="ID of the proposal being voted on.")
    vote: bool = Field(description="The vote (True for accept, False for reject).")
    voter_id: str = Field(description="ID of the node casting the vote.")
    u: str = Field(description="Unique identifier for this message.")
    cluster_id: str = Field(description="Cluster ID this vote originates from.")


type PubSubMessage_Union = (
    PubSubPublish
    | PubSubSubscribe
    | PubSubUnsubscribe
    | PubSubNotification
    | PubSubAck
    | PubSubGossip
)


type RPCMessage = (
    RPCRequest
    | RPCInternalRequest
    | RPCInternalAnswer
    | RPCServerRequest
    | GossipMessage
    | RPCResponse
    | PubSubMessage_Union
)


@dataclass
class MPREGException(Exception):
    rpc_error: RPCError = field(
        default_factory=lambda: RPCError(
            code=-1, message="Unknown / Unset Error Condition"
        )
    )


@dataclass
class CommandNotFoundException(MPREGException):
    """Error indicating that a requested command was not found.

    Note: Commands not being found are a service/protocol level problem and not just "an error return value"
    """

    command_name: str = ""
    code: Literal[1001] = 1001
    message: Literal["Command not found"] = "Command not found"
    details: str | None = None
