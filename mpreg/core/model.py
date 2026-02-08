from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field

from mpreg.datastructures.type_aliases import (
    ClusterID,
    DurationMilliseconds,
    FunctionId,
    FunctionVersion,
    RequestId,
    Timestamp,
    VersionConstraintSpec,
)


class RPCFunctionDescriptor(BaseModel):
    """Describes a function capability with identity and version metadata."""

    name: str = Field(description="Human-readable function name.")
    function_id: FunctionId = Field(
        description="Unique identifier for the function capability."
    )
    version: FunctionVersion = Field(description="Semantic version of the function.")
    resources: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Resources required to run this function.",
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
    function_id: FunctionId | None = Field(
        default=None,
        description="Optional unique function identifier for routing.",
    )
    version_constraint: VersionConstraintSpec | None = Field(
        default=None,
        description="Optional semantic version constraint for matching.",
    )
    target_cluster: ClusterID | None = Field(
        default=None,
        description="Optional target cluster identifier for federated routing.",
    )
    routing_topic: str | None = Field(
        default=None,
        description="Optional unified routing topic for policy-based routing.",
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
    instance_id: str = Field(
        default="",
        description="Unique instance ID for the departing server process.",
    )
    timestamp: Timestamp = Field(
        default_factory=time.time, description="When this goodbye was sent"
    )


class RPCServerStatus(BaseModel):
    """
    Represents a server's status update message.
    """

    what: Literal["STATUS"] = "STATUS"
    server_url: str = Field(description="URL of the reporting server")
    cluster_id: str = Field(description="Cluster ID of the reporting server")
    instance_id: str = Field(
        default="",
        description="Unique instance ID for the reporting server process.",
    )
    timestamp: Timestamp = Field(
        default_factory=time.time, description="When this status was generated"
    )
    status: str = Field(default="ok", description="High-level status string")
    active_clients: int = Field(default=0, description="Active client connections")
    peer_count: int = Field(default=0, description="Known peers in cluster")
    funs: tuple[str, ...] = Field(
        default_factory=tuple, description="Known function names on this server"
    )
    locs: frozenset[str] = Field(
        default_factory=frozenset,
        description="Resource locations associated with this server",
    )
    function_catalog: tuple[RPCFunctionDescriptor, ...] = Field(
        default_factory=tuple,
        description="Detailed function descriptors for routing/identity.",
    )
    advertised_urls: tuple[str, ...] = Field(
        default_factory=tuple,
        description="URLs advertised for inbound connections",
    )
    metrics: dict[str, Any] = Field(
        default_factory=dict, description="Additional status metrics"
    )


@dataclass(frozen=True, slots=True)
class CacheStatusMetrics:
    """Structured cache status metrics for server status payloads."""

    cache_region: str
    cache_latitude: float
    cache_longitude: float
    cache_capacity_mb: int
    cache_utilization_percent: float
    cache_avg_latency_ms: float
    cache_reliability_score: float


@dataclass(frozen=True, slots=True)
class QueueStatusMetrics:
    """Structured queue status metrics for server status payloads."""

    total_queues: int
    total_messages_sent: int
    total_messages_received: int
    total_messages_acknowledged: int
    total_messages_failed: int
    active_subscriptions: int
    success_rate: float


@dataclass(frozen=True, slots=True)
class ServerStatusMetrics:
    """Structured server status metrics with optional subsystem payloads."""

    messages_processed: int
    rpc_responses_skipped: int
    server_messages: int
    other_messages: int
    cache_metrics: CacheStatusMetrics | None = None
    queue_metrics: QueueStatusMetrics | None = None
    route_metrics: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if payload.get("cache_metrics") is None:
            payload.pop("cache_metrics", None)
        if payload.get("queue_metrics") is None:
            payload.pop("queue_metrics", None)
        if payload.get("route_metrics") is None:
            payload.pop("route_metrics", None)
        return payload


type RPCServerMessage = RPCServerGoodbye | RPCServerStatus


class FabricGossipEnvelope(BaseModel):
    """Envelope for federation gossip messages transported over MPREG connections."""

    role: Literal["fabric-gossip"] = "fabric-gossip"
    payload: dict[str, Any] = Field(description="Serialized federation gossip message")


class FabricMessageEnvelope(BaseModel):
    """Envelope for unified fabric messages transported over MPREG connections."""

    role: Literal["fabric-message"] = "fabric-message"
    payload: dict[str, Any] = Field(description="Serialized unified fabric message")


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
    intermediate_results: tuple[RPCIntermediateResult, ...] = Field(
        default_factory=tuple,
        description="Intermediate results from each execution level",
    )
    execution_summary: RPCExecutionSummary | None = Field(
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
    PubSubPublish | PubSubSubscribe | PubSubUnsubscribe | PubSubNotification | PubSubAck
)


type RPCMessage = RPCRequest | RPCServerRequest | RPCResponse | PubSubMessage_Union


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

    def __post_init__(self) -> None:
        details = self.details
        if details is None and self.command_name:
            details = f"Command not found: {self.command_name}"
        self.rpc_error = RPCError(
            code=self.code,
            message=self.message,
            details=details,
        )
        if details:
            self.args = (details,)
        else:
            self.args = (self.message,)
