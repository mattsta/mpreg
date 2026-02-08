"""
Semantic type aliases for MPREG datastructures.

This module provides meaningful type aliases that make the codebase more
self-documenting and type-safe by replacing raw types like str, int, float
with semantic aliases.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any

# Time and timestamp types
type Timestamp = float
type DurationSeconds = float
type DurationMilliseconds = float
type TimestampNanoseconds = int

# ID and identifier types
type NodeId = str
type ClusterId = str
type TenantId = str
type ViewerId = str
type MessageIdString = str
type SubscriptionIdString = str
type SessionId = str
type RequestId = str
type ConnectionId = str
type CorrelationId = str

# Cache-related types
type CacheNamespaceName = str
type CacheKeyString = str
type CacheSubkeyString = str
type CacheVersionNumber = int
type CacheSizeBytes = int
type CacheAccessCount = int
type CacheTagName = str

# Message and queue types
type QueueName = str
type SubscriberId = str
type TopicName = str
type MessagePayload = Any
type MessageHeaders = Mapping[str, str]
type DeliveryTag = str
type RetryCount = int

# Pattern and subscription types
type PatternString = str
type SubscriptionId = str
type MatchCount = int
type CacheSize = int

# Network and federation types
type HostAddress = str
type PortNumber = int
type PortAssignmentCallback = Callable[[PortNumber], None | Awaitable[None]]
type UrlString = str
type TransportProtocolName = str
type ConnectionTypeName = str
type NodeURL = str  # WebSocket URL for a node (e.g., ws://host:<port>)
type ClusterID = str  # Cluster identifier for federation
type FunctionName = str  # Name of an RPC function
type FunctionId = str  # Unique identifier for a function capability
type FunctionVersion = str  # Semantic version string for a function
type VersionConstraintSpec = str  # Constraint spec for function versions
type ServiceName = str  # Name of a service endpoint
type ResourceName = str  # Name of a resource/location
type NamespaceName = str  # Namespace/prefix for discovery policies
type EndpointScope = str  # Discovery scope for endpoints (local/zone/region/global)
type ClusterWeight = float
type NetworkLatencyMs = float

# Statistics and metrics types
type HitCount = int
type MissCount = int
type EvictionCount = int
type EntryCount = int
type SuccessRate = float
type LatencyMetric = float

# Vector clock types
type VectorClockTimestamp = int
type VectorClockNodeId = str

# Priority and status types
type PriorityLevel = str
type StatusCode = str
type ErrorMessage = str

# Size and capacity types
type ByteSize = int
type MaxSize = int
type CurrentSize = int
type Percentage = float

# Configuration types
type ConfigValue = Any
type SettingName = str
type FeatureFlag = bool

# Merkle tree types
type MerkleHash = str
type MerkleLeafData = bytes
type MerkleProofPath = list[tuple[MerkleHash, bool]]  # (hash, is_right_sibling)
type MerkleTreeDepth = int
type MerkleLeafIndex = int

# Serialization types
type JsonScalar = str | int | float | bool | None
type JsonArray = list["JsonValue"] | tuple["JsonValue", ...]
type JsonValue = JsonScalar | JsonArray | dict[str, "JsonValue"]
type JsonDict = dict[str, JsonValue]
type SerializedData = str | bytes
type CompressionLevel = int

# RPC metadata types
type RpcName = str
type RpcNamespace = str
type RpcParamName = str
type RpcDocString = str
type RpcDocSummary = str
type RpcTypeName = str
type RpcSpecDigest = str
type RpcSpecVersion = str
type RpcTag = str
type RpcExampleName = str

# Blockchain and DAO types
type BlockId = str
type TransactionId = str
type ChainId = str
type DaoId = str
type DaoMemberId = str
type ProposalId = str
type PolicyId = str
type VotingPower = int
type TokenBalance = int

# Federation and hub types
type HubId = str
type RegionName = str
type AreaId = str
type FederationRouteId = str
type GeographicDistanceKm = float
type HubTierName = str
type CapacityUnits = int
type BandwidthMbps = int
type ProcessingLatencyMs = float
type ReliabilityScore = float
type CostPerMegabyte = int
type SlaComplianceRate = float

# Message queue types
type MessageQueueId = str
type MessagePriorityLevel = str
type ProcessingFeeAmount = int
type MessageRetryCount = int
type MaxRetryCount = int
type QueueDepth = int
type MessagesPerSecond = float
type MessageSizeBytes = int

# Route and path types
type RouteId = str
type HopCount = int
type PathLatencyMs = float
type RouteCostScore = float
type RouteReliability = float
type RouteKeyId = str

# Governance policy types
type PolicyParameterName = str
type PolicyParameterValue = str | int | float | bool
type MetadataKey = str
type MetadataValue = str | int | float | bool
