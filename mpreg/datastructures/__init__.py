"""
MPREG Centralized Datastructures Module.

This module provides centralized, well-tested implementations of core datastructures
used throughout MPREG to eliminate duplication and ensure consistency.

Key datastructures:
- VectorClock: Causal ordering and distributed time tracking
- Graph algorithms: Formally verified Dijkstra's and A* pathfinding with property-based testing
- CacheKey: Unified cache key implementation
- Message types: Standardized message formats
- Blockchain: Distributed ledger with consensus mechanisms
- DAO: Decentralized governance with voting and execution
"""

from __future__ import annotations

from .block import Block
from .blockchain import Blockchain
from .blockchain_crypto import CryptoKeyPair, TransactionSigner, generate_keypair
from .blockchain_store import BlockchainStore
from .blockchain_types import (
    ConsensusConfig,
    ConsensusType,
    CryptoConfig,
    OperationType,
)
from .cache_structures import CacheEntry, CacheKey, CacheStatistics
from .dao import DecentralizedAutonomousOrganization
from .dao_types import (
    DaoConfig,
    DaoExecution,
    DaoMember,
    DaoProposal,
    DaoType,
    DaoVote,
    DaoVotingResult,
    ExecutionStatus,
    MembershipType,
    ProposalStatus,
    ProposalType,
    VoteType,
)
from .graph_algorithms import (
    AStarAlgorithm,
    AStarConfig,
    Coordinate,
    CoordinateMap,
    DijkstraAlgorithm,
    DijkstraConfig,
    EdgeWeight,
    Graph,
    GraphEdge,
    GraphNode,
    HeuristicFunction,
    NodeId,
    PathCost,
    PathfindingResult,
    PathList,
    euclidean_distance_heuristic,
    manhattan_distance_heuristic,
    zero_heuristic,
)
from .merkle_tree import MerkleNode, MerkleProof, MerkleTree
from .message_structures import (
    BaseMessage,
    MessageHeader,
    MessageHeaders,
    MessageId,
    MessagePriority,
    QueuedMessage,
)
from .transaction import Transaction, TransactionPool
from .vector_clock import VectorClock

__all__ = [
    "VectorClock",
    "MerkleTree",
    "MerkleNode",
    "MerkleProof",
    "CacheKey",
    "CacheEntry",
    "CacheStatistics",
    "MessageId",
    "MessagePriority",
    "MessageHeader",
    "MessageHeaders",
    "BaseMessage",
    "QueuedMessage",
    "Transaction",
    "TransactionPool",
    "Block",
    "Blockchain",
    "BlockchainStore",
    "CryptoConfig",
    "CryptoKeyPair",
    "TransactionSigner",
    "generate_keypair",
    "OperationType",
    "ConsensusType",
    "ConsensusConfig",
    "DecentralizedAutonomousOrganization",
    "DaoType",
    "MembershipType",
    "ProposalType",
    "ProposalStatus",
    "VoteType",
    "ExecutionStatus",
    "DaoConfig",
    "DaoMember",
    "DaoProposal",
    "DaoVote",
    "DaoVotingResult",
    "DaoExecution",
    # Graph algorithms
    "DijkstraAlgorithm",
    "DijkstraConfig",
    "AStarAlgorithm",
    "AStarConfig",
    "PathfindingResult",
    "Graph",
    "GraphNode",
    "GraphEdge",
    "NodeId",
    "EdgeWeight",
    "PathCost",
    "PathList",
    "HeuristicFunction",
    "Coordinate",
    "CoordinateMap",
    "euclidean_distance_heuristic",
    "manhattan_distance_heuristic",
    "zero_heuristic",
]
