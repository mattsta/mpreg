# Blockchain Datastructures Architecture

## Overview

This document outlines the design of blockchain datastructures for MPREG's federated system, combining our Vector Clock and Merkle Tree implementations with consensus mechanisms to create a distributed ledger suitable for federation coordination.

## Core Design Principles

1. **Immutable Datastructures**: All blockchain components are immutable and thread-safe
2. **Cryptographic Integrity**: SHA-256 hashing throughout, with Merkle tree verification
3. **Causal Consistency**: Vector clocks for distributed consensus and ordering
4. **Property-Based Testing**: Comprehensive hypothesis testing for all components
5. **Federation-First**: Designed specifically for MPREG's federated architecture

## Architecture Components

### Core Datastructures

```
Transaction -> Block -> Blockchain
     |           |         |
     |           |    +----+----+
     |           |    |         |
     |       MerkleTree   VectorClock
     |           |         |
Digital     Cryptographic  Causal
Signature   Verification   Ordering
```

### Component Hierarchy

1. **Transaction**: Individual operations with cryptographic signatures
2. **Block**: Collection of transactions with Merkle root and vector clock
3. **Blockchain**: Ordered chain of blocks with consensus mechanisms
4. **Consensus**: Proof-of-stake/authority mechanisms for federation nodes

## Detailed Component Design

### Transaction Datastructure

```python
@dataclass(frozen=True, slots=True)
class Transaction:
    """
    Immutable transaction with cryptographic signature.

    Represents a single operation in the federated system.
    """

    transaction_id: TransactionId
    sender: NodeId
    receiver: NodeId
    operation_type: OperationType
    payload: TransactionPayload
    timestamp: Timestamp
    signature: DigitalSignature
    nonce: int = 0

    def verify_signature(self, public_key: PublicKey) -> bool
    def get_hash(self) -> TransactionHash
    def is_valid(self) -> bool
    def to_bytes(self) -> bytes
```

**Key Features:**

- Cryptographic signatures for authenticity
- Nonce for replay attack prevention
- Flexible payload for different operation types
- Self-validation methods

### Block Datastructure

```python
@dataclass(frozen=True, slots=True)
class Block:
    """
    Immutable block containing transactions with Merkle proof and vector clock.

    Combines MPREG's distributed time (Vector Clock) with cryptographic
    verification (Merkle Tree) for federated consensus.
    """

    block_id: BlockId
    previous_hash: BlockHash
    merkle_root: MerkleHash
    vector_clock: VectorClock
    transactions: tuple[Transaction, ...]
    miner: NodeId
    difficulty: int
    nonce: int
    timestamp: Timestamp

    # Derived from transactions
    _transactions_tree: MerkleTree = field(init=False)

    def get_block_hash(self) -> BlockHash
    def verify_transactions(self) -> bool
    def generate_transaction_proof(self, tx_index: int) -> MerkleProof
    def verify_proof_of_work(self) -> bool
    def is_valid_successor(self, previous_block: Block) -> bool
```

**Key Features:**

- Merkle tree of transactions for efficient verification
- Vector clock for causal ordering across federation
- Proof-of-work for consensus (adaptable to proof-of-stake)
- Transaction inclusion proofs

### Blockchain Datastructure

```python
@dataclass(frozen=True, slots=True)
class Blockchain:
    """
    Immutable blockchain with consensus mechanisms.

    Maintains ordered chain of blocks with validation and
    federation-specific consensus rules.
    """

    chain_id: ChainId
    genesis_block: Block
    blocks: tuple[Block, ...]
    difficulty_target: int
    consensus_config: ConsensusConfig

    # Derived state
    _current_difficulty: int = field(init=False)
    _total_work: int = field(init=False)

    def add_block(self, block: Block) -> Blockchain
    def validate_chain(self) -> bool
    def get_latest_block(self) -> Block
    def find_transaction(self, tx_id: TransactionId) -> tuple[Block, Transaction] | None
    def get_balance(self, node_id: NodeId) -> int
    def fork_at_block(self, block_hash: BlockHash) -> Blockchain
    def resolve_fork(self, competing_chain: Blockchain) -> Blockchain
```

**Key Features:**

- Immutable chain operations
- Fork resolution using total work
- Transaction lookups across blocks
- Balance calculation for federation nodes

## Consensus Mechanisms

### Proof of Authority (Federation Consensus)

```python
@dataclass(frozen=True, slots=True)
class ProofOfAuthority:
    """
    Federation-specific consensus where authorized nodes take turns mining.

    Uses vector clocks to determine mining order and prevent conflicts.
    """

    authorized_miners: frozenset[NodeId]
    round_duration: int  # seconds
    vector_clock: VectorClock

    def can_mine_block(self, miner: NodeId, current_time: float) -> bool
    def get_next_miner(self) -> NodeId
    def update_consensus_state(self, new_block: Block) -> ProofOfAuthority
```

### Proof of Stake (Future Extension)

```python
@dataclass(frozen=True, slots=True)
class ProofOfStake:
    """
    Stake-based consensus for larger federation networks.
    """

    stakes: dict[NodeId, int]
    minimum_stake: int
    slashing_conditions: SlashingConfig

    def select_validator(self, randomness: bytes) -> NodeId
    def validate_stake_proof(self, proof: StakeProof) -> bool
```

## Federation Integration Points

### 1. Blockchain + Message Queue

```python
class BlockchainMessageQueue:
    """Message queue with blockchain ordering guarantees."""

    def __init__(self, blockchain: Blockchain):
        self.blockchain = blockchain
        self.pending_transactions: list[Transaction] = []

    def enqueue_message(self, message: BaseMessage, sender: NodeId) -> Transaction:
        """Convert message to blockchain transaction."""

    def process_block(self, block: Block) -> list[BaseMessage]:
        """Extract messages from block transactions."""

    def get_ordered_messages(self) -> list[BaseMessage]:
        """Get messages in blockchain order."""
```

### 2. Blockchain + Federation Routing

```python
class BlockchainFederationRouter:
    """Federation routing with blockchain-verified node registry."""

    def register_node(self, node_info: NodeInfo, signature: DigitalSignature) -> Transaction:
        """Register federation node on blockchain."""

    def route_message(self, message: BaseMessage, target: NodeId) -> bool:
        """Route using blockchain-verified node information."""

    def get_verified_nodes(self) -> dict[NodeId, NodeInfo]:
        """Get blockchain-verified node registry."""
```

### 3. Blockchain + RPC System

```python
class BlockchainRPC:
    """RPC system with blockchain transaction verification."""

    def call_remote_procedure(
        self,
        target: NodeId,
        procedure: str,
        args: dict,
        require_consensus: bool = False
    ) -> RPCResult:
        """Execute RPC with optional blockchain consensus."""

    def submit_consensus_call(self, rpc_call: RPCCall) -> Transaction:
        """Submit RPC call requiring federation consensus."""
```

## Type Aliases and Semantic Types

```python
# Core blockchain types
type TransactionId = str
type BlockId = str
type BlockHash = str
type MerkleHash = str
type NodeId = str
type Timestamp = float
type ChainId = str

# Cryptographic types
type DigitalSignature = bytes
type PublicKey = bytes
type PrivateKey = bytes
type TransactionHash = str

# Consensus types
type Difficulty = int
type Nonce = int
type StakeAmount = int

# Payload types
type TransactionPayload = bytes
type OperationType = str

# Configuration types
@dataclass(frozen=True, slots=True)
class ConsensusConfig:
    consensus_type: str  # "proof_of_authority", "proof_of_stake"
    block_time_target: int  # seconds
    difficulty_adjustment_interval: int  # blocks
    max_transactions_per_block: int

@dataclass(frozen=True, slots=True)
class SlashingConfig:
    double_sign_penalty: float
    offline_penalty: float
    byzantine_penalty: float
```

## Implementation Strategy

### Phase 1: Core Datastructures

1. Implement `Transaction` with signature verification
2. Implement `Block` with Merkle tree integration
3. Implement `Blockchain` with basic validation
4. Write comprehensive property-based tests

### Phase 2: Consensus Mechanisms

1. Implement `ProofOfAuthority` for federation consensus
2. Add vector clock integration for causal ordering
3. Implement fork resolution algorithms
4. Test consensus under various scenarios

### Phase 3: Federation Integration

1. Integrate with existing message queue system
2. Add blockchain routing verification
3. Implement consensus-based RPC calls
4. Create federation node registry on blockchain

### Phase 4: Advanced Features

1. Add `ProofOfStake` consensus option
2. Implement cross-chain communication
3. Add privacy features (zero-knowledge proofs)
4. Performance optimizations

## Testing Strategy

### Property-Based Tests

```python
# Example property tests
@given(transaction_strategy())
def test_transaction_hash_consistency(tx):
    """Transactions with same data have same hash."""
    assert tx.get_hash() == tx.get_hash()

@given(block_strategy())
def test_block_merkle_verification(block):
    """Block Merkle root matches transaction tree."""
    expected_root = MerkleTree.from_leaves([tx.to_bytes() for tx in block.transactions]).root_hash()
    assert block.merkle_root == expected_root

@given(blockchain_strategy())
def test_blockchain_causality(chain):
    """Blockchain respects vector clock causality."""
    for i in range(1, len(chain.blocks)):
        current = chain.blocks[i]
        previous = chain.blocks[i-1]
        assert previous.vector_clock.happens_before(current.vector_clock)
```

### Integration Tests

```python
def test_federation_consensus_scenario():
    """Test complete federation consensus scenario."""
    # Setup federation nodes
    # Create transactions
    # Mine blocks with proof of authority
    # Verify consensus across nodes
    # Test fork resolution
```

## Performance Considerations

### Space Complexity

- **Transaction**: O(1) per transaction
- **Block**: O(n) where n = transactions per block
- **Blockchain**: O(m) where m = total blocks
- **Merkle Proofs**: O(log n) per proof

### Time Complexity

- **Block Validation**: O(n log n) where n = transactions
- **Chain Validation**: O(m × n log n) where m = blocks, n = transactions
- **Transaction Lookup**: O(log m) with indexing
- **Fork Resolution**: O(k) where k = fork length

### Optimization Strategies

1. **Block Indexing**: Hash tables for fast transaction/block lookup
2. **Merkle Caching**: Cache Merkle tree computations
3. **Parallel Validation**: Validate transactions in parallel
4. **Sparse Chains**: Only store essential blocks for light clients

## Security Considerations

### Cryptographic Security

- **Hash Function**: SHA-256 (256-bit security)
- **Digital Signatures**: Ed25519 or secp256k1
- **Merkle Proofs**: Tamper-evident verification
- **Nonce Protection**: Prevent replay attacks

### Consensus Security

- **51% Attack**: Mitigated by proof-of-authority in federation
- **Nothing-at-Stake**: Not applicable with authorized miners
- **Vector Clock Conflicts**: Resolved by federation consensus
- **Fork Attacks**: Longest valid chain wins

### Federation Security

- **Node Authentication**: Blockchain-verified node registry
- **Message Integrity**: All federation messages on blockchain
- **Byzantine Tolerance**: Configurable fault tolerance
- **Network Partitions**: Vector clock merge on reconnection

## Migration and Compatibility

### Backward Compatibility

- Existing MPREG systems continue working
- Blockchain features are opt-in
- Gradual migration path for federation nodes
- Legacy message formats supported

### Upgrade Strategy

1. Deploy blockchain datastructures module
2. Add blockchain support to new federation nodes
3. Migrate critical operations to blockchain consensus
4. Phase out legacy consensus mechanisms

This architecture provides a solid foundation for building blockchain functionality into MPREG's federated system while maintaining compatibility and leveraging our existing Vector Clock and Merkle Tree implementations.
