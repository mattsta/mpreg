# Vector Clock Implementation Guide

## Overview

The `VectorClock` is a fundamental distributed systems datastructure that provides causal ordering of events across multiple nodes without requiring synchronized physical clocks. Our implementation is immutable, thread-safe, and mathematically correct.

## Key Features

- **Causal Ordering**: Determines happens-before relationships between distributed events
- **Immutable Design**: All operations return new instances (thread-safe)
- **Mathematical Properties**: Satisfies associativity, commutativity, and transitivity
- **Semantic Types**: Uses proper type aliases instead of raw dictionaries
- **Comprehensive Testing**: 300+ property-based tests verify correctness

## Core Concepts

### What is a Vector Clock?

A vector clock is a logical clock that captures the causal ordering of events in a distributed system. Each node maintains a vector of timestamps, one for each node in the system.

```python
from mpreg.datastructures import VectorClock

# Empty vector clock
clock = VectorClock()

# Clock with entries for multiple nodes
clock = VectorClock.from_dict({
    "node-1": 5,
    "node-2": 3,
    "node-3": 8
})
```

### Causal Relationships

Vector clocks can determine three types of relationships between events:

1. **Happens-Before**: Event A causally precedes event B
2. **Happens-After**: Event A causally follows event B
3. **Concurrent**: Events A and B are causally independent

## API Reference

### Construction

```python
# Create empty vector clock
clock = VectorClock()

# Create from dictionary
clock = VectorClock.from_dict({"alice": 1, "bob": 2})

# Create from node entries
from mpreg.datastructures.vector_clock import ClockEntry
entries = {ClockEntry("alice", 1), ClockEntry("bob", 2)}
clock = VectorClock.from_entries(entries)
```

### Basic Operations

```python
# Increment local timestamp
clock = clock.increment("node-1")  # Returns new clock

# Update with another clock (take maximum timestamps)
other_clock = VectorClock.from_dict({"node-1": 3, "node-2": 1})
merged_clock = clock.update(other_clock)

# Access timestamps
timestamp = clock.get_timestamp("node-1")  # Returns int
timestamp = clock["node-1"]  # Dictionary-style access
```

### Comparison Operations

```python
clock1 = VectorClock.from_dict({"alice": 1, "bob": 2})
clock2 = VectorClock.from_dict({"alice": 2, "bob": 2})

# Check causal relationships
if clock1.happens_before(clock2):
    print("clock1 causally precedes clock2")

if clock1.concurrent_with(clock2):
    print("clock1 and clock2 are concurrent")

# String comparison
relationship = clock1.compare(clock2)  # Returns "before", "after", "equal", or "concurrent"
```

### Utility Methods

```python
# Check if empty
if clock.is_empty():
    print("No events recorded")

# Get all timestamps
timestamps = clock.to_dict()  # Returns dict[str, int]

# Get node IDs
nodes = clock.get_nodes()  # Returns frozenset[str]

# String representation
print(f"Clock state: {clock}")  # VectorClock(alice=1, bob=2)
```

## Usage Examples

### Distributed Event Ordering

```python
from mpreg.datastructures import VectorClock

# Three nodes in a distributed system
alice_clock = VectorClock()
bob_clock = VectorClock()
charlie_clock = VectorClock()

# Alice performs an action
alice_clock = alice_clock.increment("alice")
print(f"Alice: {alice_clock}")  # VectorClock(alice=1)

# Alice sends message to Bob (Bob updates with Alice's clock)
bob_clock = bob_clock.update(alice_clock).increment("bob")
print(f"Bob: {bob_clock}")  # VectorClock(alice=1, bob=1)

# Charlie acts independently
charlie_clock = charlie_clock.increment("charlie")
print(f"Charlie: {charlie_clock}")  # VectorClock(charlie=1)

# Check relationships
assert alice_clock.happens_before(bob_clock)  # Alice's event caused Bob's
assert alice_clock.concurrent_with(charlie_clock)  # Independent events
```

### Message Passing Protocol

```python
class DistributedMessage:
    def __init__(self, sender: str, content: str, vector_clock: VectorClock):
        self.sender = sender
        self.content = content
        self.vector_clock = vector_clock

def send_message(sender_clock: VectorClock, sender: str, content: str) -> tuple[VectorClock, DistributedMessage]:
    # Increment sender's clock before sending
    new_clock = sender_clock.increment(sender)
    message = DistributedMessage(sender, content, new_clock)
    return new_clock, message

def receive_message(receiver_clock: VectorClock, receiver: str, message: DistributedMessage) -> VectorClock:
    # Update with message clock, then increment receiver
    return receiver_clock.update(message.vector_clock).increment(receiver)

# Example usage
alice_clock = VectorClock()
bob_clock = VectorClock()

# Alice sends message
alice_clock, msg = send_message(alice_clock, "alice", "Hello Bob!")
print(f"Alice after send: {alice_clock}")  # VectorClock(alice=1)

# Bob receives message
bob_clock = receive_message(bob_clock, "bob", msg)
print(f"Bob after receive: {bob_clock}")  # VectorClock(alice=1, bob=1)
```

### Federation Consensus

```python
def merge_federation_state(local_clock: VectorClock, remote_clocks: list[VectorClock]) -> VectorClock:
    """Merge local state with multiple remote federation nodes."""
    result = local_clock
    for remote_clock in remote_clocks:
        result = result.update(remote_clock)
    return result

# Federation nodes sync their vector clocks
node1_clock = VectorClock.from_dict({"node1": 5, "node2": 3})
node2_clock = VectorClock.from_dict({"node1": 4, "node2": 7, "node3": 2})
node3_clock = VectorClock.from_dict({"node1": 6, "node3": 4})

# Merge all states
consensus_clock = merge_federation_state(node1_clock, [node2_clock, node3_clock])
print(f"Consensus: {consensus_clock}")  # VectorClock(node1=6, node2=7, node3=4)
```

## Mathematical Properties

Our vector clock implementation satisfies key mathematical properties:

### Associativity

```python
# (A ∪ B) ∪ C = A ∪ (B ∪ C)
result1 = clock_a.update(clock_b).update(clock_c)
result2 = clock_a.update(clock_b.update(clock_c))
assert result1 == result2
```

### Commutativity

```python
# A ∪ B = B ∪ A
result1 = clock_a.update(clock_b)
result2 = clock_b.update(clock_a)
assert result1 == result2
```

### Monotonicity

```python
# Incrementing always increases the clock
old_clock = clock.increment("node")
new_clock = old_clock.increment("node")
assert old_clock.happens_before(new_clock)
```

## Integration with MPREG

### Message Queue Integration

```python
from mpreg.datastructures import VectorClock
from mpreg.core.message_queue import QueuedMessage, MessageId

class CausalMessage:
    """Message with vector clock for causal ordering."""

    def __init__(self, content: str, sender: str, vector_clock: VectorClock):
        self.message_id = MessageId.generate(sender)
        self.content = content
        self.sender = sender
        self.vector_clock = vector_clock
        self.timestamp = vector_clock.get_timestamp(sender)

    def can_deliver_after(self, other: 'CausalMessage') -> bool:
        """Check if this message can be delivered after another."""
        return other.vector_clock.happens_before(self.vector_clock)
```

### Federation Synchronization

```python
class FederationNode:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.vector_clock = VectorClock()

    def perform_local_action(self) -> VectorClock:
        """Perform local action and update vector clock."""
        self.vector_clock = self.vector_clock.increment(self.node_id)
        return self.vector_clock

    def sync_with_peer(self, peer_clock: VectorClock) -> VectorClock:
        """Synchronize state with peer node."""
        self.vector_clock = self.vector_clock.update(peer_clock)
        return self.vector_clock
```

## Performance Characteristics

- **Space Complexity**: O(n) where n is the number of nodes
- **Time Complexity**:
  - Increment: O(n)
  - Update: O(n)
  - Comparison: O(n)
- **Memory Usage**: Minimal due to immutable design with structural sharing

## Best Practices

1. **Always use assignment**: `clock = clock.increment("node")` (immutable design)
2. **Handle missing nodes**: Use `get_timestamp()` with defaults for optional nodes
3. **Batch updates**: Chain operations when possible for efficiency
4. **Consistent node IDs**: Use stable, unique identifiers across the system
5. **Garbage collection**: Remove entries for permanently offline nodes periodically

## Testing

Our implementation includes comprehensive property-based tests:

```python
# Example property test
@given(vector_clock_strategy(), node_id_strategy())
def test_increment_monotonicity(clock, node_id):
    new_clock = clock.increment(node_id)
    assert clock.happens_before(new_clock) or clock == new_clock
```

Run tests with:

```bash
uv run python -m pytest tests/test_datastructures_vector_clock.py -v
```

## Troubleshooting

### Common Issues

1. **Forgetting assignment**: Vector clocks are immutable

   ```python
   # Wrong
   clock.increment("node")  # Original clock unchanged

   # Correct
   clock = clock.increment("node")  # Assign new clock
   ```

2. **Node ID consistency**: Use the same node ID across all operations

   ```python
   # Wrong - inconsistent IDs
   clock = clock.increment("node-1")
   clock = clock.increment("node_1")  # Different ID!

   # Correct - consistent ID
   NODE_ID = "node-1"
   clock = clock.increment(NODE_ID)
   ```

3. **Comparing with wrong types**: Always compare VectorClock instances

   ```python
   # Wrong
   clock.happens_before({"node": 1})  # TypeError

   # Correct
   other = VectorClock.from_dict({"node": 1})
   clock.happens_before(other)  # Works
   ```

## Further Reading

- [Vector Clocks in Distributed Systems](https://en.wikipedia.org/wiki/Vector_clock)
- [Time, Clocks, and the Ordering of Events](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) - Lamport's seminal paper
- [Distributed Systems Concepts](https://web.mit.edu/6.824/www/papers/) - MIT 6.824 papers
