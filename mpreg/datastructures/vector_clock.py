"""
Centralized VectorClock implementation for MPREG.

This module provides a unified, immutable VectorClock implementation that consolidates
all the scattered implementations across the codebase. It uses proper encapsulation
with dataclasses instead of raw dictionaries.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field

from hypothesis import strategies as st

from .type_aliases import (
    JsonDict,
    VectorClockNodeId,
    VectorClockTimestamp,
)


@dataclass(frozen=True, slots=True)
class ClockEntry:
    """Single entry in a vector clock."""

    node_id: VectorClockNodeId
    timestamp: VectorClockTimestamp

    def __post_init__(self) -> None:
        if self.timestamp < 0:
            raise ValueError(f"Timestamp must be non-negative, got {self.timestamp}")
        if not self.node_id:
            raise ValueError("Node ID cannot be empty")


@dataclass(frozen=True, slots=True)
class VectorClock:
    """
    Immutable vector clock for tracking causal dependencies in distributed systems.

    A vector clock is a logical clock that captures causal relationships between
    events in a distributed system. Each node maintains its own logical timestamp,
    and the vector clock ensures proper ordering of events across nodes.

    This implementation consolidates all VectorClock usage across MPREG:
    - Replaces type alias: `type VectorClock = dict[ClusterNodeId, int]`
    - Replaces mutable class from fabric gossip
    - Replaces immutable class from location_consistency.py

    Features:
    - Immutable for thread safety and correctness
    - Proper encapsulation using dataclasses
    - Comprehensive comparison operations
    - Efficient copy-on-write semantics
    - Property-based testing support
    """

    _entries: frozenset[ClockEntry] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        # Validate no duplicate node IDs
        node_ids = [entry.node_id for entry in self._entries]
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("Duplicate node IDs in vector clock")

    @classmethod
    def empty(cls) -> VectorClock:
        """Create an empty vector clock."""
        return cls()

    @classmethod
    def from_dict(
        cls, clock_dict: Mapping[VectorClockNodeId, VectorClockTimestamp]
    ) -> VectorClock:
        """Create vector clock from dictionary (for migration from old code)."""
        entries = frozenset(
            ClockEntry(node_id=node_id, timestamp=timestamp)
            for node_id, timestamp in clock_dict.items()
        )
        return cls(_entries=entries)

    @classmethod
    def single_entry(
        cls, node_id: VectorClockNodeId, timestamp: VectorClockTimestamp = 1
    ) -> VectorClock:
        """Create vector clock with single entry."""
        return cls(
            _entries=frozenset([ClockEntry(node_id=node_id, timestamp=timestamp)])
        )

    def to_dict(self) -> JsonDict:
        """Convert to dictionary (for compatibility with old code)."""
        return {entry.node_id: entry.timestamp for entry in self._entries}

    def get_timestamp(self, node_id: VectorClockNodeId) -> VectorClockTimestamp:
        """Get timestamp for a specific node, returns 0 if not present."""
        for entry in self._entries:
            if entry.node_id == node_id:
                return entry.timestamp
        return 0

    def increment(self, node_id: VectorClockNodeId) -> VectorClock:
        """Increment timestamp for a specific node, returning new vector clock."""
        if not node_id:
            raise ValueError("Node ID cannot be empty")

        new_entries = set(self._entries)

        # Remove existing entry for this node if present
        existing_entry = None
        for entry in new_entries:
            if entry.node_id == node_id:
                existing_entry = entry
                break

        if existing_entry:
            new_entries.remove(existing_entry)
            new_timestamp = existing_entry.timestamp + 1
        else:
            new_timestamp = 1

        new_entries.add(ClockEntry(node_id=node_id, timestamp=new_timestamp))
        return VectorClock(_entries=frozenset(new_entries))

    def update(self, other: VectorClock) -> VectorClock:
        """Update with another vector clock, taking maximum timestamps."""
        if not isinstance(other, VectorClock):
            raise TypeError(f"Can only update with VectorClock, got {type(other)}")

        # Get all unique node IDs
        all_node_ids = {entry.node_id for entry in self._entries} | {
            entry.node_id for entry in other._entries
        }

        new_entries = set()
        for node_id in all_node_ids:
            self_timestamp = self.get_timestamp(node_id)
            other_timestamp = other.get_timestamp(node_id)
            max_timestamp = max(self_timestamp, other_timestamp)

            # Include all timestamps (including 0)
            new_entries.add(ClockEntry(node_id=node_id, timestamp=max_timestamp))

        return VectorClock(_entries=frozenset(new_entries))

    def merge(self, other: VectorClock) -> VectorClock:
        """Alias for update() for backward compatibility."""
        return self.update(other)

    def happens_before(self, other: VectorClock) -> bool:
        """
        Check if this vector clock happens before another.

        Clock A happens before clock B if:
        - For all nodes, A[node] <= B[node]
        - For at least one node, A[node] < B[node]
        """
        if not isinstance(other, VectorClock):
            raise TypeError(f"Can only compare with VectorClock, got {type(other)}")

        # Empty clock happens before non-empty clock
        if not self._entries and other._entries:
            return True
        # Non-empty clock does not happen before empty clock
        if self._entries and not other._entries:
            return False
        # Both empty - neither happens before the other
        if not self._entries and not other._entries:
            return False

        all_node_ids = {entry.node_id for entry in self._entries} | {
            entry.node_id for entry in other._entries
        }

        all_leq = True
        at_least_one_less = False

        for node_id in all_node_ids:
            self_timestamp = self.get_timestamp(node_id)
            other_timestamp = other.get_timestamp(node_id)

            if self_timestamp > other_timestamp:
                all_leq = False
                break
            elif self_timestamp < other_timestamp:
                at_least_one_less = True

        return all_leq and at_least_one_less

    def happens_after(self, other: VectorClock) -> bool:
        """Check if this vector clock happens after another."""
        return other.happens_before(self)

    def concurrent_with(self, other: VectorClock) -> bool:
        """Check if this vector clock is concurrent with another."""
        # If clocks are equal, they are not concurrent
        if self.compare(other) == "equal":
            return False
        return not (self.happens_before(other) or other.happens_before(self))

    def compare(self, other: VectorClock) -> str:
        """
        Compare with another vector clock.

        Returns:
            'equal': Clocks are equal
            'before': This clock is before the other
            'after': This clock is after the other
            'concurrent': Clocks are concurrent
        """
        if not isinstance(other, VectorClock):
            raise TypeError(f"Can only compare with VectorClock, got {type(other)}")

        if self == other:
            return "equal"
        elif self.happens_before(other):
            return "before"
        elif self.happens_after(other):
            return "after"
        else:
            return "concurrent"

    def is_empty(self) -> bool:
        """Check if this vector clock is empty."""
        return len(self._entries) == 0

    def node_count(self) -> int:
        """Get number of nodes in this vector clock."""
        return len(self._entries)

    def node_ids(self) -> frozenset[VectorClockNodeId]:
        """Get all node IDs in this vector clock."""
        return frozenset(entry.node_id for entry in self._entries)

    def max_timestamp(self) -> VectorClockTimestamp:
        """Get the maximum timestamp across all nodes."""
        if not self._entries:
            return 0
        return max(entry.timestamp for entry in self._entries)

    def total_events(self) -> VectorClockTimestamp:
        """Get total number of events (sum of all timestamps)."""
        return sum(entry.timestamp for entry in self._entries)

    def copy(self) -> VectorClock:
        """Create a copy of this vector clock (returns self since immutable)."""
        return self

    def __iter__(self) -> Iterator[ClockEntry]:
        """Iterate over clock entries."""
        return iter(self._entries)

    def __len__(self) -> int:
        """Get number of entries in the vector clock."""
        return len(self._entries)

    def __contains__(self, node_id: VectorClockNodeId) -> bool:
        """Check if a node ID is present in this vector clock."""
        return any(entry.node_id == node_id for entry in self._entries)

    def __getitem__(self, node_id: VectorClockNodeId) -> VectorClockTimestamp:
        """Get timestamp for a node ID, raises KeyError if not found."""
        for entry in self._entries:
            if entry.node_id == node_id:
                return entry.timestamp
        raise KeyError(f"Node ID '{node_id}' not found in vector clock")

    def __repr__(self) -> str:
        """String representation of vector clock."""
        if not self._entries:
            return "VectorClock()"

        entries_str = ", ".join(
            f"{entry.node_id}:{entry.timestamp}"
            for entry in sorted(self._entries, key=lambda e: e.node_id)
        )
        return f"VectorClock({{{entries_str}}})"


# Hypothesis strategies for property-based testing
def clock_entry_strategy() -> st.SearchStrategy[ClockEntry]:
    """Generate valid ClockEntry instances for testing."""
    return st.builds(
        ClockEntry,
        node_id=st.text(
            min_size=1,
            max_size=20,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_"
            ),
        ),
        timestamp=st.integers(min_value=0, max_value=1000),
    )


def vector_clock_strategy(
    max_entries: int = 10, node_ids: list[str] | None = None
) -> st.SearchStrategy[VectorClock]:
    """Generate valid VectorClock instances for testing."""
    if node_ids is not None:
        # Use specific node IDs - sample unique subset
        unique_node_ids = st.lists(
            st.sampled_from(node_ids),
            max_size=min(len(node_ids), max_entries),
            unique=True,
        )

        @st.composite
        def build_from_node_ids(draw):
            selected_nodes = draw(unique_node_ids)
            entries = []
            for node_id in selected_nodes:
                timestamp = draw(st.integers(min_value=0, max_value=1000))
                entries.append(ClockEntry(node_id=node_id, timestamp=timestamp))
            return VectorClock(_entries=frozenset(entries))

        return build_from_node_ids()
    else:
        # Generate random unique node IDs
        @st.composite
        def build_with_unique_nodes(draw):
            num_entries = draw(st.integers(min_value=0, max_value=max_entries))
            node_ids = draw(
                st.lists(
                    st.text(
                        min_size=1,
                        max_size=20,
                        alphabet=st.characters(
                            whitelist_categories=["Lu", "Ll", "Nd"],
                            whitelist_characters="-_",
                        ),
                    ),
                    min_size=num_entries,
                    max_size=num_entries,
                    unique=True,
                )
            )

            entries = []
            for node_id in node_ids:
                timestamp = draw(st.integers(min_value=0, max_value=1000))
                entries.append(ClockEntry(node_id=node_id, timestamp=timestamp))

            return VectorClock(_entries=frozenset(entries))

        return build_with_unique_nodes()


def ordered_vector_clocks_strategy() -> st.SearchStrategy[
    tuple[VectorClock, VectorClock]
]:
    """Generate pairs of vector clocks where first happens before second."""
    node_ids = ["node1", "node2", "node3"]

    @st.composite
    def generate_ordered_pair(draw):
        # Generate first clock - ensure unique node IDs
        num_entries = draw(st.integers(min_value=0, max_value=3))
        selected_nodes = draw(
            st.lists(
                st.sampled_from(node_ids),
                min_size=num_entries,
                max_size=num_entries,
                unique=True,
            )
        )

        first_entries = set()
        for node_id in selected_nodes:
            timestamp = draw(st.integers(min_value=0, max_value=50))
            first_entries.add(ClockEntry(node_id=node_id, timestamp=timestamp))

        first_clock = VectorClock(_entries=frozenset(first_entries))

        # Generate second clock that happens after first
        second_entries_dict = {}
        for entry in first_entries:
            # Keep same or higher timestamp
            new_timestamp = draw(
                st.integers(min_value=entry.timestamp, max_value=entry.timestamp + 50)
            )
            second_entries_dict[entry.node_id] = new_timestamp

        # Ensure at least one timestamp is strictly greater
        if first_entries:
            entry_to_increment = draw(st.sampled_from(list(first_entries)))
            second_entries_dict[entry_to_increment.node_id] = (
                entry_to_increment.timestamp + 1
            )

        # Convert back to ClockEntry set
        second_entries = {
            ClockEntry(node_id=node_id, timestamp=timestamp)
            for node_id, timestamp in second_entries_dict.items()
        }

        second_clock = VectorClock(_entries=frozenset(second_entries))
        return first_clock, second_clock

    return generate_ordered_pair()
