"""
Comprehensive property-based tests for VectorClock datastructure.

This test suite uses the hypothesis library to aggressively test the correctness
of the centralized VectorClock implementation with property-based testing.
"""

import pytest
from hypothesis import assume, example, given
from hypothesis import strategies as st

from mpreg.datastructures.vector_clock import (
    ClockEntry,
    VectorClock,
    clock_entry_strategy,
    ordered_vector_clocks_strategy,
    vector_clock_strategy,
)


class TestClockEntry:
    """Test ClockEntry datastructure."""

    @given(clock_entry_strategy())
    def test_clock_entry_creation(self, entry: ClockEntry):
        """Test that valid clock entries can be created."""
        assert entry.node_id
        assert entry.timestamp >= 0
        assert isinstance(entry.node_id, str)
        assert isinstance(entry.timestamp, int)

    def test_clock_entry_validation(self):
        """Test clock entry validation."""
        # Valid entry
        entry = ClockEntry(node_id="node1", timestamp=5)
        assert entry.node_id == "node1"
        assert entry.timestamp == 5

        # Invalid timestamp
        with pytest.raises(ValueError, match="Timestamp must be non-negative"):
            ClockEntry(node_id="node1", timestamp=-1)

        # Invalid node ID
        with pytest.raises(ValueError, match="Node ID cannot be empty"):
            ClockEntry(node_id="", timestamp=1)

    @given(clock_entry_strategy(), clock_entry_strategy())
    def test_clock_entry_equality(self, entry1: ClockEntry, entry2: ClockEntry):
        """Test clock entry equality semantics."""
        if entry1.node_id == entry2.node_id and entry1.timestamp == entry2.timestamp:
            assert entry1 == entry2
            assert hash(entry1) == hash(entry2)
        else:
            assert entry1 != entry2


class TestVectorClockBasics:
    """Test basic VectorClock functionality."""

    def test_empty_vector_clock(self):
        """Test empty vector clock creation and properties."""
        clock = VectorClock.empty()
        assert clock.is_empty()
        assert clock.node_count() == 0
        assert len(clock) == 0
        assert clock.max_timestamp() == 0
        assert clock.total_events() == 0
        assert list(clock.node_ids()) == []

    def test_single_entry_clock(self):
        """Test vector clock with single entry."""
        clock = VectorClock.single_entry("node1", 5)
        assert not clock.is_empty()
        assert clock.node_count() == 1
        assert len(clock) == 1
        assert clock.max_timestamp() == 5
        assert clock.total_events() == 5
        assert "node1" in clock.node_ids()
        assert clock.get_timestamp("node1") == 5
        assert clock["node1"] == 5
        assert "node1" in clock

    def test_from_dict_conversion(self):
        """Test conversion from/to dictionary."""
        original_dict = {"node1": 3, "node2": 7, "node3": 1}
        clock = VectorClock.from_dict(original_dict)
        converted_dict = clock.to_dict()

        assert converted_dict == original_dict
        assert clock.get_timestamp("node1") == 3
        assert clock.get_timestamp("node2") == 7
        assert clock.get_timestamp("node3") == 1
        assert clock.get_timestamp("nonexistent") == 0

    def test_key_error_on_missing_node(self):
        """Test that accessing missing node with [] raises KeyError."""
        clock = VectorClock.single_entry("node1", 5)
        with pytest.raises(KeyError, match="Node ID 'nonexistent' not found"):
            _ = clock["nonexistent"]

    @given(vector_clock_strategy())
    def test_vector_clock_properties(self, clock: VectorClock):
        """Test basic properties of any vector clock."""
        # Node count matches length
        assert clock.node_count() == len(clock)

        # Empty iff node count is 0
        assert clock.is_empty() == (clock.node_count() == 0)

        # Max timestamp is non-negative
        assert clock.max_timestamp() >= 0

        # Total events is non-negative
        assert clock.total_events() >= 0

        # If empty, max timestamp and total events are 0
        if clock.is_empty():
            assert clock.max_timestamp() == 0
            assert clock.total_events() == 0

        # All node IDs are in the clock
        for node_id in clock.node_ids():
            assert node_id in clock
            assert clock.get_timestamp(node_id) >= 0

    @given(vector_clock_strategy())
    def test_copy_is_identity(self, clock: VectorClock):
        """Test that copy returns the same object (since immutable)."""
        copy = clock.copy()
        assert copy is clock  # Same object for immutable types


class TestVectorClockOperations:
    """Test VectorClock operations."""

    @given(vector_clock_strategy(), st.text(min_size=1, max_size=20))
    def test_increment_operation(self, clock: VectorClock, node_id: str):
        """Test increment operation properties."""
        new_clock = clock.increment(node_id)

        # Original clock unchanged (immutability)
        assert clock.get_timestamp(node_id) == clock.get_timestamp(node_id)

        # New clock has incremented timestamp
        assert new_clock.get_timestamp(node_id) == clock.get_timestamp(node_id) + 1

        # Other nodes unchanged
        for other_node in clock.node_ids():
            if other_node != node_id:
                assert new_clock.get_timestamp(other_node) == clock.get_timestamp(
                    other_node
                )

        # Node count increased by at most 1
        assert new_clock.node_count() <= clock.node_count() + 1

        # If new node, count increased by 1
        if node_id not in clock:
            assert new_clock.node_count() == clock.node_count() + 1
        else:
            assert new_clock.node_count() == clock.node_count()

    def test_increment_empty_string(self):
        """Test that incrementing empty string raises error."""
        clock = VectorClock.empty()
        with pytest.raises(ValueError, match="Node ID cannot be empty"):
            clock.increment("")

    @given(vector_clock_strategy(), vector_clock_strategy())
    def test_update_operation(self, clock1: VectorClock, clock2: VectorClock):
        """Test update operation properties."""
        result = clock1.update(clock2)

        # Result contains all nodes from both clocks
        all_nodes = clock1.node_ids() | clock2.node_ids()
        for node_id in all_nodes:
            expected_timestamp = max(
                clock1.get_timestamp(node_id), clock2.get_timestamp(node_id)
            )
            assert result.get_timestamp(node_id) == expected_timestamp

        # Update is commutative
        result2 = clock2.update(clock1)
        assert result.to_dict() == result2.to_dict()

        # Update is idempotent
        result_again = result.update(clock2)
        assert result.to_dict() == result_again.to_dict()

    @given(vector_clock_strategy())
    def test_update_with_self(self, clock: VectorClock):
        """Test updating clock with itself."""
        result = clock.update(clock)
        assert result.to_dict() == clock.to_dict()

    @given(vector_clock_strategy())
    def test_update_with_empty(self, clock: VectorClock):
        """Test updating with empty clock."""
        empty = VectorClock.empty()
        result1 = clock.update(empty)
        result2 = empty.update(clock)

        assert result1.to_dict() == clock.to_dict()
        assert result2.to_dict() == clock.to_dict()

    def test_update_type_checking(self):
        """Test that update with wrong type raises error."""
        clock = VectorClock.single_entry("node1", 1)
        with pytest.raises(TypeError, match="Can only update with VectorClock"):
            clock.update("not a clock")  # type: ignore

    @given(vector_clock_strategy())
    def test_merge_alias(self, clock: VectorClock):
        """Test that merge is alias for update."""
        other = VectorClock.single_entry("test", 5)
        result1 = clock.update(other)
        result2 = clock.merge(other)
        assert result1.to_dict() == result2.to_dict()


class TestVectorClockComparison:
    """Test VectorClock comparison operations."""

    @given(vector_clock_strategy())
    def test_compare_with_self(self, clock: VectorClock):
        """Test comparing clock with itself."""
        assert clock.compare(clock) == "equal"
        assert not clock.happens_before(clock)
        assert not clock.happens_after(clock)
        assert not clock.concurrent_with(clock)

    def test_compare_empty_clocks(self):
        """Test comparing empty clocks."""
        empty1 = VectorClock.empty()
        empty2 = VectorClock.empty()

        assert empty1.compare(empty2) == "equal"
        assert not empty1.happens_before(empty2)
        assert not empty1.happens_after(empty2)
        assert not empty1.concurrent_with(empty2)

    def test_empty_vs_non_empty(self):
        """Test comparing empty with non-empty clock."""
        empty = VectorClock.empty()
        non_empty = VectorClock.single_entry("node1", 1)

        assert empty.compare(non_empty) == "before"
        assert empty.happens_before(non_empty)
        assert non_empty.happens_after(empty)
        assert not empty.concurrent_with(non_empty)
        assert not non_empty.concurrent_with(empty)

    @given(ordered_vector_clocks_strategy())
    def test_ordered_clocks_comparison(self, clocks: tuple[VectorClock, VectorClock]):
        """Test comparison of ordered clocks."""
        first, second = clocks

        # Skip if clocks are equal
        assume(first.to_dict() != second.to_dict())

        # First should happen before second
        assert first.happens_before(second) or first.concurrent_with(second)
        assert second.happens_after(first) or first.concurrent_with(second)

        # Compare should return appropriate values
        comparison = first.compare(second)
        assert comparison in ("before", "concurrent")

        if comparison == "before":
            assert first.happens_before(second)
            assert second.happens_after(first)
            assert not first.concurrent_with(second)

    def test_specific_ordering_examples(self):
        """Test specific ordering examples."""
        # Clock A = {node1: 1, node2: 2}
        # Clock B = {node1: 2, node2: 3}
        # A happens before B
        clock_a = VectorClock.from_dict({"node1": 1, "node2": 2})
        clock_b = VectorClock.from_dict({"node1": 2, "node2": 3})

        assert clock_a.happens_before(clock_b)
        assert clock_b.happens_after(clock_a)
        assert clock_a.compare(clock_b) == "before"
        assert clock_b.compare(clock_a) == "after"

    def test_concurrent_clocks(self):
        """Test concurrent clocks."""
        # Clock A = {node1: 2, node2: 1}
        # Clock B = {node1: 1, node2: 2}
        # A and B are concurrent
        clock_a = VectorClock.from_dict({"node1": 2, "node2": 1})
        clock_b = VectorClock.from_dict({"node1": 1, "node2": 2})

        assert not clock_a.happens_before(clock_b)
        assert not clock_b.happens_before(clock_a)
        assert clock_a.concurrent_with(clock_b)
        assert clock_b.concurrent_with(clock_a)
        assert clock_a.compare(clock_b) == "concurrent"
        assert clock_b.compare(clock_a) == "concurrent"

    def test_compare_type_checking(self):
        """Test that compare with wrong type raises error."""
        clock = VectorClock.single_entry("node1", 1)
        with pytest.raises(TypeError, match="Can only compare with VectorClock"):
            clock.compare("not a clock")  # type: ignore

        with pytest.raises(TypeError, match="Can only compare with VectorClock"):
            clock.happens_before("not a clock")  # type: ignore


class TestVectorClockProperties:
    """Test mathematical properties of VectorClock operations."""

    @given(vector_clock_strategy(), vector_clock_strategy(), vector_clock_strategy())
    def test_update_associativity(self, a: VectorClock, b: VectorClock, c: VectorClock):
        """Test that update operation is associative: (a ∪ b) ∪ c = a ∪ (b ∪ c)."""
        result1 = a.update(b).update(c)
        result2 = a.update(b.update(c))
        assert result1.to_dict() == result2.to_dict()

    @given(vector_clock_strategy())
    def test_update_identity(self, clock: VectorClock):
        """Test that empty clock is identity for update: clock ∪ ∅ = ∅ ∪ clock = clock."""
        empty = VectorClock.empty()
        assert clock.update(empty).to_dict() == clock.to_dict()
        assert empty.update(clock).to_dict() == clock.to_dict()

    @given(vector_clock_strategy(), vector_clock_strategy())
    def test_update_commutativity(self, a: VectorClock, b: VectorClock):
        """Test that update operation is commutative: a ∪ b = b ∪ a."""
        result1 = a.update(b)
        result2 = b.update(a)
        assert result1.to_dict() == result2.to_dict()

    @given(vector_clock_strategy())
    def test_update_idempotency(self, clock: VectorClock):
        """Test that update is idempotent: clock ∪ clock = clock."""
        result = clock.update(clock)
        assert result.to_dict() == clock.to_dict()

    @given(vector_clock_strategy(), st.text(min_size=1, max_size=20))
    def test_increment_monotonicity(self, clock: VectorClock, node_id: str):
        """Test that increment creates a clock that happens after the original."""
        new_clock = clock.increment(node_id)

        if not clock.is_empty():
            # New clock should happen after original (unless they're equal)
            assert new_clock.happens_after(clock) or new_clock == clock

        # Original should happen before or equal to new
        assert clock.happens_before(new_clock) or clock == new_clock

    @given(vector_clock_strategy(), vector_clock_strategy())
    def test_happens_before_transitivity(self, a: VectorClock, b: VectorClock):
        """Test transitivity of happens_before relation."""
        # Create c that definitely happens after b
        c = b.increment("test_node")

        if a.happens_before(b):
            assert a.happens_before(c)

    @given(vector_clock_strategy(), vector_clock_strategy())
    def test_comparison_consistency(self, a: VectorClock, b: VectorClock):
        """Test consistency between comparison methods."""
        comparison = a.compare(b)

        if comparison == "equal":
            assert a == b
            assert not a.happens_before(b)
            assert not a.happens_after(b)
            assert not a.concurrent_with(b)
        elif comparison == "before":
            assert a.happens_before(b)
            assert b.happens_after(a)
            assert not a.concurrent_with(b)
        elif comparison == "after":
            assert a.happens_after(b)
            assert b.happens_before(a)
            assert not a.concurrent_with(b)
        elif comparison == "concurrent":
            assert a.concurrent_with(b)
            assert b.concurrent_with(a)
            assert not a.happens_before(b)
            assert not a.happens_after(b)


class TestVectorClockRepresentation:
    """Test VectorClock string representation and debugging."""

    def test_empty_repr(self):
        """Test string representation of empty clock."""
        empty = VectorClock.empty()
        assert repr(empty) == "VectorClock()"
        assert str(empty.to_dict()) == "{}"

    def test_single_entry_repr(self):
        """Test string representation of single entry clock."""
        clock = VectorClock.single_entry("node1", 5)
        assert "node1:5" in repr(clock)
        assert "VectorClock" in repr(clock)

    def test_multiple_entries_repr(self):
        """Test string representation of multiple entries clock."""
        clock = VectorClock.from_dict({"node1": 3, "node2": 7})
        repr_str = repr(clock)
        assert "VectorClock" in repr_str
        assert "node1:3" in repr_str
        assert "node2:7" in repr_str

    @given(vector_clock_strategy())
    def test_repr_roundtrip_info(self, clock: VectorClock):
        """Test that repr contains useful information."""
        repr_str = repr(clock)
        assert "VectorClock" in repr_str

        # For non-empty clocks, should contain node info
        if not clock.is_empty():
            for node_id in clock.node_ids():
                # Node ID should appear in representation
                assert node_id in repr_str


class TestVectorClockValidation:
    """Test VectorClock validation and error conditions."""

    def test_duplicate_node_ids_validation(self):
        """Test that duplicate node IDs are not allowed."""
        entries = frozenset(
            [
                ClockEntry(node_id="node1", timestamp=1),
                ClockEntry(node_id="node1", timestamp=2),  # Duplicate
            ]
        )

        with pytest.raises(ValueError, match="Duplicate node IDs"):
            VectorClock(_entries=entries)

    @given(vector_clock_strategy(), vector_clock_strategy())
    def test_equality_and_hash_consistency(self, a: VectorClock, b: VectorClock):
        """Test that equal clocks have equal hashes."""
        if a == b:
            assert hash(a) == hash(b)

    def test_immutability(self):
        """Test that VectorClock is truly immutable."""
        clock = VectorClock.single_entry("node1", 5)

        # These operations should return new objects
        incremented = clock.increment("node2")
        updated = clock.update(VectorClock.single_entry("node3", 1))

        # Original unchanged
        assert clock.get_timestamp("node2") == 0
        assert clock.get_timestamp("node3") == 0

        # New objects have changes
        assert incremented.get_timestamp("node2") == 1
        assert updated.get_timestamp("node3") == 1


# Examples for documentation and edge case testing
class TestVectorClockExamples:
    """Test specific examples and edge cases."""

    @example(VectorClock.empty(), "node1")
    @example(VectorClock.single_entry("node1", 1), "node1")
    @example(VectorClock.single_entry("node1", 1), "node2")
    @given(vector_clock_strategy(), st.text(min_size=1, max_size=20))
    def test_increment_examples(self, clock: VectorClock, node_id: str):
        """Test increment with specific examples."""
        result = clock.increment(node_id)
        assert result.get_timestamp(node_id) == clock.get_timestamp(node_id) + 1

    def test_documentation_examples(self):
        """Test examples that would appear in documentation."""
        # Example 1: Basic usage
        clock = VectorClock.empty()
        clock = clock.increment("alice")
        clock = clock.increment("bob")
        clock = clock.increment("alice")

        assert clock.get_timestamp("alice") == 2
        assert clock.get_timestamp("bob") == 1

        # Example 2: Merging clocks
        clock1 = VectorClock.from_dict({"alice": 2, "bob": 1})
        clock2 = VectorClock.from_dict({"alice": 1, "charlie": 3})
        merged = clock1.update(clock2)

        expected = {"alice": 2, "bob": 1, "charlie": 3}
        assert merged.to_dict() == expected

        # Example 3: Causal ordering
        clock_a = VectorClock.from_dict({"node1": 1, "node2": 2})
        clock_b = VectorClock.from_dict({"node1": 2, "node2": 3})

        assert clock_a.happens_before(clock_b)
        assert clock_a.compare(clock_b) == "before"
