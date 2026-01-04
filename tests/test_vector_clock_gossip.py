"""Property-based tests for VectorClock ordering semantics."""

from hypothesis import given

from mpreg.datastructures.vector_clock import VectorClock, vector_clock_strategy


@given(vector_clock_strategy(max_entries=5))
def test_vector_clock_compare_reflexive(clock: VectorClock) -> None:
    assert clock.compare(clock) == "equal"


def test_vector_clock_transitive_increments() -> None:
    clock = VectorClock.single_entry("node_a", 0)
    inc1 = clock.increment("node_a")
    inc2 = inc1.increment("node_a")
    assert clock.compare(inc1) == "before"
    assert inc1.compare(inc2) == "before"
    assert clock.compare(inc2) == "before"


@given(vector_clock_strategy(max_entries=3))
def test_vector_clock_comparison_values(clock: VectorClock) -> None:
    other = clock.increment("node_b")
    comparison = clock.compare(other)
    assert comparison in {"equal", "before", "after", "concurrent"}
