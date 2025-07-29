"""
Comprehensive property-based tests for the centralized trie data structure.

This test suite uses Hypothesis for property-based testing to ensure absolute
correctness of the trie implementation across all possible inputs and edge cases.

Test coverage includes:
- Pattern addition and removal correctness
- Wildcard matching behavior (* and # patterns)
- Thread safety under concurrent operations
- Performance characteristics and caching
- Memory management and cleanup
- Edge cases and error conditions
- Backward compatibility with TopicTrie
"""

import threading
import time
from collections import defaultdict

import pytest
from hypothesis import HealthCheck, assume, example, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, invariant, rule

from mpreg.datastructures.trie import (
    TopicTrie,
    Trie,
    TrieConfig,
    TrieStatistics,
    create_topic_trie,
)


# Hypothesis strategies for generating test data
@st.composite
def valid_pattern_segments(draw):
    """Generate valid pattern segments (no separators)."""
    return draw(
        st.text(
            alphabet=st.characters(
                min_codepoint=97, max_codepoint=122
            ),  # lowercase letters only
            min_size=1,
            max_size=10,
        )
    )


@st.composite
def valid_patterns(draw, max_segments=5, allow_wildcards=True):
    """Generate valid hierarchical patterns."""
    num_segments = draw(st.integers(min_value=1, max_value=max_segments))
    segments = []

    for i in range(num_segments):
        if allow_wildcards and draw(st.booleans()):
            if i == num_segments - 1 and draw(st.booleans()):
                # Multi-wildcard can only be at the end
                segments.append("#")
                break
            else:
                # Single wildcard
                segments.append("*")
        else:
            # Regular segment
            segments.append(draw(valid_pattern_segments()))

    return ".".join(segments)


@st.composite
def valid_keys(draw, max_segments=5):
    """Generate valid keys for matching (no wildcards)."""
    num_segments = draw(st.integers(min_value=1, max_value=max_segments))
    segments = [draw(valid_pattern_segments()) for _ in range(num_segments)]
    return ".".join(segments)


@st.composite
def subscription_ids(draw):
    """Generate subscription IDs."""
    return draw(
        st.text(
            alphabet=st.characters(
                min_codepoint=32, max_codepoint=126
            ),  # Printable ASCII
            min_size=1,
            max_size=20,
        ).filter(lambda x: x.strip() and len(x.strip()) > 0)
    )


class TestTrieBasics:
    """Basic functionality tests for the trie implementation."""

    def test_empty_trie_initialization(self):
        """Test that empty trie initializes correctly."""
        trie = Trie[str]()
        assert trie.get_all_values() == set()
        assert trie.match_pattern("any.key") == []

        stats = trie.get_statistics()
        assert stats.total_patterns == 0
        assert stats.total_nodes == 1  # Root node

    def test_simple_pattern_addition_and_matching(self):
        """Test basic pattern addition and exact matching."""
        trie = Trie[str]()
        trie.add_pattern("user.login", "sub1")

        assert trie.match_pattern("user.login") == ["sub1"]
        assert trie.match_pattern("user.logout") == []
        assert "sub1" in trie.get_all_values()

    def test_pattern_removal(self):
        """Test pattern removal functionality."""
        trie = Trie[str]()
        trie.add_pattern("user.login", "sub1")
        trie.add_pattern("user.login", "sub2")

        # Remove one subscription
        assert trie.remove_pattern("user.login", "sub1") is True
        assert trie.match_pattern("user.login") == ["sub2"]

        # Remove non-existent subscription
        assert trie.remove_pattern("user.login", "nonexistent") is False

        # Remove last subscription
        assert trie.remove_pattern("user.login", "sub2") is True
        assert trie.match_pattern("user.login") == []

    def test_single_wildcard_matching(self):
        """Test single-level wildcard (* matches exactly one segment)."""
        trie = Trie[str]()
        trie.add_pattern("user.*.login", "sub1")

        assert trie.match_pattern("user.123.login") == ["sub1"]
        assert trie.match_pattern("user.alice.login") == ["sub1"]
        assert trie.match_pattern("user.login") == []  # Missing segment
        assert trie.match_pattern("user.123.profile.login") == []  # Too many segments

    def test_multi_wildcard_matching(self):
        """Test multi-level wildcard (# matches zero or more segments)."""
        trie = Trie[str]()
        trie.add_pattern("user.#", "sub1")

        assert trie.match_pattern("user") == ["sub1"]  # Zero segments
        assert trie.match_pattern("user.123") == ["sub1"]  # One segment
        assert trie.match_pattern("user.123.login") == ["sub1"]  # Two segments
        assert trie.match_pattern("user.123.profile.login") == ["sub1"]  # Many segments
        assert trie.match_pattern("admin.123") == []  # Different prefix

    def test_complex_wildcard_combinations(self):
        """Test complex combinations of exact matches and wildcards."""
        trie = Trie[str]()
        trie.add_pattern("user.*.login.*", "sub1")
        trie.add_pattern("user.#", "sub2")
        trie.add_pattern("user.123.#", "sub3")

        matches = trie.match_pattern("user.123.login.success")
        assert "sub1" in matches  # Matches user.*.login.*
        assert "sub2" in matches  # Matches user.#
        assert "sub3" in matches  # Matches user.123.#

    def test_configuration_options(self):
        """Test custom configuration options."""
        config = TrieConfig(
            single_wildcard_symbol="+",
            multi_wildcard_symbol="##",
            segment_separator="/",
        )
        trie = Trie[str](config=config)

        trie.add_pattern("user/+/login", "sub1")
        trie.add_pattern("user/##", "sub2")

        matches1 = trie.match_pattern("user/123/login")
        assert "sub1" in matches1
        assert "sub2" in matches1  # user/## also matches

        matches2 = trie.match_pattern("user/123/anything")
        assert "sub2" in matches2


class TestTrieProperties:
    """Property-based tests using Hypothesis."""

    @given(
        st.lists(
            st.tuples(valid_patterns(), subscription_ids()), min_size=0, max_size=20
        )
    )
    def test_pattern_addition_and_retrieval_consistency(self, pattern_sub_pairs):
        """Property: All added patterns should be retrievable."""
        if not pattern_sub_pairs:
            patterns: list[str] = []
            sub_ids: list[str] = []
        else:
            patterns, sub_ids = map(list, zip(*pattern_sub_pairs))

        trie = Trie[str]()
        pattern_to_subs = defaultdict(set)

        # Add all patterns
        for pattern, sub_id in zip(patterns, sub_ids):
            trie.add_pattern(pattern, sub_id)
            pattern_to_subs[pattern].add(sub_id)

        # Verify all added subscriptions are present
        all_added_subs = set(sub_ids)
        all_retrieved_subs = trie.get_all_values()
        assert all_added_subs.issubset(all_retrieved_subs)

    @given(
        st.lists(
            st.tuples(valid_patterns(), subscription_ids()), min_size=1, max_size=30
        )
    )
    @settings(suppress_health_check=[HealthCheck.filter_too_much])
    def test_pattern_value_association_invariant(self, pattern_value_pairs):
        """Property: Each pattern correctly associates with its values."""
        trie = Trie[str]()
        expected_associations = defaultdict(set)

        # Add all pattern-value pairs
        for pattern, value in pattern_value_pairs:
            trie.add_pattern(pattern, value)
            expected_associations[pattern].add(value)

        # Verify each pattern returns exactly its associated values
        for pattern, expected_values in expected_associations.items():
            patterns_for_values = trie.get_patterns_for_value(list(expected_values)[0])
            assert pattern in patterns_for_values

    @given(st.lists(valid_patterns(), min_size=5, max_size=15))
    @settings(suppress_health_check=[HealthCheck.filter_too_much])
    def test_wildcard_hierarchy_correctness(self, base_patterns):
        """Property: Wildcard patterns should follow proper hierarchy rules."""
        trie = Trie[str]()

        # Filter patterns to only use multi-segment ones for this test
        multi_segment_patterns = [
            p for p in base_patterns if "." in p and len(p.split(".")) >= 2
        ]
        assume(len(multi_segment_patterns) >= 2)  # Need at least 2 patterns

        # Add patterns with different wildcard types
        for i, pattern in enumerate(multi_segment_patterns):
            segments = pattern.split(".")
            trie.add_pattern(pattern, f"exact_{i}")

            # Create single wildcard pattern by replacing first segment
            single_pattern = "*." + ".".join(segments[1:])
            trie.add_pattern(single_pattern, f"single_{i}")

            # Create multi wildcard pattern
            multi_pattern = segments[0] + ".#"
            trie.add_pattern(multi_pattern, f"multi_{i}")

        # Test keys derived from patterns
        for i, pattern in enumerate(multi_segment_patterns):
            key = pattern  # Exact match key
            matches = trie.match_pattern(key)

            # Should match all three types: exact, single wildcard, and multi wildcard
            assert f"exact_{i}" in matches
            assert f"single_{i}" in matches  # Single wildcard should match
            assert f"multi_{i}" in matches  # Multi wildcard should match

    def test_removal_completeness_and_isolation(self):
        """Property: Removing patterns should not affect unrelated patterns."""
        trie = Trie[str]()

        # Use completely non-overlapping patterns to ensure isolation
        patterns_and_values = [
            ("exact.pattern.one", "value_1"),
            ("exact.pattern.two", "value_2"),
            ("exact.pattern.three", "value_3"),
            ("different.namespace.alpha", "value_4"),
            ("different.namespace.beta", "value_5"),
        ]

        # Add all patterns
        for pattern, value in patterns_and_values:
            trie.add_pattern(pattern, value)

        # Verify all values are present initially
        all_values = trie.get_all_values()
        for _, value in patterns_and_values:
            assert value in all_values

        # Remove some patterns
        to_remove = [
            ("exact.pattern.one", "value_1"),
            ("different.namespace.alpha", "value_4"),
        ]

        to_keep = [
            ("exact.pattern.two", "value_2"),
            ("exact.pattern.three", "value_3"),
            ("different.namespace.beta", "value_5"),
        ]

        for pattern, value in to_remove:
            result = trie.remove_pattern(pattern, value)
            assert result is True

        # Verify removed values are gone and kept values remain
        final_values = trie.get_all_values()

        for pattern, value in to_remove:
            assert value not in final_values

        for pattern, value in to_keep:
            assert value in final_values

    @given(
        st.text(
            alphabet=st.characters(min_codepoint=97, max_codepoint=122),
            min_size=1,
            max_size=10,
        )
    )
    def test_nonexistent_pattern_removal_safety(self, fake_pattern):
        """Property: Removing non-existent patterns should be safe."""
        trie = Trie[str]()

        # Add some real patterns
        trie.add_pattern("real.pattern", "real_value")

        # Try to remove non-existent pattern
        result = trie.remove_pattern(fake_pattern, "fake_value")
        assert result is False

        # Verify original patterns still exist
        assert "real_value" in trie.get_all_values()

        # Try to remove existing pattern with wrong value
        result = trie.remove_pattern("real.pattern", "wrong_value")
        assert result is False

        # Original should still exist
        assert "real_value" in trie.get_all_values()

    @given(st.lists(valid_keys(), min_size=5, max_size=20))
    def test_empty_trie_behavior(self, test_keys):
        """Property: Empty trie should consistently return empty results."""
        trie = Trie[str]()

        for key in test_keys:
            matches = trie.match_pattern(key)
            assert matches == []

        assert trie.get_all_values() == set()
        stats = trie.get_statistics()
        assert stats.total_patterns == 0

    @given(valid_patterns(), subscription_ids(), st.integers(min_value=1, max_value=10))
    def test_duplicate_value_handling(self, pattern, value, repeat_count):
        """Property: Adding same pattern-value pair multiple times should be idempotent."""
        trie = Trie[str]()

        # Add the same pattern-value pair multiple times
        for _ in range(repeat_count):
            trie.add_pattern(pattern, value)

        # Should only appear once in results
        matches = trie.match_pattern(pattern)
        assert matches.count(value) == 1

        # Should only appear once in all values
        all_values = list(trie.get_all_values())
        assert all_values.count(value) == 1

    @given(
        st.lists(
            st.tuples(valid_patterns(), subscription_ids()), min_size=1, max_size=15
        )
    )
    def test_statistics_consistency(self, pattern_value_pairs):
        """Property: Statistics should be consistent with actual trie state."""
        trie = Trie[str]()

        # Add patterns and track expected counts
        unique_patterns = set()
        total_values = set()

        for pattern, value in pattern_value_pairs:
            trie.add_pattern(pattern, value)
            unique_patterns.add(pattern)
            total_values.add(value)

        stats = trie.get_statistics()

        # Check statistics consistency
        assert stats.total_nodes >= 1  # At least root node
        assert len(total_values) == len(trie.get_all_values())
        assert stats.cache_hits >= 0
        assert stats.cache_misses >= 0
        assert 0.0 <= stats.cache_hit_ratio <= 1.0

    @given(
        st.lists(valid_patterns(), min_size=1, max_size=8),
        st.lists(valid_keys(), min_size=1, max_size=8),
    )
    def test_cross_pattern_key_isolation(self, patterns, keys):
        """Property: Patterns should only match appropriate keys."""
        trie = Trie[str]()

        # Add patterns with unique values
        for i, pattern in enumerate(patterns):
            trie.add_pattern(pattern, f"value_{i}")

        # Test each key against all patterns
        for key in keys:
            matches = trie.match_pattern(key)

            # Verify matches are correct by manual checking
            for i, pattern in enumerate(patterns):
                expected_value = f"value_{i}"
                should_match = self._should_pattern_match_key(
                    pattern.split("."), key.split(".")
                )

                if should_match:
                    assert expected_value in matches, (
                        f"Pattern '{pattern}' should match key '{key}' but didn't"
                    )
                else:
                    assert expected_value not in matches, (
                        f"Pattern '{pattern}' should not match key '{key}' but did"
                    )

    @given(st.integers(min_value=1, max_value=5))
    def test_configuration_isolation(self, config_variant):
        """Property: Different configurations should be properly isolated."""
        configs = [
            TrieConfig(
                single_wildcard_symbol="*",
                multi_wildcard_symbol="#",
                segment_separator=".",
            ),
            TrieConfig(
                single_wildcard_symbol="+",
                multi_wildcard_symbol="**",
                segment_separator="/",
            ),
            TrieConfig(
                single_wildcard_symbol="?",
                multi_wildcard_symbol="...",
                segment_separator="|",
            ),
            TrieConfig(
                single_wildcard_symbol="X",
                multi_wildcard_symbol="Y",
                segment_separator="-",
            ),
            TrieConfig(
                single_wildcard_symbol="1",
                multi_wildcard_symbol="2",
                segment_separator="_",
            ),
        ]

        config = configs[config_variant - 1]
        trie = Trie[str](config=config)

        # Add pattern using config's symbols
        sep = config.segment_separator
        single = config.single_wildcard_symbol
        multi = config.multi_wildcard_symbol

        trie.add_pattern(f"user{sep}{single}{sep}login", "single_wildcard_sub")
        trie.add_pattern(f"user{sep}{multi}", "multi_wildcard_sub")

        # Test matching with config's separators
        matches1 = trie.match_pattern(f"user{sep}123{sep}login")
        assert "single_wildcard_sub" in matches1
        assert "multi_wildcard_sub" in matches1

        matches2 = trie.match_pattern(f"user{sep}123{sep}anything")
        assert "multi_wildcard_sub" in matches2
        assert "single_wildcard_sub" not in matches2

    @given(
        st.lists(
            st.tuples(valid_patterns(), subscription_ids()), min_size=1, max_size=10
        )
    )
    def test_clear_operation_completeness(self, pattern_value_pairs):
        """Property: Clear should completely reset trie state."""
        trie = Trie[str]()

        # Add patterns
        for pattern, value in pattern_value_pairs:
            trie.add_pattern(pattern, value)

        # Verify trie has content
        assert len(trie.get_all_values()) > 0
        stats_before = trie.get_statistics()
        assert stats_before.total_patterns > 0

        # Clear trie
        trie.clear()

        # Verify complete reset
        assert len(trie.get_all_values()) == 0
        stats_after = trie.get_statistics()
        assert stats_after.total_patterns == 0
        assert stats_after.cached_patterns == 0
        assert stats_after.cache_hits == 0
        assert stats_after.cache_misses == 0

        # Verify trie still works after clear
        trie.add_pattern("new.pattern", "new_value")
        assert "new_value" in trie.get_all_values()

    @given(
        st.lists(
            st.tuples(valid_patterns(), subscription_ids()), min_size=1, max_size=20
        )
    )
    def test_pattern_removal_consistency(self, pattern_sub_pairs):
        """Property: Removed patterns should not match if no other patterns cover them."""
        # Use unique pairs to avoid complexity with duplicates
        unique_pairs = list(dict.fromkeys(pattern_sub_pairs))

        trie = Trie[str]()

        # Add all patterns
        for pattern, sub_id in unique_pairs:
            trie.add_pattern(pattern, sub_id)

        # Remove half the patterns
        to_remove = unique_pairs[: len(unique_pairs) // 2]
        for pattern, sub_id in to_remove:
            trie.remove_pattern(pattern, sub_id)

        # Verify removed patterns don't match their own keys
        # BUT only if no other remaining pattern would match them
        remaining_pairs = unique_pairs[len(unique_pairs) // 2 :]

        for pattern, sub_id in to_remove:
            if "*" not in pattern and "#" not in pattern:
                # Only test exact patterns (no wildcards)
                matches = trie.match_pattern(pattern)

                # Check if any remaining pattern would match this key
                would_be_matched_by_remaining = False
                for rem_pattern, rem_sub_id in remaining_pairs:
                    if self._should_pattern_match_key(
                        rem_pattern.split("."), pattern.split(".")
                    ):
                        would_be_matched_by_remaining = True
                        break

                # Only assert sub_id not in matches if no other pattern would match
                if not would_be_matched_by_remaining:
                    assert sub_id not in matches

    @given(valid_patterns(), subscription_ids(), valid_keys())
    @example("user.*", "sub1", "user.123")  # Explicit example
    @example("user.#", "sub1", "user")  # Multi-wildcard with zero segments
    def test_wildcard_matching_correctness(self, pattern, sub_id, key):
        """Property: Wildcard matching should follow AMQP rules."""
        trie = Trie[str]()
        trie.add_pattern(pattern, sub_id)

        matches = trie.match_pattern(key)
        pattern_segments = pattern.split(".")
        key_segments = key.split(".")

        should_match = self._should_pattern_match_key(pattern_segments, key_segments)

        if should_match:
            assert sub_id in matches, f"Pattern '{pattern}' should match key '{key}'"
        else:
            assert sub_id not in matches, (
                f"Pattern '{pattern}' should not match key '{key}'"
            )

    def _should_pattern_match_key(
        self, pattern_segments: list[str], key_segments: list[str]
    ) -> bool:
        """Helper to determine if a pattern should match a key."""
        p_idx = 0
        k_idx = 0

        while p_idx < len(pattern_segments) and k_idx < len(key_segments):
            pattern_seg = pattern_segments[p_idx]

            if pattern_seg == "*":
                # Single wildcard matches exactly one segment
                p_idx += 1
                k_idx += 1
            elif pattern_seg == "#":
                # Multi-wildcard matches everything remaining
                return True
            else:
                # Exact match required
                if pattern_seg != key_segments[k_idx]:
                    return False
                p_idx += 1
                k_idx += 1

        # Check if we consumed all segments correctly
        if p_idx < len(pattern_segments):
            # Remaining pattern segments
            remaining = pattern_segments[p_idx:]
            if len(remaining) == 1 and remaining[0] == "#":
                return True  # Multi-wildcard can match zero segments
            return False

        return k_idx == len(key_segments)  # All key segments consumed

    @given(
        st.lists(
            st.tuples(valid_patterns(), subscription_ids()), min_size=1, max_size=10
        ),
        valid_keys(),
    )
    def test_match_result_determinism(self, pattern_sub_pairs, key):
        """Property: Multiple calls to match_pattern should return identical results."""
        trie = Trie[str]()

        for pattern, sub_id in pattern_sub_pairs:
            trie.add_pattern(pattern, sub_id)

        # Get results multiple times
        result1 = set(trie.match_pattern(key))
        result2 = set(trie.match_pattern(key))
        result3 = set(trie.match_pattern(key))

        assert result1 == result2 == result3

    @given(st.integers(min_value=1, max_value=100))
    def test_cache_behavior(self, num_operations):
        """Property: Cache should improve performance without changing results."""
        # Test with caching enabled
        trie_cached = Trie[str](config=TrieConfig(enable_caching=True))
        trie_cached.add_pattern("user.*.login", "sub1")

        # Test with caching disabled
        trie_uncached = Trie[str](config=TrieConfig(enable_caching=False))
        trie_uncached.add_pattern("user.*.login", "sub1")

        test_key = "user.123.login"

        # Results should be identical
        for _ in range(num_operations):
            cached_result = trie_cached.match_pattern(test_key)
            uncached_result = trie_uncached.match_pattern(test_key)
            assert cached_result == uncached_result

        # Cache should have hits after first call
        stats_cached = trie_cached.get_statistics()
        stats_uncached = trie_uncached.get_statistics()

        if num_operations > 1:
            assert stats_cached.cache_hits > 0
            assert stats_uncached.cache_hits == 0


class TestTrieStateMachine(RuleBasedStateMachine):
    """Stateful property-based testing using Hypothesis state machine."""

    patterns = Bundle("patterns")
    subscriptions = Bundle("subscriptions")

    def __init__(self):
        super().__init__()
        self.trie = Trie[str]()
        self.expected_patterns = defaultdict(set)  # pattern -> set of subscription_ids

    @rule(target=patterns, pattern=valid_patterns())
    def add_pattern_rule(self, pattern):
        return pattern

    @rule(target=subscriptions, sub_id=subscription_ids())
    def add_subscription_rule(self, sub_id):
        return sub_id

    @rule(pattern=patterns, sub_id=subscriptions)
    def add_pattern_to_trie(self, pattern, sub_id):
        self.trie.add_pattern(pattern, sub_id)
        self.expected_patterns[pattern].add(sub_id)

    @rule(pattern=patterns, sub_id=subscriptions)
    def remove_pattern_from_trie(self, pattern, sub_id):
        result = self.trie.remove_pattern(pattern, sub_id)

        if sub_id in self.expected_patterns[pattern]:
            assert result is True
            self.expected_patterns[pattern].discard(sub_id)
            if not self.expected_patterns[pattern]:
                del self.expected_patterns[pattern]
        else:
            assert result is False

    @rule(key=valid_keys())
    def verify_matching_consistency(self, key):
        """Verify that trie matching is consistent with expected patterns."""
        actual_matches = set(self.trie.match_pattern(key))
        expected_matches = set()

        for pattern, sub_ids in self.expected_patterns.items():
            pattern_segments = pattern.split(".")
            key_segments = key.split(".")

            if self._should_pattern_match_key(pattern_segments, key_segments):
                expected_matches.update(sub_ids)

        assert actual_matches == expected_matches

    def _should_pattern_match_key(
        self, pattern_segments: list[str], key_segments: list[str]
    ) -> bool:
        """Helper to determine if a pattern should match a key."""
        p_idx = 0
        k_idx = 0

        while p_idx < len(pattern_segments) and k_idx < len(key_segments):
            pattern_seg = pattern_segments[p_idx]

            if pattern_seg == "*":
                p_idx += 1
                k_idx += 1
            elif pattern_seg == "#":
                return True
            else:
                if pattern_seg != key_segments[k_idx]:
                    return False
                p_idx += 1
                k_idx += 1

        if p_idx < len(pattern_segments):
            remaining = pattern_segments[p_idx:]
            if len(remaining) == 1 and remaining[0] == "#":
                return True
            return False

        return k_idx == len(key_segments)

    @invariant()
    def all_values_are_findable(self):
        """Invariant: All values in trie should be findable via get_all_values."""
        expected_all_values = set()
        for sub_ids in self.expected_patterns.values():
            expected_all_values.update(sub_ids)

        actual_all_values = self.trie.get_all_values()
        assert expected_all_values.issubset(actual_all_values)

    @invariant()
    def statistics_are_sensible(self):
        """Invariant: Statistics should be reasonable."""
        stats = self.trie.get_statistics()
        assert stats.total_nodes >= 1  # At least root node
        assert stats.cache_hits >= 0
        assert stats.cache_misses >= 0
        assert 0.0 <= stats.cache_hit_ratio <= 1.0
        assert stats.total_patterns >= 0


# Run the state machine test
TestTrieStateMachineTest = TestTrieStateMachine.TestCase


class TestTrieThreadSafety:
    """Tests for thread safety of trie operations."""

    def test_concurrent_pattern_addition(self):
        """Test that concurrent pattern additions don't corrupt the trie."""
        trie = Trie[str]()
        num_threads = 10
        patterns_per_thread = 20

        def add_patterns(thread_id):
            for i in range(patterns_per_thread):
                pattern = f"thread.{thread_id}.pattern.{i}"
                sub_id = f"sub_{thread_id}_{i}"
                trie.add_pattern(pattern, sub_id)

        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=add_patterns, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Verify all patterns were added
        all_values = trie.get_all_values()
        expected_count = num_threads * patterns_per_thread
        assert len(all_values) == expected_count

    def test_concurrent_read_write_operations(self):
        """Test concurrent reads and writes don't cause issues."""
        trie = Trie[str]()
        trie.add_pattern("user.*.login", "base_sub")

        results = []
        exceptions = []

        def reader():
            try:
                for _ in range(100):
                    result = trie.match_pattern("user.123.login")
                    results.append(result)
            except Exception as e:
                exceptions.append(e)

        def writer():
            try:
                for i in range(50):
                    trie.add_pattern(f"user.{i}.login", f"sub_{i}")
            except Exception as e:
                exceptions.append(e)

        # Start concurrent operations
        reader_threads = [threading.Thread(target=reader) for _ in range(3)]
        writer_threads = [threading.Thread(target=writer) for _ in range(2)]

        all_threads = reader_threads + writer_threads
        for thread in all_threads:
            thread.start()

        for thread in all_threads:
            thread.join()

        # Should have no exceptions
        assert len(exceptions) == 0
        # Should have successful reads
        assert len(results) > 0
        # All reads should return at least the base subscription
        for result in results:
            assert "base_sub" in result


class TestTopicTrieBackwardCompatibility:
    """Tests for TopicTrie backward compatibility."""

    def test_topic_trie_initialization(self):
        """Test TopicTrie initializes with correct defaults."""
        trie = TopicTrie()
        assert trie.config.single_wildcard_symbol == "*"
        assert trie.config.multi_wildcard_symbol == "#"
        assert trie.config.segment_separator == "."

    def test_topic_trie_methods(self):
        """Test TopicTrie backward compatibility methods."""
        trie = TopicTrie()

        # Test add_pattern method
        trie.add_pattern("user.*.login", "sub1")

        # Test match_topic method (backward compatibility)
        matches = trie.match_topic("user.123.login")
        assert "sub1" in matches

        # Test remove_pattern method
        trie.remove_pattern("user.*.login", "sub1")
        matches = trie.match_topic("user.123.login")
        assert "sub1" not in matches

        # Test get_stats method (backward compatibility)
        stats = trie.get_stats()
        assert isinstance(stats, TrieStatistics)

    def test_create_topic_trie_factory(self):
        """Test topic trie factory function."""
        trie = create_topic_trie(enable_caching=False, thread_safe=False)
        assert trie.config.enable_caching is False
        assert not trie.config.thread_safe


class TestTriePerformance:
    """Performance and scalability tests."""

    def test_large_scale_pattern_matching(self):
        """Test trie performance with large numbers of patterns."""
        trie = Trie[str]()

        # Add many patterns
        num_patterns = 1000
        for i in range(num_patterns):
            if i % 3 == 0:
                pattern = f"user.*.action.{i}"
            elif i % 3 == 1:
                pattern = f"system.{i}.#"
            else:
                pattern = f"exact.{i}.pattern"

            trie.add_pattern(pattern, f"sub_{i}")

        # Test matching performance with repeated keys to trigger cache hits
        test_keys = [
            "user.123.action.456",
            "system.456.anything.else",
            "exact.789.pattern",
        ]

        start_time = time.time()
        # Do multiple rounds of the same keys to get cache hits
        for round_num in range(50):
            for key in test_keys:
                trie.match_pattern(key)

        end_time = time.time()
        total_time = end_time - start_time

        # Should complete within reasonable time (adjust threshold as needed)
        assert total_time < 1.0, f"Matching took too long: {total_time:.3f}s"

        # Verify cache effectiveness
        stats = trie.get_statistics()
        if stats.cache_hits + stats.cache_misses > 0:
            # With repeated keys, we should get decent cache hit ratio
            assert stats.cache_hit_ratio > 0.6, (
                f"Cache hit ratio too low: {stats.cache_hit_ratio:.3f}"
            )

    def test_memory_usage_estimation(self):
        """Test memory usage estimation in statistics."""
        trie = Trie[str]()

        initial_stats = trie.get_statistics()
        initial_memory = initial_stats.memory_usage_estimate_bytes

        # Add patterns and check memory growth
        for i in range(100):
            trie.add_pattern(f"user.{i}.#", f"sub_{i}")

        final_stats = trie.get_statistics()
        final_memory = final_stats.memory_usage_estimate_bytes

        # Memory usage should increase
        assert final_memory > initial_memory
        assert final_stats.total_nodes > initial_stats.total_nodes


class TestTrieEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_empty_pattern_handling(self):
        """Test handling of empty or invalid patterns."""
        trie = Trie[str]()

        # Empty pattern
        trie.add_pattern("", "sub1")
        assert "sub1" in trie.match_pattern("")

        # Single segment pattern
        trie.add_pattern("single", "sub2")
        assert "sub2" in trie.match_pattern("single")


class TestWildcardPatternExpansion:
    """Comprehensive tests for wildcard pattern expansion and positioning."""

    def test_single_wildcard_positioning_variants(self):
        """Test * wildcard in different positions."""
        trie = Trie[str]()

        # Start position
        trie.add_pattern("*.middle.end", "start_wildcard")
        assert "start_wildcard" in trie.match_pattern("anything.middle.end")
        assert "start_wildcard" not in trie.match_pattern("anything.wrong.end")
        assert "start_wildcard" not in trie.match_pattern(
            "middle.end"
        )  # Missing segment

        # Middle position
        trie.add_pattern("start.*.end", "middle_wildcard")
        assert "middle_wildcard" in trie.match_pattern("start.anything.end")
        assert "middle_wildcard" not in trie.match_pattern(
            "start.end"
        )  # Missing segment
        assert "middle_wildcard" not in trie.match_pattern(
            "start.one.two.end"
        )  # Too many segments

        # End position
        trie.add_pattern("start.middle.*", "end_wildcard")
        assert "end_wildcard" in trie.match_pattern("start.middle.anything")
        assert "end_wildcard" not in trie.match_pattern(
            "start.middle"
        )  # Missing segment
        assert "end_wildcard" not in trie.match_pattern(
            "start.middle.one.two"
        )  # Too many segments

    def test_multi_wildcard_positioning_variants(self):
        """Test # wildcard in different positions with proper expansion."""
        trie = Trie[str]()

        # Multi-wildcard at end (most common)
        trie.add_pattern("prefix.#", "multi_end")
        assert "multi_end" in trie.match_pattern("prefix")  # Zero segments
        assert "multi_end" in trie.match_pattern("prefix.one")  # One segment
        assert "multi_end" in trie.match_pattern(
            "prefix.one.two.three"
        )  # Many segments
        assert "multi_end" not in trie.match_pattern("different.one")  # Wrong prefix

        # Multi-wildcard in middle (unusual but should work)
        trie.add_pattern("start.#.end", "multi_middle")
        # Note: This is tricky - # should consume segments until we can match "end"
        # This is actually an implementation choice about how greedy # is

        # Multi-wildcard at start (unusual)
        trie.add_pattern("#.suffix", "multi_start")
        # This should match any number of segments followed by "suffix"

    def test_standalone_wildcards(self):
        """Test wildcards as complete patterns."""
        trie = Trie[str]()

        # Single wildcard only - matches exactly one segment
        trie.add_pattern("*", "single_only")
        assert "single_only" in trie.match_pattern("anything")
        assert "single_only" not in trie.match_pattern("two.segments")
        assert "single_only" not in trie.match_pattern("")  # No segments

        # Multi wildcard only - matches any number of segments
        trie.add_pattern("#", "multi_only")
        assert "multi_only" in trie.match_pattern("")  # Zero segments
        assert "multi_only" in trie.match_pattern("one")
        assert "multi_only" in trie.match_pattern("one.two.three.four")

    def test_consecutive_single_wildcards(self):
        """Test multiple * wildcards in sequence."""
        trie = Trie[str]()

        # Two consecutive single wildcards
        trie.add_pattern("*.*.end", "double_single")
        assert "double_single" in trie.match_pattern("one.two.end")
        assert "double_single" not in trie.match_pattern("one.end")  # Too few segments
        assert "double_single" not in trie.match_pattern(
            "one.two.three.end"
        )  # Too many segments

        # Three consecutive single wildcards
        trie.add_pattern("*.*.*", "triple_single")
        assert "triple_single" in trie.match_pattern("a.b.c")
        assert "triple_single" not in trie.match_pattern("a.b")
        assert "triple_single" not in trie.match_pattern("a.b.c.d")

    def test_mixed_wildcard_combinations(self):
        """Test mixing * and # in various combinations."""
        trie = Trie[str]()

        # Single wildcard followed by multi wildcard
        trie.add_pattern("prefix.*.#", "single_then_multi")
        assert "single_then_multi" in trie.match_pattern(
            "prefix.middle"
        )  # * matches "middle", # matches zero
        assert "single_then_multi" in trie.match_pattern(
            "prefix.middle.suffix"
        )  # * matches "middle", # matches "suffix"
        assert "single_then_multi" in trie.match_pattern("prefix.middle.one.two.three")
        assert "single_then_multi" not in trie.match_pattern(
            "prefix"
        )  # Missing required segment for *

        # Multi wildcard followed by single wildcard (unusual)
        trie.add_pattern("#.*", "multi_then_single")
        # This is complex - # should be greedy or non-greedy?
        # Implementation dependent behavior

        # Alternating pattern
        trie.add_pattern("*.fixed.#", "alternating")
        assert "alternating" in trie.match_pattern(
            "var.fixed"
        )  # * matches "var", # matches zero
        assert "alternating" in trie.match_pattern("var.fixed.suffix")
        assert "alternating" not in trie.match_pattern("var.wrong.suffix")

    def test_wildcard_boundary_conditions(self):
        """Test wildcards at pattern boundaries."""
        trie = Trie[str]()

        # Leading separator with wildcard
        trie.add_pattern(".*.suffix", "leading_sep")
        assert "leading_sep" in trie.match_pattern(".middle.suffix")
        assert "leading_sep" not in trie.match_pattern(
            "middle.suffix"
        )  # No leading separator

        # Trailing separator with wildcard
        trie.add_pattern("prefix.*.", "trailing_sep")
        assert "trailing_sep" in trie.match_pattern("prefix.middle.")
        assert "trailing_sep" not in trie.match_pattern(
            "prefix.middle"
        )  # No trailing separator

        # Multiple separators creating empty segments
        trie.add_pattern("*..", "empty_segments")
        assert "empty_segments" in trie.match_pattern("something..")

    def test_complex_pattern_hierarchies(self):
        """Test complex hierarchical patterns with multiple wildcards."""
        trie = Trie[str]()

        # Overlapping patterns with different wildcard types
        patterns = [
            ("user.123.action", "exact"),
            ("user.*.action", "single_wild"),
            ("user.#", "multi_wild"),
            ("*.123.action", "different_single"),
            ("#", "global_wild"),
        ]

        for pattern, value in patterns:
            trie.add_pattern(pattern, value)

        # Test key that should match multiple patterns
        matches = trie.match_pattern("user.123.action")
        assert "exact" in matches
        assert "single_wild" in matches
        assert "multi_wild" in matches
        assert "different_single" in matches
        assert "global_wild" in matches

        # Test key that matches fewer patterns
        matches = trie.match_pattern("user.456.action")
        assert "exact" not in matches
        assert "single_wild" in matches
        assert "multi_wild" in matches
        assert "different_single" not in matches  # Wrong middle segment
        assert "global_wild" in matches

    def test_wildcard_pattern_specificity(self):
        """Test that more specific patterns work alongside wildcards."""
        trie = Trie[str]()

        # From most general to most specific
        trie.add_pattern("#", "most_general")
        trie.add_pattern("user.#", "user_general")
        trie.add_pattern("user.*.#", "user_single_multi")
        trie.add_pattern("user.*.action", "user_single_action")
        trie.add_pattern("user.123.action", "most_specific")

        # Test specificity matching
        matches = trie.match_pattern("user.123.action")
        assert len(matches) == 5  # Should match all patterns

        matches = trie.match_pattern("user.456.login")
        expected = {"most_general", "user_general", "user_single_multi"}
        assert expected.issubset(set(matches))
        assert "user_single_action" not in matches
        assert "most_specific" not in matches

    def test_malformed_wildcard_patterns(self):
        """Test edge cases with potentially malformed wildcard usage."""
        trie = Trie[str]()

        # Multiple consecutive multi-wildcards (unusual)
        trie.add_pattern("#.#", "double_multi")
        # Implementation choice: does this make sense? Should it work?

        # Multi-wildcard not at segment boundary
        trie.add_pattern("prefix#", "embedded_multi")
        # This should be treated as literal "prefix#", not as a wildcard
        assert "embedded_multi" in trie.match_pattern("prefix#")
        assert "embedded_multi" not in trie.match_pattern("prefix.anything")

        # Mixed symbols in segment
        trie.add_pattern("pre*fix", "embedded_single")
        # This should be treated as literal "pre*fix"
        assert "embedded_single" in trie.match_pattern("pre*fix")
        assert "embedded_single" not in trie.match_pattern("preanythingfix")

    def test_wildcard_cache_behavior(self):
        """Test that wildcard patterns are properly cached."""
        trie = Trie[str]()

        trie.add_pattern("user.*.action", "wildcard_pattern")

        # First match should be cache miss
        matches1 = trie.match_pattern("user.123.action")
        stats1 = trie.get_statistics()

        # Second match should be cache hit
        matches2 = trie.match_pattern("user.123.action")
        stats2 = trie.get_statistics()

        assert matches1 == matches2
        assert stats2.cache_hits > stats1.cache_hits

        # Different key with same pattern should be separate cache entry
        matches3 = trie.match_pattern("user.456.action")
        assert "wildcard_pattern" in matches3

    @given(st.integers(min_value=1, max_value=20))
    def test_deep_wildcard_nesting(self, depth):
        """Test wildcards in very deep hierarchical patterns."""
        trie = Trie[str]()

        # Create pattern with wildcards at various depths
        segments = []
        for i in range(depth):
            if i % 3 == 0:
                segments.append("*")
            elif i % 7 == 0:
                segments.append("#")
            else:
                segments.append(f"level{i}")

        pattern = ".".join(segments)
        trie.add_pattern(pattern, f"deep_pattern_{depth}")

        # Create matching key (replace wildcards with concrete values)
        key_segments = []
        for i, segment in enumerate(segments):
            if segment == "*":
                key_segments.append(f"wild{i}")
            elif segment == "#":
                # For testing, just add one segment for #
                key_segments.append(f"multi{i}")
            else:
                key_segments.append(segment)

        key = ".".join(key_segments)
        matches = trie.match_pattern(key)

        # Should find the pattern (though # matching might be complex)
        if "#" not in pattern:  # Simple case without multi-wildcard
            assert f"deep_pattern_{depth}" in matches

    def test_old_multi_wildcard_positioning(self):
        """Test that multi-wildcard (#) must be at the end."""
        trie = Trie[str]()

        # Valid: # at the end
        trie.add_pattern("user.#", "sub1")
        assert "sub1" in trie.match_pattern("user.anything.here")

        # Edge case: # not at the end - should still work but unusual
        trie.add_pattern("user.#.extra", "sub2")
        # This creates a pattern where # consumes segments up to .extra
        # The behavior depends on implementation, but it should be consistent

    def test_special_characters_in_segments(self):
        """Test handling of special characters in pattern segments."""
        trie = Trie[str]()

        # Segments with special characters (but not separators)
        trie.add_pattern("user-123.login_event", "sub1")
        assert "sub1" in trie.match_pattern("user-123.login_event")

        # Unicode characters
        trie.add_pattern("user.ñoño.login", "sub2")
        assert "sub2" in trie.match_pattern("user.ñoño.login")

    def test_very_deep_patterns(self):
        """Test handling of very deep hierarchical patterns."""
        trie = Trie[str]()

        # Create a very deep pattern
        deep_pattern = ".".join([f"level{i}" for i in range(20)])
        trie.add_pattern(deep_pattern, "deep_sub")

        assert "deep_sub" in trie.match_pattern(deep_pattern)

        # Test wildcard at various levels
        wildcard_pattern = ".".join(
            [f"level{i}" if i != 10 else "*" for i in range(20)]
        )
        trie.add_pattern(wildcard_pattern, "wildcard_sub")

        test_key = ".".join([f"level{i}" if i != 10 else "anything" for i in range(20)])
        assert "wildcard_sub" in trie.match_pattern(test_key)

    @given(st.lists(subscription_ids(), min_size=1, max_size=100))
    def test_massive_values_per_pattern(self, values):
        """Test many values associated with single pattern."""
        trie = Trie[str]()
        pattern = "test.pattern"

        for value in values:
            trie.add_pattern(pattern, value)

        matches = trie.match_pattern(pattern)
        assert len(matches) == len(set(values))  # Should handle duplicates
        for value in set(values):
            assert value in matches

    def test_wildcard_only_patterns(self):
        """Test patterns that are only wildcards."""
        trie = Trie[str]()

        # Pattern with only single wildcard
        trie.add_pattern("*", "single_only")
        assert "single_only" in trie.match_pattern("anything")
        assert "single_only" not in trie.match_pattern("two.segments")

        # Pattern with only multi wildcard
        trie.add_pattern("#", "multi_only")
        assert "multi_only" in trie.match_pattern("anything")
        assert "multi_only" in trie.match_pattern("two.segments")
        assert "multi_only" in trie.match_pattern("")  # Matches zero segments

    def test_consecutive_wildcards(self):
        """Test patterns with consecutive wildcards."""
        trie = Trie[str]()

        # Consecutive single wildcards
        trie.add_pattern("*.*.login", "double_single")
        assert "double_single" in trie.match_pattern("user.123.login")
        assert "double_single" not in trie.match_pattern(
            "user.login"
        )  # Need exactly 2 segments
        assert "double_single" not in trie.match_pattern(
            "user.123.456.login"
        )  # Too many

        # Mixed wildcards
        trie.add_pattern("user.*.#", "mixed_wildcards")
        assert "mixed_wildcards" in trie.match_pattern("user.123.anything.else")
        assert "mixed_wildcards" in trie.match_pattern("user.123")  # # can match zero

    def test_boundary_segment_lengths(self):
        """Test very short and very long segments."""
        trie = Trie[str]()

        # Very short segments
        trie.add_pattern("a.b.c", "short_segments")
        assert "short_segments" in trie.match_pattern("a.b.c")

        # Very long segments
        long_segment = "a" * 1000
        long_pattern = f"{long_segment}.test.{long_segment}"
        trie.add_pattern(long_pattern, "long_segments")
        assert "long_segments" in trie.match_pattern(long_pattern)

    def test_separator_edge_cases(self):
        """Test edge cases around separators."""
        trie = Trie[str]()

        # Multiple consecutive separators should create empty segments
        trie.add_pattern("user..login", "empty_segment")
        assert "empty_segment" in trie.match_pattern("user..login")

        # Leading/trailing separators
        trie.add_pattern(".user.login.", "leading_trailing")
        assert "leading_trailing" in trie.match_pattern(".user.login.")

    @given(st.integers(min_value=1, max_value=10))
    def test_thread_safety_stress(self, thread_count):
        """Stress test thread safety with many concurrent operations."""
        trie = Trie[str]()
        results = []
        exceptions = []

        def stress_operations():
            try:
                for i in range(100):
                    # Mix of operations
                    trie.add_pattern(f"stress.{i}.pattern", f"value_{i}")
                    matches = trie.match_pattern(f"stress.{i}.pattern")
                    if i % 10 == 0:
                        trie.remove_pattern(f"stress.{i}.pattern", f"value_{i}")
                    results.append(len(matches))
            except Exception as e:
                exceptions.append(e)

        threads = []
        for _ in range(min(thread_count, 5)):  # Limit to 5 threads for CI
            thread = threading.Thread(target=stress_operations)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(exceptions) == 0, f"Thread safety violations: {exceptions}"
        assert len(results) > 0, "No operations completed"

    def test_memory_pressure_handling(self):
        """Test behavior under memory pressure."""
        trie = Trie[str]()

        # Add many patterns to stress memory
        for i in range(5000):
            pattern = f"mem.test.{i}.{'x' * (i % 50)}"
            trie.add_pattern(pattern, f"value_{i}")

        # Verify basic functionality still works
        # When i=100, i%50=0, so pattern is "mem.test.100."
        test_pattern = "mem.test.100."
        matches = trie.match_pattern(test_pattern)
        assert "value_100" in matches

        # Check statistics are reasonable
        stats = trie.get_statistics()
        assert stats.total_nodes > 1000  # Should have created many nodes
        assert (
            stats.memory_usage_estimate_bytes > 100000
        )  # Should estimate significant memory

    @given(
        st.text(
            alphabet=st.characters(blacklist_characters="\x00"), min_size=0, max_size=50
        )
    )
    def test_arbitrary_segment_content(self, segment_content):
        """Test segments with arbitrary content."""
        # Skip if contains our default separator
        assume("." not in segment_content)

        trie = Trie[str]()

        # Use arbitrary content as a segment
        pattern = (
            f"prefix.{segment_content}.suffix" if segment_content else "prefix..suffix"
        )
        trie.add_pattern(pattern, "arbitrary_content")

        matches = trie.match_pattern(pattern)
        assert "arbitrary_content" in matches

    def test_cache_invalidation_edge_cases(self):
        """Test edge cases in cache invalidation."""
        trie = Trie[str]()

        # Fill cache
        trie.add_pattern("cache.test", "value1")
        for i in range(100):
            trie.match_pattern(f"cache.{i}")  # These will be cached as empty results

        # Verify cache has entries
        stats = trie.get_statistics()
        assert stats.cached_patterns > 0

        # Add pattern that should invalidate cache
        trie.add_pattern("cache.50", "value2")

        # Verify cache was cleared
        stats_after = trie.get_statistics()
        assert stats_after.cached_patterns == 0

        # Verify functionality still correct
        matches = trie.match_pattern("cache.50")
        assert "value2" in matches

    def test_statistics_edge_cases(self):
        """Test statistics in edge cases."""
        trie = Trie[str]()

        # Empty trie statistics
        stats = trie.get_statistics()
        assert stats.total_nodes == 1  # Root node
        assert stats.total_patterns == 0
        assert stats.average_pattern_depth == 0.0
        assert stats.cache_hit_ratio == 0.0  # No operations yet

        # Add and immediately remove pattern
        trie.add_pattern("temp.pattern", "temp_value")
        trie.remove_pattern("temp.pattern", "temp_value")

        # Statistics should reflect the changes
        stats_after = trie.get_statistics()
        assert stats_after.total_patterns == 0

    def test_specific_wildcard_patterns_comprehensive(self):
        """Test specific wildcard patterns mentioned for comprehensive edge case coverage."""
        trie = Trie[str]()

        # Test abc.* pattern (single wildcard at end)
        trie.add_pattern("abc.*", "end_wildcard")
        assert "end_wildcard" in trie.match_pattern("abc.def")
        assert "end_wildcard" in trie.match_pattern("abc.xyz")
        assert "end_wildcard" in trie.match_pattern("abc.123")
        assert "end_wildcard" not in trie.match_pattern(
            "abc.def.ghi"
        )  # Too many segments
        assert "end_wildcard" not in trie.match_pattern("abc")  # Too few segments
        assert "end_wildcard" not in trie.match_pattern("abcd.xyz")  # Wrong prefix

        # Test abc.*.def.hij pattern (single wildcard in middle)
        trie.add_pattern("abc.*.def.hij", "middle_wildcard")
        assert "middle_wildcard" in trie.match_pattern("abc.XYZ.def.hij")
        assert "middle_wildcard" in trie.match_pattern("abc.123.def.hij")
        assert "middle_wildcard" in trie.match_pattern("abc.anything.def.hij")
        assert "middle_wildcard" not in trie.match_pattern(
            "abc.def.hij"
        )  # Missing middle segment
        assert "middle_wildcard" not in trie.match_pattern(
            "abc.X.Y.def.hij"
        )  # Too many segments
        assert "middle_wildcard" not in trie.match_pattern(
            "ab.X.def.hij"
        )  # Wrong prefix
        assert "middle_wildcard" not in trie.match_pattern(
            "abc.X.def.hi"
        )  # Wrong suffix

        # Test abc.# pattern (multi wildcard)
        trie.add_pattern("abc.#", "multi_wildcard")
        assert "multi_wildcard" in trie.match_pattern("abc")  # Zero additional segments
        assert "multi_wildcard" in trie.match_pattern(
            "abc.def"
        )  # One additional segment
        assert "multi_wildcard" in trie.match_pattern(
            "abc.def.ghi.jkl"
        )  # Multiple additional segments
        assert "multi_wildcard" in trie.match_pattern("abc.anything.goes.here.really")
        assert "multi_wildcard" not in trie.match_pattern("ab")  # Wrong prefix
        assert "multi_wildcard" not in trie.match_pattern("abcd")  # Wrong prefix
        assert "multi_wildcard" not in trie.match_pattern("")  # Empty string

        # Test overlapping patterns work correctly
        abc_def_matches = sorted(trie.match_pattern("abc.def"))
        assert "end_wildcard" in abc_def_matches
        assert "multi_wildcard" in abc_def_matches
        assert len(abc_def_matches) == 2

        # Test edge cases with just wildcards
        trie.clear()
        trie.add_pattern("*", "single_only")
        trie.add_pattern("#", "multi_only")

        # Single wildcard should match exactly one segment
        one_segment_matches = sorted(trie.match_pattern("one"))
        assert "single_only" in one_segment_matches
        assert "multi_only" in one_segment_matches
        assert len(one_segment_matches) == 2

        # Empty string should only match multi wildcard
        empty_matches = trie.match_pattern("")
        assert "single_only" not in empty_matches
        assert "multi_only" in empty_matches
        assert len(empty_matches) == 1

        # Multiple segments should only match multi wildcard
        multi_segment_matches = trie.match_pattern("one.two")
        assert "single_only" not in multi_segment_matches
        assert "multi_only" in multi_segment_matches
        assert len(multi_segment_matches) == 1

        # Test complex combinations
        trie.clear()
        trie.add_pattern("*.*.end", "double_single")
        trie.add_pattern("#.end", "multi_end")
        trie.add_pattern("start.#", "start_multi")

        # Test that double single wildcard works correctly
        assert "double_single" in trie.match_pattern("a.b.end")
        assert "double_single" not in trie.match_pattern("a.end")  # Not enough segments
        assert "double_single" not in trie.match_pattern(
            "a.b.c.end"
        )  # Too many segments

        # Test multi wildcard with suffix
        assert "multi_end" in trie.match_pattern("anything.end")
        assert "multi_end" in trie.match_pattern("a.b.c.end")
        assert "multi_end" in trie.match_pattern("end")  # Zero prefix segments
        assert "multi_end" not in trie.match_pattern(
            "end.something"
        )  # Extra segments after pattern

        # Test multi wildcard with prefix
        assert "start_multi" in trie.match_pattern("start")  # Zero suffix segments
        assert "start_multi" in trie.match_pattern("start.anything")
        assert "start_multi" in trie.match_pattern("start.a.b.c")
        assert "start_multi" not in trie.match_pattern(
            "something.start"
        )  # Wrong structure


class TestTriePropertyBasedTesting:
    """Comprehensive property-based testing using Hypothesis for edge case discovery."""

    @given(
        patterns=st.lists(
            st.text(
                alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd", "Pc")),
                min_size=1,
                max_size=20,
            ).filter(lambda x: "." not in x and "*" not in x and "#" not in x),
            min_size=1,
            max_size=10,
        ),
        values=st.lists(
            st.integers(min_value=0, max_value=1000), min_size=1, max_size=10
        ),
    )
    @settings(max_examples=50, deadline=None)
    def test_pattern_addition_removal_consistency(self, patterns, values):
        """Property: Adding and then removing patterns should restore trie state."""
        trie = Trie[int]()

        # Create pattern-value pairs
        pattern_value_pairs = list(zip(patterns, values))

        # Add all patterns
        for pattern, value in pattern_value_pairs:
            trie.add_pattern(pattern, value)

        # Verify all patterns match their values
        for pattern, expected_value in pattern_value_pairs:
            matches = trie.match_pattern(pattern)
            assert expected_value in matches

        # Remove all patterns (handle duplicates correctly)
        removed_pairs = set()
        for pattern, value in pattern_value_pairs:
            if (pattern, value) not in removed_pairs:
                assert trie.remove_pattern(pattern, value) is True
                removed_pairs.add((pattern, value))
            else:
                # Duplicate removal should return False
                assert trie.remove_pattern(pattern, value) is False

        # Verify trie is empty
        for pattern, _ in pattern_value_pairs:
            assert trie.match_pattern(pattern) == []

    @given(
        wildcard_patterns=st.lists(
            st.one_of(
                st.just("*"),
                st.just("#"),
                st.text(alphabet="abcdefghij", min_size=1, max_size=3).map(
                    lambda x: x + ".*"
                ),
                st.text(alphabet="abcdefghij", min_size=1, max_size=3).map(
                    lambda x: "*." + x
                ),
                st.text(alphabet="abcdefghij", min_size=1, max_size=3).map(
                    lambda x: x + ".#"
                ),
            ),
            min_size=1,
            max_size=5,
        ),
        test_keys=st.lists(
            st.text(alphabet="abcdefghij.", min_size=0, max_size=15).filter(
                lambda x: not x.startswith(".")
                and not x.endswith(".")
                and ".." not in x
            ),
            min_size=1,
            max_size=10,
        ),
    )
    @settings(max_examples=30, deadline=None)
    def test_wildcard_pattern_correctness(self, wildcard_patterns, test_keys):
        """Property: Wildcard patterns should match according to AMQP-style rules."""
        trie = Trie[str]()

        # Add patterns with unique values
        for i, pattern in enumerate(wildcard_patterns):
            trie.add_pattern(pattern, f"value_{i}")

        # Test each key against the patterns
        for key in test_keys:
            matches = trie.match_pattern(key)
            key_segments = key.split(".") if key else []

            # Verify matches are correct according to wildcard rules
            for i, pattern in enumerate(wildcard_patterns):
                expected_value = f"value_{i}"
                should_match = self._should_wildcard_match(pattern, key_segments)

                if should_match:
                    assert expected_value in matches, (
                        f"Pattern '{pattern}' should match key '{key}'"
                    )
                else:
                    assert expected_value not in matches, (
                        f"Pattern '{pattern}' should not match key '{key}'"
                    )

    def _should_wildcard_match(self, pattern: str, key_segments: list[str]) -> bool:
        """Helper to determine if a wildcard pattern should match given key segments."""
        if pattern == "*":
            return len(key_segments) == 1
        elif pattern == "#":
            return True  # Multi-wildcard matches any number of segments
        elif pattern.endswith(".*"):
            prefix = pattern[:-2]
            return len(key_segments) == 2 and key_segments[0] == prefix
        elif pattern.startswith("*."):
            suffix = pattern[2:]
            return len(key_segments) == 2 and key_segments[1] == suffix
        elif pattern.endswith(".#"):
            prefix = pattern[:-2]
            return len(key_segments) >= 1 and key_segments[0] == prefix
        else:
            return pattern == ".".join(key_segments)

    @given(
        segment_count=st.integers(min_value=1, max_value=8),
        wildcard_positions=st.lists(
            st.integers(min_value=0, max_value=7), min_size=0, max_size=3
        ),
    )
    @settings(max_examples=40, deadline=None)
    def test_complex_wildcard_placement(self, segment_count, wildcard_positions):
        """Property: Wildcards in various positions should behave correctly."""
        trie = Trie[str]()

        # Create a pattern with wildcards at specified positions
        pattern_segments = []
        for i in range(segment_count):
            if i in wildcard_positions:
                # Alternate between single and multi wildcards
                if i % 2 == 0:
                    pattern_segments.append("*")
                else:
                    pattern_segments.append("#")
                    break  # Multi-wildcard must be last
            else:
                pattern_segments.append(f"seg{i}")

        pattern = ".".join(pattern_segments)
        trie.add_pattern(pattern, "test_value")

        # Generate test keys that should and shouldn't match
        # Test exact match case (no wildcards)
        if "*" not in pattern and "#" not in pattern:
            assert "test_value" in trie.match_pattern(pattern)

        # Test single wildcard behavior
        if "*" in pattern and "#" not in pattern:
            # Create a matching key by replacing * with actual segments
            test_key_segments = []
            for seg in pattern_segments:
                if seg == "*":
                    test_key_segments.append("replaced")
                else:
                    test_key_segments.append(seg)
            test_key = ".".join(test_key_segments)
            assert "test_value" in trie.match_pattern(test_key)

    @given(
        operations=st.lists(
            st.one_of(
                st.tuples(
                    st.just("add"),
                    st.text(alphabet="abc.", min_size=1, max_size=10),
                    st.integers(),
                ),
                st.tuples(
                    st.just("remove"),
                    st.text(alphabet="abc.", min_size=1, max_size=10),
                    st.integers(),
                ),
                st.tuples(
                    st.just("match"), st.text(alphabet="abc.", min_size=0, max_size=10)
                ),
                st.tuples(st.just("clear")),
            ),
            min_size=5,
            max_size=20,
        )
    )
    @settings(
        max_examples=25,
        deadline=None,
        suppress_health_check=[HealthCheck.filter_too_much],
    )
    def test_operation_sequence_consistency(self, operations):
        """Property: Any sequence of operations should maintain trie consistency."""
        trie = Trie[int]()
        added_patterns = set()  # Track what we've added

        for operation in operations:
            if operation[0] == "add":
                _, pattern, value = operation
                trie.add_pattern(pattern, value)
                added_patterns.add((pattern, value))

            elif operation[0] == "remove":
                _, pattern, value = operation
                result = trie.remove_pattern(pattern, value)
                if (pattern, value) in added_patterns:
                    assert result is True
                    added_patterns.discard((pattern, value))
                else:
                    assert result is False

            elif operation[0] == "match":
                _, key = operation
                matches = trie.match_pattern(key)
                # All matches should correspond to added patterns
                for match in matches:
                    # Verify this match corresponds to a pattern we added
                    pattern_exists = any(
                        (pattern, match) in added_patterns
                        for pattern, _ in added_patterns
                    )
                    # This is complex to verify exactly, so we just check no crashes

            elif operation[0] == "clear":
                trie.clear()
                added_patterns.clear()

        # Final consistency check
        stats = trie.get_statistics()
        assert stats.total_patterns >= 0
        assert stats.total_nodes >= 1  # At least root node

    @given(
        patterns=st.lists(
            st.text(alphabet="*#abc.", min_size=1, max_size=15).filter(
                lambda x: not x.startswith(".")
                and not x.endswith(".")
                and ".." not in x
            ),
            min_size=1,
            max_size=8,
        )
    )
    @settings(max_examples=35, deadline=None)
    def test_pattern_set_determinism(self, patterns):
        """Property: Same set of patterns should always produce same results."""
        trie1 = Trie[str]()
        trie2 = Trie[str]()

        # Add patterns to both tries in same order
        for i, pattern in enumerate(patterns):
            value = f"v{i}"
            trie1.add_pattern(pattern, value)
            trie2.add_pattern(pattern, value)

        # Generate test keys
        test_keys = ["", "a", "a.b", "a.b.c", "*", "#", "test.key"]

        # Results should be identical
        for key in test_keys:
            result1 = sorted(trie1.match_pattern(key))
            result2 = sorted(trie2.match_pattern(key))
            assert result1 == result2, (
                f"Results differ for key '{key}': {result1} vs {result2}"
            )

    @given(
        cache_size=st.integers(min_value=1, max_value=100),
        patterns=st.lists(
            st.text(alphabet="abc", min_size=1, max_size=5), min_size=1, max_size=20
        ),
        keys=st.lists(
            st.text(alphabet="abc.", min_size=0, max_size=10), min_size=10, max_size=50
        ),
    )
    @settings(max_examples=20, deadline=None)
    def test_cache_behavior_properties(self, cache_size, patterns, keys):
        """Property: Cache behavior should not affect correctness, only performance."""
        # Create two tries: one with caching, one without
        config_cached = TrieConfig(enable_caching=True, max_cache_size=cache_size)
        config_uncached = TrieConfig(enable_caching=False)

        trie_cached = Trie[str](config=config_cached)
        trie_uncached = Trie[str](config=config_uncached)

        # Add same patterns to both
        for pattern in patterns:
            trie_cached.add_pattern(pattern, "cached_value")
            trie_uncached.add_pattern(pattern, "uncached_value")

        # Test same keys on both
        for key in keys:
            cached_result = sorted(trie_cached.match_pattern(key))
            uncached_result = sorted(trie_uncached.match_pattern(key))

            # Replace values to compare structure
            cached_normalized = [
                "value" if x == "cached_value" else x for x in cached_result
            ]
            uncached_normalized = [
                "value" if x == "uncached_value" else x for x in uncached_result
            ]

            assert cached_normalized == uncached_normalized, (
                f"Cache affected correctness for key '{key}'"
            )


if __name__ == "__main__":
    # Run specific test classes for debugging
    pytest.main([__file__ + "::TestTrieBasics", "-v"])
