"""
Centralized high-performance trie data structure for MPREG.

This module provides a generic trie implementation optimized for pattern matching
with wildcard support. Originally extracted from the topic exchange system but
designed to be reusable across different MPREG components.

Key features:
- Generic pattern matching with configurable wildcards
- Thread-safe operations with RLock
- Performance optimization with caching and statistics tracking
- Memory-efficient design with slots-based dataclasses
- Support for hierarchical pattern matching (e.g., AMQP-style topic routing)
- Comprehensive metrics and monitoring

Wildcard Support:
- Single-level wildcard: matches exactly one segment (default: '*')
- Multi-level wildcard: matches zero or more segments (default: '#')
- Configurable separators (default: '.')

Examples:
    >>> trie = Trie()
    >>> trie.add_pattern("user.*.login", "subscription_1")
    >>> trie.add_pattern("user.#", "subscription_2")
    >>> matches = trie.match_pattern("user.123.login")
    >>> # Returns: ["subscription_1", "subscription_2"]
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from threading import RLock
from typing import TypeVar

from mpreg.datastructures.type_aliases import (
    CacheSize,
    MatchCount,
    PatternString,
    SubscriptionId,
)

# Generic type for values stored in the trie
T = TypeVar("T")

# Type aliases for trie semantics
type TriePattern = str  # Hierarchical pattern with wildcards (e.g., "user.*.login")
type TrieSegment = str  # Individual segment of a pattern (e.g., "user", "*", "login")
type TrieKey = str  # Key being matched against patterns
type TrieCacheKey = str  # Cache key for performance optimization


@dataclass(frozen=True, slots=True)
class TrieStatistics:
    """Performance statistics for trie operations."""

    cache_hits: int
    cache_misses: int
    cache_hit_ratio: float
    cached_patterns: CacheSize
    total_nodes: int
    total_patterns: int
    average_pattern_depth: float
    memory_usage_estimate_bytes: int


@dataclass(slots=True)
class TrieNode[T]:
    """
    A node in the generic trie data structure.

    Supports hierarchical pattern matching with configurable wildcards.
    Thread-safe and memory-efficient implementation.
    """

    # Exact segment matches at this level
    children: dict[TrieSegment, TrieNode[T]] = field(default_factory=dict)

    # Wildcard matches
    single_wildcard: TrieNode[T] | None = None  # Matches one segment
    multi_wildcard: TrieNode[T] | None = None  # Matches zero or more segments

    # Values that terminate at this node
    values: set[T] = field(default_factory=set)

    # Performance tracking
    match_count: MatchCount = 0
    last_accessed: float = field(default_factory=time.time)


@dataclass(slots=True)
class TrieConfig:
    """Configuration for trie behavior and performance tuning."""

    # Wildcard configuration
    single_wildcard_symbol: str = "*"  # Symbol for single-level wildcard
    multi_wildcard_symbol: str = "#"  # Symbol for multi-level wildcard
    segment_separator: str = "."  # Separator between pattern segments

    # Performance tuning
    enable_caching: bool = True
    max_cache_size: int = 10000  # Maximum cached pattern results
    cache_cleanup_threshold: int = 1000  # Remove oldest entries when exceeded

    # Threading
    thread_safe: bool = True  # Enable thread-safe operations

    # Memory optimization
    enable_node_cleanup: bool = False  # Enable automatic cleanup of empty nodes


@dataclass(slots=True)
class Trie[T]:
    """
    Generic high-performance trie for hierarchical pattern matching.

    Supports configurable wildcards and separators, making it suitable for
    various use cases including topic routing, file path matching, and
    hierarchical subscription systems.

    Thread-safe operations with performance optimization through caching
    and comprehensive statistics tracking.
    """

    config: TrieConfig = field(default_factory=TrieConfig)
    root: TrieNode[T] = field(default_factory=TrieNode)

    # Performance optimization
    pattern_cache: dict[TrieCacheKey, list[T]] = field(default_factory=dict)
    cache_hits: int = 0
    cache_misses: int = 0
    total_patterns_added: int = 0

    # Thread safety
    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        """Initialize trie state after dataclass creation."""
        if not self.config.thread_safe:
            # Use a dummy lock for non-thread-safe mode
            self._lock = type(
                "DummyLock",
                (),
                {
                    "__enter__": lambda self: None,
                    "__exit__": lambda self, *args: None,
                    "acquire": lambda self: None,
                    "release": lambda self: None,
                },
            )()

    def add_pattern(self, pattern: TriePattern, value: T) -> None:
        """
        Add a pattern to the trie with an associated value.

        Args:
            pattern: Hierarchical pattern with optional wildcards
            value: Value to associate with this pattern
        """
        with self._lock:
            # Handle empty pattern as zero segments, not one empty segment
            if pattern == "":
                segments = []
            else:
                segments = pattern.split(self.config.segment_separator)
            current = self.root

            for segment in segments:
                if segment == self.config.single_wildcard_symbol:
                    # Single-level wildcard
                    if current.single_wildcard is None:
                        current.single_wildcard = TrieNode[T]()
                    current = current.single_wildcard
                elif segment == self.config.multi_wildcard_symbol:
                    # Multi-level wildcard
                    if current.multi_wildcard is None:
                        current.multi_wildcard = TrieNode[T]()
                    current = current.multi_wildcard
                else:
                    # Exact match
                    if segment not in current.children:
                        current.children[segment] = TrieNode[T]()
                    current = current.children[segment]

            current.values.add(value)
            self.total_patterns_added += 1

            # Clear cache when patterns change
            if self.config.enable_caching:
                self.pattern_cache.clear()

    def remove_pattern(self, pattern: TriePattern, value: T) -> bool:
        """
        Remove a pattern and its associated value from the trie.

        Args:
            pattern: Pattern to remove
            value: Specific value to remove from the pattern

        Returns:
            True if the pattern/value was found and removed, False otherwise
        """
        with self._lock:
            # Handle empty pattern as zero segments, not one empty segment
            if pattern == "":
                segments = []
            else:
                segments = pattern.split(self.config.segment_separator)
            path: list[tuple[TrieNode[T], TrieSegment]] = []
            current = self.root

            # Navigate to the target node
            for segment in segments:
                path.append((current, segment))
                if segment == self.config.single_wildcard_symbol:
                    if current.single_wildcard is None:
                        return False  # Pattern not found
                    current = current.single_wildcard
                elif segment == self.config.multi_wildcard_symbol:
                    if current.multi_wildcard is None:
                        return False  # Pattern not found
                    current = current.multi_wildcard
                else:
                    if segment not in current.children:
                        return False  # Pattern not found
                    current = current.children[segment]

            # Remove value from target node
            if value not in current.values:
                return False

            current.values.discard(value)

            # Optional: Clean up empty nodes if enabled
            if self.config.enable_node_cleanup:
                self._cleanup_empty_nodes(path, current)

            # Clear cache when patterns change
            if self.config.enable_caching:
                self.pattern_cache.clear()

            return True

    def match_pattern(self, key: TrieKey) -> list[T]:
        """
        Find all values associated with patterns that match the given key.

        Args:
            key: Key to match against stored patterns

        Returns:
            List of values from all matching patterns
        """
        # Check cache first
        if self.config.enable_caching and key in self.pattern_cache:
            self.cache_hits += 1
            return self.pattern_cache[key]

        self.cache_misses += 1

        with self._lock:
            # Handle empty key as zero segments, not one empty segment
            segments = [] if key == "" else key.split(self.config.segment_separator)
            matches: set[T] = set()

            # Recursive matching
            self._match_recursive(self.root, segments, 0, matches)

            result = list(matches)

            # Cache the result
            if self.config.enable_caching:
                self.pattern_cache[key] = result
                self._manage_cache_size()

            return result

    def get_patterns_for_value(self, value: T) -> list[TriePattern]:
        """
        Find all patterns associated with a specific value.

        Args:
            value: Value to search for

        Returns:
            List of patterns that contain this value
        """
        with self._lock:
            patterns: list[TriePattern] = []
            self._collect_patterns_for_value(self.root, [], value, patterns)
            return patterns

    def get_all_values(self) -> set[T]:
        """Get all values stored in the trie."""
        with self._lock:
            values: set[T] = set()
            self._collect_all_values(self.root, values)
            return values

    def get_statistics(self) -> TrieStatistics:
        """Get comprehensive performance and usage statistics."""
        with self._lock:
            cache_hit_ratio = (
                self.cache_hits / (self.cache_hits + self.cache_misses)
                if (self.cache_hits + self.cache_misses) > 0
                else 0.0
            )

            total_nodes = self._count_nodes(self.root)
            all_patterns = self._collect_all_patterns(self.root, [])
            avg_depth = (
                sum(
                    len(pattern.split(self.config.segment_separator))
                    for pattern in all_patterns
                )
                / len(all_patterns)
                if all_patterns
                else 0.0
            )

            # Rough memory estimate
            memory_estimate = (
                total_nodes * 200  # Estimated bytes per node
                + len(self.pattern_cache) * 100  # Estimated bytes per cache entry
                + self.total_patterns_added * 50  # Estimated bytes per pattern
            )

            return TrieStatistics(
                cache_hits=self.cache_hits,
                cache_misses=self.cache_misses,
                cache_hit_ratio=cache_hit_ratio,
                cached_patterns=len(self.pattern_cache),
                total_nodes=total_nodes,
                total_patterns=len(all_patterns),
                average_pattern_depth=avg_depth,
                memory_usage_estimate_bytes=memory_estimate,
            )

    def clear(self) -> None:
        """Clear all patterns and values from the trie."""
        with self._lock:
            self.root = TrieNode[T]()
            self.pattern_cache.clear()
            self.cache_hits = 0
            self.cache_misses = 0
            self.total_patterns_added = 0

    def _match_recursive(
        self,
        node: TrieNode[T],
        segments: list[TrieSegment],
        index: int,
        matches: set[T],
    ) -> None:
        """Recursively match key segments against trie patterns."""

        # Update performance tracking
        node.last_accessed = time.time()
        node.match_count += 1

        # If we've consumed all segments, collect values
        if index >= len(segments):
            matches.update(node.values)
            # Multi-wildcard can match zero segments
            if node.multi_wildcard:
                matches.update(node.multi_wildcard.values)
            return

        current_segment = segments[index]

        # Try exact match
        if current_segment in node.children:
            self._match_recursive(
                node.children[current_segment], segments, index + 1, matches
            )

        # Try single wildcard (matches exactly one segment)
        if node.single_wildcard:
            self._match_recursive(node.single_wildcard, segments, index + 1, matches)

        # Try multi-wildcard (matches zero or more segments)
        if node.multi_wildcard:
            # Multi-wildcard can consume 0 or more segments
            # Try consuming 0 segments first (continue with current position)
            self._match_recursive(node.multi_wildcard, segments, index, matches)

            # Then try consuming 1, 2, 3... segments
            for i in range(index + 1, len(segments) + 1):
                self._match_recursive(node.multi_wildcard, segments, i, matches)

    def _cleanup_empty_nodes(
        self, path: list[tuple[TrieNode[T], TrieSegment]], current: TrieNode[T]
    ) -> None:
        """Clean up empty nodes after pattern removal (optional optimization)."""
        # This is a complex optimization that requires careful handling
        # to avoid breaking other patterns. For now, we skip implementation
        # to maintain correctness. Could be implemented in future if needed.
        pass

    def _manage_cache_size(self) -> None:
        """Manage cache size to prevent unlimited growth."""
        if len(self.pattern_cache) > self.config.max_cache_size:
            # Remove oldest entries (simple LRU approximation)
            old_keys = list(self.pattern_cache.keys())[
                : self.config.cache_cleanup_threshold
            ]
            for key in old_keys:
                del self.pattern_cache[key]

    def _count_nodes(self, node: TrieNode[T]) -> int:
        """Count total nodes in the trie."""
        count = 1
        for child in node.children.values():
            count += self._count_nodes(child)
        if node.single_wildcard:
            count += self._count_nodes(node.single_wildcard)
        if node.multi_wildcard:
            count += self._count_nodes(node.multi_wildcard)
        return count

    def _collect_patterns_for_value(
        self,
        node: TrieNode[T],
        current_path: list[str],
        target_value: T,
        patterns: list[TriePattern],
    ) -> None:
        """Recursively collect all patterns that contain a specific value."""
        if target_value in node.values:
            pattern = self.config.segment_separator.join(current_path)
            patterns.append(pattern or self.config.segment_separator)

        # Traverse exact matches
        for segment, child in node.children.items():
            self._collect_patterns_for_value(
                child, current_path + [segment], target_value, patterns
            )

        # Traverse wildcards
        if node.single_wildcard:
            self._collect_patterns_for_value(
                node.single_wildcard,
                current_path + [self.config.single_wildcard_symbol],
                target_value,
                patterns,
            )

        if node.multi_wildcard:
            self._collect_patterns_for_value(
                node.multi_wildcard,
                current_path + [self.config.multi_wildcard_symbol],
                target_value,
                patterns,
            )

    def _collect_all_values(self, node: TrieNode[T], values: set[T]) -> None:
        """Recursively collect all values in the trie."""
        values.update(node.values)

        for child in node.children.values():
            self._collect_all_values(child, values)

        if node.single_wildcard:
            self._collect_all_values(node.single_wildcard, values)

        if node.multi_wildcard:
            self._collect_all_values(node.multi_wildcard, values)

    def _collect_all_patterns(
        self, node: TrieNode[T], current_path: list[str]
    ) -> list[TriePattern]:
        """Recursively collect all patterns in the trie."""
        patterns: list[TriePattern] = []

        if node.values:
            pattern = self.config.segment_separator.join(current_path)
            patterns.append(pattern or self.config.segment_separator)

        # Traverse exact matches
        for segment, child in node.children.items():
            patterns.extend(self._collect_all_patterns(child, current_path + [segment]))

        # Traverse wildcards
        if node.single_wildcard:
            patterns.extend(
                self._collect_all_patterns(
                    node.single_wildcard,
                    current_path + [self.config.single_wildcard_symbol],
                )
            )

        if node.multi_wildcard:
            patterns.extend(
                self._collect_all_patterns(
                    node.multi_wildcard,
                    current_path + [self.config.multi_wildcard_symbol],
                )
            )

        return patterns


# Specialized trie for topic routing (backward compatibility)
class TopicTrie(Trie[SubscriptionId]):
    """
    Specialized trie for topic pattern matching with subscription IDs.

    Provides backward compatibility with the original TopicTrie implementation
    while leveraging the generic trie infrastructure.
    """

    def __init__(
        self,
        enable_caching: bool = True,
        max_cache_size: int = 10000,
        thread_safe: bool = True,
    ):
        config = TrieConfig(
            single_wildcard_symbol="*",
            multi_wildcard_symbol="#",
            segment_separator=".",
            enable_caching=enable_caching,
            max_cache_size=max_cache_size,
            thread_safe=thread_safe,
        )
        super().__init__(config=config)

    def add_pattern(
        self, pattern: PatternString, subscription_id: SubscriptionId
    ) -> None:
        """Add a topic pattern with subscription ID."""
        super().add_pattern(pattern, subscription_id)

    def remove_pattern(
        self, pattern: PatternString, subscription_id: SubscriptionId
    ) -> bool:
        """Remove a topic pattern and subscription ID."""
        return super().remove_pattern(pattern, subscription_id)

    def match_topic(self, topic: str) -> list[SubscriptionId]:
        """Find all subscription IDs that match the given topic."""
        return super().match_pattern(topic)

    def get_stats(self) -> TrieStatistics:
        """Get performance statistics (backward compatibility method name)."""
        return super().get_statistics()


# Factory functions for common use cases
def create_topic_trie(
    enable_caching: bool = True, max_cache_size: int = 10000, thread_safe: bool = True
) -> TopicTrie:
    """Create a trie optimized for topic routing."""
    return TopicTrie(
        enable_caching=enable_caching,
        max_cache_size=max_cache_size,
        thread_safe=thread_safe,
    )


def create_generic_trie[T](
    value_type: type[T],
    single_wildcard: str = "*",
    multi_wildcard: str = "#",
    separator: str = ".",
    enable_caching: bool = True,
    thread_safe: bool = True,
) -> Trie[T]:
    """Create a generic trie with custom configuration."""
    config = TrieConfig(
        single_wildcard_symbol=single_wildcard,
        multi_wildcard_symbol=multi_wildcard,
        segment_separator=separator,
        enable_caching=enable_caching,
        thread_safe=thread_safe,
    )
    return Trie[T](config=config)
