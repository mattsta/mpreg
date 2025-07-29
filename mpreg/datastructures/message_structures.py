"""
Centralized message datastructures for MPREG.

This module consolidates message-related datastructures across the codebase
into a unified, well-tested implementation.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from hypothesis import strategies as st


class MessagePriority(Enum):
    """Message priority levels."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class MessageStatus(Enum):
    """General message status enumeration."""

    PENDING = "pending"
    PROCESSING = "processing"
    DELIVERED = "delivered"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass(frozen=True, slots=True)
class MessageId:
    """
    Unified message ID implementation.

    Consolidates all MessageId implementations across MPREG into a single,
    immutable datastructure with proper validation and comparison.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: float = field(default_factory=time.time)
    source_node: str = ""

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("Message ID cannot be empty")
        if self.created_at < 0:
            raise ValueError("Created at timestamp cannot be negative")

    @classmethod
    def generate(cls, source_node: str = "") -> MessageId:
        """Generate new message ID with current timestamp."""
        return cls(source_node=source_node)

    @classmethod
    def from_string(cls, id_str: str, source_node: str = "") -> MessageId:
        """Create message ID from string."""
        return cls(id=id_str, source_node=source_node)

    def age_seconds(self) -> float:
        """Get age of message ID in seconds."""
        return time.time() - self.created_at

    def __str__(self) -> str:
        return self.id

    def __hash__(self) -> int:
        return hash(self.id)


@dataclass(frozen=True, slots=True)
class MessageHeaders:
    """
    Unified message headers implementation.

    Encapsulates message metadata and headers in a type-safe way
    instead of using raw dictionaries.
    """

    _headers: frozenset[tuple[str, str]] = field(default_factory=frozenset)

    @classmethod
    def empty(cls) -> MessageHeaders:
        """Create empty message headers."""
        return cls()

    @classmethod
    def from_dict(cls, headers_dict: Mapping[str, str]) -> MessageHeaders:
        """Create headers from dictionary."""
        return cls(_headers=frozenset(headers_dict.items()))

    def to_dict(self) -> dict[str, str]:
        """Convert headers to dictionary."""
        return dict(self._headers)

    def get(self, key: str, default: str = "") -> str:
        """Get header value by key."""
        for header_key, header_value in self._headers:
            if header_key == key:
                return header_value
        return default

    def has(self, key: str) -> bool:
        """Check if header key exists."""
        return any(header_key == key for header_key, _ in self._headers)

    def with_header(self, key: str, value: str) -> MessageHeaders:
        """Create new headers with additional header."""
        new_headers = set(self._headers)
        # Remove existing header with same key
        new_headers = {(k, v) for k, v in new_headers if k != key}
        new_headers.add((key, value))
        return MessageHeaders(_headers=frozenset(new_headers))

    def without_header(self, key: str) -> MessageHeaders:
        """Create new headers without specified header."""
        new_headers = {(k, v) for k, v in self._headers if k != key}
        return MessageHeaders(_headers=frozenset(new_headers))

    def keys(self) -> frozenset[str]:
        """Get all header keys."""
        return frozenset(key for key, _ in self._headers)

    def values(self) -> frozenset[str]:
        """Get all header values."""
        return frozenset(value for _, value in self._headers)

    def items(self) -> frozenset[tuple[str, str]]:
        """Get all header items."""
        return self._headers

    def __len__(self) -> int:
        return len(self._headers)

    def __contains__(self, key: str) -> bool:
        return self.has(key)

    def __getitem__(self, key: str) -> str:
        """Get header value by key, raises KeyError if not found."""
        for header_key, header_value in self._headers:
            if header_key == key:
                return header_value
        raise KeyError(f"Header '{key}' not found")


@dataclass(frozen=True, slots=True)
class BaseMessage:
    """
    Unified base message implementation.

    Consolidates common message functionality across MPREG into a single
    base class that can be extended for specific message types.
    """

    message_id: MessageId = field(default_factory=MessageId.generate)
    timestamp: float = field(default_factory=time.time)
    priority: MessagePriority = MessagePriority.NORMAL
    status: MessageStatus = MessageStatus.PENDING
    headers: MessageHeaders = field(default_factory=MessageHeaders.empty)
    payload: Any = None
    source: str = ""
    destination: str = ""

    def __post_init__(self) -> None:
        if self.timestamp < 0:
            raise ValueError("Timestamp cannot be negative")

    def age_seconds(self) -> float:
        """Get age of message in seconds."""
        return time.time() - self.timestamp

    def with_status(self, status: MessageStatus) -> BaseMessage:
        """Create new message with updated status."""
        return BaseMessage(
            message_id=self.message_id,
            timestamp=self.timestamp,
            priority=self.priority,
            status=status,
            headers=self.headers,
            payload=self.payload,
            source=self.source,
            destination=self.destination,
        )

    def with_header(self, key: str, value: str) -> BaseMessage:
        """Create new message with additional header."""
        return BaseMessage(
            message_id=self.message_id,
            timestamp=self.timestamp,
            priority=self.priority,
            status=self.status,
            headers=self.headers.with_header(key, value),
            payload=self.payload,
            source=self.source,
            destination=self.destination,
        )

    def with_payload(self, payload: Any) -> BaseMessage:
        """Create new message with updated payload."""
        return BaseMessage(
            message_id=self.message_id,
            timestamp=self.timestamp,
            priority=self.priority,
            status=self.status,
            headers=self.headers,
            payload=payload,
            source=self.source,
            destination=self.destination,
        )

    def is_expired(self, timeout_seconds: float) -> bool:
        """Check if message has expired based on timeout."""
        return self.age_seconds() > timeout_seconds

    def is_high_priority(self) -> bool:
        """Check if message has high or critical priority."""
        return self.priority in (MessagePriority.HIGH, MessagePriority.CRITICAL)


@dataclass(frozen=True, slots=True)
class QueuedMessage(BaseMessage):
    """
    Queued message with queue-specific metadata.

    Extends BaseMessage for messages that are placed in queues
    with additional queue-specific tracking information.
    """

    queue_name: str = ""
    visibility_timeout: float = 30.0
    retry_count: int = 0
    max_retries: int = 3
    delivery_tag: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.visibility_timeout < 0:
            raise ValueError("Visibility timeout cannot be negative")
        if self.retry_count < 0:
            raise ValueError("Retry count cannot be negative")
        if self.max_retries < 0:
            raise ValueError("Max retries cannot be negative")

    def can_retry(self) -> bool:
        """Check if message can be retried."""
        return self.retry_count < self.max_retries

    def with_retry(self) -> QueuedMessage:
        """Create new message with incremented retry count."""
        return QueuedMessage(
            message_id=self.message_id,
            timestamp=self.timestamp,
            priority=self.priority,
            status=self.status,
            headers=self.headers,
            payload=self.payload,
            source=self.source,
            destination=self.destination,
            queue_name=self.queue_name,
            visibility_timeout=self.visibility_timeout,
            retry_count=self.retry_count + 1,
            max_retries=self.max_retries,
            delivery_tag=self.delivery_tag,
        )

    def is_visible(self, visibility_start: float) -> bool:
        """Check if message is visible (not in visibility timeout)."""
        return time.time() >= (visibility_start + self.visibility_timeout)


# Hypothesis strategies for property-based testing
def message_id_strategy() -> st.SearchStrategy[MessageId]:
    """Generate valid MessageId instances for testing."""
    return st.builds(
        MessageId,
        id=st.text(min_size=1, max_size=100),
        created_at=st.floats(min_value=0, max_value=time.time() + 86400),
        source_node=st.text(max_size=50),
    )


def message_headers_strategy() -> st.SearchStrategy[MessageHeaders]:
    """Generate valid MessageHeaders instances for testing."""
    return st.dictionaries(
        st.text(min_size=1, max_size=50), st.text(max_size=200), max_size=20
    ).map(MessageHeaders.from_dict)


def base_message_strategy() -> st.SearchStrategy[BaseMessage]:
    """Generate valid BaseMessage instances for testing."""
    return st.builds(
        BaseMessage,
        message_id=message_id_strategy(),
        timestamp=st.floats(min_value=0, max_value=time.time() + 86400),
        priority=st.sampled_from(MessagePriority),
        status=st.sampled_from(MessageStatus),
        headers=message_headers_strategy(),
        payload=st.one_of(
            st.none(),
            st.integers(),
            st.text(),
            st.lists(st.integers(), max_size=10),
            st.dictionaries(st.text(max_size=10), st.integers(), max_size=5),
        ),
        source=st.text(max_size=100),
        destination=st.text(max_size=100),
    )


def queued_message_strategy() -> st.SearchStrategy[QueuedMessage]:
    """Generate valid QueuedMessage instances for testing."""
    return st.builds(
        QueuedMessage,
        message_id=message_id_strategy(),
        timestamp=st.floats(min_value=0, max_value=time.time() + 86400),
        priority=st.sampled_from(MessagePriority),
        status=st.sampled_from(MessageStatus),
        headers=message_headers_strategy(),
        payload=st.one_of(
            st.none(), st.integers(), st.text(), st.lists(st.integers(), max_size=10)
        ),
        source=st.text(max_size=100),
        destination=st.text(max_size=100),
        queue_name=st.text(min_size=1, max_size=100),
        visibility_timeout=st.floats(min_value=0, max_value=3600),
        retry_count=st.integers(min_value=0, max_value=10),
        max_retries=st.integers(min_value=0, max_value=20),
        delivery_tag=st.text(max_size=100),
    )
