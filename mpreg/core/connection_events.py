"""
Connection Event Management System

This module provides a centralized event bus for connection state changes,
enabling different components to stay synchronized with peer connection status.

Architecture:
- ConnectionEventBus: Central event dispatcher
- ConnectionAwareComponent: Protocol for connection-aware components
- ConnectionEvent: Data class for connection state changes
"""

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from ..datastructures.type_aliases import NodeURL

logger = logging.getLogger(__name__)


class ConnectionEventType(Enum):
    """Types of connection events."""

    ESTABLISHED = "connection_established"
    LOST = "connection_lost"
    RECONNECTED = "connection_reconnected"


@dataclass
class ConnectionEvent:
    """Event data for connection state changes."""

    event_type: ConnectionEventType
    node_url: NodeURL
    timestamp: float
    local_node_url: NodeURL | None = None
    connection_metadata: dict | None = None

    @classmethod
    def established(
        cls, node_url: NodeURL, local_node_url: NodeURL | None = None
    ) -> "ConnectionEvent":
        """Create a connection established event."""
        return cls(
            event_type=ConnectionEventType.ESTABLISHED,
            node_url=node_url,
            timestamp=time.time(),
            local_node_url=local_node_url,
        )

    @classmethod
    def lost(
        cls, node_url: NodeURL, local_node_url: NodeURL | None = None
    ) -> "ConnectionEvent":
        """Create a connection lost event."""
        return cls(
            event_type=ConnectionEventType.LOST,
            node_url=node_url,
            timestamp=time.time(),
            local_node_url=local_node_url,
        )


class ConnectionAwareComponent(Protocol):
    """Protocol for components that need to be aware of connection changes."""

    def on_connection_established(self, event: ConnectionEvent) -> None:
        """Called when a new connection is established."""
        ...

    def on_connection_lost(self, event: ConnectionEvent) -> None:
        """Called when a connection is lost."""
        ...


class ConnectionEventBus:
    """Central event bus for connection state changes."""

    def __init__(self):
        self._subscribers: set[ConnectionAwareComponent] = set()
        self._event_history: list[ConnectionEvent] = []
        self._max_history = 100  # Keep last 100 events

    def subscribe(self, component: ConnectionAwareComponent) -> None:
        """Subscribe a component to connection events."""
        self._subscribers.add(component)
        logger.debug(
            f"Component {type(component).__name__} subscribed to connection events"
        )

    def unsubscribe(self, component: ConnectionAwareComponent) -> None:
        """Unsubscribe a component from connection events."""
        self._subscribers.discard(component)
        logger.debug(
            f"Component {type(component).__name__} unsubscribed from connection events"
        )

    def publish(self, event: ConnectionEvent) -> None:
        """Publish a connection event to all subscribers."""
        # Only log at debug level instead of printing to console
        logger.debug(
            f"Publishing connection event: {event.event_type.value} for {event.node_url} "
            f"to {len(self._subscribers)} subscribers"
        )

        # Store in history
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history.pop(0)

        # Notify all subscribers
        for (
            subscriber
        ) in self._subscribers.copy():  # Copy to avoid modification during iteration
            try:
                if event.event_type == ConnectionEventType.ESTABLISHED:
                    subscriber.on_connection_established(event)
                elif event.event_type == ConnectionEventType.LOST:
                    subscriber.on_connection_lost(event)
                else:
                    logger.warning(f"Unknown event type: {event.event_type}")

            except Exception as e:
                logger.error(
                    f"Error notifying subscriber {type(subscriber).__name__}: {e}"
                )

    def get_recent_events(self, limit: int = 10) -> list[ConnectionEvent]:
        """Get recent connection events."""
        return self._event_history[-limit:] if self._event_history else []

    def get_subscriber_count(self) -> int:
        """Get the number of subscribed components."""
        return len(self._subscribers)
