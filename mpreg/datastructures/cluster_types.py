"""
Type-safe dataclasses for Cluster management.

This module provides well-typed interfaces for cluster state management,
replacing primitive types with semantic dataclasses for better encapsulation.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from .federated_types import (
    AdvertisedURLs,
    FederatedAnnouncementTracker,
)
from .type_aliases import ClusterID, FunctionName, NodeURL, ResourceName

# Type aliases for cluster-specific collections
ServerSet = set[NodeURL]
"""Collection of server URLs in the cluster."""

ResourceServerMapping = dict[frozenset[ResourceName], ServerSet]
"""Maps resource requirements to servers that can fulfill them."""

FunctionResourceMapping = dict[FunctionName, ResourceServerMapping]
"""Maps function names to their resource requirements and available servers."""


@dataclass(frozen=True, slots=True)
class ClusterConfiguration:
    """
    Immutable configuration for a cluster.

    This separates cluster configuration from mutable state.
    """

    cluster_id: ClusterID
    advertised_urls: AdvertisedURLs
    local_url: NodeURL
    dead_peer_timeout_seconds: float = 30.0

    @classmethod
    def create(
        cls,
        cluster_id: str,
        advertised_urls: tuple[str, ...],
        local_url: str = "",
        dead_peer_timeout_seconds: float = 30.0,
    ) -> ClusterConfiguration:
        """Create cluster configuration from primitive types."""
        return cls(
            cluster_id=cluster_id,
            advertised_urls=tuple(url for url in advertised_urls),
            local_url=local_url,
            dead_peer_timeout_seconds=dead_peer_timeout_seconds,
        )


@dataclass(slots=True)
class ClusterFunctionRegistry:
    """
    Manages the cluster's function-to-server mappings.

    This encapsulates the complex funtimes mapping with better typing
    and clearer interfaces.
    """

    # Core function registry: function -> {resource_set -> server_set}
    _function_mappings: FunctionResourceMapping = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(set))
    )

    # All known servers in the cluster
    known_servers: ServerSet = field(default_factory=set)

    def add_function_capability(
        self,
        function_name: FunctionName,
        resource_requirements: frozenset[ResourceName],
        server_url: NodeURL,
    ) -> None:
        """Add a function capability for a specific server."""
        # Convert the defaultdict to a regular dict for type safety
        if function_name not in self._function_mappings:
            self._function_mappings[function_name] = defaultdict(set)

        self._function_mappings[function_name][resource_requirements].add(server_url)
        self.known_servers.add(server_url)

    def remove_server_functions(self, server_url: NodeURL) -> None:
        """Remove all functions provided by a specific server."""
        functions_to_clean = []

        for function_name, resource_mapping in self._function_mappings.items():
            resources_to_clean = []

            for resource_set, server_set in resource_mapping.items():
                server_set.discard(server_url)
                if not server_set:  # No servers left for this resource set
                    resources_to_clean.append(resource_set)

            # Clean up empty resource sets
            for resource_set in resources_to_clean:
                del resource_mapping[resource_set]

            # Mark function for cleanup if no resource sets left
            if not resource_mapping:
                functions_to_clean.append(function_name)

        # Clean up empty functions
        for function_name in functions_to_clean:
            del self._function_mappings[function_name]

        self.known_servers.discard(server_url)

    def get_servers_for_function(
        self,
        function_name: FunctionName,
        resource_requirements: frozenset[ResourceName] | None = None,
    ) -> ServerSet:
        """Get servers that can handle a function with specific resource requirements."""
        if function_name not in self._function_mappings:
            return set()

        resource_mapping = self._function_mappings[function_name]

        if resource_requirements is None:
            # Return all servers that provide this function
            all_servers = set()
            for server_set in resource_mapping.values():
                all_servers.update(server_set)
            return all_servers

        return resource_mapping.get(resource_requirements, set()).copy()

    def has_function(self, function_name: FunctionName) -> bool:
        """Check if the cluster has any servers providing this function."""
        return function_name in self._function_mappings

    def get_all_functions(self) -> set[FunctionName]:
        """Get all function names available in the cluster."""
        return set(self._function_mappings.keys())

    @property
    def function_count(self) -> int:
        """Total number of unique functions in the cluster."""
        return len(self._function_mappings)

    @property
    def server_count(self) -> int:
        """Total number of known servers in the cluster."""
        return len(self.known_servers)


@dataclass(slots=True)
class ClusterState:
    """
    Complete cluster state management with proper typing.

    This replaces the monolithic Cluster dataclass with well-structured,
    type-safe components.
    """

    # Inline the ClusterConfiguration fields instead of nesting
    cluster_id: ClusterID
    advertised_urls: AdvertisedURLs
    local_url: NodeURL
    dead_peer_timeout_seconds: float = 30.0

    function_registry: ClusterFunctionRegistry = field(
        default_factory=ClusterFunctionRegistry
    )
    announcement_tracker: FederatedAnnouncementTracker = field(
        default_factory=FederatedAnnouncementTracker
    )

    # Runtime state (these would be populated by the server)
    waiting_for: dict[str, Any] = field(default_factory=dict, init=False)
    answers: dict[str, Any] = field(default_factory=dict, init=False)
    peer_connections: dict[str, Any] = field(default_factory=dict, init=False)
    peers_info: dict[str, Any] = field(default_factory=dict, init=False)

    @classmethod
    def create(
        cls,
        cluster_id: str,
        advertised_urls: tuple[str, ...],
        local_url: str = "",
        dead_peer_timeout_seconds: float = 30.0,
    ) -> ClusterState:
        """Create cluster state from primitive types."""
        return cls(
            cluster_id=cluster_id,
            advertised_urls=tuple(url for url in advertised_urls),
            local_url=local_url,
            dead_peer_timeout_seconds=dead_peer_timeout_seconds,
        )

    def add_function(
        self, function_name: str, resources: tuple[str, ...], server_url: str
    ) -> None:
        """Add a function capability (convenience method for primitive types)."""
        self.function_registry.add_function_capability(
            function_name, frozenset(r for r in resources), server_url
        )

    def get_servers_for_function(self, function_name: str) -> set[str]:
        """Get servers for a function (convenience method returning primitive types)."""
        servers = self.function_registry.get_servers_for_function(function_name)
        return {str(server) for server in servers}

    def mark_announcement_seen(self, announcement_id: str) -> None:
        """Mark an announcement as seen (convenience method for primitive types)."""
        import time

        self.announcement_tracker.mark_seen(announcement_id, time.time())

    def has_seen_announcement(self, announcement_id: str) -> bool:
        """Check if announcement was seen (convenience method for primitive types)."""
        return self.announcement_tracker.has_seen(announcement_id)

    def cleanup_expired_announcements(self) -> int:
        """Clean up expired announcements and return count removed."""
        import time

        return self.announcement_tracker.cleanup_expired(time.time())
