#!/usr/bin/env python3
"""
MPREG Federation Examples

This file contains practical, production-ready examples demonstrating
how to use the MPREG planet-scale federation system for real-world
applications.
"""

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from mpreg.federation.federation_consensus import (
    ConsensusManager,
    StateType,
    StateValue,
)
from mpreg.federation.federation_gossip import (
    GossipMessage,
    GossipMessageType,
    GossipProtocol,
    VectorClock,
)
from mpreg.federation.federation_graph import (
    GeographicCoordinate,
)
from mpreg.federation.federation_hubs import (
    HubCapabilities,
    HubTier,
)
from mpreg.federation.federation_membership import MembershipProtocol
from mpreg.federation.federation_registry import HubRegistrationInfo, HubRegistry


@dataclass(slots=True)
class DistributedDataStore:
    """
    Example: Distributed data store using MPREG federation.

    This demonstrates how to build a distributed key-value store
    with eventual consistency using the federation system.
    """

    node_id: str
    coordinates: GeographicCoordinate
    region: str

    # Fields initialized in __post_init__
    hub_registry: HubRegistry = field(init=False)
    gossip_protocol: GossipProtocol = field(init=False)
    consensus_manager: ConsensusManager = field(init=False)
    membership_protocol: MembershipProtocol = field(init=False)
    local_data: dict[str, Any] = field(default_factory=dict)
    data_versions: dict[str, int] = field(default_factory=dict)
    replication_factor: int = 3
    consistency_level: str = "eventual"

    def __post_init__(self):
        # Initialize federation components
        self.hub_registry = HubRegistry(self.node_id)
        self.gossip_protocol = GossipProtocol(self.node_id, self.hub_registry)
        self.consensus_manager = ConsensusManager(self.node_id, self.gossip_protocol)
        self.membership_protocol = MembershipProtocol(
            self.node_id, self.gossip_protocol, self.consensus_manager
        )

        # Local data storage and replication settings are now set via field defaults

        logger.info(f"Initialized distributed data store node {self.node_id}")

    async def start(self):
        """Start the distributed data store."""
        await self.gossip_protocol.start()
        await self.consensus_manager.start()
        await self.membership_protocol.start()

        # Register as a data store provider
        hub_info = HubRegistrationInfo(
            hub_id=self.node_id,
            hub_tier=HubTier.LOCAL,
            hub_capabilities=HubCapabilities(
                max_clusters=100,
                max_subscriptions=10000,
                bandwidth_mbps=1000,
                cpu_capacity=100.0,
            ),
            coordinates=self.coordinates,
            region=self.region,
        )
        await self.hub_registry.register_hub(hub_info)

        logger.info(f"Started distributed data store node {self.node_id}")

    async def stop(self):
        """Stop the distributed data store."""
        await self.membership_protocol.stop()
        await self.consensus_manager.stop()
        await self.gossip_protocol.stop()
        await self.hub_registry.deregister_hub(self.node_id)

        logger.info(f"Stopped distributed data store node {self.node_id}")

    async def put(self, key: str, value: Any, consistency: str = "eventual") -> bool:
        """
        Store a key-value pair with specified consistency level.

        Args:
            key: The key to store
            value: The value to store
            consistency: Consistency level ("eventual", "strong", "weak")

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Storing {key} = {value} with {consistency} consistency")

        # Create state value
        vector_clock = VectorClock()
        vector_clock.increment(self.node_id)
        state_value = StateValue(
            value=value,
            vector_clock=vector_clock,
            node_id=self.node_id,
            state_type=StateType.SIMPLE_VALUE,
        )

        if consistency == "strong":
            # Use consensus for strong consistency
            proposal_id = await self.consensus_manager.propose_state_change(
                key, state_value
            )
            if proposal_id:
                # Wait for consensus
                await asyncio.sleep(2.0)  # Allow time for voting
                proposal = self.consensus_manager.active_proposals.get(proposal_id)
                if proposal and proposal.has_consensus():
                    self.local_data[key] = value
                    self.data_versions[key] = self.data_versions.get(key, 0) + 1
                    logger.info(f"Stored {key} with strong consistency")
                    return True
            return False

        elif consistency == "eventual":
            # Use gossip for eventual consistency
            self.local_data[key] = value
            self.data_versions[key] = self.data_versions.get(key, 0) + 1

            # Propagate via gossip
            update_message = GossipMessage(
                message_id=f"data_update_{key}_{int(time.time())}",
                message_type=GossipMessageType.STATE_UPDATE,
                sender_id=self.node_id,
                payload={
                    "key": key,
                    "value": value,
                    "version": self.data_versions[key],
                    "timestamp": time.time(),
                },
            )

            await self.gossip_protocol.add_message(update_message)
            logger.info(f"Stored {key} with eventual consistency")
            return True

        else:  # weak consistency
            # Just store locally
            self.local_data[key] = value
            self.data_versions[key] = self.data_versions.get(key, 0) + 1
            logger.info(f"Stored {key} with weak consistency")
            return True

    async def get(self, key: str, consistency: str = "eventual") -> Any | None:
        """
        Retrieve a value by key with specified consistency level.

        Args:
            key: The key to retrieve
            consistency: Consistency level ("eventual", "strong", "weak")

        Returns:
            The value if found, None otherwise
        """
        if consistency == "weak":
            # Just return local value
            return self.local_data.get(key)

        elif consistency == "eventual":
            # Check local first, then query peers via gossip
            if key in self.local_data:
                return self.local_data[key]

            # Query peers (simplified implementation)
            query_message = GossipMessage(
                message_id=f"data_query_{key}_{int(time.time())}",
                message_type=GossipMessageType.ANTI_ENTROPY,
                sender_id=self.node_id,
                payload={"type": "query", "key": key, "requester": self.node_id},
            )

            await self.gossip_protocol.add_message(query_message)

            # Wait a bit for responses
            await asyncio.sleep(1.0)

            # Check if we received the data
            return self.local_data.get(key)

        else:  # strong consistency
            # Would need to implement quorum reads
            # For now, return local value
            return self.local_data.get(key)

    async def delete(self, key: str, consistency: str = "eventual") -> bool:
        """Delete a key-value pair."""
        if key in self.local_data:
            del self.local_data[key]
            self.data_versions[key] = self.data_versions.get(key, 0) + 1

            if consistency == "eventual":
                # Propagate deletion via gossip
                delete_message = GossipMessage(
                    message_id=f"data_delete_{key}_{int(time.time())}",
                    message_type=GossipMessageType.STATE_UPDATE,
                    sender_id=self.node_id,
                    payload={
                        "key": key,
                        "action": "delete",
                        "version": self.data_versions[key],
                        "timestamp": time.time(),
                    },
                )

                await self.gossip_protocol.add_message(delete_message)

            logger.info(f"Deleted {key} with {consistency} consistency")
            return True

        return False

    def get_stats(self) -> dict[str, Any]:
        """Get statistics about the data store."""
        return {
            "node_id": self.node_id,
            "region": self.region,
            "local_keys": len(self.local_data),
            "total_versions": sum(self.data_versions.values()),
            "gossip_stats": self.gossip_protocol.get_comprehensive_statistics(),
            "membership_stats": self.membership_protocol.get_membership_statistics(),
        }


@dataclass(slots=True)
class DistributedTaskQueue:
    """
    Example: Distributed task queue using MPREG federation.

    This demonstrates how to build a distributed task processing
    system with load balancing and fault tolerance.
    """

    node_id: str
    coordinates: GeographicCoordinate
    region: str

    # Fields initialized in __post_init__
    hub_registry: HubRegistry = field(init=False)
    gossip_protocol: GossipProtocol = field(init=False)
    consensus_manager: ConsensusManager = field(init=False)
    membership_protocol: MembershipProtocol = field(init=False)
    pending_tasks: dict[str, dict] = field(default_factory=dict)
    processing_tasks: dict[str, dict] = field(default_factory=dict)
    completed_tasks: dict[str, dict] = field(default_factory=dict)
    max_concurrent_tasks: int = 10
    current_tasks: int = 0

    def __post_init__(self):
        # Initialize federation components
        self.hub_registry = HubRegistry(self.node_id)
        self.gossip_protocol = GossipProtocol(self.node_id, self.hub_registry)
        self.consensus_manager = ConsensusManager(self.node_id, self.gossip_protocol)
        self.membership_protocol = MembershipProtocol(
            self.node_id, self.gossip_protocol, self.consensus_manager
        )

        # Task queue state and worker configuration are now set via field defaults

        logger.info(f"Initialized distributed task queue node {self.node_id}")

    async def start(self):
        """Start the task queue."""
        await self.gossip_protocol.start()
        await self.consensus_manager.start()
        await self.membership_protocol.start()

        # Register as a task processor
        hub_info = HubRegistrationInfo(
            hub_id=self.node_id,
            hub_tier=HubTier.LOCAL,
            hub_capabilities=HubCapabilities(
                max_clusters=self.max_concurrent_tasks,
                max_subscriptions=10000,
                bandwidth_mbps=1000,
                cpu_capacity=100.0,
            ),
            coordinates=self.coordinates,
            region=self.region,
        )
        await self.hub_registry.register_hub(hub_info)

        # Start task processing loop
        asyncio.create_task(self._process_tasks())

        logger.info(f"Started distributed task queue node {self.node_id}")

    async def stop(self):
        """Stop the task queue."""
        await self.membership_protocol.stop()
        await self.consensus_manager.stop()
        await self.gossip_protocol.stop()
        await self.hub_registry.deregister_hub(self.node_id)

        logger.info(f"Stopped distributed task queue node {self.node_id}")

    async def submit_task(self, task_id: str, task_type: str, payload: dict) -> bool:
        """
        Submit a task to the distributed queue.

        Args:
            task_id: Unique task identifier
            task_type: Type of task (e.g., "cpu", "io", "network")
            payload: Task payload/parameters

        Returns:
            True if task was submitted successfully
        """
        logger.info(f"Submitting task {task_id} of type {task_type}")

        task = {
            "task_id": task_id,
            "task_type": task_type,
            "payload": payload,
            "submitted_at": time.time(),
            "submitted_by": self.node_id,
            "status": "pending",
        }

        # Add to local queue
        self.pending_tasks[task_id] = task

        # Propagate task via gossip for load balancing
        task_message = GossipMessage(
            message_id=f"task_submit_{task_id}",
            message_type=GossipMessageType.STATE_UPDATE,
            sender_id=self.node_id,
            payload={"action": "task_submit", "task": task},
        )

        await self.gossip_protocol.add_message(task_message)

        logger.info(f"Task {task_id} submitted successfully")
        return True

    async def _process_tasks(self):
        """Background task processing loop."""
        while True:
            try:
                # Check if we can process more tasks
                if (
                    self.current_tasks < self.max_concurrent_tasks
                    and self.pending_tasks
                ):
                    # Get next task
                    task_id = next(iter(self.pending_tasks))
                    task = self.pending_tasks.pop(task_id)

                    # Start processing
                    self.processing_tasks[task_id] = task
                    self.current_tasks += 1

                    # Process task asynchronously
                    asyncio.create_task(self._execute_task(task))

                await asyncio.sleep(0.1)  # Small delay to prevent busy waiting

            except Exception as e:
                logger.error(f"Error in task processing loop: {e}")
                await asyncio.sleep(1.0)

    async def _execute_task(self, task: dict):
        """Execute a single task."""
        task_id = task["task_id"]
        task_type = task["task_type"]

        try:
            logger.info(f"Executing task {task_id} of type {task_type}")

            # Update task status
            task["status"] = "processing"
            task["started_at"] = time.time()
            task["processed_by"] = self.node_id

            # Simulate task execution based on type
            if task_type == "cpu":
                # CPU-intensive task
                await self._simulate_cpu_task(task["payload"])
            elif task_type == "io":
                # I/O-intensive task
                await self._simulate_io_task(task["payload"])
            elif task_type == "network":
                # Network-intensive task
                await self._simulate_network_task(task["payload"])
            else:
                # Generic task
                await asyncio.sleep(1.0)

            # Task completed successfully
            task["status"] = "completed"
            task["completed_at"] = time.time()

            # Move to completed tasks
            self.completed_tasks[task_id] = task

            logger.info(f"Task {task_id} completed successfully")

        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}")
            task["status"] = "failed"
            task["error"] = str(e)
            task["failed_at"] = time.time()

            # Move to completed tasks (for error tracking)
            self.completed_tasks[task_id] = task

        finally:
            # Clean up
            if task_id in self.processing_tasks:
                del self.processing_tasks[task_id]
            self.current_tasks -= 1

            # Propagate task completion via gossip
            completion_message = GossipMessage(
                message_id=f"task_complete_{task_id}",
                message_type=GossipMessageType.STATE_UPDATE,
                sender_id=self.node_id,
                payload={"action": "task_complete", "task": task},
            )

            await self.gossip_protocol.add_message(completion_message)

    async def _simulate_cpu_task(self, payload: dict):
        """Simulate CPU-intensive task."""
        # Simulate CPU work
        duration = payload.get("duration", 2.0)
        await asyncio.sleep(duration)

        # Simulate some computation
        result = sum(i * i for i in range(1000))
        payload["result"] = result

    async def _simulate_io_task(self, payload: dict):
        """Simulate I/O-intensive task."""
        # Simulate file I/O
        filename = payload.get("filename", "test.txt")
        await asyncio.sleep(0.5)  # Simulate disk I/O

        payload["result"] = f"Processed file: {filename}"

    async def _simulate_network_task(self, payload: dict):
        """Simulate network-intensive task."""
        # Simulate network request
        url = payload.get("url", "https://example.com")
        await asyncio.sleep(1.0)  # Simulate network latency

        payload["result"] = f"Fetched data from: {url}"

    def get_task_stats(self) -> dict[str, Any]:
        """Get task queue statistics."""
        return {
            "node_id": self.node_id,
            "region": self.region,
            "pending_tasks": len(self.pending_tasks),
            "processing_tasks": len(self.processing_tasks),
            "completed_tasks": len(self.completed_tasks),
            "current_load": self.current_tasks / self.max_concurrent_tasks,
            "max_concurrent_tasks": self.max_concurrent_tasks,
        }


@dataclass(slots=True)
class DistributedConfigManager:
    """
    Example: Distributed configuration manager using MPREG federation.

    This demonstrates how to build a distributed configuration system
    with real-time updates and conflict resolution.
    """

    node_id: str
    coordinates: GeographicCoordinate
    region: str

    # Fields initialized in __post_init__
    hub_registry: HubRegistry = field(init=False)
    gossip_protocol: GossipProtocol = field(init=False)
    consensus_manager: ConsensusManager = field(init=False)
    membership_protocol: MembershipProtocol = field(init=False)
    configurations: dict[str, dict] = field(default_factory=dict)
    config_versions: dict[str, int] = field(default_factory=dict)
    config_watchers: dict[str, list[Callable[..., Any]]] = field(default_factory=dict)

    def __post_init__(self):
        # Initialize federation components
        self.hub_registry = HubRegistry(self.node_id)
        self.gossip_protocol = GossipProtocol(self.node_id, self.hub_registry)
        self.consensus_manager = ConsensusManager(self.node_id, self.gossip_protocol)
        self.membership_protocol = MembershipProtocol(
            self.node_id, self.gossip_protocol, self.consensus_manager
        )

        # Configuration state is now set via field defaults

        logger.info(f"Initialized distributed config manager node {self.node_id}")

    async def start(self):
        """Start the configuration manager."""
        await self.gossip_protocol.start()
        await self.consensus_manager.start()
        await self.membership_protocol.start()

        # Register as a config provider
        hub_info = HubRegistrationInfo(
            hub_id=self.node_id,
            hub_tier=HubTier.LOCAL,
            hub_capabilities=HubCapabilities(
                max_clusters=100,
                max_subscriptions=10000,
                bandwidth_mbps=1000,
                cpu_capacity=100.0,
            ),
            coordinates=self.coordinates,
            region=self.region,
        )
        await self.hub_registry.register_hub(hub_info)

        logger.info(f"Started distributed config manager node {self.node_id}")

    async def stop(self):
        """Stop the configuration manager."""
        await self.membership_protocol.stop()
        await self.consensus_manager.stop()
        await self.gossip_protocol.stop()
        await self.hub_registry.deregister_hub(self.node_id)

        logger.info(f"Stopped distributed config manager node {self.node_id}")

    async def set_config(
        self, config_key: str, config_value: dict, require_consensus: bool = True
    ) -> bool:
        """
        Set a configuration value.

        Args:
            config_key: Configuration key (e.g., "app.database.timeout")
            config_value: Configuration value
            require_consensus: Whether to require consensus for the change

        Returns:
            True if configuration was set successfully
        """
        logger.info(f"Setting config {config_key} (consensus: {require_consensus})")

        config = {
            "key": config_key,
            "value": config_value,
            "version": self.config_versions.get(config_key, 0) + 1,
            "updated_at": time.time(),
            "updated_by": self.node_id,
        }

        if require_consensus:
            # Use consensus for critical configuration changes
            vector_clock = VectorClock()
            vector_clock.increment(self.node_id)
            state_value = StateValue(
                value=config,
                vector_clock=vector_clock,
                node_id=self.node_id,
                state_type=StateType.MAP_STATE,
            )

            proposal_id = await self.consensus_manager.propose_state_change(
                config_key, state_value
            )

            if proposal_id:
                # Wait for consensus
                await asyncio.sleep(2.0)
                proposal = self.consensus_manager.active_proposals.get(proposal_id)
                if proposal and proposal.has_consensus():
                    await self._apply_config_change(config)
                    logger.info(f"Config {config_key} set with consensus")
                    return True

            return False

        else:
            # Apply immediately and propagate via gossip
            await self._apply_config_change(config)

            # Propagate via gossip
            config_message = GossipMessage(
                message_id=f"config_update_{config_key}_{int(time.time())}",
                message_type=GossipMessageType.CONFIG_UPDATE,
                sender_id=self.node_id,
                payload={"action": "config_update", "config": config},
            )

            await self.gossip_protocol.add_message(config_message)

            logger.info(f"Config {config_key} set without consensus")
            return True

    async def _apply_config_change(self, config: dict):
        """Apply a configuration change locally."""
        config_key = config["key"]

        # Update local configuration
        self.configurations[config_key] = config
        self.config_versions[config_key] = config["version"]

        # Notify watchers
        if config_key in self.config_watchers:
            for watcher in self.config_watchers[config_key]:
                try:
                    await watcher(config_key, config["value"])
                except Exception as e:
                    logger.error(f"Error notifying config watcher: {e}")

    def get_config(self, config_key: str) -> dict | None:
        """Get a configuration value."""
        return self.configurations.get(config_key, {}).get("value")

    def watch_config(self, config_key: str, callback: Callable):
        """Watch for changes to a configuration key."""
        if config_key not in self.config_watchers:
            self.config_watchers[config_key] = []

        self.config_watchers[config_key].append(callback)
        logger.info(f"Added watcher for config {config_key}")

    def get_all_configs(self) -> dict[str, Any]:
        """Get all configuration values."""
        return {key: config["value"] for key, config in self.configurations.items()}

    def get_config_stats(self) -> dict[str, Any]:
        """Get configuration manager statistics."""
        return {
            "node_id": self.node_id,
            "region": self.region,
            "total_configs": len(self.configurations),
            "watched_configs": len(self.config_watchers),
            "total_watchers": sum(
                len(watchers) for watchers in self.config_watchers.values()
            ),
        }


async def demo_distributed_data_store():
    """Demonstrate the distributed data store."""
    print("\n=== Distributed Data Store Demo ===")

    # Create data store nodes
    nodes = [
        DistributedDataStore(
            "store-node-1", GeographicCoordinate(40.7128, -74.0060), "us-east"
        ),
        DistributedDataStore(
            "store-node-2", GeographicCoordinate(37.7749, -122.4194), "us-west"
        ),
        DistributedDataStore(
            "store-node-3", GeographicCoordinate(51.5074, -0.1278), "eu-west"
        ),
    ]

    # Start all nodes
    for node in nodes:
        await node.start()

    try:
        # Wait for nodes to discover each other
        await asyncio.sleep(2.0)

        # Store some data with different consistency levels
        await nodes[0].put("user:1", {"name": "Alice", "age": 30}, "strong")
        await nodes[1].put("user:2", {"name": "Bob", "age": 25}, "eventual")
        await nodes[2].put("user:3", {"name": "Charlie", "age": 35}, "weak")

        # Wait for propagation
        await asyncio.sleep(3.0)

        # Retrieve data from different nodes
        user1 = await nodes[1].get("user:1", "eventual")
        user2 = await nodes[2].get("user:2", "eventual")
        user3 = await nodes[0].get("user:3", "weak")

        print(f"Retrieved user:1 from node-2: {user1}")
        print(f"Retrieved user:2 from node-3: {user2}")
        print(f"Retrieved user:3 from node-1: {user3}")

        # Show statistics
        for node in nodes:
            stats = node.get_stats()
            print(
                f"Node {node.node_id}: {stats['local_keys']} keys, {stats['total_versions']} versions"
            )

    finally:
        # Stop all nodes
        for node in nodes:
            await node.stop()


async def demo_distributed_task_queue():
    """Demonstrate the distributed task queue."""
    print("\n=== Distributed Task Queue Demo ===")

    # Create task queue nodes
    nodes = [
        DistributedTaskQueue(
            "queue-node-1", GeographicCoordinate(40.7128, -74.0060), "us-east"
        ),
        DistributedTaskQueue(
            "queue-node-2", GeographicCoordinate(37.7749, -122.4194), "us-west"
        ),
    ]

    # Start all nodes
    for node in nodes:
        await node.start()

    try:
        # Wait for nodes to discover each other
        await asyncio.sleep(2.0)

        # Submit various tasks
        await nodes[0].submit_task("task-1", "cpu", {"duration": 1.0})
        await nodes[0].submit_task("task-2", "io", {"filename": "data.txt"})
        await nodes[1].submit_task(
            "task-3", "network", {"url": "https://api.example.com"}
        )
        await nodes[1].submit_task("task-4", "cpu", {"duration": 2.0})

        # Wait for task processing
        await asyncio.sleep(5.0)

        # Show task statistics
        for node in nodes:
            stats = node.get_task_stats()
            print(
                f"Node {node.node_id}: {stats['completed_tasks']} completed, load: {stats['current_load']:.2f}"
            )

    finally:
        # Stop all nodes
        for node in nodes:
            await node.stop()


async def demo_distributed_config_manager():
    """Demonstrate the distributed configuration manager."""
    print("\n=== Distributed Config Manager Demo ===")

    # Create config manager nodes
    nodes = [
        DistributedConfigManager(
            "config-node-1", GeographicCoordinate(40.7128, -74.0060), "us-east"
        ),
        DistributedConfigManager(
            "config-node-2", GeographicCoordinate(37.7749, -122.4194), "us-west"
        ),
    ]

    # Start all nodes
    for node in nodes:
        await node.start()

    try:
        # Wait for nodes to discover each other
        await asyncio.sleep(2.0)

        # Set up configuration watchers
        async def on_config_change(key: str, value: dict):
            print(f"Config changed: {key} = {value}")

        nodes[0].watch_config("app.database.timeout", on_config_change)
        nodes[1].watch_config("app.cache.size", on_config_change)

        # Set some configurations
        await nodes[0].set_config(
            "app.database.timeout",
            {"value": 30, "unit": "seconds"},
            require_consensus=True,
        )
        await nodes[1].set_config(
            "app.cache.size", {"value": 100, "unit": "MB"}, require_consensus=False
        )
        await nodes[0].set_config(
            "feature.new_ui", {"enabled": True}, require_consensus=False
        )

        # Wait for propagation
        await asyncio.sleep(3.0)

        # Retrieve configurations from different nodes
        db_timeout = nodes[1].get_config("app.database.timeout")
        cache_size = nodes[0].get_config("app.cache.size")
        feature_flag = nodes[1].get_config("feature.new_ui")

        print(f"Database timeout: {db_timeout}")
        print(f"Cache size: {cache_size}")
        print(f"New UI feature: {feature_flag}")

        # Show statistics
        for node in nodes:
            stats = node.get_config_stats()
            print(
                f"Node {node.node_id}: {stats['total_configs']} configs, {stats['total_watchers']} watchers"
            )

    finally:
        # Stop all nodes
        for node in nodes:
            await node.stop()


async def main():
    """Run all federation examples."""
    print("MPREG Federation Examples")
    print("=" * 50)

    # Run all demos
    await demo_distributed_data_store()
    await demo_distributed_task_queue()
    await demo_distributed_config_manager()

    print("\n" + "=" * 50)
    print("All federation examples completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
