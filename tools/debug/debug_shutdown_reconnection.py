#!/usr/bin/env python3
"""
Debug tool to trace connection lifecycle during cluster shutdown.

This tool creates a small cluster, monitors connection events, and traces
exactly when and why reconnection attempts happen during shutdown.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


@dataclass
class ConnectionTracker:
    """Tracks connection events for analysis."""

    events: list[dict[str, Any]] = field(default_factory=list)

    def log_event(
        self, event_type: str, node_name: str, peer_url: str, details: str = ""
    ) -> None:
        """Log a connection-related event."""
        event = {
            "timestamp": time.time(),
            "event_type": event_type,
            "node_name": node_name,
            "peer_url": peer_url,
            "details": details,
        }
        self.events.append(event)
        logger.info(f"CONNECTION_EVENT: {event}")

    def get_events_after(self, start_time: float) -> list[dict[str, Any]]:
        """Get events after a specific timestamp."""
        return [e for e in self.events if e["timestamp"] >= start_time]


@dataclass
class DebugNode:
    """A debug-enabled MPREG server with connection tracking."""

    name: str
    port: int
    mpreg_server: MPREGServer = field(init=False)
    server_task: asyncio.Task[Any] | None = field(default=None, init=False)
    is_running: bool = False
    tracker: ConnectionTracker = field(default_factory=ConnectionTracker)

    def __post_init__(self) -> None:
        """Initialize debug node."""
        settings = MPREGSettings(
            host="127.0.0.1",
            port=self.port,
            name=f"DebugNode_{self.name}",
            cluster_id="debug-cluster",
            resources={f"debug-resource-{self.name}"},
            gossip_interval=1.0,  # Slower gossip for easier debugging
        )

        self.mpreg_server = MPREGServer(settings=settings)

        # Hook into connection events
        self._patch_connection_methods()

    def _patch_connection_methods(self) -> None:
        """Track connection events without patching (just log)."""
        # Note: We can't patch MPREGServer methods due to __slots__
        # Instead, we'll rely on the built-in logging and manual tracking
        pass

    async def start(self) -> None:
        """Start the debug node."""
        self.tracker.log_event("NODE_START", self.name, "", "Starting node")
        self.server_task = asyncio.create_task(self.mpreg_server.server())
        await asyncio.sleep(0.5)  # Let server initialize
        self.is_running = True
        self.tracker.log_event(
            "NODE_RUNNING", self.name, "", "Node started and running"
        )

    async def stop(self) -> None:
        """Stop the debug node."""
        self.tracker.log_event(
            "NODE_SHUTDOWN_START", self.name, "", "Starting node shutdown"
        )
        self.is_running = False

        if self.server_task:
            self.server_task.cancel()
            try:
                await self.server_task
            except asyncio.CancelledError:
                pass
            self.server_task = None

        self.tracker.log_event(
            "NODE_SHUTDOWN_COMPLETE", self.name, "", "Node shutdown complete"
        )

    async def connect_to(self, other_node: "DebugNode") -> None:
        """Connect this node to another node."""
        peer_url = f"ws://{other_node.mpreg_server.settings.host}:{other_node.mpreg_server.settings.port}"
        self.mpreg_server.settings.connect = peer_url
        self.tracker.log_event(
            "CONNECT_TO",
            self.name,
            peer_url,
            f"Configured to connect to {other_node.name}",
        )


class ShutdownDebugger:
    """Debug cluster shutdown behavior."""

    def __init__(self) -> None:
        self.nodes: dict[str, DebugNode] = {}
        self.global_tracker = ConnectionTracker()

    async def create_test_cluster(self) -> None:
        """Create a small test cluster for debugging."""
        logger.info("Creating test cluster for shutdown debugging")

        # Create 4 nodes: 1 hub + 3 spokes
        self.nodes["hub"] = DebugNode("hub", 19000)
        self.nodes["spoke1"] = DebugNode("spoke1", 19001)
        self.nodes["spoke2"] = DebugNode("spoke2", 19002)
        self.nodes["spoke3"] = DebugNode("spoke3", 19003)

        # Start hub first
        await self.nodes["hub"].start()

        # Connect spokes to hub (star topology)
        for spoke_name in ["spoke1", "spoke2", "spoke3"]:
            await self.nodes[spoke_name].connect_to(self.nodes["hub"])
            await self.nodes[spoke_name].start()
            await asyncio.sleep(0.5)  # Stagger startup

        # Let cluster stabilize
        await asyncio.sleep(2.0)
        logger.info("Test cluster created and stabilized")

    async def test_shutdown_sequence(self) -> list[dict[str, Any]]:
        """Test different shutdown sequences and track reconnection behavior."""
        logger.info("=== Testing spokes-first shutdown ===")
        shutdown_start = time.time()

        # Stop spokes first (like the planet scale example)
        spoke_names = ["spoke1", "spoke2", "spoke3"]
        for spoke_name in spoke_names:
            logger.info(f"Stopping {spoke_name}")
            await self.nodes[spoke_name].stop()
            await asyncio.sleep(0.2)  # Brief pause between shutdowns

        # Wait a bit to see reconnection attempts
        logger.info("Waiting 3 seconds to observe reconnection attempts...")
        await asyncio.sleep(3.0)

        # Stop hub last
        logger.info("Stopping hub")
        await self.nodes["hub"].stop()

        # Wait to see final reconnection attempts
        logger.info("Waiting 2 seconds to observe final reconnection attempts...")
        await asyncio.sleep(2.0)

        # Analyze events
        shutdown_events: list[dict[str, Any]] = []
        for node in self.nodes.values():
            shutdown_events.extend(node.tracker.get_events_after(shutdown_start))

        # Sort events by timestamp
        shutdown_events.sort(key=lambda e: e["timestamp"])

        logger.info("=== SHUTDOWN EVENT ANALYSIS ===")
        for event in shutdown_events:
            event_time = event["timestamp"] - shutdown_start
            logger.info(
                f"T+{event_time:.3f}s: {event['event_type']} - {event['node_name']} -> {event['peer_url']} - {event['details']}"
            )

        # Count reconnection attempts after shutdown started
        reconnect_attempts = [
            e for e in shutdown_events if e["event_type"] == "ESTABLISH_ATTEMPT"
        ]
        logger.info(
            f"Total reconnection attempts during shutdown: {len(reconnect_attempts)}"
        )

        return shutdown_events

    async def analyze_background_tasks(self) -> None:
        """Analyze which background tasks are causing reconnections."""
        logger.info("=== BACKGROUND TASK ANALYSIS ===")

        for node_name, node in self.nodes.items():
            if node.mpreg_server and node.is_running:
                background_tasks = node.mpreg_server._background_tasks
                logger.info(
                    f"Node {node_name} has {len(background_tasks)} background tasks"
                )

                # Check peers_info to see what reconnections might be triggered
                peers_info = node.mpreg_server.cluster.peers_info
                logger.info(f"Node {node_name} knows about {len(peers_info)} peers:")
                for peer_url, peer_info in peers_info.items():
                    logger.info(f"  {peer_url} -> cluster_id: {peer_info.cluster_id}")


async def main() -> None:
    """Main debug function."""
    logger.info("Starting shutdown reconnection debugging")

    debugger = ShutdownDebugger()

    try:
        # Create and analyze cluster
        await debugger.create_test_cluster()
        await debugger.analyze_background_tasks()

        # Test shutdown sequence
        events = await debugger.test_shutdown_sequence()

        # Analysis
        logger.info("=== ANALYSIS COMPLETE ===")
        logger.info("Key findings:")

        # Count events by type
        event_counts: dict[str, int] = {}
        for event in events:
            event_type = event["event_type"]
            event_counts[event_type] = event_counts.get(event_type, 0) + 1

        for event_type, count in event_counts.items():
            logger.info(f"  {event_type}: {count}")

        # Identify problematic patterns
        failed_attempts = [e for e in events if e["event_type"] == "ESTABLISH_FAILED"]
        if failed_attempts:
            logger.warning(
                f"Found {len(failed_attempts)} failed connection attempts during shutdown"
            )
            for attempt in failed_attempts[:5]:  # Show first 5
                logger.warning(
                    f"  {attempt['node_name']} -> {attempt['peer_url']}: {attempt['details']}"
                )

    except Exception as e:
        logger.error(f"Debug session failed: {e}")
        raise

    finally:
        # Cleanup
        logger.info("Cleaning up debug session")
        for node in debugger.nodes.values():
            if node.is_running:
                await node.stop()


if __name__ == "__main__":
    # Set up logging for easier debugging
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
