#!/usr/bin/env python3
"""
DEEP DIVE DEBUG: Gossip Protocol Analysis Tool

This script creates a controlled environment to analyze the gossip protocol
behavior and identify the root cause of infinite loops in large clusters.
"""

import asyncio
import json
import time
from collections import Counter, defaultdict
from dataclasses import dataclass

from mpreg.core.config import MPREGSettings
from mpreg.server import Cluster, MPREGServer


@dataclass
class GossipEvent:
    """Track individual gossip events for analysis."""

    timestamp: float
    sender: str
    receiver: str
    peer_count: int
    message_id: str


@dataclass
class PeerUpdateEvent:
    """Track peer update events."""

    timestamp: float
    node: str
    peer_url: str
    action: str  # "add", "update", "remove"
    reason: str
    old_last_seen: float
    new_last_seen: float


class GossipAnalyzer:
    """Analyze gossip protocol behavior to identify stability issues."""

    def __init__(self):
        self.gossip_events: list[GossipEvent] = []
        self.peer_events: list[PeerUpdateEvent] = []
        self.message_duplicates: Counter = Counter()
        self.update_loops: dict[str, list[float]] = defaultdict(list)

    def record_gossip(
        self, sender: str, receiver: str, peer_count: int, message_id: str
    ):
        """Record a gossip message being sent."""
        event = GossipEvent(
            timestamp=time.time(),
            sender=sender,
            receiver=receiver,
            peer_count=peer_count,
            message_id=message_id,
        )
        self.gossip_events.append(event)

        # Track message duplicates
        self.message_duplicates[message_id] += 1

    def record_peer_update(
        self,
        node: str,
        peer_url: str,
        action: str,
        reason: str,
        old_last_seen: float,
        new_last_seen: float,
    ):
        """Record a peer update event."""
        event = PeerUpdateEvent(
            timestamp=time.time(),
            node=node,
            peer_url=peer_url,
            action=action,
            reason=reason,
            old_last_seen=old_last_seen,
            new_last_seen=new_last_seen,
        )
        self.peer_events.append(event)

        # Track potential update loops (same peer updated repeatedly)
        key = f"{node}->{peer_url}"
        self.update_loops[key].append(time.time())

    def analyze_stability(self) -> dict:
        """Analyze collected data for stability issues."""
        now = time.time()

        # Find update loops (same peer updated >10 times in 30 seconds)
        loops = {}
        for key, timestamps in self.update_loops.items():
            recent = [t for t in timestamps if now - t < 30]
            if len(recent) > 10:
                loops[key] = len(recent)

        # Find duplicate messages
        duplicates = {
            msg_id: count
            for msg_id, count in self.message_duplicates.items()
            if count > 5
        }

        # Calculate gossip rate (messages per second)
        recent_gossip = [e for e in self.gossip_events if now - e.timestamp < 60]
        gossip_rate = len(recent_gossip) / 60 if recent_gossip else 0

        # Find timestamp precision issues
        timestamp_deltas: list[float] = []
        peer_updates_by_peer = defaultdict(list)
        for event in self.peer_events:
            peer_updates_by_peer[event.peer_url].append(event)

        precision_issues = {}
        for peer_url, updates in peer_updates_by_peer.items():
            if len(updates) > 2:
                sorted_updates = sorted(updates, key=lambda x: x.timestamp)
                deltas = []
                for i in range(1, len(sorted_updates)):
                    delta = abs(
                        sorted_updates[i].new_last_seen
                        - sorted_updates[i - 1].new_last_seen
                    )
                    deltas.append(delta)

                if deltas and min(deltas) < 0.001:  # Sub-millisecond differences
                    precision_issues[peer_url] = min(deltas)

        return {
            "update_loops": loops,
            "duplicate_messages": duplicates,
            "gossip_rate_per_second": gossip_rate,
            "timestamp_precision_issues": precision_issues,
            "total_gossip_events": len(self.gossip_events),
            "total_peer_events": len(self.peer_events),
            "analysis_duration": 60,
        }

    def print_report(self):
        """Print detailed analysis report."""
        analysis = self.analyze_stability()

        print("=" * 80)
        print("GOSSIP PROTOCOL DEEP DIVE ANALYSIS REPORT")
        print("=" * 80)

        print("\n📊 OVERALL STATISTICS:")
        print(f"  Total gossip events: {analysis['total_gossip_events']}")
        print(f"  Total peer events: {analysis['total_peer_events']}")
        print(
            f"  Gossip rate: {analysis['gossip_rate_per_second']:.2f} messages/second"
        )

        print("\n🔄 UPDATE LOOPS (same peer updated >10 times in 30s):")
        if analysis["update_loops"]:
            for key, count in analysis["update_loops"].items():
                print(f"  {key}: {count} updates")
        else:
            print("  No update loops detected")

        print("\n📨 DUPLICATE MESSAGES (>5 occurrences):")
        if analysis["duplicate_messages"]:
            for msg_id, count in analysis["duplicate_messages"].items():
                print(f"  {msg_id}: {count} duplicates")
        else:
            print("  No excessive message duplication")

        print("\n⏱️  TIMESTAMP PRECISION ISSUES:")
        if analysis["timestamp_precision_issues"]:
            for peer, min_delta in analysis["timestamp_precision_issues"].items():
                print(f"  {peer}: minimum delta = {min_delta:.6f}s")
        else:
            print("  No timestamp precision issues detected")

        # Recent activity sample
        print("\n📋 RECENT PEER UPDATE SAMPLE (last 10):")
        recent_events = sorted(self.peer_events, key=lambda x: x.timestamp)[-10:]
        for event in recent_events:
            print(
                f"  {event.timestamp:.3f}: {event.node} {event.action} {event.peer_url} ({event.reason})"
            )


# Global analyzer instance
analyzer = GossipAnalyzer()


class InstrumentedMPREGServer(MPREGServer):
    """MPREG Server with gossip instrumentation for debugging."""

    async def _send_gossip_messages(self) -> None:
        """Instrumented version of _send_gossip_messages."""
        peer_count = len(self.cluster.peers_info)
        message_id = f"{self.settings.name}-{time.time()}"

        active_connections = [
            (url, conn)
            for url, conn in self.peer_connections.items()
            if conn.is_connected
        ]

        for peer_url, _ in active_connections:
            analyzer.record_gossip(
                sender=self.settings.name,
                receiver=peer_url,
                peer_count=peer_count,
                message_id=message_id,
            )

        # Call original implementation
        return await super()._send_gossip_messages()


class InstrumentedCluster(Cluster):
    """Instrumented cluster for debugging gossip processing."""

    def __init__(self, original_cluster: Cluster, server_name: str):
        # Initialize parent with original cluster's config
        super().__init__(config=original_cluster.config)
        self.original_cluster = original_cluster
        self.server_name = server_name

        # Copy state from original cluster
        self._peers_info = original_cluster.peers_info.copy()
        self.peer_connections = original_cluster.peer_connections.copy()
        self.waitingFor = original_cluster.waitingFor.copy()
        self.answer = original_cluster.answer.copy()
        self.registry = original_cluster.registry
        self.serializer = original_cluster.serializer

    def process_gossip_message(self, gossip_message):
        """Instrumented version of process_gossip_message."""
        for peer_info in gossip_message.peers:
            if peer_info.url in self.original_cluster.peers_info:
                old_last_seen = self.original_cluster.peers_info[
                    peer_info.url
                ].last_seen
                action = "update"
                reason = f"gossip from {gossip_message.u[:8]}"
            else:
                old_last_seen = 0.0
                action = "add"
                reason = "new peer"

            analyzer.record_peer_update(
                node=self.server_name,
                peer_url=peer_info.url,
                action=action,
                reason=reason,
                old_last_seen=old_last_seen,
                new_last_seen=peer_info.last_seen,
            )

        return self.original_cluster.process_gossip_message(gossip_message)

    def __getattr__(self, name):
        return getattr(self.original_cluster, name)


async def debug_gossip_protocol(num_nodes: int = 10):
    """Create a controlled gossip cluster and analyze its behavior."""
    print(f"🔬 Starting gossip protocol analysis with {num_nodes} nodes...")

    # Create servers with instrumented gossip
    servers = []
    ports = list(range(20000, 20000 + num_nodes))

    for i, port in enumerate(ports):
        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name=f"Debug-Node-{i}",
            cluster_id="debug-cluster",
            resources={f"node-{i}"},
            gossip_interval=1.0,  # 1 second intervals
        )

        server = InstrumentedMPREGServer(settings=settings)

        # Replace cluster with instrumented version
        original_cluster = server.cluster
        server.cluster = InstrumentedCluster(original_cluster, server.settings.name)

        servers.append(server)

    # Start servers with staggered connections
    tasks = []
    for i, server in enumerate(servers):
        # Connect to previous servers to form a chain
        if i > 0:
            server.settings.connect = f"ws://127.0.0.1:{ports[i - 1]}"

        task = asyncio.create_task(server.server())
        tasks.append(task)
        await asyncio.sleep(0.5)  # Staggered startup

    print(f"✅ All {num_nodes} servers started, analyzing gossip for 60 seconds...")

    # Let gossip run for analysis period
    await asyncio.sleep(60)

    # Generate analysis report
    analyzer.print_report()

    # Clean shutdown
    print("\n🛑 Shutting down servers...")
    for server in servers:
        try:
            await server.shutdown_async()
        except Exception as e:
            print(f"Shutdown error: {e}")

    # Cancel remaining tasks
    for task in tasks:
        if not task.done():
            task.cancel()

    return analyzer.analyze_stability()


if __name__ == "__main__":
    import sys

    num_nodes = int(sys.argv[1]) if len(sys.argv) > 1 else 10

    print("🧪 GOSSIP PROTOCOL DEEP DIVE DEBUG")
    print(f"Creating {num_nodes}-node cluster for analysis...")

    result = asyncio.run(debug_gossip_protocol(num_nodes))

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(json.dumps(result, indent=2))
