#!/usr/bin/env python3
"""
DEEP DIVE DEBUG: Vector Clock Gossip Analysis

This tool analyzes WHY vector clocks are still allowing update loops.
We need to understand if the issue is:
1. Incorrect vector clock implementation
2. Missing vector clock updates in some code paths
3. Concurrent update resolution logic flaws
4. Edge cases in gossip processing

ALGORITHMIC DEBUGGING: NO GUESSING, ONLY FACTS
"""

import asyncio
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any

from mpreg.core.config import MPREGSettings
from mpreg.datastructures.vector_clock import VectorClock
from mpreg.server import Cluster, MPREGServer


@dataclass
class VectorClockEvent:
    """Track vector clock events for deep analysis."""

    timestamp: float
    node: str
    peer_url: str
    action: str  # "create", "update", "skip", "concurrent_resolve"
    existing_clock: dict[str, int] | None
    incoming_clock: dict[str, int]
    comparison_result: str  # "equal", "before", "after", "concurrent"
    should_update: bool
    reason: str


class VectorClockAnalyzer:
    """Deep analysis of vector clock behavior in gossip protocol."""

    def __init__(self):
        self.events: list[VectorClockEvent] = []
        self.peer_clock_history: dict[str, list[dict[str, int]]] = defaultdict(list)
        self.update_patterns: dict[str, list[str]] = defaultdict(list)

    def record_vector_clock_event(
        self,
        node: str,
        peer_url: str,
        action: str,
        existing_clock: dict[str, int] | None,
        incoming_clock: dict[str, int],
        comparison_result: str,
        should_update: bool,
        reason: str,
    ):
        """Record a vector clock event for analysis."""
        event = VectorClockEvent(
            timestamp=time.time(),
            node=node,
            peer_url=peer_url,
            action=action,
            existing_clock=existing_clock,
            incoming_clock=incoming_clock,
            comparison_result=comparison_result,
            should_update=should_update,
            reason=reason,
        )
        self.events.append(event)

        # Track clock evolution for this peer
        self.peer_clock_history[peer_url].append(incoming_clock.copy())

        # Track update patterns
        key = f"{node}->{peer_url}"
        self.update_patterns[key].append(
            f"{action}:{comparison_result}:{should_update}"
        )

    def analyze_clock_evolution(self) -> dict[str, Any]:
        """Analyze how vector clocks evolve over time."""
        analysis = {}

        for peer_url, clock_history in self.peer_clock_history.items():
            if len(clock_history) < 2:
                continue

            # Check for clock progression
            progressions = []
            regressions = []
            duplicates = []

            for i in range(1, len(clock_history)):
                prev_clock = VectorClock.from_dict(clock_history[i - 1])
                curr_clock = VectorClock.from_dict(clock_history[i])

                comparison = prev_clock.compare(curr_clock)
                if comparison == "before":
                    progressions.append(i)
                elif comparison == "after":
                    regressions.append(i)
                elif comparison == "equal":
                    duplicates.append(i)

            analysis[peer_url] = {
                "total_updates": len(clock_history),
                "progressions": len(progressions),
                "regressions": len(regressions),
                "duplicates": len(duplicates),
                "progression_ratio": len(progressions) / len(clock_history)
                if clock_history
                else 0,
                "duplicate_ratio": len(duplicates) / len(clock_history)
                if clock_history
                else 0,
            }

        return analysis

    def analyze_update_patterns(self) -> dict[str, Any]:
        """Analyze patterns in update decisions."""
        patterns = {}

        for key, pattern_list in self.update_patterns.items():
            pattern_counts = Counter(pattern_list)
            total = len(pattern_list)

            patterns[key] = {
                "total_attempts": total,
                "pattern_distribution": dict(pattern_counts),
                "most_common": pattern_counts.most_common(3),
            }

        return patterns

    def find_problematic_sequences(self) -> list[dict[str, Any]]:
        """Find sequences that indicate problems."""
        problems = []

        # Group events by peer
        peer_events = defaultdict(list)
        for event in self.events:
            peer_events[event.peer_url].append(event)

        for peer_url, events in peer_events.items():
            # Look for rapid consecutive updates
            for i in range(1, len(events)):
                prev_event = events[i - 1]
                curr_event = events[i]

                time_diff = curr_event.timestamp - prev_event.timestamp

                # Flag rapid updates (< 100ms apart)
                if time_diff < 0.1 and curr_event.should_update:
                    problems.append(
                        {
                            "type": "rapid_consecutive_updates",
                            "peer_url": peer_url,
                            "time_diff": time_diff,
                            "prev_comparison": prev_event.comparison_result,
                            "curr_comparison": curr_event.comparison_result,
                            "prev_clock": prev_event.incoming_clock,
                            "curr_clock": curr_event.incoming_clock,
                        }
                    )

                # Flag identical clock updates
                if (
                    curr_event.incoming_clock == prev_event.incoming_clock
                    and curr_event.should_update
                ):
                    problems.append(
                        {
                            "type": "identical_clock_update",
                            "peer_url": peer_url,
                            "clock": curr_event.incoming_clock,
                            "reason": curr_event.reason,
                        }
                    )

        return problems

    def print_comprehensive_report(self):
        """Print comprehensive analysis report."""
        print("=" * 100)
        print("VECTOR CLOCK DEEP DIVE ANALYSIS REPORT")
        print("=" * 100)

        print("\n📊 OVERALL STATISTICS:")
        print(f"  Total vector clock events: {len(self.events)}")
        print(f"  Unique peers tracked: {len(self.peer_clock_history)}")
        print(f"  Update patterns tracked: {len(self.update_patterns)}")

        # Clock evolution analysis
        print("\n🕐 CLOCK EVOLUTION ANALYSIS:")
        evolution_analysis = self.analyze_clock_evolution()
        for peer_url, stats in evolution_analysis.items():
            if stats["duplicate_ratio"] > 0.3:  # Flag high duplicate ratio
                print(
                    f"  🚨 {peer_url}: {stats['duplicates']}/{stats['total_updates']} duplicates ({stats['duplicate_ratio']:.1%})"
                )
            elif stats["regressions"] > 0:
                print(f"  ⚠️  {peer_url}: {stats['regressions']} regressions detected")
            else:
                print(
                    f"  ✅ {peer_url}: {stats['progressions']}/{stats['total_updates']} progressions ({stats['progression_ratio']:.1%})"
                )

        # Update pattern analysis
        print("\n🔄 UPDATE PATTERN ANALYSIS:")
        pattern_analysis = self.analyze_update_patterns()
        for key, stats in pattern_analysis.items():
            if stats["total_attempts"] > 20:  # Flag excessive attempts
                most_common = (
                    stats["most_common"][0] if stats["most_common"] else ("unknown", 0)
                )
                print(
                    f"  🚨 {key}: {stats['total_attempts']} attempts, most common: {most_common}"
                )

        # Problematic sequences
        print("\n❌ PROBLEMATIC SEQUENCES:")
        problems = self.find_problematic_sequences()
        if not problems:
            print("  ✅ No problematic sequences detected")
        else:
            for i, problem in enumerate(problems[:10]):  # Show first 10
                print(
                    f"  {i + 1}. {problem['type']}: {problem.get('peer_url', 'unknown')}"
                )
                if problem["type"] == "rapid_consecutive_updates":
                    print(f"     Time diff: {problem['time_diff']:.3f}s")
                    print(
                        f"     Comparisons: {problem['prev_comparison']} -> {problem['curr_comparison']}"
                    )
                elif problem["type"] == "identical_clock_update":
                    print(f"     Clock: {problem['clock']}")
                    print(f"     Reason: {problem['reason']}")

        # Sample recent events
        print("\n📋 RECENT VECTOR CLOCK EVENTS (last 20):")
        for event in self.events[-20:]:
            status = "✅" if event.should_update else "❌"
            print(
                f"  {status} {event.node} -> {event.peer_url}: {event.comparison_result} ({event.reason})"
            )


# Global analyzer instance
analyzer = VectorClockAnalyzer()


class InstrumentedCluster(Cluster):
    """Cluster with vector clock instrumentation."""

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
        """Instrumented version with vector clock analysis."""
        for peer_info in gossip_message.peers:
            # Only process if the cluster_id matches
            if peer_info.cluster_id != self.original_cluster.cluster_id:
                continue

            existing_clock = None
            action = "create"

            if peer_info.url in self.original_cluster.peers_info:
                existing_peer = self.original_cluster.peers_info[peer_info.url]
                existing_clock = existing_peer.logical_clock
                action = "update"

                # DETAILED VECTOR CLOCK ANALYSIS
                existing_vc = VectorClock.from_dict(existing_clock)
                incoming_vc = VectorClock.from_dict(peer_info.logical_clock)
                comparison = existing_vc.compare(incoming_vc)

                should_update = False
                reason = ""

                if comparison == "before":
                    should_update = True
                    reason = "vector clock ordering (incoming after existing)"
                elif comparison == "concurrent":
                    should_update = (
                        self.original_cluster._resolve_concurrent_peer_update(
                            existing_peer, peer_info
                        )
                    )
                    reason = f"concurrent update resolution ({should_update})"
                else:
                    should_update = False
                    reason = f"vector clock ordering (incoming {comparison} existing)"

                # Record the event for analysis
                analyzer.record_vector_clock_event(
                    node=self.server_name,
                    peer_url=peer_info.url,
                    action=action,
                    existing_clock=existing_clock,
                    incoming_clock=peer_info.logical_clock,
                    comparison_result=comparison,
                    should_update=should_update,
                    reason=reason,
                )
            else:
                # New peer
                analyzer.record_vector_clock_event(
                    node=self.server_name,
                    peer_url=peer_info.url,
                    action="create",
                    existing_clock=None,
                    incoming_clock=peer_info.logical_clock,
                    comparison_result="new",
                    should_update=True,
                    reason="new peer",
                )

        return self.original_cluster.process_gossip_message(gossip_message)

    def __getattr__(self, name):
        return getattr(self.original_cluster, name)


async def debug_vector_clock_gossip(num_nodes: int = 10, analysis_duration: int = 30):
    """Create instrumented cluster to analyze vector clock behavior."""
    print(f"🔬 Starting DEEP DIVE vector clock analysis with {num_nodes} nodes...")

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
            gossip_interval=2.0,  # Slower gossip for better analysis
        )

        server = MPREGServer(settings=settings)

        # Replace cluster with instrumented version
        original_cluster = server.cluster
        server.cluster = InstrumentedCluster(original_cluster, server.settings.name)

        servers.append(server)

    # Start servers with staggered connections
    tasks = []
    for i, server in enumerate(servers):
        # Connect to previous server to form a chain
        if i > 0:
            server.settings.connect = f"ws://127.0.0.1:{ports[i - 1]}"

        task = asyncio.create_task(server.server())
        tasks.append(task)
        await asyncio.sleep(0.3)  # Slower startup for cleaner analysis

    print(
        f"✅ All {num_nodes} servers started, analyzing for {analysis_duration} seconds..."
    )

    # Let the system run for analysis
    await asyncio.sleep(analysis_duration)

    # Generate comprehensive analysis report
    analyzer.print_comprehensive_report()

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


if __name__ == "__main__":
    import sys

    num_nodes = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 30

    print("🧪 VECTOR CLOCK DEEP DIVE DEBUG")
    print(f"Creating {num_nodes}-node cluster for {duration}s analysis...")

    asyncio.run(debug_vector_clock_gossip(num_nodes, duration))
