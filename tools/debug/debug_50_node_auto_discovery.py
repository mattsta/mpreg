#!/usr/bin/env python3
"""
DEEP DIVE DEBUG: 50-Node Auto-Discovery Analysis

This tool creates a minimal 50-node cluster with focused debugging
to identify why peer discovery is incomplete in large clusters.

NO VERBOSE LOGGING - ONLY ESSENTIAL METRICS AND ANALYSIS
"""

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


@dataclass
class ClusterAnalysisMetrics:
    """Compact metrics for cluster analysis."""

    node_count: int
    discovery_start_time: float
    discovery_end_time: float
    peer_counts: dict[int, int]  # node_id -> peer_count
    connection_matrix: dict[tuple[int, int], bool]  # (from, to) -> connected
    gossip_message_counts: dict[int, int]  # node_id -> gossip_count
    function_propagation: dict[int, bool]  # node_id -> has_test_function


class AutoDiscoveryAnalyzer:
    """Focused analyzer for 50-node auto-discovery issues."""

    def __init__(self):
        self.metrics = ClusterAnalysisMetrics(
            node_count=0,
            discovery_start_time=0,
            discovery_end_time=0,
            peer_counts={},
            connection_matrix={},
            gossip_message_counts={},
            function_propagation={},
        )
        self.servers: list[MPREGServer] = []

    async def create_50_node_multi_hub_cluster(self) -> list[MPREGServer]:
        """Create 50-node multi-hub cluster with ORIGINAL TEST TOPOLOGY."""
        print("🔧 Creating 50-node multi-hub cluster (ORIGINAL TEST TOPOLOGY)...")

        # Use ports 30000-30049 to avoid conflicts
        base_port = 30000
        servers = []

        # ORIGINAL TOPOLOGY: Only 3 hubs (nodes 0, 1, 2), all others connect round-robin
        num_hubs = min(3, 50)

        for i in range(50):
            port = base_port + i

            # Original MULTI_HUB topology from test
            peers = []
            if i < num_hubs:  # First 3 are hubs
                # Hubs connect to the previous hub (forming a hub chain)
                if i > 0:
                    peers.append(f"ws://127.0.0.1:{base_port + (i - 1)}")
            else:
                # Non-hub nodes connect to one of the hubs (round-robin)
                hub_port = base_port + (i % num_hubs)
                peers.append(f"ws://127.0.0.1:{hub_port}")

            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"Node-{i}",
                cluster_id="debug-50-cluster",
                resources={f"node-{i}"},
                peers=peers,
                log_level="ERROR",  # MINIMAL LOGGING
                gossip_interval=2.0,  # Slower gossip for large cluster
            )

            server = MPREGServer(settings=settings)
            servers.append(server)

            # Start server and stagger slightly
            asyncio.create_task(server.server())
            await asyncio.sleep(0.05)  # Very short stagger

            if (i + 1) % 10 == 0:
                print(f"  Created {i + 1}/50 nodes...")

        self.servers = servers
        self.metrics.node_count = 50
        print("✅ All 50 nodes created and starting...")
        print(f"TOPOLOGY: 3 hubs (0,1,2), {50 - 3} nodes distributed round-robin")
        return servers

    def analyze_cluster_connectivity(self) -> dict[str, Any]:
        """Analyze actual cluster connectivity patterns."""
        analysis: dict[str, Any] = {
            "peer_distribution": {},
            "connectivity_issues": [],
            "hub_effectiveness": {},
            "isolated_nodes": [],
        }

        # Analyze peer counts per node
        for i, server in enumerate(self.servers):
            peer_count = len(server.cluster.servers) - 1  # Exclude self
            self.metrics.peer_counts[i] = peer_count

            # Check for isolated nodes (< 5 peers)
            if peer_count < 5:
                analysis["isolated_nodes"].append(i)

            # Analyze hub effectiveness (every 10th node should have more peers)
            if i % 10 == 0:
                analysis["hub_effectiveness"][i] = {
                    "peer_count": peer_count,
                    "expected_role": "hub",
                    "is_effective": peer_count > 10,
                }

        # Distribution analysis
        peer_counts = list(self.metrics.peer_counts.values())
        analysis["peer_distribution"] = {
            "min": min(peer_counts),
            "max": max(peer_counts),
            "avg": sum(peer_counts) / len(peer_counts),
            "median": sorted(peer_counts)[len(peer_counts) // 2],
            "below_10": len([p for p in peer_counts if p < 10]),
            "below_20": len([p for p in peer_counts if p < 20]),
            "below_30": len([p for p in peer_counts if p < 30]),
        }

        return analysis

    def identify_convergence_bottlenecks(self) -> dict[str, Any]:
        """Identify specific bottlenecks preventing full convergence."""
        bottlenecks: dict[str, Any] = {
            "network_partitions": [],
            "hub_failures": [],
            "gossip_efficiency": {},
            "timing_issues": {},
        }

        # Check for network partitions (groups of nodes with similar peer sets)
        peer_groups = defaultdict(list)
        for node_id, peer_count in self.metrics.peer_counts.items():
            peer_groups[peer_count].append(node_id)

        # If we have large groups with same peer count, might indicate partitioning
        for peer_count, nodes in peer_groups.items():
            if len(nodes) > 5 and peer_count < 40:  # 40 is 80% of 49 expected peers
                bottlenecks["network_partitions"].append(
                    {
                        "peer_count": peer_count,
                        "affected_nodes": nodes,
                        "partition_size": len(nodes),
                    }
                )

        # Check hub effectiveness
        for i in range(0, 50, 10):  # Check hub nodes (0, 10, 20, 30, 40)
            if i < len(self.servers):
                hub_peers = self.metrics.peer_counts.get(i, 0)
                if hub_peers < 15:  # Hubs should have good connectivity
                    bottlenecks["hub_failures"].append(
                        {"hub_node": i, "peer_count": hub_peers, "expected_min": 15}
                    )

        return bottlenecks

    async def run_focused_analysis(self, analysis_duration: int = 90) -> dict[str, Any]:
        """Run focused analysis of 50-node auto-discovery."""
        print("\n🔬 DEEP DIVE: 50-Node Auto-Discovery Analysis")
        print("=" * 60)

        # Create cluster
        servers = await self.create_50_node_multi_hub_cluster()

        # Wait for initial startup
        print("⏳ Waiting 10s for initial cluster startup...")
        await asyncio.sleep(10)

        self.metrics.discovery_start_time = time.time()

        # Analysis checkpoints
        checkpoints = [15, 30, 45, 60, 75, 90]  # seconds
        results = {}

        for checkpoint in checkpoints:
            if checkpoint > analysis_duration:
                break

            print(f"\n📊 CHECKPOINT: {checkpoint}s")
            await asyncio.sleep(
                checkpoint - (time.time() - self.metrics.discovery_start_time)
            )

            # Analyze current state
            connectivity = self.analyze_cluster_connectivity()
            bottlenecks = self.identify_convergence_bottlenecks()

            # Compact report
            peer_dist = connectivity["peer_distribution"]
            print(
                f"  Peer Discovery: min={peer_dist['min']}, max={peer_dist['max']}, avg={peer_dist['avg']:.1f}"
            )
            print(f"  Nodes <30 peers: {peer_dist['below_30']}/50")
            print(f"  Isolated nodes (<5 peers): {len(connectivity['isolated_nodes'])}")
            print(f"  Network partitions: {len(bottlenecks['network_partitions'])}")
            print(f"  Hub failures: {len(bottlenecks['hub_failures'])}")

            results[f"checkpoint_{checkpoint}s"] = {
                "timestamp": time.time(),
                "connectivity": connectivity,
                "bottlenecks": bottlenecks,
                "nodes_meeting_threshold": peer_dist["max"] >= 39,  # 80% of 49 peers
            }

        self.metrics.discovery_end_time = time.time()

        # Final analysis
        print("\n🎯 FINAL ANALYSIS")
        print("=" * 40)

        final_connectivity = self.analyze_cluster_connectivity()
        final_bottlenecks = self.identify_convergence_bottlenecks()

        success_threshold = 40  # 80% of 50 nodes
        nodes_with_good_discovery = len(
            [p for p in self.metrics.peer_counts.values() if p >= 39]
        )
        overall_success = nodes_with_good_discovery >= success_threshold

        print(f"SUCCESS: {overall_success}")
        print(f"Nodes with good discovery (≥39 peers): {nodes_with_good_discovery}/50")
        print(f"Required for success: {success_threshold}/50")

        if not overall_success:
            print("\n❌ ROOT CAUSE ANALYSIS:")
            for partition in final_bottlenecks["network_partitions"]:
                print(
                    f"  • Partition: {partition['partition_size']} nodes stuck at {partition['peer_count']} peers"
                )
            for hub_fail in final_bottlenecks["hub_failures"]:
                print(
                    f"  • Hub {hub_fail['hub_node']} underperforming: {hub_fail['peer_count']} peers"
                )

            if final_connectivity["isolated_nodes"]:
                print(f"  • Isolated nodes: {final_connectivity['isolated_nodes']}")

        # Clean shutdown
        print("\n🛑 Shutting down cluster...")
        for i, server in enumerate(servers):
            try:
                await server.shutdown_async()
            except Exception as e:
                if i < 5:  # Only report first few errors
                    print(f"Shutdown error {i}: {e}")

        return {
            "success": overall_success,
            "final_state": final_connectivity,
            "bottlenecks": final_bottlenecks,
            "checkpoints": results,
            "metrics": self.metrics,
        }


async def main():
    """Run the focused 50-node auto-discovery analysis."""
    analyzer = AutoDiscoveryAnalyzer()
    results = await analyzer.run_focused_analysis(90)

    print("\n🏁 ANALYSIS COMPLETE")
    print("=" * 30)
    print(f"Overall Success: {results['success']}")

    if not results["success"]:
        print("\n💡 RECOMMENDATIONS:")
        bottlenecks = results["bottlenecks"]

        if bottlenecks["network_partitions"]:
            print(
                "  1. Network partitioning detected - review gossip propagation algorithm"
            )
            print("  2. Consider increasing gossip frequency for large clusters")

        if bottlenecks["hub_failures"]:
            print("  3. Hub node underperformance - review multi-hub topology")
            print("  4. Consider adaptive peer connection strategies")

        final_state = results["final_state"]
        if final_state["peer_distribution"]["avg"] < 30:
            print("  5. Overall low peer discovery - may need longer convergence time")
            print("  6. Consider exponential backoff in gossip intervals")


if __name__ == "__main__":
    asyncio.run(main())
