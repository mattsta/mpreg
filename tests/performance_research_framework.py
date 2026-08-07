"""
Performance Research Framework for MPREG Distributed Systems.

This module provides comprehensive performance analysis, metrics collection,
and research capabilities for understanding distributed system behavior
across different topologies and scales.

Features:
- Real-time performance monitoring
- Statistical analysis and trend detection
- Network topology analysis
- Resource utilization tracking
- Scalability boundary detection
- Performance regression testing
"""

import asyncio
import json
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import networkx as nx
import psutil

from mpreg.server import MPREGServer


@dataclass
class TopologyEdge:
    """Typed edge identifier for topology metrics."""

    source_id: str
    target_id: str


@dataclass
class NetworkTopologyMetrics:
    """Network topology analysis metrics."""

    node_count: int
    edge_count: int
    diameter: int  # Maximum shortest path between any two nodes
    average_path_length: float
    clustering_coefficient: float
    density: float  # Actual edges / possible edges
    centrality_scores: dict[str, float] = field(default_factory=dict)
    connected_components: int = 0
    bridge_edges: list[TopologyEdge] = field(default_factory=list)
    articulation_points: list[str] = field(default_factory=list)


@dataclass
class ResourceUtilizationMetrics:
    """System resource utilization metrics."""

    timestamp: float
    cpu_percent: float
    memory_mb: float
    network_bytes_sent: int
    network_bytes_recv: int
    disk_io_read: int
    disk_io_write: int
    open_file_descriptors: int
    thread_count: int


@dataclass
class PerformanceTrend:
    """Performance trend analysis."""

    metric_name: str
    values: list[float]
    timestamps: list[float]
    trend_direction: str  # "increasing", "decreasing", "stable"
    slope: float
    r_squared: float
    anomalies: list[int] = field(default_factory=list)  # Indices of anomalous values


class RealTimePerformanceMonitor:
    """Real-time performance monitoring for MPREG servers."""

    def __init__(self, servers: list[MPREGServer], sampling_interval: float = 1.0):
        self.servers = servers
        self.sampling_interval = sampling_interval
        self.monitoring = False
        self.metrics_history: list[ResourceUtilizationMetrics] = []
        self.network_metrics_history: list[NetworkTopologyMetrics] = []
        self.performance_trends: dict[str, PerformanceTrend] = {}

    async def start_monitoring(self):
        """Start real-time performance monitoring."""
        self.monitoring = True
        print(
            f"🔍 Starting real-time performance monitoring ({self.sampling_interval}s intervals)"
        )

        while self.monitoring:
            await self._collect_metrics_sample()
            await asyncio.sleep(self.sampling_interval)

    def stop_monitoring(self):
        """Stop performance monitoring."""
        self.monitoring = False
        print(
            f"⏹️  Stopped performance monitoring ({len(self.metrics_history)} samples collected)"
        )

    async def _collect_metrics_sample(self):
        """Collect a single metrics sample."""
        timestamp = time.time()

        # System resource metrics
        cpu_percent = psutil.cpu_percent()
        memory_info = psutil.virtual_memory()
        network_io = psutil.net_io_counters()
        disk_io = psutil.disk_io_counters()

        resource_metrics = ResourceUtilizationMetrics(
            timestamp=timestamp,
            cpu_percent=cpu_percent,
            memory_mb=memory_info.used / (1024 * 1024),
            network_bytes_sent=network_io.bytes_sent,
            network_bytes_recv=network_io.bytes_recv,
            disk_io_read=disk_io.read_bytes if disk_io else 0,
            disk_io_write=disk_io.write_bytes if disk_io else 0,
            open_file_descriptors=len(psutil.Process().open_files()),
            thread_count=psutil.Process().num_threads(),
        )

        self.metrics_history.append(resource_metrics)

        # Network topology metrics
        network_metrics = await self._analyze_network_topology()
        self.network_metrics_history.append(network_metrics)

        # Update performance trends
        self._update_performance_trends(resource_metrics)

    async def _analyze_network_topology(self) -> NetworkTopologyMetrics:
        """Analyze the current network topology of servers."""

        # Build NetworkX graph from server connections
        G: nx.Graph = nx.Graph()

        for server in self.servers:
            server_url = f"ws://127.0.0.1:{server.settings.port}"
            G.add_node(server_url)

            # Add edges for peer connections
            for peer_url, connection in server.peer_connections.items():
                if connection.is_connected:
                    G.add_edge(server_url, peer_url)

        # Calculate topology metrics
        node_count = G.number_of_nodes()
        edge_count = G.number_of_edges()

        if node_count > 1:
            try:
                diameter = nx.diameter(G) if nx.is_connected(G) else float("inf")
                avg_path_length = (
                    nx.average_shortest_path_length(G)
                    if nx.is_connected(G)
                    else float("inf")
                )
                clustering_coeff = nx.average_clustering(G)
            except nx.NetworkXError, ZeroDivisionError, ValueError:
                # Handle disconnected graphs or computation errors
                diameter = 0
                avg_path_length = 0.0
                clustering_coeff = 0.0
        else:
            diameter = 0
            avg_path_length = 0.0
            clustering_coeff = 0.0

        density = nx.density(G)
        connected_components = nx.number_connected_components(G)

        # Find bridge edges and articulation points
        bridge_edges = [
            TopologyEdge(source_id=source, target_id=target)
            for source, target in nx.bridges(G)
        ]
        articulation_points = list(nx.articulation_points(G))

        # Calculate centrality scores
        centrality_scores = {}
        if node_count > 1:
            try:
                betweenness_centrality = nx.betweenness_centrality(G)
                centrality_scores = {
                    node: score for node, score in betweenness_centrality.items()
                }
            except nx.NetworkXError, ValueError, KeyError:
                # Handle graph computation errors or empty graphs
                centrality_scores = {}

        return NetworkTopologyMetrics(
            node_count=node_count,
            edge_count=edge_count,
            diameter=int(diameter) if diameter is not None else 0,
            average_path_length=avg_path_length,
            clustering_coefficient=clustering_coeff,
            density=density,
            centrality_scores=centrality_scores,
            connected_components=connected_components,
            bridge_edges=bridge_edges,
            articulation_points=articulation_points,
        )

    def _update_performance_trends(self, metrics: ResourceUtilizationMetrics):
        """Update performance trend analysis."""

        # Track key metrics for trend analysis
        trend_metrics = {
            "cpu_percent": metrics.cpu_percent,
            "memory_mb": metrics.memory_mb,
            "network_bytes_sent": metrics.network_bytes_sent,
            "network_bytes_recv": metrics.network_bytes_recv,
        }

        for metric_name, value in trend_metrics.items():
            if metric_name not in self.performance_trends:
                self.performance_trends[metric_name] = PerformanceTrend(
                    metric_name=metric_name,
                    values=[],
                    timestamps=[],
                    trend_direction="stable",
                    slope=0.0,
                    r_squared=0.0,
                )

            trend = self.performance_trends[metric_name]
            trend.values.append(value)
            trend.timestamps.append(metrics.timestamp)

            # Keep only recent values (last 100 samples)
            if len(trend.values) > 100:
                trend.values.pop(0)
                trend.timestamps.pop(0)

            # Update trend analysis if we have enough data
            if len(trend.values) >= 10:
                self._analyze_trend(trend)

    def _analyze_trend(self, trend: PerformanceTrend):
        """Analyze performance trend using linear regression."""
        if len(trend.values) < 2:
            return

        # Simple linear regression
        n = len(trend.values)
        x = list(range(n))
        y = trend.values

        x_mean = sum(x) / n
        y_mean = sum(y) / n

        numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            trend.slope = 0.0
            trend.r_squared = 0.0
        else:
            trend.slope = numerator / denominator

            # Calculate R²
            y_pred = [trend.slope * (x[i] - x_mean) + y_mean for i in range(n)]
            ss_res = sum((y[i] - y_pred[i]) ** 2 for i in range(n))
            ss_tot = sum((y[i] - y_mean) ** 2 for i in range(n))

            trend.r_squared = 1.0 - (ss_res / ss_tot) if ss_tot != 0 else 0.0

        # Determine trend direction
        if abs(trend.slope) < 0.01:  # Threshold for "stable"
            trend.trend_direction = "stable"
        elif trend.slope > 0:
            trend.trend_direction = "increasing"
        else:
            trend.trend_direction = "decreasing"

        # Detect anomalies (values > 2 standard deviations from mean)
        if len(trend.values) >= 10:
            mean_val = statistics.mean(trend.values)
            std_val = statistics.stdev(trend.values)
            threshold = 2 * std_val

            trend.anomalies.clear()
            for i, value in enumerate(trend.values):
                if abs(value - mean_val) > threshold:
                    trend.anomalies.append(i)

    def get_performance_summary(self) -> dict[str, Any]:
        """Get comprehensive performance summary."""
        if not self.metrics_history:
            return {"error": "No metrics collected"}

        # Calculate summary statistics
        cpu_values = [m.cpu_percent for m in self.metrics_history]
        memory_values = [m.memory_mb for m in self.metrics_history]

        latest_network = (
            self.network_metrics_history[-1] if self.network_metrics_history else None
        )

        summary = {
            "monitoring_duration_seconds": (
                self.metrics_history[-1].timestamp - self.metrics_history[0].timestamp
            )
            if len(self.metrics_history) > 1
            else 0,
            "samples_collected": len(self.metrics_history),
            "cpu_utilization": {
                "current": cpu_values[-1] if cpu_values else 0,
                "average": statistics.mean(cpu_values) if cpu_values else 0,
                "peak": max(cpu_values) if cpu_values else 0,
                "std_dev": statistics.stdev(cpu_values) if len(cpu_values) > 1 else 0,
            },
            "memory_utilization": {
                "current_mb": memory_values[-1] if memory_values else 0,
                "average_mb": statistics.mean(memory_values) if memory_values else 0,
                "peak_mb": max(memory_values) if memory_values else 0,
                "std_dev_mb": statistics.stdev(memory_values)
                if len(memory_values) > 1
                else 0,
            },
            "network_topology": {
                "nodes": latest_network.node_count if latest_network else 0,
                "edges": latest_network.edge_count if latest_network else 0,
                "density": latest_network.density if latest_network else 0,
                "diameter": latest_network.diameter if latest_network else 0,
                "clustering_coefficient": latest_network.clustering_coefficient
                if latest_network
                else 0,
                "connected_components": latest_network.connected_components
                if latest_network
                else 0,
            },
            "performance_trends": {
                name: {
                    "direction": trend.trend_direction,
                    "slope": trend.slope,
                    "r_squared": trend.r_squared,
                    "anomalies_count": len(trend.anomalies),
                }
                for name, trend in self.performance_trends.items()
            },
        }

        return summary

    def export_detailed_metrics(self, filename: str):
        """Export detailed metrics to JSON file."""
        export_data = {
            "metadata": {
                "servers_monitored": len(self.servers),
                "sampling_interval": self.sampling_interval,
                "total_samples": len(self.metrics_history),
                "monitoring_start": self.metrics_history[0].timestamp
                if self.metrics_history
                else 0,
                "monitoring_end": self.metrics_history[-1].timestamp
                if self.metrics_history
                else 0,
            },
            "resource_metrics": [
                {
                    "timestamp": m.timestamp,
                    "cpu_percent": m.cpu_percent,
                    "memory_mb": m.memory_mb,
                    "network_bytes_sent": m.network_bytes_sent,
                    "network_bytes_recv": m.network_bytes_recv,
                    "disk_io_read": m.disk_io_read,
                    "disk_io_write": m.disk_io_write,
                    "open_file_descriptors": m.open_file_descriptors,
                    "thread_count": m.thread_count,
                }
                for m in self.metrics_history
            ],
            "network_topology_history": [
                {
                    "node_count": n.node_count,
                    "edge_count": n.edge_count,
                    "diameter": n.diameter,
                    "average_path_length": n.average_path_length,
                    "clustering_coefficient": n.clustering_coefficient,
                    "density": n.density,
                    "connected_components": n.connected_components,
                    "bridge_edges_count": len(n.bridge_edges),
                    "articulation_points_count": len(n.articulation_points),
                }
                for n in self.network_metrics_history
            ],
            "performance_trends": {
                name: {
                    "metric_name": trend.metric_name,
                    "values": trend.values,
                    "timestamps": trend.timestamps,
                    "trend_direction": trend.trend_direction,
                    "slope": trend.slope,
                    "r_squared": trend.r_squared,
                    "anomalies": trend.anomalies,
                }
                for name, trend in self.performance_trends.items()
            },
        }

        with open(filename, "w") as f:
            json.dump(export_data, f, indent=2)

        print(f"📊 Detailed performance metrics exported to {filename}")


class ScalabilityBenchmark:
    """Scalability benchmark suite for distributed systems."""

    def __init__(self):
        self.benchmark_results: list[dict[str, Any]] = []

    async def run_scalability_benchmark(
        self,
        topology_builder,
        cluster_sizes: list[int],
        topology_types: list[str],
        iterations: int = 3,
    ) -> list[dict[str, Any]]:
        """Run comprehensive scalability benchmark."""

        print("🚀 Starting scalability benchmark")
        print(f"   Cluster sizes: {cluster_sizes}")
        print(f"   Topology types: {topology_types}")
        print(f"   Iterations per configuration: {iterations}")

        benchmark_results = []

        for topology_type in topology_types:
            for cluster_size in cluster_sizes:
                print(f"\n📊 Benchmarking {topology_type} with {cluster_size} nodes...")

                iteration_results = []

                for iteration in range(iterations):
                    print(f"   Iteration {iteration + 1}/{iterations}")

                    # Run single benchmark iteration
                    result = await self._run_single_benchmark(
                        topology_builder, topology_type, cluster_size, iteration
                    )

                    iteration_results.append(result)

                    # Brief pause between iterations
                    await asyncio.sleep(1.0)

                # Calculate statistics across iterations
                aggregated_result = self._aggregate_iteration_results(
                    topology_type, cluster_size, iteration_results
                )

                benchmark_results.append(aggregated_result)

        self.benchmark_results = benchmark_results
        return benchmark_results

    async def _run_single_benchmark(
        self, topology_builder, topology_type: str, cluster_size: int, iteration: int
    ) -> dict[str, Any]:
        """Run a single benchmark iteration."""

        start_time = time.time()

        # Allocate ports
        ports = topology_builder.port_allocator.allocate_port_range(
            cluster_size, "servers"
        )

        # Create cluster
        servers = await topology_builder._create_gossip_cluster(
            ports, f"benchmark-{topology_type.lower()}-{iteration}", topology_type
        )

        setup_time = time.time() - start_time

        # Start performance monitoring
        monitor = RealTimePerformanceMonitor(servers, sampling_interval=0.5)
        monitor_task = asyncio.create_task(monitor.start_monitoring())

        # Wait for convergence
        convergence_start = time.time()
        await asyncio.sleep(max(3.0, cluster_size * 0.2))
        convergence_time = time.time() - convergence_start

        # Test function propagation
        def benchmark_function(data: str) -> str:
            return f"Benchmark test: {data}"

        propagation_start = time.time()
        servers[0].register_command(
            "benchmark_test", benchmark_function, ["benchmark-resource"]
        )
        await asyncio.sleep(max(2.0, cluster_size * 0.1))

        # Count successful propagations
        nodes_with_function = sum(
            1 for server in servers if "benchmark_test" in server.cluster.funtimes
        )
        propagation_time = time.time() - propagation_start
        propagation_success_rate = nodes_with_function / cluster_size

        # Stop monitoring and collect metrics
        monitor.stop_monitoring()
        monitor_task.cancel()

        performance_summary = monitor.get_performance_summary()

        # Calculate connection metrics
        total_connections = sum(len(server.peer_connections) for server in servers)
        theoretical_max = cluster_size * (cluster_size - 1)
        connection_efficiency = (
            total_connections / theoretical_max if theoretical_max > 0 else 0
        )

        # Release ports
        for port in ports:
            topology_builder.port_allocator.release_port(port)

        return {
            "topology_type": topology_type,
            "cluster_size": cluster_size,
            "iteration": iteration,
            "setup_time_ms": setup_time * 1000,
            "convergence_time_ms": convergence_time * 1000,
            "propagation_time_ms": propagation_time * 1000,
            "propagation_success_rate": propagation_success_rate,
            "total_connections": total_connections,
            "connection_efficiency": connection_efficiency,
            "performance_summary": performance_summary,
        }

    def _aggregate_iteration_results(
        self,
        topology_type: str,
        cluster_size: int,
        iteration_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Aggregate results across multiple iterations."""

        # Extract metrics for aggregation
        setup_times = [r["setup_time_ms"] for r in iteration_results]
        convergence_times = [r["convergence_time_ms"] for r in iteration_results]
        propagation_times = [r["propagation_time_ms"] for r in iteration_results]
        success_rates = [r["propagation_success_rate"] for r in iteration_results]
        connection_efficiencies = [
            r["connection_efficiency"] for r in iteration_results
        ]

        return {
            "topology_type": topology_type,
            "cluster_size": cluster_size,
            "iterations": len(iteration_results),
            "setup_time_ms": {
                "mean": statistics.mean(setup_times),
                "std_dev": statistics.stdev(setup_times) if len(setup_times) > 1 else 0,
                "min": min(setup_times),
                "max": max(setup_times),
            },
            "convergence_time_ms": {
                "mean": statistics.mean(convergence_times),
                "std_dev": statistics.stdev(convergence_times)
                if len(convergence_times) > 1
                else 0,
                "min": min(convergence_times),
                "max": max(convergence_times),
            },
            "propagation_time_ms": {
                "mean": statistics.mean(propagation_times),
                "std_dev": statistics.stdev(propagation_times)
                if len(propagation_times) > 1
                else 0,
                "min": min(propagation_times),
                "max": max(propagation_times),
            },
            "propagation_success_rate": {
                "mean": statistics.mean(success_rates),
                "std_dev": statistics.stdev(success_rates)
                if len(success_rates) > 1
                else 0,
                "min": min(success_rates),
                "max": max(success_rates),
            },
            "connection_efficiency": {
                "mean": statistics.mean(connection_efficiencies),
                "std_dev": statistics.stdev(connection_efficiencies)
                if len(connection_efficiencies) > 1
                else 0,
                "min": min(connection_efficiencies),
                "max": max(connection_efficiencies),
            },
        }

    def export_benchmark_results(self, filename: str):
        """Export benchmark results to JSON file."""
        export_data = {
            "metadata": {
                "benchmark_timestamp": datetime.now().isoformat(),
                "total_configurations": len(self.benchmark_results),
            },
            "results": self.benchmark_results,
        }

        with open(filename, "w") as f:
            json.dump(export_data, f, indent=2)

        print(f"📊 Benchmark results exported to {filename}")

    def generate_performance_report(self) -> str:
        """Generate human-readable performance report."""
        if not self.benchmark_results:
            return "No benchmark results available."

        report_lines = [
            "=" * 80,
            "SCALABILITY BENCHMARK PERFORMANCE REPORT",
            "=" * 80,
            "",
        ]

        # Group results by topology type
        topology_groups = defaultdict(list)
        for result in self.benchmark_results:
            topology_groups[result["topology_type"]].append(result)

        for topology_type, results in topology_groups.items():
            report_lines.extend([f"🏗️  {topology_type} TOPOLOGY ANALYSIS:", "-" * 40])

            for result in results:
                size = result["cluster_size"]
                convergence = result["convergence_time_ms"]["mean"]
                propagation = result["propagation_time_ms"]["mean"]
                efficiency = result["connection_efficiency"]["mean"]
                success_rate = result["propagation_success_rate"]["mean"]

                report_lines.append(
                    f"   {size:2d} nodes: "
                    f"convergence={convergence:5.0f}ms, "
                    f"propagation={propagation:5.0f}ms, "
                    f"efficiency={efficiency:.2%}, "
                    f"success={success_rate:.2%}"
                )

            report_lines.append("")

        # Find optimal configurations
        report_lines.extend(["🏆 OPTIMAL CONFIGURATIONS:", "-" * 40])

        # Best convergence time
        best_convergence = min(
            self.benchmark_results, key=lambda r: r["convergence_time_ms"]["mean"]
        )
        report_lines.append(
            f"   Fastest convergence: {best_convergence['topology_type']} "
            f"({best_convergence['cluster_size']} nodes) - "
            f"{best_convergence['convergence_time_ms']['mean']:.0f}ms"
        )

        # Best efficiency
        best_efficiency = max(
            self.benchmark_results, key=lambda r: r["connection_efficiency"]["mean"]
        )
        report_lines.append(
            f"   Best efficiency: {best_efficiency['topology_type']} "
            f"({best_efficiency['cluster_size']} nodes) - "
            f"{best_efficiency['connection_efficiency']['mean']:.2%}"
        )

        # Most reliable
        best_reliability = max(
            self.benchmark_results, key=lambda r: r["propagation_success_rate"]["mean"]
        )
        report_lines.append(
            f"   Most reliable: {best_reliability['topology_type']} "
            f"({best_reliability['cluster_size']} nodes) - "
            f"{best_reliability['propagation_success_rate']['mean']:.2%} success"
        )

        report_lines.extend(["", "=" * 80])

        return "\n".join(report_lines)
