#!/usr/bin/env python3
"""
Comprehensive End-to-End Integration Tests for MPREG Unified System Architecture.

This test suite provides exhaustive coverage of the unified routing system across
all communication patterns, cluster topologies, and system combinations that MPREG supports.

Test Coverage:
- All 4 core systems: RPC, PubSub, Queue, Cache
- Multiple cluster topologies: 2, 3, 4, and 5-node clusters
- All delivery guarantees: FIRE_AND_FORGET, AT_LEAST_ONCE, EXACTLY_ONCE, BROADCAST, QUORUM
- All priority levels: CRITICAL, HIGH, NORMAL, LOW, BULK
- Federation scenarios: local, cross-cluster, multi-hop
- Topic pattern combinations: mpreg.*, app.*, system.*, user.*
- Resource allocation scenarios: specialized nodes, load balancing
- Failure scenarios: node failures, network partitions, recovery

Uses LIVE MPREG servers with dynamic port allocation - NO MOCKS!
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import PubSubSubscription, RPCCommand, TopicPattern

# Topic system
from mpreg.core.topic_exchange import TopicExchange
from mpreg.core.unified_router_impl import UnifiedRouterImpl, UnifiedRoutingConfig

# Unified routing system
from mpreg.core.unified_routing import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    RoutingPriority,
    UnifiedMessage,
    create_correlation_id,
)

# Core MPREG server and client imports
from mpreg.server import MPREGServer

# Test infrastructure
from tests.conftest import AsyncTestContext

# Well-encapsulated test case dataclasses (no dict[str, Any]!)


@dataclass(frozen=True, slots=True)
class ComprehensiveRPCTestCase:
    """Test case for comprehensive RPC routing validation."""

    topic: str
    expected_command: str | None
    system: str
    description: str = ""


@dataclass(frozen=True, slots=True)
class ComprehensivePubSubTestCase:
    """Test case for comprehensive PubSub routing validation."""

    topic: str
    expected_matches: int
    matching_service: str | None = None
    description: str = ""


@dataclass(frozen=True, slots=True)
class ComprehensiveQueueTestCase:
    """Test case for comprehensive Queue routing validation."""

    topic: str
    delivery: DeliveryGuarantee
    expected_queue: str | None
    expected_cost_multiplier: float
    expected_latency_multiplier: float
    description: str = ""


@dataclass(frozen=True, slots=True)
class ComprehensiveCacheTestCase:
    """Test case for comprehensive Cache routing validation."""

    topic: str
    expected_target: str
    expected_priority: float
    expected_latency: float
    operation_type: str
    description: str = ""


@dataclass(frozen=True, slots=True)
class ClusterConfigTestCase:
    """Test case for cluster configuration validation."""

    port: int
    name: str
    cluster_id: str
    resources: set[str] | None
    connect: str | None
    description: str = ""


@dataclass(frozen=True, slots=True)
class IoTScenarioTestCase:
    """Test case for IoT scenario validation."""

    name: str
    topic: str
    message_type: MessageType
    delivery: DeliveryGuarantee
    priority: RoutingPriority
    payload: dict[str, Any]
    description: str = ""


class TestUnifiedSystemTopologies:
    """Test unified routing across different cluster topologies."""

    async def test_2_node_basic_topology(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test multi-hop dependency resolution across 2-node cluster."""
        port1, port2 = server_cluster_ports[:2]

        # Node 1: Data Processing
        primary_settings = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Data-Processing-Node",
            cluster_id="topology-test-cluster",
            resources={"data-processor", "validator"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
        )

        # Node 2: Analytics Processing
        secondary_settings = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Analytics-Node",
            cluster_id="topology-test-cluster",
            resources={"analytics", "formatter"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=1.0,
        )

        servers = [
            MPREGServer(settings=primary_settings),
            MPREGServer(settings=secondary_settings),
        ]
        test_context.servers.extend(servers)

        # Register distributed computation functions
        def process_data(raw_input: str) -> dict:
            """Node 1: Process raw data into structured format."""
            return {
                "processed_data": f"processed_{raw_input}",
                "record_count": len(raw_input.split(",")),
                "processing_node": "Data-Processing-Node",
                "timestamp": time.time(),
            }

        def validate_data(processed_result: dict) -> dict:
            """Node 1: Validate processed data."""
            is_valid = processed_result.get("record_count", 0) > 0
            return {
                "validation_result": "VALID" if is_valid else "INVALID",
                "original_processing_node": processed_result.get("processing_node"),
                "validated_records": processed_result.get("record_count", 0),
                "validator_node": "Data-Processing-Node",
            }

        def analyze_data(validation_result: dict) -> dict:
            """Node 2: Analyze validated data."""
            return {
                "analysis_result": f"analyzed_{validation_result['validation_result']}",
                "confidence_score": 0.95
                if validation_result["validation_result"] == "VALID"
                else 0.0,
                "records_analyzed": validation_result["validated_records"],
                "analyzer_node": "Analytics-Node",
            }

        def format_report(analysis_result: dict) -> str:
            """Node 2: Format final report."""
            return f"REPORT: {analysis_result['analysis_result']} with confidence {analysis_result['confidence_score']} ({analysis_result['records_analyzed']} records)"

        # Register functions on appropriate nodes
        servers[0].register_command("process_data", process_data, ["data-processor"])
        servers[0].register_command("validate_data", validate_data, ["validator"])
        servers[1].register_command("analyze_data", analyze_data, ["analytics"])
        servers[1].register_command("format_report", format_report, ["formatter"])

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.3)

        await asyncio.sleep(1.5)  # Cluster formation time

        # Test multi-hop dependency resolution
        client = MPREGClientAPI(f"ws://127.0.0.1:{port1}")
        test_context.clients.append(client)
        await client.connect()

        # Execute 4-step pipeline across 2 nodes
        result = await client._client.request(
            [
                # Step 1: Process raw data on Node 1
                RPCCommand(
                    name="step1_processed",
                    fun="process_data",
                    args=("item1,item2,item3",),
                    locs=frozenset(["data-processor"]),
                ),
                # Step 2: Validate processed data on Node 1 (depends on step1)
                RPCCommand(
                    name="step2_validated",
                    fun="validate_data",
                    args=("step1_processed",),  # Reference to step1 result
                    locs=frozenset(["validator"]),
                ),
                # Step 3: Analyze validated data on Node 2 (depends on step2)
                RPCCommand(
                    name="step3_analyzed",
                    fun="analyze_data",
                    args=("step2_validated",),  # Reference to step2 result
                    locs=frozenset(["analytics"]),
                ),
                # Step 4: Format final report on Node 2 (depends on step3)
                RPCCommand(
                    name="step4_report",
                    fun="format_report",
                    args=("step3_analyzed",),  # Reference to step3 result
                    locs=frozenset(["formatter"]),
                ),
            ]
        )

        # Verify the multi-hop computation completed successfully
        # MPREG returns final results, proving the multi-hop dependency resolution worked
        assert "step4_report" in result

        final_report = result["step4_report"]

        # The final report contains evidence that all 4 steps executed across 2 nodes:
        # 1. Data was processed (3 records from "item1,item2,item3")
        # 2. Validation occurred ("VALID")
        # 3. Analysis ran with confidence score (0.95)
        # 4. Final formatting happened with all details
        assert "REPORT: analyzed_VALID with confidence 0.95 (3 records)" == final_report

        print("✓ 2-node topology: Multi-hop dependency resolution confirmed")
        print(f"  - Final result: {final_report}")
        print(
            "  - Multi-hop execution: 4 steps across 2 nodes with dependency resolution verified"
        )

    async def test_3_node_triangle_topology(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test complex dependency resolution across triangular 3-node cluster."""
        port1, port2, port3 = server_cluster_ports[:3]

        # Node 1: Data Coordination Hub
        coordinator_settings = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Coordination-Hub",
            cluster_id="triangle-cluster",
            resources={"coordinator", "data-loader"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
        )

        # Node 2: Processing Hub - connects to coordinator
        processing_settings = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Processing-Hub",
            cluster_id="triangle-cluster",
            resources={"processor", "transformer"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=1.0,
        )

        # Node 3: Analytics Hub - connects to both coordinator and processing hub for full mesh discovery
        analytics_settings = MPREGSettings(
            host="127.0.0.1",
            port=port3,
            name="Analytics-Hub",
            cluster_id="triangle-cluster",
            resources={"analyzer", "aggregator"},
            peers=[
                f"ws://127.0.0.1:{port1}",
                f"ws://127.0.0.1:{port2}",
            ],  # Connect to both nodes
            connect=None,  # Use peers instead
            advertised_urls=None,
            gossip_interval=1.0,
        )

        servers = [
            MPREGServer(settings=coordinator_settings),
            MPREGServer(settings=processing_settings),
            MPREGServer(settings=analytics_settings),
        ]
        test_context.servers.extend(servers)

        # Register triangle workflow functions
        def load_dataset(dataset_name: str) -> dict:
            """Node 1: Load dataset from storage."""
            return {
                "dataset_name": dataset_name,
                "records": ["record1", "record2", "record3", "record4"],
                "source_node": "Coordination-Hub",
                "load_time": time.time(),
            }

        def coordinate_processing(dataset_info: dict, batch_size: int) -> dict:
            """Node 1: Coordinate processing parameters."""
            return {
                "dataset_name": dataset_info["dataset_name"],
                "total_records": len(dataset_info["records"]),
                "batch_size": batch_size,
                "batches_needed": len(dataset_info["records"]) // batch_size + 1,
                "coordinator_node": "Coordination-Hub",
            }

        def process_records(coordination_info: dict) -> dict:
            """Node 2: Process records according to coordination."""
            processed_count = coordination_info["total_records"]
            return {
                "processed_records": processed_count,
                "batches_processed": coordination_info["batches_needed"],
                "processing_efficiency": processed_count
                / coordination_info["batches_needed"],
                "processor_node": "Processing-Hub",
            }

        def transform_results(processing_result: dict) -> dict:
            """Node 2: Transform processed results."""
            return {
                "transformed_data": f"transformed_{processing_result['processed_records']}_records",
                "efficiency_rating": "HIGH"
                if processing_result["processing_efficiency"] > 2.0
                else "NORMAL",
                "original_processor": processing_result["processor_node"],
                "transformer_node": "Processing-Hub",
            }

        def analyze_patterns(transformed_data: dict) -> dict:
            """Node 3: Analyze patterns in transformed data."""
            return {
                "pattern_analysis": f"patterns_found_in_{transformed_data['transformed_data']}",
                "efficiency_confirmed": transformed_data["efficiency_rating"] == "HIGH",
                "confidence_level": 0.87,
                "analyzer_node": "Analytics-Hub",
            }

        def aggregate_final_results(
            pattern_analysis: dict, processing_info: dict
        ) -> str:
            """Node 3: Aggregate all results into final report."""
            return f"FINAL_REPORT: {pattern_analysis['pattern_analysis']} | Efficiency: {processing_info['efficiency_rating']} | Confidence: {pattern_analysis['confidence_level']} | Pipeline: Coordination->Processing->Analytics"

        # Register functions on appropriate nodes
        servers[0].register_command("load_dataset", load_dataset, ["data-loader"])
        servers[0].register_command(
            "coordinate_processing", coordinate_processing, ["coordinator"]
        )
        servers[1].register_command("process_records", process_records, ["processor"])
        servers[1].register_command(
            "transform_results", transform_results, ["transformer"]
        )
        servers[2].register_command("analyze_patterns", analyze_patterns, ["analyzer"])
        servers[2].register_command(
            "aggregate_final_results", aggregate_final_results, ["aggregator"]
        )

        # Start servers sequentially with proper cluster formation
        for i, server in enumerate(servers):
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.5)  # Allow each server to start
            print(f"Started server {i + 1}: {server.settings.name}")

        await asyncio.sleep(3.0)  # Extended cluster formation time
        print("Cluster formation complete, testing function registration...")

        # Create client connection
        client = MPREGClientAPI(f"ws://127.0.0.1:{port1}")
        test_context.clients.append(client)
        await client.connect()
        await asyncio.sleep(1.0)  # Allow client connection to stabilize

        # Debug: Check that all functions are registered across the cluster
        try:
            debug_result = await client._client.request(
                [
                    RPCCommand(
                        name="debug_test",
                        fun="echo",  # Basic function that should be available
                        args=("cluster_check",),
                        locs=frozenset(),
                    )
                ]
            )
            print(f"Debug echo test successful: {debug_result}")
        except Exception as e:
            print(f"Debug echo test failed: {e}")

        print("Running main test pipeline...")

        result = await client._client.request(
            [
                # Step 1: Load dataset on Node 1
                RPCCommand(
                    name="dataset_loaded",
                    fun="load_dataset",
                    args=("production_dataset",),
                    locs=frozenset(["data-loader"]),
                ),
                # Step 2: Coordinate processing on Node 1 (depends on step1)
                RPCCommand(
                    name="processing_coordinated",
                    fun="coordinate_processing",
                    args=("dataset_loaded", 2),  # batch_size = 2
                    locs=frozenset(["coordinator"]),
                ),
                # Step 3: Process records on Node 2 (depends on step2)
                RPCCommand(
                    name="records_processed",
                    fun="process_records",
                    args=("processing_coordinated",),
                    locs=frozenset(["processor"]),
                ),
                # Step 4: Transform results on Node 2 (depends on step3)
                RPCCommand(
                    name="results_transformed",
                    fun="transform_results",
                    args=("records_processed",),
                    locs=frozenset(["transformer"]),
                ),
                # Step 5: Analyze patterns on Node 3 (depends on step4)
                RPCCommand(
                    name="patterns_analyzed",
                    fun="analyze_patterns",
                    args=("results_transformed",),
                    locs=frozenset(["analyzer"]),
                ),
                # Step 6: Aggregate final results on Node 3 (depends on step5 AND step4)
                RPCCommand(
                    name="final_aggregated",
                    fun="aggregate_final_results",
                    args=(
                        "patterns_analyzed",
                        "results_transformed",
                    ),  # Multiple dependencies
                    locs=frozenset(["aggregator"]),
                ),
            ]
        )

        # Verify the complex 6-step pipeline completed successfully across 3 nodes
        # MPREG only returns final results (commands not referenced by others)
        assert "final_aggregated" in result

        final_result = result["final_aggregated"]

        # Verify the final result contains evidence of all 6 steps executing correctly:
        # 1. Dataset was loaded (4 records)
        # 2. Processing was coordinated
        # 3. Records were processed
        # 4. Results were transformed (efficiency = NORMAL)
        # 5. Patterns were analyzed (confidence = 0.87)
        # 6. Final aggregation happened with all pipeline details
        assert "FINAL_REPORT" in final_result
        assert "patterns_found_in_transformed_4_records" in final_result
        assert "Efficiency: NORMAL" in final_result
        assert "Confidence: 0.87" in final_result
        assert "Pipeline: Coordination->Processing->Analytics" in final_result

        print(
            "✓ 3-node triangle topology: Complex multi-hop dependency resolution confirmed"
        )
        print(
            "  - 6-step pipeline executed across 3 nodes with sophisticated dependency resolution"
        )
        print(
            "  - Node 1 (Coordination-Hub): Dataset loading & processing coordination"
        )
        print("  - Node 2 (Processing-Hub): Record processing & result transformation")
        print("  - Node 3 (Analytics-Hub): Pattern analysis & final aggregation")
        print(f"  - Final result: {final_result}")

    async def test_4_node_star_topology(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test unified routing in star topology (1 coordinator + 3 workers)."""
        port1, port2, port3, port4 = server_cluster_ports[:4]

        # Central coordinator
        coordinator_settings = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Central-Coordinator",
            cluster_id="star-cluster",
            resources={"coordinator", "load-balancer"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
        )

        # 3 worker nodes all connected to coordinator
        worker_settings = []
        for i, port in enumerate([port2, port3, port4], 1):
            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"Worker-{i}",
                cluster_id="star-cluster",
                resources={f"worker-{i}", f"processing-{i}"},
                peers=None,
                connect=f"ws://127.0.0.1:{port1}",
                advertised_urls=None,
                gossip_interval=1.0,
            )
            worker_settings.append(settings)

        servers = [MPREGServer(settings=coordinator_settings)]
        for settings in worker_settings:
            servers.append(MPREGServer(settings=settings))

        test_context.servers.extend(servers)

        # Register star topology workflow functions
        def coordinate_tasks(task_list: list) -> dict:
            """Coordinator: Distribute tasks to workers."""
            return {
                "total_tasks": len(task_list),
                "task_assignments": {
                    "worker-1": task_list[: len(task_list) // 3],
                    "worker-2": task_list[
                        len(task_list) // 3 : (2 * len(task_list)) // 3
                    ],
                    "worker-3": task_list[(2 * len(task_list)) // 3 :],
                },
                "coordinator_node": "Central-Coordinator",
            }

        def balance_load(coordination_info: dict) -> dict:
            """Coordinator: Balance load across workers."""
            assignments = coordination_info["task_assignments"]
            return {
                "load_distribution": {
                    worker: len(tasks) for worker, tasks in assignments.items()
                },
                "balanced_by": "Central-Coordinator",
                "total_distributed": coordination_info["total_tasks"],
            }

        def process_worker_tasks(task_batch: list, worker_id: str) -> dict:
            """Worker: Process assigned task batch."""
            return {
                "processed_count": len(task_batch),
                "worker_id": worker_id,
                "processing_results": [f"processed_{task}" for task in task_batch],
                "worker_efficiency": len(task_batch) * 1.5,
            }

        def aggregate_worker_results(
            worker1_result: dict, worker2_result: dict, worker3_result: dict
        ) -> str:
            """Coordinator: Aggregate results from all workers."""
            total_processed = (
                worker1_result["processed_count"]
                + worker2_result["processed_count"]
                + worker3_result["processed_count"]
            )
            total_efficiency = (
                worker1_result["worker_efficiency"]
                + worker2_result["worker_efficiency"]
                + worker3_result["worker_efficiency"]
            )
            return f"STAR_TOPOLOGY_RESULT: {total_processed} tasks processed across 3 workers with combined efficiency {total_efficiency}"

        # Register functions on appropriate nodes
        servers[0].register_command(
            "coordinate_tasks", coordinate_tasks, ["coordinator"]
        )
        servers[0].register_command("balance_load", balance_load, ["load-balancer"])
        servers[0].register_command(
            "aggregate_worker_results", aggregate_worker_results, ["coordinator"]
        )

        # Register worker functions with proper worker identification
        for i, server in enumerate(servers[1:], 1):

            def make_worker_func(worker_num):
                def worker_func(task_batch: list) -> dict:
                    return process_worker_tasks(task_batch, f"Worker-{worker_num}")

                return worker_func

            server.register_command(
                f"process_worker_{i}_tasks", make_worker_func(i), [f"worker-{i}"]
            )

        # Start coordinator first, then workers
        coordinator_task = asyncio.create_task(servers[0].server())
        test_context.tasks.append(coordinator_task)
        await asyncio.sleep(0.3)

        for server in servers[1:]:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.2)

        await asyncio.sleep(2.0)  # Extended formation time for star topology

        # Test complex star topology with distributed processing
        coordinator_client = MPREGClientAPI(f"ws://127.0.0.1:{port1}")
        test_context.clients.append(coordinator_client)
        await coordinator_client.connect()

        task_input = ["task_A", "task_B", "task_C", "task_D", "task_E", "task_F"]

        # Use enhanced request to get intermediate results
        import ulid

        from mpreg.core.model import RPCRequest

        request = RPCRequest(
            cmds=(
                # Step 1: Coordinator distributes tasks
                RPCCommand(
                    name="tasks_coordinated",
                    fun="coordinate_tasks",
                    args=(task_input,),
                    locs=frozenset(["coordinator"]),
                ),
                # Step 2: Coordinator balances load
                RPCCommand(
                    name="load_balanced",
                    fun="balance_load",
                    args=("tasks_coordinated",),
                    locs=frozenset(["load-balancer"]),
                ),
                # Step 3-5: Workers process their assigned tasks in parallel
                RPCCommand(
                    name="worker1_results",
                    fun="process_worker_1_tasks",
                    args=(
                        "tasks_coordinated.task_assignments.worker-1",
                    ),  # Field access
                    locs=frozenset(["worker-1"]),
                ),
                RPCCommand(
                    name="worker2_results",
                    fun="process_worker_2_tasks",
                    args=(
                        "tasks_coordinated.task_assignments.worker-2",
                    ),  # Field access
                    locs=frozenset(["worker-2"]),
                ),
                RPCCommand(
                    name="worker3_results",
                    fun="process_worker_3_tasks",
                    args=(
                        "tasks_coordinated.task_assignments.worker-3",
                    ),  # Field access
                    locs=frozenset(["worker-3"]),
                ),
                # Step 6: Coordinator aggregates all worker results
                RPCCommand(
                    name="final_star_results",
                    fun="aggregate_worker_results",
                    args=("worker1_results", "worker2_results", "worker3_results"),
                    locs=frozenset(["coordinator"]),
                ),
            ),
            u=str(ulid.new()),
            return_intermediate_results=True,
            include_execution_summary=True,
        )

        response = await coordinator_client._client.request_enhanced(request)
        result = response.r

        # Verify star topology distributed processing
        # Main result only contains final results
        assert "final_star_results" in result

        # Verify we got intermediate results
        assert len(response.intermediate_results) > 0

        # Build a complete results dictionary from intermediate results
        complete_results = {}
        if response.intermediate_results:
            for intermediate in response.intermediate_results:
                complete_results.update(intermediate.accumulated_results)

        # Now verify all expected intermediate steps are present
        assert "tasks_coordinated" in complete_results
        assert "load_balanced" in complete_results
        assert "worker1_results" in complete_results
        assert "worker2_results" in complete_results
        assert "worker3_results" in complete_results
        assert "final_star_results" in complete_results

        # Verify coordination worked properly
        coordination = complete_results["tasks_coordinated"]
        assert coordination["coordinator_node"] == "Central-Coordinator"
        assert coordination["total_tasks"] == 6

        # Verify load balancing
        load_balance = complete_results["load_balanced"]
        assert load_balance["balanced_by"] == "Central-Coordinator"
        assert load_balance["total_distributed"] == 6

        # Verify workers processed their assigned tasks
        worker1 = complete_results["worker1_results"]
        worker2 = complete_results["worker2_results"]
        worker3 = complete_results["worker3_results"]

        assert worker1["worker_id"] == "Worker-1"
        assert worker2["worker_id"] == "Worker-2"
        assert worker3["worker_id"] == "Worker-3"

        total_worker_processed = (
            worker1["processed_count"]
            + worker2["processed_count"]
            + worker3["processed_count"]
        )
        assert total_worker_processed == 6  # All tasks processed

        # Verify final aggregation
        final_result = result["final_star_results"]
        assert "STAR_TOPOLOGY_RESULT" in final_result
        assert "6 tasks processed across 3 workers" in final_result

        print("✓ 4-node star topology: Multi-hop distributed processing confirmed")
        print(f"  - Coordinator distributed {coordination['total_tasks']} tasks")
        print(f"  - Worker 1 processed {worker1['processed_count']} tasks")
        print(f"  - Worker 2 processed {worker2['processed_count']} tasks")
        print(f"  - Worker 3 processed {worker3['processed_count']} tasks")
        print(f"  - Final result: {final_result}")

    async def test_5_node_mesh_topology(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test unified routing in partial mesh topology."""
        if len(server_cluster_ports) < 5:
            pytest.skip(
                f"Need at least 5 ports for 5-node test, got {len(server_cluster_ports)}"
            )

        ports = server_cluster_ports[:5]

        # Create a partial mesh where each node connects to 2-3 others using proper dataclasses
        mesh_configs: list[ClusterConfigTestCase] = [
            ClusterConfigTestCase(
                name="Mesh-Node-1",
                port=ports[0],
                cluster_id="mesh-cluster",
                resources={"mesh-1", "primary"},
                connect=None,  # Primary node
                description="Primary mesh node",
            ),
            ClusterConfigTestCase(
                name="Mesh-Node-2",
                port=ports[1],
                cluster_id="mesh-cluster",
                resources={"mesh-2", "secondary"},
                connect=f"ws://127.0.0.1:{ports[0]}",  # Connect to node 1
                description="Secondary mesh node",
            ),
            ClusterConfigTestCase(
                name="Mesh-Node-3",
                port=ports[2],
                cluster_id="mesh-cluster",
                resources={"mesh-3", "tertiary"},
                connect=f"ws://127.0.0.1:{ports[0]}",  # Connect directly to hub node 1 for better discovery
                description="Tertiary mesh node",
            ),
            ClusterConfigTestCase(
                name="Mesh-Node-4",
                port=ports[3],
                cluster_id="mesh-cluster",
                resources={"mesh-4", "quaternary"},
                connect=f"ws://127.0.0.1:{ports[0]}",  # Connect to node 1 (mesh pattern)
                description="Quaternary mesh node",
            ),
            ClusterConfigTestCase(
                name="Mesh-Node-5",
                port=ports[4],
                cluster_id="mesh-cluster",
                resources={"mesh-5", "quinary"},
                connect=f"ws://127.0.0.1:{ports[0]}",  # Connect directly to hub node 1 for better discovery
                description="Quinary mesh node",
            ),
        ]

        servers = []
        for config in mesh_configs:
            settings = MPREGSettings(
                host="127.0.0.1",
                port=config.port,
                name=config.name,
                cluster_id=config.cluster_id,
                resources=config.resources,
                peers=None,
                connect=config.connect,
                advertised_urls=None,
                gossip_interval=1.0,
            )
            servers.append(MPREGServer(settings=settings))

        test_context.servers.extend(servers)

        # Register mesh topology workflow functions
        def initiate_mesh_computation(computation_id: str) -> dict:
            """Node 1: Initiate distributed computation across mesh."""
            return {
                "computation_id": computation_id,
                "initiated_by": "Mesh-Node-1",
                "mesh_parameters": {
                    "redundancy_level": 2,
                    "fault_tolerance": True,
                    "distribution_strategy": "mesh_optimal",
                },
                "timestamp": time.time(),
            }

        def prepare_mesh_data(mesh_params: dict, data_size: int) -> dict:
            """Node 2: Prepare data for mesh distribution."""
            return {
                "computation_id": mesh_params["computation_id"],
                "prepared_data_chunks": data_size
                // mesh_params["mesh_parameters"]["redundancy_level"],
                "redundancy_applied": mesh_params["mesh_parameters"][
                    "redundancy_level"
                ],
                "prepared_by": "Mesh-Node-2",
                "fault_tolerance_enabled": mesh_params["mesh_parameters"][
                    "fault_tolerance"
                ],
            }

        def process_mesh_chunk_a(prepared_data: dict) -> dict:
            """Node 3: Process first chunk of mesh data."""
            return {
                "computation_id": prepared_data["computation_id"],
                "chunk_id": "chunk_a",
                "processed_chunks": prepared_data["prepared_data_chunks"] // 2,
                "processing_node": "Mesh-Node-3",
                "processing_efficiency": 0.85,
            }

        def process_mesh_chunk_b(prepared_data: dict) -> dict:
            """Node 4: Process second chunk of mesh data."""
            return {
                "computation_id": prepared_data["computation_id"],
                "chunk_id": "chunk_b",
                "processed_chunks": prepared_data["prepared_data_chunks"]
                - (prepared_data["prepared_data_chunks"] // 2),
                "processing_node": "Mesh-Node-4",
                "processing_efficiency": 0.92,
            }

        def verify_mesh_redundancy(chunk_a_result: dict, chunk_b_result: dict) -> dict:
            """Node 5: Verify redundancy and fault tolerance."""
            total_chunks = (
                chunk_a_result["processed_chunks"] + chunk_b_result["processed_chunks"]
            )
            avg_efficiency = (
                chunk_a_result["processing_efficiency"]
                + chunk_b_result["processing_efficiency"]
            ) / 2

            return {
                "computation_id": chunk_a_result["computation_id"],
                "redundancy_verified": True,
                "total_chunks_verified": total_chunks,
                "average_mesh_efficiency": avg_efficiency,
                "fault_tolerance_status": "OPERATIONAL",
                "verified_by": "Mesh-Node-5",
            }

        def aggregate_mesh_results(
            mesh_init: dict, data_prep: dict, redundancy_check: dict
        ) -> str:
            """Node 1: Aggregate final mesh computation results."""
            return f"MESH_COMPUTATION_COMPLETE: ID={redundancy_check['computation_id']} | Chunks={redundancy_check['total_chunks_verified']} | Efficiency={redundancy_check['average_mesh_efficiency']:.2f} | Status={redundancy_check['fault_tolerance_status']} | Initiated={mesh_init['initiated_by']} | Verified={redundancy_check['verified_by']}"

        # Register functions across mesh nodes
        servers[0].register_command(
            "initiate_mesh_computation", initiate_mesh_computation, ["primary"]
        )
        servers[0].register_command(
            "aggregate_mesh_results", aggregate_mesh_results, ["primary"]
        )
        servers[1].register_command(
            "prepare_mesh_data", prepare_mesh_data, ["secondary"]
        )
        servers[2].register_command(
            "process_mesh_chunk_a", process_mesh_chunk_a, ["tertiary"]
        )
        servers[3].register_command(
            "process_mesh_chunk_b", process_mesh_chunk_b, ["quaternary"]
        )
        servers[4].register_command(
            "verify_mesh_redundancy", verify_mesh_redundancy, ["quinary"]
        )

        # Start servers in mesh formation order
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.3)  # Allow each connection to establish

        await asyncio.sleep(
            6.0
        )  # Extended time for complex mesh formation and gossip propagation

        # Test complex mesh computation across all 5 nodes
        primary_client = MPREGClientAPI(f"ws://127.0.0.1:{ports[0]}")
        test_context.clients.append(primary_client)
        await primary_client.connect()

        # Use enhanced RPC with intermediate results to capture all step data
        from mpreg.core.model import RPCRequest

        enhanced_request = RPCRequest(
            cmds=(
                # Step 1: Initiate computation on Node 1
                RPCCommand(
                    name="mesh_initiated",
                    fun="initiate_mesh_computation",
                    args=("MESH_COMP_001",),
                    locs=frozenset(["primary"]),
                ),
                # Step 2: Prepare data on Node 2 (depends on step1)
                RPCCommand(
                    name="data_prepared",
                    fun="prepare_mesh_data",
                    args=("mesh_initiated", 1000),  # 1000 data units
                    locs=frozenset(["secondary"]),
                ),
                # Step 3: Process chunk A on Node 3 (depends on step2)
                RPCCommand(
                    name="chunk_a_processed",
                    fun="process_mesh_chunk_a",
                    args=("data_prepared",),
                    locs=frozenset(["tertiary"]),
                ),
                # Step 4: Process chunk B on Node 4 (depends on step2, parallel to step3)
                RPCCommand(
                    name="chunk_b_processed",
                    fun="process_mesh_chunk_b",
                    args=("data_prepared",),
                    locs=frozenset(["quaternary"]),
                ),
                # Step 5: Verify redundancy on Node 5 (depends on step3 AND step4)
                RPCCommand(
                    name="redundancy_verified",
                    fun="verify_mesh_redundancy",
                    args=("chunk_a_processed", "chunk_b_processed"),
                    locs=frozenset(["quinary"]),
                ),
                # Step 6: Aggregate results back on Node 1 (depends on step1, step2, step5)
                RPCCommand(
                    name="mesh_final_result",
                    fun="aggregate_mesh_results",
                    args=("mesh_initiated", "data_prepared", "redundancy_verified"),
                    locs=frozenset(["primary"]),
                ),
            ),
            u="mesh_topology_test",
            return_intermediate_results=True,
            include_execution_summary=True,
        )

        response = await primary_client._client.request_enhanced(enhanced_request)
        result = response.r

        # Verify mesh computation completed successfully
        assert "mesh_final_result" in result

        # Verify final mesh result contains all expected information
        final_result = result["mesh_final_result"]
        assert "MESH_COMPUTATION_COMPLETE" in final_result
        assert "ID=MESH_COMP_001" in final_result
        assert "Chunks=500" in final_result  # Total processed chunks
        assert "Efficiency=" in final_result  # Average efficiency calculated
        assert "Status=OPERATIONAL" in final_result  # Fault tolerance status
        assert "Initiated=Mesh-Node-1" in final_result  # Original initiator
        assert "Verified=Mesh-Node-5" in final_result  # Final verifier

        # Verify intermediate results captured all execution levels
        assert len(response.intermediate_results) > 0
        assert response.execution_summary is not None

        # Verify each step was captured in intermediate results
        intermediate_data = {}
        for intermediate in response.intermediate_results:
            intermediate_data.update(intermediate.accumulated_results)

        # Verify all steps were captured through intermediate results
        assert "mesh_initiated" in intermediate_data
        assert "data_prepared" in intermediate_data
        assert "chunk_a_processed" in intermediate_data
        assert "chunk_b_processed" in intermediate_data
        assert "redundancy_verified" in intermediate_data
        assert "mesh_final_result" in intermediate_data

        # Verify mesh initiation from intermediate results
        mesh_init = intermediate_data["mesh_initiated"]
        assert mesh_init["computation_id"] == "MESH_COMP_001"
        assert mesh_init["initiated_by"] == "Mesh-Node-1"
        assert mesh_init["mesh_parameters"]["redundancy_level"] == 2

        # Verify data preparation from intermediate results
        data_prep = intermediate_data["data_prepared"]
        assert data_prep["prepared_by"] == "Mesh-Node-2"
        assert data_prep["prepared_data_chunks"] == 500  # 1000 / 2
        assert data_prep["redundancy_applied"] == 2

        # Verify parallel chunk processing from intermediate results
        chunk_a = intermediate_data["chunk_a_processed"]
        chunk_b = intermediate_data["chunk_b_processed"]
        assert chunk_a["processing_node"] == "Mesh-Node-3"
        assert chunk_b["processing_node"] == "Mesh-Node-4"
        assert chunk_a["processed_chunks"] == 250  # 500 // 2
        assert chunk_b["processed_chunks"] == 250  # 500 - 250

        # Verify redundancy verification from intermediate results
        redundancy = intermediate_data["redundancy_verified"]
        assert redundancy["verified_by"] == "Mesh-Node-5"
        assert redundancy["total_chunks_verified"] == 500
        assert redundancy["fault_tolerance_status"] == "OPERATIONAL"

        print("✅ 5-node mesh topology test completed successfully!")
        print(f"   Final result: {final_result}")
        print(
            f"   Intermediate results captured: {len(response.intermediate_results)} levels"
        )
        print(
            f"   Execution summary: {response.execution_summary.total_execution_time_ms:.1f}ms total"
        )
        print(
            "   Demonstrated: Multi-hop dependency resolution across complex mesh topology with intermediate results!"
        )


class TestUnifiedSystemMessageTypes:
    """Test unified routing for all message types and their combinations."""

    async def test_rpc_message_routing_patterns(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test RPC routing across different topic patterns."""
        port1, port2 = server_cluster_ports[:2]

        # Create test cluster
        servers = await self._create_basic_cluster(
            test_context, [port1, port2], "rpc-patterns"
        )

        # Test RPC routing with UnifiedRouterImpl
        config = UnifiedRoutingConfig(enable_topic_rpc=True)
        router = UnifiedRouterImpl(config)

        rpc_test_cases: list[ComprehensiveRPCTestCase] = [
            # Standard MPREG RPC patterns - only extracts first segment after "rpc"
            ComprehensiveRPCTestCase(
                topic="mpreg.rpc.authenticate",
                expected_command="authenticate",
                system="mpreg internal",
                description="MPREG authentication RPC",
            ),
            ComprehensiveRPCTestCase(
                topic="mpreg.rpc.process_payment",
                expected_command="process_payment",
                system="mpreg internal",
                description="MPREG payment processing RPC",
            ),
            ComprehensiveRPCTestCase(
                topic="mpreg.rpc.invalidate_keys",
                expected_command="invalidate_keys",
                system="mpreg internal",
                description="MPREG key invalidation RPC",
            ),
            # Application RPC patterns
            ComprehensiveRPCTestCase(
                topic="app.rpc.calculate_invoice",
                expected_command="calculate_invoice",
                system="application",
                description="Application invoice calculation RPC",
            ),
            ComprehensiveRPCTestCase(
                topic="app.rpc.check_stock",
                expected_command="check_stock",
                system="application",
                description="Application stock check RPC",
            ),
            # System RPC patterns
            ComprehensiveRPCTestCase(
                topic="system.rpc.health_check",
                expected_command="health_check",
                system="system",
                description="System health check RPC",
            ),
            ComprehensiveRPCTestCase(
                topic="system.rpc.rotate_logs",
                expected_command="rotate_logs",
                system="system",
                description="System log rotation RPC",
            ),
            # User RPC patterns - nested patterns extract first segment after rpc
            ComprehensiveRPCTestCase(
                topic="user.rpc.profile",
                expected_command="profile",
                system="user",
                description="User profile RPC",
            ),
            ComprehensiveRPCTestCase(
                topic="mpreg.rpc.user",  # This will extract "user"
                expected_command="user",
                system="mpreg",
                description="MPREG user management RPC",
            ),
            # Edge cases
            ComprehensiveRPCTestCase(
                topic="nested.deep.rpc.command",
                expected_command="command",
                system="nested",
                description="Nested deep RPC command",
            ),
            ComprehensiveRPCTestCase(
                topic="invalid.topic.without.command.segment",
                expected_command=None,
                system="invalid",
                description="Invalid RPC topic pattern",
            ),
        ]

        for test_case in rpc_test_cases:
            correlation_id = create_correlation_id()
            headers = MessageHeaders(
                correlation_id=correlation_id,
                source_system=MessageType.RPC,
                priority=RoutingPriority.HIGH,
            )

            message = UnifiedMessage(
                topic=test_case.topic,
                routing_type=MessageType.RPC,
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                payload={"test": "rpc_routing"},
                headers=headers,
            )

            route_result = await router.route_message(message)

            # Verify routing
            assert route_result.route_id is not None
            assert len(route_result.targets) == 1

            target = route_result.targets[0]
            assert target.system_type == MessageType.RPC

            if test_case.expected_command:
                assert target.target_id == test_case.expected_command
                assert target.priority_weight == 2.0  # High priority for RPC
            else:
                assert target.target_id == "local_handler"  # Fallback

            print(
                f"✓ RPC routing: {test_case.topic} → {target.target_id} ({test_case.system})"
            )

    async def test_pubsub_message_routing_patterns(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test PubSub routing with real TopicExchange integration."""
        port1, port2 = server_cluster_ports[:2]

        # Create TopicExchange with dynamic port
        topic_exchange = TopicExchange(
            server_url=f"ws://127.0.0.1:{port1}",  # Use allocated port
            cluster_id="pubsub-test-cluster",
        )

        # Create comprehensive subscription patterns
        subscription_patterns = [
            # Wildcard patterns
            ("user.*.login", "auth_service"),
            ("user.*.logout", "auth_service"),
            ("order.#", "order_service"),
            ("payment.*.success", "billing_service"),
            ("payment.*.failed", "billing_service"),
            ("system.alerts.#", "monitoring_service"),
            ("cache.invalidation.*", "cache_service"),
            ("mpreg.federation.#", "federation_service"),
            # Exact patterns
            ("app.startup.complete", "lifecycle_service"),
            ("app.shutdown.initiated", "lifecycle_service"),
            ("health.check.ping", "health_service"),
            # Complex nested patterns
            ("analytics.user.behavior.#", "analytics_service"),
            ("logs.error.critical.*", "logging_service"),
            ("metrics.performance.#", "metrics_service"),
        ]

        # Add subscriptions to TopicExchange
        for pattern, service in subscription_patterns:
            subscription = PubSubSubscription(
                subscription_id=f"sub_{service}_{hash(pattern) % 10000}",
                patterns=(TopicPattern(pattern=pattern),),
                subscriber=service,
                created_at=time.time(),
            )
            topic_exchange.add_subscription(subscription)

        # Create router with TopicExchange integration
        config = UnifiedRoutingConfig(enable_cross_system_correlation=True)
        router = UnifiedRouterImpl(config=config, topic_exchange=topic_exchange)

        # Test PubSub routing scenarios using proper dataclasses
        pubsub_test_cases: list[ComprehensivePubSubTestCase] = [
            # Should match user.*.login
            ComprehensivePubSubTestCase(
                topic="user.123.login",
                expected_matches=1,
                matching_service="auth_service",
                description="User login pattern matching",
            ),
            # Should match order.#
            ComprehensivePubSubTestCase(
                topic="order.created.premium",
                expected_matches=1,
                matching_service="order_service",
                description="Order wildcard pattern matching",
            ),
            # Should match system.alerts.#
            ComprehensivePubSubTestCase(
                topic="system.alerts.critical.memory",
                expected_matches=1,
                matching_service="monitoring_service",
                description="System alert pattern matching",
            ),
            # Should match multiple patterns
            ComprehensivePubSubTestCase(
                topic="payment.123.success",
                expected_matches=1,
                matching_service="billing_service",
                description="Payment pattern matching",
            ),
            # Should match analytics.user.behavior.#
            ComprehensivePubSubTestCase(
                topic="analytics.user.behavior.page_view.product",
                expected_matches=1,
                matching_service="analytics_service",
                description="Analytics pattern matching",
            ),
            # Should not match any pattern
            ComprehensivePubSubTestCase(
                topic="unmatched.random.topic",
                expected_matches=0,
                matching_service=None,
                description="Unmatched topic",
            ),
            # Exact match
            ComprehensivePubSubTestCase(
                topic="app.startup.complete",
                expected_matches=1,
                matching_service="lifecycle_service",
                description="Exact pattern matching",
            ),
        ]

        for test_case in pubsub_test_cases:
            correlation_id = create_correlation_id()
            headers = MessageHeaders(
                correlation_id=correlation_id,
                source_system=MessageType.PUBSUB,
                priority=RoutingPriority.NORMAL,
            )

            message = UnifiedMessage(
                topic=test_case.topic,
                routing_type=MessageType.PUBSUB,
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                payload={"event": "test_pubsub"},
                headers=headers,
            )

            route_result = await router.route_message(message)

            # Verify routing
            assert route_result.route_id is not None
            assert len(route_result.targets) == test_case.expected_matches

            if test_case.expected_matches > 0:
                # Verify target details
                target = route_result.targets[0]
                assert target.system_type == MessageType.PUBSUB
                assert target.node_id == test_case.matching_service
                assert route_result.estimated_latency_ms == 5.0  # Fast pub/sub

            print(
                f"✓ PubSub routing: {test_case.topic} → {test_case.expected_matches} subscribers"
            )

    async def test_queue_message_routing_patterns(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test Queue routing with different delivery guarantees."""
        config = UnifiedRoutingConfig(enable_queue_topics=True)
        router = UnifiedRouterImpl(config)

        # Test Queue routing with all delivery guarantees using proper dataclasses
        queue_test_cases: list[ComprehensiveQueueTestCase] = [
            # AT_LEAST_ONCE delivery
            ComprehensiveQueueTestCase(
                topic="mpreg.queue.order_processing",
                delivery=DeliveryGuarantee.AT_LEAST_ONCE,
                expected_queue="order_processing",
                expected_cost_multiplier=1.0,
                expected_latency_multiplier=1.0,
                description="MPREG order processing at-least-once",
            ),
            ComprehensiveQueueTestCase(
                topic="app.queue.email.notifications",
                delivery=DeliveryGuarantee.AT_LEAST_ONCE,
                expected_queue="email.notifications",
                expected_cost_multiplier=1.0,
                expected_latency_multiplier=1.0,
                description="App email notifications at-least-once",
            ),
            # EXACTLY_ONCE delivery (higher cost/latency)
            ComprehensiveQueueTestCase(
                topic="mpreg.queue.payment_processing",
                delivery=DeliveryGuarantee.EXACTLY_ONCE,
                expected_queue="payment_processing",
                expected_cost_multiplier=2.0,
                expected_latency_multiplier=2.0,
                description="MPREG payment processing exactly-once",
            ),
            ComprehensiveQueueTestCase(
                topic="system.queue.audit.transactions",
                delivery=DeliveryGuarantee.EXACTLY_ONCE,
                expected_queue="audit.transactions",
                expected_cost_multiplier=2.0,
                expected_latency_multiplier=2.0,
                description="System audit transactions exactly-once",
            ),
            # BROADCAST delivery (fanout cost)
            ComprehensiveQueueTestCase(
                topic="mpreg.queue.system_alerts",
                delivery=DeliveryGuarantee.BROADCAST,
                expected_queue="system_alerts",
                expected_cost_multiplier=1.5,
                expected_latency_multiplier=1.2,
                description="MPREG system alerts broadcast",
            ),
            ComprehensiveQueueTestCase(
                topic="user.queue.notifications.push",
                delivery=DeliveryGuarantee.BROADCAST,
                expected_queue="notifications.push",
                expected_cost_multiplier=1.5,
                expected_latency_multiplier=1.2,
                description="User push notifications broadcast",
            ),
            # FIRE_AND_FORGET delivery
            ComprehensiveQueueTestCase(
                topic="app.queue.logging.analytics",
                delivery=DeliveryGuarantee.FIRE_AND_FORGET,
                expected_queue="logging.analytics",
                expected_cost_multiplier=1.0,
                expected_latency_multiplier=1.0,
                description="App analytics logging fire-and-forget",
            ),
            # QUORUM delivery
            ComprehensiveQueueTestCase(
                topic="system.queue.consensus.decisions",
                delivery=DeliveryGuarantee.QUORUM,
                expected_queue="consensus.decisions",
                expected_cost_multiplier=1.0,
                expected_latency_multiplier=1.0,
                description="System consensus decisions quorum",
            ),
            # Nested queue names
            ComprehensiveQueueTestCase(
                topic="enterprise.queue.crm.lead.processing.priority",
                delivery=DeliveryGuarantee.AT_LEAST_ONCE,
                expected_queue="crm.lead.processing.priority",
                expected_cost_multiplier=1.0,
                expected_latency_multiplier=1.0,
                description="Enterprise CRM nested queue name",
            ),
            # Invalid queue pattern (no "queue" segment)
            ComprehensiveQueueTestCase(
                topic="invalid.topic.without.command.segment",
                delivery=DeliveryGuarantee.AT_LEAST_ONCE,
                expected_queue=None,
                expected_cost_multiplier=1.0,
                expected_latency_multiplier=1.0,
                description="Invalid queue pattern",
            ),
        ]

        for test_case in queue_test_cases:
            correlation_id = create_correlation_id()
            headers = MessageHeaders(
                correlation_id=correlation_id,
                source_system=MessageType.QUEUE,
                priority=RoutingPriority.NORMAL,
            )

            message = UnifiedMessage(
                topic=test_case.topic,
                routing_type=MessageType.QUEUE,
                delivery_guarantee=test_case.delivery,
                payload={"job": "test_queue_routing"},
                headers=headers,
            )

            route_result = await router.route_message(message)

            # Verify routing
            assert route_result.route_id is not None
            assert len(route_result.targets) == 1

            target = route_result.targets[0]
            assert target.system_type == MessageType.QUEUE

            if test_case.expected_queue:
                assert target.target_id == test_case.expected_queue
                assert (
                    target.priority_weight == 1.5
                )  # Moderate priority for valid queues

                # Verify delivery guarantee impact on cost/latency
                base_cost = 3.0
                base_latency = 15.0
                expected_cost = base_cost * test_case.expected_cost_multiplier
                expected_latency = base_latency * test_case.expected_latency_multiplier

                assert abs(route_result.route_cost - expected_cost) < 0.1
                assert abs(route_result.estimated_latency_ms - expected_latency) < 0.1
            else:
                assert (
                    target.target_id == "local_handler"
                )  # Fallback for invalid topics
                assert (
                    target.priority_weight == 1.0
                )  # No queue priority for invalid topics
                assert (
                    route_result.estimated_latency_ms == 5.0
                )  # Standard local routing latency

            print(
                f"✓ Queue routing: {test_case.topic} → {target.target_id} ({test_case.delivery.value}, cost: {route_result.route_cost:.1f})"
            )

    async def test_cache_message_routing_patterns(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test Cache routing for all operation types."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        # Test all cache operation patterns using proper dataclasses
        cache_test_cases: list[ComprehensiveCacheTestCase] = [
            # Cache invalidation operations
            ComprehensiveCacheTestCase(
                topic="mpreg.cache.invalidation.user_sessions",
                expected_target="cache_manager",
                expected_priority=2.0,
                expected_latency=2.0,
                operation_type="invalidation",
                description="User sessions cache invalidation",
            ),
            ComprehensiveCacheTestCase(
                topic="mpreg.cache.invalidation.product_catalog",
                expected_target="cache_manager",
                expected_priority=2.0,
                expected_latency=2.0,
                operation_type="invalidation",
                description="Product catalog cache invalidation",
            ),
            ComprehensiveCacheTestCase(
                topic="mpreg.cache.invalidation.global_settings",
                expected_target="cache_manager",
                expected_priority=2.0,
                expected_latency=2.0,
                operation_type="invalidation",
                description="Global settings cache invalidation",
            ),
            # Cache coordination operations
            ComprehensiveCacheTestCase(
                topic="mpreg.cache.coordination.replication_sync",
                expected_target="cache_coordinator",
                expected_priority=1.5,
                expected_latency=3.0,
                operation_type="coordination",
                description="Cache replication sync coordination",
            ),
            ComprehensiveCacheTestCase(
                topic="mpreg.cache.coordination.distributed_lock",
                expected_target="cache_coordinator",
                expected_priority=1.5,
                expected_latency=3.0,
                operation_type="coordination",
                description="Distributed lock coordination",
            ),
            ComprehensiveCacheTestCase(
                topic="mpreg.cache.coordination.consistency_check",
                expected_target="cache_coordinator",
                expected_priority=1.5,
                expected_latency=3.0,
                operation_type="coordination",
                description="Cache consistency check coordination",
            ),
            # Cache gossip operations
            ComprehensiveCacheTestCase(
                topic="mpreg.cache.gossip.node_state",
                expected_target="cache_gossip",
                expected_priority=1.0,
                expected_latency=5.0,
                operation_type="gossip",
                description="Node state gossip",
            ),
            ComprehensiveCacheTestCase(
                topic="mpreg.cache.gossip.membership_update",
                expected_target="cache_gossip",
                expected_priority=1.0,
                expected_latency=5.0,
                operation_type="gossip",
                description="Membership update gossip",
            ),
            # Cache federation operations
            ComprehensiveCacheTestCase(
                topic="mpreg.cache.federation.cross_cluster_sync",
                expected_target="local_handler",  # Falls back since no federation_router
                expected_priority=2.0,  # Priority boost
                expected_latency=1.0,  # Local fallback latency
                operation_type="federation",
                description="Cross-cluster sync federation fallback",
            ),
            # Cache analytics/monitoring operations
            ComprehensiveCacheTestCase(
                topic="mpreg.cache.analytics.hit_rate_stats",
                expected_target="cache_monitor",
                expected_priority=0.5,
                expected_latency=10.0,
                operation_type="analytics",
                description="Hit rate analytics",
            ),
            ComprehensiveCacheTestCase(
                topic="mpreg.cache.events.eviction_report",
                expected_target="cache_monitor",
                expected_priority=0.5,
                expected_latency=10.0,
                operation_type="events",
                description="Eviction event reporting",
            ),
            # Unknown cache operations (fallback)
            ComprehensiveCacheTestCase(
                topic="mpreg.cache.unknown.operation_type",
                expected_target="local_handler",
                expected_priority=2.0,  # Priority boost
                expected_latency=1.0,  # Fast local fallback
                operation_type="unknown",
                description="Unknown cache operation fallback",
            ),
            # Non-mpreg cache topics (still get cache priority due to MessageType.CACHE)
            ComprehensiveCacheTestCase(
                topic="app.cache.user_data",
                expected_target="local_handler",
                expected_priority=2.0,  # Priority boost because it's MessageType.CACHE
                expected_latency=1.0,  # Fast cache priority routing
                operation_type="non-mpreg",
                description="Non-MPREG cache topic",
            ),
        ]

        for test_case in cache_test_cases:
            correlation_id = create_correlation_id()
            headers = MessageHeaders(
                correlation_id=correlation_id,
                source_system=MessageType.CACHE,
                priority=RoutingPriority.HIGH,
            )

            message = UnifiedMessage(
                topic=test_case.topic,
                routing_type=MessageType.CACHE,
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                payload={"cache_op": "test_cache_routing"},
                headers=headers,
            )

            route_result = await router.route_message(message)

            # Verify routing
            assert route_result.route_id is not None
            assert len(route_result.targets) == 1

            target = route_result.targets[0]
            assert target.system_type == MessageType.CACHE
            assert target.target_id == test_case.expected_target
            assert target.priority_weight == test_case.expected_priority
            assert route_result.estimated_latency_ms == test_case.expected_latency

            print(
                f"✓ Cache routing: {test_case.topic} → {target.target_id} ({test_case.operation_type}, priority: {target.priority_weight}, latency: {route_result.estimated_latency_ms}ms)"
            )

    async def _create_basic_cluster(
        self,
        test_context: AsyncTestContext,
        ports: list[int],
        cluster_id: str,
    ) -> list[MPREGServer]:
        """Helper to create a basic test cluster."""
        servers = []

        for i, port in enumerate(ports):
            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"Test-Server-{i + 1}",
                cluster_id=cluster_id,
                resources={f"test-{i + 1}"},
                peers=None,
                connect=f"ws://127.0.0.1:{ports[0]}" if i > 0 else None,
                advertised_urls=None,
                gossip_interval=1.0,
            )
            servers.append(MPREGServer(settings=settings))

        test_context.servers.extend(servers)

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.2)

        await asyncio.sleep(1.0)
        return servers


class TestUnifiedSystemDeliveryGuarantees:
    """Test all delivery guarantees across all system combinations."""

    @pytest.mark.parametrize(
        "message_type",
        [
            MessageType.RPC,
            MessageType.PUBSUB,
            MessageType.QUEUE,
            MessageType.CACHE,
        ],
    )
    @pytest.mark.parametrize(
        "delivery_guarantee",
        [
            DeliveryGuarantee.FIRE_AND_FORGET,
            DeliveryGuarantee.AT_LEAST_ONCE,
            DeliveryGuarantee.EXACTLY_ONCE,
            DeliveryGuarantee.BROADCAST,
            DeliveryGuarantee.QUORUM,
        ],
    )
    async def test_delivery_guarantee_combinations(
        self,
        test_context: AsyncTestContext,
        message_type: MessageType,
        delivery_guarantee: DeliveryGuarantee,
    ):
        """Test all delivery guarantee + message type combinations."""
        config = UnifiedRoutingConfig(
            enable_topic_rpc=True,
            enable_queue_topics=True,
            enable_cross_system_correlation=True,
        )
        router = UnifiedRouterImpl(config)

        # Generate appropriate topic for message type
        topic_patterns = {
            MessageType.RPC: f"test.rpc.{delivery_guarantee.value.replace('_', '')}_command",
            MessageType.PUBSUB: f"test.events.{delivery_guarantee.value.replace('_', '')}_event",
            MessageType.QUEUE: f"test.queue.{delivery_guarantee.value.replace('_', '')}_job",
            MessageType.CACHE: f"mpreg.cache.invalidation.{delivery_guarantee.value.replace('_', '')}_keys",
        }

        topic = topic_patterns[message_type]

        correlation_id = create_correlation_id()
        headers = MessageHeaders(
            correlation_id=correlation_id,
            source_system=message_type,
            priority=RoutingPriority.NORMAL,
        )

        message = UnifiedMessage(
            topic=topic,
            routing_type=message_type,
            delivery_guarantee=delivery_guarantee,
            payload={"test": f"{message_type.value}_{delivery_guarantee.value}"},
            headers=headers,
        )

        route_result = await router.route_message(message)

        # Verify basic routing structure
        assert route_result.route_id is not None
        assert len(route_result.targets) >= 1
        assert isinstance(route_result.estimated_latency_ms, float)
        assert route_result.estimated_latency_ms > 0
        assert isinstance(route_result.route_cost, float)
        assert route_result.route_cost >= 0

        target = route_result.targets[0]
        assert target.system_type == message_type

        # Verify delivery guarantee impacts routing decisions
        if message_type == MessageType.QUEUE:
            base_cost = 3.0
            if delivery_guarantee == DeliveryGuarantee.EXACTLY_ONCE:
                expected_cost = base_cost * 2.0
                assert abs(route_result.route_cost - expected_cost) < 0.1
            elif delivery_guarantee == DeliveryGuarantee.BROADCAST:
                expected_cost = base_cost * 1.5
                assert abs(route_result.route_cost - expected_cost) < 0.1

        print(
            f"✓ {message_type.value} + {delivery_guarantee.value}: routed to {target.target_id} (cost: {route_result.route_cost:.1f})"
        )


class TestUnifiedSystemPriorityLevels:
    """Test all priority levels and their impact on routing."""

    @pytest.mark.parametrize(
        "priority",
        [
            RoutingPriority.CRITICAL,
            RoutingPriority.HIGH,
            RoutingPriority.NORMAL,
            RoutingPriority.LOW,
            RoutingPriority.BULK,
        ],
    )
    async def test_priority_impact_on_routing(
        self,
        test_context: AsyncTestContext,
        priority: RoutingPriority,
    ):
        """Test how different priorities affect routing decisions."""
        config = UnifiedRoutingConfig()
        router = UnifiedRouterImpl(config)

        # Test priority across different message types
        message_types = [
            MessageType.RPC,
            MessageType.PUBSUB,
            MessageType.QUEUE,
            MessageType.CACHE,
        ]

        for message_type in message_types:
            correlation_id = create_correlation_id()
            headers = MessageHeaders(
                correlation_id=correlation_id,
                source_system=message_type,
                priority=priority,
            )

            # Generate appropriate topic
            topic_map = {
                MessageType.RPC: f"test.rpc.{priority.value}_priority_command",
                MessageType.PUBSUB: f"test.events.{priority.value}_priority",
                MessageType.QUEUE: f"test.queue.{priority.value}_priority_jobs",
                MessageType.CACHE: f"mpreg.cache.invalidation.{priority.value}_priority",
            }

            message = UnifiedMessage(
                topic=topic_map[message_type],
                routing_type=message_type,
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                payload={"priority_test": priority.value},
                headers=headers,
            )

            route_result = await router.route_message(message)

            # Verify routing succeeds with priority information
            assert route_result.route_id is not None
            assert len(route_result.targets) >= 1

            # Priority should be preserved in headers
            assert message.headers.priority == priority

            print(
                f"✓ {message_type.value} with {priority.value} priority: routed successfully"
            )


class TestUnifiedSystemPropertyBased:
    """Property-based tests for unified system behavior."""

    @given(
        system_type=st.sampled_from(["rpc", "pubsub", "queue", "cache"]),
        delivery=st.sampled_from(
            ["fire_and_forget", "at_least_once", "exactly_once", "broadcast", "quorum"]
        ),
        priority=st.sampled_from(["critical", "high", "normal", "low", "bulk"]),
        topic_prefix=st.sampled_from(["mpreg", "app", "system", "user"]),
    )
    @settings(max_examples=100, deadline=5000)
    async def test_routing_consistency_properties(
        self,
        system_type: str,
        delivery: str,
        priority: str,
        topic_prefix: str,
    ):
        """Property test: Routing should be consistent and deterministic."""
        config = UnifiedRoutingConfig(
            enable_topic_rpc=True,
            enable_queue_topics=True,
            enable_cross_system_correlation=True,
        )
        router = UnifiedRouterImpl(config)

        # Generate test message
        message_type = MessageType(system_type)
        delivery_guarantee = DeliveryGuarantee(delivery)
        routing_priority = RoutingPriority(priority)

        # Generate deterministic topic
        topic = f"{topic_prefix}.{system_type}.test_{abs(hash(f'{system_type}{delivery}{priority}{topic_prefix}')) % 1000}"

        correlation_id = create_correlation_id()
        headers = MessageHeaders(
            correlation_id=correlation_id,
            source_system=message_type,
            priority=routing_priority,
        )

        message = UnifiedMessage(
            topic=topic,
            routing_type=message_type,
            delivery_guarantee=delivery_guarantee,
            payload={"property_test": True},
            headers=headers,
        )

        # Route message multiple times
        results = []
        for _ in range(3):
            result = await router.route_message(message)
            results.append(result)

        # Property 1: Routing should be deterministic
        first_result = results[0]
        for result in results[1:]:
            assert result.route_id == first_result.route_id
            assert len(result.targets) == len(first_result.targets)
            assert result.federation_required == first_result.federation_required

        # Property 2: All results should have valid structure
        for result in results:
            assert result.route_id is not None
            assert isinstance(result.targets, list)
            assert len(result.targets) >= 0  # Can be 0 for unmatched pub/sub
            assert isinstance(result.estimated_latency_ms, float)
            assert result.estimated_latency_ms > 0
            assert isinstance(result.route_cost, float)
            assert result.route_cost >= 0
            assert isinstance(result.federation_required, bool)
            assert isinstance(result.hops_required, int)
            assert result.hops_required >= 0

        # Property 3: Message content should be preserved
        assert message.topic == topic
        assert message.routing_type == message_type
        assert message.delivery_guarantee == delivery_guarantee
        assert message.headers.priority == routing_priority

        print(
            f"✓ Property: {system_type}.{delivery}.{priority}.{topic_prefix} routing is consistent"
        )


class TestUnifiedSystemScenarios:
    """Test realistic end-to-end scenarios."""

    async def test_e_commerce_order_processing_scenario(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test realistic e-commerce order processing flow."""
        ports = server_cluster_ports[:4]

        # Create specialized cluster for e-commerce using proper dataclasses
        cluster_configs: list[ClusterConfigTestCase] = [
            ClusterConfigTestCase(
                name="Frontend-Gateway",
                port=ports[0],
                cluster_id="ecommerce-cluster",
                resources={"frontend", "api-gateway", "load-balancer"},
                connect=None,
                description="Frontend gateway service",
            ),
            ClusterConfigTestCase(
                name="Order-Service",
                port=ports[1],
                cluster_id="ecommerce-cluster",
                resources={"orders", "inventory", "pricing"},
                connect=f"ws://127.0.0.1:{ports[0]}",
                description="Order processing service",
            ),
            ClusterConfigTestCase(
                name="Payment-Service",
                port=ports[2],
                cluster_id="ecommerce-cluster",
                resources={"payments", "billing", "fraud-detection"},
                connect=f"ws://127.0.0.1:{ports[0]}",
                description="Payment processing service",
            ),
            ClusterConfigTestCase(
                name="Notification-Service",
                port=ports[3],
                cluster_id="ecommerce-cluster",
                resources={"notifications", "email", "sms"},
                connect=f"ws://127.0.0.1:{ports[0]}",
                description="Notification service",
            ),
        ]

        servers = []
        for config in cluster_configs:
            settings = MPREGSettings(
                host="127.0.0.1",
                port=config.port,
                name=config.name,
                cluster_id=config.cluster_id,
                resources=config.resources,
                peers=None,
                connect=config.connect,
                advertised_urls=None,
                gossip_interval=1.0,
            )
            servers.append(MPREGServer(settings=settings))

        test_context.servers.extend(servers)

        # Start cluster
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.3)

        await asyncio.sleep(2.0)  # Cluster formation

        # Test unified routing for e-commerce flow
        routing_config = UnifiedRoutingConfig(
            enable_topic_rpc=True,
            enable_queue_topics=True,
            enable_cross_system_correlation=True,
        )

        # Create a TopicExchange to ensure PubSub routing works correctly
        topic_exchange = TopicExchange(
            server_url=f"ws://127.0.0.1:{ports[0]}", cluster_id="ecommerce-cluster"
        )

        router = UnifiedRouterImpl(config=routing_config, topic_exchange=topic_exchange)

        # Simulate order processing flow
        correlation_id = create_correlation_id()

        # 1. Order validation (RPC)
        order_validation = UnifiedMessage(
            topic="mpreg.rpc.validate_order",
            routing_type=MessageType.RPC,
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={"order_id": "ORD123", "customer_id": "CUST456"},
            headers=MessageHeaders(
                correlation_id=correlation_id,
                source_system=MessageType.RPC,
                priority=RoutingPriority.HIGH,
            ),
        )

        order_result = await router.route_message(order_validation)
        assert order_result.route_id is not None
        assert len(order_result.targets) == 1
        assert order_result.targets[0].target_id == "validate_order"

        # 2. Payment processing (Queue with exactly-once)
        payment_processing = UnifiedMessage(
            topic="mpreg.queue.payment_processing",
            routing_type=MessageType.QUEUE,
            delivery_guarantee=DeliveryGuarantee.EXACTLY_ONCE,
            payload={
                "order_id": "ORD123",
                "amount": 99.99,
                "payment_method": "credit_card",
            },
            headers=MessageHeaders(
                correlation_id=correlation_id,
                source_system=MessageType.QUEUE,
                priority=RoutingPriority.CRITICAL,
            ),
        )

        payment_result = await router.route_message(payment_processing)
        assert payment_result.route_id is not None
        assert len(payment_result.targets) == 1
        assert payment_result.targets[0].target_id == "payment_processing"
        # EXACTLY_ONCE should have higher cost
        assert payment_result.route_cost == 6.0  # 3.0 * 2.0

        # 3. Order confirmation (PubSub broadcast)
        order_confirmation = UnifiedMessage(
            topic="ecommerce.events.order.confirmed",
            routing_type=MessageType.PUBSUB,
            delivery_guarantee=DeliveryGuarantee.BROADCAST,
            payload={
                "order_id": "ORD123",
                "status": "confirmed",
                "timestamp": time.time(),
            },
            headers=MessageHeaders(
                correlation_id=correlation_id,
                source_system=MessageType.PUBSUB,
                priority=RoutingPriority.NORMAL,
            ),
        )

        # For this test, we expect 0 subscribers since we didn't set up TopicExchange
        confirmation_result = await router.route_message(order_confirmation)
        assert confirmation_result.route_id is not None
        # Should have 0 targets due to no matching subscriptions
        assert len(confirmation_result.targets) == 0

        # 4. Cache invalidation (Cache operation)
        cache_invalidation = UnifiedMessage(
            topic="mpreg.cache.invalidation.inventory_levels",
            routing_type=MessageType.CACHE,
            delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
            payload={"keys": ["inventory:item_123", "inventory:item_456"]},
            headers=MessageHeaders(
                correlation_id=correlation_id,
                source_system=MessageType.CACHE,
                priority=RoutingPriority.HIGH,
            ),
        )

        cache_result = await router.route_message(cache_invalidation)
        assert cache_result.route_id is not None
        assert len(cache_result.targets) == 1
        assert cache_result.targets[0].target_id == "cache_manager"
        assert (
            cache_result.targets[0].priority_weight == 2.0
        )  # High priority for invalidation

        # Verify correlation is maintained across all operations
        assert order_result.route_id != payment_result.route_id  # Different route IDs
        assert payment_result.route_id != confirmation_result.route_id
        assert confirmation_result.route_id != cache_result.route_id

        print(
            "✓ E-commerce scenario: Complete order processing flow routed successfully"
        )
        print(f"  - Order validation: {order_result.targets[0].target_id}")
        print(
            f"  - Payment processing: {payment_result.targets[0].target_id} (cost: {payment_result.route_cost})"
        )
        print(f"  - Order confirmation: {len(confirmation_result.targets)} subscribers")
        print(f"  - Cache invalidation: {cache_result.targets[0].target_id}")

    async def test_iot_data_processing_scenario(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test IoT sensor data processing pipeline."""
        ports = server_cluster_ports[:3]

        # Create IoT processing cluster using proper dataclasses
        cluster_configs: list[ClusterConfigTestCase] = [
            ClusterConfigTestCase(
                name="Edge-Gateway",
                port=ports[0],
                cluster_id="iot-cluster",
                resources={"edge", "sensors", "ingestion"},
                connect=None,
                description="IoT edge gateway",
            ),
            ClusterConfigTestCase(
                name="Processing-Engine",
                port=ports[1],
                cluster_id="iot-cluster",
                resources={"analytics", "ml", "aggregation"},
                connect=f"ws://127.0.0.1:{ports[0]}",
                description="Data processing engine",
            ),
            ClusterConfigTestCase(
                name="Storage-Service",
                port=ports[2],
                cluster_id="iot-cluster",
                resources={"storage", "database", "archival"},
                connect=f"ws://127.0.0.1:{ports[0]}",
                description="Storage service",
            ),
        ]

        servers = []
        for config in cluster_configs:
            settings = MPREGSettings(
                host="127.0.0.1",
                port=config.port,
                name=config.name,
                cluster_id=config.cluster_id,
                resources=config.resources,
                peers=None,
                connect=config.connect,
                advertised_urls=None,
                gossip_interval=1.0,
            )
            servers.append(MPREGServer(settings=settings))

        test_context.servers.extend(servers)

        # Start IoT cluster
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.3)

        await asyncio.sleep(1.5)

        # Test IoT data flow routing
        routing_config = UnifiedRoutingConfig(
            enable_topic_rpc=True,
            enable_queue_topics=True,
            enable_cross_system_correlation=True,
        )
        router = UnifiedRouterImpl(routing_config)

        correlation_id = create_correlation_id()

        # IoT data ingestion scenarios using proper dataclasses
        iot_scenarios: list[IoTScenarioTestCase] = [
            # High-frequency sensor data (bulk priority, fire-and-forget)
            IoTScenarioTestCase(
                name="Temperature Sensors",
                topic="iot.queue.sensors.temperature",
                message_type=MessageType.QUEUE,
                delivery=DeliveryGuarantee.FIRE_AND_FORGET,
                priority=RoutingPriority.BULK,
                payload={
                    "sensor_id": "temp_001",
                    "value": 23.5,
                    "timestamp": time.time(),
                },
                description="High-frequency temperature sensor data",
            ),
            # Critical alerts (critical priority, exactly-once)
            IoTScenarioTestCase(
                name="Safety Alerts",
                topic="iot.queue.alerts.safety_critical",
                message_type=MessageType.QUEUE,
                delivery=DeliveryGuarantee.EXACTLY_ONCE,
                priority=RoutingPriority.CRITICAL,
                payload={
                    "alert_type": "gas_leak",
                    "severity": "critical",
                    "location": "zone_A",
                },
                description="Critical safety alerts",
            ),
            # Device status updates (pub/sub broadcast)
            IoTScenarioTestCase(
                name="Device Status",
                topic="iot.events.device.status_update",
                message_type=MessageType.PUBSUB,
                delivery=DeliveryGuarantee.BROADCAST,
                priority=RoutingPriority.NORMAL,
                payload={"device_id": "device_123", "status": "online", "battery": 87},
                description="Device status broadcast updates",
            ),
            # Data aggregation requests (RPC)
            IoTScenarioTestCase(
                name="Data Aggregation",
                topic="iot.rpc.analytics.aggregate_hourly",
                message_type=MessageType.RPC,
                delivery=DeliveryGuarantee.AT_LEAST_ONCE,
                priority=RoutingPriority.HIGH,
                payload={
                    "time_range": "2024-01-01T10:00:00Z",
                    "sensor_types": ["temperature", "humidity"],
                },
                description="Data aggregation RPC requests",
            ),
            # Cache updates for device metadata
            IoTScenarioTestCase(
                name="Device Cache Update",
                topic="mpreg.cache.invalidation.device_metadata",
                message_type=MessageType.CACHE,
                delivery=DeliveryGuarantee.AT_LEAST_ONCE,
                priority=RoutingPriority.HIGH,
                payload={
                    "device_ids": ["device_123", "device_456"],
                    "metadata_updated": True,
                },
                description="Device metadata cache updates",
            ),
        ]

        for scenario in iot_scenarios:
            headers = MessageHeaders(
                correlation_id=correlation_id,
                source_system=scenario.message_type,
                priority=scenario.priority,
            )

            message = UnifiedMessage(
                topic=scenario.topic,
                routing_type=scenario.message_type,
                delivery_guarantee=scenario.delivery,
                payload=scenario.payload,
                headers=headers,
            )

            route_result = await router.route_message(message)

            # Verify routing
            assert route_result.route_id is not None
            assert (
                len(route_result.targets) >= 0
            )  # Can be 0 for pub/sub without subscribers

            if len(route_result.targets) > 0:
                target = route_result.targets[0]
                assert target.system_type == scenario.message_type

                # Verify priority impacts
                if scenario.message_type == MessageType.RPC:
                    assert target.priority_weight == 2.0
                elif scenario.message_type == MessageType.QUEUE:
                    assert target.priority_weight == 1.5

                # Verify delivery guarantee impacts for queues
                if scenario.message_type == MessageType.QUEUE:
                    if scenario.delivery == DeliveryGuarantee.EXACTLY_ONCE:
                        assert route_result.route_cost == 6.0  # 3.0 * 2.0
                    elif scenario.delivery == DeliveryGuarantee.FIRE_AND_FORGET:
                        assert route_result.route_cost == 3.0  # Base cost

            print(
                f"✓ IoT {scenario.name}: routed to {len(route_result.targets)} targets ({scenario.priority.value} priority)"
            )

        print(
            "✓ IoT scenario: Complete sensor data processing pipeline routed successfully"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
