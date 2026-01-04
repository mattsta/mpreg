"""Advanced cluster scenarios showcasing MPREG's unique distributed capabilities.

These tests demonstrate complex topological environments, advanced request routing,
and sophisticated distributed computing patterns that highlight MPREG's power.
"""

import asyncio

import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
from mpreg.server import MPREGServer
from tests.test_helpers import TestPortManager


@pytest.fixture
async def five_node_cluster():
    """Create a 5-node cluster with different specializations."""
    servers = []
    port_manager = TestPortManager()

    try:
        # Get ports for all nodes
        coordinator_port = port_manager.get_server_port()
        gpu_port = port_manager.get_server_port()
        cpu_port = port_manager.get_server_port()
        db_port = port_manager.get_server_port()
        edge_port = port_manager.get_server_port()

        # Node 1: Primary coordinator with no specific resources (pure load balancer)
        server1 = MPREGServer(
            MPREGSettings(
                port=coordinator_port,
                name="Coordinator",
                resources=set(),  # Pure balancer node
                log_level="INFO",
            )
        )

        # Node 2: GPU processing node
        server2 = MPREGServer(
            MPREGSettings(
                port=gpu_port,
                name="GPU-Worker",
                resources={"gpu", "ml-models", "dataset-large"},
                peers=[f"ws://127.0.0.1:{coordinator_port}"],
                log_level="INFO",
            )
        )

        # Node 3: CPU-intensive processing
        server3 = MPREGServer(
            MPREGSettings(
                port=cpu_port,
                name="CPU-Worker",
                resources={"cpu-heavy", "analytics", "dataset-medium"},
                peers=[f"ws://127.0.0.1:{coordinator_port}"],
                log_level="INFO",
            )
        )

        # Node 4: Database operations
        server4 = MPREGServer(
            MPREGSettings(
                port=db_port,
                name="Database-Worker",
                resources={"database", "cache", "storage"},
                peers=[f"ws://127.0.0.1:{coordinator_port}"],
                log_level="INFO",
            )
        )

        # Node 5: Edge processing
        server5 = MPREGServer(
            MPREGSettings(
                port=edge_port,
                name="Edge-Worker",
                resources={"edge", "sensors", "realtime"},
                peers=[f"ws://127.0.0.1:{coordinator_port}"],
                log_level="INFO",
            )
        )

        servers = [server1, server2, server3, server4, server5]

        # Register specialized functions on each node
        await _register_specialized_functions(servers)

        # Start all servers concurrently
        tasks = []
        for server in servers:
            task = asyncio.create_task(server.server())
            tasks.append(task)
            await asyncio.sleep(0.1)  # Stagger startup slightly

        # Give time for cluster formation
        await asyncio.sleep(2.0)

        yield servers

    finally:
        # Proper cleanup
        for server in servers:
            try:
                await server.shutdown_async()
            except Exception as e:
                print(f"Error shutting down server: {e}")

        # Cancel all tasks
        for task in tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete with timeout
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=2.0
            )
        except TimeoutError:
            print("Some server tasks did not complete within timeout")

        # Give servers time to shut down
        await asyncio.sleep(0.5)

        # Cleanup port allocations
        port_manager.cleanup()


async def _register_specialized_functions(servers):
    """Register specialized functions on each server node."""

    # GPU Node functions
    def gpu_train_model(model_name: str, epochs: int) -> dict:
        return {
            "model": model_name,
            "epochs": epochs,
            "accuracy": 0.95,
            "gpu_memory_used": "8GB",
            "training_time": f"{epochs * 2}min",
        }

    def gpu_inference(model: str, data: str) -> dict:
        return {
            "model": model,
            "prediction": f"prediction_for_{data}",
            "confidence": 0.92,
            "inference_time": "15ms",
        }

    servers[1].register_command("train_model", gpu_train_model, ["gpu", "ml-models"])
    servers[1].register_command("run_inference", gpu_inference, ["gpu", "ml-models"])

    # CPU Node functions
    def heavy_compute(data: str, iterations: int) -> dict:
        return {
            "result": f"computed_{data}",
            "iterations": iterations,
            "cpu_cores_used": 16,
            "computation_time": f"{iterations * 0.1}s",
        }

    def analytics_process(dataset: str, metrics: list) -> dict:
        return {
            "dataset": dataset,
            "metrics": metrics,
            "results": {
                f"metric_{i}": f"result_for_metric_{i}"
                for i, metric in enumerate(metrics)
            },
            "processing_node": "CPU-Worker",
        }

    servers[2].register_command("heavy_compute", heavy_compute, ["cpu-heavy"])
    servers[2].register_command("analytics", analytics_process, ["analytics"])

    # Database Node functions
    def store_data(key: str, value: str) -> dict:
        return {
            "key": key,
            "value": value,
            "stored": True,
            "timestamp": "2025-01-17T12:00:00Z",
            "storage_node": "Database-Worker",
        }

    def query_data(query: str) -> dict:
        return {
            "query": query,
            "results": [f"row_{i}" for i in range(3)],
            "count": 3,
            "query_time": "2ms",
        }

    servers[3].register_command("store", store_data, ["database"])
    servers[3].register_command("query", query_data, ["database"])

    # Edge Node functions
    def process_sensor_data(sensor_id: str, data: list) -> dict:
        return {
            "sensor": sensor_id,
            "processed_readings": len(data),
            "average": sum(data) / len(data) if data else 0,
            "edge_node": "Edge-Worker",
            "latency": "1ms",
        }

    def realtime_alert(threshold: float, current_value: float) -> dict:
        alert = current_value > threshold
        return {
            "alert": alert,
            "threshold": threshold,
            "current": current_value,
            "severity": "HIGH" if alert else "NORMAL",
            "response_time": "0.5ms",
        }

    servers[4].register_command("process_sensors", process_sensor_data, ["sensors"])
    servers[4].register_command("check_alert", realtime_alert, ["realtime"])


class TestAdvancedClusterScenarios:
    """Test complex multi-node cluster scenarios."""

    async def test_heterogeneous_cluster_formation(self, five_node_cluster):
        """Test that a heterogeneous cluster forms correctly."""
        servers = five_node_cluster

        # Connect to coordinator and verify cluster membership
        async with MPREGClientAPI(
            f"ws://127.0.0.1:{five_node_cluster[0].settings.port}"
        ) as client:
            # Should be able to route to any specialized function
            gpu_result = await client.call(
                "train_model", "ResNet50", 10, locs=frozenset(["gpu", "ml-models"])
            )
            assert gpu_result["model"] == "ResNet50"
            assert gpu_result["epochs"] == 10

            cpu_result = await client.call(
                "heavy_compute", "dataset1", 100, locs=frozenset(["cpu-heavy"])
            )
            assert cpu_result["result"] == "computed_dataset1"

            db_result = await client.call(
                "store", "key1", "value1", locs=frozenset(["database"])
            )
            assert db_result["stored"] is True

    async def test_complex_cross_cluster_workflow(self, five_node_cluster):
        """Test a complex workflow that spans multiple specialized nodes."""
        servers = five_node_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{five_node_cluster[0].settings.port}"
        ) as client:
            # Complex ML pipeline across cluster
            workflow = await client._client.request(
                [
                    # Step 1: Process sensor data on edge
                    RPCCommand(
                        name="sensor_data",
                        fun="process_sensors",
                        args=("sensor_001", [1.2, 1.5, 1.8, 2.1]),
                        locs=frozenset(["sensors"]),
                    ),
                    # Step 2: Train model on GPU using processed data
                    RPCCommand(
                        name="trained_model",
                        fun="train_model",
                        args=("EdgeML", 5),
                        locs=frozenset(["gpu", "ml-models"]),
                    ),
                    # Step 3: Store training results in database
                    RPCCommand(
                        name="stored_model",
                        fun="store",
                        args=("model_results", "trained_model"),
                        locs=frozenset(["database"]),
                    ),
                    # Step 4: Run analytics on CPU using stored model data
                    RPCCommand(
                        name="final_analytics",
                        fun="analytics",
                        args=("stored_model", ["accuracy", "loss", "convergence"]),
                        locs=frozenset(["analytics"]),
                    ),
                ]
            )

            # Verify cross-cluster coordination worked
            assert "final_analytics" in workflow
            assert workflow["final_analytics"]["processing_node"] == "CPU-Worker"
            assert len(workflow["final_analytics"]["metrics"]) == 3
            assert "metric_0" in workflow["final_analytics"]["results"]
            assert "metric_1" in workflow["final_analytics"]["results"]
            assert "metric_2" in workflow["final_analytics"]["results"]

    async def test_intelligent_load_balancing(self, five_node_cluster):
        """Test that requests are intelligently routed based on resources."""
        servers = five_node_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{five_node_cluster[0].settings.port}"
        ) as client:
            # Multiple concurrent requests that should route to different nodes
            tasks = []

            # GPU-bound tasks
            for i in range(3):
                task = client.call(
                    "run_inference",
                    f"model_{i}",
                    f"data_{i}",
                    locs=frozenset(["gpu", "ml-models"]),
                )
                tasks.append(task)

            # CPU-bound tasks
            for i in range(3):
                task = client.call(
                    "heavy_compute", f"dataset_{i}", 50, locs=frozenset(["cpu-heavy"])
                )
                tasks.append(task)

            # Database tasks
            for i in range(3):
                task = client.call(
                    "store", f"key_{i}", f"value_{i}", locs=frozenset(["database"])
                )
                tasks.append(task)

            # Execute all concurrently
            results = await asyncio.gather(*tasks)

            # Verify all completed successfully
            assert len(results) == 9

            # Verify GPU results
            gpu_results = results[:3]
            for result in gpu_results:
                assert "prediction" in result
                assert "confidence" in result

            # Verify CPU results
            cpu_results = results[3:6]
            for result in cpu_results:
                assert "computation_time" in result
                assert result["cpu_cores_used"] == 16

            # Verify DB results
            db_results = results[6:9]
            for result in db_results:
                assert result["stored"] is True

    async def test_fault_tolerant_routing(self, five_node_cluster):
        """Test that the cluster handles node-specific failures gracefully."""
        servers = five_node_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{five_node_cluster[0].settings.port}"
        ) as client:
            # First verify normal operation
            result = await client.call(
                "train_model", "TestModel", 1, locs=frozenset(["gpu", "ml-models"])
            )
            assert result["model"] == "TestModel"

            # Test that requests to non-existent resources fail gracefully
            with pytest.raises(Exception):  # Should timeout or error gracefully
                await client.call(
                    "nonexistent_function",
                    "arg",
                    locs=frozenset(["nonexistent_resource"]),
                    timeout=1.0,
                )

    async def test_dynamic_resource_discovery(self, five_node_cluster):
        """Test that new functions are discoverable across the cluster."""
        servers = five_node_cluster
        from mpreg.datastructures.function_identity import FunctionSelector
        from mpreg.fabric.index import FunctionQuery
        from tests.test_helpers import wait_for_condition

        # Add a new function to the GPU node at runtime
        def new_gpu_function(input_data: str) -> dict:
            return {
                "processed": f"gpu_processed_{input_data}",
                "node": "GPU-Worker",
                "function": "new_gpu_function",
            }

        servers[1].register_command("new_function", new_gpu_function, ["gpu"])

        await wait_for_condition(
            lambda: bool(
                servers[0]._fabric_control_plane
                and servers[0]._fabric_control_plane.index.find_functions(
                    FunctionQuery(
                        selector=FunctionSelector(name="new_function"),
                        resources=frozenset({"gpu"}),
                    )
                )
            ),
            timeout=5.0,
            error_message="new_function not propagated to coordinator",
        )

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{five_node_cluster[0].settings.port}"
        ) as client:
            # Should be able to call the new function through the coordinator
            result = await client.call(
                "new_function", "test_data", locs=frozenset(["gpu"])
            )

            assert result["processed"] == "gpu_processed_test_data"
            assert result["function"] == "new_gpu_function"


class TestTopologicalRequestRouting:
    """Test sophisticated request routing in complex topologies."""

    async def test_multi_stage_dependency_resolution(self, five_node_cluster):
        """Test complex dependency resolution across multiple nodes."""
        servers = five_node_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{five_node_cluster[0].settings.port}"
        ) as client:
            # Create a complex dependency graph that spans the entire cluster
            complex_workflow = await client._client.request(
                [
                    # Stage 1: Edge processing
                    RPCCommand(
                        name="edge_data",
                        fun="process_sensors",
                        args=("temp_sensor", [20.1, 20.5, 21.0]),
                        locs=frozenset(["sensors"]),
                    ),
                    # Stage 2: Real-time alerting based on edge data
                    RPCCommand(
                        name="alert_check",
                        fun="check_alert",
                        args=(
                            20.8,
                            "edge_data.average",
                        ),  # Will resolve edge_data result's average field
                        locs=frozenset(["realtime"]),
                    ),
                    # Stage 3: Store alert results
                    RPCCommand(
                        name="alert_stored",
                        fun="store",
                        args=("alert_result", "alert_check"),
                        locs=frozenset(["database"]),
                    ),
                    # Stage 4: Analyze stored data
                    RPCCommand(
                        name="analysis",
                        fun="analytics",
                        args=(
                            "alert_stored",
                            ["severity", "frequency"],
                        ),  # Now depends on alert_stored
                        locs=frozenset(["analytics"]),
                    ),
                    # Stage 5: Final computation on analysis
                    RPCCommand(
                        name="final_result",
                        fun="heavy_compute",
                        args=("analysis", 10),
                        locs=frozenset(["cpu-heavy"]),
                    ),
                ]
            )

            # Verify the entire pipeline executed correctly
            assert "final_result" in complex_workflow
            # The heavy_compute function receives the full analysis object as a string
            assert complex_workflow["final_result"]["result"].startswith(
                "computed_{'dataset':"
            )
            assert "metric_0" in complex_workflow["final_result"]["result"]
            assert "metric_1" in complex_workflow["final_result"]["result"]

    async def test_parallel_branch_convergence(self, five_node_cluster):
        """Test parallel processing branches that converge."""
        servers = five_node_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{five_node_cluster[0].settings.port}"
        ) as client:
            # Create parallel branches that converge
            parallel_workflow = await client._client.request(
                [
                    # Parallel Branch A: GPU processing
                    RPCCommand(
                        name="gpu_branch",
                        fun="run_inference",
                        args=("ModelA", "input_data"),
                        locs=frozenset(["gpu", "ml-models"]),
                    ),
                    # Parallel Branch B: CPU processing
                    RPCCommand(
                        name="cpu_branch",
                        fun="heavy_compute",
                        args=("input_data", 20),
                        locs=frozenset(["cpu-heavy"]),
                    ),
                    # Parallel Branch C: Database lookup
                    RPCCommand(
                        name="db_branch",
                        fun="query",
                        args=("SELECT * FROM data",),
                        locs=frozenset(["database"]),
                    ),
                    # Convergence: Analytics using all three branches
                    RPCCommand(
                        name="converged_analysis",
                        fun="analytics",
                        args=(
                            "convergence_test",
                            ["gpu_branch", "cpu_branch", "db_branch"],
                        ),
                        locs=frozenset(["analytics"]),
                    ),
                ]
            )

            # Verify convergence worked
            assert "converged_analysis" in parallel_workflow
            assert len(parallel_workflow["converged_analysis"]["metrics"]) == 3
