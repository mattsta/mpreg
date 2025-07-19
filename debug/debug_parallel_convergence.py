#!/usr/bin/env python3

import asyncio
import sys

sys.path.append(".")

from mpreg.client_api import MPREGClientAPI
from mpreg.model import RPCCommand
from mpreg.server import MPREGServer, MPREGSettings


async def debug_parallel_convergence():
    print("=== DEBUG: Parallel Branch Convergence Test ===")

    # Create 5 servers like the test
    servers = []

    # Node 1: Coordinator
    server1 = MPREGServer(
        MPREGSettings(
            port=9001,
            name="Coordinator",
            resources=set(),
            peers=None,
            log_level="DEBUG",
        )
    )
    servers.append(server1)

    # Node 2: GPU Worker
    server2 = MPREGServer(
        MPREGSettings(
            port=9002,
            name="GPU-Worker",
            resources={"gpu", "ml-models", "dataset-large"},
            peers=["ws://127.0.0.1:9001"],
            log_level="DEBUG",
        )
    )
    servers.append(server2)

    # Node 3: CPU Worker
    server3 = MPREGServer(
        MPREGSettings(
            port=9003,
            name="CPU-Worker",
            resources={"cpu-heavy", "analytics", "dataset-medium"},
            peers=["ws://127.0.0.1:9001"],
            log_level="DEBUG",
        )
    )
    servers.append(server3)

    # Node 4: Database Worker
    server4 = MPREGServer(
        MPREGSettings(
            port=9004,
            name="Database-Worker",
            resources={"database"},
            peers=["ws://127.0.0.1:9001"],
            log_level="DEBUG",
        )
    )
    servers.append(server4)

    # Node 5: IoT/Sensors Worker
    server5 = MPREGServer(
        MPREGSettings(
            port=9005,
            name="IoT-Worker",
            resources={"sensors", "realtime"},
            peers=["ws://127.0.0.1:9001"],
            log_level="DEBUG",
        )
    )
    servers.append(server5)

    # GPU Node functions
    def train_model(data: str, epochs: int) -> dict:
        return {
            "model": f"trained_on_{data}",
            "epochs": epochs,
            "accuracy": 0.95,
            "loss": 0.05,
            "training_node": "GPU-Worker",
        }

    def run_inference(model: str, input_data: str) -> dict:
        return {
            "model": model,
            "input": input_data,
            "prediction": f"inference_result_for_{input_data}",
            "confidence": 0.87,
            "inference_node": "GPU-Worker",
        }

    servers[1].register_command("train_model", train_model, ["gpu", "ml-models"])
    servers[1].register_command("run_inference", run_inference, ["gpu", "ml-models"])

    # CPU Node functions
    def heavy_compute(data: str, iterations: int) -> dict:
        return {
            "result": f"computed_{data}",
            "iterations": iterations,
            "cpu_cores_used": 16,
            "computation_time": f"{iterations * 0.1}s",
        }

    def analytics_process(dataset: str, metrics: list) -> dict:
        print(f"Analytics called with dataset='{dataset}', metrics={metrics}")
        return {
            "dataset": dataset,
            "metrics": metrics,
            "results": {metric: f"{metric}_result" for metric in metrics},
            "processing_node": "CPU-Worker",
        }

    servers[2].register_command("heavy_compute", heavy_compute, ["cpu-heavy"])
    servers[2].register_command("analytics", analytics_process, ["analytics"])

    # Database Node functions
    def query_data(sql: str) -> dict:
        return {
            "query": sql,
            "results": [{"id": 1, "data": "sample"}, {"id": 2, "data": "data"}],
            "rows_returned": 2,
            "query_node": "Database-Worker",
        }

    servers[3].register_command("query", query_data, ["database"])

    # Start all servers
    tasks = []
    for server in servers:
        task = asyncio.create_task(server.start())
        tasks.append(task)

    # Wait for servers to start and connect
    await asyncio.sleep(2)

    try:
        async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
            print("Testing parallel branch convergence...")

            # The exact same commands as the failing test
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

            print("SUCCESS! Parallel workflow completed:")
            print(f"Keys: {list(parallel_workflow.keys())}")
            if "converged_analysis" in parallel_workflow:
                print(f"Analytics result: {parallel_workflow['converged_analysis']}")
            else:
                print("ERROR: converged_analysis not found in results")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # Cleanup
        print("Stopping servers...")
        for server in servers:
            await server.stop()
        for task in tasks:
            task.cancel()


if __name__ == "__main__":
    asyncio.run(debug_parallel_convergence())
