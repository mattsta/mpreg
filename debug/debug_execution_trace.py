#!/usr/bin/env python3

import asyncio
import sys

sys.path.append(".")

from mpreg.client_api import MPREGClientAPI
from mpreg.model import RPCCommand
from mpreg.server import MPREGServer, MPREGSettings


async def debug_execution_trace():
    print("=== DEBUG: Execution Trace for Parallel Convergence ===")

    # Single server test first to isolate the issue
    server = MPREGServer(
        MPREGSettings(
            port=9001,
            name="TestServer",
            resources={"gpu", "ml-models", "cpu-heavy", "analytics", "database"},
            peers=None,
            log_level="DEBUG",
        )
    )

    # Register all functions on one server
    def run_inference(model: str, input_data: str) -> dict:
        print(f"🔧 run_inference called: model={model}, input_data={input_data}")
        result = {
            "model": model,
            "input": input_data,
            "prediction": f"inference_result_for_{input_data}",
            "confidence": 0.87,
            "inference_node": "TestServer",
        }
        print(f"🔧 run_inference returning: {result}")
        return result

    def heavy_compute(data: str, iterations: int) -> dict:
        print(f"🔧 heavy_compute called: data={data}, iterations={iterations}")
        result = {
            "result": f"computed_{data}",
            "iterations": iterations,
            "cpu_cores_used": 16,
            "computation_time": f"{iterations * 0.1}s",
        }
        print(f"🔧 heavy_compute returning: {result}")
        return result

    def analytics_process(dataset: str, metrics: list) -> dict:
        print(f"🔧 analytics called: dataset={dataset}, metrics={metrics}")
        print(f"🔧 analytics metrics type: {type(metrics)}")
        print(f"🔧 analytics metrics content: {metrics}")
        result = {
            "dataset": dataset,
            "metrics": metrics,
            "results": {metric: f"{metric}_result" for metric in metrics},
            "processing_node": "TestServer",
        }
        print(f"🔧 analytics returning: {result}")
        return result

    def query_data(sql: str) -> dict:
        print(f"🔧 query called: sql={sql}")
        result = {
            "query": sql,
            "results": [{"id": 1, "data": "sample"}, {"id": 2, "data": "data"}],
            "rows_returned": 2,
            "query_node": "TestServer",
        }
        print(f"🔧 query returning: {result}")
        return result

    server.register_command("run_inference", run_inference, ["gpu", "ml-models"])
    server.register_command("heavy_compute", heavy_compute, ["cpu-heavy"])
    server.register_command("analytics", analytics_process, ["analytics"])
    server.register_command("query", query_data, ["database"])

    # Start server
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(1)

    try:
        async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
            print("📡 Connected to server, sending request...")

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

            print("✅ SUCCESS! Single-server parallel workflow completed:")
            print(f"Keys: {list(parallel_workflow.keys())}")
            if "converged_analysis" in parallel_workflow:
                print(f"Analytics result: {parallel_workflow['converged_analysis']}")
            else:
                print("❌ ERROR: converged_analysis not found in results")

    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # Cleanup
        print("🛑 Stopping server...")
        await server.stop()
        server_task.cancel()


if __name__ == "__main__":
    asyncio.run(debug_execution_trace())
