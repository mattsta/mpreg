#!/usr/bin/env python3
"""Simple working benchmark."""

import asyncio
import statistics
import time

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def main():
    print("⚡ Simple MPREG Benchmark")
    print("=" * 30)

    # Start a simple server
    server = MPREGServer(
        MPREGSettings(
            port=9001,
            name="Benchmark-Server",
            resources={"compute"},
            log_level="WARNING",
        )
    )

    # Register simple functions
    def fast_compute(x: int) -> int:
        return x * 2

    def slow_compute(x: int, delay: float = 0.01) -> dict:
        import time

        time.sleep(delay)
        return {"input": x, "result": x**2}

    server.register_command("fast_compute", fast_compute, ["compute"])
    server.register_command("slow_compute", slow_compute, ["compute"])

    # Start server
    server_task = asyncio.create_task(server.server())
    await asyncio.sleep(0.5)

    try:
        async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
            # Latency test
            print("🚀 Testing latency...")
            latencies = []
            for i in range(20):
                start = time.perf_counter()
                result = await client.call(
                    "fast_compute", i, locs=frozenset(["compute"])
                )
                end = time.perf_counter()
                latencies.append((end - start) * 1000)
                assert result == i * 2

            avg_latency = statistics.mean(latencies)
            print(f"✅ Average latency: {avg_latency:.2f}ms")

            # Throughput test
            print("\n⚡ Testing throughput...")
            start = time.perf_counter()

            tasks = []
            for i in range(20):
                task = client.call("fast_compute", i, locs=frozenset(["compute"]))
                tasks.append(task)

            results = await asyncio.gather(*tasks)
            end = time.perf_counter()

            total_time = end - start
            throughput = len(results) / total_time

            print(f"✅ Throughput: {throughput:.1f} requests/second")
            print(f"✅ Total time: {total_time:.3f} seconds")

            # Concurrent slow operations
            print("\n🔄 Testing concurrent slow operations...")
            start = time.perf_counter()

            slow_tasks = []
            for i in range(10):
                task = client.call("slow_compute", i, 0.01, locs=frozenset(["compute"]))
                slow_tasks.append(task)

            slow_results = await asyncio.gather(*slow_tasks)
            end = time.perf_counter()

            concurrent_time = end - start
            print(
                f"✅ 10 concurrent 10ms operations completed in {concurrent_time:.3f}s"
            )
            print(f"✅ Concurrency speedup: {(10 * 0.01) / concurrent_time:.1f}x")

            print("\n🎉 Benchmark completed successfully!")

    finally:
        server._shutdown_event.set()
        await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main())
