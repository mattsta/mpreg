#!/usr/bin/env python3
"""
DEBUG: Compare pytest vs direct execution to find the difference
"""

import asyncio

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def test_simple_cluster_direct():
    """Test creating just 5 servers directly (like my debug script)."""
    print("🔧 Creating 5 servers directly...")

    servers = []
    base_port = 31000

    for i in range(5):
        port = base_port + i

        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name=f"Node-{i}",
            cluster_id="debug-cluster",
            resources={f"node-{i}"},
            peers=[f"ws://127.0.0.1:{base_port}"] if i > 0 else [],
            log_level="ERROR",
            gossip_interval=2.0,
        )

        server = MPREGServer(settings=settings)
        servers.append(server)

        # Start server
        asyncio.create_task(server.server())
        await asyncio.sleep(0.1)
        print(f"  Started server {i}")

    print("✅ All 5 servers started, waiting 10s...")
    await asyncio.sleep(10)

    # Check peer counts
    for i, server in enumerate(servers):
        peer_count = len(server.cluster.servers) - 1
        print(f"Node {i}: {peer_count}/4 peers")

    # Clean shutdown
    print("🛑 Shutting down...")
    for server in servers:
        try:
            await server.shutdown_async()
        except Exception as e:
            print(f"Shutdown error: {e}")

    print("✅ DIRECT TEST COMPLETED")


async def test_simple_cluster_pytest_style():
    """Test creating 5 servers using pytest-style patterns."""
    print("🔧 Creating 5 servers pytest-style...")

    # Mimic what pytest tests do
    from tests.conftest import AsyncTestContext

    test_context = AsyncTestContext()
    servers = []
    base_port = 32000

    try:
        for i in range(5):
            port = base_port + i

            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"Node-{i}",
                cluster_id="debug-cluster",
                resources={f"node-{i}"},
                peers=[f"ws://127.0.0.1:{base_port}"] if i > 0 else [],
                log_level="ERROR",
                gossip_interval=2.0,
            )

            server = MPREGServer(settings=settings)
            servers.append(server)
            test_context.servers.append(server)

            # Start server
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.1)
            print(f"  Started server {i}")

        print("✅ All 5 servers started, waiting 10s...")
        await asyncio.sleep(10)

        # Check peer counts
        for i, server in enumerate(servers):
            peer_count = len(server.cluster.servers) - 1
            print(f"Node {i}: {peer_count}/4 peers")

        print("✅ PYTEST-STYLE TEST COMPLETED")

    finally:
        await test_context.cleanup()


async def main():
    """Run both tests to compare."""
    print("=" * 50)
    print("TESTING: Direct vs Pytest-style execution")
    print("=" * 50)

    print("\n1. DIRECT TEST (like my working debug script):")
    await test_simple_cluster_direct()

    print("\n" + "=" * 50)
    print("\n2. PYTEST-STYLE TEST:")
    await test_simple_cluster_pytest_style()


if __name__ == "__main__":
    asyncio.run(main())
