#!/usr/bin/env python3
"""
DEBUG: Find exactly where pytest hangs by testing each component separately
"""

import asyncio

import pytest
from tests.port_allocator import get_port_allocator

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext


class TestPytestHangDebug:
    """Test each component separately to find where pytest hangs."""

    def test_port_allocator_only(self):
        """Test if port allocator works in pytest."""
        print("🔧 Testing port allocator...")
        allocator = get_port_allocator()
        ports = allocator.allocate_port_range(5, "debug")
        print(f"✅ Allocated ports: {ports}")
        for port in ports:
            allocator.release_port(port)
        print("✅ Port allocator works!")

    @pytest.mark.asyncio
    async def test_async_context_only(self, test_context: AsyncTestContext):
        """Test if AsyncTestContext works in pytest."""
        print("🔧 Testing AsyncTestContext...")
        print(
            f"✅ Context created: {len(test_context.servers)} servers, {len(test_context.tasks)} tasks"
        )
        print("✅ AsyncTestContext works!")

    @pytest.mark.asyncio
    async def test_single_server_creation(self, test_context: AsyncTestContext):
        """Test creating just ONE server in pytest."""
        print("🔧 Testing single server creation...")

        allocator = get_port_allocator()
        port = allocator.allocate_port("debug")
        print(f"  Allocated port: {port}")

        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="Debug-Node-0",
            cluster_id="debug-cluster",
            resources={"debug-node"},
            log_level="ERROR",
        )
        print("  Created settings")

        server = MPREGServer(settings=settings)
        test_context.servers.append(server)
        print("  Created server")

        # Start server
        task = asyncio.create_task(server.server())
        test_context.tasks.append(task)
        print("  Started server task")

        await asyncio.sleep(1.0)
        print("  Waited 1 second")

        allocator.release_port(port)
        print("✅ Single server creation works!")

    @pytest.mark.asyncio
    async def test_two_server_creation(self, test_context: AsyncTestContext):
        """Test creating TWO servers in pytest."""
        print("🔧 Testing two server creation...")

        allocator = get_port_allocator()
        ports = allocator.allocate_port_range(2, "debug")
        print(f"  Allocated ports: {ports}")

        for i, port in enumerate(ports):
            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"Debug-Node-{i}",
                cluster_id="debug-cluster",
                resources={f"debug-node-{i}"},
                log_level="ERROR",
                peers=[f"ws://127.0.0.1:{ports[0]}"] if i > 0 else [],
            )
            print(f"  Created settings for server {i}")

            server = MPREGServer(settings=settings)
            test_context.servers.append(server)
            print(f"  Created server {i}")

            # Start server
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            print(f"  Started server {i} task")

            await asyncio.sleep(0.5)
            print(f"  Waited after server {i}")

        print("  Waiting for discovery...")
        await asyncio.sleep(3.0)

        # Check discovery
        for i, server in enumerate(test_context.servers):
            peer_count = len(server.cluster.servers) - 1
            print(f"  Server {i}: {peer_count} peers")

        for port in ports:
            allocator.release_port(port)
        print("✅ Two server creation works!")


if __name__ == "__main__":
    # Also test directly
    async def test_direct():
        """Test directly without pytest."""
        print("🧪 Testing direct execution...")

        allocator = get_port_allocator()
        port = allocator.allocate_port("debug-direct")

        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="Direct-Node",
            cluster_id="direct-cluster",
            resources={"direct-node"},
            log_level="ERROR",
        )

        server = MPREGServer(settings=settings)
        task = asyncio.create_task(server.server())

        await asyncio.sleep(1.0)

        await server.shutdown_async()
        allocator.release_port(port)
        print("✅ Direct execution works!")

    print("🔍 Running direct test first...")
    asyncio.run(test_direct())
