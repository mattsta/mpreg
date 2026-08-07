"""
Test both PRE-CONNECTION and LIVE function registration scenarios.

This test validates that MPREG supports:
1. Functions registered BEFORE cluster connection (shared on connect)
2. Functions registered AFTER cluster connection (live broadcast)
"""

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


class TestDualRegistrationScenarios:
    """Test both pre-connection and live function registration."""

    @pytest.mark.asyncio
    async def test_pre_connection_registration(
        self,
        test_context: Any,
        port_pair: list[int],
        client_factory: Callable[[int], Any],
    ):
        """
        SCENARIO 1: Functions registered BEFORE cluster connection.

        These functions should be announced via the fabric catalog
        when servers connect to each other.
        """
        port1, port2 = port_pair

        # Create servers but DON'T start them yet
        settings1 = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Pre-Connection Server 1",
            cluster_id="test-cluster",
            resources={"pre-resource-1"},
            peers=None,
            connect=None,
        )

        settings2 = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Pre-Connection Server 2",
            cluster_id="test-cluster",
            resources={"pre-resource-2"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",  # Server2 connects to Server1
        )

        server1 = MPREGServer(settings=settings1)
        server2 = MPREGServer(settings=settings2)
        test_context.servers.extend([server1, server2])

        # ✨ CRITICAL: Register functions BEFORE starting/connecting servers
        def pre_function_1(data: str) -> str:
            return f"pre_server1: {data}"

        def pre_function_2(data: str) -> str:
            return f"pre_server2: {data}"

        print("🔧 Registering functions BEFORE cluster connection...")
        server1.register_command("pre_function_1", pre_function_1, ["pre-resource-1"])
        server2.register_command("pre_function_2", pre_function_2, ["pre-resource-2"])

        # Now start the servers and let them connect
        print("🚀 Starting servers and establishing cluster...")
        task1 = asyncio.create_task(server1.server())
        test_context.tasks.append(task1)
        await asyncio.sleep(0.2)  # Let server1 start

        task2 = asyncio.create_task(server2.server())
        test_context.tasks.append(task2)
        await asyncio.sleep(1.0)  # Let them connect and exchange functions

        print("🧪 Testing cross-server function calls...")

        # Test: Server2 should know about pre_function_1 from Server1
        client2 = await client_factory(port2)
        result1 = await client2.call(
            "pre_function_1", "test_data", locs=frozenset(["pre-resource-1"])
        )
        assert result1 == "pre_server1: test_data"

        # Test: Server1 should know about pre_function_2 from Server2
        client1 = await client_factory(port1)
        result2 = await client1.call(
            "pre_function_2", "test_data", locs=frozenset(["pre-resource-2"])
        )
        assert result2 == "pre_server2: test_data"

        print("✅ PRE-CONNECTION registration scenario PASSED!")

    @pytest.mark.asyncio
    async def test_live_registration(
        self,
        cluster_2_servers: tuple[MPREGServer, MPREGServer],
        client_factory: Callable[[int], Any],
    ):
        """
        SCENARIO 2: Functions registered AFTER cluster connection.

        These functions should be immediately broadcast to all connected peers
        using the live federation system.
        """
        server1, server2 = cluster_2_servers

        print("🔥 Testing LIVE function registration on connected cluster...")

        # Cluster is already connected, now register functions dynamically
        def live_function_1(data: str) -> str:
            return f"live_server1: {data}"

        def live_function_2(data: str) -> str:
            return f"live_server2: {data}"

        print("📈 Registering live function on server1...")
        server1.register_command(
            "live_function_1", live_function_1, ["live-resource-1"]
        )

        print("📈 Registering live function on server2...")
        server2.register_command(
            "live_function_2", live_function_2, ["live-resource-2"]
        )

        # Wait for live broadcast and confirmation
        await asyncio.sleep(1.0)

        print("🧪 Testing live function availability...")

        # Test: Server2 should immediately know about live_function_1
        client2 = await client_factory(server2.settings.port)
        result1 = await client2.call(
            "live_function_1", "live_data", locs=frozenset(["live-resource-1"])
        )
        assert result1 == "live_server1: live_data"

        # Test: Server1 should immediately know about live_function_2
        client1 = await client_factory(server1.settings.port)
        result2 = await client1.call(
            "live_function_2", "live_data", locs=frozenset(["live-resource-2"])
        )
        assert result2 == "live_server2: live_data"

        print("✅ LIVE registration scenario PASSED!")

    @pytest.mark.asyncio
    async def test_combined_scenarios(
        self,
        test_context: Any,
        port_pair: list[int],
        client_factory: Callable[[int], Any],
    ):
        """
        COMBINED: Test both pre-connection AND live registration together.

        This validates the complete function lifecycle:
        1. Some functions exist before connection
        2. Additional functions added after connection
        3. All functions work cross-server
        """
        port1, port2 = port_pair

        # Create servers
        settings1 = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Combined Server 1",
            cluster_id="test-cluster",
            resources={"combined-resource-1"},
            peers=None,
            connect=None,
        )

        settings2 = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Combined Server 2",
            cluster_id="test-cluster",
            resources={"combined-resource-2"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
        )

        server1 = MPREGServer(settings=settings1)
        server2 = MPREGServer(settings=settings2)
        test_context.servers.extend([server1, server2])

        # PHASE 1: Pre-connection functions
        def initial_func_1(data: str) -> str:
            return f"initial_1: {data}"

        def initial_func_2(data: str) -> str:
            return f"initial_2: {data}"

        print("🔧 PHASE 1: Registering initial functions...")
        server1.register_command(
            "initial_func_1", initial_func_1, ["combined-resource-1"]
        )
        server2.register_command(
            "initial_func_2", initial_func_2, ["combined-resource-2"]
        )

        # Start cluster
        print("🚀 Starting cluster...")
        task1 = asyncio.create_task(server1.server())
        task2 = asyncio.create_task(server2.server())
        test_context.tasks.extend([task1, task2])
        await asyncio.sleep(1.0)

        # PHASE 2: Live functions
        def live_func_1(data: str) -> str:
            return f"live_1: {data}"

        def live_func_2(data: str) -> str:
            return f"live_2: {data}"

        print("🔥 PHASE 2: Adding live functions...")
        server1.register_command("live_func_1", live_func_1, ["combined-resource-1"])
        server2.register_command("live_func_2", live_func_2, ["combined-resource-2"])
        await asyncio.sleep(1.0)

        # PHASE 3: Test all functions work
        print("🧪 PHASE 3: Testing all functions...")
        client1 = await client_factory(port1)
        client2 = await client_factory(port2)

        # Test initial functions (pre-connection)
        result1 = await client2.call(
            "initial_func_1", "test", locs=frozenset(["combined-resource-1"])
        )
        result2 = await client1.call(
            "initial_func_2", "test", locs=frozenset(["combined-resource-2"])
        )

        # Test live functions (post-connection)
        result3 = await client2.call(
            "live_func_1", "test", locs=frozenset(["combined-resource-1"])
        )
        result4 = await client1.call(
            "live_func_2", "test", locs=frozenset(["combined-resource-2"])
        )

        assert result1 == "initial_1: test"
        assert result2 == "initial_2: test"
        assert result3 == "live_1: test"
        assert result4 == "live_2: test"

        print(
            "🎉 COMBINED scenarios PASSED - Both pre-connection and live registration work!"
        )
