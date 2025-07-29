"""Test dependency resolution and field access patterns."""

import asyncio

import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
from mpreg.server import MPREGServer

from .test_helpers import TestPortManager


@pytest.fixture
async def two_server_cluster():
    """Create a simple 2-server cluster for dependency testing."""
    servers = []
    port_manager = TestPortManager()

    try:
        port1 = port_manager.get_server_port()
        port2 = port_manager.get_server_port()

        server1 = MPREGServer(
            MPREGSettings(
                port=port1, name="Server1", resources={"step1"}, log_level="INFO"
            )
        )

        server2 = MPREGServer(
            MPREGSettings(
                port=port2,
                name="Server2",
                resources={"step2"},
                peers=[f"ws://127.0.0.1:{port1}"],
                log_level="INFO",
            )
        )

        # Register test functions
        def first_step(value: str) -> dict:
            return {"result": f"processed_{value}", "number": 42}

        def second_step(data: dict, multiplier: int) -> dict:
            return {
                "final": f"final_{data['result']}",
                "computed": data["number"] * multiplier,
            }

        server1.register_command("first_step", first_step, ["step1"])
        server2.register_command("second_step", second_step, ["step2"])

        servers = [server1, server2]

        # Start servers
        tasks = []
        for server in servers:
            task = asyncio.create_task(server.server())
            tasks.append(task)
            await asyncio.sleep(0.1)

        await asyncio.sleep(2.0)  # Allow cluster formation

        yield servers

    finally:
        # Proper cleanup
        for server in servers:
            try:
                server.shutdown()
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

        await asyncio.sleep(0.5)


@pytest.fixture
async def field_access_cluster():
    """Create a cluster for testing field access patterns."""
    servers = []
    port_manager = TestPortManager()

    try:
        port1 = port_manager.get_server_port()
        port2 = port_manager.get_server_port()

        server1 = MPREGServer(
            MPREGSettings(
                port=port1,
                name="Sensor-Server",
                resources={"sensors"},
                log_level="INFO",
            )
        )

        server2 = MPREGServer(
            MPREGSettings(
                port=port2,
                name="Alert-Server",
                resources={"realtime"},
                peers=[f"ws://127.0.0.1:{port1}"],
                log_level="INFO",
            )
        )

        # Register functions that match the failing test pattern
        def process_sensors(sensor_id: str, data: list) -> dict:
            return {
                "sensor": sensor_id,
                "processed_readings": len(data),
                "average": sum(data) / len(data) if data else 0,
                "edge_node": "Sensor-Server",
                "latency": "1ms",
            }

        def check_alert(threshold: float, current_value: float) -> dict:
            alert = current_value > threshold
            return {
                "alert": alert,
                "threshold": threshold,
                "current": current_value,
                "severity": "HIGH" if alert else "NORMAL",
                "response_time": "0.5ms",
            }

        server1.register_command("process_sensors", process_sensors, ["sensors"])
        server2.register_command("check_alert", check_alert, ["realtime"])

        servers = [server1, server2]

        # Start servers
        tasks = []
        for server in servers:
            task = asyncio.create_task(server.server())
            tasks.append(task)
            await asyncio.sleep(0.1)

        await asyncio.sleep(2.0)  # Allow cluster formation

        yield servers

    finally:
        # Proper cleanup
        for server in servers:
            try:
                server.shutdown()
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

        await asyncio.sleep(0.5)


class TestDependencyResolution:
    """Test dependency resolution patterns."""

    async def test_simple_dependency_chain(self, two_server_cluster):
        """Test simple two-step dependency resolution."""
        servers = two_server_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{servers[0].settings.port}"
        ) as client:
            # Test dependency resolution
            result = await client._client.request(
                [
                    RPCCommand(
                        name="step1_result",
                        fun="first_step",
                        args=("test_input",),
                        locs=frozenset(["step1"]),
                    ),
                    RPCCommand(
                        name="step2_result",
                        fun="second_step",
                        args=("step1_result", 5),  # Should resolve step1_result
                        locs=frozenset(["step2"]),
                    ),
                ]
            )

            assert "step2_result" in result
            assert result["step2_result"]["final"] == "final_processed_test_input"
            assert result["step2_result"]["computed"] == 210  # 42 * 5

    async def test_field_access_dependency(self, field_access_cluster):
        """Test dependency resolution with field access."""
        servers = field_access_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{servers[0].settings.port}"
        ) as client:
            # Test dependency resolution with field access
            result = await client._client.request(
                [
                    RPCCommand(
                        name="edge_data",
                        fun="process_sensors",
                        args=("temp_sensor", [20.1, 20.5, 21.0]),
                        locs=frozenset(["sensors"]),
                    ),
                    RPCCommand(
                        name="alert_check",
                        fun="check_alert",
                        args=(20.8, "edge_data.average"),  # Field access!
                        locs=frozenset(["realtime"]),
                    ),
                ]
            )

            assert "alert_check" in result
            assert (
                result["alert_check"]["current"] == 20.533333333333335
            )  # Average of [20.1, 20.5, 21.0]
            assert result["alert_check"]["severity"] == "NORMAL"  # 20.533 < 20.8
            assert not result["alert_check"]["alert"]


class TestComplexDependencyChains:
    """Test more complex dependency patterns that should work."""

    async def test_three_step_dependency_chain(self, two_server_cluster):
        """Test a three-step dependency chain that reuses existing infrastructure."""
        servers = two_server_cluster

        # Add a third function to server1 for the final step
        def third_step(final_data: str, computed_val: int) -> dict:
            return {
                "ultimate_result": f"ultimate_{final_data}",
                "ultimate_number": computed_val + 100,
            }

        servers[0].register_command("third_step", third_step, ["step1"])

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{servers[0].settings.port}"
        ) as client:
            # Test 3-step dependency chain
            result = await client._client.request(
                [
                    RPCCommand(
                        name="step1_result",
                        fun="first_step",
                        args=("input",),
                        locs=frozenset(["step1"]),
                    ),
                    RPCCommand(
                        name="step2_result",
                        fun="second_step",
                        args=("step1_result", 3),
                        locs=frozenset(["step2"]),
                    ),
                    RPCCommand(
                        name="step3_result",
                        fun="third_step",
                        args=(
                            "step2_result.final",
                            "step2_result.computed",
                        ),  # Field access
                        locs=frozenset(["step1"]),  # Back to first server
                    ),
                ]
            )

            assert "step3_result" in result
            assert (
                result["step3_result"]["ultimate_result"]
                == "ultimate_final_processed_input"
            )
            assert result["step3_result"]["ultimate_number"] == 226  # (42 * 3) + 100

    async def test_four_step_dependency_chain(self, two_server_cluster):
        """Test a four-step dependency chain to find the breaking point."""
        servers = two_server_cluster

        # Add functions for steps 3 and 4
        def third_step(final_data: str, computed_val: int) -> dict:
            return {
                "step3_result": f"step3_{final_data}",
                "step3_number": computed_val + 50,
            }

        def fourth_step(data: str, number: int) -> dict:
            return {"final_result": f"final_{data}", "final_number": number * 2}

        servers[0].register_command("third_step", third_step, ["step1"])
        servers[1].register_command("fourth_step", fourth_step, ["step2"])

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{servers[0].settings.port}"
        ) as client:
            # Test 4-step dependency chain
            result = await client._client.request(
                [
                    # Step 1: Local execution
                    RPCCommand(
                        name="step1_result",
                        fun="first_step",
                        args=("input",),
                        locs=frozenset(["step1"]),
                    ),
                    # Step 2: Remote execution depends on step1
                    RPCCommand(
                        name="step2_result",
                        fun="second_step",
                        args=("step1_result", 2),
                        locs=frozenset(["step2"]),
                    ),
                    # Step 3: Local execution depends on step2 fields
                    RPCCommand(
                        name="step3_result",
                        fun="third_step",
                        args=("step2_result.final", "step2_result.computed"),
                        locs=frozenset(["step1"]),
                    ),
                    # Step 4: Remote execution depends on step3 fields
                    RPCCommand(
                        name="step4_result",
                        fun="fourth_step",
                        args=("step3_result.step3_result", "step3_result.step3_number"),
                        locs=frozenset(["step2"]),
                    ),
                ]
            )

            assert "step4_result" in result
            # Verify the full chain executed: 42 -> 84 -> 134 -> 268
            assert result["step4_result"]["final_number"] == 268
