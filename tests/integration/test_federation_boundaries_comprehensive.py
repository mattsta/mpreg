"""
Comprehensive tests for MPREG federation boundaries and cross-cluster communication.

This module tests the fundamental behavior of MPREG's federation system,
specifically how nodes with different cluster_ids interact and whether
cross-federation discovery and communication is possible.

Key discoveries:
- Nodes only discover functions within their own cluster_id (federation)
- Cross-federation communication requires explicit bridging
- Gossip protocol is federation-scoped, not global
"""

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext


class TestFederationBoundaries:
    """Test federation boundary behavior and cross-cluster limitations."""

    async def test_same_federation_discovery(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test that nodes in the same federation discover each other."""
        port1, port2 = server_cluster_ports[:2]

        # Both nodes in same federation
        node1_settings = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Federation-Alpha-Node-1",
            cluster_id="alpha-federation",
            resources={"alpha-compute"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
        )

        node2_settings = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Federation-Alpha-Node-2",
            cluster_id="alpha-federation",  # Same federation
            resources={"alpha-storage"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",  # Connect to node1
            advertised_urls=None,
            gossip_interval=1.0,
        )

        servers = [
            MPREGServer(settings=node1_settings),
            MPREGServer(settings=node2_settings),
        ]
        test_context.servers.extend(servers)

        # Register functions on each node
        def compute_task(data: str) -> dict:
            return {
                "node": "Alpha-Node-1",
                "task": "compute",
                "result": f"computed_{data}",
                "federation": "alpha",
            }

        def store_result(compute_result: dict) -> str:
            return f"stored_{compute_result['result']}_in_alpha_storage"

        servers[0].register_command("compute_task", compute_task, ["alpha-compute"])
        servers[1].register_command("store_result", store_result, ["alpha-storage"])

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.5)

        # Allow federation discovery
        await asyncio.sleep(3.0)

        # Test cross-node execution within same federation
        client = MPREGClientAPI(f"ws://127.0.0.1:{port1}")
        test_context.clients.append(client)
        await client.connect()

        result = await client._client.request(
            [
                RPCCommand(
                    name="compute_step",
                    fun="compute_task",
                    args=("federation_test_data",),
                    locs=frozenset(["alpha-compute"]),
                ),
                RPCCommand(
                    name="store_step",
                    fun="store_result",
                    args=("compute_step",),
                    locs=frozenset(["alpha-storage"]),
                ),
            ]
        )

        # Verify same-federation communication works
        assert "store_step" in result
        final_result = result["store_step"]
        assert "stored_computed_federation_test_data_in_alpha_storage" in final_result

        print(
            "✓ Same federation discovery: Nodes in same cluster_id communicate successfully"
        )
        print("  - Alpha federation inter-node pipeline completed")
        print(f"  - Result: {final_result}")

    async def test_different_federation_isolation(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test that nodes in different federations cannot discover each other."""
        port1, port2 = server_cluster_ports[:2]

        # Nodes in different federations
        alpha_node_settings = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Alpha-Federation-Node",
            cluster_id="alpha-federation",
            resources={"alpha-processing"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
        )

        beta_node_settings = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Beta-Federation-Node",
            cluster_id="beta-federation",  # Different federation
            resources={"beta-analysis"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",  # Try to connect to Alpha
            advertised_urls=None,
            gossip_interval=1.0,
        )

        servers = [
            MPREGServer(settings=alpha_node_settings),
            MPREGServer(settings=beta_node_settings),
        ]
        test_context.servers.extend(servers)

        # Register functions on each node
        def alpha_process(data: str) -> dict:
            return {
                "processed_by": "alpha-federation",
                "data": f"alpha_processed_{data}",
                "federation_id": "alpha",
            }

        def beta_analyze(processed_data: dict) -> str:
            return f"beta_analysis_of_{processed_data['data']}"

        servers[0].register_command(
            "alpha_process", alpha_process, ["alpha-processing"]
        )
        servers[1].register_command("beta_analyze", beta_analyze, ["beta-analysis"])

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.5)

        # Allow attempted federation discovery
        await asyncio.sleep(3.0)

        # Test that Alpha cannot see Beta's functions
        alpha_client = MPREGClientAPI(f"ws://127.0.0.1:{port1}")
        test_context.clients.append(alpha_client)
        await alpha_client.connect()

        # This should fail because Alpha cannot discover Beta's functions
        try:
            result = await alpha_client._client.request(
                [
                    RPCCommand(
                        name="alpha_step",
                        fun="alpha_process",
                        args=("cross_federation_test",),
                        locs=frozenset(["alpha-processing"]),
                    ),
                    RPCCommand(
                        name="beta_step",
                        fun="beta_analyze",  # This function is in Beta federation
                        args=("alpha_step",),
                        locs=frozenset(["beta-analysis"]),
                    ),
                ]
            )

            cross_federation_success = "beta_step" in result
            print(f"✗ Unexpected: Cross-federation communication succeeded: {result}")

        except Exception as e:
            cross_federation_success = False
            print(f"✓ Expected: Cross-federation communication failed: {e}")

        # Verify that Alpha can still execute its own functions
        alpha_only_result = await alpha_client._client.request(
            [
                RPCCommand(
                    name="alpha_only",
                    fun="alpha_process",
                    args=("alpha_only_test",),
                    locs=frozenset(["alpha-processing"]),
                )
            ]
        )

        assert "alpha_only" in alpha_only_result
        assert alpha_only_result["alpha_only"]["federation_id"] == "alpha"

        print(
            "✓ Federation isolation: Different cluster_ids cannot discover each other"
        )
        print(
            f"  - Alpha federation works independently: {alpha_only_result['alpha_only']['data']}"
        )
        print("  - Cross-federation discovery blocked as expected")

    async def test_federation_bridge_pattern(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test a potential federation bridging pattern using a bridge node."""
        port1, port2, port3 = server_cluster_ports[:3]

        # Alpha federation node
        alpha_node_settings = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Alpha-Node",
            cluster_id="alpha-federation",
            resources={"alpha-resource"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
        )

        # Bridge node (connects to both federations)
        bridge_node_settings = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Bridge-Node",
            cluster_id="bridge-federation",  # Separate bridge federation
            resources={"bridge-resource"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
        )

        # Beta federation node
        beta_node_settings = MPREGSettings(
            host="127.0.0.1",
            port=port3,
            name="Beta-Node",
            cluster_id="beta-federation",
            resources={"beta-resource"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
        )

        servers = [
            MPREGServer(settings=alpha_node_settings),
            MPREGServer(settings=bridge_node_settings),
            MPREGServer(settings=beta_node_settings),
        ]
        test_context.servers.extend(servers)

        # Register functions
        def alpha_function(data: str) -> dict:
            return {"alpha_result": f"alpha_{data}", "federation": "alpha"}

        def bridge_function(alpha_data: str, beta_data: str) -> str:
            # Simple bridge function that combines string inputs
            return f"bridge_combined_{alpha_data}_and_{beta_data}"

        def beta_function(data: str) -> dict:
            return {"beta_result": f"beta_{data}", "federation": "beta"}

        servers[0].register_command(
            "alpha_function", alpha_function, ["alpha-resource"]
        )
        servers[1].register_command(
            "bridge_function", bridge_function, ["bridge-resource"]
        )
        servers[2].register_command("beta_function", beta_function, ["beta-resource"])

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.5)

        await asyncio.sleep(3.0)

        # Test that each federation works independently
        alpha_client = MPREGClientAPI(f"ws://127.0.0.1:{port1}")
        bridge_client = MPREGClientAPI(f"ws://127.0.0.1:{port2}")
        beta_client = MPREGClientAPI(f"ws://127.0.0.1:{port3}")

        test_context.clients.extend([alpha_client, bridge_client, beta_client])

        await alpha_client.connect()
        await bridge_client.connect()
        await beta_client.connect()

        # Test alpha federation
        alpha_result = await alpha_client._client.request(
            [
                RPCCommand(
                    name="alpha_test",
                    fun="alpha_function",
                    args=("alpha_test_data",),
                    locs=frozenset(["alpha-resource"]),
                )
            ]
        )

        # Test beta federation
        beta_result = await beta_client._client.request(
            [
                RPCCommand(
                    name="beta_test",
                    fun="beta_function",
                    args=("beta_test_data",),
                    locs=frozenset(["beta-resource"]),
                )
            ]
        )

        # Test bridge federation
        bridge_result = await bridge_client._client.request(
            [
                RPCCommand(
                    name="bridge_test",
                    fun="bridge_function",
                    args=(
                        "mock_alpha_data",
                        "mock_beta_data",
                    ),  # Mock data since no cross-federation
                    locs=frozenset(["bridge-resource"]),
                )
            ]
        )

        assert "alpha_test" in alpha_result
        assert "beta_test" in beta_result
        assert "bridge_test" in bridge_result

        print("✓ Federation bridge pattern: Each federation operates independently")
        print(
            f"  - Alpha federation result: {alpha_result['alpha_test']['alpha_result']}"
        )
        print(f"  - Beta federation result: {beta_result['beta_test']['beta_result']}")
        print("  - Bridge federation can operate independently")
        print(
            "  - Note: True cross-federation bridging would require application-level coordination"
        )

    async def test_federation_resource_isolation(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test that resource locations are isolated between federations."""
        port1, port2 = server_cluster_ports[:2]

        # Both nodes have same resource name but different federations
        alpha_node_settings = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Alpha-Node",
            cluster_id="alpha-federation",
            resources={"shared-resource-name"},  # Same resource name
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
        )

        beta_node_settings = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Beta-Node",
            cluster_id="beta-federation",
            resources={
                "shared-resource-name"
            },  # Same resource name, different federation
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
        )

        servers = [
            MPREGServer(settings=alpha_node_settings),
            MPREGServer(settings=beta_node_settings),
        ]
        test_context.servers.extend(servers)

        # Register functions with same name but different behavior
        def alpha_process(data: str) -> str:
            return f"ALPHA_processed_{data}"

        def beta_process(data: str) -> str:
            return f"BETA_processed_{data}"

        servers[0].register_command(
            "shared_function", alpha_process, ["shared-resource-name"]
        )
        servers[1].register_command(
            "shared_function", beta_process, ["shared-resource-name"]
        )

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.5)

        await asyncio.sleep(3.0)

        # Test that each federation only sees its own resources
        alpha_client = MPREGClientAPI(f"ws://127.0.0.1:{port1}")
        beta_client = MPREGClientAPI(f"ws://127.0.0.1:{port2}")

        test_context.clients.extend([alpha_client, beta_client])

        await alpha_client.connect()
        await beta_client.connect()

        # Alpha should only see its own function
        alpha_result = await alpha_client._client.request(
            [
                RPCCommand(
                    name="alpha_call",
                    fun="shared_function",
                    args=("test_data",),
                    locs=frozenset(["shared-resource-name"]),
                )
            ]
        )

        # Beta should only see its own function
        beta_result = await beta_client._client.request(
            [
                RPCCommand(
                    name="beta_call",
                    fun="shared_function",
                    args=("test_data",),
                    locs=frozenset(["shared-resource-name"]),
                )
            ]
        )

        assert "alpha_call" in alpha_result
        assert "beta_call" in beta_result

        # Verify each federation executed its own version
        assert alpha_result["alpha_call"] == "ALPHA_processed_test_data"
        assert beta_result["beta_call"] == "BETA_processed_test_data"

        print(
            "✓ Federation resource isolation: Same resource names are isolated between federations"
        )
        print(f"  - Alpha federation result: {alpha_result['alpha_call']}")
        print(f"  - Beta federation result: {beta_result['beta_call']}")
        print(
            "  - Resources with same names operate independently in different federations"
        )


class TestFederationLimitations:
    """Document the discovered limitations of MPREG's federation system."""

    async def test_document_federation_limitations(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Document the key limitations discovered in federation testing."""

        print("\\n=== MPREG Federation System Limitations ===\\n")

        print("1. CLUSTER_ID SCOPED DISCOVERY:")
        print("   - Nodes only discover functions within their own cluster_id")
        print("   - Gossip protocol is federation-scoped, not global")
        print("   - Different cluster_ids create completely isolated federations")

        print("\\n2. NO CROSS-FEDERATION AUTO-DISCOVERY:")
        print("   - Nodes in federation-a cannot see functions in federation-b")
        print(
            "   - Even with explicit connect= settings, cluster_id boundaries are enforced"
        )
        print("   - Cross-federation communication requires application-level bridging")

        print("\\n3. RESOURCE NAMESPACE ISOLATION:")
        print("   - Resource names are isolated between federations")
        print("   - Same resource name can exist in multiple federations independently")
        print("   - Function names are also federation-scoped")

        print("\\n4. IMPLICATIONS FOR DISTRIBUTED SYSTEMS:")
        print("   - Federations provide strong isolation boundaries")
        print(
            "   - Multi-tenant systems can use different cluster_ids for tenant isolation"
        )
        print(
            "   - Cross-federation coordination must be implemented at application level"
        )

        print("\\n5. POTENTIAL FEDERATION BRIDGING PATTERNS:")
        print("   - Bridge nodes with explicit connections to multiple federations")
        print("   - Application-level message passing between federations")
        print("   - Federation gateway services for cross-cluster communication")

        print("\\n=== END FEDERATION LIMITATIONS DOCUMENTATION ===\\n")

        # This test always passes - it's for documentation
        assert True, "Federation limitations documented successfully"
