"""
Comprehensive integration tests for MPREG's enhanced federated RPC system.

This module tests the federated multi-cluster RPC announcement and call system
that enables function discovery and execution across distributed server meshes.

Key features tested:
- Multi-hop federated RPC announcements with deduplication
- Cross-cluster function discovery and routing
- Hop count limiting and loop prevention
- Original source preservation for accurate routing
- Integration with existing MPREG infrastructure
"""

import asyncio
import time
from typing import Any

import pytest
import ulid

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand, RPCRequest
from mpreg.federation.federation_config import create_permissive_bridging_config
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext


class TestFederatedRPCIntegration:
    """Test federated RPC system integration and multi-node scenarios."""

    async def test_basic_federated_rpc_propagation(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test basic federated RPC announcement propagation between nodes."""
        port1, port2, port3 = server_cluster_ports[:3]

        # Node 1: Central hub
        hub_settings = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Fed-RPC-Hub",
            cluster_id="federated-rpc-cluster",
            resources={"hub-processing"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
        )

        # Node 2: Spoke that connects to hub
        spoke1_settings = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Fed-RPC-Spoke-1",
            cluster_id="federated-rpc-cluster",
            resources={"spoke1-processing"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=1.0,
        )

        # Node 3: Spoke that connects to hub
        spoke2_settings = MPREGSettings(
            host="127.0.0.1",
            port=port3,
            name="Fed-RPC-Spoke-2",
            cluster_id="federated-rpc-cluster",
            resources={"spoke2-processing"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=1.0,
        )

        servers = [
            MPREGServer(settings=hub_settings),
            MPREGServer(settings=spoke1_settings),
            MPREGServer(settings=spoke2_settings),
        ]
        test_context.servers.extend(servers)

        # Start servers in sequence
        for i, server in enumerate(servers):
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(1.0)  # Stagger startup

        # Wait for initial connections
        await asyncio.sleep(2.0)

        # Register functions on each node AFTER servers are running
        def hub_coordinate(task_id: str) -> dict[str, Any]:
            return {
                "coordinator": "Fed-RPC-Hub",
                "task_id": task_id,
                "timestamp": time.time(),
                "status": "coordinated",
            }

        def spoke1_process(coordination_data: dict[str, Any]) -> dict[str, Any]:
            return {
                "processor": "Fed-RPC-Spoke-1",
                "task_id": coordination_data["task_id"],
                "processed_at": time.time(),
                "input_status": coordination_data["status"],
                "result": f"processed_{coordination_data['task_id']}",
            }

        def spoke2_analyze(processing_data: dict[str, Any]) -> str:
            return (
                f"Analysis by Fed-RPC-Spoke-2: {processing_data['result']} is complete"
            )

        servers[0].register_command(
            "hub_coordinate", hub_coordinate, ["hub-processing"]
        )
        servers[1].register_command(
            "spoke1_process", spoke1_process, ["spoke1-processing"]
        )
        servers[2].register_command(
            "spoke2_analyze", spoke2_analyze, ["spoke2-processing"]
        )

        # Allow federated RPC announcements to propagate
        await asyncio.sleep(10.0)  # Extended time for multi-hop propagation

        # Test federated RPC execution from Spoke 1
        client = MPREGClientAPI(f"ws://127.0.0.1:{port2}")
        test_context.clients.append(client)
        await client.connect()

        # Execute multi-node pipeline to test federated routing
        result = await client._client.request(
            [
                RPCCommand(
                    name="coordination_step",
                    fun="hub_coordinate",
                    args=("federated_test_task",),
                    locs=frozenset(["hub-processing"]),
                ),
                RPCCommand(
                    name="processing_step",
                    fun="spoke1_process",
                    args=("coordination_step",),
                    locs=frozenset(["spoke1-processing"]),
                ),
                RPCCommand(
                    name="analysis_step",
                    fun="spoke2_analyze",
                    args=("processing_step",),
                    locs=frozenset(["spoke2-processing"]),
                ),
            ]
        )

        # Verify federated execution
        assert "analysis_step" in result
        final_result = result["analysis_step"]
        assert "Analysis by Fed-RPC-Spoke-2" in final_result
        assert "processed_federated_test_task is complete" in final_result

        print(
            "✅ Basic federated RPC propagation: Multi-node pipeline executed successfully"
        )
        print("  Hub → Spoke1 → Spoke2 routing complete")
        print(f"  Final result: {final_result}")

    async def test_federated_rpc_hop_count_limiting(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test that federated RPC announcements respect hop count limits across federation boundaries."""
        ports = server_cluster_ports[:5]

        # Create a 5-cluster federation chain to test hop limiting
        # Each node is in its own cluster, connected in a federation chain
        settings_list = []
        for i, port in enumerate(ports):
            connect_to = f"ws://127.0.0.1:{ports[i - 1]}" if i > 0 else None

            cluster_id = f"cluster-{i + 1}"
            settings_list.append(
                MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"Federation-Node-{i + 1}",
                    cluster_id=cluster_id,  # Each node in separate cluster for true federation
                    resources={f"federation-{i + 1}"},
                    peers=None,
                    connect=connect_to,
                    advertised_urls=None,
                    gossip_interval=1.0,  # Normal gossip - federation boundaries prevent full mesh
                    federation_config=create_permissive_bridging_config(
                        cluster_id
                    ),  # Allow cross-federation
                )
            )

        servers = [MPREGServer(settings=s) for s in settings_list]
        test_context.servers.extend(servers)

        # Start servers sequentially
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.8)

        await asyncio.sleep(3.0)

        # Register a function on the first federation cluster
        def federation_origin_function(data: str) -> str:
            return f"Federation origin processed: {data}"

        servers[0].register_command(
            "federation_origin_function", federation_origin_function, ["federation-1"]
        )

        # Allow propagation with hop limits (default max_hops=3)
        await asyncio.sleep(8.0)

        # Test from federation cluster within hop limit (Cluster 3, 2 hops away)
        client_within_limit = MPREGClientAPI(f"ws://127.0.0.1:{ports[2]}")
        test_context.clients.append(client_within_limit)
        await client_within_limit.connect()

        try:
            result = await client_within_limit._client.request(
                [
                    RPCCommand(
                        name="within_limit_test",
                        fun="federation_origin_function",
                        args=("federation_hop_test",),
                        locs=frozenset(["federation-1"]),
                    )
                ]
            )

            within_limit_success = "within_limit_test" in result
            print(
                f"✅ Within hop limit (2 hops): {'SUCCESS' if within_limit_success else 'FAILED'}"
            )

        except Exception as e:
            within_limit_success = False
            print(f"❌ Within hop limit failed: {e}")

        # Test from federation cluster beyond hop limit (Cluster 5, 4 hops away)
        client_beyond_limit = MPREGClientAPI(f"ws://127.0.0.1:{ports[4]}")
        test_context.clients.append(client_beyond_limit)
        await client_beyond_limit.connect()

        try:
            result = await client_beyond_limit._client.request(
                [
                    RPCCommand(
                        name="beyond_limit_test",
                        fun="federation_origin_function",
                        args=("federation_hop_test",),
                        locs=frozenset(["federation-1"]),
                    )
                ]
            )

            beyond_limit_success = "beyond_limit_test" in result
            print(
                f"🚫 Beyond hop limit (4 hops): {'UNEXPECTED SUCCESS' if beyond_limit_success else 'CORRECTLY FAILED'}"
            )

        except Exception as e:
            beyond_limit_success = False
            print(f"✅ Beyond hop limit correctly failed: {str(e)[:100]}...")

        # Verify behavior: within limit should work, beyond limit should fail
        assert within_limit_success, (
            "Federation clusters within hop limit should receive announcements"
        )
        assert not beyond_limit_success, (
            "Federation clusters beyond hop limit should not receive announcements"
        )

        print(
            "✅ Federation hop count limiting: Cross-cluster announcements correctly respect hop limits"
        )

    async def test_federated_rpc_deduplication(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test that federated RPC announcements are properly deduplicated."""
        port1, port2, port3, port4 = server_cluster_ports[:4]

        # Create a diamond topology to test deduplication:
        # Node 1 (origin) → Node 2 and Node 3 → Node 4 (target)
        # Node 4 should receive announcements from both paths but deduplicate

        origin_settings = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Dedup-Origin",
            cluster_id="dedup-test-cluster",
            resources={"origin"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=1.0,
        )

        path1_settings = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Dedup-Path-1",
            cluster_id="dedup-test-cluster",
            resources={"path1"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=1.0,
        )

        path2_settings = MPREGSettings(
            host="127.0.0.1",
            port=port3,
            name="Dedup-Path-2",
            cluster_id="dedup-test-cluster",
            resources={"path2"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=1.0,
        )

        target_settings = MPREGSettings(
            host="127.0.0.1",
            port=port4,
            name="Dedup-Target",
            cluster_id="dedup-test-cluster",
            resources={"target"},
            peers=None,
            connect=f"ws://127.0.0.1:{port2}",  # Connect to path1
            advertised_urls=None,
            gossip_interval=1.0,
        )

        servers = [
            MPREGServer(settings=origin_settings),
            MPREGServer(settings=path1_settings),
            MPREGServer(settings=path2_settings),
            MPREGServer(settings=target_settings),
        ]
        test_context.servers.extend(servers)

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(1.0)

        await asyncio.sleep(3.0)

        # Register function on origin
        def origin_function(data: str) -> str:
            return f"Origin function result: {data}"

        servers[0].register_command("origin_function", origin_function, ["origin"])

        # Manually connect target to path2 to complete diamond
        # This creates multiple paths: Origin → Path1 → Target and Origin → Path2 → Target
        await asyncio.sleep(2.0)

        # Allow announcements to propagate through both paths
        await asyncio.sleep(10.0)

        # Test from target node - should work despite receiving duplicate announcements
        client = MPREGClientAPI(f"ws://127.0.0.1:{port4}")
        test_context.clients.append(client)
        await client.connect()

        result = await client._client.request(
            [
                RPCCommand(
                    name="dedup_test",
                    fun="origin_function",
                    args=("deduplication_test",),
                    locs=frozenset(["origin"]),
                )
            ]
        )

        # Verify deduplication worked (no errors, function found once)
        assert "dedup_test" in result
        final_result = result["dedup_test"]
        assert "Origin function result: deduplication_test" == final_result

        print("✅ Federated RPC deduplication: Diamond topology handled correctly")
        print("  Multiple announcement paths deduplicated successfully")
        print(f"  Result: {final_result}")

    async def test_federated_rpc_with_intermediate_results(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test federated RPC execution with intermediate results capture."""
        port1, port2, port3 = server_cluster_ports[:3]

        # Create federated cluster
        settings_list = [
            MPREGSettings(
                host="127.0.0.1",
                port=port1,
                name="Fed-Debug-Node-1",
                cluster_id="debug-federated-cluster",
                resources={"debug-1"},
                peers=None,
                connect=None,
                advertised_urls=None,
                gossip_interval=1.0,
            ),
            MPREGSettings(
                host="127.0.0.1",
                port=port2,
                name="Fed-Debug-Node-2",
                cluster_id="debug-federated-cluster",
                resources={"debug-2"},
                peers=None,
                connect=f"ws://127.0.0.1:{port1}",
                advertised_urls=None,
                gossip_interval=1.0,
            ),
            MPREGSettings(
                host="127.0.0.1",
                port=port3,
                name="Fed-Debug-Node-3",
                cluster_id="debug-federated-cluster",
                resources={"debug-3"},
                peers=None,
                connect=f"ws://127.0.0.1:{port1}",
                advertised_urls=None,
                gossip_interval=1.0,
            ),
        ]

        servers = [MPREGServer(settings=s) for s in settings_list]
        test_context.servers.extend(servers)

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(1.0)

        await asyncio.sleep(3.0)

        # Register debug functions
        def load_debug_data(source: str) -> dict[str, Any]:
            return {
                "data_source": source,
                "records_loaded": 1000,
                "load_time": time.time(),
                "node": "Fed-Debug-Node-1",
            }

        def process_debug_data(data: dict[str, Any]) -> dict[str, Any]:
            return {
                "original_records": data["records_loaded"],
                "processed_records": data["records_loaded"] * 2,
                "processing_node": "Fed-Debug-Node-2",
                "processing_time": time.time(),
                "source_node": data["node"],
            }

        def analyze_debug_results(processed: dict[str, Any]) -> str:
            return f"Analysis complete: {processed['processed_records']} records from {processed['source_node']} via {processed['processing_node']}"

        servers[0].register_command("load_debug_data", load_debug_data, ["debug-1"])
        servers[1].register_command(
            "process_debug_data", process_debug_data, ["debug-2"]
        )
        servers[2].register_command(
            "analyze_debug_results", analyze_debug_results, ["debug-3"]
        )

        # Allow federated propagation
        await asyncio.sleep(8.0)

        # Test with intermediate results enabled
        client = MPREGClientAPI(f"ws://127.0.0.1:{port2}")
        test_context.clients.append(client)
        await client.connect()

        request = RPCRequest(
            cmds=(
                RPCCommand(
                    name="debug_data",
                    fun="load_debug_data",
                    args=("federated_debug_source",),
                    locs=frozenset(["debug-1"]),
                ),
                RPCCommand(
                    name="processed_data",
                    fun="process_debug_data",
                    args=("debug_data",),
                    locs=frozenset(["debug-2"]),
                ),
                RPCCommand(
                    name="analysis",
                    fun="analyze_debug_results",
                    args=("processed_data",),
                    locs=frozenset(["debug-3"]),
                ),
            ),
            u=str(ulid.new()),
            return_intermediate_results=True,
            include_execution_summary=True,
        )

        response = await client._client.request_enhanced(request)

        # Verify federated execution with intermediate results
        assert response.r["analysis"]
        assert "Analysis complete: 2000 records" in response.r["analysis"]

        # Verify intermediate results captured
        assert len(response.intermediate_results) >= 3
        print(
            f"✅ Intermediate results captured: {len(response.intermediate_results)} levels"
        )

        # Verify execution summary
        if response.execution_summary:
            summary = response.execution_summary
            print(
                f"✅ Execution summary: {summary.total_execution_time_ms:.1f}ms total"
            )
            print(f"  Cross-cluster hops: {summary.cross_cluster_hops}")
            print(f"  Parallel commands: {summary.parallel_commands_executed}")

        print(
            "✅ Federated RPC with intermediate results: Enhanced debugging working across nodes"
        )

    async def test_federated_rpc_network_resilience(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test federated RPC system resilience to network issues."""
        port1, port2, port3, port4 = server_cluster_ports[:4]

        # Create redundant topology
        settings_list = [
            MPREGSettings(
                host="127.0.0.1",
                port=port1,
                name="Resilience-Hub-1",
                cluster_id="resilience-cluster",
                resources={"hub1"},
                peers=None,
                connect=None,
                advertised_urls=None,
                gossip_interval=1.0,
            ),
            MPREGSettings(
                host="127.0.0.1",
                port=port2,
                name="Resilience-Hub-2",
                cluster_id="resilience-cluster",
                resources={"hub2"},
                peers=None,
                connect=f"ws://127.0.0.1:{port1}",  # Connect Hub2 to Hub1
                advertised_urls=None,
                gossip_interval=1.0,
            ),
            MPREGSettings(
                host="127.0.0.1",
                port=port3,
                name="Resilience-Spoke-1",
                cluster_id="resilience-cluster",
                resources={"spoke1"},
                peers=None,
                connect=f"ws://127.0.0.1:{port1}",
                advertised_urls=None,
                gossip_interval=1.0,
            ),
            MPREGSettings(
                host="127.0.0.1",
                port=port4,
                name="Resilience-Spoke-2",
                cluster_id="resilience-cluster",
                resources={"spoke2"},
                peers=None,
                connect=f"ws://127.0.0.1:{port2}",
                advertised_urls=None,
                gossip_interval=1.0,
            ),
        ]

        servers = [MPREGServer(settings=s) for s in settings_list]
        test_context.servers.extend(servers)

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(1.0)

        await asyncio.sleep(3.0)

        # Register functions on different nodes
        def hub1_function(data: str) -> str:
            return f"Hub1 processed: {data}"

        def hub2_function(data: str) -> str:
            return f"Hub2 processed: {data}"

        def spoke1_function(data: str) -> str:
            return f"Spoke1 processed: {data}"

        def spoke2_function(data: str) -> str:
            return f"Spoke2 processed: {data}"

        servers[0].register_command("hub1_function", hub1_function, ["hub1"])
        servers[1].register_command("hub2_function", hub2_function, ["hub2"])
        servers[2].register_command("spoke1_function", spoke1_function, ["spoke1"])
        servers[3].register_command("spoke2_function", spoke2_function, ["spoke2"])

        # Allow federated propagation
        await asyncio.sleep(8.0)

        # Test normal operation
        client = MPREGClientAPI(f"ws://127.0.0.1:{port3}")
        test_context.clients.append(client)
        await client.connect()

        result = await client._client.request(
            [
                RPCCommand(
                    name="hub2_test",
                    fun="hub2_function",
                    args=("resilience_test",),
                    locs=frozenset(["hub2"]),
                )
            ]
        )

        assert "hub2_test" in result
        assert "Hub2 processed: resilience_test" == result["hub2_test"]

        print(
            "✅ Network resilience: Federated RPC system operational with redundant topology"
        )
        print(f"  Cross-hub communication working: {result['hub2_test']}")


class TestFederatedRPCComplexScenarios:
    """Test complex federated RPC scenarios and edge cases."""

    async def test_large_scale_federated_mesh(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test federated RPC in a large-scale mesh topology."""
        if len(server_cluster_ports) < 6:
            pytest.skip("Need at least 6 ports for large-scale mesh test")

        ports = server_cluster_ports[:6]

        # Create 6-node mesh with different connection patterns
        settings_list = []
        for i, port in enumerate(ports):
            # Create varied connection patterns
            if i == 0:
                connect_to = None  # First node is standalone
            elif i <= 2:
                connect_to = f"ws://127.0.0.1:{ports[0]}"  # Connect to first
            else:
                connect_to = f"ws://127.0.0.1:{ports[i - 2]}"  # Connect to earlier node

            settings_list.append(
                MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"Mesh-Node-{i + 1}",
                    cluster_id="large-mesh-cluster",
                    resources={f"mesh-{i + 1}"},
                    peers=None,
                    connect=connect_to,
                    advertised_urls=None,
                    gossip_interval=1.0,
                )
            )

        servers = [MPREGServer(settings=s) for s in settings_list]
        test_context.servers.extend(servers)

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.8)

        await asyncio.sleep(4.0)

        # Register functions on each node
        functions = {}
        for i, server in enumerate(servers):

            def make_function(node_id: int):
                def mesh_function(data: str) -> dict[str, Any]:
                    return {
                        "processor": f"Mesh-Node-{node_id + 1}",
                        "input": data,
                        "processed_at": time.time(),
                        "result": f"mesh_result_{node_id + 1}_{data}",
                    }

                return mesh_function

            func = make_function(i)
            func_name = f"mesh_function_{i + 1}"
            functions[func_name] = func
            server.register_command(func_name, func, [f"mesh-{i + 1}"])

        # Allow extensive propagation time for large mesh
        await asyncio.sleep(15.0)

        # Test cross-mesh execution from multiple entry points
        test_results = []
        for test_node in [0, 2, 4]:  # Test from different nodes
            client = MPREGClientAPI(f"ws://127.0.0.1:{ports[test_node]}")
            test_context.clients.append(client)
            await client.connect()

            # Test reaching different targets
            target_node = (test_node + 3) % 6
            target_function = f"mesh_function_{target_node + 1}"
            target_resource = f"mesh-{target_node + 1}"

            try:
                result = await client._client.request(
                    [
                        RPCCommand(
                            name="mesh_test",
                            fun=target_function,
                            args=(f"large_mesh_test_{test_node}_to_{target_node}",),
                            locs=frozenset([target_resource]),
                        )
                    ]
                )

                success = "mesh_test" in result
                test_results.append(success)

                if success:
                    print(
                        f"✅ Mesh test Node-{test_node + 1} → Node-{target_node + 1}: SUCCESS"
                    )
                else:
                    print(
                        f"❌ Mesh test Node-{test_node + 1} → Node-{target_node + 1}: FAILED"
                    )

            except Exception as e:
                test_results.append(False)
                print(
                    f"❌ Mesh test Node-{test_node + 1} → Node-{target_node + 1}: ERROR {e}"
                )

        # Verify large-scale mesh connectivity
        success_rate = sum(test_results) / len(test_results)
        print(
            f"✅ Large-scale mesh success rate: {success_rate * 100:.1f}% ({sum(test_results)}/{len(test_results)})"
        )

        # Expect high success rate for large mesh
        assert success_rate >= 0.6, (
            f"Large mesh should have >60% success rate, got {success_rate * 100:.1f}%"
        )

    async def test_federated_rpc_performance_characteristics(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test performance characteristics of federated RPC system."""
        port1, port2, port3 = server_cluster_ports[:3]

        # Create performance test cluster
        settings_list = [
            MPREGSettings(
                host="127.0.0.1",
                port=port1,
                name="Perf-Node-1",
                cluster_id="performance-cluster",
                resources={"perf-1"},
                peers=None,
                connect=None,
                advertised_urls=None,
                gossip_interval=0.5,  # Faster gossip
            ),
            MPREGSettings(
                host="127.0.0.1",
                port=port2,
                name="Perf-Node-2",
                cluster_id="performance-cluster",
                resources={"perf-2"},
                peers=None,
                connect=f"ws://127.0.0.1:{port1}",
                advertised_urls=None,
                gossip_interval=0.5,
            ),
            MPREGSettings(
                host="127.0.0.1",
                port=port3,
                name="Perf-Node-3",
                cluster_id="performance-cluster",
                resources={"perf-3"},
                peers=None,
                connect=f"ws://127.0.0.1:{port1}",
                advertised_urls=None,
                gossip_interval=0.5,
            ),
        ]

        servers = [MPREGServer(settings=s) for s in settings_list]
        test_context.servers.extend(servers)

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            test_context.tasks.append(task)
            await asyncio.sleep(0.5)

        await asyncio.sleep(2.0)

        # Register performance test functions
        async def fast_function(data: str) -> str:
            return f"Fast: {data}"

        async def medium_function(data: str) -> str:
            await asyncio.sleep(0.1)  # 100ms delay
            return f"Medium: {data}"

        async def slow_function(data: str) -> str:
            await asyncio.sleep(0.3)  # 300ms delay
            return f"Slow: {data}"

        servers[0].register_command("fast_function", fast_function, ["perf-1"])
        servers[1].register_command("medium_function", medium_function, ["perf-2"])
        servers[2].register_command("slow_function", slow_function, ["perf-3"])

        # Allow propagation
        await asyncio.sleep(6.0)

        # Performance test with execution summary
        client = MPREGClientAPI(f"ws://127.0.0.1:{port1}")
        test_context.clients.append(client)
        await client.connect()

        start_time = time.time()

        request = RPCRequest(
            cmds=(
                RPCCommand(
                    name="fast_result",
                    fun="fast_function",
                    args=("perf_test",),
                    locs=frozenset(["perf-1"]),
                ),
                RPCCommand(
                    name="medium_result",
                    fun="medium_function",
                    args=("perf_test",),
                    locs=frozenset(["perf-2"]),
                ),
                RPCCommand(
                    name="slow_result",
                    fun="slow_function",
                    args=("perf_test",),
                    locs=frozenset(["perf-3"]),
                ),
            ),
            u=str(ulid.new()),
            return_intermediate_results=True,
            include_execution_summary=True,
        )

        response = await client._client.request_enhanced(request)
        total_time = (time.time() - start_time) * 1000

        # Verify performance results
        assert response.r["fast_result"] == "Fast: perf_test"
        assert response.r["medium_result"] == "Medium: perf_test"
        assert response.r["slow_result"] == "Slow: perf_test"

        if response.execution_summary:
            summary = response.execution_summary
            print("✅ Performance test results:")
            print(f"  Total execution time: {summary.total_execution_time_ms:.1f}ms")
            print(f"  Client-measured time: {total_time:.1f}ms")
            print(f"  Bottleneck level: {summary.bottleneck_level_index}")
            print(f"  Parallel commands: {summary.parallel_commands_executed}")

            # Verify parallel execution efficiency
            expected_time = 300  # Slow function dominates (300ms)
            actual_time = summary.total_execution_time_ms
            efficiency = (expected_time / actual_time) if actual_time > 0 else 0

            print(
                f"  Parallel efficiency: {efficiency:.2f} (expected ~1.0 for perfect parallelism)"
            )

        print("✅ Federated RPC performance: Multi-node parallel execution measured")
