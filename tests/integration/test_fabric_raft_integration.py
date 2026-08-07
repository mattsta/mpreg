import asyncio

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.datastructures.production_raft import RaftState
from mpreg.datastructures.production_raft_implementation import (
    ProductionRaft,
    RaftConfiguration,
)
from mpreg.datastructures.raft_storage_adapters import RaftStorageFactory
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager, wait_for_condition
from tests.test_production_raft_integration import TestableStateMachine


async def _wait_for_peers(server: MPREGServer, expected: int) -> None:
    await wait_for_condition(
        lambda: len(server.cluster.peer_neighbors()) >= expected,
        timeout=8.0,
        interval=0.2,
        error_message=f"Server {server.settings.name} did not see {expected} peers",
    )


@pytest.mark.asyncio
async def test_fabric_raft_leader_election_and_replication() -> None:
    async with AsyncTestContext() as ctx:
        with TestPortManager() as port_manager:
            ports = [port_manager.get_server_port() for _ in range(3)]
            cluster_id = "fabric-raft"

            settings = []
            for idx, port in enumerate(ports):
                connect = None
                if idx > 0:
                    connect = f"ws://127.0.0.1:{ports[0]}"
                settings.append(
                    MPREGSettings(
                        host="127.0.0.1",
                        port=port,
                        name=f"Fabric-Raft-{idx}",
                        cluster_id=cluster_id,
                        connect=connect,
                        gossip_interval=0.5,
                        fabric_routing_enabled=True,
                    )
                )

            servers = [MPREGServer(settings=s) for s in settings]
            ctx.servers.extend(servers)
            ctx.tasks.extend(
                [asyncio.create_task(server.server()) for server in servers]
            )

            await asyncio.sleep(1.5)
            for server in servers:
                await _wait_for_peers(server, expected=2)

            node_ids = {server.cluster.local_url for server in servers}
            raft_config = RaftConfiguration(
                election_timeout_min=0.2,
                election_timeout_max=0.35,
                heartbeat_interval=0.05,
                rpc_timeout=0.12,
            )

            raft_nodes: dict[str, ProductionRaft] = {}
            state_machines: dict[str, TestableStateMachine] = {}
            for server in servers:
                node_id = server.cluster.local_url
                transport = server.fabric_raft_transport()
                if transport is None:
                    raise AssertionError("Fabric Raft transport not initialized")
                storage = RaftStorageFactory.create_memory_storage(f"raft_{node_id}")
                state_machine = TestableStateMachine()
                state_machines[node_id] = state_machine
                node = ProductionRaft(
                    node_id=node_id,
                    cluster_members=set(node_ids),
                    storage=storage,
                    transport=transport,
                    state_machine=state_machine,
                    config=raft_config,
                )
                server.register_raft_node(node)
                raft_nodes[node_id] = node

            ctx.tasks.extend(
                [asyncio.create_task(node.start()) for node in raft_nodes.values()]
            )

            leader_id = None
            deadline = asyncio.get_running_loop().time() + 6.0
            while asyncio.get_running_loop().time() < deadline:
                leaders = [
                    node_id
                    for node_id, node in raft_nodes.items()
                    if node.current_state == RaftState.LEADER
                ]
                if len(leaders) == 1:
                    leader_id = leaders[0]
                    break
                await asyncio.sleep(0.1)

            assert leader_id is not None
            leader = raft_nodes[leader_id]
            result = await leader.submit_command("set_key=42")
            assert result is not None

            await wait_for_condition(
                lambda: all(
                    machine.state.get("key") == 42
                    for machine in state_machines.values()
                ),
                timeout=6.0,
                interval=0.2,
                error_message="Raft state did not replicate to all nodes",
            )

            await asyncio.gather(
                *(node.stop() for node in raft_nodes.values()),
                return_exceptions=True,
            )
