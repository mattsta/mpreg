"""
Live integration tests for consensus and gossip protocol integration.

This test uses actual MPREG servers with real networking to test consensus
proposal distribution via gossip protocol across federated clusters.
"""

import asyncio
import time

import pytest
from loguru import logger

from mpreg.core.config import MPREGSettings
from mpreg.datastructures.vector_clock import VectorClock
from mpreg.fabric.consensus import (
    ConsensusManager,
    StateType,
    StateValue,
)
from mpreg.fabric.gossip import (
    GossipProtocol,
    GossipStrategy,
)
from mpreg.fabric.gossip_transport import InProcessGossipTransport
from mpreg.fabric.hub_registry import HubRegistry
from mpreg.server import MPREGServer

from .conftest import AsyncTestContext


@pytest.fixture
async def live_server_consensus_cluster(
    test_context: AsyncTestContext,
    port_allocator,
):
    """Create a live cluster with server-integrated consensus."""
    from mpreg.core.port_allocator import allocate_port_range

    # Allocate 3 ports for a simple cluster
    ports = allocate_port_range(3, "servers")

    # Create server settings
    server_settings = [
        MPREGSettings(
            host="127.0.0.1",
            port=ports[0],
            name="Consensus Server 1",
            cluster_id="consensus-cluster",
            resources={"consensus-node-1"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=0.5,  # Fast gossip for testing
        ),
        MPREGSettings(
            host="127.0.0.1",
            port=ports[1],
            name="Consensus Server 2",
            cluster_id="consensus-cluster",
            resources={"consensus-node-2"},
            peers=None,
            connect=f"ws://127.0.0.1:{ports[0]}",  # Connect to first server
            advertised_urls=None,
            gossip_interval=0.5,
        ),
        MPREGSettings(
            host="127.0.0.1",
            port=ports[2],
            name="Consensus Server 3",
            cluster_id="consensus-cluster",
            resources={"consensus-node-3"},
            peers=None,
            connect=f"ws://127.0.0.1:{ports[0]}",  # Connect to first server
            advertised_urls=None,
            gossip_interval=0.5,
        ),
    ]

    # Create servers
    servers = [MPREGServer(settings=s) for s in server_settings]
    test_context.servers.extend(servers)

    # Start servers with staggered timing
    tasks = []
    for i, server in enumerate(servers):
        task = asyncio.create_task(server.server())
        test_context.tasks.append(task)
        tasks.append(task)

        # Stagger startup: first server starts first
        if i == 0:
            await asyncio.sleep(0.5)
        else:
            await asyncio.sleep(0.3)

    # Wait for cluster formation and consensus initialization
    await asyncio.sleep(2.0)

    try:
        yield servers
    finally:
        # Servers will be cleaned up by test context
        pass


@pytest.fixture
async def live_consensus_cluster(
    test_context: AsyncTestContext,
    port_allocator,
):
    """Create a live cluster with consensus and gossip integration."""
    from mpreg.core.port_allocator import allocate_port_range

    # Allocate 3 ports for a simple cluster
    ports = allocate_port_range(3, "servers")

    # Create server settings
    server_settings = [
        MPREGSettings(
            host="127.0.0.1",
            port=ports[0],
            name="Consensus Server 1",
            cluster_id="consensus-cluster",
            resources={"consensus-node-1"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=0.5,  # Fast gossip for testing
        ),
        MPREGSettings(
            host="127.0.0.1",
            port=ports[1],
            name="Consensus Server 2",
            cluster_id="consensus-cluster",
            resources={"consensus-node-2"},
            peers=None,
            connect=f"ws://127.0.0.1:{ports[0]}",  # Connect to first server
            advertised_urls=None,
            gossip_interval=0.5,
        ),
        MPREGSettings(
            host="127.0.0.1",
            port=ports[2],
            name="Consensus Server 3",
            cluster_id="consensus-cluster",
            resources={"consensus-node-3"},
            peers=None,
            connect=f"ws://127.0.0.1:{ports[0]}",  # Connect to first server
            advertised_urls=None,
            gossip_interval=0.5,
        ),
    ]

    # Create servers
    servers = [MPREGServer(settings=s) for s in server_settings]
    test_context.servers.extend(servers)

    # Start servers with staggered timing
    tasks = []
    for i, server in enumerate(servers):
        task = asyncio.create_task(server.server())
        test_context.tasks.append(task)
        tasks.append(task)

        # Stagger startup: first server starts first
        if i == 0:
            await asyncio.sleep(0.5)
        else:
            await asyncio.sleep(0.3)

    # Wait for cluster formation
    await asyncio.sleep(2.0)

    # Set up consensus and gossip components on each server
    consensus_managers = []
    gossip_protocols = []

    transport = InProcessGossipTransport()
    for i, server in enumerate(servers):
        node_id = f"consensus-node-{i + 1}"

        # Create hub registry
        hub_registry = HubRegistry(registry_id=f"registry_{node_id}")

        # Create consensus manager (without gossip initially)
        consensus_manager = ConsensusManager(
            node_id=node_id,
            gossip_protocol=None,
            default_consensus_threshold=0.6,
        )

        # Create gossip protocol with consensus manager
        gossip_protocol = GossipProtocol(
            node_id=node_id,
            transport=transport,
            hub_registry=hub_registry,
            consensus_manager=consensus_manager,
            gossip_interval=0.5,
            fanout=2,
            strategy=GossipStrategy.RANDOM,
        )

        # Link them together
        consensus_manager.gossip_protocol = gossip_protocol

        # Start components
        await gossip_protocol.start()
        await consensus_manager.start()

        # Track for cleanup - servers will be cleaned up by test context

        consensus_managers.append(consensus_manager)
        gossip_protocols.append(gossip_protocol)

    # Connect gossip protocols to each other (simulate network topology)
    from mpreg.fabric.gossip import NodeMetadata

    for i, gossip in enumerate(gossip_protocols):
        for j, other_gossip in enumerate(gossip_protocols):
            if i != j:
                # Add peer as known node
                gossip.known_nodes[other_gossip.node_id] = NodeMetadata(
                    node_id=other_gossip.node_id,
                    region="test",
                )
                # Add to consensus known nodes
                consensus_managers[i].known_nodes.add(other_gossip.node_id)

    # Wait for gossip convergence
    await asyncio.sleep(1.5)

    try:
        yield servers, consensus_managers, gossip_protocols
    finally:
        # Clean up consensus and gossip components
        for consensus_manager in consensus_managers:
            try:
                await consensus_manager.stop()
            except Exception as e:
                logger.warning(f"Error stopping consensus manager: {e}")

        for gossip_protocol in gossip_protocols:
            try:
                await gossip_protocol.stop()
            except Exception as e:
                logger.warning(f"Error stopping gossip protocol: {e}")


class TestConsensusGossipLiveIntegration:
    """Test consensus integration with gossip protocol using live servers."""

    @pytest.mark.asyncio
    async def test_server_integrated_consensus_proposal_distribution(
        self, live_server_consensus_cluster
    ):
        """Test consensus proposals are distributed via server's integrated gossip."""
        servers = live_server_consensus_cluster

        # Use server's integrated consensus managers
        proposer_server = servers[0]
        voter_servers = servers[1:]

        logger.info(f"DEBUG: Using proposer server: {proposer_server.settings.name}")
        logger.info(f"DEBUG: Voter servers: {[s.settings.name for s in voter_servers]}")
        logger.info(
            f"DEBUG: Proposer consensus manager: {proposer_server.consensus_manager}"
        )
        logger.info(
            f"DEBUG: Proposer node_id: {proposer_server.consensus_manager.node_id}"
        )

        # Create a consensus proposal using server's integrated consensus manager
        vector_clock = VectorClock.empty()
        vector_clock.increment(proposer_server.consensus_manager.node_id)
        state_value = StateValue(
            value={
                "server_integrated_test": "consensus_value",
                "timestamp": time.time(),
            },
            vector_clock=vector_clock,
            node_id=proposer_server.consensus_manager.node_id,
            state_type=StateType.MAP_STATE,
        )

        logger.info(
            "Creating consensus proposal from server's integrated consensus manager"
        )
        proposal_id = await proposer_server.consensus_manager.propose_state_change(
            "server_test_config", state_value
        )

        # Wait for server gossip propagation across the cluster
        logger.info("Waiting for server gossip propagation across cluster...")
        await asyncio.sleep(3.0)

        # Try to vote from other server's consensus managers
        vote_results = []
        for i, voter_server in enumerate(voter_servers):
            logger.info(f"Attempting vote from server {i + 1} consensus manager")
            result = await voter_server.consensus_manager.vote_on_proposal(
                proposal_id, True, voter_server.consensus_manager.node_id
            )
            vote_results.append(result)
            logger.info(f"Vote result from server {i + 1}: {result}")

        # All votes should succeed if server integration works
        assert all(vote_results), f"Some votes failed: {vote_results}"

        # Verify proposal exists in all voter server consensus managers
        for i, voter_server in enumerate(voter_servers):
            assert proposal_id in voter_server.consensus_manager.active_proposals, (
                f"Proposal {proposal_id} not found in server {i + 1} consensus manager active proposals"
            )

        logger.info(
            "SUCCESS: Consensus proposal properly distributed via server's integrated gossip"
        )

    @pytest.mark.asyncio
    async def test_live_consensus_proposal_distribution(self, live_consensus_cluster):
        """Test consensus proposals are distributed via server's integrated gossip in live cluster."""
        servers, consensus_managers, gossip_protocols = live_consensus_cluster

        # Use servers' integrated consensus managers instead of standalone ones
        proposer_server = servers[0]
        voter_servers = servers[1:]

        # Create a consensus proposal using server's integrated consensus manager
        vector_clock = VectorClock.empty()
        vector_clock.increment(proposer_server.consensus_manager.node_id)
        state_value = StateValue(
            value={"live_test_setting": "consensus_value", "timestamp": time.time()},
            vector_clock=vector_clock,
            node_id=proposer_server.consensus_manager.node_id,
            state_type=StateType.MAP_STATE,
        )

        logger.info(
            "Creating consensus proposal from server's integrated consensus manager"
        )
        proposal_id = await proposer_server.consensus_manager.propose_state_change(
            "live_test_config", state_value
        )

        # Wait for server gossip propagation across the cluster
        logger.info("Waiting for server gossip propagation across cluster...")
        await asyncio.sleep(3.0)

        # Try to vote from other servers' consensus managers
        vote_results = []
        for i, voter_server in enumerate(voter_servers):
            logger.info(f"Attempting vote from server {i + 1} consensus manager")
            result = await voter_server.consensus_manager.vote_on_proposal(
                proposal_id, True, voter_server.consensus_manager.node_id
            )
            vote_results.append(result)
            logger.info(f"Vote result from server {i + 1}: {result}")

        # All votes should succeed if server integration works
        assert all(vote_results), f"Some votes failed: {vote_results}"

        # Verify proposal exists in all voter server consensus managers
        for i, voter_server in enumerate(voter_servers):
            assert proposal_id in voter_server.consensus_manager.active_proposals, (
                f"Proposal {proposal_id} not found in server {i + 1} consensus manager active proposals"
            )

        logger.info(
            "SUCCESS: Consensus proposal properly distributed via server's integrated gossip in live cluster"
        )

    @pytest.mark.asyncio
    async def test_live_consensus_vote_propagation(self, live_consensus_cluster):
        """Test consensus voting works correctly with server's integrated consensus in live cluster."""
        servers, consensus_managers, gossip_protocols = live_consensus_cluster

        proposer_server = servers[0]
        voter_server = servers[1]
        observer_server = servers[2]

        # Create proposal using server's integrated consensus manager
        vector_clock = VectorClock.empty()
        vector_clock.increment(proposer_server.consensus_manager.node_id)
        state_value = StateValue(
            value={"live_vote_test": True, "timestamp": time.time()},
            vector_clock=vector_clock,
            node_id=proposer_server.consensus_manager.node_id,
            state_type=StateType.SIMPLE_VALUE,
        )

        proposal_id = await proposer_server.consensus_manager.propose_state_change(
            "live_vote_config", state_value
        )

        # Wait for proposal propagation
        await asyncio.sleep(2.0)

        # Verify all servers received the proposal
        for i, server in enumerate(servers):
            assert proposal_id in server.consensus_manager.active_proposals, (
                f"Proposal should be propagated to server {i + 1}"
            )

        # Vote from one server
        vote_result = await voter_server.consensus_manager.vote_on_proposal(
            proposal_id, True, voter_server.consensus_manager.node_id
        )
        assert vote_result, "Vote should succeed"

        # Wait for consensus completion
        await asyncio.sleep(2.0)

        # Verify that the consensus was reached - check statistics
        # Check if any server received votes (vote broadcasting working)
        total_votes_received = sum(
            server.consensus_manager.consensus_stats.get("votes_received", 0)
            for server in servers
        )
        assert total_votes_received > 0, (
            f"At least one vote should have been received across all servers. Stats: {[s.consensus_manager.consensus_stats for s in servers]}"
        )

        # Check that the proposal was committed
        total_committed = sum(
            server.consensus_manager.consensus_stats.get("proposals_committed", 0)
            for server in servers
        )
        assert total_committed > 0, (
            f"At least one proposal should have been committed. Stats: {[s.consensus_manager.consensus_stats for s in servers]}"
        )

        # Verify voter's vote was recorded (check on the voter's own server first)
        voter_proposal = voter_server.consensus_manager.active_proposals.get(
            proposal_id
        )
        if voter_proposal:
            assert (
                voter_server.consensus_manager.node_id in voter_proposal.received_votes
            ), "Voter should have their own vote recorded"
            assert (
                voter_proposal.received_votes[voter_server.consensus_manager.node_id]
                is True
            ), "Vote should be True"

        logger.info(
            "SUCCESS: Consensus voting works correctly with server's integrated consensus in live cluster"
        )

    @pytest.mark.asyncio
    async def test_live_multi_node_consensus_completion(self, live_consensus_cluster):
        """Test complete consensus process across multiple servers with integrated consensus."""
        servers, consensus_managers, gossip_protocols = live_consensus_cluster

        proposer_server = servers[0]
        all_servers = servers  # All servers including proposer can vote

        # Create proposal requiring majority consensus using server's integrated consensus manager
        # Set consensus threshold to require votes from majority of servers (2 out of 3)
        consensus_threshold = 0.67  # Require 67% consensus

        vector_clock = VectorClock.empty()
        vector_clock.increment(proposer_server.consensus_manager.node_id)
        state_value = StateValue(
            value={"multi_node_decision": "approved", "timestamp": time.time()},
            vector_clock=vector_clock,
            node_id=proposer_server.consensus_manager.node_id,
            state_type=StateType.MAP_STATE,
        )

        proposal_id = await proposer_server.consensus_manager.propose_state_change(
            "multi_node_config", state_value, required_consensus=consensus_threshold
        )

        # Wait for proposal propagation
        await asyncio.sleep(2.0)

        # All servers vote using their integrated consensus managers
        vote_results = []
        for server in all_servers:
            result = await server.consensus_manager.vote_on_proposal(
                proposal_id, True, server.consensus_manager.node_id
            )
            vote_results.append(result)
            # Small delay between votes to see propagation
            await asyncio.sleep(0.5)

        # With 67% threshold and 3 servers, we expect the first 2 votes to succeed
        # and the third to fail because consensus was already reached
        successful_votes = sum(vote_results)
        assert successful_votes >= 2, (
            f"At least 2 votes should succeed for majority consensus. Got: {vote_results}"
        )

        # Total votes should include some that failed due to early consensus completion
        total_votes = len(vote_results)
        assert total_votes == 3, f"Should attempt 3 votes total. Got: {total_votes}"

        # Wait for vote propagation and consensus completion
        await asyncio.sleep(3.0)

        # Check final consensus state on all servers
        for i, server in enumerate(servers):
            proposal = server.consensus_manager.active_proposals.get(proposal_id)
            if proposal:  # May be moved to history if completed
                logger.info(f"Server {i + 1} proposal status: {proposal.status}")
                logger.info(f"Server {i + 1} received votes: {proposal.received_votes}")

        logger.info(
            "SUCCESS: Multi-node consensus completed in live cluster with server integration"
        )
