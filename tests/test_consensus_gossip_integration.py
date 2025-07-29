"""
Test consensus integration with gossip protocol.

This test demonstrates the issue where consensus proposals are not properly
distributed via gossip, causing votes on unknown proposals.
"""

import asyncio
import time

import pytest
from loguru import logger

from mpreg.datastructures.vector_clock import VectorClock
from mpreg.federation.federation_consensus import (
    ConsensusManager,
    StateType,
    StateValue,
)
from mpreg.federation.federation_gossip import (
    GossipProtocol,
    GossipStrategy,
    NodeMetadata,
)
from mpreg.federation.federation_registry import HubRegistry


class TestConsensusGossipIntegration:
    """Test consensus integration with gossip protocol."""

    @pytest.mark.asyncio
    async def test_consensus_proposal_gossip_integration(self):
        """Test that consensus proposals are distributed via gossip."""
        # Create hub registries
        hub_registry1 = HubRegistry(registry_id="registry_node1")
        hub_registry2 = HubRegistry(registry_id="registry_node2")

        # Create consensus managers first (without gossip)
        consensus1 = ConsensusManager(
            node_id="node1",
            gossip_protocol=None,
            default_consensus_threshold=0.6,
        )

        consensus2 = ConsensusManager(
            node_id="node2",
            gossip_protocol=None,
            default_consensus_threshold=0.6,
        )

        # Create gossip protocols with consensus managers
        gossip1 = GossipProtocol(
            node_id="node1",
            hub_registry=hub_registry1,
            consensus_manager=consensus1,
            gossip_interval=0.5,
            fanout=2,
            strategy=GossipStrategy.RANDOM,
        )

        gossip2 = GossipProtocol(
            node_id="node2",
            hub_registry=hub_registry2,
            consensus_manager=consensus2,
            gossip_interval=0.5,
            fanout=2,
            strategy=GossipStrategy.RANDOM,
        )

        # Set gossip protocols in consensus managers
        consensus1.gossip_protocol = gossip1
        consensus2.gossip_protocol = gossip2

        try:
            # Start components
            await gossip1.start()
            await gossip2.start()
            await consensus1.start()
            await consensus2.start()

            # Connect gossip protocols (simulate network connection)
            gossip1.known_nodes["node2"] = NodeMetadata(
                node_id="node2",
                region="test",
            )
            gossip2.known_nodes["node1"] = NodeMetadata(
                node_id="node1",
                region="test",
            )

            # Add each other to consensus known nodes
            consensus1.known_nodes.add("node2")
            consensus2.known_nodes.add("node1")

            # Create a consensus proposal from node1
            vector_clock = VectorClock.empty()
            vector_clock.increment("node1")
            state_value = StateValue(
                value={"test_setting": "test_value", "timestamp": time.time()},
                vector_clock=vector_clock,
                node_id="node1",
                state_type=StateType.MAP_STATE,
            )

            # Propose from node1
            proposal_id = await consensus1.propose_state_change(
                "test_config", state_value
            )

            # Wait for proposal to be queued in gossip
            await asyncio.sleep(0.5)

            # Debug: check what messages are in the queue
            logger.info(f"Pending messages in gossip1: {len(gossip1.pending_messages)}")
            logger.info(f"Recent messages in gossip1: {len(gossip1.recent_messages)}")

            # Check recent messages for consensus proposals
            recent_proposal_messages = []
            for msg_id, msg in gossip1.recent_messages.items():
                logger.info(
                    f"Recent message: type={msg.message_type.value}, id={msg_id}"
                )
                if msg.message_type.value == "consensus_proposal":
                    recent_proposal_messages.append(msg)

            # Manually propagate the gossip message to simulate network
            # (In real deployment, this would happen automatically via network)
            proposal_messages = recent_proposal_messages

            if proposal_messages:
                proposal_message = proposal_messages[0]
                # Simulate receiving the message on node2
                await gossip2.handle_received_message(proposal_message)
                logger.info(f"Simulated gossip propagation of proposal {proposal_id}")
            else:
                logger.warning("No consensus proposal messages found in gossip queue!")

            # Wait for processing
            await asyncio.sleep(0.5)

            # Try to vote from node2 - this should work if gossip integration is fixed
            vote_result = await consensus2.vote_on_proposal(proposal_id, True, "node2")

            # Assert that the vote was successful
            assert vote_result, (
                f"Vote from node2 failed - proposal {proposal_id} not found"
            )

            # Verify proposal exists in node2's active proposals
            assert proposal_id in consensus2.active_proposals, (
                f"Proposal {proposal_id} not found in node2 active proposals"
            )

            logger.info(
                f"SUCCESS: Consensus proposal {proposal_id} properly distributed via gossip"
            )

        finally:
            # Cleanup
            await consensus1.stop()
            await consensus2.stop()
            await gossip1.stop()
            await gossip2.stop()

    @pytest.mark.asyncio
    async def test_consensus_vote_gossip_integration(self):
        """Test that consensus votes are distributed via gossip."""
        # Create minimal setup
        hub_registry1 = HubRegistry(registry_id="registry_node1")
        hub_registry2 = HubRegistry(registry_id="registry_node2")

        # Create consensus managers first
        consensus1 = ConsensusManager(
            node_id="node1",
            gossip_protocol=None,
            default_consensus_threshold=0.6,
        )

        consensus2 = ConsensusManager(
            node_id="node2",
            gossip_protocol=None,
            default_consensus_threshold=0.6,
        )

        # Create gossip protocols with consensus managers
        gossip1 = GossipProtocol(
            node_id="node1",
            hub_registry=hub_registry1,
            consensus_manager=consensus1,
            gossip_interval=0.5,
            fanout=2,
            strategy=GossipStrategy.RANDOM,
        )

        gossip2 = GossipProtocol(
            node_id="node2",
            hub_registry=hub_registry2,
            consensus_manager=consensus2,
            gossip_interval=0.5,
            fanout=2,
            strategy=GossipStrategy.RANDOM,
        )

        # Set gossip protocols in consensus managers
        consensus1.gossip_protocol = gossip1
        consensus2.gossip_protocol = gossip2

        try:
            # Start components
            await gossip1.start()
            await gossip2.start()
            await consensus1.start()
            await consensus2.start()

            # Connect nodes
            gossip1.known_nodes["node2"] = NodeMetadata(
                node_id="node2",
                region="test",
            )
            gossip2.known_nodes["node1"] = NodeMetadata(
                node_id="node1",
                region="test",
            )

            consensus1.known_nodes.add("node2")
            consensus2.known_nodes.add("node1")

            # Create proposal
            vector_clock = VectorClock.empty()
            vector_clock.increment("node1")
            state_value = StateValue(
                value={"vote_test": True},
                vector_clock=vector_clock,
                node_id="node1",
                state_type=StateType.SIMPLE_VALUE,
            )

            proposal_id = await consensus1.propose_state_change(
                "vote_test_config", state_value
            )

            # Wait for proposal propagation
            await asyncio.sleep(1.5)

            # Vote from node2
            vote_result = await consensus2.vote_on_proposal(proposal_id, True, "node2")
            assert vote_result, "Vote should succeed"

            # Wait for vote propagation
            await asyncio.sleep(1.5)

            # Check that node1 sees the vote from node2
            proposal = consensus1.active_proposals.get(proposal_id)
            assert proposal is not None, "Proposal should exist in node1"
            assert "node2" in proposal.received_votes, (
                "Vote from node2 should be recorded in node1"
            )
            assert proposal.received_votes["node2"] is True, "Vote should be True"

            logger.info("SUCCESS: Consensus vote properly propagated via gossip")

        finally:
            # Cleanup
            await consensus1.stop()
            await consensus2.stop()
            await gossip1.stop()
            await gossip2.stop()
