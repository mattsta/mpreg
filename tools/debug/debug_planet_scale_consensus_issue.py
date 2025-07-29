#!/usr/bin/env python3
"""
Debug Planet Scale Consensus Issue

This script analyzes why the planet scale integration example shows
"Vote received for unknown proposal" warnings. It demonstrates the
difference between standalone gossip protocols and server-integrated consensus.
"""

import asyncio
import time

from loguru import logger

from mpreg.federation.federation_consensus import (
    ConsensusManager,
    StateType,
    StateValue,
)
from mpreg.federation.federation_gossip import (
    GossipProtocol,
    GossipStrategy,
    VectorClock,
)
from mpreg.federation.federation_registry import HubRegistry


async def debug_standalone_gossip_isolation():
    """
    Demonstrate the issue with standalone gossip protocols in planet scale example.

    This shows that standalone gossip protocols are isolated and don't
    actually communicate with each other.
    """
    logger.info("🔍 DEBUG: Analyzing standalone gossip protocol isolation")

    # Create two isolated nodes (similar to planet scale example)
    node1_registry = HubRegistry(registry_id="registry_node1")
    node1_gossip = GossipProtocol(
        node_id="node1",
        hub_registry=node1_registry,
        gossip_interval=0.5,
        fanout=2,
        strategy=GossipStrategy.RANDOM,
    )
    node1_consensus = ConsensusManager(
        node_id="node1",
        gossip_protocol=node1_gossip,
        default_consensus_threshold=0.6,
    )

    node2_registry = HubRegistry(registry_id="registry_node2")
    node2_gossip = GossipProtocol(
        node_id="node2",
        hub_registry=node2_registry,
        gossip_interval=0.5,
        fanout=2,
        strategy=GossipStrategy.RANDOM,
    )
    node2_consensus = ConsensusManager(
        node_id="node2",
        gossip_protocol=node2_gossip,
        default_consensus_threshold=0.6,
    )

    # Start both protocols
    await node1_gossip.start()
    await node1_consensus.start()
    await node2_gossip.start()
    await node2_consensus.start()

    try:
        # Check connectivity - these protocols are isolated!
        logger.info(f"Node1 known_nodes before: {node1_gossip.known_nodes}")
        logger.info(f"Node2 known_nodes before: {node2_gossip.known_nodes}")

        # Create proposal on node1
        vector_clock = VectorClock()
        vector_clock.increment("node1")
        state_value = StateValue(
            value={"test_setting": "isolated_value", "timestamp": time.time()},
            vector_clock=vector_clock,
            node_id="node1",
            state_type=StateType.MAP_STATE,
        )

        logger.info("📋 Creating proposal on node1...")
        proposal_id = await node1_consensus.propose_state_change(
            "test_config", state_value
        )
        logger.info(f"Created proposal: {proposal_id}")

        # Wait for "propagation" (but there's no actual networking!)
        await asyncio.sleep(2.0)

        # Check if node2 received the proposal
        logger.info(
            f"Node1 active_proposals: {list(node1_consensus.active_proposals.keys())}"
        )
        logger.info(
            f"Node2 active_proposals: {list(node2_consensus.active_proposals.keys())}"
        )

        # Try to vote from node2 - this will fail with "unknown proposal"
        logger.info("🗳️ Attempting vote from node2...")
        vote_result = await node2_consensus.vote_on_proposal(proposal_id, True, "node2")
        logger.info(f"Vote result: {vote_result}")

        if not vote_result:
            logger.error("❌ CONFIRMED: Standalone gossip protocols are isolated!")
            logger.error(
                "❌ This is why planet scale example shows 'unknown proposal' warnings"
            )

        # Check final state
        logger.info(f"Node1 known_nodes after: {node1_gossip.known_nodes}")
        logger.info(f"Node2 known_nodes after: {node2_gossip.known_nodes}")

    finally:
        await node1_consensus.stop()
        await node1_gossip.stop()
        await node2_consensus.stop()
        await node2_gossip.stop()


async def demonstrate_solution_approach():
    """
    Demonstrate how server-integrated consensus would solve this.

    This shows the correct approach used in our working tests.
    """
    logger.info("💡 SOLUTION: Server-integrated consensus approach")
    logger.info("✅ In working tests, we use:")
    logger.info("   - MPREG servers with actual networking")
    logger.info("   - Server-integrated consensus managers")
    logger.info("   - Real peer connections for message propagation")
    logger.info("   - Callback-based broadcasting via server infrastructure")

    logger.info("❌ Planet scale example uses:")
    logger.info("   - Standalone gossip protocols with no networking")
    logger.info("   - Isolated consensus managers")
    logger.info("   - No actual message propagation between nodes")
    logger.info("   - Simulated connections that don't actually work")


async def main():
    """Main debug analysis."""
    logger.info("🚀 Starting Planet Scale Consensus Issue Debug")

    await debug_standalone_gossip_isolation()
    await demonstrate_solution_approach()

    logger.info("🔍 CONCLUSION:")
    logger.info("   The planet scale example warnings are EXPECTED BEHAVIOR")
    logger.info("   because standalone gossip protocols are isolated.")
    logger.info("   ")
    logger.info("   To fix this, the example should either:")
    logger.info("   1. Use MPREG servers with real networking (recommended)")
    logger.info("   2. Implement actual networking between gossip protocols")
    logger.info("   3. Document that this is a demo with simulated networking")


if __name__ == "__main__":
    asyncio.run(main())
