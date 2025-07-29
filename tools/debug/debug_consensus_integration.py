#!/usr/bin/env python3
"""
Debug script for consensus integration issues.

This script creates a minimal test case to debug why consensus proposals
are being marked as "inactive" when tests try to vote on them.
"""

import asyncio
import time

from loguru import logger

from mpreg.core.config import MPREGSettings
from mpreg.datastructures.vector_clock import VectorClock
from mpreg.federation.federation_consensus import StateType, StateValue
from mpreg.server import MPREGServer


async def debug_consensus_integration():
    """Debug the consensus integration step by step."""
    logger.info("🔍 Starting consensus integration debug")

    # Create minimal 2-server cluster
    server1_settings = MPREGSettings(
        host="127.0.0.1",
        port=10000,
        name="Debug Server 1",
        cluster_id="debug-cluster",
        resources={"node-1"},
        gossip_interval=0.5,
    )

    server2_settings = MPREGSettings(
        host="127.0.0.1",
        port=10001,
        name="Debug Server 2",
        cluster_id="debug-cluster",
        resources={"node-2"},
        connect="ws://127.0.0.1:10000",
        gossip_interval=0.5,
    )

    # Create servers
    server1 = MPREGServer(settings=server1_settings)
    server2 = MPREGServer(settings=server2_settings)

    # Start servers
    logger.info("🚀 Starting servers")
    task1 = asyncio.create_task(server1.server())
    await asyncio.sleep(0.5)  # Let server1 start first

    task2 = asyncio.create_task(server2.server())
    await asyncio.sleep(1.0)  # Let cluster form

    try:
        # Check consensus managers
        logger.info(f"📊 Server1 consensus manager: {server1.consensus_manager}")
        logger.info(f"📊 Server1 known_nodes: {server1.consensus_manager.known_nodes}")
        logger.info(
            f"📊 Server1 peer_connections: {list(server1.peer_connections.keys())}"
        )

        logger.info(f"📊 Server2 consensus manager: {server2.consensus_manager}")
        logger.info(f"📊 Server2 known_nodes: {server2.consensus_manager.known_nodes}")
        logger.info(
            f"📊 Server2 peer_connections: {list(server2.peer_connections.keys())}"
        )

        # Create a simple proposal
        vector_clock = VectorClock.empty()
        vector_clock.increment(server1.consensus_manager.node_id)
        state_value = StateValue(
            value={"debug_test": "value", "timestamp": time.time()},
            vector_clock=vector_clock,
            node_id=server1.consensus_manager.node_id,
            state_type=StateType.MAP_STATE,
        )

        logger.info("🏛️ Creating consensus proposal...")
        proposal_id = await server1.consensus_manager.propose_state_change(
            "debug_config", state_value
        )
        logger.info(f"📝 Created proposal: {proposal_id}")

        # Check proposal state on both servers
        logger.info(
            f"📋 Server1 active_proposals: {list(server1.consensus_manager.active_proposals.keys())}"
        )
        logger.info(
            f"📋 Server2 active_proposals: {list(server2.consensus_manager.active_proposals.keys())}"
        )

        # Wait for propagation
        logger.info("⏳ Waiting for proposal propagation...")
        await asyncio.sleep(2.0)

        logger.info(
            f"📋 After wait - Server1 active_proposals: {list(server1.consensus_manager.active_proposals.keys())}"
        )
        logger.info(
            f"📋 After wait - Server2 active_proposals: {list(server2.consensus_manager.active_proposals.keys())}"
        )

        # Try to vote from server2
        logger.info("🗳️ Attempting vote from server2...")
        vote_result = await server2.consensus_manager.vote_on_proposal(
            proposal_id, True, server2.consensus_manager.node_id
        )
        logger.info(f"🗳️ Vote result: {vote_result}")

        if not vote_result:
            logger.error("❌ Vote failed! Checking proposal status...")

            # Check if proposal exists in server2
            if proposal_id in server2.consensus_manager.active_proposals:
                proposal = server2.consensus_manager.active_proposals[proposal_id]
                logger.info(f"📊 Proposal status: {proposal.status}")
                logger.info(f"📊 Proposal expired: {proposal.is_expired()}")
                logger.info(f"📊 Proposal votes: {proposal.received_votes}")
                logger.info(f"📊 Proposal deadline: {proposal.consensus_deadline}")
                logger.info(f"📊 Current time: {time.time()}")
            else:
                logger.error(
                    f"❌ Proposal {proposal_id} not found in server2 active_proposals!"
                )

        else:
            logger.info("✅ Vote succeeded!")

    finally:
        logger.info("🛑 Shutting down servers...")
        task1.cancel()
        task2.cancel()

        try:
            await server1.shutdown_async()
            await server2.shutdown_async()
        except Exception as e:
            logger.warning(f"Shutdown error: {e}")


if __name__ == "__main__":
    asyncio.run(debug_consensus_integration())
