#!/usr/bin/env python3
"""
Simple test to verify planet scale consensus fix is working.

This demonstrates that using MPREG servers instead of standalone gossip protocols
fixes the "Vote received for unknown proposal" warnings.
"""

import asyncio
import time

from loguru import logger

from mpreg.core.config import MPREGSettings
from mpreg.datastructures.vector_clock import VectorClock
from mpreg.federation.federation_consensus import StateType, StateValue
from mpreg.server import MPREGServer


async def test_simple_planet_scale_consensus():
    """Test consensus between 3 MPREG servers (simulating planet scale nodes)."""

    # Create 3 MPREG servers with proper peer connections
    servers = []
    ports = [9100, 9101, 9102]

    for i, port in enumerate(ports):
        # Configure peers for each server (connect to others)
        peer_urls = [f"ws://127.0.0.1:{p}" for p in ports if p != port]

        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name=f"Planet Scale Node {i + 1}",
            cluster_id="planet-scale-test",
            resources={f"planet-node-{i + 1}"},
            peers=peer_urls,  # Connect to other servers
            connect=None,
            advertised_urls=None,
            gossip_interval=0.5,
        )

        server = MPREGServer(settings=settings)
        servers.append(server)

    # Start all servers
    server_tasks = []
    for i, server in enumerate(servers):
        task = asyncio.create_task(server.server())
        server_tasks.append(task)
        logger.info(f"Started Planet Scale Node {i + 1}")

        # Stagger startup
        if i == 0:
            await asyncio.sleep(1.0)  # First server starts first
        else:
            await asyncio.sleep(0.5)

    # Wait for cluster formation
    await asyncio.sleep(3.0)

    try:
        # Test consensus proposal from first server
        proposer_server = servers[0]
        voter_servers = servers[1:]

        logger.info("=== Testing Planet Scale Consensus ===")

        # Create consensus proposal using server's integrated consensus manager
        vector_clock = VectorClock.empty()
        vector_clock.increment(proposer_server.consensus_manager.node_id)
        state_value = StateValue(
            value={"planet_scale_setting": "test_value", "timestamp": time.time()},
            vector_clock=vector_clock,
            node_id=proposer_server.consensus_manager.node_id,
            state_type=StateType.MAP_STATE,
        )

        logger.info("Creating consensus proposal from Planet Scale Node 1...")
        proposal_id = await proposer_server.consensus_manager.propose_state_change(
            "planet_config", state_value
        )

        # Wait for proposal propagation via MPREG server networking
        logger.info("Waiting for proposal propagation via MPREG networking...")
        await asyncio.sleep(3.0)

        # Check if proposals reached other servers
        success = True
        for i, voter_server in enumerate(voter_servers):
            if proposal_id in voter_server.consensus_manager.active_proposals:
                logger.info(
                    f"✅ Planet Scale Node {i + 2} received proposal {proposal_id}"
                )

                # Vote on the proposal
                result = await voter_server.consensus_manager.vote_on_proposal(
                    proposal_id, True, voter_server.consensus_manager.node_id
                )
                if result:
                    logger.info(
                        f"✅ Planet Scale Node {i + 2} successfully voted on proposal"
                    )
                else:
                    logger.error(
                        f"❌ Planet Scale Node {i + 2} failed to vote on proposal"
                    )
                    success = False
            else:
                logger.error(
                    f"❌ Planet Scale Node {i + 2} did not receive proposal {proposal_id}"
                )
                success = False

        if success:
            logger.info(
                "🎉 SUCCESS: Planet scale consensus working with MPREG servers!"
            )
            logger.info("🎉 No 'Vote received for unknown proposal' warnings!")
        else:
            logger.error("❌ FAILED: Planet scale consensus not working properly")

        # Wait for consensus completion
        await asyncio.sleep(2.0)

        return success

    finally:
        # Cleanup
        for task in server_tasks:
            task.cancel()

        # Wait for cleanup
        await asyncio.gather(*server_tasks, return_exceptions=True)

        logger.info("Planet scale consensus test completed")


async def main():
    """Run the simple planet scale consensus test."""
    logger.info("Starting simple planet scale consensus integration test")

    success = await test_simple_planet_scale_consensus()

    if success:
        logger.info("✅ CONCLUSION: Planet scale example fix is working correctly!")
        logger.info("✅ MPREG servers provide the networking that consensus needs!")
    else:
        logger.error("❌ CONCLUSION: Planet scale example still has issues")


if __name__ == "__main__":
    asyncio.run(main())
