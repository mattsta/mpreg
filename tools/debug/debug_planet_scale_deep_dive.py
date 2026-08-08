#!/usr/bin/env python3
"""
Deep dive debug of planet scale consensus issue.

This script will systematically analyze:
1. Are MPREG servers actually connecting to each other?
2. Are consensus proposals being broadcast correctly?
3. Are vote messages reaching the right destinations?
4. What's the exact flow of consensus messages?
"""

import asyncio
import time

from loguru import logger

from mpreg.core.config import MPREGSettings
from mpreg.datastructures.vector_clock import VectorClock
from mpreg.fabric.consensus import StateType, StateValue
from mpreg.server import MPREGServer


async def debug_planet_scale_consensus_deep_dive():
    """Deep dive debugging of planet scale consensus."""

    logger.info("🔍 DEEP DIVE DEBUG: Planet Scale Consensus Issue")

    # Create 3 MPREG servers with careful peer configuration
    servers = []
    ports = [9200, 9201, 9202]

    logger.info("=== PHASE 1: Creating MPREG Servers ===")

    for i, port in enumerate(ports):
        # Configure peers for proper connectivity
        peer_urls = [f"ws://127.0.0.1:{p}" for p in ports if p != port]

        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name=f"Debug Node {i + 1}",
            cluster_id="debug-consensus",
            resources={f"debug-node-{i + 1}"},
            peers=peer_urls,
            connect=None,
            advertised_urls=None,
            gossip_interval=0.5,
        )

        server = MPREGServer(settings=settings)
        servers.append(server)

        logger.info(f"Created Debug Node {i + 1} on port {port}")
        logger.info(f"  - Node ID: {server.consensus_manager.node_id}")
        logger.info(f"  - Peer URLs: {peer_urls}")

    # Start servers with detailed monitoring
    logger.info("=== PHASE 2: Starting MPREG Servers ===")

    server_tasks = []
    for i, server in enumerate(servers):
        logger.info(f"Starting Debug Node {i + 1}...")
        task = asyncio.create_task(server.server())
        server_tasks.append(task)

        # Stagger startup to allow proper connection establishment
        if i == 0:
            await asyncio.sleep(2.0)  # First server waits longer
        else:
            await asyncio.sleep(1.0)

    # Wait for cluster formation and debug connectivity
    logger.info("=== PHASE 3: Debugging Connectivity ===")
    await asyncio.sleep(5.0)

    try:
        # Debug connectivity between servers
        for i, server in enumerate(servers):
            logger.info(f"Debug Node {i + 1} connectivity:")
            logger.info(f"  - Peer connections: {len(server.peer_connections)}")
            logger.info(
                f"  - Peer connection URLs: {list(server.peer_connections.keys())}"
            )
            logger.info(
                f"  - Consensus known nodes: {len(server.consensus_manager.known_nodes)}"
            )
            logger.info(
                f"  - Consensus known node IDs: {list(server.consensus_manager.known_nodes)}"
            )

        # Test consensus proposal
        logger.info("=== PHASE 4: Testing Consensus Proposal ===")

        proposer_server = servers[0]
        logger.info(
            f"Using proposer: Debug Node 1 (ID: {proposer_server.consensus_manager.node_id})"
        )

        # Create state value
        vector_clock = VectorClock.empty()
        vector_clock.increment(proposer_server.consensus_manager.node_id)
        state_value = StateValue(
            value={"debug_setting": "test_consensus", "timestamp": time.time()},
            vector_clock=vector_clock,
            node_id=proposer_server.consensus_manager.node_id,
            state_type=StateType.MAP_STATE,
        )

        logger.info("Creating consensus proposal...")
        proposal_id = await proposer_server.consensus_manager.propose_state_change(
            "debug_config", state_value
        )
        logger.info(f"Created proposal: {proposal_id}")

        # Debug proposal state immediately after creation
        logger.info("=== PHASE 5: Debugging Proposal State ===")
        await asyncio.sleep(1.0)  # Brief wait

        for i, server in enumerate(servers):
            active_proposals = list(server.consensus_manager.active_proposals.keys())
            logger.info(f"Debug Node {i + 1} active proposals: {active_proposals}")

            if proposal_id in server.consensus_manager.active_proposals:
                proposal = server.consensus_manager.active_proposals[proposal_id]
                logger.info(f"  - Proposal {proposal_id} found!")
                logger.info(f"  - Status: {proposal.status}")
                logger.info(f"  - Required votes: {proposal.required_votes}")
                logger.info(f"  - Received votes: {proposal.received_votes}")
            else:
                logger.warning(
                    f"  - Proposal {proposal_id} NOT found in Debug Node {i + 1}"
                )

        # Wait longer for propagation
        logger.info("=== PHASE 6: Waiting for Proposal Propagation ===")
        await asyncio.sleep(5.0)

        # Check again after longer wait
        for i, server in enumerate(servers):
            active_proposals = list(server.consensus_manager.active_proposals.keys())
            logger.info(
                f"Debug Node {i + 1} active proposals after wait: {active_proposals}"
            )

        # Test voting
        logger.info("=== PHASE 7: Testing Voting ===")

        voter_servers = servers[1:]
        for i, voter_server in enumerate(voter_servers):
            logger.info(f"Testing vote from Debug Node {i + 2}...")

            result = await voter_server.consensus_manager.vote_on_proposal(
                proposal_id, True, voter_server.consensus_manager.node_id
            )

            logger.info(f"Vote result from Debug Node {i + 2}: {result}")

            if not result:
                logger.error(
                    f"❌ Vote failed from Debug Node {i + 2} - this will trigger 'unknown proposal' warning"
                )
            else:
                logger.info(f"✅ Vote succeeded from Debug Node {i + 2}")

        # Final state analysis
        logger.info("=== PHASE 8: Final State Analysis ===")
        await asyncio.sleep(2.0)

        for i, server in enumerate(servers):
            stats = server.consensus_manager.consensus_stats
            logger.info(f"Debug Node {i + 1} consensus stats:")
            logger.info(f"  - Proposals created: {stats.get('proposals_created', 0)}")
            logger.info(
                f"  - Proposals evaluated: {stats.get('proposals_evaluated', 0)}"
            )
            logger.info(f"  - Votes received: {stats.get('votes_received', 0)}")
            logger.info(f"  - Votes processed: {stats.get('votes_processed', 0)}")
            logger.info(
                f"  - Proposals committed: {stats.get('proposals_committed', 0)}"
            )

        # Diagnosis
        logger.info("=== PHASE 9: Diagnosis ===")

        # Check if servers are actually connected
        total_peer_connections = sum(len(server.peer_connections) for server in servers)
        if total_peer_connections == 0:
            logger.error("❌ DIAGNOSIS: No peer connections established!")
            logger.error(
                "❌ Root cause: MPREG servers are not connecting to each other"
            )
        else:
            logger.info(f"✅ Total peer connections: {total_peer_connections}")

        # Check if proposals propagated
        proposer_has_proposal = (
            proposal_id in servers[0].consensus_manager.active_proposals
        )
        voters_have_proposal = all(
            proposal_id in server.consensus_manager.active_proposals
            for server in servers[1:]
        )

        if proposer_has_proposal and not voters_have_proposal:
            logger.error("❌ DIAGNOSIS: Consensus proposals not propagating!")
            logger.error("❌ Root cause: Consensus broadcast is not working")
        elif proposer_has_proposal and voters_have_proposal:
            logger.info("✅ DIAGNOSIS: Consensus proposals are propagating correctly!")
        else:
            logger.error("❌ DIAGNOSIS: Consensus proposal creation failed!")

        return total_peer_connections > 0 and voters_have_proposal

    finally:
        # Cleanup
        logger.info("=== CLEANUP ===")
        for task in server_tasks:
            task.cancel()

        await asyncio.gather(*server_tasks, return_exceptions=True)


async def main():
    """Run deep dive debugging."""
    logger.info("Starting deep dive debug of planet scale consensus")

    success = await debug_planet_scale_consensus_deep_dive()

    logger.info("=== FINAL CONCLUSION ===")
    if success:
        logger.info("✅ Planet scale consensus is working correctly")
    else:
        logger.error("❌ Planet scale consensus has fundamental issues")


if __name__ == "__main__":
    asyncio.run(main())
