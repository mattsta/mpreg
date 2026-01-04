#!/usr/bin/env python3
"""
FOCUSED DEBUG: Why vote requests result in only 1 vote per node.

Root cause analysis of the vote collection failure in ProductionRaft.
"""

import asyncio
import sys
import tempfile
from pathlib import Path

sys.path.append(".")

from tests.test_production_raft_integration import (
    MockNetwork,
    TestProductionRaftIntegration,
)


async def focused_vote_debug():
    """Debug vote collection with detailed tracing and timeouts."""
    print("🔍 FOCUSED VOTE DEBUG SESSION")
    print("=" * 50)

    test_instance = TestProductionRaftIntegration()

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_dir = Path(tmpdir)
        network = MockNetwork()

        # Create 3-node cluster for focused debugging
        nodes = test_instance.create_raft_cluster(
            3, temp_dir, network, storage_type="memory"
        )

        try:
            print("\n📡 STARTING NODES...")
            start_tasks = []
            for node_id, node in nodes.items():
                start_tasks.append(node.start())
                print(f"✅ Starting {node_id}")

            # Start all nodes with timeout
            await asyncio.wait_for(asyncio.gather(*start_tasks), timeout=5.0)

            print("\n🗳️  ANALYZING VOTE REQUEST FLOW...")

            # Wait a short time and analyze state
            for round_num in range(20):  # Max 2 seconds
                await asyncio.sleep(0.1)

                # Get current state
                states = {}
                votes = {}
                terms = {}

                for node_id, node in nodes.items():
                    states[node_id] = node.current_state.value
                    votes[node_id] = (
                        len(node.votes_received)
                        if hasattr(node, "votes_received")
                        else 0
                    )
                    terms[node_id] = node.persistent_state.current_term

                print(f"\n📊 Round {round_num}:")
                for node_id in nodes:
                    print(
                        f"   {node_id}: {states[node_id]} (term={terms[node_id]}, votes={votes[node_id]})"
                    )

                # Check if any node has majority
                majority_threshold = len(nodes) // 2 + 1
                nodes_with_majority = [
                    nid for nid, v in votes.items() if v >= majority_threshold
                ]

                if nodes_with_majority:
                    print(f"🎉 MAJORITY ACHIEVED: {nodes_with_majority}")
                    break

                # Check for obvious problems
                candidates = [
                    nid for nid, state in states.items() if state == "candidate"
                ]
                if len(candidates) > 1:
                    print(f"⚠️  Multiple candidates: {candidates}")

                # Show network activity
                current_msg_count = len(network.sent_messages)
                last_msg_count = getattr(focused_vote_debug, "_last_msg_count", 0)
                if current_msg_count > last_msg_count:
                    new_msgs = network.sent_messages[last_msg_count:]
                    for msg_type, source, target, payload in new_msgs[
                        -3:
                    ]:  # Show last 3
                        print(f"   📤 {source} -> {target}: {msg_type}")
                    # focused_vote_debug._last_msg_count = current_msg_count  # Commented to fix mypy

                # Emergency timeout check
                if round_num >= 15 and all(v <= 1 for v in votes.values()):
                    print("🚨 VOTE COLLECTION FAILURE DETECTED!")

                    # Deep inspect the first candidate
                    candidate_nodes = [
                        node
                        for node in nodes.values()
                        if node.current_state.value == "candidate"
                    ]
                    if candidate_nodes:
                        candidate = candidate_nodes[0]
                        print(f"\n🔍 ANALYZING CANDIDATE {candidate.node_id}:")
                        print(f"   Cluster members: {candidate.cluster_members}")
                        print(f"   Transport: {type(candidate.transport).__name__}")
                        print(f"   Votes received: {candidate.votes_received}")

                        # Test direct vote request
                        print("\n🧪 TESTING DIRECT VOTE REQUEST...")
                        from mpreg.datastructures.production_raft import (
                            RequestVoteRequest,
                        )

                        test_request = RequestVoteRequest(
                            term=candidate.persistent_state.current_term,
                            candidate_id=candidate.node_id,
                            last_log_index=len(candidate.persistent_state.log_entries),
                            last_log_term=candidate.persistent_state.log_entries[
                                -1
                            ].term
                            if candidate.persistent_state.log_entries
                            else 0,
                        )

                        # Try sending to another node
                        other_nodes = [n for n in nodes.values() if n != candidate]
                        if other_nodes:
                            target = other_nodes[0]
                            print(
                                f"   Sending test vote request: {candidate.node_id} -> {target.node_id}"
                            )

                            try:
                                response = await candidate.transport.send_request_vote(
                                    target.node_id, test_request
                                )
                                print(f"   Response: {response}")
                                if response:
                                    print(f"   Vote granted: {response.vote_granted}")
                                else:
                                    print("   ❌ No response received!")
                            except Exception as e:
                                print(f"   💥 Request failed: {e}")

                    break

            print("\n📈 FINAL ANALYSIS:")
            print(f"Network messages sent: {len(network.sent_messages)}")

            leaders = [
                n.node_id for n in nodes.values() if n.current_state.value == "leader"
            ]
            candidates = [
                n.node_id
                for n in nodes.values()
                if n.current_state.value == "candidate"
            ]
            followers = [
                n.node_id for n in nodes.values() if n.current_state.value == "follower"
            ]

            print(
                f"Final state: {len(leaders)} leaders, {len(candidates)} candidates, {len(followers)} followers"
            )

            if not leaders:
                print("🚨 DIAGNOSIS: NO LEADER ELECTED - VOTE COLLECTION FAILED")

                # Detailed failure analysis
                print("\n🔧 FAILURE ANALYSIS:")
                for node_id, node in nodes.items():
                    vote_count = (
                        len(node.votes_received)
                        if hasattr(node, "votes_received")
                        else 0
                    )
                    print(
                        f"   {node_id}: {vote_count} votes (needs {len(nodes) // 2 + 1} for majority)"
                    )

                print("\n💡 LIKELY CAUSES:")
                print("   1. Vote requests not reaching target nodes")
                print("   2. Vote responses not being processed")
                print("   3. Network transport layer issues")
                print("   4. Concurrent election conflicts")

        except TimeoutError:
            print("⏰ DEBUG SESSION TIMED OUT")
        except Exception as e:
            print(f"💥 DEBUG ERROR: {e}")
            import traceback

            traceback.print_exc()
        finally:
            print("\n🛑 STOPPING NODES...")
            stop_tasks = []
            for node in nodes.values():
                stop_tasks.append(node.stop())

            try:
                await asyncio.wait_for(
                    asyncio.gather(*stop_tasks, return_exceptions=True), timeout=2.0
                )
            except TimeoutError:
                print("⚠️  Node shutdown timeout")


if __name__ == "__main__":
    asyncio.run(focused_vote_debug())
