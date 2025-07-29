"""
DEEP DIVE DEBUGGING: Vote Request Network Analysis

This script performs in-depth analysis of why vote requests aren't reaching other nodes
or vote responses aren't being properly processed.
"""

import asyncio
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.append(".")

from mpreg.datastructures.production_raft import RequestVoteRequest, RequestVoteResponse
from tests.test_production_raft_integration import (
    MockNetwork,
    NetworkAwareTransport,
    TestProductionRaftIntegration,
)


@dataclass
class NetworkDebugMessage:
    """Track network messages for debugging."""

    source: str
    target: str
    message_type: str
    payload: Any
    timestamp: float
    processed: bool = False
    error: str = ""


@dataclass
class VoteFlowTracker:
    """Track vote request/response flow for debugging."""

    vote_requests_sent: list[NetworkDebugMessage] = field(default_factory=list)
    vote_responses_received: list[NetworkDebugMessage] = field(default_factory=list)
    transport_errors: list[str] = field(default_factory=list)
    rpc_call_log: list[tuple[str, str, float]] = field(default_factory=list)


class DeepDebugTransport(NetworkAwareTransport):
    """Enhanced transport with detailed debugging."""

    def __init__(self, node_id: str, network: MockNetwork, tracker: VoteFlowTracker):
        super().__init__(node_id, network)
        self.tracker = tracker

    async def send_request_vote(
        self, target: str, request: RequestVoteRequest
    ) -> RequestVoteResponse | None:
        """Send RequestVote RPC with comprehensive debugging."""
        start_time = time.time()

        # Log the attempt
        debug_msg = NetworkDebugMessage(
            source=self.node_id,
            target=target,
            message_type="request_vote",
            payload=request,
            timestamp=start_time,
        )
        self.tracker.vote_requests_sent.append(debug_msg)
        self.tracker.rpc_call_log.append(
            ("send_request_vote", f"{self.node_id}->{target}", start_time)
        )

        print(f"🔍 DEBUG: {self.node_id} sending RequestVote to {target}")
        print(f"   Request: term={request.term}, candidate={request.candidate_id}")

        # Check network connectivity first
        if not self.network.can_communicate(self.node_id, target):
            error = f"Network partition: {self.node_id} cannot reach {target}"
            print(f"❌ NETWORK ERROR: {error}")
            debug_msg.error = error
            self.tracker.transport_errors.append(error)
            return None

        # Log to network tracking
        self.network.sent_messages.append(
            ("request_vote", self.node_id, target, request)
        )

        # Apply message delay
        if self.network.message_delay > 0:
            print(f"⏱️  Network delay: {self.network.message_delay}s")
            await asyncio.sleep(self.network.message_delay)

        # Find target node
        target_node = self.network.nodes.get(target)
        if not target_node:
            error = f"Target node {target} not found in network"
            print(f"❌ NODE ERROR: {error}")
            debug_msg.error = error
            self.tracker.transport_errors.append(error)
            return None

        print(f"✅ Found target node {target}, calling handle_request_vote...")

        try:
            # Call the RPC handler
            response = await target_node.handle_request_vote(request)
            debug_msg.processed = True

            if response:
                # Log the response
                response_msg = NetworkDebugMessage(
                    source=target,
                    target=self.node_id,
                    message_type="request_vote_response",
                    payload=response,
                    timestamp=time.time(),
                )
                self.tracker.vote_responses_received.append(response_msg)

                print(
                    f"📨 RESPONSE from {target}: vote_granted={response.vote_granted}, term={response.term}"
                )
                if response.vote_granted:
                    print(f"✅ VOTE GRANTED by {target}")
                else:
                    print(f"❌ VOTE DENIED by {target}")
            else:
                print(f"⚠️  NULL RESPONSE from {target}")

            return response

        except Exception as e:
            error = f"RPC call failed: {e}"
            print(f"💥 RPC ERROR: {error}")
            debug_msg.error = error
            self.tracker.transport_errors.append(error)
            import traceback

            print(f"Traceback: {traceback.format_exc()}")
            return None


async def deep_debug_vote_network():
    """Perform deep debugging of vote request network flow."""
    print("🚀 STARTING DEEP DIVE VOTE NETWORK DEBUG SESSION")
    print("=" * 60)

    tracker = VoteFlowTracker()

    test_instance = TestProductionRaftIntegration()

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_dir = Path(tmpdir)
        network = MockNetwork()

        # Create cluster with debug transport
        cluster_members = {"node_0", "node_1", "node_2"}
        nodes = {}

        for node_id in cluster_members:
            from mpreg.datastructures.production_raft_implementation import (
                ProductionRaft,
                RaftConfiguration,
            )
            from mpreg.datastructures.raft_storage_adapters import RaftStorageFactory
            from tests.test_production_raft_integration import TestableStateMachine

            storage = RaftStorageFactory.create_memory_storage(f"debug_{node_id}")
            transport = DeepDebugTransport(node_id, network, tracker)
            state_machine = TestableStateMachine()

            config = RaftConfiguration(
                election_timeout_min=0.15,
                election_timeout_max=0.25,
                heartbeat_interval=0.025,
                rpc_timeout=0.08,
            )

            node = ProductionRaft(
                node_id=node_id,
                cluster_members=cluster_members,
                storage=storage,
                transport=transport,
                state_machine=state_machine,
                config=config,
            )

            nodes[node_id] = node
            network.register_node(node_id, node)
            print(f"🏗️  Created debug node {node_id}")

        try:
            print("\n📡 STARTING NODES...")
            for node in nodes.values():
                await node.start()
                print(f"✅ Started {node.node_id}")

            print("\n🗳️  MONITORING ELECTION PROCESS...")

            # Monitor for a few seconds
            for round_num in range(20):  # 2 seconds total
                await asyncio.sleep(0.1)

                leaders = [
                    n for n in nodes.values() if n.current_state.value == "leader"
                ]
                candidates = [
                    n for n in nodes.values() if n.current_state.value == "candidate"
                ]
                followers = [
                    n for n in nodes.values() if n.current_state.value == "follower"
                ]

                print(
                    f"\n📊 Round {round_num}: L={len(leaders)} C={len(candidates)} F={len(followers)}"
                )

                # Show node states
                for node_id, node in nodes.items():
                    votes = (
                        len(node.votes_received)
                        if hasattr(node, "votes_received")
                        else 0
                    )
                    print(
                        f"   {node_id}: {node.current_state.value} (term={node.persistent_state.current_term}, votes={votes})"
                    )

                # Show network activity this round
                recent_requests = [
                    msg
                    for msg in tracker.vote_requests_sent
                    if msg.timestamp > time.time() - 0.15
                ]
                recent_responses = [
                    msg
                    for msg in tracker.vote_responses_received
                    if msg.timestamp > time.time() - 0.15
                ]

                if recent_requests:
                    print(f"   📤 Recent vote requests: {len(recent_requests)}")
                    for msg in recent_requests[-3:]:  # Show last 3
                        status = "✅" if msg.processed else "❌"
                        print(f"      {status} {msg.source}->{msg.target}")

                if recent_responses:
                    print(f"   📥 Recent vote responses: {len(recent_responses)}")
                    for msg in recent_responses[-3:]:  # Show last 3
                        granted = msg.payload.vote_granted if msg.payload else False
                        print(
                            f"      {'✅' if granted else '❌'} {msg.source}->{msg.target} (granted={granted})"
                        )

                if leaders:
                    print(f"🎉 LEADER ELECTED: {leaders[0].node_id}")
                    break

            print("\n📈 FINAL NETWORK ANALYSIS:")
            print(f"Total vote requests sent: {len(tracker.vote_requests_sent)}")
            print(
                f"Total vote responses received: {len(tracker.vote_responses_received)}"
            )
            print(f"Transport errors: {len(tracker.transport_errors)}")

            # Detailed analysis
            print("\n🔍 DETAILED VOTE REQUEST ANALYSIS:")
            for msg in tracker.vote_requests_sent:
                status = "PROCESSED" if msg.processed else "FAILED"
                error_info = f" (ERROR: {msg.error})" if msg.error else ""
                print(f"   {msg.source} -> {msg.target}: {status}{error_info}")

            print("\n🔍 DETAILED VOTE RESPONSE ANALYSIS:")
            for msg in tracker.vote_responses_received:
                if msg.payload:
                    print(
                        f"   {msg.source} -> {msg.target}: granted={msg.payload.vote_granted}, term={msg.payload.term}"
                    )

            if tracker.transport_errors:
                print(f"\n💥 TRANSPORT ERRORS ({len(tracker.transport_errors)}):")
                for error in tracker.transport_errors:
                    print(f"   - {error}")

            # Network state analysis
            print("\n🌐 NETWORK STATE:")
            print(f"   Registered nodes: {list(network.nodes.keys())}")
            print(f"   Network partitions: {len(network.partitions)}")
            print(f"   Total messages sent: {len(network.sent_messages)}")

        finally:
            print("\n🛑 STOPPING NODES...")
            for node in nodes.values():
                await node.stop()


if __name__ == "__main__":
    asyncio.run(deep_debug_vote_network())
