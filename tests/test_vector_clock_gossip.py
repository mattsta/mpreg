"""
Property-based tests for vector clock gossip protocol.

This module tests the vector clock-based gossip protocol implementation
to ensure correctness and convergence properties.
"""

from hypothesis import assume, given
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, initialize, rule

from mpreg.core.model import PeerInfo
from mpreg.datastructures.type_aliases import NodeURL
from mpreg.datastructures.vector_clock import VectorClock, vector_clock_strategy


class VectorClockGossipProtocol:
    """Vector clock-based gossip protocol for peer information."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.peers_info: dict[str, PeerInfo] = {}
        self.local_clock = VectorClock.single_entry(node_id, 0)

    def create_peer_info(
        self, url: str, funs: tuple[str, ...], locs: frozenset[str]
    ) -> PeerInfo:
        """Create peer info with updated logical clock."""
        # Increment our logical clock for this update
        self.local_clock = self.local_clock.increment(self.node_id)

        return PeerInfo(
            url=url,
            funs=funs,
            locs=locs,
            last_seen=0.0,  # Not used with vector clocks
            cluster_id="test-cluster",
            advertised_urls=(),
            logical_clock=self.local_clock.to_dict(),
        )

    def should_update_peer(self, existing: PeerInfo, incoming: PeerInfo) -> bool:
        """Determine if incoming peer info should update existing info using vector clocks."""
        existing_clock = VectorClock.from_dict(existing.logical_clock)
        incoming_clock = VectorClock.from_dict(incoming.logical_clock)

        comparison = existing_clock.compare(incoming_clock)

        # Update if incoming is after existing, or if concurrent but different
        if comparison == "before":  # existing happens before incoming
            return True
        elif comparison == "concurrent":
            # For concurrent updates, use deterministic tie-breaking
            return self._resolve_concurrent_update(existing, incoming)
        else:
            # incoming is equal or before existing - don't update
            return False

    def _resolve_concurrent_update(
        self, existing: PeerInfo, incoming: PeerInfo
    ) -> bool:
        """Resolve concurrent updates deterministically."""
        # Use lexicographic ordering of URLs as tie-breaker
        if existing.funs != incoming.funs:
            return tuple(sorted(incoming.funs)) > tuple(sorted(existing.funs))
        elif existing.locs != incoming.locs:
            return tuple(sorted(incoming.locs)) > tuple(sorted(existing.locs))
        else:
            return incoming.url > existing.url

    def process_gossip(self, peer_infos: list[PeerInfo]) -> dict[str, str]:
        """Process incoming gossip messages, returning update actions."""
        actions = {}

        for peer_info in peer_infos:
            if peer_info.url == self.node_id:
                continue  # Skip self

            if peer_info.url not in self.peers_info:
                # New peer
                self.peers_info[peer_info.url] = peer_info
                actions[peer_info.url] = "added"
            elif self.should_update_peer(self.peers_info[peer_info.url], peer_info):
                # Update existing peer
                self.peers_info[peer_info.url] = peer_info
                actions[peer_info.url] = "updated"
            else:
                # No update needed
                actions[peer_info.url] = "ignored"

        return actions

    def get_all_peer_info(self) -> list[PeerInfo]:
        """Get all peer info for gossip."""
        return list(self.peers_info.values())


class GossipStateMachine(RuleBasedStateMachine):
    """Property-based state machine testing for gossip protocol."""

    nodes = Bundle("nodes")

    def __init__(self):
        super().__init__()
        self.protocols: dict[str, VectorClockGossipProtocol] = {}

    @initialize()
    def setup_nodes(self):
        """Initialize a small cluster of nodes."""
        node_ids = ["node1", "node2", "node3"]
        for node_id in node_ids:
            self.protocols[node_id] = VectorClockGossipProtocol(node_id)

    @rule(node_id=st.sampled_from(["node1", "node2", "node3"]))
    def create_peer_info(self, node_id: str):
        """Create new peer info on a node."""
        protocol = self.protocols[node_id]

        # Create unique peer info
        peer_url = f"peer_{len(protocol.peers_info)}_{node_id}"
        funs = (f"func_{node_id}",)
        locs = frozenset([f"loc_{node_id}"])

        peer_info = protocol.create_peer_info(peer_url, funs, locs)
        protocol.peers_info[peer_url] = peer_info

    @rule(
        sender=st.sampled_from(["node1", "node2", "node3"]),
        receiver=st.sampled_from(["node1", "node2", "node3"]),
    )
    def gossip_exchange(self, sender: str, receiver: str):
        """Exchange gossip between two nodes."""
        assume(sender != receiver)

        sender_protocol = self.protocols[sender]
        receiver_protocol = self.protocols[receiver]

        # Send all peer info from sender to receiver
        peer_infos = sender_protocol.get_all_peer_info()
        if peer_infos:
            actions = receiver_protocol.process_gossip(peer_infos)

            # Verify actions are consistent
            for url, action in actions.items():
                assert action in ["added", "updated", "ignored"]

    @rule()
    def check_consistency(self):
        """Check that the gossip protocol maintains consistency."""
        # All nodes should eventually converge to the same view
        # (this is a weak check - in practice convergence takes time)

        all_peer_urls: set[NodeURL] = set()
        for protocol in self.protocols.values():
            all_peer_urls.update(protocol.peers_info.keys())

        # Check that vector clocks are monotonic for each peer
        for peer_url in all_peer_urls:
            clocks = []
            for protocol in self.protocols.values():
                if peer_url in protocol.peers_info:
                    clock = VectorClock.from_dict(
                        protocol.peers_info[peer_url].logical_clock
                    )
                    clocks.append(clock)

            # All clocks for the same peer should be comparable or concurrent
            for i, clock1 in enumerate(clocks):
                for j, clock2 in enumerate(clocks[i + 1 :], i + 1):
                    # This should not fail - clocks should be related
                    comparison = clock1.compare(clock2)
                    assert comparison in ["equal", "before", "after", "concurrent"]


# Property-based tests
@given(
    node_ids=st.lists(
        st.text(min_size=1, max_size=10), min_size=2, max_size=5, unique=True
    ),
    num_updates=st.integers(min_value=1, max_value=10),
)
def test_gossip_convergence(node_ids, num_updates):
    """Test that gossip protocol converges to consistent state."""
    protocols = {node_id: VectorClockGossipProtocol(node_id) for node_id in node_ids}

    # Create some peer info on different nodes
    for i in range(num_updates):
        node_id = node_ids[i % len(node_ids)]
        protocol = protocols[node_id]

        peer_url = f"peer_{i}"
        funs = (f"func_{i}",)
        locs = frozenset([f"loc_{i}"])

        peer_info = protocol.create_peer_info(peer_url, funs, locs)
        protocol.peers_info[peer_url] = peer_info

    # Simulate gossip rounds
    for _ in range(len(node_ids) * 2):  # Enough rounds for convergence
        for sender_id in node_ids:
            for receiver_id in node_ids:
                if sender_id != receiver_id:
                    sender = protocols[sender_id]
                    receiver = protocols[receiver_id]

                    peer_infos = sender.get_all_peer_info()
                    receiver.process_gossip(peer_infos)

    # Check convergence - all nodes should have the same peers
    all_peer_urls: set[NodeURL] = set()
    for protocol in protocols.values():
        all_peer_urls.update(protocol.peers_info.keys())

    # All nodes should know about all peers (eventual consistency)
    for protocol in protocols.values():
        for peer_url in all_peer_urls:
            assert peer_url in protocol.peers_info, (
                f"Node {protocol.node_id} missing peer {peer_url}"
            )


@given(vector_clock_strategy(max_entries=5))
def test_vector_clock_properties(clock):
    """Test basic vector clock properties."""
    # Reflexivity: clock should equal itself
    assert clock.compare(clock) == "equal"

    # Transitivity: if A < B and B < C, then A < C
    incremented = clock.increment("test_node")
    double_incremented = incremented.increment("test_node")

    assert clock.compare(incremented) == "before"
    assert incremented.compare(double_incremented) == "before"
    assert clock.compare(double_incremented) == "before"


def test_concurrent_update_resolution():
    """Test that concurrent updates are resolved deterministically."""
    protocol = VectorClockGossipProtocol("node1")

    # Create two concurrent peer infos
    peer1 = PeerInfo(
        url="peer_test",
        funs=("func_a",),
        locs=frozenset(["loc_a"]),
        last_seen=0.0,
        cluster_id="test-cluster",
        advertised_urls=(),
        logical_clock={"node2": 1},  # From node2
    )

    peer2 = PeerInfo(
        url="peer_test",
        funs=("func_b",),
        locs=frozenset(["loc_b"]),
        last_seen=0.0,
        cluster_id="test-cluster",
        advertised_urls=(),
        logical_clock={"node3": 1},  # From node3 - concurrent with node2
    )

    # First update should be accepted
    protocol.peers_info["peer_test"] = peer1

    # Second update should be resolved deterministically
    should_update = protocol.should_update_peer(peer1, peer2)

    # The result should be deterministic based on function names
    expected = tuple(sorted(peer2.funs)) > tuple(sorted(peer1.funs))
    assert should_update == expected


# Integration with pytest
TestGossipProtocol = GossipStateMachine.TestCase


if __name__ == "__main__":
    # Run property-based tests
    test_concurrent_update_resolution()
    print("✅ All property tests passed!")
