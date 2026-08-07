"""
Comprehensive tests for membership and failure detection system.

This module tests the SWIM-based membership protocol including:
- MembershipProtocol SWIM-based failure detection
- MembershipInfo node state tracking
- ProbeRequest direct and indirect probing
- MembershipEvent change propagation
- Suspicion management and recovery
- Integration with gossip protocol

Test Coverage:
- SWIM protocol lifecycle and operations
- Direct and indirect probing mechanisms
- Suspicion escalation and timeout handling
- Membership event propagation
- Failure detection accuracy and timing
- Performance and scalability validation
"""

import asyncio
import time
from unittest.mock import AsyncMock, Mock

import pytest

from mpreg.fabric.consensus import ConsensusManager
from mpreg.fabric.federation_graph import GeographicCoordinate
from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.membership import (
    MembershipEvent,
    MembershipEventType,
    MembershipInfo,
    MembershipProtocol,
    MembershipState,
    ProbeRequest,
    ProbeResult,
)


@pytest.fixture
def sample_membership_info():
    """Create a sample membership info for testing."""
    return MembershipInfo(
        node_id="test_node_001",
        state=MembershipState.ALIVE,
        incarnation=1,
        coordinates=GeographicCoordinate(40.7128, -74.0060),
        region="us-east",
    )


@pytest.fixture
def sample_probe_request():
    """Create a sample probe request for testing."""
    return ProbeRequest(
        probe_id="test_probe_001",
        target_node="target_node",
        source_node="source_node",
        probe_type="direct",
        sequence_number=1,
        timeout=5.0,
    )


@pytest.fixture
def sample_membership_event():
    """Create a sample membership event for testing."""
    return MembershipEvent(
        event_id="test_event_001",
        event_type=MembershipEventType.NODE_JOINED,
        node_id="test_node",
        incarnation=1,
        source_node="source_node",
        reason="startup",
    )


@pytest.fixture
def mock_gossip_protocol():
    """Create a mock gossip protocol for testing."""
    mock_protocol = Mock(spec=GossipProtocol)
    mock_protocol.add_message = AsyncMock()
    return mock_protocol


@pytest.fixture
def mock_consensus_manager():
    """Create a mock consensus manager for testing."""
    return Mock(spec=ConsensusManager)


@pytest.fixture
def sample_membership_protocol(mock_gossip_protocol, mock_consensus_manager):
    """Create a sample membership protocol for testing."""
    return MembershipProtocol(
        node_id="test_membership_node",
        gossip_protocol=mock_gossip_protocol,
        consensus_manager=mock_consensus_manager,
        probe_interval=0.1,  # Fast for testing
        probe_timeout=1.0,
        suspicion_timeout=2.0,
        indirect_probe_count=2,
    )


class TestMembershipInfo:
    """Test suite for membership info implementation."""

    def test_membership_info_creation(self, sample_membership_info):
        """Test membership info creation."""
        info = sample_membership_info

        assert info.node_id == "test_node_001"
        assert info.state == MembershipState.ALIVE
        assert info.incarnation == 1
        assert info.region == "us-east"
        assert info.coordinates.latitude == 40.7128
        assert info.coordinates.longitude == -74.0060

    def test_membership_state_checks(self, sample_membership_info):
        """Test membership state check methods."""
        info = sample_membership_info

        # Initially alive
        assert info.is_alive()
        assert not info.is_suspect()
        assert not info.is_failed()
        assert info.is_available()

        # Change to suspect
        info.state = MembershipState.SUSPECT
        assert not info.is_alive()
        assert info.is_suspect()
        assert not info.is_failed()
        assert info.is_available()

        # Change to failed
        info.state = MembershipState.FAILED
        assert not info.is_alive()
        assert not info.is_suspect()
        assert info.is_failed()
        assert not info.is_available()

    def test_membership_staleness_tracking(self, sample_membership_info):
        """Test membership staleness tracking."""
        info = sample_membership_info

        # Initial staleness should be minimal
        staleness = info.get_staleness()
        assert staleness >= 0
        assert staleness < 1.0  # Should be very small

        # Wait and check staleness increases
        time.sleep(0.1)
        new_staleness = info.get_staleness()
        assert new_staleness > staleness

        # Update seen time
        info.update_seen()
        updated_staleness = info.get_staleness()
        assert updated_staleness < new_staleness

    def test_membership_suspicion_tracking(self, sample_membership_info):
        """Test membership suspicion tracking."""
        info = sample_membership_info

        # Initially no suspicion
        assert info.suspicion_level == 0.0
        assert len(info.suspected_by) == 0
        assert info.suspected_at == 0.0

        # Add suspicion
        info.add_suspicion("node_1")
        assert info.suspicion_level > 0.0
        assert "node_1" in info.suspected_by
        assert info.suspected_at > 0.0

        # Add more suspicion
        info.add_suspicion("node_2")
        info.add_suspicion("node_3")
        assert info.suspicion_level > 0.3  # Should be higher with more suspicions
        assert len(info.suspected_by) == 3

        # Clear suspicion
        info.clear_suspicion()
        assert info.suspicion_level == 0.0
        assert len(info.suspected_by) == 0
        assert info.suspected_at == 0.0

    def test_membership_suspicion_expiration(self, sample_membership_info):
        """Test membership suspicion expiration."""
        info = sample_membership_info
        info.suspicion_timeout = 0.5  # Realistic timeout for stable testing

        # Add suspicion
        info.add_suspicion("node_1")
        assert not info.is_suspicion_expired()

        # Wait for expiration
        time.sleep(0.6)
        assert info.is_suspicion_expired()

    def test_membership_summary(self, sample_membership_info):
        """Test membership summary generation."""
        info = sample_membership_info

        summary = info.get_membership_summary()

        assert summary.node_id == "test_node_001"
        assert summary.state == MembershipState.ALIVE.value
        assert summary.incarnation == 1
        assert summary.region == "us-east"
        assert hasattr(summary, "staleness_seconds")
        assert hasattr(summary, "probe_failures")
        assert hasattr(summary, "suspicion_level")
        assert summary.is_available


class TestMembershipEvent:
    """Test suite for membership event implementation."""

    def test_membership_event_creation(self, sample_membership_event):
        """Test membership event creation."""
        event = sample_membership_event

        assert event.event_id == "test_event_001"
        assert event.event_type == MembershipEventType.NODE_JOINED
        assert event.node_id == "test_node"
        assert event.incarnation == 1
        assert event.source_node == "source_node"
        assert event.reason == "startup"

    def test_membership_event_summary(self, sample_membership_event):
        """Test membership event summary generation."""
        event = sample_membership_event

        summary = event.get_event_summary()

        assert summary.event_id == "test_event_001"
        assert summary.event_type == MembershipEventType.NODE_JOINED.value
        assert summary.node_id == "test_node"
        assert summary.incarnation == 1
        assert summary.source_node == "source_node"
        assert summary.reason == "startup"
        assert hasattr(summary, "age_seconds")


class TestProbeRequest:
    """Test suite for probe request implementation."""

    def test_probe_request_creation(self, sample_probe_request):
        """Test probe request creation."""
        probe = sample_probe_request

        assert probe.probe_id == "test_probe_001"
        assert probe.target_node == "target_node"
        assert probe.source_node == "source_node"
        assert probe.probe_type == "direct"
        assert probe.sequence_number == 1
        assert probe.timeout == 5.0

    def test_probe_request_expiration(self, sample_probe_request):
        """Test probe request expiration."""
        probe = sample_probe_request
        probe.timeout = 0.5  # Realistic timeout for stable testing

        # Initially not expired
        assert not probe.is_expired()

        # Wait for expiration
        time.sleep(0.6)
        assert probe.is_expired()

    def test_probe_request_age(self, sample_probe_request):
        """Test probe request age calculation."""
        probe = sample_probe_request

        # Initial age should be minimal
        age = probe.get_age()
        assert age >= 0
        assert age < 1.0

        # Wait and check age increases
        time.sleep(0.1)
        new_age = probe.get_age()
        assert new_age > age


class TestMembershipProtocol:
    """Test suite for membership protocol implementation."""

    def test_membership_protocol_initialization(self, sample_membership_protocol):
        """Test membership protocol initialization."""
        protocol = sample_membership_protocol

        assert protocol.node_id == "test_membership_node"
        assert protocol.probe_interval == 0.1
        assert protocol.probe_timeout == 1.0
        assert protocol.suspicion_timeout == 2.0
        assert protocol.indirect_probe_count == 2

        # Should have own membership info
        assert protocol.node_id in protocol.membership
        assert protocol.membership[protocol.node_id].state == MembershipState.ALIVE

    @pytest.mark.asyncio
    async def test_membership_protocol_lifecycle(self, sample_membership_protocol):
        """Test membership protocol start and stop."""
        protocol = sample_membership_protocol

        # Start protocol
        await protocol.start()
        assert len(protocol._background_tasks) > 0
        assert protocol.membership_stats["protocol_started"] > 0

        # Stop protocol
        await protocol.stop()
        assert len(protocol._background_tasks) == 0
        assert protocol.membership_stats["protocol_stopped"] > 0

    def test_membership_protocol_node_lists(self, sample_membership_protocol):
        """Test membership protocol node list methods."""
        protocol = sample_membership_protocol

        # Add test nodes
        protocol.membership["alive_node"] = MembershipInfo(
            node_id="alive_node", state=MembershipState.ALIVE, incarnation=1
        )

        protocol.membership["suspect_node"] = MembershipInfo(
            node_id="suspect_node", state=MembershipState.SUSPECT, incarnation=1
        )

        protocol.membership["failed_node"] = MembershipInfo(
            node_id="failed_node", state=MembershipState.FAILED, incarnation=1
        )

        # Test node lists
        alive_nodes = protocol.get_alive_nodes()
        suspect_nodes = protocol.get_suspect_nodes()
        failed_nodes = protocol.get_failed_nodes()

        assert protocol.node_id in alive_nodes
        assert "alive_node" in alive_nodes
        assert "suspect_node" in suspect_nodes
        assert "failed_node" in failed_nodes

        # Test membership list
        membership_list = protocol.get_membership_list()
        assert len(membership_list) == 4  # Including self

    def test_membership_protocol_probe_target_selection(
        self, sample_membership_protocol
    ):
        """Test probe target selection."""
        protocol = sample_membership_protocol

        # Add test nodes
        protocol.membership["node_1"] = MembershipInfo(
            node_id="node_1",
            state=MembershipState.ALIVE,
            incarnation=1,
            last_probe=time.time() - 100,  # Old probe
        )

        protocol.membership["node_2"] = MembershipInfo(
            node_id="node_2",
            state=MembershipState.ALIVE,
            incarnation=1,
            last_probe=time.time() - 50,  # More recent probe
        )

        protocol.membership["node_3"] = MembershipInfo(
            node_id="node_3", state=MembershipState.FAILED, incarnation=1
        )

        # Select target
        target = protocol._select_probe_target()

        # Should select available node (not failed, not self)
        assert target in ["node_1", "node_2"]

        # Should prefer node that hasn't been probed recently
        # (though this is probabilistic, so we can't guarantee)
        assert target is not None

    @pytest.mark.asyncio
    async def test_membership_protocol_probe_handling(self, sample_membership_protocol):
        """Test probe message handling."""
        protocol = sample_membership_protocol

        # Test probe for ourselves
        probe_payload = {
            "probe_id": "test_probe",
            "target_node": protocol.node_id,
            "source_node": "remote_node",
            "probe_type": "direct",
            "sequence_number": 1,
            "timestamp": time.time(),
        }

        await protocol.handle_probe_message(probe_payload)

        # Should have sent ACK via gossip
        assert protocol.gossip_protocol.add_message.called

        # Should have updated membership for source node
        assert "remote_node" in protocol.membership
        assert protocol.membership["remote_node"].state == MembershipState.ALIVE

    @pytest.mark.asyncio
    async def test_membership_protocol_probe_ack_handling(
        self, sample_membership_protocol
    ):
        """Test probe ACK handling."""
        protocol = sample_membership_protocol

        # Add pending probe
        probe_id = "test_probe_ack"
        protocol.pending_probes[probe_id] = ProbeRequest(
            probe_id=probe_id, target_node="target_node", source_node=protocol.node_id
        )

        # Handle ACK
        ack_payload = {
            "probe_id": probe_id,
            "source_node": "responding_node",
            "target_node": protocol.node_id,
            "timestamp": time.time(),
        }

        await protocol.handle_probe_ack(ack_payload)

        # Probe should be removed from pending
        assert probe_id not in protocol.pending_probes

        # Should have updated membership for responding node
        assert "responding_node" in protocol.membership
        assert protocol.membership["responding_node"].state == MembershipState.ALIVE

    @pytest.mark.asyncio
    async def test_membership_protocol_event_handling(self, sample_membership_protocol):
        """Test membership event handling."""
        protocol = sample_membership_protocol

        # Test node join event
        join_payload = {
            "event_id": "join_test",
            "event_type": MembershipEventType.NODE_JOINED.value,
            "node_id": "new_node",
            "incarnation": 1,
            "timestamp": time.time(),
            "source_node": "new_node",
            "reason": "startup",
        }

        await protocol.handle_membership_event(join_payload)

        # Should have added new node
        assert "new_node" in protocol.membership
        assert protocol.membership["new_node"].state == MembershipState.ALIVE
        assert protocol.membership["new_node"].incarnation == 1

        # Test node failure event
        fail_payload = {
            "event_id": "fail_test",
            "event_type": MembershipEventType.NODE_FAILED.value,
            "node_id": "new_node",
            "incarnation": 2,
            "timestamp": time.time(),
            "source_node": "other_node",
            "reason": "timeout",
        }

        await protocol.handle_membership_event(fail_payload)

        # Should have updated node state
        assert protocol.membership["new_node"].state == MembershipState.FAILED
        assert protocol.membership["new_node"].incarnation == 2

    @pytest.mark.asyncio
    async def test_membership_protocol_suspicion_management(
        self, sample_membership_protocol
    ):
        """Test suspicion management."""
        protocol = sample_membership_protocol

        # Add node to suspect
        protocol.membership["suspect_node"] = MembershipInfo(
            node_id="suspect_node", state=MembershipState.ALIVE, incarnation=1
        )

        # Suspect the node
        await protocol._suspect_node("suspect_node", "test_reason")

        # Should be in suspect state
        assert protocol.membership["suspect_node"].state == MembershipState.SUSPECT
        assert protocol.membership["suspect_node"].suspicion_level > 0
        assert protocol.membership_stats["nodes_suspected"] == 1

        # Should have propagated via gossip
        assert protocol.gossip_protocol.add_message.called

    @pytest.mark.asyncio
    async def test_membership_protocol_recovery(self, sample_membership_protocol):
        """Test node recovery from suspicion."""
        protocol = sample_membership_protocol

        # Add suspected node
        protocol.membership["recovery_node"] = MembershipInfo(
            node_id="recovery_node", state=MembershipState.SUSPECT, incarnation=1
        )
        protocol.membership["recovery_node"].add_suspicion("other_node")

        # Recover the node
        await protocol._recover_node("recovery_node")

        # Should be alive again
        assert protocol.membership["recovery_node"].state == MembershipState.ALIVE
        assert protocol.membership["recovery_node"].suspicion_level == 0
        assert protocol.membership_stats["nodes_recovered"] == 1

        # Should have propagated via gossip
        assert protocol.gossip_protocol.add_message.called

    @pytest.mark.asyncio
    async def test_membership_protocol_failure_marking(
        self, sample_membership_protocol
    ):
        """Test marking node as failed."""
        protocol = sample_membership_protocol

        # Add node to fail
        protocol.membership["fail_node"] = MembershipInfo(
            node_id="fail_node", state=MembershipState.SUSPECT, incarnation=1
        )

        # Fail the node
        await protocol._fail_node("fail_node", "test_reason")

        # Should be in failed state
        assert protocol.membership["fail_node"].state == MembershipState.FAILED
        assert protocol.membership["fail_node"].suspicion_level == 0
        assert protocol.membership_stats["nodes_failed"] == 1

        # Should have propagated via gossip
        assert protocol.gossip_protocol.add_message.called

    @pytest.mark.asyncio
    async def test_membership_protocol_refutation(self, sample_membership_protocol):
        """Test membership event refutation."""
        protocol = sample_membership_protocol

        initial_incarnation = protocol.own_info.incarnation

        # Handle event about ourselves with old incarnation
        event_payload = {
            "event_id": "refute_test",
            "event_type": MembershipEventType.NODE_FAILED.value,
            "node_id": protocol.node_id,
            "incarnation": initial_incarnation,  # Same incarnation
            "timestamp": time.time(),
            "source_node": "other_node",
            "reason": "timeout",
        }

        await protocol.handle_membership_event(event_payload)

        # Should have incremented incarnation
        assert protocol.own_info.incarnation > initial_incarnation
        assert protocol.membership_stats["refutations_sent"] == 1

        # Should have propagated refutation via gossip
        assert protocol.gossip_protocol.add_message.called

    @pytest.mark.asyncio
    async def test_membership_protocol_suspicion_timeout(
        self, sample_membership_protocol
    ):
        """Test suspicion timeout handling."""
        protocol = sample_membership_protocol

        # Add suspected node with expired suspicion
        protocol.membership["timeout_node"] = MembershipInfo(
            node_id="timeout_node",
            state=MembershipState.SUSPECT,
            incarnation=1,
            suspected_at=time.time() - 10,  # Long ago
            suspicion_timeout=1.0,  # Short timeout
        )

        # Process suspicions
        await protocol._process_suspicions()

        # Should have been marked as failed
        assert protocol.membership["timeout_node"].state == MembershipState.FAILED

    @pytest.mark.asyncio
    async def test_membership_protocol_cleanup(self, sample_membership_protocol):
        """Test cleanup of expired probes."""
        protocol = sample_membership_protocol

        # Add expired probe
        expired_probe = ProbeRequest(
            probe_id="expired_probe",
            target_node="target",
            source_node=protocol.node_id,
            timestamp=time.time() - 10,  # Old timestamp
            timeout=1.0,  # Short timeout
        )

        protocol.pending_probes["expired_probe"] = expired_probe

        # Run cleanup
        await protocol._cleanup_expired_probes()

        # Should have been removed
        assert "expired_probe" not in protocol.pending_probes

    def test_membership_protocol_statistics(self, sample_membership_protocol):
        """Test membership protocol statistics."""
        protocol = sample_membership_protocol

        # Add some test data
        protocol.membership["test_node"] = MembershipInfo(
            node_id="test_node", state=MembershipState.ALIVE, incarnation=1
        )

        protocol.membership_stats["test_stat"] = 42
        protocol.probe_stats["test_probe"] = 10

        stats = protocol.get_membership_statistics()

        assert hasattr(stats, "membership_info")
        assert hasattr(stats, "membership_counts")
        assert hasattr(stats, "membership_stats")
        assert hasattr(stats, "probe_stats")

        membership_info = stats.membership_info
        assert membership_info["node_id"] == "test_membership_node"
        assert membership_info["probe_interval"] == 0.1
        assert membership_info["probe_timeout"] == 1.0

        membership_counts = stats.membership_counts
        assert membership_counts["total_nodes"] == 2  # Self + test_node
        assert membership_counts["alive_nodes"] == 2

        assert stats.membership_stats["test_stat"] == 42
        assert stats.probe_stats["test_probe"] == 10


class TestEndToEndMembershipScenarios:
    """Test suite for end-to-end membership scenarios."""

    @pytest.mark.asyncio
    async def test_node_join_workflow(self):
        """Test complete node join workflow."""
        # Create two membership protocols
        protocol1 = MembershipProtocol(
            node_id="node_1",
            probe_interval=0.1,
            probe_timeout=0.5,
            suspicion_timeout=1.0,
        )

        protocol2 = MembershipProtocol(
            node_id="node_2",
            probe_interval=0.1,
            probe_timeout=0.5,
            suspicion_timeout=1.0,
        )

        # Start both protocols
        await protocol1.start()
        await protocol2.start()

        try:
            # Simulate node_2 joining by adding it to node_1's membership
            join_event = {
                "event_id": "join_test",
                "event_type": MembershipEventType.NODE_JOINED.value,
                "node_id": "node_2",
                "incarnation": 1,
                "timestamp": time.time(),
                "source_node": "node_2",
                "reason": "startup",
            }

            await protocol1.handle_membership_event(join_event)

            # Should have added node_2 to node_1's membership
            assert "node_2" in protocol1.membership
            assert protocol1.membership["node_2"].state == MembershipState.ALIVE

        finally:
            # Stop protocols
            await protocol1.stop()
            await protocol2.stop()

    @pytest.mark.asyncio
    async def test_failure_detection_workflow(self):
        """Test complete failure detection workflow."""
        protocol = MembershipProtocol(
            node_id="detector",
            probe_interval=0.1,
            probe_timeout=1.0,
            suspicion_timeout=2.0,
        )

        # Add a node to monitor
        protocol.membership["target_node"] = MembershipInfo(
            node_id="target_node",
            state=MembershipState.ALIVE,
            incarnation=1,
            suspicion_timeout=0.5,  # Match protocol timeout
        )

        await protocol.start()

        try:
            # Simulate failed probe
            await protocol._process_probe_result("target_node", ProbeResult.FAILURE)

            # Should be suspected
            assert protocol.membership["target_node"].state == MembershipState.SUSPECT

            # Wait for suspicion timeout
            await asyncio.sleep(0.6)

            # Process suspicions
            await protocol._process_suspicions()

            # Should be marked as failed
            assert protocol.membership["target_node"].state == MembershipState.FAILED

        finally:
            await protocol.stop()

    @pytest.mark.asyncio
    async def test_recovery_workflow(self):
        """Test node recovery workflow."""
        protocol = MembershipProtocol(
            node_id="recovery_test",
            probe_interval=0.1,
            probe_timeout=0.2,
            suspicion_timeout=1.0,
        )

        # Add suspected node
        protocol.membership["recovery_node"] = MembershipInfo(
            node_id="recovery_node", state=MembershipState.SUSPECT, incarnation=1
        )
        protocol.membership["recovery_node"].add_suspicion("other_node")

        await protocol.start()

        try:
            # Simulate successful probe
            await protocol._process_probe_result("recovery_node", ProbeResult.SUCCESS)

            # Should be recovered
            assert protocol.membership["recovery_node"].state == MembershipState.ALIVE
            assert protocol.membership["recovery_node"].suspicion_level == 0

        finally:
            await protocol.stop()

    @pytest.mark.asyncio
    async def test_multiple_nodes_interaction(self):
        """Test interaction between multiple nodes."""
        protocols = {}

        # Create multiple protocols
        for i in range(3):
            protocols[f"node_{i}"] = MembershipProtocol(
                node_id=f"node_{i}",
                probe_interval=0.1,
                probe_timeout=0.2,
                suspicion_timeout=0.5,
            )

        # Start all protocols
        for protocol in protocols.values():
            await protocol.start()

        try:
            # Simulate cross-membership awareness
            for i in range(3):
                for j in range(3):
                    if i != j:
                        protocols[f"node_{i}"].membership[f"node_{j}"] = MembershipInfo(
                            node_id=f"node_{j}",
                            state=MembershipState.ALIVE,
                            incarnation=1,
                        )

            # Simulate node_1 suspecting node_2
            await protocols["node_1"]._suspect_node("node_2", "test_suspicion")

            # Check that node_2 is suspected in node_1's view
            assert (
                protocols["node_1"].membership["node_2"].state
                == MembershipState.SUSPECT
            )

            # Simulate node_2 recovery
            await protocols["node_1"]._recover_node("node_2")

            # Check that node_2 is recovered
            assert (
                protocols["node_1"].membership["node_2"].state == MembershipState.ALIVE
            )

        finally:
            # Stop all protocols
            for protocol in protocols.values():
                await protocol.stop()


class TestMembershipPerformanceAndScalability:
    """Test suite for membership performance and scalability."""

    @pytest.mark.asyncio
    async def test_membership_protocol_performance(self):
        """Test membership protocol performance with many nodes."""
        protocol = MembershipProtocol(
            node_id="perf_test_node",
            probe_interval=0.01,
            probe_timeout=0.5,
            suspicion_timeout=1.5,
        )

        # Add many nodes
        for i in range(100):
            protocol.membership[f"node_{i}"] = MembershipInfo(
                node_id=f"node_{i}", state=MembershipState.ALIVE, incarnation=1
            )

        await protocol.start()

        try:
            # Test target selection performance
            start_time = time.time()

            for _ in range(50):
                target = protocol._select_probe_target()
                assert target is not None
                assert target.startswith("node_")

            selection_time = time.time() - start_time

            # Should be very fast
            assert selection_time < 0.1

            # Test membership list operations
            start_time = time.time()

            alive_nodes = protocol.get_alive_nodes()
            suspect_nodes = protocol.get_suspect_nodes()
            failed_nodes = protocol.get_failed_nodes()

            list_time = time.time() - start_time

            # Should be very fast
            assert list_time < 0.01
            assert len(alive_nodes) == 101  # 100 + self
            assert len(suspect_nodes) == 0
            assert len(failed_nodes) == 0

        finally:
            await protocol.stop()

    @pytest.mark.asyncio
    async def test_membership_event_processing_performance(self):
        """Test performance of membership event processing."""
        protocol = MembershipProtocol(
            node_id="event_perf_test",
            probe_interval=1.0,  # Slow to avoid interference
            probe_timeout=0.5,
            suspicion_timeout=1.5,
        )

        await protocol.start()

        try:
            # Process many events
            start_time = time.time()

            for i in range(100):
                event_payload = {
                    "event_id": f"event_{i}",
                    "event_type": MembershipEventType.NODE_JOINED.value,
                    "node_id": f"node_{i}",
                    "incarnation": 1,
                    "timestamp": time.time(),
                    "source_node": f"node_{i}",
                    "reason": "startup",
                }

                await protocol.handle_membership_event(event_payload)

            processing_time = time.time() - start_time

            # Should process quickly
            assert processing_time < 0.5

            # Should have added all nodes
            assert len(protocol.membership) == 101  # 100 + self

        finally:
            await protocol.stop()

    def test_membership_info_memory_efficiency(self):
        """Test memory efficiency of membership info."""
        # Create many membership info objects
        membership_infos = []

        for i in range(1000):
            info = MembershipInfo(
                node_id=f"node_{i}",
                state=MembershipState.ALIVE,
                incarnation=1,
                coordinates=GeographicCoordinate(40.0 + i * 0.01, -74.0 + i * 0.01),
                region=f"region_{i % 10}",
            )
            membership_infos.append(info)

        # Test operations on all objects
        start_time = time.time()

        alive_count = sum(1 for info in membership_infos if info.is_alive())
        staleness_sum = sum(info.get_staleness() for info in membership_infos)

        operation_time = time.time() - start_time

        # Should be very fast
        assert operation_time < 0.1
        assert alive_count == 1000
        assert staleness_sum >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
