"""
Comprehensive tests for blockchain-federation integration.

Tests the integration between the blockchain message queue system
and the MPREG federation infrastructure, including DAO governance
across hub hierarchies and cross-region coordination.
"""

import time
from unittest.mock import patch

import pytest

from mpreg.core.blockchain_message_queue_types import (
    BlockchainMessage,
    DeliveryGuarantee,
    MessagePriority,
)
from mpreg.datastructures import (
    DaoMember,
    DecentralizedAutonomousOrganization,
    VoteType,
)
from mpreg.fabric.blockchain_message_federation import (
    BlockchainFederationBridge,
    CrossRegionCoordinator,
    FederationMessageRoute,
    FederationRouteManager,
    HubMessageQueue,
)
from mpreg.fabric.federation_graph import (
    GeographicCoordinate,
)
from mpreg.fabric.hubs import (
    GlobalHub,
    HubCapabilities,
    HubTier,
    LocalHub,
    RegionalHub,
)


class TestHubMessageQueue:
    """Test hub-specific blockchain message queue functionality."""

    def setup_method(self):
        """Set up test environment."""
        # Create test DAO
        self.dao = DecentralizedAutonomousOrganization(
            name="Test Federation DAO",
            description="DAO for testing federation integration",
        )

        # Add DAO members
        members = [
            DaoMember(member_id="hub_operator", voting_power=1000, token_balance=5000),
            DaoMember(
                member_id="federation_admin", voting_power=800, token_balance=4000
            ),
        ]

        for member in members:
            self.dao = self.dao.add_member(member)

        # Create test hub
        self.test_hub = LocalHub(
            hub_id="test_local_hub",
            hub_tier=HubTier.LOCAL,
            capabilities=HubCapabilities(
                max_clusters=100, coverage_radius_km=100.0, bandwidth_mbps=1000
            ),
            coordinates=GeographicCoordinate(37.7749, -122.4194),  # San Francisco
            region="us_west",
        )

        # Create hub message queue
        self.hub_queue = HubMessageQueue(self.test_hub, self.dao)

    def test_hub_queue_initialization(self):
        """Test hub message queue initialization."""
        assert self.hub_queue.hub_id == "test_local_hub"
        assert self.hub_queue.hub_tier == HubTier.LOCAL
        assert len(self.hub_queue.hub_state.connected_hubs) == 0
        assert self.hub_queue.queue.queue_id.startswith("hub_queue_")

        # Check governance member added
        assert len(self.hub_queue.queue.governance.dao.members) > 0

    @pytest.mark.asyncio
    async def test_process_federation_message(self):
        """Test processing federation messages."""
        # Create test message
        message = BlockchainMessage(
            sender_id="client_1",
            recipient_id="client_2",
            message_type="test_federation",
            priority=MessagePriority.NORMAL,
            processing_fee=100,
            payload=b"test federation message",
        )

        # Process message
        route = await self.hub_queue.process_federation_message(
            message, "destination_hub"
        )

        # Should create hierarchical route since destination not directly connected
        assert route is not None
        assert route.source_hub_id == "test_local_hub"
        assert route.destination_hub_id == "destination_hub"
        assert len(route.hub_path) > 1  # Multi-hop route
        assert route.estimated_hops >= 1

    @pytest.mark.asyncio
    async def test_direct_route_creation(self):
        """Test creation of direct routes between hubs."""
        # Add connected hub
        self.hub_queue.hub_state.add_connected_hub("connected_hub")

        message = BlockchainMessage(
            sender_id="client_1",
            recipient_id="client_2",
            priority=MessagePriority.HIGH,
            processing_fee=50,
        )

        route = await self.hub_queue.process_federation_message(
            message, "connected_hub"
        )

        assert route is not None
        assert route.estimated_hops == 1
        assert route.hub_path == ["test_local_hub", "connected_hub"]
        assert not route.cross_region

    @pytest.mark.asyncio
    async def test_emergency_message_priority(self):
        """Test emergency message handling."""
        emergency_message = BlockchainMessage(
            sender_id="emergency_service",
            recipient_id="response_team",
            priority=MessagePriority.EMERGENCY,
            delivery_guarantee=DeliveryGuarantee.EXACTLY_ONCE,
            processing_fee=500,
            payload=b"EMERGENCY: Immediate response required",
        )

        route = await self.hub_queue.process_federation_message(
            emergency_message, "emergency_hub"
        )

        assert route is not None
        assert route.sla_tier == 1  # Premium SLA for emergency
        # Emergency should get fast routing
        assert route.base_route.latency_ms <= 100

    def test_hub_metrics_collection(self):
        """Test hub performance metrics collection."""
        # Simulate some activity
        self.hub_queue.hub_state.add_connected_hub("hub_1")
        self.hub_queue.hub_state.add_connected_hub("hub_2")

        # Add some simulated metrics
        from mpreg.fabric.federation_types import HubPerformanceMetrics

        test_metrics = HubPerformanceMetrics(
            hub_id="test_local_hub", messages_routed=100, cross_region_messages=25
        )
        self.hub_queue.hub_state.add_performance_metrics(test_metrics)

        status = self.hub_queue.get_hub_status()

        assert status.hub_id == "test_local_hub"
        assert status.connected_hubs_count == 2
        assert status.messages_routed == 100
        assert status.cross_region_messages == 25


class TestFederationRouteManager:
    """Test federation-wide route management and governance."""

    def setup_method(self):
        """Set up test environment."""
        # Create global DAO
        self.global_dao = DecentralizedAutonomousOrganization(
            name="Global Federation DAO",
            description="Global governance for message federation",
        )

        # Add global members
        global_members = [
            DaoMember(member_id="global_admin", voting_power=2000, token_balance=10000),
            DaoMember(
                member_id="regional_coord_1", voting_power=1500, token_balance=7500
            ),
            DaoMember(
                member_id="regional_coord_2", voting_power=1500, token_balance=7500
            ),
        ]

        for member in global_members:
            self.global_dao = self.global_dao.add_member(member)

        # Create route manager
        self.route_manager = FederationRouteManager(self.global_dao)

        # Create test hubs
        self.hub1 = LocalHub(
            hub_id="hub_1",
            hub_tier=HubTier.LOCAL,
            capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(37.7749, -122.4194),
            region="us_west",
        )

        self.hub2 = RegionalHub(
            hub_id="hub_2",
            hub_tier=HubTier.REGIONAL,
            capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(40.7128, -74.0060),
            region="us_east",
        )

        # Create hub queues
        self.hub_queue_1 = HubMessageQueue(self.hub1, self.global_dao)
        self.hub_queue_2 = HubMessageQueue(self.hub2, self.global_dao)

        # Register with route manager
        self.route_manager.register_hub_queue(self.hub_queue_1)
        self.route_manager.register_hub_queue(self.hub_queue_2)

    @pytest.mark.asyncio
    async def test_global_routing_policy_proposal(self):
        """Test proposing global routing policies."""
        policy_spec = {
            "name": "Priority Cross-Region Routing",
            "description": "Optimize routing for cross-region emergency messages",
            "emergency_latency_max_ms": 50,
            "preferred_routes": ["emergency_route_1", "emergency_route_2"],
            "cost_optimization": True,
        }

        proposal_id = await self.route_manager.propose_global_routing_policy(
            "global_admin", policy_spec
        )

        assert proposal_id in self.route_manager.global_governance.dao.proposals
        proposal = self.route_manager.global_governance.dao.proposals[proposal_id]
        assert "Priority Cross-Region Routing" in proposal.title
        assert proposal.metadata["scope"] == "global_federation"
        assert len(proposal.metadata["affected_hubs"]) == 2

    @pytest.mark.asyncio
    async def test_global_policy_execution(self):
        """Test execution of approved global policies."""
        # Create and approve policy
        policy_spec = {
            "name": "Test Global Policy",
            "description": "Test policy for execution",
            "test_parameter": 42,
        }

        proposal_id = await self.route_manager.propose_global_routing_policy(
            "global_admin", policy_spec
        )

        # Vote and approve
        self.route_manager.global_governance.dao = (
            self.route_manager.global_governance.dao.cast_vote(
                "global_admin", proposal_id, VoteType.FOR
            )
        )
        self.route_manager.global_governance.dao = (
            self.route_manager.global_governance.dao.cast_vote(
                "regional_coord_1", proposal_id, VoteType.FOR
            )
        )
        self.route_manager.global_governance.dao = (
            self.route_manager.global_governance.dao.cast_vote(
                "regional_coord_2", proposal_id, VoteType.FOR
            )
        )

        # Finalize proposal
        with patch("time.time", return_value=time.time() + 86400 * 8):
            self.route_manager.global_governance.dao = (
                self.route_manager.global_governance.dao.finalize_proposal(proposal_id)
            )

        # Execute policy
        await self.route_manager.execute_global_policy(proposal_id)

        # Verify policy applied to all hubs
        for hub_queue in [self.hub_queue_1, self.hub_queue_2]:
            active_policies = hub_queue.queue.governance.get_active_policies("routing")
            assert any(p.policy_name == "Test Global Policy" for p in active_policies)

    def test_hub_queue_registration(self):
        """Test hub queue registration with route manager."""
        assert len(self.route_manager.hub_queues) == 2
        assert "hub_1" in self.route_manager.hub_queues
        assert "hub_2" in self.route_manager.hub_queues

        # Verify governance policy sync
        global_policies = self.route_manager.global_governance.get_active_policies()
        for hub_queue in self.route_manager.hub_queues.values():
            hub_policies = hub_queue.queue.governance.get_active_policies()
            # Hub should have at least the same number of policies as global
            assert len(hub_policies) >= len(global_policies)


class TestCrossRegionCoordinator:
    """Test cross-region message coordination."""

    def setup_method(self):
        """Set up test environment."""
        # Create global DAO
        self.global_dao = DecentralizedAutonomousOrganization(
            name="Cross Region DAO", description="Cross-region coordination DAO"
        )

        # Create route manager and coordinator
        self.route_manager = FederationRouteManager(self.global_dao)
        self.coordinator = CrossRegionCoordinator(self.route_manager)

        # Create regional hubs
        self.west_hub = RegionalHub(
            hub_id="west_regional",
            hub_tier=HubTier.REGIONAL,
            capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(37.7749, -122.4194),
            region="us_west",
        )

        self.east_hub = RegionalHub(
            hub_id="east_regional",
            hub_tier=HubTier.REGIONAL,
            capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(40.7128, -74.0060),
            region="us_east",
        )

        # Create hub queues
        self.west_queue = HubMessageQueue(self.west_hub, self.global_dao)
        self.east_queue = HubMessageQueue(self.east_hub, self.global_dao)

        # Register hubs
        self.route_manager.register_hub_queue(self.west_queue)
        self.route_manager.register_hub_queue(self.east_queue)

        # Register regional coordinators
        self.coordinator.register_regional_coordinator("us_west", "west_regional")
        self.coordinator.register_regional_coordinator("us_east", "east_regional")

    def test_regional_coordinator_registration(self):
        """Test registration of regional coordinators."""
        assert self.coordinator.region_coordinators["us_west"] == "west_regional"
        assert self.coordinator.region_coordinators["us_east"] == "east_regional"

    @pytest.mark.asyncio
    async def test_cross_region_message_coordination(self):
        """Test coordination of cross-region message delivery."""
        # Create cross-region message
        message = BlockchainMessage(
            sender_id="west_client",
            recipient_id="east_client",
            message_type="cross_region_test",
            priority=MessagePriority.HIGH,
            processing_fee=200,
            payload=b"cross-region coordination test",
        )

        # Coordinate delivery
        success = await self.coordinator.coordinate_cross_region_delivery(
            message, "us_west", "us_east"
        )

        assert success is True

        # Check metrics were recorded
        metrics_key = "us_west_us_east"
        assert metrics_key in self.coordinator.cross_region_metrics
        assert (
            self.coordinator.cross_region_metrics[metrics_key].messages_delivered == 1
        )

    @pytest.mark.asyncio
    async def test_cross_region_coordination_missing_coordinator(self):
        """Test handling of missing regional coordinator."""
        message = BlockchainMessage(
            sender_id="client_1",
            recipient_id="client_2",
            priority=MessagePriority.NORMAL,
            processing_fee=100,
        )

        # Try coordination with missing region
        success = await self.coordinator.coordinate_cross_region_delivery(
            message, "us_west", "unknown_region"
        )

        assert success is False

    def test_cross_region_performance_metrics(self):
        """Test cross-region performance metrics collection."""
        # Create test metrics and add them
        from mpreg.fabric.federation_types import CrossRegionPerformanceMetrics

        metrics_key = "us_west_us_east"
        test_metrics = CrossRegionPerformanceMetrics(
            route_key=metrics_key,
            messages_delivered=10,
            total_delivery_time_ms=500.0,  # 50ms avg
            total_cost=100.0,  # 10 avg cost
            total_hops=30,  # 3 avg hops
        )

        self.coordinator.cross_region_state.add_delivery_metrics(
            metrics_key, test_metrics
        )

        performance = self.coordinator.get_cross_region_performance()

        assert metrics_key in performance
        route_perf = performance[metrics_key]
        assert route_perf["messages_delivered"] == 10
        assert route_perf["average_latency_ms"] == 50.0
        assert route_perf["average_cost"] == 10.0
        assert route_perf["average_hops"] == 3.0


class TestBlockchainFederationBridge:
    """Test the main federation bridge integration."""

    def setup_method(self):
        """Set up test environment."""
        # Create global DAO
        self.global_dao = DecentralizedAutonomousOrganization(
            name="Federation Bridge DAO",
            description="Main federation bridge governance",
        )

        # Add global members
        members = [
            DaoMember(member_id="bridge_admin", voting_power=2000, token_balance=10000),
            DaoMember(
                member_id="hub_operator_1", voting_power=1000, token_balance=5000
            ),
            DaoMember(
                member_id="hub_operator_2", voting_power=1000, token_balance=5000
            ),
        ]

        for member in members:
            self.global_dao = self.global_dao.add_member(member)

        # Create federation bridge
        self.bridge = BlockchainFederationBridge(self.global_dao)

        # Create test hubs
        self.local_hub = LocalHub(
            hub_id="local_test",
            hub_tier=HubTier.LOCAL,
            capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(37.7749, -122.4194),
            region="us_west",
        )

        self.regional_hub = RegionalHub(
            hub_id="regional_test",
            hub_tier=HubTier.REGIONAL,
            capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(37.7749, -122.4194),
            region="us_west",
        )

        self.global_hub = GlobalHub(
            hub_id="global_test",
            hub_tier=HubTier.GLOBAL,
            capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(0.0, 0.0),
            region="global",
        )

    @pytest.mark.asyncio
    async def test_hub_integration(self):
        """Test integration of hubs with the federation bridge."""
        # Integrate local hub
        local_queue = await self.bridge.integrate_hub(self.local_hub)
        assert local_queue.hub_id == "local_test"
        assert "local_test" in self.bridge.hub_queues

        # Integrate regional hub
        regional_queue = await self.bridge.integrate_hub(self.regional_hub)
        assert regional_queue.hub_id == "regional_test"

        # Regional hub should be registered as coordinator
        assert "us_west" in self.bridge.cross_region_coordinator.region_coordinators
        assert (
            self.bridge.cross_region_coordinator.region_coordinators["us_west"]
            == "regional_test"
        )

    @pytest.mark.asyncio
    async def test_federated_message_sending(self):
        """Test sending messages through the federated system."""
        # Integrate hubs
        await self.bridge.integrate_hub(self.local_hub)
        await self.bridge.integrate_hub(self.regional_hub)

        # Create test message
        message = BlockchainMessage(
            sender_id="local_client",
            recipient_id="regional_client",
            message_type="federated_test",
            priority=MessagePriority.NORMAL,
            processing_fee=100,
            payload=b"federated message test",
        )

        # Send through federation
        success = await self.bridge.send_federated_message(
            "local_test", message, "regional_test"
        )

        # Should succeed since both hubs are in same region
        assert success is True

    @pytest.mark.asyncio
    async def test_cross_region_federated_message(self):
        """Test cross-region federated message delivery."""
        # Create hubs in different regions
        east_regional = RegionalHub(
            hub_id="east_regional",
            hub_tier=HubTier.REGIONAL,
            capabilities=HubCapabilities(),
            coordinates=GeographicCoordinate(40.7128, -74.0060),
            region="us_east",
        )

        # Integrate hubs
        await self.bridge.integrate_hub(self.regional_hub)  # us_west
        await self.bridge.integrate_hub(east_regional)  # us_east

        # Create cross-region message
        message = BlockchainMessage(
            sender_id="west_client",
            recipient_id="east_client",
            priority=MessagePriority.HIGH,
            processing_fee=200,
            payload=b"cross-region test",
        )

        # Send cross-region
        success = await self.bridge.send_federated_message(
            "regional_test", message, "east_regional"
        )

        assert success is True

    @pytest.mark.asyncio
    async def test_federation_policy_proposal(self):
        """Test proposing federation-wide policies."""
        # Integrate a hub first
        await self.bridge.integrate_hub(self.local_hub)

        # Propose routing policy
        from mpreg.fabric.federation_types import FederationPolicySpec

        policy_spec = FederationPolicySpec(
            policy_name="Federation Test Policy",
            policy_type="routing",
            description="Test policy for federation",
        )

        proposal_id = await self.bridge.propose_federation_policy(
            "bridge_admin", policy_spec
        )

        assert proposal_id in self.bridge.route_manager.global_governance.dao.proposals

    def test_federation_status(self):
        """Test getting comprehensive federation status."""
        # Add some test data
        self.bridge.hub_queues["test_hub"] = HubMessageQueue(
            self.local_hub, self.global_dao
        )

        status = self.bridge.get_federation_status()

        assert "total_hubs" in status
        assert "global_dao_members" in status
        assert "active_global_policies" in status
        assert "cross_region_performance" in status
        assert "hub_metrics" in status

        assert status["total_hubs"] >= 1
        assert status["global_dao_members"] == len(self.global_dao.members)


class TestFederationMessageRoute:
    """Test federation-specific message routing."""

    def test_federation_route_creation(self):
        """Test creation of federation message routes."""
        from mpreg.core.blockchain_message_queue_types import MessageRoute, RouteStatus

        base_route = MessageRoute(
            route_id="test_federation_route",
            source_hub="hub_a",
            destination_hub="hub_b",
            latency_ms=75,
            reliability_score=0.95,
            cost_per_mb=10,
            status=RouteStatus.ACTIVE,
        )

        federation_route = FederationMessageRoute(
            base_route=base_route,
            source_hub_id="hub_a",
            destination_hub_id="hub_b",
            hub_path=["hub_a", "regional_hub", "hub_b"],
            geographic_distance_km=500.0,
            estimated_hops=2,
            cross_region=True,
            sla_tier=1,
        )

        assert federation_route.route_id == "test_federation_route"
        assert federation_route.estimated_hops == 2
        assert federation_route.cross_region is True
        assert federation_route.total_cost_score > 0  # Should calculate cost

    def test_dao_preferred_route_discount(self):
        """Test DAO preference affecting route cost."""
        from mpreg.core.blockchain_message_queue_types import MessageRoute, RouteStatus

        base_route = MessageRoute(
            route_id="preferred_route",
            source_hub="hub_a",
            destination_hub="hub_b",
            cost_per_mb=10,
            status=RouteStatus.ACTIVE,
        )

        # Non-preferred route
        normal_route = FederationMessageRoute(
            base_route=base_route,
            source_hub_id="hub_a",
            destination_hub_id="hub_b",
            geographic_distance_km=100.0,
            estimated_hops=1,
            preferred_by_dao=False,
        )

        # DAO-preferred route
        preferred_route = FederationMessageRoute(
            base_route=base_route,
            source_hub_id="hub_a",
            destination_hub_id="hub_b",
            geographic_distance_km=100.0,
            estimated_hops=1,
            preferred_by_dao=True,
        )

        # Preferred route should have lower cost
        assert preferred_route.total_cost_score < normal_route.total_cost_score


if __name__ == "__main__":
    pytest.main([__file__])
