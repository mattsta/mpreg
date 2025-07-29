"""
Blockchain-backed message queue integration with MPREG federation.

Refactored version using proper dataclasses and semantic type aliases
for type safety and maintainability.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from ..core.blockchain_message_queue import (
    BlockchainMessageQueue,
    MessageQueueGovernance,
)
from ..core.blockchain_message_queue_types import (
    BlockchainMessage,
    DeliveryGuarantee,
    MessagePriority,
    MessageRoute,
    QueueGovernancePolicy,
    QueueMetrics,
    RouteStatus,
)
from ..datastructures import (
    Blockchain,
    DecentralizedAutonomousOrganization,
)
from ..datastructures.type_aliases import (
    FederationRouteId,
    HubId,
    ProposalId,
    RegionName,
)
from .federation_hubs import FederationHub, HubTier
from .federation_types import (
    CrossRegionDeliveryRequest,
    CrossRegionPerformanceMetrics,
    FederationMessageRoute,
    FederationPolicySpec,
    HubPerformanceMetrics,
    MutableCrossRegionState,
    MutableHubState,
)


class HubMessageQueue:
    """Hub-specific blockchain message queue with federation integration."""

    def __init__(
        self,
        hub: FederationHub,
        dao: DecentralizedAutonomousOrganization,
        blockchain: Blockchain | None = None,
    ):
        self.hub = hub
        self.hub_id: HubId = hub.hub_id
        self.hub_tier = hub.hub_tier

        # Create hub-specific blockchain if not provided
        if blockchain is None:
            blockchain = Blockchain.create_new_chain(
                chain_id=f"hub_{self.hub_id}",
                genesis_miner=f"hub_{self.hub_id}_genesis",
            )

        # Initialize blockchain message queue
        self.queue = BlockchainMessageQueue(
            queue_id=f"hub_queue_{self.hub_id}", blockchain=blockchain
        )

        # Add hub operator to governance
        self.queue.add_governance_member(
            member_id=f"hub_operator_{self.hub_id}",
            voting_power=1000,
            token_balance=10000,
        )

        # Federation-specific state using proper datastructures
        self.hub_state = MutableHubState(hub_id=self.hub_id)
        self.cross_region_routes: dict[FederationRouteId, FederationMessageRoute] = {}

    async def process_federation_message(
        self, message: BlockchainMessage, destination_hub: HubId
    ) -> FederationMessageRoute | None:
        """Process message for federation routing."""

        try:
            # Enhanced message with federation metadata
            federation_message = self._create_federation_message(
                message, destination_hub
            )

            # Submit to blockchain queue
            success = self.queue.submit_message(federation_message)

            if success:
                # Find optimal federation route
                route = await self._find_federation_route(
                    destination_hub, message.priority
                )

                # Record federation metrics
                self._record_federation_metrics(message, route)

                return route

            return None

        except Exception as e:
            logger.error(f"Hub {self.hub_id} federation message processing failed: {e}")
            return None

    def _create_federation_message(
        self, message: BlockchainMessage, destination_hub: HubId
    ) -> BlockchainMessage:
        """Create enhanced message with federation metadata."""
        return BlockchainMessage(
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            message_type="federation_routed",
            priority=message.priority,
            delivery_guarantee=message.delivery_guarantee,
            payload=message.payload,
            processing_fee=message.processing_fee,
            metadata={
                **message.metadata,
                "source_hub": self.hub_id,
                "destination_hub": destination_hub,
                "hub_tier": self.hub_tier.value,
                "federation_routing": True,
            },
        )

    async def _find_federation_route(
        self, destination_hub: HubId, priority: MessagePriority
    ) -> FederationMessageRoute | None:
        """Find optimal route through federation infrastructure."""

        # Check direct connection first
        if destination_hub in self.hub_state.connected_hubs:
            return self._create_direct_route(destination_hub, priority)

        # Find path through hub hierarchy
        return await self._find_hierarchical_route(destination_hub, priority)

    def _create_direct_route(
        self, destination_hub: HubId, priority: MessagePriority
    ) -> FederationMessageRoute:
        """Create direct route between hubs."""

        base_route = MessageRoute(
            route_id=f"direct_{self.hub_id}_{destination_hub}",
            source_hub=self.hub_id,
            destination_hub=destination_hub,
            latency_ms=50,  # Direct connection baseline
            reliability_score=0.95,
            cost_per_mb=5,
            status=RouteStatus.ACTIVE,
        )

        return FederationMessageRoute(
            base_route=base_route,
            source_hub_id=self.hub_id,
            destination_hub_id=destination_hub,
            hub_path=[self.hub_id, destination_hub],
            estimated_hops=1,
            cross_region=False,
            sla_tier=1 if priority == MessagePriority.EMERGENCY else 2,
        )

    async def _find_hierarchical_route(
        self, destination_hub: HubId, priority: MessagePriority
    ) -> FederationMessageRoute | None:
        """Find route through hub hierarchy."""

        hub_path = [self.hub_id]

        # Route up hierarchy based on tier
        if self.hub_tier == HubTier.LOCAL:
            hub_path.append(f"regional_{self.hub.region}")

        if self.hub_tier in [HubTier.LOCAL, HubTier.REGIONAL]:
            hub_path.append("global_hub")

        # Route down to destination
        hub_path.append(destination_hub)

        base_route = MessageRoute(
            route_id=f"hierarchical_{self.hub_id}_{destination_hub}",
            source_hub=self.hub_id,
            destination_hub=destination_hub,
            latency_ms=len(hub_path) * 25,  # 25ms per hop
            reliability_score=0.92,
            cost_per_mb=len(hub_path) * 2,
            status=RouteStatus.ACTIVE,
        )

        return FederationMessageRoute(
            base_route=base_route,
            source_hub_id=self.hub_id,
            destination_hub_id=destination_hub,
            hub_path=hub_path,
            estimated_hops=len(hub_path) - 1,
            cross_region=True,
            sla_tier=1
            if priority == MessagePriority.EMERGENCY
            else (2 if priority == MessagePriority.HIGH else 3),
        )

    def _record_federation_metrics(
        self, message: BlockchainMessage, route: FederationMessageRoute | None
    ) -> None:
        """Record performance metrics for federation routing."""

        if route:
            # Create structured performance metrics
            metrics = HubPerformanceMetrics(
                hub_id=self.hub_id,
                messages_routed=1,
                total_hops_served=route.estimated_hops,
                cross_region_messages=1 if route.cross_region else 0,
                connected_hubs_count=len(self.hub_state.connected_hubs),
                total_fees_collected=message.processing_fee,
            )

            # Add to hub state
            self.hub_state.add_performance_metrics(metrics)

            # Create blockchain metrics
            queue_metrics = QueueMetrics(
                queue_id=self.queue.queue_id,
                route_id=route.route_id,
                messages_processed=1,
                average_latency_ms=route.base_route.latency_ms,
                total_fees_collected=message.processing_fee,
                delivery_success_rate=route.base_route.reliability_score,
            )

            # Record on blockchain
            self.queue.router.record_performance_metrics(queue_metrics)
        else:
            # Record failed routing
            failed_metrics = HubPerformanceMetrics(
                hub_id=self.hub_id, messages_failed=1
            )
            self.hub_state.add_performance_metrics(failed_metrics)

    def get_hub_status(self) -> HubPerformanceMetrics:
        """Get current hub performance status."""
        if not self.hub_state.recent_metrics:
            return HubPerformanceMetrics(hub_id=self.hub_id)

        # Aggregate recent metrics
        recent = self.hub_state.recent_metrics[-10:]  # Last 10 measurements

        return HubPerformanceMetrics(
            hub_id=self.hub_id,
            messages_routed=sum(m.messages_routed for m in recent),
            messages_failed=sum(m.messages_failed for m in recent),
            total_hops_served=sum(m.total_hops_served for m in recent),
            cross_region_messages=sum(m.cross_region_messages for m in recent),
            current_queue_depth=len(self.queue.priority_queue.message_queue),
            connected_hubs_count=len(self.hub_state.connected_hubs),
            active_routes_count=len(self.cross_region_routes),
            total_fees_collected=sum(m.total_fees_collected for m in recent),
        )


class FederationRouteManager:
    """Manages routing policies across the federation using DAO governance."""

    def __init__(self, global_dao: DecentralizedAutonomousOrganization):
        self.global_dao = global_dao
        self.global_governance = MessageQueueGovernance(global_dao)
        self.hub_queues: dict[HubId, HubMessageQueue] = {}
        self.active_policies: dict[str, FederationPolicySpec] = {}

    def register_hub_queue(self, hub_queue: HubMessageQueue) -> None:
        """Register a hub's message queue with the federation."""
        self.hub_queues[hub_queue.hub_id] = hub_queue

        # Connect hub to route manager
        hub_queue.hub_state.add_connected_hub(hub_queue.hub_id)

        # Sync governance policies
        self._sync_governance_policies(hub_queue)

    async def propose_global_routing_policy(
        self, proposer_id: str, policy_spec: FederationPolicySpec | dict
    ) -> ProposalId:
        """Propose routing policy that affects the entire federation."""

        # Handle both FederationPolicySpec and dict inputs for backward compatibility
        if isinstance(policy_spec, dict):
            policy_dict = policy_spec.copy()
            policy_dict.update(
                {
                    "democratic_routing": True,
                    "scope": policy_dict.get("scope", "global_federation"),
                }
            )
        else:
            # Convert policy spec to proposal format
            policy_dict = {
                "name": policy_spec.policy_name,
                "description": policy_spec.description,
                "scope": policy_spec.scope,
                "affected_hubs": list(policy_spec.affected_hubs),
                "affected_regions": list(policy_spec.affected_regions),
                "emergency_latency_max_ms": policy_spec.emergency_latency_max_ms,
                "cost_optimization_enabled": policy_spec.cost_optimization_enabled,
                "democratic_routing": True,
            }

        # Enhance policy dict with federation metadata before proposing
        enhanced_policy = policy_dict.copy()
        enhanced_policy.update(
            {
                "affected_hubs": list(self.hub_queues.keys()),
                "scope": "global_federation",
            }
        )

        proposal_id = self.global_governance.propose_routing_policy(
            proposer_id, enhanced_policy
        )

        # Store policy spec for later execution (only if it's a FederationPolicySpec)
        if isinstance(policy_spec, FederationPolicySpec):
            self.active_policies[proposal_id] = policy_spec

        logger.info(f"Global federation routing policy proposed: {proposal_id}")
        return proposal_id

    async def execute_global_policy(self, proposal_id: ProposalId) -> None:
        """Execute approved global routing policy across all hubs."""

        try:
            policy = self.global_governance.execute_governance_decision(proposal_id)
            policy_spec = self.active_policies.get(proposal_id)

            # Apply policy to all hubs (both with and without stored policy_spec)
            for hub_id, hub_queue in self.hub_queues.items():
                if (
                    not policy_spec
                    or not policy_spec.affected_hubs
                    or hub_id in policy_spec.affected_hubs
                ):
                    await self._apply_policy_to_hub(hub_queue, policy, policy_spec)

            logger.info(
                f"Global policy {proposal_id} applied to {len(self.hub_queues)} hubs"
            )

        except Exception as e:
            logger.error(f"Failed to execute global policy {proposal_id}: {e}")

    async def _apply_policy_to_hub(
        self,
        hub_queue: HubMessageQueue,
        policy: QueueGovernancePolicy,
        policy_spec: FederationPolicySpec | None,
    ) -> None:
        """Apply global policy to a specific hub."""

        # Add policy to hub's governance
        hub_queue.queue.governance.active_policies[policy.policy_id] = policy
        hub_queue.hub_state.active_policy_ids.add(policy.policy_id)

        # Update hub routing behavior based on policy type
        if policy_spec and policy_spec.policy_type == "routing":
            await self._update_hub_routing(hub_queue, policy_spec)
        elif policy_spec and policy_spec.policy_type == "fees":
            await self._update_hub_fees(hub_queue, policy_spec)
        elif policy.policy_type == "routing":
            # Fallback to policy.policy_type when policy_spec is None
            await self._update_hub_routing_fallback(hub_queue, policy)

    async def _update_hub_routing(
        self, hub_queue: HubMessageQueue, policy_spec: FederationPolicySpec
    ) -> None:
        """Update hub routing based on DAO policy."""

        # Update latency requirements for different priorities
        hub_queue.hub_state.update_health_score(1.0)  # Policy compliance

        logger.info(f"Updated routing policy for hub {hub_queue.hub_id}")

    async def _update_hub_routing_fallback(
        self, hub_queue: HubMessageQueue, policy: QueueGovernancePolicy
    ) -> None:
        """Update hub routing based on DAO policy (fallback without policy_spec)."""

        # Update latency requirements for different priorities
        hub_queue.hub_state.update_health_score(1.0)  # Policy compliance

        logger.info(f"Updated routing policy for hub {hub_queue.hub_id} (fallback)")

    async def _update_hub_fees(
        self, hub_queue: HubMessageQueue, policy_spec: FederationPolicySpec
    ) -> None:
        """Update hub fee structures based on DAO policy."""

        logger.info(f"Updated fee structure for hub {hub_queue.hub_id}")

    def _sync_governance_policies(self, hub_queue: HubMessageQueue) -> None:
        """Sync global governance policies to hub queue."""

        # Copy active global policies to hub
        for policy in self.global_governance.get_active_policies():
            hub_queue.queue.governance.active_policies[policy.policy_id] = policy
            hub_queue.hub_state.active_policy_ids.add(policy.policy_id)

        logger.info(
            f"Synced {len(self.global_governance.active_policies)} policies to hub {hub_queue.hub_id}"
        )


class CrossRegionCoordinator:
    """Coordinates message delivery across geographic regions."""

    def __init__(self, route_manager: FederationRouteManager):
        self.route_manager = route_manager
        self.cross_region_state = MutableCrossRegionState()

        # Backward compatibility properties
        self.region_coordinators = self.cross_region_state.region_coordinators
        self.cross_region_metrics = self.cross_region_state.route_metrics

    async def coordinate_cross_region_delivery(
        self,
        message: BlockchainMessage,
        source_region: RegionName,
        destination_region: RegionName,
    ) -> bool:
        """Coordinate message delivery across regions."""

        try:
            # Create delivery request
            delivery_request = CrossRegionDeliveryRequest(
                message_id=f"cross_region_{message.message_id}",
                original_message_id=message.message_id,
                source_region=source_region,
                destination_region=destination_region,
                source_hub_id="",  # Will be filled by coordinator
                destination_hub_id="",  # Will be filled by coordinator
                priority_level=self._get_priority_level(message.priority),
                processing_fee=message.processing_fee,
                coordination_fee=message.processing_fee,  # Additional fee for coordination
                requires_exactly_once=message.delivery_guarantee
                == DeliveryGuarantee.EXACTLY_ONCE,
            )

            # Find regional coordinators
            source_coordinator = self.cross_region_state.region_coordinators.get(
                source_region
            )
            destination_coordinator = self.cross_region_state.region_coordinators.get(
                destination_region
            )

            if not source_coordinator or not destination_coordinator:
                logger.warning(
                    f"Missing coordinators for {source_region} -> {destination_region}"
                )
                return False

            # Process coordination
            success = await self._process_coordination(
                delivery_request, source_coordinator, destination_coordinator
            )

            if success:
                # Record metrics
                self._record_cross_region_metrics(
                    source_region, destination_region, delivery_request
                )

            return success

        except Exception as e:
            logger.error(
                f"Cross-region coordination failed {source_region} -> {destination_region}: {e}"
            )
            return False

    def _get_priority_level(self, priority: MessagePriority) -> int:
        """Convert message priority to numeric level."""
        priority_map = {
            MessagePriority.EMERGENCY: 1,
            MessagePriority.HIGH: 2,
            MessagePriority.NORMAL: 3,
            MessagePriority.LOW: 4,
            MessagePriority.BULK: 5,
        }
        return priority_map.get(priority, 3)

    async def _process_coordination(
        self,
        request: CrossRegionDeliveryRequest,
        source_coordinator: HubId,
        destination_coordinator: HubId,
    ) -> bool:
        """Process the cross-region coordination request."""

        # Submit to source coordinator
        source_hub_queue = self.route_manager.hub_queues.get(source_coordinator)
        if not source_hub_queue:
            return False

        # Create coordination message
        coordination_message = BlockchainMessage(
            message_id=request.message_id,
            sender_id="cross_region_coordinator",
            recipient_id=destination_coordinator,
            message_type="cross_region_coordinated",
            priority=MessagePriority.HIGH,
            delivery_guarantee=DeliveryGuarantee.EXACTLY_ONCE
            if request.requires_exactly_once
            else DeliveryGuarantee.AT_LEAST_ONCE,
            processing_fee=request.coordination_fee,
            metadata={
                "original_message_id": request.original_message_id,
                "source_region": request.source_region,
                "destination_region": request.destination_region,
                "coordination_required": True,
            },
        )

        # Process through federation
        route = await source_hub_queue.process_federation_message(
            coordination_message, destination_coordinator
        )

        return route is not None

    def register_regional_coordinator(self, region: RegionName, hub_id: HubId) -> None:
        """Register a hub as the coordinator for a region."""
        self.cross_region_state.register_coordinator(region, hub_id)
        logger.info(f"Registered {hub_id} as coordinator for region {region}")

    def _record_cross_region_metrics(
        self,
        source_region: RegionName,
        destination_region: RegionName,
        request: CrossRegionDeliveryRequest,
    ) -> None:
        """Record metrics for cross-region message delivery."""

        route_key = f"{source_region}_{destination_region}"

        metrics = CrossRegionPerformanceMetrics(
            route_key=route_key,
            messages_delivered=1,
            total_delivery_time_ms=50.0,  # Estimated
            total_cost=float(request.processing_fee + request.coordination_fee),
            total_hops=3,  # Estimated cross-region hops
            coordination_attempts=1,
        )

        self.cross_region_state.add_delivery_metrics(route_key, metrics)

    def get_cross_region_performance(self) -> dict[str, dict[str, float]]:
        """Get cross-region performance metrics."""
        performance = {}
        for route_key, metrics in self.cross_region_state.route_metrics.items():
            performance[route_key] = {
                "messages_delivered": metrics.messages_delivered,
                "average_latency_ms": metrics.average_delivery_time_ms,
                "average_cost": metrics.average_cost,
                "average_hops": metrics.average_hops,
            }
        return performance


@dataclass(slots=True)
class BlockchainFederationBridge:
    """Main integration bridge connecting blockchain message queues with federation."""

    global_dao: DecentralizedAutonomousOrganization
    route_manager: FederationRouteManager = field(init=False)
    cross_region_coordinator: CrossRegionCoordinator = field(init=False)
    hub_queues: dict[HubId, HubMessageQueue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize federation bridge components."""
        self.route_manager = FederationRouteManager(self.global_dao)
        self.cross_region_coordinator = CrossRegionCoordinator(self.route_manager)

    async def integrate_hub(
        self, hub: FederationHub, blockchain: Blockchain | None = None
    ) -> HubMessageQueue:
        """Integrate a federation hub with blockchain message queue."""

        # Create hub message queue
        hub_queue = HubMessageQueue(hub, self.global_dao, blockchain)

        # Register with federation
        self.route_manager.register_hub_queue(hub_queue)
        self.hub_queues[hub.hub_id] = hub_queue

        # Set up regional coordination if needed
        if hub.hub_tier == HubTier.REGIONAL:
            self.cross_region_coordinator.register_regional_coordinator(
                hub.region, hub.hub_id
            )

        logger.info(
            f"Integrated hub {hub.hub_id} ({hub.hub_tier.value}) with blockchain message queue"
        )
        return hub_queue

    async def send_federated_message(
        self,
        sender_hub_id: HubId,
        message: BlockchainMessage,
        destination_hub_id: HubId,
    ) -> bool:
        """Send message through the federated blockchain queue system."""

        sender_hub = self.hub_queues.get(sender_hub_id)
        if not sender_hub:
            logger.error(f"Sender hub {sender_hub_id} not found")
            return False

        try:
            # Process through federation
            route = await sender_hub.process_federation_message(
                message, destination_hub_id
            )

            if route and route.cross_region:
                # Handle cross-region coordination
                sender_region = sender_hub.hub.region
                dest_hub = self.hub_queues.get(destination_hub_id)
                dest_region = dest_hub.hub.region if dest_hub else "unknown"

                return await self.cross_region_coordinator.coordinate_cross_region_delivery(
                    message, sender_region, dest_region
                )

            return route is not None

        except Exception as e:
            logger.error(
                f"Federated message send failed {sender_hub_id} -> {destination_hub_id}: {e}"
            )
            return False

    def get_federation_status(self) -> dict[str, Any]:
        """Get comprehensive federation status."""

        # Calculate hub statistics
        local_hubs = sum(
            1 for h in self.hub_queues.values() if h.hub_tier == HubTier.LOCAL
        )
        regional_hubs = sum(
            1 for h in self.hub_queues.values() if h.hub_tier == HubTier.REGIONAL
        )
        global_hubs = sum(
            1 for h in self.hub_queues.values() if h.hub_tier == HubTier.GLOBAL
        )

        # Calculate performance statistics
        all_metrics = []
        for hub_queue in self.hub_queues.values():
            if hub_queue.hub_state.recent_metrics:
                all_metrics.extend(hub_queue.hub_state.recent_metrics)

        total_messages = sum(m.messages_routed + m.messages_failed for m in all_metrics)
        total_successful = sum(m.messages_routed for m in all_metrics)

        return {
            "total_hubs": len(self.hub_queues),
            "local_hubs": local_hubs,
            "regional_hubs": regional_hubs,
            "global_hubs": global_hubs,
            "active_hubs": len(
                [h for h in self.hub_queues.values() if h.hub_state.health_score > 0.5]
            ),
            "global_dao_members": len(self.global_dao.members),
            "active_global_policies": len(
                self.route_manager.global_governance.active_policies
            ),
            "total_messages_processed": total_messages,
            "overall_success_rate": total_successful / total_messages
            if total_messages > 0
            else 1.0,
            "total_active_routes": sum(
                len(h.cross_region_routes) for h in self.hub_queues.values()
            ),
            "cross_region_performance": self.cross_region_coordinator.get_cross_region_performance(),
            "hub_metrics": {
                hub_id: hub.get_hub_status() for hub_id, hub in self.hub_queues.items()
            },
        }

    async def propose_federation_policy(
        self, proposer_id: str, policy_spec: FederationPolicySpec
    ) -> ProposalId:
        """Propose a new federation-wide policy through DAO governance."""

        return await self.route_manager.propose_global_routing_policy(
            proposer_id, policy_spec
        )

    async def execute_federation_policy(self, proposal_id: ProposalId) -> None:
        """Execute approved federation policy across all hubs."""
        await self.route_manager.execute_global_policy(proposal_id)
