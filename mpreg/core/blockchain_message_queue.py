"""
Blockchain-backed message queue implementation with DAO governance.

Provides democratic message queue with equitable priority algorithms,
blockchain audit trails, and federated routing capabilities.
"""

from __future__ import annotations

import json
import time
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Any

from ..datastructures import (
    Block,
    Blockchain,
    ConsensusConfig,
    ConsensusType,
    DaoProposal,
    DecentralizedAutonomousOrganization,
    OperationType,
    ProposalType,
    Transaction,
)
from ..datastructures.blockchain_crypto import TransactionSigner
from ..datastructures.blockchain_types import CryptoConfig
from .blockchain_ledger import BlockchainLedger
from .blockchain_message_queue_types import (
    BlockchainMessage,
    DeliveryGuarantee,
    MessagePriority,
    MessageQueueId,
    MessageRoute,
    PolicyMetadata,
    PolicyParameters,
    QueueGovernancePolicy,
    QueueMetrics,
    RouteId,
    RouteStatus,
    RoutingCriteria,
)


class MessageQueueGovernance:
    """DAO governance for message queue operations."""

    def __init__(
        self,
        dao: DecentralizedAutonomousOrganization,
        *,
        ledger: BlockchainLedger | None = None,
        signer: TransactionSigner | None = None,
    ):
        self.ledger = ledger or BlockchainLedger(dao.blockchain)
        if dao.blockchain != self.ledger.blockchain:
            dao = replace(dao, blockchain=self.ledger.blockchain)
        self.dao = dao
        self.signer = signer
        self.active_policies: dict[str, QueueGovernancePolicy] = {}
        self.route_registry: dict[RouteId, MessageRoute] = {}
        self.performance_history: list[QueueMetrics] = []

    def _sync_dao_from_ledger(self) -> None:
        if self.dao.blockchain != self.ledger.blockchain:
            self.dao = replace(self.dao, blockchain=self.ledger.blockchain)

    def _sync_ledger_from_dao(self) -> None:
        if self.dao.blockchain != self.ledger.blockchain:
            self.ledger.replace(self.dao.blockchain)

    def _sign_transaction(self, transaction: Transaction) -> Transaction:
        if self.signer is None:
            if self.ledger.blockchain.crypto_config.require_signatures:
                raise ValueError("Signer required when signatures are enforced")
            return transaction
        return self.signer.sign(transaction)

    def propose_routing_policy(
        self, proposer_id: str, policy_spec: dict[str, Any]
    ) -> str:
        """Propose new routing policy through DAO governance."""
        self._sync_dao_from_ledger()

        proposal = DaoProposal(
            proposer_id=proposer_id,
            proposal_type=ProposalType.PARAMETER_CHANGE,
            title=f"Routing Policy: {policy_spec['name']}",
            description=f"Update message routing policy: {policy_spec['description']}",
            execution_data=json.dumps(policy_spec).encode(),
            metadata={
                "policy_type": "routing",
                "affects_routes": policy_spec.get("routes", []),
                "scope": policy_spec.get("scope", "local"),
                "affected_hubs": policy_spec.get("affected_hubs", []),
            },
        )

        self.dao = self.dao.create_proposal(proposer_id, proposal)
        self._sync_ledger_from_dao()
        return list(self.dao.proposals.keys())[-1]

    def propose_fee_structure(
        self, proposer_id: str, fee_structure: dict[str, Any]
    ) -> str:
        """Propose fee structure changes through DAO."""
        self._sync_dao_from_ledger()

        proposal = DaoProposal(
            proposer_id=proposer_id,
            proposal_type=ProposalType.PARAMETER_CHANGE,
            title=f"Fee Structure Update: {fee_structure['name']}",
            description=f"Update message processing fees: {fee_structure['description']}",
            execution_data=json.dumps(fee_structure).encode(),
            metadata={
                "policy_type": "fees",
                "progressive": fee_structure.get("progressive", True),
            },
        )

        self.dao = self.dao.create_proposal(proposer_id, proposal)
        self._sync_ledger_from_dao()
        return list(self.dao.proposals.keys())[-1]

    def propose_priority_algorithm(
        self, proposer_id: str, algorithm_spec: dict[str, Any]
    ) -> str:
        """Propose message prioritization algorithm."""
        self._sync_dao_from_ledger()

        proposal = DaoProposal(
            proposer_id=proposer_id,
            proposal_type=ProposalType.PARAMETER_CHANGE,
            title=f"Priority Algorithm: {algorithm_spec['name']}",
            description=f"Update message prioritization: {algorithm_spec['description']}",
            execution_data=json.dumps(algorithm_spec).encode(),
            metadata={
                "policy_type": "prioritization",
                "fairness_score": algorithm_spec.get("fairness", 0.8),
            },
        )

        self.dao = self.dao.create_proposal(proposer_id, proposal)
        self._sync_ledger_from_dao()
        return list(self.dao.proposals.keys())[-1]

    def execute_governance_decision(self, proposal_id: str) -> QueueGovernancePolicy:
        """Execute DAO decision to update queue governance."""

        self._sync_dao_from_ledger()
        if proposal_id not in self.dao.proposals:
            raise ValueError(f"Proposal {proposal_id} not found")

        proposal = self.dao.proposals[proposal_id]
        if proposal.status.value != "passed":
            raise ValueError("Only passed proposals can be executed")

        # Parse proposal execution data
        policy_spec = json.loads(proposal.execution_data.decode())

        # Create type-safe policy parameters
        parameters = PolicyParameters(
            emergency_latency_max_ms=policy_spec.get("emergency_latency_max_ms", 100.0),
            high_priority_latency_max_ms=policy_spec.get(
                "high_priority_latency_max_ms", 500.0
            ),
            normal_latency_max_ms=policy_spec.get("normal_latency_max_ms", 2000.0),
            cost_optimization_enabled=policy_spec.get(
                "cost_optimization_enabled", True
            ),
            preferred_routes=policy_spec.get("preferred_routes", []),
            custom_parameters={
                k: v
                for k, v in policy_spec.items()
                if k
                not in {
                    "name",
                    "description",
                    "emergency_latency_max_ms",
                    "high_priority_latency_max_ms",
                    "normal_latency_max_ms",
                    "cost_optimization_enabled",
                    "preferred_routes",
                }
            },
        )

        # Create type-safe policy metadata
        metadata = PolicyMetadata(
            scope=proposal.metadata.get("scope", "local"),
            policy_type=proposal.metadata.get("policy_type", "general"),
            affected_hubs=proposal.metadata.get("affected_hubs", []),
            affected_routes=proposal.metadata.get("affects_routes", []),
            fairness_score=proposal.metadata.get("fairness_score", 0.8),
            democratic_routing=policy_spec.get("democratic_routing", False),
            cross_region_impact=policy_spec.get("cross_region_impact", False),
            custom_metadata={
                k: v
                for k, v in proposal.metadata.items()
                if k
                not in {
                    "scope",
                    "policy_type",
                    "affected_hubs",
                    "affects_routes",
                    "fairness_score",
                }
            },
        )

        # Create governance policy
        policy = QueueGovernancePolicy(
            policy_name=policy_spec["name"],
            policy_type=proposal.metadata.get("policy_type", "general"),
            parameters=parameters,
            dao_proposal_id=proposal_id,
            created_by=proposal.proposer_id,
            approved_by_dao=True,
            effective_from=time.time(),
            metadata=metadata,
        )

        # Activate policy
        self.active_policies[policy.policy_id] = policy

        # Record on blockchain
        self._record_policy_change(policy)
        self._sync_dao_from_ledger()

        return policy

    def get_active_policies(
        self, policy_type: str | None = None
    ) -> list[QueueGovernancePolicy]:
        """Get currently active governance policies."""
        current_time = time.time()

        active = [
            policy
            for policy in self.active_policies.values()
            if policy.is_active(current_time)
        ]

        if policy_type:
            active = [p for p in active if p.policy_type == policy_type]

        return active

    def calculate_message_fee(self, message: BlockchainMessage) -> int:
        """Calculate processing fee based on DAO-governed fee structure."""

        fee_policies = self.get_active_policies("fees")
        base_fee = 1  # Default minimal fee

        for policy in fee_policies:
            fee_params = policy.parameters

            # Progressive fee structure - smaller messages pay relatively less
            size_factor = min(len(message.payload) / 1024, 100)  # Cap at 100KB
            priority_multiplier = {
                MessagePriority.EMERGENCY: fee_params.priority_fee_multiplier
                * 5.0,  # Emergency gets 5x higher
                MessagePriority.HIGH: fee_params.priority_fee_multiplier * 1.5,
                MessagePriority.NORMAL: fee_params.priority_fee_multiplier,
                MessagePriority.LOW: fee_params.priority_fee_multiplier * 0.5,
                MessagePriority.BULK: fee_params.priority_fee_multiplier * 0.1,
            }.get(message.priority, 1.0)

            # Guarantee multiplier
            guarantee_multiplier = {
                DeliveryGuarantee.AT_MOST_ONCE: 0.5,
                DeliveryGuarantee.AT_LEAST_ONCE: 1.0,
                DeliveryGuarantee.EXACTLY_ONCE: 2.0,
                DeliveryGuarantee.ORDERED: 1.5,
            }.get(message.delivery_guarantee, 1.0)

            calculated_fee = int(
                base_fee * size_factor * priority_multiplier * guarantee_multiplier
            )

            # Ensure minimum fee for sustainability
            min_fee = fee_params.base_fee_per_mb
            base_fee = max(calculated_fee, min_fee)

        return base_fee

    def _record_policy_change(self, policy: QueueGovernancePolicy) -> None:
        """Record policy change on blockchain."""
        policy_tx = Transaction(
            sender=policy.created_by or "governance",
            receiver="message_queue_governance",
            operation_type=OperationType.SMART_CONTRACT,
            payload=json.dumps(
                {
                    "action": "policy_change",
                    "policy_id": policy.policy_id,
                    "policy_name": policy.policy_name,
                    "policy_type": policy.policy_type,
                    "approved": policy.approved_by_dao,
                    "effective_from": policy.effective_from,
                }
            ).encode(),
            fee=0,
        )
        policy_tx = self._sign_transaction(policy_tx)
        new_block = Block.create_next_block(
            previous_block=self.ledger.blockchain.get_latest_block(),
            transactions=(policy_tx,),
            miner="queue_governance",
        )
        self.ledger.add_block(new_block)


class EquitablePriorityQueue:
    """Priority queue with fairness guarantees governed by DAO."""

    def __init__(self, governance: MessageQueueGovernance):
        self.governance = governance
        self.message_queue: list[BlockchainMessage] = []
        self.sender_quotas: dict[str, int] = {}
        self.last_quota_reset: float = time.time()
        self.recent_senders: deque[str] = deque(maxlen=20)

    def enqueue(self, message: BlockchainMessage) -> BlockchainMessage:
        """Add message to queue with fairness checks."""

        # Check sender quotas to prevent spam/monopolization
        if not self._check_sender_quota(message.sender_id):
            raise ValueError(f"Sender {message.sender_id} exceeded quota")

        # Calculate fee based on DAO governance
        calculated_fee = self.governance.calculate_message_fee(message)
        if message.processing_fee < calculated_fee:
            raise ValueError(
                f"Insufficient fee: {message.processing_fee} < {calculated_fee}"
            )

        # Update message with calculated fee
        updated_message = BlockchainMessage(
            message_id=message.message_id,
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            message_type=message.message_type,
            priority=message.priority,
            delivery_guarantee=message.delivery_guarantee,
            payload=message.payload,
            route_id=message.route_id,
            processing_fee=calculated_fee,
            created_at=message.created_at,
            expires_at=message.expires_at,
            retry_count=message.retry_count,
            max_retries=message.max_retries,
            blockchain_record=message.blockchain_record,
            metadata=message.metadata,
        )

        # Insert with priority order but fairness consideration
        self._insert_with_fairness(updated_message)

        # Update sender quota
        self._update_sender_quota(message.sender_id)

        return updated_message

    def dequeue(self) -> BlockchainMessage | None:
        """Remove highest priority message considering fairness."""
        if not self.message_queue:
            return None

        # Reset quotas periodically
        self._maybe_reset_quotas()

        # Get next message with fairness consideration
        return self._dequeue_with_fairness()

    def _check_sender_quota(self, sender_id: str) -> bool:
        """Check if sender is within quota limits."""
        priority_policies = self.governance.get_active_policies("prioritization")

        for policy in priority_policies:
            params = policy.parameters
            max_messages_param = params.get_parameter("max_messages_per_sender")
            max_messages_per_window = (
                int(max_messages_param)
                if isinstance(max_messages_param, int | float)
                else 100
            )

            current_quota = self.sender_quotas.get(sender_id, 0)
            return current_quota < max_messages_per_window

        return True  # No quota policy active

    def _insert_with_fairness(self, message: BlockchainMessage) -> None:
        """Insert message maintaining priority order with fairness."""

        # Calculate fairness-adjusted priority
        adjusted_priority = self._calculate_adjusted_priority(message)

        # Insert maintaining priority order
        inserted = False
        for i, existing_msg in enumerate(self.message_queue):
            existing_priority = self._calculate_adjusted_priority(existing_msg)

            if adjusted_priority > existing_priority:
                self.message_queue.insert(i, message)
                inserted = True
                break

        if not inserted:
            self.message_queue.append(message)

    def _calculate_adjusted_priority(self, message: BlockchainMessage) -> float:
        """Calculate priority adjusted for fairness."""

        base_priority = {
            MessagePriority.EMERGENCY: 1000,
            MessagePriority.HIGH: 100,
            MessagePriority.NORMAL: 10,
            MessagePriority.LOW: 1,
            MessagePriority.BULK: 0.1,
        }.get(message.priority, 10)

        # Fairness adjustments based on DAO policies
        priority_policies = self.governance.get_active_policies("prioritization")

        for policy in priority_policies:
            params = policy.parameters

            # Anti-monopoly: reduce priority for high-volume senders
            sender_quota = self.sender_quotas.get(message.sender_id, 0)
            monopoly_penalty = min(sender_quota / 50, 0.5)  # Max 50% reduction

            # Age bonus: older messages get slight priority boost
            age_bonus = min(
                (time.time() - message.created_at) / 3600, 0.3
            )  # Max 30% bonus

            # Fee incentive: higher fees get priority (but capped for fairness)
            fee_bonus = min(message.processing_fee / 1000, 0.2)  # Max 20% bonus

            # Apply fairness adjustments
            fairness_multiplier = (
                (1.0 - monopoly_penalty)  # Reduce for high volume
                * (1.0 + age_bonus)  # Boost for waiting
                * (1.0 + fee_bonus)  # Small fee incentive
            )

            fairness_factor = params.get_parameter("fairness_factor")
            if isinstance(fairness_factor, int | float):
                base_priority *= fairness_multiplier * float(fairness_factor)
            else:
                base_priority *= fairness_multiplier

        return base_priority

    def _dequeue_with_fairness(self) -> BlockchainMessage:
        """Dequeue with additional fairness checks."""

        # Normally dequeue highest priority
        message = self.message_queue.pop(0)

        # Check for fairness violations
        priority_policies = self.governance.get_active_policies("prioritization")

        for policy in priority_policies:
            params = policy.parameters
            max_consecutive_param = params.get_parameter("max_consecutive_same_sender")
            max_consecutive_from_sender = (
                int(max_consecutive_param)
                if isinstance(max_consecutive_param, int | float)
                else 5
            )

            consecutive_count = 0
            for sender_id in reversed(self.recent_senders):
                if sender_id == message.sender_id:
                    consecutive_count += 1
                else:
                    break

            if consecutive_count >= max_consecutive_from_sender:
                for idx, candidate in enumerate(self.message_queue):
                    if candidate.sender_id != message.sender_id:
                        self.message_queue.insert(0, message)
                        message = self.message_queue.pop(idx + 1)
                        break

        self.recent_senders.append(message.sender_id)
        return message

    def _update_sender_quota(self, sender_id: str) -> None:
        """Update sender's quota usage."""
        self.sender_quotas[sender_id] = self.sender_quotas.get(sender_id, 0) + 1

    def _maybe_reset_quotas(self) -> None:
        """Reset quotas periodically."""
        current_time = time.time()
        quota_window = 3600  # 1 hour window

        if current_time - self.last_quota_reset > quota_window:
            self.sender_quotas.clear()
            self.last_quota_reset = current_time


class BlockchainMessageRouter:
    """Message router with blockchain audit trail."""

    def __init__(
        self,
        ledger: BlockchainLedger,
        governance: MessageQueueGovernance,
        *,
        signer: TransactionSigner | None = None,
    ):
        self.ledger = ledger
        self.governance = governance
        self.signer = signer
        self.active_routes: dict[RouteId, MessageRoute] = {}
        self.message_history: dict[str, list[str]] = {}  # message_id -> route_path

    def _sign_transaction(self, transaction: Transaction) -> Transaction:
        if self.signer is None:
            if self.ledger.blockchain.crypto_config.require_signatures:
                raise ValueError("Signer required when signatures are enforced")
            return transaction
        return self.signer.sign(transaction)

    @property
    def blockchain(self) -> Blockchain:
        return self.ledger.blockchain

    def register_route(self, route: MessageRoute, registrar_id: str) -> RouteId:
        """Register new route with blockchain record."""

        # Create route registration transaction
        registration_tx = Transaction(
            sender=registrar_id,
            receiver="message_queue_system",
            operation_type=OperationType.FEDERATION_JOIN,
            payload=json.dumps(
                {
                    "action": "register_route",
                    "route_data": {
                        "route_id": route.route_id,
                        "source_hub": route.source_hub,
                        "destination_hub": route.destination_hub,
                        "path_hops": route.path_hops,
                        "latency_ms": route.latency_ms,
                        "bandwidth_mbps": route.bandwidth_mbps,
                        "reliability_score": route.reliability_score,
                        "cost_per_mb": route.cost_per_mb,
                    },
                }
            ).encode(),
            fee=10,
        )
        registration_tx = self._sign_transaction(registration_tx)

        # Add to blockchain
        new_block = Block.create_next_block(
            previous_block=self.ledger.blockchain.get_latest_block(),
            transactions=(registration_tx,),
            miner="message_queue_manager",
        )

        self.ledger.add_block(new_block)
        self.active_routes[route.route_id] = route

        return route.route_id

    def route_message(self, message: BlockchainMessage) -> MessageRoute:
        """Route message and record decision on blockchain."""

        # Find optimal route based on DAO policies
        optimal_route = self._find_optimal_route(message)

        if not optimal_route:
            raise ValueError("No suitable route found")

        # Record routing decision on blockchain
        routing_tx = Transaction(
            sender="message_queue_router",
            receiver=message.recipient_id,
            operation_type=OperationType.SMART_CONTRACT,
            payload=json.dumps(
                {
                    "action": "route_message",
                    "message_id": message.message_id,
                    "route_id": optimal_route.route_id,
                    "sender_id": message.sender_id,
                    "recipient_id": message.recipient_id,
                    "priority": message.priority.value,
                    "fee_paid": message.processing_fee,
                    "routing_timestamp": time.time(),
                }
            ).encode(),
            fee=message.processing_fee,
        )
        routing_tx = self._sign_transaction(routing_tx)

        # Add to blockchain
        new_block = Block.create_next_block(
            previous_block=self.ledger.blockchain.get_latest_block(),
            transactions=(routing_tx,),
            miner="message_queue_manager",
        )

        self.ledger.add_block(new_block)

        # Track message routing history
        if message.message_id not in self.message_history:
            self.message_history[message.message_id] = []
        self.message_history[message.message_id].append(optimal_route.route_id)

        return optimal_route

    def _find_optimal_route(self, message: BlockchainMessage) -> MessageRoute | None:
        """Find optimal route based on DAO governance policies."""

        routing_policies = self.governance.get_active_policies("routing")

        # Default routing criteria
        criteria = RoutingCriteria()

        # Override with DAO policies
        for policy in routing_policies:
            # Create updated criteria with policy parameters
            criteria = RoutingCriteria(
                latency_threshold_ms=policy.parameters.emergency_latency_max_ms,
                cost_optimization=policy.parameters.cost_optimization_enabled,
                preferred_routes=policy.parameters.preferred_routes,
                # Keep other defaults
                cost_threshold=criteria.cost_threshold,
                min_reliability=criteria.min_reliability,
                latency_weight=criteria.latency_weight,
                cost_weight=criteria.cost_weight,
                reliability_weight=criteria.reliability_weight,
                prefer_direct=criteria.prefer_direct,
            )

        # Filter applicable routes
        applicable_routes = []
        for route in self.active_routes.values():
            if route.status == RouteStatus.ACTIVE:
                # Check if route can handle message requirements
                if self._route_meets_requirements(route, message, criteria):
                    applicable_routes.append(route)

        if not applicable_routes:
            return None

        # Score routes based on criteria
        best_route = None
        best_score = -1.0

        for route in applicable_routes:
            score = self._calculate_route_score(route, message, criteria)
            if score > best_score:
                best_score = score
                best_route = route

        return best_route

    def _route_meets_requirements(
        self, route: MessageRoute, message: BlockchainMessage, criteria: RoutingCriteria
    ) -> bool:
        """Check if route meets message requirements."""

        # Basic checks
        if route.reliability_score < criteria.min_reliability:
            return False

        # Priority-based latency requirements
        max_latency = {
            MessagePriority.EMERGENCY: 100,  # 100ms max
            MessagePriority.HIGH: 500,  # 500ms max
            MessagePriority.NORMAL: 2000,  # 2s max
            MessagePriority.LOW: 10000,  # 10s max
            MessagePriority.BULK: 60000,  # 1 minute max
        }.get(message.priority, 2000)

        if route.latency_ms > max_latency:
            return False

        # Cost checks for fairness
        message_size_mb = len(message.payload) / (1024 * 1024)
        total_cost = route.cost_per_mb * message_size_mb

        # Ensure affordable routing for small messages
        return not (message_size_mb < 0.1 and total_cost > message.processing_fee * 0.5)

    def _calculate_route_score(
        self, route: MessageRoute, message: BlockchainMessage, criteria: RoutingCriteria
    ) -> float:
        """Calculate route fitness score using type-safe criteria."""

        # Use the criteria's built-in scoring method
        base_score = criteria.calculate_route_score(
            route.latency_ms, route.cost_per_mb, route.reliability_score
        )

        # Priority adjustments
        if message.priority == MessagePriority.EMERGENCY:
            base_score *= 1.5  # Emergency gets priority boost
        elif message.priority == MessagePriority.BULK:
            base_score *= 0.8  # Bulk gets lower priority

        # Preferred route bonus
        if route.route_id in criteria.preferred_routes:
            base_score *= 1.2

        total_score = base_score

        return total_score

    def record_performance_metrics(self, metrics: QueueMetrics) -> None:
        """Record queue performance metrics on blockchain."""

        metrics_tx = Transaction(
            sender="message_queue_monitor",
            receiver="message_queue_system",
            operation_type=OperationType.NODE_UPDATE,
            payload=json.dumps(
                {
                    "action": "record_metrics",
                    "metrics": {
                        "queue_id": metrics.queue_id,
                        "route_id": metrics.route_id,
                        "timestamp": metrics.timestamp,
                        "messages_processed": metrics.messages_processed,
                        "average_latency_ms": metrics.average_latency_ms,
                        "throughput_msgs_per_sec": metrics.throughput_msgs_per_sec,
                        "error_rate": metrics.error_rate,
                        "total_fees_collected": metrics.total_fees_collected,
                        "delivery_success_rate": metrics.delivery_success_rate,
                        "sla_compliance_rate": metrics.sla_compliance_rate,
                    },
                }
            ).encode(),
            fee=1,
        )
        metrics_tx = self._sign_transaction(metrics_tx)

        # Add to blockchain
        new_block = Block.create_next_block(
            previous_block=self.ledger.blockchain.get_latest_block(),
            transactions=(metrics_tx,),
            miner="message_queue_manager",
        )

        self.ledger.add_block(new_block)


@dataclass(frozen=True, slots=True)
class BlockchainMessageQueue:
    """Complete blockchain-backed message queue with DAO governance."""

    queue_id: MessageQueueId = field(
        default_factory=lambda: f"queue_{int(time.time())}"
    )
    ledger: BlockchainLedger = field(
        default_factory=lambda: BlockchainLedger(
            Blockchain.create_new_chain(
                chain_id="message_queue",
                genesis_miner="queue_genesis",
                consensus_config=ConsensusConfig(
                    consensus_type=ConsensusType.PROOF_OF_AUTHORITY,
                    block_time_target=10,  # 10-second blocks for messaging
                    difficulty_adjustment_interval=144,
                    max_transactions_per_block=1000,
                ),
                crypto_config=CryptoConfig(require_signatures=True),
            )
        )
    )
    signer: TransactionSigner | None = None
    governance: MessageQueueGovernance = field(init=False)
    priority_queue: EquitablePriorityQueue = field(init=False)
    router: BlockchainMessageRouter = field(init=False)
    created_at: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        """Initialize queue components."""
        # Use object.__setattr__ for frozen dataclass
        signer = self.signer or TransactionSigner.create()
        object.__setattr__(self, "signer", signer)

        # Create default DAO for governance
        default_dao = DecentralizedAutonomousOrganization(
            name=f"Message Queue {self.queue_id} DAO",
            description=f"Governance for message queue {self.queue_id}",
            blockchain=self.ledger.blockchain,
            signer=signer,
        )
        object.__setattr__(
            self,
            "governance",
            MessageQueueGovernance(default_dao, ledger=self.ledger, signer=self.signer),
        )
        object.__setattr__(
            self, "priority_queue", EquitablePriorityQueue(self.governance)
        )
        object.__setattr__(
            self,
            "router",
            BlockchainMessageRouter(self.ledger, self.governance, signer=self.signer),
        )

    @property
    def blockchain(self) -> Blockchain:
        return self.ledger.blockchain

    def submit_message(self, message: BlockchainMessage) -> bool:
        """Submit message to queue with full processing."""
        try:
            # Enqueue with fairness checks
            stored_message = self.priority_queue.enqueue(message)

            if stored_message.blockchain_record:
                # Record submission on blockchain
                self._record_message_submission(stored_message)

            return True
        except Exception as e:
            # Record failure for monitoring
            self._record_submission_failure(message, str(e))
            raise

    def process_next_message(self) -> BlockchainMessage | None:
        """Process next message from queue."""
        while True:
            message = self.priority_queue.dequeue()
            if message is None:
                return None
            if message.expires_at and message.expires_at < time.time():
                self._record_processing_failure(message, "expired")
                continue
            # Route the message
            try:
                route = self.router.route_message(message)

                # Record successful processing
                self._record_message_processing(message, route)

                return message
            except Exception as e:
                # Record processing failure
                self._record_processing_failure(message, str(e))

                # Re-queue for retry if within limits
                if message.retry_count < message.max_retries:
                    retry_message = BlockchainMessage(
                        message_id=message.message_id,
                        sender_id=message.sender_id,
                        recipient_id=message.recipient_id,
                        message_type=message.message_type,
                        priority=message.priority,
                        delivery_guarantee=message.delivery_guarantee,
                        payload=message.payload,
                        route_id=message.route_id,
                        processing_fee=message.processing_fee,
                        created_at=message.created_at,
                        expires_at=message.expires_at,
                        retry_count=message.retry_count + 1,
                        max_retries=message.max_retries,
                        blockchain_record=message.blockchain_record,
                        metadata=message.metadata,
                    )
                    if (
                        retry_message.expires_at is None
                        or retry_message.expires_at >= time.time()
                    ):
                        self.priority_queue.enqueue(retry_message)

                raise

    def get_queue_metrics(self) -> QueueMetrics:
        """Get current queue performance metrics."""
        current_time = time.time()

        return QueueMetrics(
            queue_id=self.queue_id,
            timestamp=current_time,
            queue_depth=len(self.priority_queue.message_queue),
            # Additional metrics would be calculated from historical data
        )

    def add_governance_member(
        self, member_id: str, voting_power: int, token_balance: int
    ) -> None:
        """Add member to queue governance DAO."""
        from ..datastructures import DaoMember

        member = DaoMember(
            member_id=member_id, voting_power=voting_power, token_balance=token_balance
        )

        self.governance.dao = self.governance.dao.add_member(member)
        self.governance.ledger.replace(self.governance.dao.blockchain)

    def _sign_transaction(self, transaction: Transaction) -> Transaction:
        if self.signer is None:
            if self.ledger.blockchain.crypto_config.require_signatures:
                raise ValueError("Signer required when signatures are enforced")
            return transaction
        return self.signer.sign(transaction)

    def _record_message_submission(self, message: BlockchainMessage) -> None:
        """Record message submission on blockchain."""
        submission_tx = Transaction(
            sender=message.sender_id,
            receiver=self.queue_id,
            operation_type=OperationType.SMART_CONTRACT,
            payload=json.dumps(
                {
                    "action": "submit_message",
                    "message_id": message.message_id,
                    "priority": message.priority.value,
                    "fee": message.processing_fee,
                    "timestamp": time.time(),
                }
            ).encode(),
            fee=1,
        )
        submission_tx = self._sign_transaction(submission_tx)

        new_block = Block.create_next_block(
            previous_block=self.ledger.blockchain.get_latest_block(),
            transactions=(submission_tx,),
            miner="queue_manager",
        )

        self.ledger.add_block(new_block)

    def _record_message_processing(
        self, message: BlockchainMessage, route: MessageRoute
    ) -> None:
        """Record successful message processing."""
        processing_tx = Transaction(
            sender=self.queue_id,
            receiver=message.recipient_id,
            operation_type=OperationType.SMART_CONTRACT,
            payload=json.dumps(
                {
                    "action": "process_message",
                    "message_id": message.message_id,
                    "route_id": route.route_id,
                    "processed_at": time.time(),
                }
            ).encode(),
            fee=0,
        )
        processing_tx = self._sign_transaction(processing_tx)

        new_block = Block.create_next_block(
            previous_block=self.ledger.blockchain.get_latest_block(),
            transactions=(processing_tx,),
            miner="queue_manager",
        )

        self.ledger.add_block(new_block)

    def _record_submission_failure(
        self, message: BlockchainMessage, error: str
    ) -> None:
        """Record message submission failure."""
        if not message.blockchain_record:
            return
        failure_tx = Transaction(
            sender=message.sender_id,
            receiver=self.queue_id,
            operation_type=OperationType.MESSAGE,
            payload=json.dumps(
                {
                    "action": "submit_failed",
                    "message_id": message.message_id,
                    "error": error,
                    "timestamp": time.time(),
                }
            ).encode(),
            fee=0,
        )
        failure_tx = self._sign_transaction(failure_tx)
        new_block = Block.create_next_block(
            previous_block=self.ledger.blockchain.get_latest_block(),
            transactions=(failure_tx,),
            miner="queue_manager",
        )
        self.ledger.add_block(new_block)

    def _record_processing_failure(
        self, message: BlockchainMessage, error: str
    ) -> None:
        """Record message processing failure."""
        if not message.blockchain_record:
            return
        failure_tx = Transaction(
            sender=self.queue_id,
            receiver=message.recipient_id,
            operation_type=OperationType.MESSAGE,
            payload=json.dumps(
                {
                    "action": "process_failed",
                    "message_id": message.message_id,
                    "error": error,
                    "timestamp": time.time(),
                }
            ).encode(),
            fee=0,
        )
        failure_tx = self._sign_transaction(failure_tx)
        new_block = Block.create_next_block(
            previous_block=self.ledger.blockchain.get_latest_block(),
            transactions=(failure_tx,),
            miner="queue_manager",
        )
        self.ledger.add_block(new_block)
