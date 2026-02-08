"""
Fabric routing helpers for unified message routing decisions.

This module replaces the previous unified routing layer with a fabric-native
router that uses the fabric message envelope, routing index, and planners.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from loguru import logger

from mpreg.core.topic_taxonomy import TopicValidator
from mpreg.datastructures.function_identity import FunctionSelector, VersionConstraint
from mpreg.datastructures.type_aliases import (
    ClusterId,
    HopCount,
    NodeId,
    PathLatencyMs,
    RouteCostScore,
)

from .catalog import CacheRole
from .engine import ClusterRoutePlan, ClusterRouteReason, RoutingEngine
from .index import CacheQuery, FunctionQuery, QueueQuery, RoutingIndex
from .message import (
    DeliveryGuarantee,
    MessageType,
    RoutingPriority,
    UnifiedMessage,
)
from .pubsub_router import PubSubRoutingPlanner
from .rpc_messages import FABRIC_RPC_REQUEST_KIND, FabricRPCRequest

type FabricRouteId = str
type TopicPattern = str
type RouteHandlerId = str
type RoutingPolicyId = str
type MessageCorrelationId = str

router_log = logger


class FabricRouteReason(Enum):
    LOCAL = "local"
    FEDERATED = "federated"
    NO_MATCH = "no_match"
    FALLBACK_LOCAL = "fallback_local"


@dataclass(frozen=True, slots=True)
class FabricRouteTarget:
    """Target destination for routing decisions."""

    system_type: MessageType
    target_id: str
    node_id: NodeId | None = None
    cluster_id: ClusterId | None = None
    priority_weight: float = 1.0


@dataclass(frozen=True, slots=True)
class FabricRouteResult:
    """Result of a fabric routing decision."""

    route_id: FabricRouteId
    targets: list[FabricRouteTarget]
    routing_path: list[str]
    federation_path: list[ClusterId]
    estimated_latency_ms: PathLatencyMs
    route_cost: RouteCostScore
    federation_required: bool
    hops_required: HopCount
    reason: FabricRouteReason
    cluster_routes: dict[ClusterId, ClusterRoutePlan] = field(default_factory=dict)

    @property
    def is_local_route(self) -> bool:
        return not self.federation_required and len(self.federation_path) <= 1

    @property
    def is_multi_target(self) -> bool:
        return len(self.targets) > 1


@dataclass(frozen=True, slots=True)
class FabricRoutingPolicy:
    """Policy configuration for routing decisions."""

    policy_id: RoutingPolicyId
    message_type_filter: set[MessageType] | None = None
    topic_pattern_filter: str | None = None
    priority_filter: set[RoutingPriority] | None = None

    prefer_local_routing: bool = True
    max_federation_hops: HopCount = 5
    enable_load_balancing: bool = True
    enable_geographic_optimization: bool = True

    max_latency_ms: PathLatencyMs = 10000.0
    max_route_cost: RouteCostScore = 100.0
    min_reliability_threshold: float = 0.95

    def matches_message(self, message: UnifiedMessage) -> bool:
        if (
            self.message_type_filter
            and message.message_type not in self.message_type_filter
        ):
            return False
        if (
            self.priority_filter
            and message.headers.priority not in self.priority_filter
        ):
            return False
        if self.topic_pattern_filter:
            if not TopicValidator.matches_pattern(
                message.topic, self.topic_pattern_filter
            ):
                return False
        return True


@dataclass(slots=True)
class FabricRoutingConfig:
    """Configuration for the fabric router."""

    max_routing_hops: HopCount = 10
    routing_cache_ttl_ms: float = 30000.0
    max_cached_routes: int = 10000

    default_policy: FabricRoutingPolicy = field(
        default_factory=lambda: FabricRoutingPolicy("default")
    )
    policies: list[FabricRoutingPolicy] = field(default_factory=list)

    enable_federation_optimization: bool = True
    enable_queue_topics: bool = True
    enable_topic_rpc: bool = True

    local_node_id: NodeId = "local"
    local_cluster_id: ClusterId = "local-cluster"

    def get_policy_for_message(self, message: UnifiedMessage) -> FabricRoutingPolicy:
        for policy in self.policies:
            if policy.matches_message(message):
                return policy
        return self.default_policy


@dataclass(frozen=True, slots=True)
class FabricRoutingStatisticsSnapshot:
    """Snapshot of fabric routing statistics."""

    total_routes_computed: int
    local_routes: int
    federation_routes: int
    multi_target_routes: int

    average_route_computation_ms: float
    average_local_latency_ms: PathLatencyMs
    average_federation_latency_ms: PathLatencyMs

    cache_hit_ratio: float
    policy_application_rate: float

    rpc_routes: int
    pubsub_routes: int
    queue_routes: int
    cache_routes: int
    control_plane_routes: int

    @property
    def federation_ratio(self) -> float:
        total = self.total_routes_computed
        return (self.federation_routes / total) if total > 0 else 0.0

    @property
    def local_efficiency(self) -> float:
        return self.cache_hit_ratio * (1.0 - self.federation_ratio)


class RouteHandler(Protocol):
    async def handle_route(
        self, message: UnifiedMessage, route: FabricRouteResult
    ) -> bool: ...

    async def get_handler_statistics(self) -> dict[str, Any]: ...


@dataclass(slots=True)
class RouteHandlerRegistry:
    """Registry for route handlers across different systems."""

    handlers: dict[RouteHandlerId, Any] = field(default_factory=dict)
    pattern_mappings: dict[TopicPattern, set[RouteHandlerId]] = field(
        default_factory=dict
    )
    system_mappings: dict[MessageType, set[RouteHandlerId]] = field(
        default_factory=dict
    )

    async def register_handler(
        self,
        handler_id: RouteHandlerId,
        handler: Any,
        patterns: list[TopicPattern],
        system_type: MessageType,
    ) -> None:
        self.handlers[handler_id] = handler
        for pattern in patterns:
            self.pattern_mappings.setdefault(pattern, set()).add(handler_id)
        self.system_mappings.setdefault(system_type, set()).add(handler_id)

    async def get_handlers_for_message(
        self, message: UnifiedMessage
    ) -> list[tuple[RouteHandlerId, Any]]:
        matching_handlers: set[RouteHandlerId] = set()
        for pattern, handler_ids in self.pattern_mappings.items():
            if self._pattern_matches(pattern, message.topic):
                matching_handlers.update(handler_ids)
        if message.message_type in self.system_mappings:
            matching_handlers.update(self.system_mappings[message.message_type])
        return [
            (handler_id, self.handlers[handler_id]) for handler_id in matching_handlers
        ]

    def _pattern_matches(self, pattern: TopicPattern, topic: str) -> bool:
        return TopicValidator.matches_pattern(topic, pattern)


@dataclass(slots=True)
class RoutingMetrics:
    """Metrics tracking for fabric routing operations."""

    total_routes_computed: int = 0
    local_routes: int = 0
    federation_routes: int = 0
    multi_target_routes: int = 0

    total_route_computation_time_ms: float = 0.0
    local_latency_total_ms: float = 0.0
    federation_latency_total_ms: float = 0.0

    cache_hits: int = 0
    cache_misses: int = 0

    rpc_routes: int = 0
    pubsub_routes: int = 0
    queue_routes: int = 0
    cache_routes: int = 0
    control_plane_routes: int = 0

    policies_applied: int = 0

    def record_route_computation(
        self,
        route: FabricRouteResult,
        computation_time_ms: float,
        message_type: MessageType,
        used_cache: bool,
    ) -> None:
        self.total_routes_computed += 1
        self.total_route_computation_time_ms += computation_time_ms

        if route.is_local_route:
            self.local_routes += 1
            self.local_latency_total_ms += route.estimated_latency_ms
        else:
            self.federation_routes += 1
            self.federation_latency_total_ms += route.estimated_latency_ms

        if route.is_multi_target:
            self.multi_target_routes += 1

        if used_cache:
            self.cache_hits += 1
        else:
            self.cache_misses += 1

        if message_type == MessageType.RPC:
            self.rpc_routes += 1
        elif message_type == MessageType.PUBSUB:
            self.pubsub_routes += 1
        elif message_type == MessageType.QUEUE:
            self.queue_routes += 1
        elif message_type == MessageType.CACHE:
            self.cache_routes += 1
        elif message_type == MessageType.CONTROL:
            self.control_plane_routes += 1

    def get_statistics_snapshot(self) -> FabricRoutingStatisticsSnapshot:
        total = max(1, self.total_routes_computed)
        cache_total = max(1, self.cache_hits + self.cache_misses)
        return FabricRoutingStatisticsSnapshot(
            total_routes_computed=self.total_routes_computed,
            local_routes=self.local_routes,
            federation_routes=self.federation_routes,
            multi_target_routes=self.multi_target_routes,
            average_route_computation_ms=self.total_route_computation_time_ms / total,
            average_local_latency_ms=self.local_latency_total_ms
            / max(1, self.local_routes),
            average_federation_latency_ms=self.federation_latency_total_ms
            / max(1, self.federation_routes),
            cache_hit_ratio=self.cache_hits / cache_total,
            policy_application_rate=self.policies_applied / total,
            rpc_routes=self.rpc_routes,
            pubsub_routes=self.pubsub_routes,
            queue_routes=self.queue_routes,
            cache_routes=self.cache_routes,
            control_plane_routes=self.control_plane_routes,
        )


class FabricRouter:
    """Fabric-native router that plans routes across all message systems."""

    def __init__(
        self,
        config: FabricRoutingConfig,
        routing_index: RoutingIndex | None = None,
        routing_engine: RoutingEngine | None = None,
        pubsub_planner: PubSubRoutingPlanner | None = None,
        message_queue: Any | None = None,
    ) -> None:
        self.config = config
        self.routing_index = routing_index or RoutingIndex()
        self.routing_engine = routing_engine or RoutingEngine(
            local_cluster=config.local_cluster_id,
            routing_index=self.routing_index,
        )
        self.pubsub_planner = pubsub_planner or PubSubRoutingPlanner(
            routing_index=self.routing_index,
            local_node_id=config.local_node_id,
        )
        self.message_queue = message_queue

        self.handler_registry = RouteHandlerRegistry()
        self.metrics = RoutingMetrics()
        self.route_cache: dict[str, tuple[FabricRouteResult, float]] = {}

    async def route_message(self, message: UnifiedMessage) -> FabricRouteResult:
        start_time = time.time()
        policy = self.config.get_policy_for_message(message)
        self.metrics.policies_applied += 1

        cache_key = self._get_cache_key(message)
        cached_result = self._get_cached_route(cache_key)
        if cached_result:
            computation_time = (time.time() - start_time) * 1000
            self.metrics.record_route_computation(
                cached_result, computation_time, message.message_type, used_cache=True
            )
            self._log_route_decision(message, cached_result, cached=True)
            return cached_result

        route_result = await self._compute_route(message, policy)
        self._cache_route(cache_key, route_result)

        computation_time = (time.time() - start_time) * 1000
        self.metrics.record_route_computation(
            route_result, computation_time, message.message_type, used_cache=False
        )
        self._log_route_decision(message, route_result, cached=False)
        return route_result

    def _plan_cluster_routes(
        self,
        cluster_ids: set[ClusterId],
        message: UnifiedMessage,
        policy: FabricRoutingPolicy,
    ) -> dict[ClusterId, ClusterRoutePlan]:
        if not cluster_ids:
            return {}
        hop_budget = min(
            policy.max_federation_hops,
            (
                message.headers.hop_budget
                if message.headers.hop_budget is not None
                else policy.max_federation_hops
            ),
        )
        return {
            cluster_id: self.routing_engine.plan_cluster_route(
                cluster_id,
                routing_path=message.headers.federation_path,
                hop_budget=hop_budget,
            )
            for cluster_id in cluster_ids
        }

    def _summarize_forwarding(
        self, cluster_routes: dict[ClusterId, ClusterRoutePlan]
    ) -> tuple[list[ClusterId], HopCount]:
        forwarding = [
            plan.forwarding
            for plan in cluster_routes.values()
            if plan.can_forward and plan.forwarding is not None
        ]
        if not forwarding:
            return [], 0
        hops_required = max(
            0,
            max(len(plan.planned_path) - 1 for plan in forwarding),
        )
        longest = max(forwarding, key=lambda plan: len(plan.planned_path))
        return list(longest.planned_path), hops_required

    async def register_route_pattern(
        self, pattern: TopicPattern, handler: RouteHandlerId
    ) -> None:
        is_valid, error_msg = TopicValidator.validate_topic_pattern(pattern)
        if not is_valid:
            raise ValueError(f"Invalid topic pattern: {error_msg}")
        self.handler_registry.pattern_mappings.setdefault(pattern, set()).add(handler)

    async def register_routing_policy(self, policy: FabricRoutingPolicy) -> None:
        self.config.policies.append(policy)

    async def get_routing_statistics(self) -> FabricRoutingStatisticsSnapshot:
        return self.metrics.get_statistics_snapshot()

    async def invalidate_routing_cache(
        self, pattern: TopicPattern | None = None
    ) -> None:
        if pattern is None:
            self.route_cache.clear()
            return
        to_remove = [key for key in self.route_cache if pattern in key]
        for key in to_remove:
            del self.route_cache[key]

    def _log_route_decision(
        self, message: UnifiedMessage, route_result: FabricRouteResult, cached: bool
    ) -> None:
        router_log.opt(lazy=True).debug(
            "Fabric route decision: message_id={message_id} topic={topic} type={message_type} "
            "reason={reason} cached={cached} targets={targets} path={path} hops={hops}",
            message_id=lambda: message.message_id,
            topic=lambda: message.topic,
            message_type=lambda: message.message_type.value,
            reason=lambda: route_result.reason.value,
            cached=lambda: cached,
            targets=lambda: [
                target.node_id or target.cluster_id or target.target_id
                for target in route_result.targets
            ],
            path=lambda: route_result.routing_path,
            hops=lambda: route_result.hops_required,
        )

    async def _compute_route(
        self, message: UnifiedMessage, policy: FabricRoutingPolicy
    ) -> FabricRouteResult:
        route_id = create_route_id(message)

        if (
            self._is_federation_message(message)
            and self.config.enable_federation_optimization
        ):
            route = await self._compute_federation_route(message, policy, route_id)
            if route.reason != FabricRouteReason.NO_MATCH:
                return route

        if message.message_type == MessageType.RPC:
            return await self._compute_rpc_route(message, policy, route_id)
        if message.message_type == MessageType.PUBSUB:
            return await self._compute_pubsub_route(message, policy, route_id)
        if message.message_type == MessageType.QUEUE:
            return await self._compute_queue_route(message, policy, route_id)
        if message.message_type == MessageType.CACHE:
            return await self._compute_cache_route(message, policy, route_id)
        if message.message_type == MessageType.CONTROL:
            return await self._compute_local_route(
                message, policy, route_id, priority_boost=True
            )
        return await self._compute_local_route(message, policy, route_id)

    async def _compute_federation_route(
        self,
        message: UnifiedMessage,
        policy: FabricRoutingPolicy,
        route_id: str,
    ) -> FabricRouteResult:
        target_cluster = message.headers.target_cluster
        if not target_cluster:
            return await self._compute_local_route(
                message, policy, route_id, reason=FabricRouteReason.NO_MATCH
            )
        hop_budget = min(
            policy.max_federation_hops,
            message.headers.hop_budget
            if message.headers.hop_budget is not None
            else policy.max_federation_hops,
        )
        plan = self.routing_engine.plan_function_route(
            FunctionQuery(
                selector=FunctionSelector(name="_federation"),
                cluster_id=target_cluster,
            ),
            routing_path=message.headers.federation_path,
            hop_budget=hop_budget,
        )
        if not plan.forwarding or not plan.forwarding.can_forward:
            return await self._compute_local_route(
                message, policy, route_id, reason=FabricRouteReason.NO_MATCH
            )
        federation_path = list(plan.forwarding.planned_path)
        target = FabricRouteTarget(
            system_type=message.message_type,
            target_id=target_cluster,
            cluster_id=target_cluster,
            priority_weight=1.0,
        )
        return FabricRouteResult(
            route_id=route_id,
            targets=[target],
            routing_path=federation_path,
            federation_path=federation_path,
            estimated_latency_ms=10.0,
            route_cost=float(len(federation_path)) * 10.0,
            federation_required=True,
            hops_required=max(0, len(federation_path) - 1),
            reason=FabricRouteReason.FEDERATED,
        )

    async def _compute_rpc_route(
        self, message: UnifiedMessage, policy: FabricRoutingPolicy, route_id: str
    ) -> FabricRouteResult:
        request = self._extract_rpc_request(message)
        command_name = request.command if request and request.command else None
        if not command_name:
            command_name = self._extract_rpc_command(message)
        if not command_name:
            return await self._compute_local_route(
                message, policy, route_id, reason=FabricRouteReason.NO_MATCH
            )
        version_constraint = (
            VersionConstraint.parse(request.version_constraint)
            if request and request.version_constraint
            else None
        )
        selector = FunctionSelector(
            name=command_name,
            function_id=request.function_id if request else None,
            version_constraint=version_constraint,
        )
        resources = frozenset(request.resources) if request else frozenset()
        target_cluster = (
            request.target_cluster
            if request and request.target_cluster
            else message.headers.target_cluster
        )
        query = FunctionQuery(
            selector=selector,
            resources=resources,
            cluster_id=target_cluster,
            node_id=request.target_node if request else None,
        )
        hop_budget = policy.max_federation_hops
        if message.headers.hop_budget is not None:
            hop_budget = min(hop_budget, message.headers.hop_budget)
        if request and request.federation_remaining_hops is not None:
            hop_budget = min(hop_budget, request.federation_remaining_hops)
        federation_path = (
            request.federation_path
            if request and request.federation_path
            else message.headers.federation_path
        )
        plan = self.routing_engine.plan_function_route(
            query,
            routing_path=federation_path,
            hop_budget=hop_budget,
        )
        if not plan.selected_target:
            return FabricRouteResult(
                route_id=route_id,
                targets=[],
                routing_path=[],
                federation_path=[],
                estimated_latency_ms=0.0,
                route_cost=0.0,
                federation_required=False,
                hops_required=0,
                reason=FabricRouteReason.NO_MATCH,
            )
        target = FabricRouteTarget(
            system_type=MessageType.RPC,
            target_id=command_name,
            node_id=plan.selected_target.node_id,
            cluster_id=plan.selected_target.cluster_id,
            priority_weight=2.0,
        )
        target_cluster = plan.target_cluster or plan.selected_target.cluster_id
        if plan.is_local:
            cluster_reason = ClusterRouteReason.LOCAL
        elif plan.forwarding is None:
            cluster_reason = ClusterRouteReason.NO_FEDERATION
        elif plan.forwarding.can_forward:
            cluster_reason = ClusterRouteReason.REMOTE_PLANNED
        else:
            cluster_reason = ClusterRouteReason.REMOTE_UNAVAILABLE
        cluster_routes = {}
        if target_cluster:
            cluster_routes[target_cluster] = ClusterRoutePlan(
                target_cluster=target_cluster,
                forwarding=plan.forwarding,
                is_local=plan.is_local,
                reason=cluster_reason,
            )
        federation_path = list(plan.forwarding.planned_path) if plan.forwarding else []
        is_remote = plan.selected_target.cluster_id != self.config.local_cluster_id
        return FabricRouteResult(
            route_id=route_id,
            targets=[target],
            routing_path=federation_path or [plan.selected_target.node_id],
            federation_path=federation_path,
            estimated_latency_ms=10.0,
            route_cost=5.0,
            federation_required=is_remote,
            hops_required=max(0, len(federation_path) - 1),
            reason=FabricRouteReason.FEDERATED
            if is_remote
            else FabricRouteReason.LOCAL,
            cluster_routes=cluster_routes,
        )

    async def _compute_pubsub_route(
        self, message: UnifiedMessage, policy: FabricRoutingPolicy, route_id: str
    ) -> FabricRouteResult:
        plan = self.pubsub_planner.plan(message.topic)
        cluster_ids = {sub.cluster_id for sub in plan.remote_subscriptions}
        if plan.local_subscriptions:
            cluster_ids.add(self.config.local_cluster_id)
        cluster_routes = self._plan_cluster_routes(cluster_ids, message, policy)

        targets: list[FabricRouteTarget] = []
        for sub in plan.local_subscriptions:
            targets.append(
                FabricRouteTarget(
                    system_type=MessageType.PUBSUB,
                    target_id=sub.subscription_id,
                    node_id=sub.node_id,
                    cluster_id=sub.cluster_id,
                    priority_weight=1.0,
                )
            )
        for sub in plan.remote_subscriptions:
            targets.append(
                FabricRouteTarget(
                    system_type=MessageType.PUBSUB,
                    target_id=sub.subscription_id,
                    node_id=sub.node_id,
                    cluster_id=sub.cluster_id,
                    priority_weight=1.0,
                )
            )
        if not targets:
            return await self._compute_local_route(
                message, policy, route_id, reason=FabricRouteReason.NO_MATCH
            )
        used_clusters = {
            target.cluster_id for target in targets if target.cluster_id is not None
        }
        remote_clusters = {
            cluster_id
            for cluster_id in used_clusters
            if cluster_id != self.config.local_cluster_id
        }
        federation_required = bool(remote_clusters)
        used_routes = {
            cluster_id: cluster_routes[cluster_id]
            for cluster_id in remote_clusters
            if cluster_id in cluster_routes
        }
        federation_path, hops_required = self._summarize_forwarding(used_routes)
        return FabricRouteResult(
            route_id=route_id,
            targets=targets,
            routing_path=[
                target.node_id or self.config.local_node_id for target in targets
            ],
            federation_path=federation_path,
            estimated_latency_ms=5.0,
            route_cost=float(len(targets)) * 2.0,
            federation_required=federation_required,
            hops_required=hops_required,
            reason=FabricRouteReason.LOCAL
            if not federation_required
            else FabricRouteReason.FEDERATED,
            cluster_routes=used_routes,
        )

    async def _compute_queue_route(
        self, message: UnifiedMessage, policy: FabricRoutingPolicy, route_id: str
    ) -> FabricRouteResult:
        queue_name = self._extract_queue_name(message.topic)
        if not queue_name:
            return await self._compute_local_route(
                message, policy, route_id, reason=FabricRouteReason.NO_MATCH
            )
        if self.message_queue and queue_name not in self.message_queue.list_queues():
            await self.message_queue.create_queue(queue_name)
        entries = self.routing_index.find_queues(QueueQuery(queue_name=queue_name))
        if not entries:
            target = FabricRouteTarget(
                system_type=MessageType.QUEUE,
                target_id=queue_name,
                node_id=self.config.local_node_id,
                cluster_id=self.config.local_cluster_id,
                priority_weight=1.5,
            )
            targets = [target]
            cluster_routes = self._plan_cluster_routes(
                {self.config.local_cluster_id}, message, policy
            )
        else:
            cluster_routes = self._plan_cluster_routes(
                {entry.cluster_id for entry in entries}, message, policy
            )
            filtered: list[QueueEndpoint] = []
            for entry in entries:
                plan = cluster_routes.get(entry.cluster_id)
                if (
                    plan
                    and plan.reason is ClusterRouteReason.REMOTE_UNAVAILABLE
                    and entry.cluster_id != self.config.local_cluster_id
                ):
                    continue
                filtered.append(entry)
            if not filtered:
                return await self._compute_local_route(
                    message, policy, route_id, reason=FabricRouteReason.NO_MATCH
                )
            entries = filtered
            cluster_routes = {
                cluster_id: plan
                for cluster_id, plan in cluster_routes.items()
                if cluster_id in {entry.cluster_id for entry in entries}
            }
            targets = [
                FabricRouteTarget(
                    system_type=MessageType.QUEUE,
                    target_id=entry.queue_name,
                    node_id=entry.node_id,
                    cluster_id=entry.cluster_id,
                    priority_weight=1.5,
                )
                for entry in entries
            ]
        estimated_latency = 15.0
        route_cost = 3.0
        if message.delivery == DeliveryGuarantee.EXACTLY_ONCE:
            estimated_latency *= 2.0
            route_cost *= 2.0
        remote_routes = {
            cluster_id: plan
            for cluster_id, plan in cluster_routes.items()
            if cluster_id != self.config.local_cluster_id and plan.can_forward
        }
        federation_path, hops_required = self._summarize_forwarding(remote_routes)
        return FabricRouteResult(
            route_id=route_id,
            targets=targets,
            routing_path=[
                target.node_id or self.config.local_node_id for target in targets
            ],
            federation_path=federation_path,
            estimated_latency_ms=estimated_latency,
            route_cost=route_cost,
            federation_required=bool(remote_routes),
            hops_required=hops_required,
            reason=FabricRouteReason.FEDERATED
            if remote_routes
            else FabricRouteReason.LOCAL,
            cluster_routes=remote_routes,
        )

    async def _compute_cache_route(
        self, message: UnifiedMessage, policy: FabricRoutingPolicy, route_id: str
    ) -> FabricRouteResult:
        role = _cache_role_from_topic(message.topic)
        entries = self.routing_index.find_cache_roles(
            CacheQuery(role=role, cluster_id=message.headers.target_cluster)
        )
        if not entries:
            return await self._compute_local_route(
                message, policy, route_id, priority_boost=True
            )
        cluster_routes = self._plan_cluster_routes(
            {entry.cluster_id for entry in entries}, message, policy
        )
        filtered: list[CacheRoleEntry] = []
        for entry in entries:
            plan = cluster_routes.get(entry.cluster_id)
            if (
                plan
                and plan.reason is ClusterRouteReason.REMOTE_UNAVAILABLE
                and entry.cluster_id != self.config.local_cluster_id
            ):
                continue
            filtered.append(entry)
        if not filtered:
            return await self._compute_local_route(
                message,
                policy,
                route_id,
                priority_boost=True,
                reason=FabricRouteReason.NO_MATCH,
            )
        entries = filtered
        cluster_routes = {
            cluster_id: plan
            for cluster_id, plan in cluster_routes.items()
            if cluster_id in {entry.cluster_id for entry in entries}
        }
        targets = [
            FabricRouteTarget(
                system_type=MessageType.CACHE,
                target_id=role.value,
                node_id=entry.node_id,
                cluster_id=entry.cluster_id,
                priority_weight=2.0
                if role in {CacheRole.INVALIDATOR, CacheRole.COORDINATOR}
                else 1.0,
            )
            for entry in entries
        ]
        remote_routes = {
            cluster_id: plan
            for cluster_id, plan in cluster_routes.items()
            if cluster_id != self.config.local_cluster_id and plan.can_forward
        }
        federation_path, hops_required = self._summarize_forwarding(remote_routes)
        return FabricRouteResult(
            route_id=route_id,
            targets=targets,
            routing_path=[entry.node_id for entry in entries],
            federation_path=federation_path,
            estimated_latency_ms=5.0,
            route_cost=float(len(targets)) * 1.5,
            federation_required=bool(remote_routes),
            hops_required=hops_required,
            reason=FabricRouteReason.FEDERATED
            if remote_routes
            else FabricRouteReason.LOCAL,
            cluster_routes=remote_routes,
        )

    async def _compute_local_route(
        self,
        message: UnifiedMessage,
        policy: FabricRoutingPolicy,
        route_id: str,
        *,
        priority_boost: bool = False,
        reason: FabricRouteReason = FabricRouteReason.FALLBACK_LOCAL,
    ) -> FabricRouteResult:
        target = FabricRouteTarget(
            system_type=message.message_type,
            target_id="local_handler",
            node_id=self.config.local_node_id,
            cluster_id=self.config.local_cluster_id,
            priority_weight=2.0 if priority_boost else 1.0,
        )
        base_latency = 1.0 if priority_boost else 5.0
        return FabricRouteResult(
            route_id=route_id,
            targets=[target],
            routing_path=[self.config.local_node_id],
            federation_path=[],
            estimated_latency_ms=base_latency,
            route_cost=1.0,
            federation_required=False,
            hops_required=0,
            reason=reason,
        )

    def _get_cache_key(self, message: UnifiedMessage) -> str:
        target_cluster = message.headers.target_cluster or "none"
        priority = message.headers.priority.value
        hop_budget = (
            str(message.headers.hop_budget)
            if message.headers.hop_budget is not None
            else "none"
        )
        federation_path = (
            ",".join(message.headers.federation_path)
            if message.headers.federation_path
            else "none"
        )
        base_key = (
            f"{message.topic}:{message.message_type.value}:{message.delivery.value}:"
            f"{target_cluster}:{priority}:{hop_budget}:{federation_path}"
        )
        if message.message_type is not MessageType.RPC:
            return base_key
        request = self._extract_rpc_request(message)
        if not request:
            return base_key
        command = request.command or "none"
        function_id = request.function_id or "none"
        version_constraint = request.version_constraint or "none"
        target_node = request.target_node or "none"
        resources = ",".join(sorted(request.resources)) if request.resources else "none"
        return (
            f"{base_key}:{command}:{function_id}:{version_constraint}:{target_node}:"
            f"{resources}"
        )

    def _get_cached_route(self, cache_key: str) -> FabricRouteResult | None:
        if cache_key in self.route_cache:
            route, cached_at = self.route_cache[cache_key]
            if time.time() - cached_at <= (self.config.routing_cache_ttl_ms / 1000):
                return route
            del self.route_cache[cache_key]
        return None

    def _cache_route(self, cache_key: str, route: FabricRouteResult) -> None:
        if len(self.route_cache) >= self.config.max_cached_routes:
            oldest_keys = list(self.route_cache.keys())[: len(self.route_cache) // 4]
            for key in oldest_keys:
                del self.route_cache[key]
        self.route_cache[cache_key] = (route, time.time())

    def _extract_rpc_command(self, message: UnifiedMessage) -> str | None:
        request = self._extract_rpc_request(message)
        if request and request.command:
            return request.command
        return self._extract_rpc_command_from_topic(message.topic)

    def _extract_rpc_request(self, message: UnifiedMessage) -> FabricRPCRequest | None:
        payload = message.payload
        if isinstance(payload, FabricRPCRequest):
            return payload
        if isinstance(payload, dict) and payload.get("kind") == FABRIC_RPC_REQUEST_KIND:
            try:
                return FabricRPCRequest.from_dict(payload)
            except Exception:
                return None
        return None

    def _extract_rpc_command_from_topic(self, topic: str) -> str | None:
        parts = topic.split(".")
        for i, part in enumerate(parts):
            if part == "rpc" and i + 1 < len(parts):
                return parts[i + 1]
        return None

    def _extract_queue_name(self, topic: str) -> str | None:
        parts = topic.split(".")
        for i, part in enumerate(parts):
            if part == "queue" and i + 1 < len(parts):
                return ".".join(parts[i + 1 :])
        return None

    def _is_federation_message(self, message: UnifiedMessage) -> bool:
        return bool(
            message.headers.target_cluster
            or message.headers.federation_path
            or message.topic.startswith("mpreg.fabric.")
        )


def create_correlation_id() -> MessageCorrelationId:
    import uuid

    return f"corr_{uuid.uuid4().hex[:16]}"


def create_route_id(message: UnifiedMessage) -> FabricRouteId:
    import hashlib

    route_key = (
        f"{message.topic}:{message.message_type.value}:{message.headers.correlation_id}"
    )
    return f"route_{hashlib.sha256(route_key.encode()).hexdigest()[:16]}"


def is_internal_topic(topic: TopicPattern) -> bool:
    return topic.startswith("mpreg.")


def extract_system_from_topic(topic: TopicPattern) -> MessageType | None:
    if topic.startswith("mpreg.rpc."):
        return MessageType.RPC
    if topic.startswith("mpreg.queue."):
        return MessageType.QUEUE
    if topic.startswith("mpreg.pubsub."):
        return MessageType.PUBSUB
    if topic.startswith("mpreg.cache."):
        return MessageType.CACHE
    if topic.startswith("mpreg."):
        return MessageType.CONTROL
    return MessageType.DATA


def is_federation_message(message: UnifiedMessage) -> bool:
    return bool(
        message.headers.target_cluster
        or message.headers.federation_path
        or message.topic.startswith("mpreg.fabric.")
    )


def is_control_plane_message(message: UnifiedMessage) -> bool:
    return (
        message.topic.startswith("mpreg.")
        and message.message_type == MessageType.CONTROL
    )


def _cache_role_from_topic(topic: str) -> CacheRole:
    if topic.startswith("mpreg.cache.invalidation."):
        return CacheRole.INVALIDATOR
    if topic.startswith("mpreg.cache.coordination."):
        return CacheRole.COORDINATOR
    if topic.startswith("mpreg.cache.sync."):
        return CacheRole.SYNC
    if topic.startswith("mpreg.cache.federation."):
        return CacheRole.COORDINATOR
    if topic.startswith("mpreg.cache.events.") or topic.startswith(
        "mpreg.cache.analytics."
    ):
        return CacheRole.MONITOR
    return CacheRole.COORDINATOR


def create_fabric_router(
    config: FabricRoutingConfig,
    *,
    routing_index: RoutingIndex | None = None,
    routing_engine: RoutingEngine | None = None,
    pubsub_planner: PubSubRoutingPlanner | None = None,
    message_queue: Any | None = None,
) -> FabricRouter:
    return FabricRouter(
        config=config,
        routing_index=routing_index,
        routing_engine=routing_engine,
        pubsub_planner=pubsub_planner,
        message_queue=message_queue,
    )
