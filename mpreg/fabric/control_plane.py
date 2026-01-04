"""Unified control plane for the routing fabric."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from mpreg.datastructures.type_aliases import AreaId, ClusterId, DurationSeconds, NodeId
from mpreg.fabric.gossip import GossipProtocol

from .adapters.cache_federation import CacheFederationCatalogAdapter
from .adapters.cache_profile import CacheProfileCatalogAdapter
from .adapters.topic_exchange import TopicExchangeCatalogAdapter
from .announcers import (
    FabricCacheProfileAnnouncer,
    FabricCacheRoleAnnouncer,
    FabricFunctionAnnouncer,
    FabricQueueAnnouncer,
    FabricTopicAnnouncer,
    FunctionCatalogAdapter,
)
from .broadcaster import CatalogBroadcaster
from .catalog import RoutingCatalog
from .catalog_delta import CatalogDeltaObserver, RoutingCatalogApplier
from .catalog_policy import CatalogFilterPolicy
from .catalog_publisher import CatalogDeltaPublisher
from .engine import RoutingEngine
from .federation_graph import GraphBasedFederationRouter
from .index import RoutingIndex
from .link_state import (
    LinkStateAnnouncer,
    LinkStateAreaPolicy,
    LinkStateMode,
    LinkStateProcessor,
    LinkStatePublisher,
    LinkStateTable,
)
from .peer_directory import PeerNeighbor
from .route_announcer import (
    RouteAnnouncementProcessor,
    RouteAnnouncementPublisher,
    RouteAnnouncer,
    RouteLinkMetrics,
)
from .route_control import (
    RouteAnnouncement,
    RoutePolicy,
    RouteStabilityPolicy,
    RouteTable,
    RouteWithdrawal,
)
from .route_key_gossip import RouteKeyAnnouncer, RouteKeyProcessor
from .route_keys import RouteKeyRegistry
from .route_security import RouteAnnouncementSigner, RouteSecurityConfig
from .route_withdrawal import RouteWithdrawalCoordinator


@dataclass(slots=True)
class FabricControlPlane:
    local_cluster: ClusterId
    gossip: GossipProtocol
    catalog: RoutingCatalog
    applier: RoutingCatalogApplier
    index: RoutingIndex
    engine: RoutingEngine
    broadcaster: CatalogBroadcaster
    route_table: RouteTable
    route_publisher: RouteAnnouncementPublisher
    route_processor: RouteAnnouncementProcessor | None
    route_announcer: RouteAnnouncer | None
    route_withdrawal_coordinator: RouteWithdrawalCoordinator | None
    link_state_table: LinkStateTable | None
    link_state_router: GraphBasedFederationRouter | None
    link_state_publisher: LinkStatePublisher | None
    link_state_processor: LinkStateProcessor | None
    link_state_announcer: LinkStateAnnouncer | None
    route_key_registry: RouteKeyRegistry | None
    route_key_announcer: RouteKeyAnnouncer | None
    route_key_processor: RouteKeyProcessor | None
    node_cluster_resolver: Callable[[NodeId], ClusterId | None] | None = None
    route_export_neighbor_policy_resolver: (
        Callable[[ClusterId], RoutePolicy | None] | None
    ) = None

    @classmethod
    def create(
        cls,
        *,
        local_cluster: ClusterId,
        gossip: GossipProtocol,
        engine: RoutingEngine | None = None,
        catalog_policy: CatalogFilterPolicy | None = None,
        catalog_observers: tuple[CatalogDeltaObserver, ...] = (),
        catalog_max_hops: int | None = None,
        sender_cluster_resolver: Callable[[str], ClusterId | None] | None = None,
        route_public_key_resolver: Callable[[ClusterId], bytes | Sequence[bytes] | None]
        | None = None,
        route_neighbor_policy_resolver: Callable[[ClusterId], RoutePolicy | None]
        | None = None,
        route_export_neighbor_policy_resolver: Callable[[ClusterId], RoutePolicy | None]
        | None = None,
        link_metrics_resolver: Callable[[ClusterId], RouteLinkMetrics] | None = None,
        route_policy: RoutePolicy | None = None,
        route_stability_policy: RouteStabilityPolicy | None = None,
        route_security_config: RouteSecurityConfig | None = None,
        route_signer: RouteAnnouncementSigner | None = None,
        route_export_policy: RoutePolicy | None = None,
        route_ttl_seconds: DurationSeconds = 30.0,
        route_announce_interval: DurationSeconds = 10.0,
        route_key_registry: RouteKeyRegistry | None = None,
        route_key_ttl_seconds: DurationSeconds = 120.0,
        route_key_announce_interval: DurationSeconds = 60.0,
        link_state_mode: LinkStateMode = LinkStateMode.DISABLED,
        link_state_neighbor_locator: Callable[[], Sequence[PeerNeighbor]] | None = None,
        link_state_router: GraphBasedFederationRouter | None = None,
        link_state_ttl_seconds: DurationSeconds = 30.0,
        link_state_announce_interval: DurationSeconds = 10.0,
        link_state_area: AreaId | None = None,
        link_state_area_policy: LinkStateAreaPolicy | None = None,
    ) -> FabricControlPlane:
        catalog = RoutingCatalog()
        applier = RoutingCatalogApplier(
            catalog, policy=catalog_policy, observers=catalog_observers
        )
        gossip.catalog_applier = applier
        index = RoutingIndex(catalog=catalog)
        routing_engine = engine or RoutingEngine(
            local_cluster=local_cluster, routing_index=index
        )
        publisher = CatalogDeltaPublisher(
            gossip,
            max_hops=(
                catalog_max_hops
                if catalog_max_hops is not None
                else CatalogDeltaPublisher.max_hops
            ),
        )
        broadcaster = CatalogBroadcaster(
            catalog=catalog, applier=applier, publisher=publisher
        )
        route_table = RouteTable(
            local_cluster=local_cluster,
            policy=route_policy or RoutePolicy(),
            stability_policy=route_stability_policy or RouteStabilityPolicy(),
        )
        route_publisher = RouteAnnouncementPublisher(
            gossip,
            signer=route_signer,
            export_policy=route_export_policy,
        )
        route_processor = None
        route_announcer = None
        route_withdrawal_coordinator = None
        link_state_table = None
        link_state_router_instance = None
        link_state_publisher = None
        link_state_processor = None
        link_state_announcer = None
        route_key_announcer = None
        route_key_processor = None
        if sender_cluster_resolver:
            route_processor = RouteAnnouncementProcessor(
                local_cluster=local_cluster,
                route_table=route_table,
                sender_cluster_resolver=sender_cluster_resolver,
                link_metrics_resolver=link_metrics_resolver,
                publisher=route_publisher,
                security_config=route_security_config or RouteSecurityConfig(),
                public_key_resolver=route_public_key_resolver,
                neighbor_policy_resolver=route_neighbor_policy_resolver,
            )
            gossip.route_applier = route_processor
            route_announcer = RouteAnnouncer(
                local_cluster=local_cluster,
                route_table=route_table,
                publisher=route_publisher,
                ttl_seconds=route_ttl_seconds,
                interval_seconds=route_announce_interval,
            )
            route_withdrawal_coordinator = RouteWithdrawalCoordinator(
                local_cluster=local_cluster,
                route_table=route_table,
                publisher=route_publisher,
                cluster_resolver=sender_cluster_resolver,
            )
            if route_key_registry is not None:
                route_key_processor = RouteKeyProcessor(
                    registry=route_key_registry,
                    sender_cluster_resolver=sender_cluster_resolver,
                )
                gossip.route_key_applier = route_key_processor
                route_key_announcer = RouteKeyAnnouncer(
                    local_cluster=local_cluster,
                    gossip=gossip,
                    registry=route_key_registry,
                    ttl_seconds=route_key_ttl_seconds,
                    interval_seconds=route_key_announce_interval,
                )
            if route_export_neighbor_policy_resolver:
                gossip.message_target_filter = _build_route_target_filter(
                    local_cluster=local_cluster,
                    node_cluster_resolver=sender_cluster_resolver,
                    policy_resolver=route_export_neighbor_policy_resolver,
                )
        if link_state_mode is not LinkStateMode.DISABLED:
            link_state_table = LinkStateTable(local_cluster=local_cluster)
            link_state_router_instance = (
                link_state_router
                or GraphBasedFederationRouter(
                    cache_ttl_seconds=30.0,
                    max_cache_size=10000,
                )
            )
            link_state_publisher = LinkStatePublisher(gossip)
            area_policy = link_state_area_policy
            if area_policy is not None and area_policy.default_area is None:
                area_policy = LinkStateAreaPolicy(
                    local_areas=area_policy.local_areas,
                    neighbor_areas=dict(area_policy.neighbor_areas),
                    area_neighbor_filters=dict(area_policy.area_neighbor_filters),
                    default_area=link_state_area,
                    allow_unmapped_neighbors=area_policy.allow_unmapped_neighbors,
                )
            allowed_areas = (
                area_policy.allowed_areas() if area_policy is not None else None
            )
            if allowed_areas is None and link_state_area is not None:
                allowed_areas = frozenset({link_state_area})
            link_state_processor = LinkStateProcessor(
                local_cluster=local_cluster,
                table=link_state_table,
                router=link_state_router_instance,
                sender_cluster_resolver=sender_cluster_resolver,
                allowed_areas=allowed_areas,
            )
            gossip.link_state_applier = link_state_processor
            if link_state_neighbor_locator:
                link_state_announcer = LinkStateAnnouncer(
                    local_cluster=local_cluster,
                    neighbor_locator=lambda: list(link_state_neighbor_locator()),
                    publisher=link_state_publisher,
                    area=link_state_area,
                    area_policy=area_policy,
                    ttl_seconds=link_state_ttl_seconds,
                    interval_seconds=link_state_announce_interval,
                    link_metrics_resolver=link_metrics_resolver,
                    processor=link_state_processor,
                )
        return cls(
            local_cluster=local_cluster,
            gossip=gossip,
            catalog=catalog,
            applier=applier,
            index=index,
            engine=routing_engine,
            broadcaster=broadcaster,
            route_table=route_table,
            route_publisher=route_publisher,
            route_processor=route_processor,
            route_announcer=route_announcer,
            route_withdrawal_coordinator=route_withdrawal_coordinator,
            link_state_table=link_state_table,
            link_state_router=link_state_router_instance,
            link_state_publisher=link_state_publisher,
            link_state_processor=link_state_processor,
            link_state_announcer=link_state_announcer,
            route_key_registry=route_key_registry,
            route_key_announcer=route_key_announcer,
            route_key_processor=route_key_processor,
            node_cluster_resolver=sender_cluster_resolver,
            route_export_neighbor_policy_resolver=route_export_neighbor_policy_resolver,
        )

    def function_announcer(
        self, *, adapter: FunctionCatalogAdapter
    ) -> FabricFunctionAnnouncer:
        return FabricFunctionAnnouncer(adapter=adapter, broadcaster=self.broadcaster)

    def queue_announcer(self) -> FabricQueueAnnouncer:
        return FabricQueueAnnouncer(broadcaster=self.broadcaster)

    def cache_announcer(
        self, *, cache_adapter: CacheFederationCatalogAdapter
    ) -> FabricCacheRoleAnnouncer:
        return FabricCacheRoleAnnouncer(
            adapter=cache_adapter, broadcaster=self.broadcaster
        )

    def cache_profile_announcer(
        self, *, cache_profile_adapter: CacheProfileCatalogAdapter
    ) -> FabricCacheProfileAnnouncer:
        return FabricCacheProfileAnnouncer(
            adapter=cache_profile_adapter, broadcaster=self.broadcaster
        )

    def topic_announcer(
        self, *, topic_adapter: TopicExchangeCatalogAdapter
    ) -> FabricTopicAnnouncer:
        return FabricTopicAnnouncer(adapter=topic_adapter, broadcaster=self.broadcaster)

    def update_route_configuration(
        self,
        *,
        route_policy: RoutePolicy | None = None,
        export_policy: RoutePolicy | None = None,
        neighbor_policy_resolver: Callable[[ClusterId], RoutePolicy | None]
        | None = None,
        export_neighbor_policy_resolver: Callable[[ClusterId], RoutePolicy | None]
        | None = None,
        security_config: RouteSecurityConfig | None = None,
        public_key_resolver: Callable[[ClusterId], bytes | Sequence[bytes] | None]
        | None = None,
        signer: RouteAnnouncementSigner | None = None,
        force: bool = False,
    ) -> None:
        """Update route configuration without dropping existing routes."""
        if force or route_policy is not None:
            self.route_table.policy = route_policy or RoutePolicy()
        if force or export_policy is not None:
            self.route_publisher.export_policy = export_policy
        if force or signer is not None:
            self.route_publisher.signer = signer
        if self.route_processor:
            if force or security_config is not None:
                self.route_processor.security_config = (
                    security_config or RouteSecurityConfig()
                )
            if force or public_key_resolver is not None:
                self.route_processor.public_key_resolver = public_key_resolver
            if force or neighbor_policy_resolver is not None:
                self.route_processor.neighbor_policy_resolver = neighbor_policy_resolver
        if force or export_neighbor_policy_resolver is not None:
            self.route_export_neighbor_policy_resolver = export_neighbor_policy_resolver
            if self.node_cluster_resolver and export_neighbor_policy_resolver:
                self.gossip.message_target_filter = _build_route_target_filter(
                    local_cluster=self.local_cluster,
                    node_cluster_resolver=self.node_cluster_resolver,
                    policy_resolver=export_neighbor_policy_resolver,
                )
            else:
                self.gossip.message_target_filter = None


def _build_route_target_filter(
    *,
    local_cluster: ClusterId,
    node_cluster_resolver: Callable[[NodeId], ClusterId | None],
    policy_resolver: Callable[[ClusterId], RoutePolicy | None],
) -> Callable[[object, NodeId], bool]:
    from mpreg.fabric.gossip import GossipMessage, GossipMessageType

    def _resolve_announcement(message: GossipMessage) -> RouteAnnouncement:
        payload = message.payload
        if isinstance(payload, RouteAnnouncement):
            return payload
        return RouteAnnouncement.from_dict(payload)  # type: ignore[arg-type]

    def _resolve_withdrawal(message: GossipMessage) -> RouteWithdrawal:
        payload = message.payload
        if isinstance(payload, RouteWithdrawal):
            return payload
        return RouteWithdrawal.from_dict(payload)  # type: ignore[arg-type]

    def _filter(message: GossipMessage, target_id: NodeId) -> bool:
        if message.message_type not in (
            GossipMessageType.ROUTE_ADVERTISEMENT,
            GossipMessageType.ROUTE_WITHDRAWAL,
        ):
            return True
        target_cluster = node_cluster_resolver(target_id)
        if target_cluster is None:
            return True
        policy = policy_resolver(target_cluster)
        if policy is None:
            return True
        if message.message_type is GossipMessageType.ROUTE_ADVERTISEMENT:
            announcement = _resolve_announcement(message)
            return policy.accepts_announcement(
                announcement, received_from=local_cluster
            )
        withdrawal = _resolve_withdrawal(message)
        return policy.accepts_withdrawal(withdrawal, received_from=local_cluster)

    return _filter
