"""Gossip-backed route announcements for the fabric control plane."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from mpreg.datastructures.type_aliases import (
    BandwidthMbps,
    ClusterId,
    DurationSeconds,
    NetworkLatencyMs,
    ReliabilityScore,
    RouteCostScore,
    Timestamp,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mpreg.fabric.gossip import GossipMessage, GossipProtocol

import contextlib

from .route_control import (
    RouteAnnouncement,
    RoutePath,
    RoutePolicy,
    RouteTable,
    RouteWithdrawal,
)
from .route_keys import normalize_public_keys
from .route_security import (
    RouteAnnouncementSigner,
    RouteSecurityConfig,
    verify_route_announcement,
    verify_route_withdrawal,
)


@dataclass(frozen=True, slots=True)
class RouteLinkMetrics:
    """Point-to-point link metrics applied when extending a route."""

    latency_ms: NetworkLatencyMs = 0.0
    bandwidth_mbps: BandwidthMbps | None = None
    reliability_score: ReliabilityScore = 1.0
    cost_score: RouteCostScore = 0.0


@dataclass(slots=True)
class RouteAnnouncementPublisher:
    """Publish route announcements through the gossip protocol."""

    gossip: GossipProtocol
    ttl: int = 10
    max_hops: int = 5
    signer: RouteAnnouncementSigner | None = None
    export_policy: RoutePolicy | None = None

    async def publish(self, announcement: RouteAnnouncement) -> GossipMessage | None:
        from mpreg.fabric.gossip import (
            GossipMessage,
            GossipMessageType,
        )

        if self.export_policy and not self.export_policy.accepts_announcement(
            announcement, received_from=announcement.advertiser
        ):
            return None
        if self.signer:
            announcement = self.signer.sign(announcement)

        sequence_number = self.gossip.next_sequence_number()
        message = GossipMessage(
            message_id=f"{self.gossip.node_id}:{uuid.uuid4()}",
            message_type=GossipMessageType.ROUTE_ADVERTISEMENT,
            sender_id=self.gossip.node_id,
            payload=announcement.to_dict(),
            vector_clock=self.gossip.vector_clock.copy(),
            sequence_number=sequence_number,
            ttl=self.ttl,
            max_hops=self.max_hops,
        )
        await self.gossip.add_message(message)
        return message

    async def publish_withdrawal(self, withdrawal: RouteWithdrawal) -> GossipMessage:
        from mpreg.fabric.gossip import (
            GossipMessage,
            GossipMessageType,
        )

        if self.signer:
            withdrawal = self.signer.sign_withdrawal(withdrawal)

        sequence_number = self.gossip.next_sequence_number()
        message = GossipMessage(
            message_id=f"{self.gossip.node_id}:{uuid.uuid4()}",
            message_type=GossipMessageType.ROUTE_WITHDRAWAL,
            sender_id=self.gossip.node_id,
            payload=withdrawal.to_dict(),
            vector_clock=self.gossip.vector_clock.copy(),
            sequence_number=sequence_number,
            ttl=self.ttl,
            max_hops=self.max_hops,
        )
        await self.gossip.add_message(message)
        return message


@dataclass(slots=True)
class RouteAnnouncementProcessor:
    """Apply route announcements and re-advertise improved paths."""

    local_cluster: ClusterId
    route_table: RouteTable
    sender_cluster_resolver: Callable[[str], ClusterId | None]
    link_metrics_resolver: Callable[[ClusterId], RouteLinkMetrics] | None = None
    publisher: RouteAnnouncementPublisher | None = None
    security_config: RouteSecurityConfig = field(default_factory=RouteSecurityConfig)
    public_key_resolver: (
        Callable[[ClusterId], bytes | Sequence[bytes] | None] | None
    ) = None
    neighbor_policy_resolver: Callable[[ClusterId], RoutePolicy | None] | None = None
    _epoch: int = 0

    async def handle_announcement(
        self,
        announcement: RouteAnnouncement,
        *,
        sender_id: str,
        now: Timestamp | None = None,
    ) -> bool:
        sender_cluster = self.sender_cluster_resolver(sender_id)
        if not sender_cluster or sender_cluster == self.local_cluster:
            return False
        if not self._is_announcement_authorized(
            announcement, sender_cluster=sender_cluster
        ):
            return False
        if not self._passes_neighbor_policy(
            announcement, sender_cluster=sender_cluster
        ):
            return False

        link_metrics = self._link_metrics(sender_cluster)
        updated = self.route_table.apply_announcement(
            announcement,
            received_from=sender_cluster,
            now=now,
            link_latency_ms=link_metrics.latency_ms,
            link_bandwidth_mbps=link_metrics.bandwidth_mbps,
            link_reliability=link_metrics.reliability_score,
            link_cost=link_metrics.cost_score,
        )
        if not updated or not self.publisher:
            return updated

        relay = self._build_relay(
            announcement,
            link_metrics=link_metrics,
            now=now,
        )
        await self.publisher.publish(relay)
        return updated

    async def handle_withdrawal(
        self,
        withdrawal: RouteWithdrawal,
        *,
        sender_id: str,
        now: Timestamp | None = None,
    ) -> bool:
        sender_cluster = self.sender_cluster_resolver(sender_id)
        if not sender_cluster or sender_cluster == self.local_cluster:
            return False
        if not self._is_withdrawal_authorized(
            withdrawal, sender_cluster=sender_cluster
        ):
            return False

        removed = self.route_table.apply_withdrawal(
            withdrawal,
            received_from=sender_cluster,
            now=now,
        )
        if not removed or not self.publisher:
            return bool(removed)

        for record in removed:
            relay = self._build_withdrawal_relay(
                withdrawal,
                record_path=record.path,
                now=now,
            )
            await self.publisher.publish_withdrawal(relay)
        return True

    def _is_announcement_authorized(
        self, announcement: RouteAnnouncement, *, sender_cluster: ClusterId
    ) -> bool:
        if announcement.advertiser != sender_cluster:
            return False

        if not announcement.signature:
            if self.security_config.require_signatures:
                return False
            return self.security_config.allow_unsigned

        if announcement.signature_algorithm != self.security_config.signature_algorithm:
            return False

        keys = self._resolve_public_keys(
            announcement.advertiser, fallback_key=announcement.public_key
        )
        if not keys:
            return False
        return any(
            verify_route_announcement(
                announcement,
                public_key=public_key,
                algorithm=self.security_config.signature_algorithm,
            )
            for public_key in keys
        )

    def _passes_neighbor_policy(
        self, announcement: RouteAnnouncement, *, sender_cluster: ClusterId
    ) -> bool:
        if not self.neighbor_policy_resolver:
            return True
        policy = self.neighbor_policy_resolver(sender_cluster)
        if policy is None:
            return True
        return policy.accepts_announcement(announcement, received_from=sender_cluster)

    def _is_withdrawal_authorized(
        self, withdrawal: RouteWithdrawal, *, sender_cluster: ClusterId
    ) -> bool:
        if withdrawal.advertiser != sender_cluster:
            return False
        if not withdrawal.path.hops:
            return False
        if withdrawal.path.hops[0] != withdrawal.advertiser:
            return False
        if withdrawal.destination.cluster_id != withdrawal.path.hops[-1]:
            return False
        if not withdrawal.signature:
            if self.security_config.require_signatures:
                return False
            return self.security_config.allow_unsigned
        if withdrawal.signature_algorithm != self.security_config.signature_algorithm:
            return False

        keys = self._resolve_public_keys(
            withdrawal.advertiser, fallback_key=withdrawal.public_key
        )
        if not keys:
            return False
        return any(
            verify_route_withdrawal(
                withdrawal,
                public_key=public_key,
                algorithm=self.security_config.signature_algorithm,
            )
            for public_key in keys
        )

    def _resolve_public_keys(
        self, cluster_id: ClusterId, *, fallback_key: bytes | None
    ) -> tuple[bytes, ...]:
        if self.public_key_resolver:
            resolved = normalize_public_keys(self.public_key_resolver(cluster_id))
            if resolved:
                return resolved
        return normalize_public_keys(fallback_key)

    def _build_relay(
        self,
        announcement: RouteAnnouncement,
        *,
        link_metrics: RouteLinkMetrics,
        now: Timestamp | None = None,
    ) -> RouteAnnouncement:
        timestamp = now if now is not None else time.time()
        self._epoch += 1
        metrics = announcement.metrics.with_added_hop(
            latency_ms=link_metrics.latency_ms,
            bandwidth_mbps=link_metrics.bandwidth_mbps,
            reliability_score=link_metrics.reliability_score,
            cost_score=link_metrics.cost_score,
        )
        return RouteAnnouncement(
            destination=announcement.destination,
            path=RoutePath((self.local_cluster,) + announcement.path.hops),
            metrics=metrics,
            advertiser=self.local_cluster,
            advertised_at=timestamp,
            ttl_seconds=announcement.ttl_seconds,
            epoch=self._epoch,
            route_tags=announcement.route_tags,
        )

    def _build_withdrawal_relay(
        self,
        withdrawal: RouteWithdrawal,
        *,
        record_path: RoutePath,
        now: Timestamp | None = None,
    ) -> RouteWithdrawal:
        timestamp = now if now is not None else time.time()
        self._epoch += 1
        return RouteWithdrawal(
            destination=withdrawal.destination,
            path=record_path,
            advertiser=self.local_cluster,
            withdrawn_at=timestamp,
            epoch=self._epoch,
            route_tags=withdrawal.route_tags,
        )

    def _link_metrics(self, sender_cluster: ClusterId) -> RouteLinkMetrics:
        if not self.link_metrics_resolver:
            return RouteLinkMetrics()
        return self.link_metrics_resolver(sender_cluster)


@dataclass(slots=True)
class RouteAnnouncer:
    """Periodic broadcaster for local route announcements."""

    local_cluster: ClusterId
    route_table: RouteTable
    publisher: RouteAnnouncementPublisher
    ttl_seconds: DurationSeconds = 30.0
    interval_seconds: DurationSeconds = 10.0
    _task: asyncio.Task[None] | None = field(init=False, default=None)
    _epoch: int = 0

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._announce_loop())

    async def stop(self) -> None:
        if not self._task:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def announce_once(self, *, now: Timestamp | None = None) -> None:
        self._epoch += 1
        announcement = self.route_table.build_local_announcement(
            ttl_seconds=self.ttl_seconds, epoch=self._epoch, now=now
        )
        await self.publisher.publish(announcement)

    async def _announce_loop(self) -> None:
        while True:
            await self.announce_once()
            await asyncio.sleep(self.interval_seconds)
