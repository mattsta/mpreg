"""Gossip distribution for route verification keys."""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

from loguru import logger

from mpreg.datastructures.type_aliases import ClusterId, DurationSeconds, Timestamp

from .gossip import GossipMessage, GossipMessageType, GossipProtocol
from .route_keys import RouteKeyAnnouncement, RouteKeyRegistry


@dataclass(slots=True)
class RouteKeyAnnouncer:
    """Periodically announce local route keys through gossip."""

    local_cluster: ClusterId
    gossip: GossipProtocol
    registry: RouteKeyRegistry
    ttl_seconds: DurationSeconds = 120.0
    interval_seconds: DurationSeconds = 60.0
    _task: asyncio.Task[None] | None = field(default=None, init=False)

    async def start(self) -> None:
        if self._task and not self._task.done():
            return

        async def _loop() -> None:
            while True:
                try:
                    await self.announce_once(now=time.time())
                except Exception as exc:
                    logger.warning(
                        "[{}] Route key announcement failed: {}",
                        self.local_cluster,
                        exc,
                    )
                await asyncio.sleep(self.interval_seconds)

        self._task = asyncio.create_task(_loop())

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    async def announce_once(
        self, *, now: Timestamp | None = None
    ) -> GossipMessage | None:
        timestamp = now if now is not None else time.time()
        key_set = self.registry.key_sets.get(self.local_cluster)
        if not key_set or not key_set.keys:
            return None
        announcement = RouteKeyAnnouncement(
            cluster_id=self.local_cluster,
            keys=tuple(key_set.keys.values()),
            primary_key_id=key_set.primary_key_id,
            advertised_at=timestamp,
            ttl_seconds=self.ttl_seconds,
        )
        message = GossipMessage(
            message_id=f"{self.gossip.node_id}:{uuid.uuid4()}",
            message_type=GossipMessageType.ROUTE_KEY_ANNOUNCEMENT,
            sender_id=self.gossip.node_id,
            payload=announcement,
            vector_clock=self.gossip.vector_clock.copy(),
            sequence_number=self.gossip.protocol_stats.messages_created,
            ttl=10,
            max_hops=5,
        )
        await self.gossip.add_message(message)
        self.gossip.protocol_stats.messages_created += 1
        return message


@dataclass(slots=True)
class RouteKeyProcessor:
    """Apply route key announcements to the key registry."""

    registry: RouteKeyRegistry
    sender_cluster_resolver: Callable[[str], ClusterId | None]

    async def handle_announcement(
        self,
        announcement: RouteKeyAnnouncement,
        *,
        sender_id: str,
        now: Timestamp | None = None,
    ) -> bool:
        sender_cluster = self.sender_cluster_resolver(sender_id)
        if not sender_cluster:
            return False
        if announcement.cluster_id != sender_cluster:
            return False
        updated = self.registry.apply_announcement(announcement, now=now)
        return updated > 0
