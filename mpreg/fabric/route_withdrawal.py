"""Route withdrawal coordinator for connection loss events."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass

from loguru import logger

from mpreg.core.connection_events import ConnectionAwareComponent, ConnectionEvent
from mpreg.datastructures.type_aliases import ClusterId, NodeURL, Timestamp

from .route_announcer import RouteAnnouncementPublisher
from .route_control import RouteTable, RouteWithdrawal


@dataclass(eq=False, slots=True)
class RouteWithdrawalCoordinator(ConnectionAwareComponent):
    """Withdraw routes learned from a lost neighbor and publish withdrawals."""

    local_cluster: ClusterId
    route_table: RouteTable
    publisher: RouteAnnouncementPublisher
    cluster_resolver: Callable[[NodeURL], ClusterId | None]
    _epoch: int = 0

    def on_connection_established(self, event: ConnectionEvent) -> None:
        return

    def on_connection_lost(self, event: ConnectionEvent) -> None:
        neighbor_cluster = self.cluster_resolver(event.node_url)
        if not neighbor_cluster or neighbor_cluster == self.local_cluster:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.warning(
                "Route withdrawals skipped (no running loop) for lost peer {}",
                event.node_url,
            )
            return
        loop.create_task(
            self.withdraw_routes_for_cluster(neighbor_cluster, now=event.timestamp)
        )

    async def withdraw_routes_for_cluster(
        self, neighbor_cluster: ClusterId, *, now: Timestamp | None = None
    ) -> int:
        timestamp = now if now is not None else time.time()
        removed = self.route_table.remove_routes_for_neighbor(
            learned_from=neighbor_cluster, now=now
        )
        if not removed:
            return 0
        for record in removed:
            self._epoch += 1
            withdrawal = RouteWithdrawal(
                destination=record.destination,
                path=record.path,
                advertiser=self.local_cluster,
                withdrawn_at=timestamp,
                epoch=self._epoch,
                route_tags=record.route_tags,
            )
            await self.publisher.publish_withdrawal(withdrawal)
        return len(removed)
