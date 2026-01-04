"""Tests for MPREG server status metrics."""

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager
from mpreg.core.message_queue import DeliveryGuarantee
from mpreg.core.message_queue_manager import (
    MessageQueueManager,
    QueueManagerConfiguration,
)
from mpreg.server import MPREGServer


class TestServerStatusMetrics:
    """Verify status metrics include queue statistics when attached."""

    @pytest.mark.asyncio
    async def test_status_includes_queue_metrics(self):
        settings = MPREGSettings(monitoring_enabled=False)
        server = MPREGServer(settings=settings)

        queue_manager = MessageQueueManager(QueueManagerConfiguration())
        await queue_manager.create_queue("status-queue")
        await queue_manager.send_message(
            "status-queue",
            "status.topic",
            {"payload": "test"},
            DeliveryGuarantee.AT_LEAST_ONCE,
        )

        try:
            server.attach_queue_manager(queue_manager)
            status = server._build_status_message()

            queue_metrics = status.metrics.get("queue_metrics")
            assert queue_metrics is not None
            assert queue_metrics["total_queues"] == 1
            assert queue_metrics["total_messages_sent"] >= 1
            assert "success_rate" in queue_metrics
            route_metrics = status.metrics.get("route_metrics")
            assert route_metrics is not None
        finally:
            await queue_manager.shutdown()

    @pytest.mark.asyncio
    async def test_status_includes_cache_metrics(self):
        settings = MPREGSettings(monitoring_enabled=False)
        server = MPREGServer(settings=settings)

        cache_manager = GlobalCacheManager(
            GlobalCacheConfiguration(
                enable_l2_persistent=False,
                enable_l3_distributed=False,
                enable_l4_federation=False,
            )
        )

        try:
            server.attach_cache_manager(cache_manager)
            status = server._build_status_message()

            cache_metrics = status.metrics.get("cache_metrics")
            assert cache_metrics is not None
            assert "cache_region" in cache_metrics
            assert "cache_capacity_mb" in cache_metrics
            assert "cache_utilization_percent" in cache_metrics
            route_metrics = status.metrics.get("route_metrics")
            assert route_metrics is not None
        finally:
            cache_manager.shutdown_sync()
