from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from mpreg.core.cluster_map import ClusterMapSnapshot
from mpreg.core.discovery_summary import (
    ServiceSummary,
    SummaryQueryRequest,
    SummaryQueryResponse,
)
from mpreg.core.model import MPREGException, RPCCommand

from .client import Client

cluster_client_log = logger


@dataclass(slots=True)
class EndpointHealthStats:
    latency_samples_ms: deque[float] = field(default_factory=lambda: deque(maxlen=50))
    error_count: int = 0
    success_count: int = 0
    last_updated: float | None = None

    def record_success(self, latency_ms: float) -> None:
        self.success_count += 1
        self.latency_samples_ms.append(latency_ms)
        self.last_updated = time.time()

    def record_error(self) -> None:
        self.error_count += 1
        self.last_updated = time.time()

    def average_latency(self) -> float:
        if not self.latency_samples_ms:
            return 0.0
        return sum(self.latency_samples_ms) / len(self.latency_samples_ms)

    def error_rate(self) -> float:
        total = self.success_count + self.error_count
        if total == 0:
            return 0.0
        return self.error_count / total

    def penalty(
        self,
        *,
        now: float,
        error_weight: float,
        latency_weight: float,
        stale_after_seconds: float,
    ) -> float:
        if self.last_updated is None:
            return 0.0
        if now - self.last_updated > stale_after_seconds:
            return 0.0
        return (self.error_rate() * error_weight) + (
            self.average_latency() * latency_weight
        )


@dataclass(slots=True)
class MPREGClusterClient:
    """Cluster-aware client with discovery, failover, and load-based selection."""

    seed_urls: tuple[str, ...]
    full_log: bool = True
    refresh_interval: float = 5.0
    failure_cooldown_seconds: float = 2.0
    latency_weight: float = 0.01
    error_weight: float = 50.0
    health_stale_seconds: float = 30.0
    preferred_region: str | None = None
    auto_summary_redirect: bool = False
    summary_redirect_scope: str | None = "global"
    summary_redirect_ingress_limit: int | None = 2
    summary_redirect_ingress_scope: str | None = None

    _clients: dict[str, Client] = field(default_factory=dict, init=False)
    _endpoint_scores: dict[str, float] = field(default_factory=dict, init=False)
    _endpoint_regions: dict[str, str] = field(default_factory=dict, init=False)
    _endpoint_health: dict[str, EndpointHealthStats] = field(
        default_factory=dict, init=False
    )
    _cluster_endpoints: dict[str, tuple[str, ...]] = field(
        default_factory=dict, init=False
    )
    _ingress_hints: dict[str, tuple[str, ...]] = field(default_factory=dict, init=False)
    _last_failure: dict[str, float] = field(default_factory=dict, init=False)
    _refresh_task: asyncio.Task[None] | None = field(default=None, init=False)
    _connected: bool = field(default=False, init=False)
    _rr_index: int = field(default=0, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    def __post_init__(self) -> None:
        if isinstance(self.seed_urls, str):
            self.seed_urls = (self.seed_urls,)
        else:
            self.seed_urls = tuple(self.seed_urls)

    async def connect(self) -> None:
        """Connect to at least one seed endpoint and start discovery."""
        if self._connected:
            return
        await self._connect_seed()
        await self.refresh_cluster_map()
        self._connected = True
        self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def disconnect(self) -> None:
        """Disconnect from all endpoints and stop background tasks."""
        if self._refresh_task:
            self._refresh_task.cancel()
            await asyncio.gather(self._refresh_task, return_exceptions=True)
            self._refresh_task = None
        for client in self._clients.values():
            with contextlib.suppress(Exception):
                await client.disconnect()
        self._clients.clear()
        self._endpoint_scores.clear()
        self._last_failure.clear()
        self._connected = False

    async def __aenter__(self) -> MPREGClusterClient:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.disconnect()

    async def call(
        self,
        fun: str,
        *args: Any,
        locs: frozenset[str] | None = None,
        function_id: str | None = None,
        version_constraint: str | None = None,
        target_cluster: str | None = None,
        routing_topic: str | None = None,
        timeout: float | None = None,
        preferred_urls: Iterable[str] | None = None,
        use_ingress_hints: bool = False,
        enable_summary_redirect: bool | None = None,
        **kwargs: Any,
    ) -> Any:
        """Call an RPC function with automatic failover across cluster endpoints."""
        if not self._connected:
            await self.connect()

        effective_preferred = preferred_urls
        if effective_preferred is None and use_ingress_hints and target_cluster:
            effective_preferred = self._ingress_hints.get(
                target_cluster
            ) or self._cluster_endpoints.get(target_cluster)

        candidates = self._candidate_urls(preferred_urls=effective_preferred)
        if not candidates:
            candidates = list(self.seed_urls)

        last_error: Exception | None = None
        saw_command_not_found = False
        allow_summary_redirect = (
            self.auto_summary_redirect
            if enable_summary_redirect is None
            else enable_summary_redirect
        )
        summary_attempted = False
        if (
            allow_summary_redirect
            and enable_summary_redirect is True
            and target_cluster is None
        ):
            summary_attempted = True
            summary_result = await self._attempt_summary_redirect(
                fun,
                *args,
                locs=locs,
                function_id=function_id,
                version_constraint=version_constraint,
                routing_topic=routing_topic,
                timeout=timeout,
                **kwargs,
            )
            if summary_result is not None:
                return summary_result
        for url in candidates:
            try:
                return await self._call_on_url(
                    url,
                    fun,
                    *args,
                    locs=locs,
                    function_id=function_id,
                    version_constraint=version_constraint,
                    target_cluster=target_cluster,
                    routing_topic=routing_topic,
                    timeout=timeout,
                    **kwargs,
                )
            except Exception as exc:
                last_error = exc
                if self._is_command_not_found(exc):
                    saw_command_not_found = True
                self._last_failure[url] = time.time()
                cluster_client_log.warning(
                    "Cluster client call failed on {}: {}", url, exc
                )
                continue
        if (
            allow_summary_redirect
            and saw_command_not_found
            and target_cluster is None
            and not summary_attempted
        ):
            summary_result = await self._attempt_summary_redirect(
                fun,
                *args,
                locs=locs,
                function_id=function_id,
                version_constraint=version_constraint,
                routing_topic=routing_topic,
                timeout=timeout,
                **kwargs,
            )
            if summary_result is not None:
                return summary_result
        if last_error:
            raise last_error
        raise ConnectionError("No cluster endpoints available for RPC call.")

    async def cluster_map(self) -> ClusterMapSnapshot:
        """Fetch a cluster map snapshot and return parsed data."""
        result = await self._call_any("cluster_map")
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected cluster map response, got {type(result).__name__}"
            )
        return ClusterMapSnapshot.from_dict(result)

    async def summary_query(
        self,
        *,
        namespace: str | None = None,
        service_id: str | None = None,
        scope: str | None = None,
        include_ingress: bool = False,
        ingress_limit: int | None = None,
        ingress_scope: str | None = None,
        ingress_capabilities: Iterable[str] | None = None,
        ingress_tags: Iterable[str] | None = None,
        limit: int | None = None,
        page_token: str | None = None,
    ) -> SummaryQueryResponse:
        """Fetch summary discovery records with optional ingress hints."""
        ingress_capabilities = tuple(ingress_capabilities or ())
        ingress_tags = tuple(ingress_tags or ())
        include_ingress = include_ingress or bool(
            ingress_limit is not None
            or ingress_scope
            or ingress_capabilities
            or ingress_tags
        )
        if (
            namespace
            or service_id
            or scope
            or include_ingress
            or limit is not None
            or page_token
        ):
            request = SummaryQueryRequest(
                scope=scope,
                namespace=namespace,
                service_id=service_id,
                include_ingress=include_ingress,
                ingress_limit=ingress_limit,
                ingress_scope=ingress_scope,
                ingress_capabilities=ingress_capabilities,
                ingress_tags=ingress_tags,
                limit=limit,
                page_token=page_token,
            )
            payload = request.to_dict()
            result = (
                await self._call_any("summary_query", payload)
                if payload
                else await self._call_any("summary_query")
            )
        else:
            result = await self._call_any("summary_query")
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected summary query response, got {type(result).__name__}"
            )
        response = SummaryQueryResponse.from_dict(result)
        if response.ingress:
            self._ingress_hints.update(response.ingress)
        return response

    async def call_with_summary(
        self,
        summary: ServiceSummary,
        fun: str,
        *args: Any,
        locs: frozenset[str] | None = None,
        function_id: str | None = None,
        version_constraint: str | None = None,
        target_cluster: str | None = None,
        routing_topic: str | None = None,
        timeout: float | None = None,
        ingress: dict[str, tuple[str, ...]] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Call an RPC function preferring ingress hints from a ServiceSummary."""
        preferred_urls: tuple[str, ...] | None = None
        summary_cluster = summary.source_cluster
        if summary_cluster:
            if ingress and summary_cluster in ingress:
                preferred_urls = ingress[summary_cluster]
            else:
                preferred_urls = self._ingress_hints.get(
                    summary_cluster
                ) or self._cluster_endpoints.get(summary_cluster)
        return await self.call(
            fun,
            *args,
            locs=locs,
            function_id=function_id,
            version_constraint=version_constraint,
            target_cluster=target_cluster or summary_cluster,
            routing_topic=routing_topic,
            timeout=timeout,
            preferred_urls=preferred_urls,
            use_ingress_hints=False,
            enable_summary_redirect=False,
            **kwargs,
        )

    async def refresh_cluster_map(self) -> None:
        """Refresh cluster map and update endpoint scores."""
        async with self._lock:
            try:
                snapshot = await self.cluster_map()
            except Exception as exc:
                cluster_client_log.warning("Cluster map refresh failed: {}", exc)
                return
            endpoint_scores: dict[str, float] = {}
            endpoint_regions: dict[str, str] = {}
            cluster_endpoints: dict[str, list[str]] = {}
            for node in snapshot.nodes:
                load_score = node.load.load_score if node.load else 0.0
                for url in node.advertised_urls:
                    endpoint_scores[url] = load_score
                    if node.region:
                        endpoint_regions[url] = node.region
                    cluster_endpoints.setdefault(node.cluster_id, []).append(url)
                if not node.advertised_urls and node.node_id:
                    endpoint_scores[node.node_id] = load_score
                    if node.region:
                        endpoint_regions[node.node_id] = node.region
                    cluster_endpoints.setdefault(node.cluster_id, []).append(
                        node.node_id
                    )
            if endpoint_scores:
                self._endpoint_scores = endpoint_scores
                self._endpoint_regions = {
                    url: region
                    for url, region in endpoint_regions.items()
                    if url in endpoint_scores
                }
                self._endpoint_health = {
                    url: stats
                    for url, stats in self._endpoint_health.items()
                    if url in endpoint_scores
                }
                self._last_failure = {
                    url: ts
                    for url, ts in self._last_failure.items()
                    if url in endpoint_scores
                }
                self._cluster_endpoints = {
                    cluster_id: tuple(dict.fromkeys(urls))
                    for cluster_id, urls in cluster_endpoints.items()
                }

    async def _refresh_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.refresh_interval)
                await self.refresh_cluster_map()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                cluster_client_log.warning("Cluster map refresh loop error: {}", exc)

    async def _connect_seed(self) -> None:
        last_error: Exception | None = None
        for url in self.seed_urls:
            try:
                client = await self._ensure_client(url)
                await self._ensure_connected(client)
                return
            except Exception as exc:
                last_error = exc
                cluster_client_log.warning(
                    "Seed connection failed for {}: {}", url, exc
                )
                continue
        if last_error:
            raise last_error
        raise ConnectionError("No seed endpoints available for cluster client.")

    async def _ensure_client(self, url: str) -> Client:
        client = self._clients.get(url)
        if client is None:
            client = Client(url=url, full_log=self.full_log)
            self._clients[url] = client
        return client

    async def _ensure_connected(self, client: Client) -> None:
        transport = getattr(client, "_transport", None)
        if transport is not None and transport.connected:
            return
        await client.connect()

    def _candidate_urls(
        self, *, preferred_urls: Iterable[str] | None = None
    ) -> list[str]:
        now = time.time()
        scored = []
        for url, score in self._endpoint_scores.items():
            last_failure = self._last_failure.get(url, 0.0)
            if now - last_failure < self.failure_cooldown_seconds:
                continue
            health = self._endpoint_health.get(url)
            if health is not None:
                score += health.penalty(
                    now=now,
                    error_weight=self.error_weight,
                    latency_weight=self.latency_weight,
                    stale_after_seconds=self.health_stale_seconds,
                )
            scored.append((score, url))
        scored.sort(key=lambda entry: (entry[0], entry[1]))
        if not scored:
            return []
        urls = [url for _, url in scored]
        preferred_filter = tuple(preferred_urls) if preferred_urls else None
        if preferred_filter:
            preferred_set = {
                url for url in preferred_filter if url in self._endpoint_scores
            }
            if preferred_set:
                urls = [url for url in urls if url in preferred_set]
        else:
            preferred_region = self.preferred_region
            if preferred_region:
                region_urls = [
                    url
                    for url in urls
                    if self._endpoint_regions.get(url) == preferred_region
                ]
                if region_urls:
                    urls = region_urls
        if self._rr_index:
            offset = self._rr_index % len(urls)
            urls = urls[offset:] + urls[:offset]
        self._rr_index += 1
        return urls

    async def _call_on_url(
        self,
        url: str,
        fun: str,
        *args: Any,
        locs: frozenset[str] | None,
        function_id: str | None,
        version_constraint: str | None,
        target_cluster: str | None,
        routing_topic: str | None,
        timeout: float | None,
        **kwargs: Any,
    ) -> Any:
        client = await self._ensure_client(url)
        await self._ensure_connected(client)
        command = RPCCommand(
            name=fun,
            fun=fun,
            args=tuple(args),
            locs=locs or frozenset(),
            function_id=function_id,
            version_constraint=version_constraint,
            target_cluster=target_cluster,
            routing_topic=routing_topic,
            kwargs=kwargs,
        )
        start_time = time.time()
        try:
            result = await client.request(cmds=[command], timeout=timeout)
        except Exception:
            self._record_endpoint_error(url)
            raise
        latency_ms = (time.time() - start_time) * 1000.0
        self._record_endpoint_success(url, latency_ms)
        if isinstance(result, dict) and len(result) == 1:
            return list(result.values())[0]
        return result

    def _record_endpoint_success(self, url: str, latency_ms: float) -> None:
        stats = self._endpoint_health.get(url)
        if stats is None:
            stats = EndpointHealthStats()
            self._endpoint_health[url] = stats
        stats.record_success(latency_ms)

    def _record_endpoint_error(self, url: str) -> None:
        stats = self._endpoint_health.get(url)
        if stats is None:
            stats = EndpointHealthStats()
            self._endpoint_health[url] = stats
        stats.record_error()

    async def _call_any(self, fun: str, *args: Any) -> Any:
        candidates = self._candidate_urls()
        if not candidates:
            candidates = list(self.seed_urls)
        last_error: Exception | None = None
        for url in candidates:
            try:
                return await self._call_on_url(
                    url,
                    fun,
                    *args,
                    locs=None,
                    function_id=None,
                    version_constraint=None,
                    target_cluster=None,
                    routing_topic=None,
                    timeout=5.0,
                )
            except Exception as exc:
                last_error = exc
                self._last_failure[url] = time.time()
                continue
        if last_error:
            raise last_error
        raise ConnectionError("No cluster endpoints available for discovery call.")

    async def _attempt_summary_redirect(
        self,
        fun: str,
        *args: Any,
        locs: frozenset[str] | None,
        function_id: str | None,
        version_constraint: str | None,
        routing_topic: str | None,
        timeout: float | None,
        **kwargs: Any,
    ) -> Any | None:
        try:
            response = await self.summary_query(
                service_id=fun,
                scope=self.summary_redirect_scope,
                include_ingress=True,
                ingress_limit=self.summary_redirect_ingress_limit,
                ingress_scope=self.summary_redirect_ingress_scope,
            )
        except Exception as exc:
            cluster_client_log.warning(
                "Summary redirect lookup failed for {}: {}", fun, exc
            )
            return None
        if not response.items:
            return None
        for summary in response.items:
            if summary.service_id != fun:
                continue
            try:
                return await self.call_with_summary(
                    summary,
                    fun,
                    *args,
                    locs=locs,
                    function_id=function_id,
                    version_constraint=version_constraint,
                    routing_topic=routing_topic,
                    timeout=timeout,
                    ingress=response.ingress,
                    **kwargs,
                )
            except Exception as exc:
                last_failure = summary.source_cluster or "unknown"
                cluster_client_log.warning(
                    "Summary redirect failed for {} via {}: {}",
                    fun,
                    last_failure,
                    exc,
                )
                continue
        return None

    @staticmethod
    def _is_command_not_found(exc: Exception) -> bool:
        return isinstance(exc, MPREGException) and exc.rpc_error.code == 1001
