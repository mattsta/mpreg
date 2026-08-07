"""
Global Distributed Caching System for MPREG.

This module implements a sophisticated multi-tier global caching system with:
- L1: Memory Cache (S4LRU, Cost-Based Eviction)
- L2: Persistent Cache (SSD/NVMe Storage)
- L3: Distributed Cache (Fabric Sync)
- L4: Fabric Federation (Global Replication)

The system follows MPREG's clean dataclass architecture and supports
fabric-based cache synchronization and geographic replication.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

from loguru import logger

from mpreg.fabric.cache_federation import FabricCacheProtocol

from .cache_models import (
    CacheLevel,
    CacheMetadata,
    CacheOperationResult,
    CacheOptions,
    CachePerformanceMetrics,
    ConsistencyLevel,
    GlobalCacheEntry,
    GlobalCacheKey,
    ReplicationStrategy,
)
from .caching import CacheConfiguration, SmartCacheManager
from .namespace_policy import (
    NamespacePolicyEngine,
    get_actor_cluster_id,
    get_actor_tenant_id,
)
from .persistence.cache_store import CacheL2Store
from .persistence.registry import PersistenceRegistry
from .serialization import JsonSerializer
from .task_manager import ManagedObject
from .topic_taxonomy import TopicValidator

T = TypeVar("T")

cache_log = logger

@dataclass(slots=True)
class CacheReplicationPolicy:
    """Policy for cache replication across nodes."""

    strategy: ReplicationStrategy = ReplicationStrategy.GEOGRAPHIC
    min_replicas: int = 2
    max_replicas: int = 5
    preferred_regions: list[str] = field(default_factory=list)
    consistency_model: ConsistencyLevel = ConsistencyLevel.EVENTUAL
    conflict_resolution: str = (
        "last_writer_wins"  # "vector_clock", "timestamp", "custom"
    )
    replication_timeout_ms: int = 10000

@dataclass(slots=True)
class GlobalCacheConfiguration:
    """Configuration for global cache manager."""

    # Local cache configuration
    local_cache_config: CacheConfiguration = field(default_factory=CacheConfiguration)

    # Multi-tier settings
    enable_l2_persistent: bool = True
    enable_l3_distributed: bool = True
    enable_l4_federation: bool = True

    # Persistent cache settings (L2)
    persistent_cache_dir: Path = field(default_factory=lambda: Path("/tmp/mpreg_cache"))
    persistent_cache_size_mb: int = 1000  # 1GB

    # Distributed cache settings (L3)
    gossip_interval_seconds: float = 30.0
    gossip_batch_size: int = 100

    # Fabric federation cache settings (L4)
    federation_sync_interval_seconds: float = 300.0
    cross_region_replication: bool = True

    # Replication settings
    default_replication_policy: CacheReplicationPolicy = field(
        default_factory=CacheReplicationPolicy
    )

    # Performance settings
    operation_timeout_ms: int = 30000
    max_concurrent_operations: int = 100
    enable_compression: bool = True

    # Geographic settings
    local_region: str = "unknown"
    local_cluster_id: str = ""

    # Bound L3/L4 replication work queue (drop-oldest under pressure).
    pending_replications_maxsize: int = 4096

class GlobalCacheManager(ManagedObject):
    """
    Multi-tier global cache manager with intelligent replication and federation support.

    Provides a unified interface to:
    - L1: In-memory cache with S4LRU eviction
    - L2: Persistent SSD/NVMe cache
    - L3: Distributed cache via fabric sync
    - L4: Fabric federation cache via fabric sync (cross-cluster)
    """

    def __init__(
        self,
        config: GlobalCacheConfiguration,
        cache_protocol: FabricCacheProtocol | None = None,
        *,
        persistence_registry: PersistenceRegistry | None = None,
        namespace_policy: NamespacePolicyEngine | None = None,
    ) -> None:
        super().__init__(name=f"GlobalCacheManager-{id(self)}")
        self.config = config
        self.cache_protocol = cache_protocol
        self._persistence_registry = persistence_registry
        self.namespace_policy = namespace_policy

        # Initialize L1 memory cache
        self.l1_cache = SmartCacheManager[Any](config.local_cache_config)

        # Initialize operation tracking
        self.active_operations: dict[str, asyncio.Task[Any]] = {}
        self.operation_semaphore = asyncio.Semaphore(config.max_concurrent_operations)

        # Initialize statistics
        self.operation_stats: dict[str, int] = defaultdict(int)
        self.performance_metrics: dict[CacheLevel, list[float]] = defaultdict(list)

        # Initialize replication tracking (bounded; drop-oldest under backpressure)
        self.replication_state: dict[GlobalCacheKey, set[str]] = defaultdict(set)
        max_pending = max(
            1, int(getattr(config, "pending_replications_maxsize", 4096) or 4096)
        )
        self.pending_replications: asyncio.Queue[tuple[str, GlobalCacheKey, Any]] = (
            asyncio.Queue(maxsize=max_pending)
        )
        self.pending_replications_dropped: int = 0
        # Optional OBS sink: callable(n: int) when replication work is dropped.
        self._metrics_replication_drop: Any = None

        # ConsistencyLevel.STRONG majority-commit coordinator (opt-in; default None → 1012).
        self._strong_coordinator: Any = None
        self._strong_backend: Any = None
        self._strong_metrics: dict[str, int] = defaultdict(int)
        # Bounded ring of recent STRONG put durations (ms) for operator SLIs — not WAN SLA.
        self._strong_latency_ms: list[float] = []
        self._strong_latency_max: int = 256

        # Initialize namespace index for efficient namespace operations
        self.namespace_index: dict[str, set[GlobalCacheKey]] = defaultdict(set)

        # Initialize L2 persistent cache if enabled
        self.l2_cache: dict[str, GlobalCacheEntry] = {}
        self._l2_store: CacheL2Store | None = None
        if config.enable_l2_persistent:
            self._init_persistent_cache()

        # Start background tasks using task manager
        self._start_background_tasks()

    def attach_cache_protocol(self, cache_protocol: FabricCacheProtocol) -> None:
        """Attach a fabric cache protocol for L3 distributed cache support."""
        self.cache_protocol = cache_protocol

    def attach_namespace_policy(self, engine: NamespacePolicyEngine | None) -> None:
        """Bind or replace the namespace/tenant data-plane gate."""
        self.namespace_policy = engine

    def attach_strong_coordinator(self, coordinator: Any) -> None:
        """Bind STRONG put coordinator (majority-commit barrier)."""
        self._strong_coordinator = coordinator
        self._strong_backend = getattr(coordinator, "local", None)

    async def strong_retry_abort(
        self,
        key: GlobalCacheKey,
        op_id: str,
        *,
        peers: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        """Best-effort re-ABORT for CFT residual candidates (ops-driven).

        Wraps ``StrongPutCoordinator.retry_abort``. Use after network recovery
        when ``last_abort_fail_peers`` / failed-put ``quorum_info.abort_fail_peers``
        listed peers that exhausted ABORT. Still CFT — not automatic heal, not
        residual-free while ABORT is still lost, not BFT.

        Returns coordinator result dict (``ok_peers``, ``fail_peers``,
        ``cleared``, …) or ``{error, …}`` when coordinator unbound.
        """
        from mpreg.core.errors import MpregErrorCode

        coord = self._strong_coordinator
        if coord is None or not hasattr(coord, "retry_abort"):
            self._strong_metrics["retry_abort_unbound"] += 1
            return {
                "ok_peers": [],
                "fail_peers": list(peers or []),
                "op_id": str(op_id or ""),
                "attempts": 0,
                "cleared": False,
                "error": "strong coordinator unbound",
                "error_code": int(MpregErrorCode.UNSUPPORTED_CONSISTENCY),
            }
        prev_ok = int(getattr(coord, "aborts_peer_ok", 0) or 0)
        prev_fail = int(getattr(coord, "aborts_peer_fail", 0) or 0)
        out = await coord.retry_abort(
            key, op_id, peers=list(peers) if peers is not None else None
        )
        try:
            d_ok = int(getattr(coord, "aborts_peer_ok", 0) or 0) - prev_ok
            d_fail = int(getattr(coord, "aborts_peer_fail", 0) or 0) - prev_fail
            if d_ok > 0:
                self._strong_metrics["aborts_peer_ok"] += d_ok
            if d_fail > 0:
                self._strong_metrics["aborts_peer_fail"] += d_fail
            self._strong_metrics["retry_abort_calls"] += 1
            if out.get("cleared"):
                self._strong_metrics["retry_abort_cleared"] += 1
            else:
                self._strong_metrics["retry_abort_still_fail"] += 1
        except Exception:  # noqa: BLE001
            pass
        return dict(out) if isinstance(out, dict) else {"raw": out}

    async def _strong_put(
        self,
        key: GlobalCacheKey,
        value: Any,
        *,
        metadata: CacheMetadata,
        options: CacheOptions,
    ) -> CacheOperationResult:
        """Majority-commit STRONG put, or 1012 when coordinator unbound."""
        from mpreg.core.errors import MpregErrorCode

        coord = self._strong_coordinator
        if coord is None:
            self._strong_metrics["refused_disabled"] += 1
            return CacheOperationResult(
                success=False,
                error_message=(
                    "ConsistencyLevel.STRONG is disabled "
                    "(set cache_strong_enabled=true with eligible peers). "
                    "Use EVENTUAL or WEAK, or enable the majority-commit path."
                ),
                error_code=int(MpregErrorCode.UNSUPPORTED_CONSISTENCY),
            )

        # Eligible peers: origin + fabric cache SYNC peers when transport present
        eligible: list[str] = [str(coord.origin_id)]
        if self.cache_protocol is not None:
            try:
                peers = self.cache_protocol.peer_ids()
                eligible.extend(str(p) for p in peers if str(p) not in eligible)
            except Exception:  # noqa: BLE001
                pass
        # Also accept transport peer_ids if protocol thin
        transport = getattr(coord, "transport", None)
        if transport is not None and hasattr(transport, "peer_ids"):
            try:
                for p in transport.peer_ids(exclude=None):
                    if str(p) not in eligible:
                        eligible.append(str(p))
            except Exception:  # noqa: BLE001
                pass

        # Snapshot abort counters before put so we can attribute deltas
        prev_ok = int(getattr(coord, "aborts_peer_ok", 0) or 0)
        prev_fail = int(getattr(coord, "aborts_peer_fail", 0) or 0)
        t0 = time.perf_counter()
        result = await coord.strong_put(
            key, value, metadata=metadata, eligible_peers=eligible
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        self._record_strong_latency(elapsed_ms)
        # Pull CFT abort best-effort counters from coordinator
        try:
            d_ok = int(getattr(coord, "aborts_peer_ok", 0) or 0) - prev_ok
            d_fail = int(getattr(coord, "aborts_peer_fail", 0) or 0) - prev_fail
            if d_ok > 0:
                self._strong_metrics["aborts_peer_ok"] += d_ok
            if d_fail > 0:
                self._strong_metrics["aborts_peer_fail"] += d_fail
        except Exception:  # noqa: BLE001
            pass
        if result.success and result.entry is not None:
            # Apply visible entry into real L1 only after quorum success
            self._put_to_l1(result.entry)
            self._add_to_namespace_index(key)
            self._strong_metrics["puts_ok"] += 1
        else:
            self._strong_metrics["puts_fail"] += 1
            code = getattr(result, "error_code", None)
            if code is not None:
                self._strong_metrics[f"fail_code_{int(code)}"] += 1
        return result

    def _record_strong_latency(self, elapsed_ms: float) -> None:
        ring = self._strong_latency_ms
        ring.append(float(elapsed_ms))
        max_n = self._strong_latency_max
        if len(ring) > max_n:
            del ring[: len(ring) - max_n]

    def strong_metrics_snapshot(self) -> dict[str, Any]:
        """Operator-facing STRONG put metrics (process-local; not WAN SLA)."""
        coord = self._strong_coordinator
        be = self._strong_backend
        pending = 0
        visible = 0
        backups = 0
        backups_pruned = 0
        if be is not None and hasattr(be, "pending_count"):
            try:
                pending = int(be.pending_count())
            except Exception:  # noqa: BLE001
                pending = 0
        if be is not None and hasattr(be, "visible_count"):
            try:
                visible = int(be.visible_count())
            except Exception:  # noqa: BLE001
                visible = 0
        if be is not None and hasattr(be, "backups_count"):
            try:
                backups = int(be.backups_count())
            except Exception:  # noqa: BLE001
                backups = 0
        if be is not None:
            try:
                backups_pruned = int(getattr(be, "backups_pruned_total", 0) or 0)
            except Exception:  # noqa: BLE001
                backups_pruned = 0
        samples = list(self._strong_latency_ms)
        lat: dict[str, float | int] = {
            "sample_count": len(samples),
            "last_ms": float(samples[-1]) if samples else 0.0,
        }
        if samples:
            ordered = sorted(samples)
            lat["p50_ms"] = float(ordered[len(ordered) // 2])
            lat["p99_ms"] = float(ordered[max(0, int(len(ordered) * 0.99) - 1)])
            lat["max_ms"] = float(ordered[-1])
            lat["avg_ms"] = float(sum(ordered) / len(ordered))
        cfg: dict[str, Any] = {}
        if coord is not None:
            cfg = {
                "origin_id": getattr(coord, "origin_id", None),
                "replica_factor": getattr(coord, "replica_factor", None),
                "min_replicas": getattr(coord, "min_replicas", None),
                "prepare_timeout_s": getattr(coord, "prepare_timeout_s", None),
                "commit_timeout_s": getattr(coord, "commit_timeout_s", None),
                "pending_ttl_s": getattr(coord, "pending_ttl_s", None),
                "lab_single_node": bool(getattr(coord, "lab_single_node", False)),
                "abort_attempts": getattr(coord, "abort_attempts", None),
                "aborts_peer_ok": int(getattr(coord, "aborts_peer_ok", 0) or 0),
                "aborts_peer_fail": int(getattr(coord, "aborts_peer_fail", 0) or 0),
                # T36: CFT residual candidates (ops only; not residual-free proof)
                "last_abort_fail_peers": list(
                    getattr(coord, "last_abort_fail_peers", None) or []
                ),
                "last_abort_fail_op_id": str(
                    getattr(coord, "last_abort_fail_op_id", "") or ""
                ),
                "recent_abort_fails": list(
                    getattr(coord, "recent_abort_fails", None) or []
                )[-8:],
            }
            # Prefer live coordinator totals when GCM counters lag (direct coord use)
            live_ok = int(getattr(coord, "aborts_peer_ok", 0) or 0)
            live_fail = int(getattr(coord, "aborts_peer_fail", 0) or 0)
            if live_ok > int(self._strong_metrics.get("aborts_peer_ok", 0)):
                self._strong_metrics["aborts_peer_ok"] = live_ok
            if live_fail > int(self._strong_metrics.get("aborts_peer_fail", 0)):
                self._strong_metrics["aborts_peer_fail"] = live_fail
        # Always surface CFT abort counters (0 when unused) for ops honesty.
        counters = dict(self._strong_metrics)
        counters.setdefault("aborts_peer_ok", 0)
        counters.setdefault("aborts_peer_fail", 0)
        counters.setdefault("retry_abort_calls", 0)
        counters.setdefault("retry_abort_cleared", 0)
        counters.setdefault("retry_abort_still_fail", 0)
        counters.setdefault("puts_ok", 0)
        counters.setdefault("puts_fail", 0)
        counters.setdefault("gets_refused", 0)
        counters.setdefault("deletes_refused", 0)
        # Surface prune counter inside counters for prom/ops (process-local).
        counters["backups_pruned"] = backups_pruned
        return {
            "enabled": coord is not None,
            "pending_count": pending,
            # T29/T32: local L1 / backup sizes (not residual-free proof; ops only)
            "visible_count": visible,
            "backups_count": backups,
            "backups_pruned_total": backups_pruned,
            "counters": counters,
            "latency_ms": lat,
            "coordinator": cfg,
        }

    def strong_status(self) -> dict[str, Any]:
        """Compact STRONG readiness for clients/operators."""
        snap = self.strong_metrics_snapshot()
        c = snap["counters"]
        return {
            "enabled": snap["enabled"],
            "pending_count": snap["pending_count"],
            "visible_count": int(snap.get("visible_count") or 0),
            "backups_count": int(snap.get("backups_count") or 0),
            "backups_pruned_total": int(snap.get("backups_pruned_total") or 0),
            "puts_ok": int(c.get("puts_ok", 0)),
            "puts_fail": int(c.get("puts_fail", 0)),
            "refused_disabled": int(c.get("refused_disabled", 0)),
            "gets_refused": int(c.get("gets_refused", 0)),
            "deletes_refused": int(c.get("deletes_refused", 0)),
            "coordinator": snap.get("coordinator") or {},
            "capabilities": {
                "put_majority_commit": bool(snap["enabled"]),
                "get_quorum": False,  # v1.1
                "delete_quorum": False,  # v1.1
                "local_ryw_after_put": True,
                "cft_only": True,  # not BFT
                "abort_best_effort": True,  # lost ABORT may leave peer L1
                # pending TTL is not residual GC after COMMIT apply
                "pending_ttl_clears_residual_l1": False,
                # T41: retry_abort is ops-driven API — not automatic background heal
                "retry_abort_ops_driven": True,
            },
            "aborts_peer_ok": int(c.get("aborts_peer_ok", 0)),
            "aborts_peer_fail": int(c.get("aborts_peer_fail", 0)),
            # T36: last peers that exhausted ABORT (CFT residual candidates)
            "last_abort_fail_peers": list(
                (snap.get("coordinator") or {}).get("last_abort_fail_peers") or []
            ),
            "last_abort_fail_op_id": str(
                (snap.get("coordinator") or {}).get("last_abort_fail_op_id") or ""
            ),
            "recent_abort_fails": list(
                (snap.get("coordinator") or {}).get("recent_abort_fails") or []
            ),
            # T38: retry_abort ops counters (process-local)
            "retry_abort_calls": int(c.get("retry_abort_calls", 0)),
            "retry_abort_cleared": int(c.get("retry_abort_cleared", 0)),
            "retry_abort_still_fail": int(c.get("retry_abort_still_fail", 0)),
        }

    def _enqueue_replication(
        self, op: str, key: GlobalCacheKey, entry: Any = None
    ) -> None:
        """Enqueue replication work; drop-oldest when the bounded queue is full."""
        item = (op, key, entry)
        q = self.pending_replications
        try:
            q.put_nowait(item)
            return
        except asyncio.QueueFull:
            pass
        try:
            q.get_nowait()
            self._note_replication_drop()
        except asyncio.QueueEmpty:
            pass
        try:
            q.put_nowait(item)
        except asyncio.QueueFull:
            self._note_replication_drop()

    def _note_replication_drop(self) -> None:
        self.pending_replications_dropped += 1
        self.operation_stats["replication_drops"] = (
            int(self.operation_stats.get("replication_drops", 0)) + 1
        )
        sink = getattr(self, "_metrics_replication_drop", None)
        if callable(sink):
            with contextlib.suppress(Exception):
                sink(1)

    def attach_metrics_sink(self, *, on_replication_drop: Any = None) -> None:
        """Attach optional Prom/metrics callbacks (OBS-04)."""
        if on_replication_drop is not None:
            self._metrics_replication_drop = on_replication_drop

    def _data_plane_allowed(self, namespace: str, *, write: bool) -> tuple[bool, str]:
        engine = self.namespace_policy
        if engine is None or not engine.enabled:
            return True, "policy_disabled"
        decision = engine.allows_data_access(
            namespace,
            actor_cluster=get_actor_cluster_id()
            or self.config.local_cluster_id
            or None,
            actor_tenant_id=get_actor_tenant_id(),
            write=write,
        )
        return decision.allowed, decision.reason

    def _init_persistent_cache(self) -> None:
        """Initialize persistent cache storage."""
        if self._persistence_registry is None:
            try:
                self.config.persistent_cache_dir.mkdir(parents=True, exist_ok=True)
                cache_log.info(
                    "Initialized in-memory L2 cache with persistent dir {}",
                    self.config.persistent_cache_dir,
                )
            except Exception as e:
                cache_log.error(f"Failed to initialize persistent cache: {e}")
                self.config.enable_l2_persistent = False
            return

        try:
            from mpreg.core.persistence.cache_store import CacheL2Store

            kv_store = self._persistence_registry.key_value_store("cache.l2")
            self._l2_store = CacheL2Store(store=kv_store, serializer=JsonSerializer())
            cache_log.info("Initialized persistence-backed L2 cache store")
        except Exception as e:
            cache_log.error(f"Failed to initialize persistence-backed cache: {e}")
            self.config.enable_l2_persistent = False

    def _start_background_tasks(self) -> None:
        """Start background maintenance tasks using task manager."""
        try:
            # Create managed tasks that will be properly cleaned up
            self.create_task(self._replication_worker(), name="replication_worker")
            self.create_task(self._cleanup_worker(), name="cleanup_worker")

            if self.config.enable_l3_distributed or self.config.enable_l4_federation:
                self.create_task(self._cache_sync_worker(), name="cache_sync_worker")

            cache_log.debug(
                f"Started {len(self._task_manager)} background tasks for GlobalCacheManager"
            )
        except RuntimeError:
            # No event loop running, skip background tasks
            cache_log.debug("No event loop running, skipping background tasks")

    async def get(
        self, key: GlobalCacheKey, options: CacheOptions | None = None
    ) -> CacheOperationResult:
        """
        Retrieve value from multi-tier cache.

        Searches cache levels in order: L1 → L2 → L3 → L4.

        ConsistencyLevel.STRONG on get is not a product (v1): always refuse with
        1012. Local read-your-write after STRONG put uses EVENTUAL/WEAK get (L1
        + strong-backend promote). Quorum get is v1.1.
        """
        if options is None:
            options = CacheOptions()

        if options.consistency_level is ConsistencyLevel.STRONG:
            from mpreg.core.errors import MpregErrorCode

            self._strong_metrics["gets_refused"] += 1
            return CacheOperationResult(
                success=False,
                error_message=(
                    "ConsistencyLevel.STRONG get is not implemented "
                    "(quorum read is v1.1). After STRONG put, use EVENTUAL/WEAK "
                    "get for local RYW (L1 + peer bridge)."
                ),
                error_code=int(MpregErrorCode.UNSUPPORTED_CONSISTENCY),
            )

        allowed, reason = self._data_plane_allowed(key.namespace, write=False)
        if not allowed:
            return CacheOperationResult(
                success=False,
                error_message=f"namespace_policy_denied:{reason}",
            )

        str(uuid.uuid4())
        start_time = time.time()

        async with self.operation_semaphore:
            try:
                # Try L1 memory cache first
                if CacheLevel.L1 in options.cache_levels:
                    local_key = key.to_local_key()
                    l1_value = self.l1_cache.get(local_key)

                    if l1_value is not None:
                        self.operation_stats["l1_hits"] += 1
                        lookup_time = (time.time() - start_time) * 1000

                        # Convert back to GlobalCacheEntry if needed
                        if isinstance(l1_value, GlobalCacheEntry):
                            l1_value.access()
                            return CacheOperationResult(
                                success=True,
                                cache_level=CacheLevel.L1,
                                entry=l1_value,
                                performance=CachePerformanceMetrics(
                                    lookup_time_ms=lookup_time,
                                    network_hops=0,
                                    cache_efficiency=1.0,
                                ),
                            )

                    # Peer STRONG commits land in StrongLocalBackend first;
                    # bridge miss → promote into real L1 for subsequent gets.
                    be = self._strong_backend
                    if be is not None and hasattr(be, "get_visible"):
                        try:
                            strong_ent = be.get_visible(key)
                        except Exception:  # noqa: BLE001
                            strong_ent = None
                        if strong_ent is not None and isinstance(
                            strong_ent, GlobalCacheEntry
                        ):
                            self._put_to_l1(strong_ent)
                            self.operation_stats["l1_hits"] += 1
                            lookup_time = (time.time() - start_time) * 1000
                            strong_ent.access()
                            return CacheOperationResult(
                                success=True,
                                cache_level=CacheLevel.L1,
                                entry=strong_ent,
                                performance=CachePerformanceMetrics(
                                    lookup_time_ms=lookup_time,
                                    network_hops=0,
                                    cache_efficiency=1.0,
                                ),
                            )

                # Try L2 persistent cache
                if (
                    CacheLevel.L2 in options.cache_levels
                    and self.config.enable_l2_persistent
                ):
                    l2_result = await self._get_from_l2(key, options)
                    if l2_result.success:
                        # Promote to L1
                        if l2_result.entry:
                            self._promote_to_l1(l2_result.entry)
                        self.operation_stats["l2_hits"] += 1
                        return l2_result

                # Try L3 distributed cache
                if (
                    CacheLevel.L3 in options.cache_levels
                    and self.config.enable_l3_distributed
                ):
                    l3_result = await self._get_from_l3(key, options)
                    if l3_result.success:
                        # Promote to L1 and L2
                        if l3_result.entry:
                            self._promote_to_l1(l3_result.entry)
                            if self.config.enable_l2_persistent:
                                await self._put_to_l2(l3_result.entry)
                        self.operation_stats["l3_hits"] += 1
                        return l3_result

                # Try L4 federation cache
                if (
                    CacheLevel.L4 in options.cache_levels
                    and self.config.enable_l4_federation
                ):
                    l4_result = await self._get_from_l4(key, options)
                    if l4_result.success:
                        # Promote to all lower levels
                        if l4_result.entry:
                            self._promote_to_l1(l4_result.entry)
                            if self.config.enable_l2_persistent:
                                await self._put_to_l2(l4_result.entry)
                        self.operation_stats["l4_hits"] += 1
                        return l4_result

                # Cache miss
                self.operation_stats["misses"] += 1
                return CacheOperationResult(
                    success=False, error_message="Cache miss across all levels"
                )

            except Exception as e:
                cache_log.error(f"Cache get operation failed for {key}: {e}")
                return CacheOperationResult(
                    success=False, error_message=f"Cache operation error: {e}"
                )

    async def put(
        self,
        key: GlobalCacheKey,
        value: Any,
        metadata: CacheMetadata | None = None,
        options: CacheOptions | None = None,
    ) -> CacheOperationResult:
        """
        Store value in multi-tier cache with replication.
        """
        if metadata is None:
            metadata = CacheMetadata()

        if options is None:
            options = CacheOptions()

        allowed, reason = self._data_plane_allowed(key.namespace, write=True)
        if not allowed:
            return CacheOperationResult(
                success=False,
                error_message=f"namespace_policy_denied:{reason}",
            )

        # COR-01: STRONG only via majority-commit coordinator when enabled.
        # Refuse before any local write / namespace index side effects.
        if options.consistency_level is ConsistencyLevel.STRONG:
            return await self._strong_put(
                key, value, metadata=metadata, options=options
            )

        str(uuid.uuid4())
        start_time = time.time()

        async with self.operation_semaphore:
            try:
                # Create cache entry
                entry = GlobalCacheEntry(key=key, value=value, metadata=metadata)
                entry.metadata.created_by = self.config.local_cluster_id

                # Update namespace index
                self._add_to_namespace_index(key)

                # Store in requested cache levels
                success_levels = []

                if CacheLevel.L1 in options.cache_levels:
                    self._put_to_l1(entry)
                    success_levels.append(CacheLevel.L1)

                if (
                    CacheLevel.L2 in options.cache_levels
                    and self.config.enable_l2_persistent
                ):
                    await self._put_to_l2(entry)
                    success_levels.append(CacheLevel.L2)

                fabric_written = False
                if (
                    CacheLevel.L3 in options.cache_levels
                    and self.config.enable_l3_distributed
                ):
                    await self._put_to_l3(entry, options)
                    success_levels.append(CacheLevel.L3)
                    fabric_written = True

                if (
                    CacheLevel.L4 in options.cache_levels
                    and self.config.enable_l4_federation
                    and not fabric_written
                ):
                    await self._put_to_l4(entry, options)
                    success_levels.append(CacheLevel.L4)

                # Schedule replication if needed
                if options.replication_factor > 1:
                    self._enqueue_replication("put", key, entry)

                self.operation_stats["puts"] += 1
                lookup_time = (time.time() - start_time) * 1000

                return CacheOperationResult(
                    success=True,
                    cache_level=success_levels[0] if success_levels else None,
                    entry=entry,
                    performance=CachePerformanceMetrics(
                        lookup_time_ms=lookup_time, network_hops=0, cache_efficiency=1.0
                    ),
                )

            except Exception as e:
                cache_log.error(f"Cache put operation failed for {key}: {e}")
                return CacheOperationResult(
                    success=False, error_message=f"Cache put error: {e}"
                )

    async def delete(
        self, key: GlobalCacheKey, options: CacheOptions | None = None
    ) -> CacheOperationResult:
        """
        Delete value from multi-tier cache.

        ConsistencyLevel.STRONG on delete is not a product (v1): always refuse
        with 1012 (no majority-delete barrier). Quorum delete is v1.1.
        """
        if options is None:
            options = CacheOptions()

        if options.consistency_level is ConsistencyLevel.STRONG:
            from mpreg.core.errors import MpregErrorCode

            self._strong_metrics["deletes_refused"] += 1
            return CacheOperationResult(
                success=False,
                error_message=(
                    "ConsistencyLevel.STRONG delete is not implemented "
                    "(quorum delete is v1.1). Use EVENTUAL/WEAK delete for "
                    "local/best-effort eviction only."
                ),
                error_code=int(MpregErrorCode.UNSUPPORTED_CONSISTENCY),
            )

        async with self.operation_semaphore:
            try:
                deleted_levels = []

                # Remove from namespace index
                self._remove_from_namespace_index(key)

                # Delete from L1
                if CacheLevel.L1 in options.cache_levels:
                    local_key = key.to_local_key()
                    if self.l1_cache.evict(local_key):
                        deleted_levels.append(CacheLevel.L1)

                # Delete from L2
                if (
                    CacheLevel.L2 in options.cache_levels
                    and self.config.enable_l2_persistent
                ):
                    if await self._delete_from_l2(key):
                        deleted_levels.append(CacheLevel.L2)

                # Delete from fabric (distributed/federated)
                fabric_deleted = False
                if (
                    CacheLevel.L3 in options.cache_levels
                    and self.config.enable_l3_distributed
                ):
                    self._enqueue_replication("delete", key, None)
                    deleted_levels.append(CacheLevel.L3)
                    fabric_deleted = True

                if (
                    CacheLevel.L4 in options.cache_levels
                    and self.config.enable_l4_federation
                    and not fabric_deleted
                ):
                    self._enqueue_replication("delete", key, None)
                    deleted_levels.append(CacheLevel.L4)

                self.operation_stats["deletes"] += 1

                return CacheOperationResult(
                    success=len(deleted_levels) > 0,
                    cache_level=deleted_levels[0] if deleted_levels else None,
                )

            except Exception as e:
                cache_log.error(f"Cache delete operation failed for {key}: {e}")
                return CacheOperationResult(
                    success=False, error_message=f"Cache delete error: {e}"
                )

    async def invalidate(
        self,
        pattern: str,
        /,
        *,
        options: CacheOptions | None = None,
        **kwargs: object,
    ) -> CacheOperationResult:
        """Invalidate cache entries matching *pattern*.

        Phase H F8: keyword-only ``options``; unexpected kwargs raise a clear
        ``TypeError`` listing valid names (operators often guess ``key=``,
        ``namespace=``, ``prefix=``).
        """
        if kwargs:
            bad = ", ".join(sorted(kwargs))
            raise TypeError(
                f"GlobalCacheManager.invalidate() got unexpected keyword "
                f"argument(s): {bad}. Valid: pattern (positional), options=."
            )
        if options is None:
            options = CacheOptions()

        try:
            invalidated_count = 0
            keys_to_invalidate: list[GlobalCacheKey] = []

            def matches(key_obj: GlobalCacheKey) -> bool:
                key_str = str(key_obj)
                if pattern.endswith("*"):
                    return key_str.startswith(pattern[:-1])
                if pattern.startswith("*"):
                    return key_str.endswith(pattern[1:])
                return key_str == pattern or key_obj.namespace == pattern

            # Use namespace index when possible
            if "*" not in pattern and pattern in self.namespace_index:
                keys_to_invalidate.extend(self.namespace_index[pattern])
            else:
                for namespace_keys in self.namespace_index.values():
                    for key in namespace_keys:
                        if matches(key):
                            keys_to_invalidate.append(key)

            # Deduplicate keys
            seen = set()
            unique_keys = []
            for key in keys_to_invalidate:
                key_str = str(key)
                if key_str not in seen:
                    seen.add(key_str)
                    unique_keys.append(key)

            for key in unique_keys:
                # Remove namespace index entry
                self._remove_from_namespace_index(key)

                # L1
                if CacheLevel.L1 in options.cache_levels:
                    local_key = key.to_local_key()
                    if self.l1_cache.evict(local_key):
                        invalidated_count += 1

                # L2
                if (
                    CacheLevel.L2 in options.cache_levels
                    and self.config.enable_l2_persistent
                ):
                    if await self._delete_from_l2(key):
                        invalidated_count += 1

                # L3/L4 via fabric
                fabric_invalidated = False
                if (
                    CacheLevel.L3 in options.cache_levels
                    and self.config.enable_l3_distributed
                ):
                    if self.cache_protocol:
                        from mpreg.fabric.cache_federation import CacheOperationType

                        await self.cache_protocol.propagate_cache_operation(
                            CacheOperationType.INVALIDATE,
                            key,
                            invalidation_pattern=str(key),
                        )
                        fabric_invalidated = True

                if (
                    CacheLevel.L4 in options.cache_levels
                    and self.config.enable_l4_federation
                    and not fabric_invalidated
                ):
                    if self.cache_protocol:
                        from mpreg.fabric.cache_federation import CacheOperationType

                        await self.cache_protocol.propagate_cache_operation(
                            CacheOperationType.INVALIDATE,
                            key,
                            invalidation_pattern=str(key),
                        )

            self.operation_stats["invalidations"] += 1

            return CacheOperationResult(
                success=True, error_message=f"Invalidated {invalidated_count} entries"
            )

        except Exception as e:
            cache_log.error(f"Cache invalidation failed for pattern {pattern}: {e}")
            return CacheOperationResult(
                success=False, error_message=f"Cache invalidation error: {e}"
            )

    def _promote_to_l1(self, entry: GlobalCacheEntry) -> None:
        """Promote cache entry to L1 memory cache."""
        local_key = entry.key.to_local_key()

        # Use the smart cache manager to store the entry
        self.l1_cache.put(
            key=local_key,
            value=entry,  # Store the entire GlobalCacheEntry
            computation_cost_ms=entry.metadata.computation_cost_ms,
            dependencies=set(),  # Convert GlobalCacheKey deps to local cache keys later
            ttl_seconds=entry.metadata.ttl_seconds,
        )

    def _put_to_l1(self, entry: GlobalCacheEntry) -> None:
        """Store entry in L1 memory cache."""
        self._promote_to_l1(entry)

    async def _get_from_l2(
        self, key: GlobalCacheKey, options: CacheOptions
    ) -> CacheOperationResult:
        """Get entry from L2 persistent cache."""
        if self._l2_store is not None:
            entry = await self._l2_store.get(key)
        else:
            key_str = str(key)
            entry = self.l2_cache.get(key_str)

        if entry and not entry.is_expired():
            entry.access()
            return CacheOperationResult(
                success=True,
                cache_level=CacheLevel.L2,
                entry=entry,
                performance=CachePerformanceMetrics(
                    lookup_time_ms=1.0,  # Estimated SSD access time
                    network_hops=0,
                    cache_efficiency=0.9,
                ),
            )

        return CacheOperationResult(success=False, error_message="L2 miss")

    async def _put_to_l2(self, entry: GlobalCacheEntry) -> None:
        """Store entry in L2 persistent cache."""
        if self._l2_store is not None:
            await self._l2_store.put(entry)
        else:
            key_str = str(entry.key)
            self.l2_cache[key_str] = entry

    async def _delete_from_l2(self, key: GlobalCacheKey) -> bool:
        """Delete entry from L2 persistent cache."""
        if self._l2_store is not None:
            await self._l2_store.delete(key)
            return True
        key_str = str(key)
        return self.l2_cache.pop(key_str, None) is not None

    async def _get_from_l3(
        self, key: GlobalCacheKey, options: CacheOptions
    ) -> CacheOperationResult:
        """Get entry from L3 distributed cache via fabric sync."""
        if self.cache_protocol is None:
            return CacheOperationResult(success=False, error_message="L3 sync disabled")

        entry, source = await self.cache_protocol.fetch_entry(key)
        if entry and not entry.is_expired():
            entry.access()
            return CacheOperationResult(
                success=True,
                cache_level=CacheLevel.L3,
                entry=entry,
                performance=CachePerformanceMetrics(
                    lookup_time_ms=5.0,
                    network_hops=0 if source == self.cache_protocol.node_id else 1,
                    cache_efficiency=0.8,
                ),
            )

        return CacheOperationResult(success=False, error_message="L3 miss")

    async def _put_to_l3(self, entry: GlobalCacheEntry, options: CacheOptions) -> None:
        """Store entry in L3 distributed cache.

        STRONG must not use fire-and-forget L3 gossip — coordinator path only.
        """
        if options.consistency_level is ConsistencyLevel.STRONG:
            raise ValueError(
                "ConsistencyLevel.STRONG must use the majority-commit coordinator "
                "(cache_strong_enabled); L3 gossip is not a quorum barrier."
            )
        if self.cache_protocol is None:
            return
        from mpreg.fabric.cache_federation import CacheOperationType

        await self.cache_protocol.propagate_cache_operation(
            CacheOperationType.PUT,
            entry.key,
            entry.value,
            entry.metadata,
            options.consistency_level,
        )

    async def _get_from_l4(
        self, key: GlobalCacheKey, options: CacheOptions
    ) -> CacheOperationResult:
        """Get entry from L4 fabric federation cache."""
        if self.cache_protocol is None:
            return CacheOperationResult(
                success=False, error_message="L4 fabric cache not configured"
            )

        l3_result = await self._get_from_l3(key, options)
        if l3_result.success and l3_result.entry:
            return CacheOperationResult(
                success=True,
                cache_level=CacheLevel.L4,
                entry=l3_result.entry,
                performance=l3_result.performance,
            )
        return CacheOperationResult(success=False, error_message="L4 miss")

    async def _put_to_l4(self, entry: GlobalCacheEntry, options: CacheOptions) -> None:
        """Store entry in L4 fabric federation cache."""
        await self._put_to_l3(entry, options)

    async def _replication_worker(self) -> None:
        """Background worker for handling cache replication."""
        try:
            while True:
                try:
                    # Process pending replication operations
                    operation, key, data = await asyncio.wait_for(
                        self.pending_replications.get(), timeout=1.0
                    )

                    if operation == "put":
                        await self._handle_replication(key, data)
                    elif operation == "delete":
                        await self._handle_delete_replication(key)

                except TimeoutError:
                    continue
                except Exception as e:
                    cache_log.error(f"Replication worker error: {e}")
                    # If event loop is gone, break the loop
                    if "no running event loop" in str(
                        e
                    ) or "Event loop is closed" in str(e):
                        break
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            cache_log.debug("Replication worker cancelled")
        except Exception as e:
            cache_log.error(f"Replication worker fatal error: {e}")
        finally:
            cache_log.debug("Replication worker stopped")

    async def _cleanup_worker(self) -> None:
        """Background worker for cache cleanup and maintenance."""
        try:
            while True:
                try:
                    await asyncio.sleep(60)  # Run every minute

                    # Clean up expired L2 entries
                    expired_keys = []
                    for key_str, entry in self.l2_cache.items():
                        if entry.is_expired():
                            expired_keys.append(key_str)

                    for key_str in expired_keys:
                        self.l2_cache.pop(key_str, None)

                    if expired_keys:
                        cache_log.debug(
                            f"Cleaned up {len(expired_keys)} expired L2 cache entries"
                        )

                except Exception as e:
                    cache_log.error(f"Cleanup worker error: {e}")
                    # If event loop is gone, break the loop
                    if "no running event loop" in str(
                        e
                    ) or "Event loop is closed" in str(e):
                        break
        except asyncio.CancelledError:
            cache_log.debug("Cleanup worker cancelled")
        except Exception as e:
            cache_log.error(f"Cleanup worker fatal error: {e}")
        finally:
            cache_log.debug("Cleanup worker stopped")

    async def _cache_sync_worker(self) -> None:
        """Background worker for fabric cache synchronization."""
        while True:
            try:
                await asyncio.sleep(self.config.gossip_interval_seconds)
                if self.cache_protocol:
                    peer_ids = self.cache_protocol.peer_ids()
                    if peer_ids:
                        await self.cache_protocol.sync_cache_state(peer_ids[0])

            except Exception as e:
                cache_log.error(f"Cache sync worker error: {e}")

    async def _handle_replication(
        self, key: GlobalCacheKey, entry: GlobalCacheEntry
    ) -> None:
        """Handle replication of cache entry to other nodes."""
        cache_log.debug(f"Replicating cache entry {key}")
        if self.cache_protocol:
            from mpreg.fabric.cache_federation import CacheOperationType

            await self.cache_protocol.propagate_cache_operation(
                CacheOperationType.PUT, key, entry.value, entry.metadata
            )

    async def _handle_delete_replication(self, key: GlobalCacheKey) -> None:
        """Handle replication of cache deletion."""
        cache_log.debug(f"Replicating cache deletion {key}")
        if self.cache_protocol:
            from mpreg.fabric.cache_federation import CacheOperationType

            await self.cache_protocol.propagate_cache_operation(
                CacheOperationType.DELETE, key
            )

    def get_statistics(self) -> dict[str, Any]:
        """Get comprehensive cache statistics."""
        l1_stats = self.l1_cache.get_statistics()

        return {
            "operation_stats": dict(self.operation_stats),
            "l1_statistics": {
                "hits": l1_stats.hits,
                "misses": l1_stats.misses,
                "evictions": l1_stats.evictions,
                "memory_bytes": l1_stats.memory_bytes,
                "entry_count": l1_stats.entry_count,
                "hit_rate": l1_stats.hit_rate(),
            },
            "l2_statistics": {
                "entry_count": len(self.l2_cache),
                "enabled": self.config.enable_l2_persistent,
            },
            "replication_statistics": {
                "pending_operations": self.pending_replications.qsize(),
                "pending_maxsize": self.pending_replications.maxsize,
                "dropped_operations": int(self.pending_replications_dropped),
                "tracked_replications": len(self.replication_state),
            },
            "performance_metrics": {
                level.value: {
                    "avg_time_ms": sum(times) / len(times) if times else 0.0,
                    "sample_count": len(times),
                }
                for level, times in self.performance_metrics.items()
            },
            "strong": self.strong_metrics_snapshot(),
        }

    async def shutdown(self) -> None:
        """Shutdown cache manager and cleanup resources."""
        # Shutdown background tasks using task manager
        await super().shutdown()

        # Shutdown L1 cache
        await self.l1_cache.shutdown()

        if self.cache_protocol is not None:
            await self.cache_protocol.shutdown()

        cache_log.info("Global cache manager shutdown complete")

    def shutdown_sync(self) -> None:
        """Shutdown cache manager and cleanup resources (sync version for compatibility)."""
        # For sync context, cancel tasks directly and clear cache
        try:
            # Cancel tasks directly
            if self._task_manager.tasks:
                for task in self._task_manager.tasks:
                    if not task.done():
                        task.cancel()
                self._task_manager.tasks.clear()
                self._task_manager._shutdown_requested = True
        except Exception as e:
            cache_log.warning(f"Error during sync task cancellation: {e}")

        # Shutdown L1 cache synchronously
        self.l1_cache.shutdown_sync()

        cache_log.info("Global cache manager sync shutdown complete")

    def _add_to_namespace_index(self, key: GlobalCacheKey) -> None:
        """Add key to namespace index."""
        self.namespace_index[key.namespace].add(key)

    def _remove_from_namespace_index(self, key: GlobalCacheKey) -> None:
        """Remove key from namespace index."""
        namespace_keys = self.namespace_index.get(key.namespace)
        if namespace_keys:
            namespace_keys.discard(key)
            # Clean up empty namespace sets
            if not namespace_keys:
                del self.namespace_index[key.namespace]

    def get_namespace_keys(
        self, namespace: str, pattern: str | None = None
    ) -> list[GlobalCacheKey]:
        """Get all keys in a namespace, optionally filtered by pattern."""
        namespace_keys = self.namespace_index.get(namespace, set())

        if not pattern or pattern == "*":
            return list(namespace_keys)

        return [
            key
            for key in namespace_keys
            if TopicValidator.matches_pattern(key.identifier, pattern)
        ]
