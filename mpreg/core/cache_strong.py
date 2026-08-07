"""ConsistencyLevel.STRONG majority-commit put coordinator.

Residual-free is a **CFT best-effort** claim: when ABORT is delivered, failed
puts leave no visible L1 for ``op_id`` on origin or replicas in the frozen
replica set R. Partial peer COMMIT + lost ABORT may leave peer L1 (not
residual-free). Success requires Q COMMIT_ACKs with origin-commit-last by
default. See ``aborts_peer_fail`` / ``last_abort_fail_peers``.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Protocol

from mpreg.core.cache_models import (
    CacheLevel,
    CacheMetadata,
    CacheOperationResult,
    GlobalCacheEntry,
    GlobalCacheKey,
)

class StrongErrorCode(IntEnum):
    """Operational STRONG codes (1015–1018). 1012 remains not-implemented."""

    INSUFFICIENT_QUORUM = 1015
    QUORUM_TIMEOUT = 1016
    STRONG_CONFLICT = 1017
    STRONG_PENDING_FULL = 1018

def format_residual_ops_hint(
    peers: Sequence[str] | None,
    op_id: str | None = None,
    *,
    namespace: str = "<ns>",
    key_id: str = "<id>",
    url_placeholder: str = "<ws>",
) -> str:
    """Operator remediation string when CFT residual candidates are known.

    Empty when ``peers`` is empty. Points at
    ``mpreg client cache-strong-retry-abort`` after network recovery.
    Still CFT best-effort — **not** automatic heal, residual-free proof, BFT,
    or SIEM orchestration.
    """
    clean = [p for p in dict.fromkeys(list(peers or [])) if p]
    if not clean:
        return ""
    oid = str(op_id or "").strip()
    oid_part = f" --op-id {oid}" if oid else " --op-id <op_id>"
    peer_parts = " ".join(f"--peer {p}" for p in clean)
    return (
        "hint: after network recovery, ops re-ABORT (not auto-heal): "
        f"uv run mpreg client cache-strong-retry-abort "
        f"--url {url_placeholder}{oid_part} "
        f"--namespace {namespace} --key {key_id} {peer_parts} "
        "(CFT best-effort; still fails while ABORT dropped)"
    )

@dataclass(frozen=True, slots=True)
class StrongVersion:
    logical_ts: int
    origin_node: str
    op_id: str

    def as_tuple(self) -> tuple[int, str, str]:
        return (self.logical_ts, self.origin_node, self.op_id)

    def __gt__(self, other: StrongVersion) -> bool:
        return self.as_tuple() > other.as_tuple()

    def to_dict(self) -> dict[str, Any]:
        return {
            "logical_ts": self.logical_ts,
            "origin_node": self.origin_node,
            "op_id": self.op_id,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> StrongVersion | None:
        if not raw:
            return None
        return cls(
            logical_ts=int(raw.get("logical_ts") or 0),
            origin_node=str(raw.get("origin_node") or ""),
            op_id=str(raw.get("op_id") or ""),
        )

@dataclass(slots=True)
class StrongPending:
    """Invisible prepare slot — must not be served by get/list."""

    key: GlobalCacheKey
    value: Any
    metadata: CacheMetadata
    strong_version: StrongVersion
    replica_set: tuple[str, ...]
    quorum: int
    expires_at: float
    pre_commit_backup: GlobalCacheEntry | None = None

@dataclass(frozen=True, slots=True)
class PrepareAck:
    node_id: str
    ok: bool
    reason: str = ""

@dataclass(frozen=True, slots=True)
class CommitAck:
    node_id: str
    ok: bool
    applied: bool = False
    reason: str = ""

class StrongPeerTransport(Protocol):
    """Minimal peer RPC surface used by the coordinator (in-process or fabric)."""

    async def strong_prepare(
        self,
        peer_id: str,
        *,
        key: GlobalCacheKey,
        value: Any,
        metadata: Any,
        strong_version: StrongVersion,
        replica_set: tuple[str, ...],
        quorum: int,
        cluster_id: str,
        timeout: float,
    ) -> PrepareAck: ...

    async def strong_commit(
        self,
        peer_id: str,
        *,
        op_id: str,
        key: GlobalCacheKey,
        strong_version: StrongVersion,
        cluster_id: str,
        timeout: float,
    ) -> CommitAck: ...

    async def strong_abort(
        self,
        peer_id: str,
        *,
        op_id: str,
        key: GlobalCacheKey,
        strong_version: StrongVersion,
        cluster_id: str,
        timeout: float,
    ) -> bool: ...

def _attach_strong_version(
    metadata: CacheMetadata, version: StrongVersion
) -> CacheMetadata:
    """Return metadata with strong_version stamped into access_patterns."""
    ap = dict(metadata.access_patterns or {})
    ap["strong_version"] = version.to_dict()
    return CacheMetadata(
        computation_cost_ms=metadata.computation_cost_ms,
        dependencies=set(metadata.dependencies),
        ttl_seconds=metadata.ttl_seconds,
        replication_policy=metadata.replication_policy,
        access_patterns=ap,
        geographic_hints=list(metadata.geographic_hints),
        quality_score=metadata.quality_score,
        created_by=metadata.created_by,
        size_estimate_bytes=metadata.size_estimate_bytes,
    )

def _entry_strong_version(entry: GlobalCacheEntry) -> StrongVersion | None:
    meta = getattr(entry, "metadata", None)
    if meta is None:
        return None
    ap = getattr(meta, "access_patterns", None) or {}
    if isinstance(ap, dict) and "strong_version" in ap:
        return StrongVersion.from_dict(ap.get("strong_version"))
    return None

def _entry_op_id(entry: GlobalCacheEntry) -> str | None:
    sv = _entry_strong_version(entry)
    return sv.op_id if sv else None

def majority_quorum(n: int) -> int:
    if n <= 0:
        return 0
    return n // 2 + 1

@dataclass(slots=True)
class StrongLocalBackend:
    """Local pending + L1 apply surface for the origin (and in-process peers).

    Optional ``on_visible_apply`` / ``on_visible_uncommit`` bridge peer-side
    commits into the real GlobalCacheManager L1 so fabric STRONG puts are
    readable via ``gcm.get`` on every committer (not only the origin).
    """

    node_id: str
    max_pending: int = 128
    on_visible_apply: Any | None = None  # Callable[[GlobalCacheEntry], None]
    on_visible_uncommit: Any | None = (
        None  # Callable[[GlobalCacheKey, GlobalCacheEntry | None], None]
    )
    _pending: dict[str, StrongPending] = field(default_factory=dict)
    _visible: dict[str, GlobalCacheEntry] = field(default_factory=dict)
    _key_op: dict[str, str] = field(default_factory=dict)  # key_str -> op_id
    _backups: dict[str, GlobalCacheEntry] = field(default_factory=dict)
    # Cumulative orphan pre-commit backups dropped by _prune_orphan_backups (T30/T32).
    backups_pruned_total: int = 0
    _logical_ts: int = 0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def _key_str(self, key: GlobalCacheKey) -> str:
        return f"{key.namespace}/{key.identifier}/{key.version}"

    def next_logical_ts(self) -> int:
        self._logical_ts = max(self._logical_ts + 1, int(time.time() * 1000))
        return self._logical_ts

    async def prepare(
        self,
        *,
        key: GlobalCacheKey,
        value: Any,
        metadata: Any,
        strong_version: StrongVersion,
        replica_set: tuple[str, ...],
        quorum: int,
        ttl_s: float,
    ) -> PrepareAck:
        async with self._lock:
            if self.node_id not in replica_set:
                return PrepareAck(self.node_id, False, "not_in_replica_set")
            if (
                len(self._pending) >= self.max_pending
                and strong_version.op_id not in self._pending
            ):
                return PrepareAck(self.node_id, False, "pending_full")
            existing = self._pending.get(strong_version.op_id)
            if existing is not None:
                return PrepareAck(self.node_id, True)
            meta = metadata if isinstance(metadata, CacheMetadata) else CacheMetadata()
            backup = self._visible.get(self._key_str(key))
            self._pending[strong_version.op_id] = StrongPending(
                key=key,
                value=value,
                metadata=meta,
                strong_version=strong_version,
                replica_set=replica_set,
                quorum=quorum,
                expires_at=time.time() + ttl_s,
                pre_commit_backup=backup,
            )
            return PrepareAck(self.node_id, True)

    async def commit(self, *, op_id: str, key: GlobalCacheKey) -> CommitAck:
        async with self._lock:
            pending = self._pending.get(op_id)
            if pending is None:
                ks = self._key_str(key)
                ent = self._visible.get(ks)
                if ent is not None and _entry_op_id(ent) == op_id:
                    return CommitAck(self.node_id, True, applied=True)
                return CommitAck(
                    self.node_id, False, applied=False, reason="no_pending"
                )

            # Expired prepare must not become a late visible commit.
            if pending.expires_at <= time.time():
                del self._pending[op_id]
                return CommitAck(
                    self.node_id, False, applied=False, reason="expired"
                )

            ks = self._key_str(pending.key)
            current = self._visible.get(ks)
            incoming = pending.strong_version
            if current is not None:
                cur_sv = _entry_strong_version(current)
                if cur_sv is not None and cur_sv > incoming:
                    del self._pending[op_id]
                    return CommitAck(
                        self.node_id, True, applied=False, reason="lww_lost"
                    )

            stamped = _attach_strong_version(pending.metadata, incoming)
            entry = GlobalCacheEntry(
                key=pending.key,
                value=pending.value,
                metadata=stamped,
            )
            self._visible[ks] = entry
            self._key_op[ks] = op_id
            self._pending.pop(op_id, None)
            if pending.pre_commit_backup is not None:
                self._backups[op_id] = pending.pre_commit_backup
            # Drop backups for ops no longer live (lost-ABORT CFT leak path).
            self._prune_orphan_backups()
            apply_cb = self.on_visible_apply
            if apply_cb is not None:
                try:
                    apply_cb(entry)
                except Exception:  # noqa: BLE001
                    pass
            return CommitAck(self.node_id, True, applied=True)

    async def abort(self, *, op_id: str, key: GlobalCacheKey) -> bool:
        async with self._lock:
            self._pending.pop(op_id, None)
            ks = self._key_str(key)
            ent = self._visible.get(ks)
            restored: GlobalCacheEntry | None = None
            did_uncommit = False
            if ent is not None and _entry_op_id(ent) == op_id:
                backup = self._backups.pop(op_id, None)
                if backup is not None:
                    self._visible[ks] = backup
                    self._key_op[ks] = _entry_op_id(backup) or ""
                    restored = backup
                else:
                    self._visible.pop(ks, None)
                    self._key_op.pop(ks, None)
                    restored = None
                did_uncommit = True
            else:
                self._backups.pop(op_id, None)
            self._prune_orphan_backups()
            if did_uncommit:
                uncommit_cb = self.on_visible_uncommit
                if uncommit_cb is not None:
                    try:
                        uncommit_cb(key, restored)
                    except Exception:  # noqa: BLE001
                        pass
            return True

    def _prune_orphan_backups(self) -> int:
        """Remove pre-commit backups whose op_id is no longer live.

        A backup is live only while its op is the visible ``_key_op`` value or
        still pending. Lost ABORT after COMMIT otherwise retained backups
        forever (CFT resource leak). Late ABORT for a non-visible op already
        no-ops uncommit — pruning is equivalent and safe.
        """
        live: set[str] = set(self._key_op.values()) | set(self._pending.keys())
        dead = [oid for oid in self._backups if oid not in live]
        for oid in dead:
            del self._backups[oid]
        n = len(dead)
        if n:
            self.backups_pruned_total += n
        return n

    def get_visible(self, key: GlobalCacheKey) -> GlobalCacheEntry | None:
        return self._visible.get(self._key_str(key))

    def has_pending(self, op_id: str) -> bool:
        return op_id in self._pending

    def pending_count(self) -> int:
        return len(self._pending)

    def purge_expired_pending(self, now: float | None = None) -> int:
        """Drop expired **pending prepares only**.

        Does **not** uncommit residual L1 after COMMIT apply — pending is
        already removed on successful apply. CFT residual heal is ABORT
        delivery or later LWW success put, not this purge.

        Also runs orphan pre-commit backup GC (same as commit/abort) so the
        server purge loop cannot leave unbounded backups if commit-path prune
        was skipped.
        """
        now = now if now is not None else time.time()
        dead = [oid for oid, p in self._pending.items() if p.expires_at <= now]
        for oid in dead:
            del self._pending[oid]
        self._prune_orphan_backups()
        return len(dead)

    def visible_count(self) -> int:
        """Number of visible L1 strong entries (includes any CFT residuals)."""
        return len(self._visible)

    def backups_count(self) -> int:
        """Pre-commit backups retained for live visible/pending ops (orphan GC on commit/abort)."""
        return len(self._backups)

@dataclass(slots=True)
class StrongPutCoordinator:
    """Coordinates residual-free majority-commit STRONG puts.

    Residual-free is a **CFT best-effort** claim: ABORT is retried but not
    guaranteed delivered. Partial peer COMMIT apply + lost ABORT can leave
    peer L1 until a later delivered ABORT or LWW overwrite by a successful
    put — **not** cleared by ``purge_expired_pending`` (pending is already
    gone after COMMIT apply). Not BFT, not fsync recovery.
    Counters ``aborts_peer_ok`` / ``aborts_peer_fail`` expose that boundary.
    """

    origin_id: str
    local: StrongLocalBackend
    transport: StrongPeerTransport
    cluster_id: str = "default"
    replica_factor: int = 3
    min_replicas: int = 3
    lab_single_node: bool = False
    require_origin_in_quorum: bool = True
    prepare_timeout_s: float = 2.0
    commit_timeout_s: float = 2.0
    pending_ttl_s: float = 30.0
    origin_commit_last: bool = True
    # Best-effort ABORT delivery attempts per peer (CFT; not BFT reliable abort).
    abort_attempts: int = 3
    aborts_peer_ok: int = 0
    aborts_peer_fail: int = 0
    # T36: last peers that exhausted ABORT retries (CFT residual candidates).
    # Ops diagnostics only — not residual-free proof, not auto-heal.
    last_abort_fail_peers: list[str] = field(default_factory=list)
    last_abort_fail_op_id: str = ""
    # Bounded ring of recent abort-fail events: {op_id, peers, ts}.
    recent_abort_fails: deque[dict[str, Any]] = field(
        default_factory=lambda: deque(maxlen=32)
    )

    def select_replica_set(self, eligible: Sequence[str]) -> tuple[str, ...] | None:
        peers = list(dict.fromkeys(eligible))  # stable unique
        if self.require_origin_in_quorum and self.origin_id not in peers:
            peers = [self.origin_id, *peers]
        if self.origin_id in peers:
            peers = [self.origin_id] + [p for p in peers if p != self.origin_id]
        min_r = 1 if self.lab_single_node else self.min_replicas
        if len(peers) < min_r:
            return None
        n = min(self.replica_factor, len(peers))
        if n < min_r:
            return None
        return tuple(peers[:n])

    async def strong_put(
        self,
        key: GlobalCacheKey,
        value: Any,
        *,
        metadata: CacheMetadata | None = None,
        eligible_peers: Sequence[str],
        op_id: str | None = None,
    ) -> CacheOperationResult:
        if metadata is None:
            metadata = CacheMetadata()

        replica_set = self.select_replica_set(eligible_peers)
        if replica_set is None:
            return CacheOperationResult(
                success=False,
                error_message=(
                    f"INSUFFICIENT_QUORUM: need min_replicas="
                    f"{1 if self.lab_single_node else self.min_replicas}"
                ),
                error_code=int(StrongErrorCode.INSUFFICIENT_QUORUM),
            )

        Q = majority_quorum(len(replica_set))
        oid = op_id or str(uuid.uuid4())
        logical_ts = self.local.next_logical_ts()
        # Multi-origin sequential puts: wall-ms clocks often collide. Bump
        # above any locally visible version for this key so LWW does not lose
        # solely on origin_node tie-break after a peer-origin commit we hold.
        existing = self.local.get_visible(key)
        if existing is not None:
            cur_sv = _entry_strong_version(existing)
            if cur_sv is not None and logical_ts <= cur_sv.logical_ts:
                logical_ts = cur_sv.logical_ts + 1
                self.local._logical_ts = max(self.local._logical_ts, logical_ts)
        strong_version = StrongVersion(
            logical_ts=logical_ts, origin_node=self.origin_id, op_id=oid
        )

        prepare_ok: list[str] = []
        contacted: set[str] = set()

        oack = await self.local.prepare(
            key=key,
            value=value,
            metadata=metadata,
            strong_version=strong_version,
            replica_set=replica_set,
            quorum=Q,
            ttl_s=self.pending_ttl_s,
        )
        contacted.add(self.origin_id)
        if not oack.ok:
            code = (
                int(StrongErrorCode.STRONG_PENDING_FULL)
                if oack.reason == "pending_full"
                else int(StrongErrorCode.INSUFFICIENT_QUORUM)
            )
            fail_peers = await self._abort_all(
                replica_set, contacted, oid, key, strong_version
            )
            return CacheOperationResult(
                success=False,
                error_message=f"prepare origin failed: {oack.reason}",
                error_code=code,
                operation_id=oid,
                quorum_info=self._qi_with_abort_fails(
                    {"replica_set": list(replica_set), "quorum": Q},
                    fail_peers,
                ),
            )
        prepare_ok.append(self.origin_id)

        peer_ids = [p for p in replica_set if p != self.origin_id]
        if peer_ids:
            results = await asyncio.gather(
                *[
                    self._prepare_peer(
                        p, key, value, metadata, strong_version, replica_set, Q
                    )
                    for p in peer_ids
                ],
                return_exceptions=True,
            )
            for p, res in zip(peer_ids, results, strict=True):
                contacted.add(p)
                if isinstance(res, PrepareAck) and res.ok:
                    prepare_ok.append(p)

        if len(prepare_ok) < Q:
            fail_peers = await self._abort_all(
                replica_set, contacted, oid, key, strong_version
            )
            return CacheOperationResult(
                success=False,
                error_message=(
                    f"QUORUM_TIMEOUT/INSUFFICIENT prepare acks {len(prepare_ok)}/{Q}"
                ),
                error_code=int(StrongErrorCode.QUORUM_TIMEOUT)
                if prepare_ok
                else int(StrongErrorCode.INSUFFICIENT_QUORUM),
                operation_id=oid,
                quorum_info=self._qi_with_abort_fails(
                    {
                        "replica_set": list(replica_set),
                        "quorum": Q,
                        "prepare_acks": list(prepare_ok),
                    },
                    fail_peers,
                ),
            )

        commit_applied: list[str] = []
        peer_prepare_ok = [p for p in prepare_ok if p != self.origin_id]

        if self.origin_commit_last and peer_prepare_ok:
            peer_results = await asyncio.gather(
                *[
                    self._commit_peer(p, oid, key, strong_version)
                    for p in peer_prepare_ok
                ],
                return_exceptions=True,
            )
            for p, res in zip(peer_prepare_ok, peer_results, strict=True):
                if isinstance(res, CommitAck) and res.ok and res.applied:
                    commit_applied.append(p)
                elif isinstance(res, CommitAck) and res.ok and not res.applied:
                    if res.reason == "lww_lost":
                        fail_peers = await self._abort_all(
                            replica_set, contacted, oid, key, strong_version
                        )
                        return CacheOperationResult(
                            success=False,
                            error_message="STRONG_CONFLICT: lost LWW on peer",
                            error_code=int(StrongErrorCode.STRONG_CONFLICT),
                            operation_id=oid,
                            quorum_info=self._qi_with_abort_fails(
                                {
                                    "replica_set": list(replica_set),
                                    "quorum": Q,
                                    "commit_acks": list(commit_applied),
                                },
                                fail_peers,
                            ),
                        )

            need_peers = max(0, Q - 1)
            if len(commit_applied) < need_peers:
                fail_peers = await self._abort_all(
                    replica_set, contacted, oid, key, strong_version
                )
                return CacheOperationResult(
                    success=False,
                    error_message=(
                        f"QUORUM_TIMEOUT commit peers "
                        f"{len(commit_applied)}/{need_peers}"
                    ),
                    error_code=int(StrongErrorCode.QUORUM_TIMEOUT),
                    operation_id=oid,
                    quorum_info=self._qi_with_abort_fails(
                        {
                            "replica_set": list(replica_set),
                            "quorum": Q,
                            "commit_acks": list(commit_applied),
                        },
                        fail_peers,
                    ),
                )

            ocommit = await self.local.commit(op_id=oid, key=key)
            if not ocommit.ok or not ocommit.applied:
                fail_peers = await self._abort_all(
                    replica_set, contacted, oid, key, strong_version
                )
                code = (
                    int(StrongErrorCode.STRONG_CONFLICT)
                    if ocommit.reason == "lww_lost"
                    else int(StrongErrorCode.QUORUM_TIMEOUT)
                )
                return CacheOperationResult(
                    success=False,
                    error_message=f"origin commit failed: {ocommit.reason}",
                    error_code=code,
                    operation_id=oid,
                    quorum_info=self._qi_with_abort_fails(
                        {
                            "replica_set": list(replica_set),
                            "quorum": Q,
                            "commit_acks": list(commit_applied),
                        },
                        fail_peers,
                    ),
                )
            commit_applied.append(self.origin_id)
        else:
            ocommit = await self.local.commit(op_id=oid, key=key)
            if not ocommit.ok or not ocommit.applied:
                fail_peers = await self._abort_all(
                    replica_set, contacted, oid, key, strong_version
                )
                return CacheOperationResult(
                    success=False,
                    error_message=f"origin commit failed: {ocommit.reason}",
                    error_code=int(StrongErrorCode.QUORUM_TIMEOUT),
                    operation_id=oid,
                    quorum_info=self._qi_with_abort_fails(
                        {
                            "replica_set": list(replica_set),
                            "quorum": Q,
                            "commit_acks": list(commit_applied),
                        },
                        fail_peers,
                    ),
                )
            commit_applied.append(self.origin_id)

        if len(commit_applied) < Q:
            fail_peers = await self._abort_all(
                replica_set, contacted, oid, key, strong_version
            )
            return CacheOperationResult(
                success=False,
                error_message=f"commit quorum failed {len(commit_applied)}/{Q}",
                error_code=int(StrongErrorCode.QUORUM_TIMEOUT),
                operation_id=oid,
                quorum_info=self._qi_with_abort_fails(
                    {
                        "replica_set": list(replica_set),
                        "quorum": Q,
                        "commit_acks": list(commit_applied),
                    },
                    fail_peers,
                ),
            )

        # Residual-free on non-committers: peers that prepared but did not apply
        # COMMIT must drop pending (e.g. commit dropped on a minority peer while
        # Q still formed). Best-effort ABORT; does not revoke successful commits.
        committed = set(commit_applied)
        stale = [p for p in prepare_ok if p not in committed]
        if stale:
            await asyncio.gather(
                *[
                    self._abort_peer(p, oid, key, strong_version)
                    for p in stale
                    if p != self.origin_id
                ],
                return_exceptions=True,
            )
            if self.origin_id in stale:
                await self.local.abort(op_id=oid, key=key)

        entry = self.local.get_visible(key)
        return CacheOperationResult(
            success=True,
            cache_level=CacheLevel.L1,
            entry=entry,
            operation_id=oid,
            quorum_info={
                "replica_set": list(replica_set),
                "quorum": Q,
                "commit_acks": list(commit_applied),
                "strong_version": strong_version.to_dict(),
                "aborted_non_committers": list(stale),
            },
        )

    async def _prepare_peer(
        self,
        peer_id: str,
        key: GlobalCacheKey,
        value: Any,
        metadata: Any,
        strong_version: StrongVersion,
        replica_set: tuple[str, ...],
        quorum: int,
    ) -> PrepareAck:
        try:
            return await asyncio.wait_for(
                self.transport.strong_prepare(
                    peer_id,
                    key=key,
                    value=value,
                    metadata=metadata,
                    strong_version=strong_version,
                    replica_set=replica_set,
                    quorum=quorum,
                    cluster_id=self.cluster_id,
                    timeout=self.prepare_timeout_s,
                ),
                timeout=self.prepare_timeout_s,
            )
        except Exception as exc:  # noqa: BLE001
            return PrepareAck(peer_id, False, reason=type(exc).__name__)

    async def _commit_peer(
        self,
        peer_id: str,
        op_id: str,
        key: GlobalCacheKey,
        strong_version: StrongVersion,
    ) -> CommitAck:
        try:
            return await asyncio.wait_for(
                self.transport.strong_commit(
                    peer_id,
                    op_id=op_id,
                    key=key,
                    strong_version=strong_version,
                    cluster_id=self.cluster_id,
                    timeout=self.commit_timeout_s,
                ),
                timeout=self.commit_timeout_s,
            )
        except Exception as exc:  # noqa: BLE001
            return CommitAck(peer_id, False, reason=type(exc).__name__)

    async def _abort_all(
        self,
        replica_set: tuple[str, ...],
        contacted: set[str],
        op_id: str,
        key: GlobalCacheKey,
        strong_version: StrongVersion,
    ) -> list[str]:
        """Best-effort ABORT all contacted peers; return peers that still failed.

        Failed peers are CFT residual *candidates* (may hold L1 if they applied
        COMMIT). Not residual-free proof. Empty list when all ABORTs delivered
        or no peers contacted.
        """
        targets = set(replica_set) | set(contacted)
        await self.local.abort(op_id=op_id, key=key)
        peers = [p for p in targets if p != self.origin_id]
        if not peers:
            self.last_abort_fail_peers = []
            self.last_abort_fail_op_id = op_id
            return []
        # Best-effort multi-attempt ABORT (CFT). Track last-attempt failures.
        attempts = max(1, int(self.abort_attempts))
        pending = set(peers)
        for attempt in range(attempts):
            if not pending:
                break
            results = await asyncio.gather(
                *[
                    self._abort_peer(p, op_id, key, strong_version, _count=False)
                    for p in pending
                ],
                return_exceptions=True,
            )
            still: set[str] = set()
            for p, res in zip(list(pending), results, strict=True):
                ok = res is True
                if ok:
                    self.aborts_peer_ok += 1
                else:
                    still.add(p)
            pending = still
            if pending and attempt + 1 < attempts:
                await asyncio.sleep(0)  # yield between attempts
        failed = sorted(pending)
        for _p in failed:
            self.aborts_peer_fail += 1
        self.last_abort_fail_peers = list(failed)
        self.last_abort_fail_op_id = op_id
        if failed:
            self.recent_abort_fails.append(
                {
                    "op_id": op_id,
                    "peers": list(failed),
                    "ts": time.time(),
                    "key": f"{key.namespace}/{key.identifier}",
                }
            )
        return failed

    def _qi_with_abort_fails(
        self,
        base: dict[str, Any],
        abort_fail_peers: Sequence[str] | None,
    ) -> dict[str, Any]:
        """Attach abort_fail_peers to quorum_info (ops; CFT residual candidates)."""
        out = dict(base)
        peers = list(abort_fail_peers or [])
        out["abort_fail_peers"] = peers
        # Honest flag: non-empty means ABORT exhausted for those peers.
        out["abort_best_effort_residual_candidates"] = bool(peers)
        return out

    async def retry_abort(
        self,
        key: GlobalCacheKey,
        op_id: str,
        *,
        peers: Sequence[str] | None = None,
        strong_version: StrongVersion | None = None,
    ) -> dict[str, Any]:
        """Best-effort re-ABORT to residual-candidate peers (CFT; not auto-heal).

        Intended after network recovery when ``last_abort_fail_peers`` / failed
        put ``quorum_info.abort_fail_peers`` listed peers that exhausted ABORT
        retries. Still **not** residual-free under continued loss; not BFT.

        Returns ``{ok_peers, fail_peers, op_id, attempts}``. Updates
        ``last_abort_fail_peers`` to peers that still fail after this retry.
        """
        oid = str(op_id or "")
        if not oid:
            return {
                "ok_peers": [],
                "fail_peers": [],
                "op_id": "",
                "attempts": 0,
                "error": "op_id required",
            }
        requested = list(peers) if peers is not None else list(self.last_abort_fail_peers)
        requested = [p for p in dict.fromkeys(requested) if p]
        # Self is never ABORTed via peer transport; local.abort handles it.
        # Client RPC may land on the residual peer (peers=[self]) — that must
        # still clear local L1 rather than no-op after filtering self out.
        remote = [p for p in requested if p != self.origin_id]
        self_targeted = self.origin_id in requested
        sv = strong_version or StrongVersion(
            logical_ts=0, origin_node=self.origin_id, op_id=oid
        )
        ok_peers: list[str] = []
        failed: list[str] = []
        attempts = 0
        if remote:
            attempts = max(1, int(self.abort_attempts))
            pending = set(remote)
            for attempt in range(attempts):
                if not pending:
                    break
                results = await asyncio.gather(
                    *[
                        self._abort_peer(p, oid, key, sv, _count=False)
                        for p in pending
                    ],
                    return_exceptions=True,
                )
                still: set[str] = set()
                for p, res in zip(list(pending), results, strict=True):
                    if res is True:
                        self.aborts_peer_ok += 1
                        ok_peers.append(p)
                    else:
                        still.add(p)
                pending = still
                if pending and attempt + 1 < attempts:
                    await asyncio.sleep(0)
            failed = sorted(pending)
            for _p in failed:
                self.aborts_peer_fail += 1

        # Always best-effort local abort: clears origin residual, and also the
        # residual when this coordinator *is* the residual peer (RPC fan-in).
        await self.local.abort(op_id=oid, key=key)
        if self_targeted and self.origin_id not in failed:
            ok_peers.append(self.origin_id)

        self.last_abort_fail_peers = list(failed)
        self.last_abort_fail_op_id = oid
        if failed:
            self.recent_abort_fails.append(
                {
                    "op_id": oid,
                    "peers": list(failed),
                    "ts": time.time(),
                    "key": f"{key.namespace}/{key.identifier}",
                    "retry": True,
                }
            )
        return {
            "ok_peers": sorted(set(ok_peers)),
            "fail_peers": failed,
            "op_id": oid,
            "attempts": attempts,
            "cleared": not failed,
        }

    async def _abort_peer(
        self,
        peer_id: str,
        op_id: str,
        key: GlobalCacheKey,
        strong_version: StrongVersion,
        *,
        _count: bool = True,
    ) -> bool:
        try:
            ok = await asyncio.wait_for(
                self.transport.strong_abort(
                    peer_id,
                    op_id=op_id,
                    key=key,
                    strong_version=strong_version,
                    cluster_id=self.cluster_id,
                    timeout=self.commit_timeout_s,
                ),
                timeout=self.commit_timeout_s,
            )
            ok_b = bool(ok)
            if _count:
                if ok_b:
                    self.aborts_peer_ok += 1
                else:
                    self.aborts_peer_fail += 1
            return ok_b
        except Exception:  # noqa: BLE001
            if _count:
                self.aborts_peer_fail += 1
            return False

@dataclass(slots=True)
class InProcessStrongTransport:
    """Maps peer_id → StrongLocalBackend for unit tests."""

    backends: dict[str, StrongLocalBackend] = field(default_factory=dict)
    drop_prepare: set[str] = field(default_factory=set)
    drop_commit: set[str] = field(default_factory=set)
    drop_abort: set[str] = field(default_factory=set)
    fail_prepare: set[str] = field(default_factory=set)

    def register(self, backend: StrongLocalBackend) -> None:
        self.backends[backend.node_id] = backend

    async def strong_prepare(
        self,
        peer_id: str,
        *,
        key: GlobalCacheKey,
        value: Any,
        metadata: Any,
        strong_version: StrongVersion,
        replica_set: tuple[str, ...],
        quorum: int,
        cluster_id: str,
        timeout: float,
    ) -> PrepareAck:
        if peer_id in self.drop_prepare:
            await asyncio.sleep(timeout + 0.05)
            raise TimeoutError("dropped_prepare")
        if peer_id in self.fail_prepare:
            return PrepareAck(peer_id, False, "injected_fail")
        be = self.backends.get(peer_id)
        if be is None:
            return PrepareAck(peer_id, False, "unknown_peer")
        return await be.prepare(
            key=key,
            value=value,
            metadata=metadata,
            strong_version=strong_version,
            replica_set=replica_set,
            quorum=quorum,
            ttl_s=max(timeout, 1.0),
        )

    async def strong_commit(
        self,
        peer_id: str,
        *,
        op_id: str,
        key: GlobalCacheKey,
        strong_version: StrongVersion,
        cluster_id: str,
        timeout: float,
    ) -> CommitAck:
        if peer_id in self.drop_commit:
            await asyncio.sleep(timeout + 0.05)
            raise TimeoutError("dropped_commit")
        be = self.backends.get(peer_id)
        if be is None:
            return CommitAck(peer_id, False, reason="unknown_peer")
        return await be.commit(op_id=op_id, key=key)

    async def strong_abort(
        self,
        peer_id: str,
        *,
        op_id: str,
        key: GlobalCacheKey,
        strong_version: StrongVersion,
        cluster_id: str,
        timeout: float,
    ) -> bool:
        if peer_id in self.drop_abort:
            # Simulate lost abort — pending GC / TTL must still residual-free.
            return False
        be = self.backends.get(peer_id)
        if be is None:
            return False
        return await be.abort(op_id=op_id, key=key)
