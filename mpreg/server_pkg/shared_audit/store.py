"""Bounded G-Set store with per-origin retention watermarks."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any, Iterable

from mpreg.core.native_codec import JSONDecodeError, dumps_text, loads_text
from mpreg.server_pkg.shared_audit.models import (
    SharedAuditRecord,
    entry_sort_key,
    legacy_synthetic_id,
    merge_records,
)

@dataclass(frozen=True, slots=True)
class Watermark:
    """Per-origin retention floor: records with (ts, id) < watermark are dropped."""

    min_timestamp: float
    min_entry_id: str

    def sort_key(self) -> tuple[float, str]:
        return entry_sort_key(self.min_timestamp, self.min_entry_id)

    def covers(self, timestamp: float, entry_id: str) -> bool:
        """True if this record is at or above the watermark (retained)."""
        return entry_sort_key(timestamp, entry_id) >= self.sort_key()

    def to_dict(self) -> dict[str, Any]:
        return {
            "min_timestamp": self.min_timestamp,
            "min_entry_id": self.min_entry_id,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> Watermark | None:
        if not raw:
            return None
        return cls(
            min_timestamp=float(raw.get("min_timestamp") or 0.0),
            min_entry_id=str(raw.get("min_entry_id") or ""),
        )

def _max_watermark(a: Watermark | None, b: Watermark | None) -> Watermark | None:
    if a is None:
        return b
    if b is None:
        return a
    return a if a.sort_key() >= b.sort_key() else b

@dataclass(slots=True)
class SharedAuditStore:
    """In-memory G-Set of shared audit records with watermark compaction.

    Identity key is ``(cluster_id, entry_id)``. Compaction advances per-origin
    watermarks so anti-entropy cannot resurrect dropped ids.
    """

    max_entries: int = 2000
    cluster_id: str = ""
    local_node: str = ""
    persist_path: str | None = None
    _entries: dict[tuple[str, str], SharedAuditRecord] = field(default_factory=dict)
    _watermarks: dict[str, Watermark] = field(default_factory=dict)
    _lock: RLock = field(default_factory=RLock)
    merge_conflicts: int = 0
    rejected_cross_cluster: int = 0
    rejected_below_watermark: int = 0

    def __post_init__(self) -> None:
        if self.persist_path:
            self.load_jsonl(self.persist_path)

    def watermark_for(self, origin_node: str) -> Watermark | None:
        with self._lock:
            return self._watermarks.get(origin_node)

    def watermarks_snapshot(self) -> dict[str, Watermark]:
        with self._lock:
            return dict(self._watermarks)

    def size(self) -> int:
        with self._lock:
            return len(self._entries)

    def get(self, cluster_id: str, entry_id: str) -> SharedAuditRecord | None:
        with self._lock:
            return self._entries.get((cluster_id, entry_id))

    def snapshot(
        self,
        *,
        limit: int | None = None,
        origin_node: str | None = None,
        gossip_eligible_only: bool = False,
    ) -> list[SharedAuditRecord]:
        with self._lock:
            items = list(self._entries.values())
        if origin_node is not None:
            items = [r for r in items if r.origin_node == origin_node]
        if gossip_eligible_only:
            items = [r for r in items if r.gossip_eligible]
        items.sort(key=lambda r: r.sort_key())
        # Note: items[-0:] is the full list in Python — handle limit==0 explicitly.
        if limit is not None:
            if limit == 0:
                items = []
            elif limit > 0:
                items = items[-limit:]
        return items

    def snapshot_dicts(self, **kwargs: Any) -> list[dict[str, Any]]:
        return [r.to_dict() for r in self.snapshot(**kwargs)]

    def insert(self, record: SharedAuditRecord) -> SharedAuditRecord | None:
        """Insert or merge a record. Returns the stored winner, or None if rejected."""
        if self.cluster_id and record.cluster_id and record.cluster_id != self.cluster_id:
            self.rejected_cross_cluster += 1
            return None
        if not record.entry_id or not record.cluster_id:
            return None

        with self._lock:
            wm = self._watermarks.get(record.origin_node)
            if wm is not None and not wm.covers(record.timestamp, record.entry_id):
                self.rejected_below_watermark += 1
                return None

            key = record.identity()
            existing = self._entries.get(key)
            if existing is None:
                self._entries[key] = record
                winner = record
            else:
                winner, conflict = merge_records(existing, record)
                if conflict:
                    self.merge_conflicts += 1
                self._entries[key] = winner

            self._compact_unlocked()
            stored = self._entries.get(key)
            # May have been compacted away if over budget and oldest
            return stored

    def merge_watermark(self, origin_node: str, watermark: Watermark) -> None:
        """Monotonic watermark merge (componentwise max in total order)."""
        with self._lock:
            current = self._watermarks.get(origin_node)
            merged = _max_watermark(current, watermark)
            if merged is None:
                return
            self._watermarks[origin_node] = merged
            # Drop local entries below new watermark for this origin
            drop_keys = [
                k
                for k, r in self._entries.items()
                if r.origin_node == origin_node
                and not merged.covers(r.timestamp, r.entry_id)
            ]
            for k in drop_keys:
                del self._entries[k]

    def records_for_pull(
        self,
        *,
        requester_watermarks: dict[str, Watermark],
        limit: int = 200,
    ) -> list[SharedAuditRecord]:
        """Records at/above requester watermarks (anti-resurrection filter)."""
        with self._lock:
            out: list[SharedAuditRecord] = []
            for r in self._entries.values():
                if not r.gossip_eligible:
                    continue
                req_wm = requester_watermarks.get(r.origin_node)
                if req_wm is not None and not req_wm.covers(r.timestamp, r.entry_id):
                    continue
                out.append(r)
            out.sort(key=lambda x: x.sort_key())
            if limit == 0:
                out = []
            elif limit > 0:
                out = out[-limit:]
            return out

    def _compact_unlocked(self) -> None:
        if self.max_entries <= 0:
            return
        while len(self._entries) > self.max_entries:
            by_origin: dict[str, list[SharedAuditRecord]] = defaultdict(list)
            for r in self._entries.values():
                by_origin[r.origin_node].append(r)
            if not by_origin:
                break
            # Drop from origin with most retained entries
            origin = max(by_origin.keys(), key=lambda o: len(by_origin[o]))
            ordered = sorted(by_origin[origin], key=lambda r: r.sort_key())
            if not ordered:
                break
            victim = ordered[0]
            del self._entries[victim.identity()]
            remaining = ordered[1:]
            if remaining:
                first = remaining[0]
                self._watermarks[origin] = Watermark(
                    min_timestamp=first.timestamp, min_entry_id=first.entry_id
                )
            else:
                # Advanced past all known for origin — floor just above victim
                self._watermarks[origin] = Watermark(
                    min_timestamp=victim.timestamp,
                    min_entry_id=victim.entry_id + "\0",
                )

    def append_jsonl(self, record: SharedAuditRecord) -> None:
        if not self.persist_path:
            return
        path = Path(self.persist_path)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(dumps_text(record.to_dict()) + "\n")
        except OSError:
            pass

    def load_jsonl(self, path: str | Path | None = None) -> int:
        """Load schema v1 lines; legacy lines become non-gossip synthetic ids."""
        p = Path(path or self.persist_path or "")
        if not p.is_file():
            return 0
        try:
            lines = p.read_text(encoding="utf-8").splitlines()
        except OSError:
            return 0
        loaded = 0
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                raw = loads_text(line)
            except JSONDecodeError:
                continue
            if not isinstance(raw, dict):
                continue
            rec = self._record_from_jsonl_line(raw)
            if rec is None:
                continue
            if self.insert(rec) is not None:
                loaded += 1
        return loaded

    def _record_from_jsonl_line(self, raw: dict[str, Any]) -> SharedAuditRecord | None:
        entry_id = raw.get("entry_id")
        if entry_id:
            return SharedAuditRecord.from_dict(raw)
        # Legacy MgmtAuditEntry line — synthetic local-only id, never gossip
        cluster = str(raw.get("cluster_id") or self.cluster_id or "unknown")
        origin = str(raw.get("origin_node") or self.local_node or "local")
        legacy = {
            "event": raw.get("event"),
            "timestamp": raw.get("timestamp"),
            "actor": raw.get("actor"),
            "success": raw.get("success"),
            "detail": raw.get("detail"),
        }
        return SharedAuditRecord(
            schema_version=1,
            entry_id=legacy_synthetic_id(legacy),
            cluster_id=cluster,
            origin_node=origin,
            origin_url=str(raw.get("origin_url") or ""),
            event=str(raw.get("event") or "unknown"),
            timestamp=float(raw.get("timestamp") or 0.0),
            actor=raw.get("actor"),
            success=bool(raw.get("success", False)),
            detail=dict(raw.get("detail") or {}),
            gossip_eligible=False,
        )

    def insert_and_persist(self, record: SharedAuditRecord) -> SharedAuditRecord | None:
        stored = self.insert(record)
        if stored is not None and stored.gossip_eligible:
            self.append_jsonl(stored)
        elif stored is not None and not stored.gossip_eligible:
            # Still durable locally
            self.append_jsonl(stored)
        return stored

    def bulk_insert(self, records: Iterable[SharedAuditRecord]) -> int:
        n = 0
        for r in records:
            if self.insert(r) is not None:
                n += 1
        return n
