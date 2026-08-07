"""Live multi-process mesh helpers for DistLab (port-allocator safe).

Not WAN. Same-host MPREGServer processes only.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mpreg.core.cache_models import (
    CacheOptions,
    ConsistencyLevel,
    GlobalCacheKey,
)
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer
from mpreg.testing.distlab.history import History
from mpreg.testing.distlab.models import OpKind


def strong_settings(
    port: int,
    name: str,
    *,
    cluster_id: str = "distlab-live",
    peers: list[str] | None = None,
    replica_factor: int = 3,
    min_replicas: int = 3,
) -> MPREGSettings:
    return MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=name,
        cluster_id=cluster_id,
        resources={f"r-{name}"},
        peers=peers or [],
        log_level="ERROR",
        gossip_interval=0.25,
        monitoring_enabled=False,
        enable_default_cache=True,
        cache_strong_enabled=True,
        cache_strong_replica_factor=replica_factor,
        cache_strong_min_replicas=min_replicas,
        cache_strong_prepare_timeout_s=1.5,
        cache_strong_commit_timeout_s=1.5,
    )


def audit_settings(
    port: int,
    mon: int,
    name: str,
    audit_dir: str,
    *,
    cluster_id: str = "distlab-audit-live",
    peers: list[str] | None = None,
) -> MPREGSettings:
    return MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=name,
        cluster_id=cluster_id,
        resources={f"r-{name}"},
        peers=peers or [],
        log_level="ERROR",
        gossip_interval=0.3,
        monitoring_enabled=True,
        monitoring_port=mon,
        mgmt_audit_path=str(Path(audit_dir) / f"{name}.jsonl"),
        mgmt_audit_shared_enabled=True,
        mgmt_audit_shared_reconcile_interval_s=0.35,
        mgmt_audit_shared_gossip_targets=3,
    )


def both_settings(
    port: int,
    mon: int,
    name: str,
    audit_dir: str,
    *,
    cluster_id: str = "distlab-both",
    peers: list[str] | None = None,
) -> MPREGSettings:
    strong_settings(port, name, cluster_id=cluster_id, peers=peers)
    # rebuild with monitoring + audit
    return MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=name,
        cluster_id=cluster_id,
        resources={f"r-{name}"},
        peers=peers or [],
        log_level="ERROR",
        gossip_interval=0.3,
        monitoring_enabled=True,
        monitoring_port=mon,
        enable_default_cache=True,
        cache_strong_enabled=True,
        cache_strong_replica_factor=3,
        cache_strong_min_replicas=3,
        cache_strong_prepare_timeout_s=1.5,
        cache_strong_commit_timeout_s=1.5,
        mgmt_audit_path=str(Path(audit_dir) / f"{name}.jsonl"),
        mgmt_audit_shared_enabled=True,
        mgmt_audit_shared_reconcile_interval_s=0.35,
        mgmt_audit_shared_gossip_targets=3,
    )


async def wait_cache_peers(
    servers: Sequence[MPREGServer], *, timeout: float = 10.0
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = True
        for s in servers:
            tr = getattr(s, "_cache_fabric_transport", None)
            if tr is None or len(list(tr.peer_ids())) < len(servers) - 1:
                ok = False
                break
        if ok:
            return
        await asyncio.sleep(0.15)
    detail = []
    for s in servers:
        tr = getattr(s, "_cache_fabric_transport", None)
        detail.append((s.settings.name, list(tr.peer_ids()) if tr else None))
    raise AssertionError(f"cache peers not ready: {detail}")


async def wait_gossip_connected(
    servers: Sequence[MPREGServer], *, timeout: float = 12.0
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        hub = servers[0]
        try:
            n = sum(
                1
                for c in hub._get_all_peer_connections().values()
                if getattr(c, "is_connected", False)
            )
        except Exception:  # noqa: BLE001
            n = 0
        if n >= len(servers) - 1:
            return
        await asyncio.sleep(0.15)
    raise AssertionError("gossip mesh not connected")


async def wait_audit_cluster_events(
    servers: Sequence[MPREGServer],
    *,
    min_events: int,
    timeout: float = 18.0,
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = True
        for s in servers:
            body = s._mgmt_audit_snapshot(scope="cluster", limit=200)
            events = body.get("mutations") or body.get("entries") or []
            if len(events) < min_events:
                ok = False
                break
        if ok:
            return
        await asyncio.sleep(0.2)
    dumps = []
    for s in servers:
        body = s._mgmt_audit_snapshot(scope="cluster", limit=20)
        dumps.append((s.settings.name, body.get("mutation_count"), body.get("health")))
    raise AssertionError(f"audit did not converge min={min_events}: {dumps}")


@dataclass
class LiveStrongState:
    servers: list[MPREGServer]
    _keys: dict[str, GlobalCacheKey] = field(default_factory=dict)

    def key(self, logical: str) -> GlobalCacheKey:
        if logical not in self._keys:
            self._keys[logical] = GlobalCacheKey(
                namespace="distlab-live", identifier=logical, version="v1"
            )
        return self._keys[logical]

    def pending_count(self) -> int:
        n = 0
        for s in self.servers:
            be = getattr(s, "_strong_local_backend", None)
            if be is not None:
                n += be.pending_count()
        return n

    def visible_op_ids(self, key: str) -> set[str]:
        from mpreg.core.cache_strong import _entry_op_id

        gk = self.key(key)
        out: set[str] = set()
        for s in self.servers:
            be = getattr(s, "_strong_local_backend", None)
            if be is None:
                continue
            ent = be.get_visible(gk)
            if ent is not None:
                oid = _entry_op_id(ent)
                if oid:
                    out.add(oid)
        return out

    def replica_views(self, key: str) -> dict[str, tuple[Any, str | None] | None]:
        from mpreg.core.cache_strong import _entry_op_id

        gk = self.key(key)
        views: dict[str, tuple[Any, str | None] | None] = {}
        for s in self.servers:
            be = getattr(s, "_strong_local_backend", None)
            url = s.cluster.local_url
            if be is None:
                views[url] = None
                continue
            ent = be.get_visible(gk)
            if ent is None:
                views[url] = None
            else:
                views[url] = (ent.value, _entry_op_id(ent))
        return views

    def final_op_id(self, key: str) -> str | None:
        for v in self.replica_views(key).values():
            if v is not None:
                return v[1]
        return None

    def final_value(self, key: str) -> Any:
        for v in self.replica_views(key).values():
            if v is not None:
                return v[0]
        return None


@dataclass
class LiveStrongSUT:
    """Wrap running MPREGServer list for DistLab history puts."""

    servers: list[MPREGServer]
    state: LiveStrongState = field(init=False)

    def __post_init__(self) -> None:
        self.state = LiveStrongState(servers=list(self.servers))

    def snapshot_state(self) -> LiveStrongState:
        return self.state

    async def put(
        self,
        history: History,
        *,
        process: str,
        origin_index: int,
        logical_key: str,
        value: Any,
    ) -> Any:
        s = self.servers[origin_index % len(self.servers)]
        k = self.state.key(logical_key)
        history.invoke(process, OpKind.PUT, key=logical_key, value=value)
        res = await s._cache_manager.put(
            k,
            value,
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        if res.success:
            history.ok(
                process,
                OpKind.PUT,
                key=logical_key,
                value=value,
                op_id=res.operation_id,
                meta={"quorum": res.quorum_info or {}},
            )
        else:
            history.fail(
                process,
                OpKind.PUT,
                key=logical_key,
                value=value,
                op_id=res.operation_id,
                error_code=res.error_code,
                error_message=res.error_message,
            )
        return res
