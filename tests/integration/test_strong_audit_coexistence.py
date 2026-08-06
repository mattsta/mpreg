"""Live mesh with both cache_strong_enabled and mgmt_audit_shared_enabled."""

from __future__ import annotations

import asyncio
import tempfile
import time
from pathlib import Path

import pytest

from mpreg.core.cache_models import CacheOptions, ConsistencyLevel, GlobalCacheKey
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.server import MPREGServer
from mpreg.server_pkg.mgmt_mutations import apply_node_drain
from tests.conftest import AsyncTestContext

def _both_settings(
    port: int,
    mon: int,
    name: str,
    audit_dir: str,
    *,
    peers: list[str] | None = None,
) -> MPREGSettings:
    return MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=name,
        cluster_id="coexist",
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

async def _wait_peers(servers: list[MPREGServer], *, timeout: float = 10.0) -> None:
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
    raise AssertionError("cache peers not ready")

async def _wait_connected(servers: list[MPREGServer], *, timeout: float = 12.0) -> None:
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

async def _wait_cluster_audit(
    servers: list[MPREGServer], *, min_events: int, timeout: float = 18.0
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = True
        for s in servers:
            body = s._mgmt_audit_snapshot(scope="cluster", limit=100)
            events = body.get("mutations") or body.get("entries") or []
            if len(events) < min_events:
                ok = False
                break
        if ok:
            return
        await asyncio.sleep(0.2)
    raise AssertionError("audit did not converge")

@pytest.mark.asyncio
async def test_live_strong_and_shared_audit_coexist(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(6, "servers") as ports:
        ws, mon = ports[0:3], ports[3:6]
        audit_dir = tempfile.mkdtemp(prefix="mpreg-coexist-")
        url0 = f"ws://127.0.0.1:{ws[0]}"
        servers = [
            MPREGServer(_both_settings(ws[0], mon[0], "X0", audit_dir)),
            MPREGServer(
                _both_settings(ws[1], mon[1], "X1", audit_dir, peers=[url0])
            ),
            MPREGServer(
                _both_settings(ws[2], mon[2], "X2", audit_dir, peers=[url0])
            ),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)

        await asyncio.sleep(1.0)
        for s in servers:
            assert s._cache_manager is not None
            assert s._shared_audit_store is not None
            assert s._strong_local_backend is not None
            assert getattr(s, "_strong_pending_purge_task", None) is not None

        await _wait_peers(servers)
        await _wait_connected(servers)

        # STRONG put
        key = GlobalCacheKey(namespace="co", identifier="k", version="v1")
        res = await servers[0]._cache_manager.put(
            key,
            {"coexist": True},
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert res.success is True, res.error_message
        committers = list((res.quorum_info or {}).get("commit_acks") or [])
        url_map = {s.cluster.local_url: s for s in servers}
        for curl in committers:
            got = await url_map[curl]._cache_manager.get(key)
            assert got.success and got.entry is not None

        # Shared audit drains from each node
        for i, s in enumerate(servers):
            apply_node_drain(
                s, draining=True, actor=f"x-{i}", reason=f"coexist-{i}"
            )
            await asyncio.sleep(0.15)
            apply_node_drain(
                s, draining=False, actor=f"x-{i}", reason=f"clear-{i}"
            )

        await _wait_cluster_audit(servers, min_events=6, timeout=20.0)

        # STRONG still works after audit churn
        key2 = GlobalCacheKey(namespace="co", identifier="k2", version="v1")
        res2 = await servers[1]._cache_manager.put(
            key2,
            2,
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert res2.success is True
        for s in servers:
            assert s._strong_local_backend.pending_count() == 0
