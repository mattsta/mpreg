"""Live shared-audit epidemic under multi-origin churn (INV-SHARED-AUDIT-01).

Extends boot-only coverage: multiple drains from different nodes, wait for
real fabric gossip convergence on scope=cluster.
"""

from __future__ import annotations

import asyncio
import tempfile
import time
from pathlib import Path

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.server import MPREGServer
from mpreg.server_pkg.mgmt_mutations import apply_node_drain
from tests.conftest import AsyncTestContext

def _audit_settings(
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
        cluster_id="audit-churn",
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

async def _wait_connected_mesh(
    servers: list[MPREGServer], *, timeout: float = 10.0
) -> None:
    """Wait until each node has ≥1 connected peer (star or full mesh)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = True
        for s in servers:
            try:
                conns = s._get_all_peer_connections()
                n = sum(
                    1
                    for c in conns.values()
                    if getattr(c, "is_connected", False)
                )
            except Exception:  # noqa: BLE001
                n = 0
            if n < 1 and len(servers) > 1:
                ok = False
                break
        if ok:
            # Hub should see both leaves when leaves peer to hub
            hub = servers[0]
            try:
                hub_n = sum(
                    1
                    for c in hub._get_all_peer_connections().values()
                    if getattr(c, "is_connected", False)
                )
            except Exception:  # noqa: BLE001
                hub_n = 0
            if hub_n >= len(servers) - 1:
                return
        await asyncio.sleep(0.15)
    raise AssertionError("peer mesh not connected in time")

async def _wait_cluster_events(
    servers: list[MPREGServer],
    *,
    min_events: int,
    timeout: float = 12.0,
) -> list[dict]:
    deadline = time.time() + timeout
    last: list[dict] = []
    while time.time() < deadline:
        snapshots = []
        ok = True
        for s in servers:
            body = s._mgmt_audit_snapshot(scope="cluster", limit=200)
            assert isinstance(body, dict)
            if body.get("error"):
                ok = False
                break
            events = body.get("mutations") or body.get("entries") or []
            if not events and "items" in body:
                events = body["items"]
            snapshots.append(list(events))
            if len(events) < min_events:
                ok = False
        if ok and snapshots:
            id_sets = []
            for evs in snapshots:
                ids = set()
                for e in evs:
                    if isinstance(e, dict):
                        eid = e.get("entry_id") or e.get("id")
                        if eid:
                            ids.add(eid)
                id_sets.append(ids)
            if id_sets and all(len(s) >= min_events for s in id_sets):
                inter = set.intersection(*id_sets) if id_sets else set()
                if len(inter) >= min_events:
                    last = snapshots[0]
                    return last
                if all(s == id_sets[0] for s in id_sets) and len(id_sets[0]) >= min_events:
                    last = snapshots[0]
                    return last
        await asyncio.sleep(0.2)
    dumps = []
    for s in servers:
        body = s._mgmt_audit_snapshot(scope="cluster", limit=50)
        dumps.append(
            (
                s.settings.name,
                body.get("mutation_count"),
                (body.get("health") or {}),
                [
                    m.get("origin_node")
                    for m in (body.get("mutations") or [])[:8]
                ],
            )
        )
    raise AssertionError(
        f"cluster audit did not converge to {min_events} events: {dumps}"
    )

@pytest.mark.asyncio
async def test_live_multi_origin_drain_converges(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(6, "servers") as ports:
        ws, mon = ports[0:3], ports[3:6]
        audit_dir = tempfile.mkdtemp(prefix="mpreg-audit-churn-")
        url0 = f"ws://127.0.0.1:{ws[0]}"
        servers = [
            MPREGServer(_audit_settings(ws[0], mon[0], "A0", audit_dir)),
            MPREGServer(
                _audit_settings(ws[1], mon[1], "A1", audit_dir, peers=[url0])
            ),
            MPREGServer(
                _audit_settings(ws[2], mon[2], "A2", audit_dir, peers=[url0])
            ),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)

        await asyncio.sleep(0.8)
        for s in servers:
            assert s._shared_audit_store is not None
            assert s._shared_audit_replicator is not None
        await _wait_connected_mesh(servers, timeout=12.0)

        # Stagger drains from each origin (churn)
        for i, s in enumerate(servers):
            apply_node_drain(
                s,
                draining=True,
                actor=f"churn-{i}",
                reason=f"drain-{i}",
            )
            await asyncio.sleep(0.2)
            # clear drain so next ops stay healthy
            apply_node_drain(
                s,
                draining=False,
                actor=f"churn-{i}",
                reason=f"clear-{i}",
            )
            await asyncio.sleep(0.15)

        # 3 drains + 3 clears = 6 mutation events
        await _wait_cluster_events(servers, min_events=6, timeout=20.0)

        # Each store size >= 6
        for s in servers:
            assert s._shared_audit_store.size() >= 6

        # scope=local on A1 should not require foreign-only; cluster has all
        local = servers[1]._mgmt_audit_snapshot(scope="local", limit=50)
        assert local.get("scope") == "local"
        cluster = servers[1]._mgmt_audit_snapshot(scope="cluster", limit=50)
        assert cluster.get("scope") == "cluster"
        assert cluster.get("shared_enabled") is True

@pytest.mark.asyncio
async def test_live_late_joiner_backfill(
    test_context: AsyncTestContext,
) -> None:
    """Two-node history, then third joins and catch-up via digest/PULL."""
    with port_range_context(6, "servers") as ports:
        ws, mon = ports[0:3], ports[3:6]
        audit_dir = tempfile.mkdtemp(prefix="mpreg-audit-late-")
        url0 = f"ws://127.0.0.1:{ws[0]}"
        s0 = MPREGServer(_audit_settings(ws[0], mon[0], "L0", audit_dir))
        s1 = MPREGServer(
            _audit_settings(ws[1], mon[1], "L1", audit_dir, peers=[url0])
        )
        test_context.servers.extend([s0, s1])
        tasks = [
            asyncio.create_task(s0.server()),
            asyncio.create_task(s1.server()),
        ]
        test_context.tasks.extend(tasks)
        await asyncio.sleep(0.8)
        await _wait_connected_mesh([s0, s1], timeout=12.0)

        for i in range(4):
            apply_node_drain(
                s0, draining=bool(i % 2), actor="late", reason=f"e{i}"
            )
            await asyncio.sleep(0.15)

        await _wait_cluster_events([s0, s1], min_events=4, timeout=18.0)

        # Late joiner
        s2 = MPREGServer(
            _audit_settings(ws[2], mon[2], "L2", audit_dir, peers=[url0])
        )
        test_context.servers.append(s2)
        test_context.tasks.append(asyncio.create_task(s2.server()))
        await asyncio.sleep(0.8)
        await _wait_connected_mesh([s0, s1, s2], timeout=12.0)

        await _wait_cluster_events([s0, s1, s2], min_events=4, timeout=20.0)
        assert s2._shared_audit_store.size() >= 4
