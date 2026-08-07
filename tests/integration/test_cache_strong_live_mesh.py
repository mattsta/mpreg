"""Live multi-server STRONG put over ServerCacheTransport (INV-CACHE-STRONG-01).

Closes the residual gap where only in-process transport was proven. Requires
peer→GCM L1 bridge on StrongLocalBackend.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from mpreg.core.cache_models import (
    CacheOptions,
    ConsistencyLevel,
    GlobalCacheKey,
)
from mpreg.core.config import MPREGSettings
from mpreg.core.errors import MpregErrorCode
from mpreg.core.port_allocator import port_range_context
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext


def _strong_settings(
    port: int,
    name: str,
    *,
    peers: list[str] | None = None,
    connect: str | None = None,
) -> MPREGSettings:
    return MPREGSettings(
        host="127.0.0.1",
        port=port,
        name=name,
        cluster_id="strong-live",
        resources={f"r-{name}"},
        peers=peers or [],
        connect=connect,
        log_level="ERROR",
        gossip_interval=0.25,
        monitoring_enabled=False,
        enable_default_cache=True,
        cache_strong_enabled=True,
        cache_strong_replica_factor=3,
        cache_strong_min_replicas=3,
        cache_strong_prepare_timeout_s=1.5,
        cache_strong_commit_timeout_s=1.5,
    )


async def _wait_peers(servers: list[MPREGServer], *, timeout: float = 8.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = True
        for s in servers:
            tr = getattr(s, "_cache_fabric_transport", None)
            if tr is None:
                ok = False
                break
            peers = list(tr.peer_ids())
            # each node should see the other two eventually
            if len(peers) < len(servers) - 1:
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


@pytest.mark.asyncio
async def test_live_three_node_strong_put_visible_on_committers(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        url0 = f"ws://127.0.0.1:{ports[0]}"
        servers = [
            MPREGServer(_strong_settings(ports[0], "S0")),
            MPREGServer(_strong_settings(ports[1], "S1", peers=[url0])),
            MPREGServer(_strong_settings(ports[2], "S2", peers=[url0])),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)

        await asyncio.sleep(1.0)
        for s in servers:
            assert getattr(s, "_cache_manager", None) is not None
            assert getattr(s, "_strong_local_backend", None) is not None
            cm = s._cache_manager
            assert cm._strong_coordinator is not None

        await _wait_peers(servers)

        key = GlobalCacheKey(namespace="strong-live", identifier="k1", version="v1")
        origin = servers[0]
        res = await origin._cache_manager.put(
            key,
            {"payload": "live-strong", "n": 1},
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert res.success is True, (
            f"STRONG put failed: code={res.error_code} msg={res.error_message} "
            f"quorum={res.quorum_info}"
        )
        assert res.quorum_info is not None
        assert res.quorum_info.get("quorum") == 2
        committers = list(res.quorum_info.get("commit_acks") or [])
        assert len(committers) >= 2
        assert origin.cluster.local_url in committers

        # Every committer must serve the value via GCM get (peer L1 bridge)
        url_to_server = {s.cluster.local_url: s for s in servers}
        for curl in committers:
            s = url_to_server[curl]
            got = await s._cache_manager.get(key)
            assert got.success and got.entry is not None, f"miss on {curl}"
            assert got.entry.value == {"payload": "live-strong", "n": 1}

        # Backend residual map agrees
        for s in servers:
            be = s._strong_local_backend
            if s.cluster.local_url in committers:
                ent = be.get_visible(key)
                assert ent is not None and ent.value["payload"] == "live-strong"


@pytest.mark.asyncio
async def test_live_strong_peer_shutdown_residual_free(
    test_context: AsyncTestContext,
) -> None:
    """Stop two peers so origin cannot form quorum; no residual on survivor."""
    with port_range_context(3, "servers") as ports:
        url0 = f"ws://127.0.0.1:{ports[0]}"
        servers = [
            MPREGServer(_strong_settings(ports[0], "K0")),
            MPREGServer(_strong_settings(ports[1], "K1", peers=[url0])),
            MPREGServer(_strong_settings(ports[2], "K2", peers=[url0])),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)

        await asyncio.sleep(1.0)
        await _wait_peers(servers)

        # Kill peers before put (async shutdown tears down peer sockets)
        for s in servers[1:]:
            await s.shutdown_async()
        await asyncio.sleep(0.5)

        key = GlobalCacheKey(namespace="strong-live", identifier="kill", version="v1")
        origin = servers[0]
        res = await origin._cache_manager.put(
            key,
            "should-fail",
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert res.success is False
        assert res.error_code in (
            int(MpregErrorCode.INSUFFICIENT_QUORUM),
            int(MpregErrorCode.QUORUM_TIMEOUT),
            int(MpregErrorCode.UNSUPPORTED_CONSISTENCY),
        )

        # Origin residual-free
        be = origin._strong_local_backend
        assert be.get_visible(key) is None
        assert be.pending_count() == 0
        got = await origin._cache_manager.get(key)
        assert got.success is False


@pytest.mark.asyncio
async def test_live_strong_mid_put_peer_kill_no_dirty_pending(
    test_context: AsyncTestContext,
) -> None:
    """Kill peers while a STRONG put is in flight; survivors stay residual-free.

    Not a kernel partition or kill -9 cold restart — cooperative shutdown_async
    during the barrier. Success is allowed if quorum formed before kill; failure
    must leave no visible residual / pending on the origin.
    """
    with port_range_context(3, "servers") as ports:
        url0 = f"ws://127.0.0.1:{ports[0]}"
        servers = [
            MPREGServer(_strong_settings(ports[0], "M0")),
            MPREGServer(_strong_settings(ports[1], "M1", peers=[url0])),
            MPREGServer(_strong_settings(ports[2], "M2", peers=[url0])),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)

        await asyncio.sleep(1.0)
        await _wait_peers(servers)

        key = GlobalCacheKey(
            namespace="strong-live", identifier="mid-kill", version="v1"
        )
        origin = servers[0]

        async def _put() -> object:
            return await origin._cache_manager.put(
                key,
                {"mid": True},
                options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
            )

        put_task = asyncio.create_task(_put())
        # Let prepare fan-out start, then tear down peers.
        await asyncio.sleep(0.05)
        for s in servers[1:]:
            await s.shutdown_async()

        res = await put_task
        be = origin._strong_local_backend
        assert be.pending_count() == 0
        if not res.success:
            assert be.get_visible(key) is None
            got = await origin._cache_manager.get(key)
            assert got.success is False
        else:
            # Quorum won the race — value must be consistent on origin.
            got = await origin._cache_manager.get(key)
            assert got.success and got.entry is not None
            assert got.entry.value == {"mid": True}


@pytest.mark.asyncio
async def test_live_concurrent_multi_origin_different_keys(
    test_context: AsyncTestContext,
) -> None:
    """Three origins put different keys concurrently over live wire."""
    with port_range_context(3, "servers") as ports:
        url0 = f"ws://127.0.0.1:{ports[0]}"
        servers = [
            MPREGServer(_strong_settings(ports[0], "C0")),
            MPREGServer(_strong_settings(ports[1], "C1", peers=[url0])),
            MPREGServer(_strong_settings(ports[2], "C2", peers=[url0])),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)

        await asyncio.sleep(1.0)
        await _wait_peers(servers)

        async def _put(idx: int):
            s = servers[idx]
            k = GlobalCacheKey(
                namespace="strong-live",
                identifier=f"ck{idx}",
                version="v1",
            )
            return k, await s._cache_manager.put(
                k,
                {"origin": idx},
                options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
            )

        results = await asyncio.gather(*[_put(i) for i in range(3)])
        successes = 0
        for k, res in results:
            if res.success:
                successes += 1
                # Origin (and committers) should see via GCM
                for s in servers:
                    if s.cluster.local_url in (res.quorum_info or {}).get(
                        "commit_acks", []
                    ):
                        got = await s._cache_manager.get(k)
                        assert got.success and got.entry is not None
            else:
                # Residual-free on all backends
                for s in servers:
                    be = s._strong_local_backend
                    assert be.pending_count() == 0
                    if res.operation_id and be.get_visible(k) is not None:
                        from mpreg.core.cache_strong import _entry_op_id

                        assert _entry_op_id(be.get_visible(k)) != res.operation_id
        assert successes >= 1


@pytest.mark.asyncio
async def test_live_strong_disabled_still_1012(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(1, "servers") as ports:
        s = MPREGServer(
            MPREGSettings(
                host="127.0.0.1",
                port=ports[0],
                name="off",
                cluster_id="strong-off",
                log_level="ERROR",
                enable_default_cache=True,
                cache_strong_enabled=False,
                monitoring_enabled=False,
            )
        )
        test_context.servers.append(s)
        test_context.tasks.append(asyncio.create_task(s.server()))
        await asyncio.sleep(0.6)
        key = GlobalCacheKey(namespace="x", identifier="y", version="v1")
        res = await s._cache_manager.put(
            key,
            1,
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert res.success is False
        assert res.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY)


@pytest.mark.asyncio
async def test_live_client_rpc_strong_retry_abort_clears_residual(
    test_context: AsyncTestContext,
) -> None:
    """T44: MPREGClient.cache_strong_retry_abort over live mesh clears peer L1.

    Seeds a CFT residual on one peer backend (prepare+commit, no ABORT), then
    re-delivers ABORT via platform RPC from a client attached to the origin.
    Still ops-driven CFT — not automatic heal, not BFT, not WAN.
    """
    from mpreg.client.unified_client import MPREGClient
    from mpreg.core.cache_models import CacheMetadata
    from mpreg.core.cache_strong import StrongVersion, _entry_op_id

    with port_range_context(3, "servers") as ports:
        url0 = f"ws://127.0.0.1:{ports[0]}"
        servers = [
            MPREGServer(_strong_settings(ports[0], "R0")),
            MPREGServer(_strong_settings(ports[1], "R1", peers=[url0])),
            MPREGServer(_strong_settings(ports[2], "R2", peers=[url0])),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)

        await asyncio.sleep(1.0)
        await _wait_peers(servers)

        origin = servers[0]
        peer = servers[1]
        key = GlobalCacheKey(
            namespace="strong-live", identifier="t44-rpc", version="v1"
        )
        oid = "t44-residual-op-id"
        replica = tuple(s.cluster.local_url for s in servers)
        sv = StrongVersion(
            logical_ts=1, origin_node=origin.cluster.local_url, op_id=oid
        )
        be = peer._strong_local_backend
        pack = await be.prepare(
            key=key,
            value={"stale": True, "t44": 1},
            metadata=CacheMetadata(),
            strong_version=sv,
            replica_set=replica,
            quorum=2,
            ttl_s=60.0,
        )
        assert pack.ok, getattr(pack, "reason", pack)
        cack = await be.commit(op_id=oid, key=key)
        assert cack.ok and cack.applied
        residual = be.get_visible(key)
        assert residual is not None and _entry_op_id(residual) == oid

        # Client RPC path: ops-driven re-ABORT. Unpinned call may land on any
        # node advertising resource "cache" — including the residual peer.
        # retry_abort always local-aborts and peer-aborts remotes so either
        # landing clears the residual (T44 product fix).
        async with MPREGClient(url0) as client:
            put = await client.cache_put(
                "strong-live",
                "t44-eventual-probe",
                {"probe": True},
                version="v1",
            )
            assert put.success is True, (
                f"client cache_put probe failed: {put.error_code} {put.error_message}"
            )

            retry = await client.cache_strong_retry_abort(
                "strong-live",
                "t44-rpc",
                oid,
                version="v1",
                peers=[peer.cluster.local_url],
            )
            assert retry.ops_driven is True
            assert retry.automatic_heal is False
            assert retry.success is True, (
                f"retry_abort failed: {retry.error_message} "
                f"fail={retry.fail_peers} raw={retry.raw}"
            )
            assert retry.cleared is True

        cleared = be.get_visible(key)
        assert cleared is None or _entry_op_id(cleared) != oid

        # Counters increment on whichever node handled the RPC
        total_calls = sum(
            int(s._cache_manager.strong_status().get("retry_abort_calls") or 0)
            for s in servers
        )
        total_cleared = sum(
            int(s._cache_manager.strong_status().get("retry_abort_cleared") or 0)
            for s in servers
        )
        assert total_calls >= 1
        assert total_cleared >= 1
