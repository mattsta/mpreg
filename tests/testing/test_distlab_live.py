"""Live multi-process DistLab scenarios (T5) — same-host, not WAN."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest

from mpreg.core.port_allocator import port_range_context
from mpreg.server import MPREGServer
from mpreg.server_pkg.mgmt_mutations import apply_node_drain
from mpreg.testing.distlab import History, LiveStrongSUT, default_strong_checkers
from mpreg.testing.distlab.live import (
    audit_settings,
    both_settings,
    strong_settings,
    wait_audit_cluster_events,
    wait_cache_peers,
    wait_gossip_connected,
)
from tests.conftest import AsyncTestContext

@pytest.mark.asyncio
async def test_distlab_live_strong_happy_3(test_context: AsyncTestContext) -> None:
    with port_range_context(3, "servers") as ports:
        url0 = f"ws://127.0.0.1:{ports[0]}"
        servers = [
            MPREGServer(strong_settings(ports[0], "L0")),
            MPREGServer(strong_settings(ports[1], "L1", peers=[url0])),
            MPREGServer(strong_settings(ports[2], "L2", peers=[url0])),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)
        await asyncio.sleep(1.0)
        await wait_cache_peers(servers)

        sut = LiveStrongSUT(servers=servers)
        history = History()
        res = await sut.put(
            history,
            process="c0",
            origin_index=0,
            logical_key="live-k",
            value={"v": 1},
        )
        assert res.success is True, res.error_message
        check = default_strong_checkers(key="live-k").check(
            history, state=sut.snapshot_state()
        )
        assert check.ok, check.violations
        assert sut.state.pending_count() == 0

@pytest.mark.asyncio
async def test_distlab_live_strong_multi_origin_keys(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        url0 = f"ws://127.0.0.1:{ports[0]}"
        servers = [
            MPREGServer(strong_settings(ports[0], "M0")),
            MPREGServer(strong_settings(ports[1], "M1", peers=[url0])),
            MPREGServer(strong_settings(ports[2], "M2", peers=[url0])),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)
        await asyncio.sleep(1.0)
        await wait_cache_peers(servers)

        sut = LiveStrongSUT(servers=servers)
        history = History()
        for i in range(3):
            res = await sut.put(
                history,
                process=f"c{i}",
                origin_index=i,
                logical_key=f"mk{i}",
                value=i,
            )
            assert res.success, res.error_message
        check = default_strong_checkers().check(history, state=sut.snapshot_state())
        assert check.ok, check.violations

@pytest.mark.asyncio
async def test_distlab_live_strong_disabled_1012(
    test_context: AsyncTestContext,
) -> None:
    """When cache_strong_enabled is off, STRONG put fails closed (1012)."""
    from mpreg.core.config import MPREGSettings
    from mpreg.core.errors import MpregErrorCode
    from mpreg.core.cache_models import (
        CacheOptions,
        ConsistencyLevel,
        GlobalCacheKey,
    )

    with port_range_context(1, "servers") as ports:
        s = MPREGServer(
            MPREGSettings(
                host="127.0.0.1",
                port=ports[0],
                name="D0",
                cluster_id="distlab-disabled",
                resources={"r-D0"},
                log_level="ERROR",
                monitoring_enabled=False,
                enable_default_cache=True,
                cache_strong_enabled=False,
            )
        )
        test_context.servers.append(s)
        test_context.tasks.append(asyncio.create_task(s.server()))
        await asyncio.sleep(0.6)
        key = GlobalCacheKey(namespace="d", identifier="k", version="v1")
        res = await s._cache_manager.put(
            key,
            1,
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        assert res.success is False
        assert res.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY)

@pytest.mark.asyncio
async def test_distlab_live_strong_peer_loss_residual(
    test_context: AsyncTestContext,
) -> None:
    with port_range_context(3, "servers") as ports:
        url0 = f"ws://127.0.0.1:{ports[0]}"
        servers = [
            MPREGServer(strong_settings(ports[0], "P0")),
            MPREGServer(strong_settings(ports[1], "P1", peers=[url0])),
            MPREGServer(strong_settings(ports[2], "P2", peers=[url0])),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)
        await asyncio.sleep(1.0)
        await wait_cache_peers(servers)

        # Shut down two peers so origin cannot form quorum
        for s in servers[1:]:
            await s.shutdown_async()

        sut = LiveStrongSUT(servers=servers[:1])
        history = History()
        res = await sut.put(
            history,
            process="c0",
            origin_index=0,
            logical_key="loss",
            value="x",
        )
        assert res.success is False
        # Origin-only snapshot: residual-free on remaining backend
        check = default_strong_checkers(key="loss").check(
            history, state=sut.snapshot_state()
        )
        assert check.ok, check.violations

@pytest.mark.asyncio
async def test_distlab_live_audit_multi_origin(
    test_context: AsyncTestContext,
) -> None:
    with tempfile.TemporaryDirectory() as td:
        with port_range_context(6, "servers") as ports:
            # 3 server + 3 mon ports interleaved
            sp, mp = ports[0:3], ports[3:6]
            url0 = f"ws://127.0.0.1:{sp[0]}"
            servers = [
                MPREGServer(
                    audit_settings(sp[0], mp[0], "A0", td)
                ),
                MPREGServer(
                    audit_settings(sp[1], mp[1], "A1", td, peers=[url0])
                ),
                MPREGServer(
                    audit_settings(sp[2], mp[2], "A2", td, peers=[url0])
                ),
            ]
            test_context.servers.extend(servers)
            tasks = [asyncio.create_task(s.server()) for s in servers]
            test_context.tasks.extend(tasks)
            await asyncio.sleep(1.2)
            await wait_gossip_connected(servers)

            # Multi-origin drains create gossip-eligible audit events
            for s in servers:
                apply_node_drain(s, draining=True, reason="distlab-live")
            await wait_audit_cluster_events(servers, min_events=3, timeout=20.0)

@pytest.mark.asyncio
async def test_distlab_live_coexistence_strong_audit(
    test_context: AsyncTestContext,
) -> None:
    with tempfile.TemporaryDirectory() as td:
        with port_range_context(6, "servers") as ports:
            sp, mp = ports[0:3], ports[3:6]
            url0 = f"ws://127.0.0.1:{sp[0]}"
            servers = [
                MPREGServer(both_settings(sp[0], mp[0], "B0", td)),
                MPREGServer(both_settings(sp[1], mp[1], "B1", td, peers=[url0])),
                MPREGServer(both_settings(sp[2], mp[2], "B2", td, peers=[url0])),
            ]
            test_context.servers.extend(servers)
            tasks = [asyncio.create_task(s.server()) for s in servers]
            test_context.tasks.extend(tasks)
            await asyncio.sleep(1.2)
            await wait_cache_peers(servers)
            await wait_gossip_connected(servers)

            for s in servers:
                assert getattr(s, "_strong_local_backend", None) is not None
                assert getattr(s, "_cache_manager", None) is not None

            sut = LiveStrongSUT(servers=servers)
            history = History()
            res = await sut.put(
                history,
                process="c0",
                origin_index=0,
                logical_key="both-k",
                value={"both": True},
            )
            assert res.success, res.error_message

            for s in servers:
                apply_node_drain(s, draining=True, reason="coex")
            await wait_audit_cluster_events(servers, min_events=3, timeout=20.0)

            # Clear drain so STRONG quorum path is healthy again
            for s in servers:
                apply_node_drain(s, draining=False, reason="coex-clear")
            await asyncio.sleep(0.3)

            # Second STRONG put after audit churn
            res2 = await sut.put(
                history,
                process="c1",
                origin_index=1,
                logical_key="both-k2",
                value=2,
            )
            assert res2.success, res2.error_message
            assert sut.state.pending_count() == 0
