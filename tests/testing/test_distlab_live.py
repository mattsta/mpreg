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

@pytest.mark.asyncio
async def test_distlab_live_strong_happy_4(test_context: AsyncTestContext) -> None:
    """T13: 4-node live STRONG majority put (Q=3)."""
    with port_range_context(4, "servers") as ports:
        url0 = f"ws://127.0.0.1:{ports[0]}"
        servers = [
            MPREGServer(
                strong_settings(
                    ports[0], "F0", replica_factor=4, min_replicas=3
                )
            ),
            MPREGServer(
                strong_settings(
                    ports[1], "F1", peers=[url0], replica_factor=4, min_replicas=3
                )
            ),
            MPREGServer(
                strong_settings(
                    ports[2], "F2", peers=[url0], replica_factor=4, min_replicas=3
                )
            ),
            MPREGServer(
                strong_settings(
                    ports[3], "F3", peers=[url0], replica_factor=4, min_replicas=3
                )
            ),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)
        await asyncio.sleep(1.2)
        await wait_cache_peers(servers, timeout=14.0)

        sut = LiveStrongSUT(servers=servers)
        history = History()
        res = await sut.put(
            history,
            process="c0",
            origin_index=0,
            logical_key="live4",
            value={"n": 4},
        )
        assert res.success is True, res.error_message
        check = default_strong_checkers(key="live4").check(
            history, state=sut.snapshot_state()
        )
        assert check.ok, check.violations
        assert sut.state.pending_count() == 0

@pytest.mark.asyncio
async def test_distlab_live_strong_mid_put_peer_kill(
    test_context: AsyncTestContext,
) -> None:
    """T13: kill peers after mesh ready; put must fail residual-free on survivors."""
    with port_range_context(3, "servers") as ports:
        url0 = f"ws://127.0.0.1:{ports[0]}"
        servers = [
            MPREGServer(strong_settings(ports[0], "K0")),
            MPREGServer(strong_settings(ports[1], "K1", peers=[url0])),
            MPREGServer(strong_settings(ports[2], "K2", peers=[url0])),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)
        await asyncio.sleep(1.0)
        await wait_cache_peers(servers)

        # Mid-session kill of minority so origin cannot form Q=3
        await servers[2].shutdown_async()
        await servers[1].shutdown_async()
        await asyncio.sleep(0.2)

        sut = LiveStrongSUT(servers=servers[:1])
        history = History()
        res = await sut.put(
            history,
            process="c0",
            origin_index=0,
            logical_key="midkill",
            value="x",
        )
        assert res.success is False
        check = default_strong_checkers(key="midkill").check(
            history, state=sut.snapshot_state()
        )
        assert check.ok, check.violations
        assert sut.state.pending_count() == 0

@pytest.mark.asyncio
async def test_distlab_live_audit_late_joiner(
    test_context: AsyncTestContext,
) -> None:
    """T13: late joiner converges to cluster G-Set after anti-entropy."""
    with tempfile.TemporaryDirectory() as td:
        with port_range_context(8, "servers") as ports:
            sp, mp = ports[0:4], ports[4:8]
            url0 = f"ws://127.0.0.1:{sp[0]}"
            early = [
                MPREGServer(audit_settings(sp[0], mp[0], "LJ0", td)),
                MPREGServer(audit_settings(sp[1], mp[1], "LJ1", td, peers=[url0])),
            ]
            test_context.servers.extend(early)
            tasks = [asyncio.create_task(s.server()) for s in early]
            test_context.tasks.extend(tasks)
            await asyncio.sleep(1.0)
            await wait_gossip_connected(early)

            for s in early:
                apply_node_drain(s, draining=True, reason="late-pre")
            await wait_audit_cluster_events(early, min_events=2, timeout=18.0)

            late = MPREGServer(
                audit_settings(sp[2], mp[2], "LJ2", td, peers=[url0])
            )
            test_context.servers.append(late)
            test_context.tasks.append(asyncio.create_task(late.server()))
            await asyncio.sleep(1.0)
            all_servers = early + [late]
            await wait_gossip_connected(all_servers, timeout=14.0)
            # Late node should see prior cluster events via gossip/reconcile
            await wait_audit_cluster_events(all_servers, min_events=2, timeout=22.0)

@pytest.mark.asyncio
async def test_distlab_live_strong_metrics_e2e(
    test_context: AsyncTestContext,
) -> None:
    """T17: live STRONG put then scrape /metrics/strong + prometheus on origin."""
    import aiohttp

    with port_range_context(6, "servers") as ports:
        sp, mp = ports[0:3], ports[3:6]
        url0 = f"ws://127.0.0.1:{sp[0]}"

        def _settings(port: int, mon: int, name: str, peers=None):
            from mpreg.core.config import MPREGSettings

            return MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=name,
                cluster_id="distlab-metrics-e2e",
                resources={f"r-{name}"},
                peers=peers or [],
                log_level="ERROR",
                gossip_interval=0.25,
                monitoring_enabled=True,
                monitoring_port=mon,
                enable_default_cache=True,
                cache_strong_enabled=True,
                cache_strong_replica_factor=3,
                cache_strong_min_replicas=3,
                cache_strong_prepare_timeout_s=1.5,
                cache_strong_commit_timeout_s=1.5,
            )

        servers = [
            MPREGServer(_settings(sp[0], mp[0], "E0")),
            MPREGServer(_settings(sp[1], mp[1], "E1", peers=[url0])),
            MPREGServer(_settings(sp[2], mp[2], "E2", peers=[url0])),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)
        await asyncio.sleep(1.2)
        await wait_cache_peers(servers)

        sut = LiveStrongSUT(servers=servers)
        history = History()
        res = await sut.put(
            history,
            process="c0",
            origin_index=0,
            logical_key="e2e-m",
            value={"e2e": True},
        )
        assert res.success is True, res.error_message

        mon_port = servers[0]._monitoring_system.monitoring_port
        base = f"http://127.0.0.1:{mon_port}"
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/metrics/strong") as resp:
                assert resp.status == 200
                data = await resp.json()
                strong = data["strong"]
                assert strong.get("coordinator_bound") is True
                counters = strong.get("counters") or {}
                assert int(counters.get("puts_ok", 0)) >= 1
                lat = strong.get("latency_ms") or {}
                assert int(lat.get("sample_count", 0)) >= 1
                assert strong.get("health") in {"ok", "degraded_pending"}

            async with session.get(f"{base}/mgmt/v1/strong") as resp:
                assert resp.status == 200
                data = await resp.json()
                assert "strong" in data

            async with session.get(f"{base}/metrics/prometheus") as resp:
                assert resp.status == 200
                text = await resp.text()
                assert "mpreg_strong_enabled" in text
                assert "mpreg_strong_puts_ok_total" in text

        # Local RYW on origin GCM
        from mpreg.core.cache_models import GlobalCacheKey

        key = GlobalCacheKey(namespace="distlab-live", identifier="e2e-m", version="v1")
        got = await servers[0]._cache_manager.get(key)
        assert got.success and got.entry is not None
        assert got.entry.value == {"e2e": True}

        # T18: STRONG get/delete refuse on live mesh with metrics counters
        from mpreg.core.cache_models import CacheOptions, ConsistencyLevel
        from mpreg.core.errors import MpregErrorCode

        cm = servers[0]._cache_manager
        bad_get = await cm.get(
            key, options=CacheOptions(consistency_level=ConsistencyLevel.STRONG)
        )
        assert bad_get.success is False
        assert bad_get.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY)
        bad_del = await cm.delete(
            key, options=CacheOptions(consistency_level=ConsistencyLevel.STRONG)
        )
        assert bad_del.success is False
        assert bad_del.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY)
        st = cm.strong_status()
        assert st["gets_refused"] >= 1
        assert st["deletes_refused"] >= 1
        assert (st.get("capabilities") or {}).get("get_quorum") is False

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/metrics/strong") as resp:
                assert resp.status == 200
                data = await resp.json()
                counters = (data.get("strong") or {}).get("counters") or {}
                assert int(counters.get("gets_refused", 0)) >= 1
                assert int(counters.get("deletes_refused", 0)) >= 1
            async with session.get(f"{base}/metrics/prometheus") as resp:
                text = await resp.text()
                assert "mpreg_strong_gets_refused_total" in text
                assert "mpreg_strong_deletes_refused_total" in text

@pytest.mark.asyncio
async def test_distlab_live_audit_metrics_e2e(
    test_context: AsyncTestContext,
) -> None:
    """T18: live shared-audit publish → scrape /metrics/shared-audit + prom."""
    import aiohttp

    with tempfile.TemporaryDirectory() as td:
        with port_range_context(6, "servers") as ports:
            sp, mp = ports[0:3], ports[3:6]
            url0 = f"ws://127.0.0.1:{sp[0]}"
            servers = [
                MPREGServer(audit_settings(sp[0], mp[0], "AM0", td)),
                MPREGServer(audit_settings(sp[1], mp[1], "AM1", td, peers=[url0])),
                MPREGServer(audit_settings(sp[2], mp[2], "AM2", td, peers=[url0])),
            ]
            test_context.servers.extend(servers)
            tasks = [asyncio.create_task(s.server()) for s in servers]
            test_context.tasks.extend(tasks)
            await asyncio.sleep(1.2)
            await wait_gossip_connected(servers)

            for s in servers:
                apply_node_drain(s, draining=True, reason="audit-metrics-e2e")
            await wait_audit_cluster_events(servers, min_events=3, timeout=20.0)

            mon_port = servers[0]._monitoring_system.monitoring_port
            base = f"http://127.0.0.1:{mon_port}"
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{base}/metrics/shared-audit") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    audit = data["shared_audit"]
                    assert audit.get("enabled_flag") is True
                    assert audit.get("store_present") is True
                    assert int(audit.get("store_size") or 0) >= 1
                    assert audit.get("status") in {
                        "ok",
                        "ok_no_peers",
                        "degraded_drops",
                    }
                    counters = audit.get("counters") or {}
                    # At least some epidemic activity after multi-origin drain
                    assert isinstance(counters, dict)

                async with session.get(f"{base}/metrics/prometheus") as resp:
                    assert resp.status == 200
                    text = await resp.text()
                    assert "mpreg_shared_audit_enabled" in text
                    assert "mpreg_shared_audit_store_size" in text
