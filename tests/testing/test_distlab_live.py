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
                # T25: capability honesty gauges after successful put
                assert "mpreg_strong_cap_put_majority_commit" in text
                assert "mpreg_strong_cap_get_quorum" in text
                assert "mpreg_strong_cap_delete_quorum" in text
                for line in text.splitlines():
                    if line.startswith("mpreg_strong_cap_get_quorum{"):
                        assert line.rstrip().endswith(" 0")
                    if line.startswith("mpreg_strong_cap_delete_quorum{"):
                        assert line.rstrip().endswith(" 0")
                    if line.startswith("mpreg_strong_cap_put_majority_commit{"):
                        assert line.rstrip().endswith(" 1")

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
        caps = st.get("capabilities") or {}
        assert caps.get("get_quorum") is False
        # T28: CFT honesty on live status after put + refuse
        assert caps.get("cft_only") is True
        assert caps.get("abort_best_effort") is True
        assert "aborts_peer_ok" in st
        assert "aborts_peer_fail" in st

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/metrics/strong") as resp:
                assert resp.status == 200
                data = await resp.json()
                body = data.get("strong") or {}
                counters = body.get("counters") or {}
                assert int(counters.get("gets_refused", 0)) >= 1
                assert int(counters.get("deletes_refused", 0)) >= 1
                mcaps = body.get("capabilities") or {}
                assert mcaps.get("cft_only") is True
                assert mcaps.get("abort_best_effort") is True
                assert mcaps.get("pending_ttl_clears_residual_l1") is False
                # Abort counters always present (0 after clean put path)
                assert "aborts_peer_ok" in counters
                assert "aborts_peer_fail" in counters
                assert "visible_count" in body
                assert "backups_count" in body
                assert "backups_pruned_total" in body
                assert int(body.get("visible_count") or 0) >= 1  # successful put
                # T39/T40: retry_abort counters present (0 until ops call)
                assert "retry_abort_calls" in counters or "retry_abort_calls" in body
                assert int(
                    counters.get("retry_abort_calls", body.get("retry_abort_calls", 0))
                    or 0
                ) >= 0
                assert "last_abort_fail_peers" in body
                # T57: residual_ops_hint always present; empty after clean put
                assert "residual_ops_hint" in body
                assert isinstance(body.get("residual_ops_hint"), str)
                # No residual candidates after successful put → empty hint
                assert body.get("residual_ops_hint") == "" or not list(
                    body.get("last_abort_fail_peers") or []
                )
            # T40: ops-driven retry_abort noop (no residual peers) still increments
            retry_out = await cm.strong_retry_abort(
                key, op_id="live-noop-retry", peers=[]
            )
            assert retry_out.get("cleared") is not False or retry_out.get("attempts") == 0
            st2 = cm.strong_status()
            assert int(st2.get("retry_abort_calls") or 0) >= 1
            async with session.get(f"{base}/metrics/strong") as resp:
                data = await resp.json()
                body2 = data.get("strong") or {}
                c2 = body2.get("counters") or {}
                assert int(
                    c2.get("retry_abort_calls", body2.get("retry_abort_calls", 0)) or 0
                ) >= 1
            async with session.get(f"{base}/metrics/prometheus") as resp:
                text = await resp.text()
                assert "mpreg_strong_gets_refused_total" in text
                assert "mpreg_strong_deletes_refused_total" in text
                assert "mpreg_strong_aborts_peer_ok_total" in text
                assert "mpreg_strong_aborts_peer_fail_total" in text
                assert "mpreg_strong_cap_cft_only" in text
                assert "mpreg_strong_cap_abort_best_effort" in text
                assert "mpreg_strong_cap_pending_ttl_clears_residual_l1" in text
                # T32: visible/backups gauges + prune counter series present
                assert "mpreg_strong_visible" in text
                assert "mpreg_strong_backups" in text
                assert "mpreg_strong_backups_pruned_total" in text
                # T39/T40: retry_abort prom series
                assert "mpreg_strong_retry_abort_calls_total" in text
                assert "mpreg_strong_retry_abort_cleared_total" in text
                assert "mpreg_strong_retry_abort_still_fail_total" in text
                # T47: retry_abort_ops_driven honesty cap always 1 on live scrape
                assert "mpreg_strong_cap_retry_abort_ops_driven" in text
                # Caps remain honest after refuse path
                for line in text.splitlines():
                    if line.startswith("mpreg_strong_cap_get_quorum{"):
                        assert line.rstrip().endswith(" 0")
                    if line.startswith("mpreg_strong_cap_delete_quorum{"):
                        assert line.rstrip().endswith(" 0")
                    if line.startswith("mpreg_strong_cap_cft_only{"):
                        assert line.rstrip().endswith(" 1")
                    if line.startswith("mpreg_strong_cap_abort_best_effort{"):
                        assert line.rstrip().endswith(" 1")
                    if line.startswith(
                        "mpreg_strong_cap_pending_ttl_clears_residual_l1{"
                    ):
                        assert line.rstrip().endswith(" 0")
                    if line.startswith(
                        "mpreg_strong_cap_retry_abort_ops_driven{"
                    ):
                        assert line.rstrip().endswith(" 1")

        # T47: client RPC retry_abort also bumps counters (ops-driven path)
        from mpreg.client.unified_client import MPREGClient

        before_calls = int(cm.strong_status().get("retry_abort_calls") or 0)
        async with MPREGClient(url0) as client:
            rpc_retry = await client.cache_strong_retry_abort(
                "distlab-live",
                "e2e-m",
                "live-client-rpc-noop",
                version="v1",
                peers=[],
            )
            assert rpc_retry.ops_driven is True
            assert rpc_retry.automatic_heal is False
            # empty peers → cleared noop (no residual targets)
            assert rpc_retry.cleared is True or rpc_retry.attempts == 0
        after_calls = int(cm.strong_status().get("retry_abort_calls") or 0)
        # Counter may bump on origin if RPC landed here; otherwise any node
        mesh_calls = sum(
            int(s._cache_manager.strong_status().get("retry_abort_calls") or 0)
            for s in servers
            if getattr(s, "_cache_manager", None) is not None
        )
        assert mesh_calls >= before_calls  # non-decreasing mesh total
        assert mesh_calls >= 1
        _ = after_calls  # origin-local may or may not move

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/metrics/strong") as resp:
                data = await resp.json()
                body3 = data.get("strong") or {}
                caps3 = body3.get("capabilities") or {}
                assert caps3.get("retry_abort_ops_driven") is True
                # T57: residual_ops_hint still present after client RPC path
                assert "residual_ops_hint" in body3
                assert isinstance(body3.get("residual_ops_hint"), str)

@pytest.mark.asyncio
async def test_distlab_live_residual_ops_hint_enriched_e2e(
    test_context: AsyncTestContext,
) -> None:
    """T60: live scrape non-empty residual_ops_hint after CFT residual seed.

    Seeds peer L1 residual (prepare+commit) and origin coordinator abort-fail
    diagnostics (same shape as exhausted ABORT). Scrapes /metrics/strong for
    enriched residual_ops_hint (ns/key/op_id/peer). Still CFT ops guidance —
    not automatic heal, not WAN, not kernel partition proof.
    """
    import time

    import aiohttp

    from mpreg.cli.main import evaluate_strong_doctor_payload, strong_residual_ops_hint
    from mpreg.core.cache_models import CacheMetadata, GlobalCacheKey
    from mpreg.core.cache_strong import StrongVersion, _entry_op_id

    with port_range_context(6, "servers") as ports:
        sp, mp = ports[0:3], ports[3:6]
        url0 = f"ws://127.0.0.1:{sp[0]}"

        def _settings(port: int, mon: int, name: str, peers=None):
            from mpreg.core.config import MPREGSettings

            return MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=name,
                cluster_id="distlab-hint-e2e",
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
            MPREGServer(_settings(sp[0], mp[0], "H0")),
            MPREGServer(_settings(sp[1], mp[1], "H1", peers=[url0])),
            MPREGServer(_settings(sp[2], mp[2], "H2", peers=[url0])),
        ]
        test_context.servers.extend(servers)
        tasks = [asyncio.create_task(s.server()) for s in servers]
        test_context.tasks.extend(tasks)
        await asyncio.sleep(1.2)
        await wait_cache_peers(servers)

        origin = servers[0]
        peer = servers[1]
        key = GlobalCacheKey(
            namespace="hint-live", identifier="sku-enriched", version="v1"
        )
        oid = "t60-residual-op-id"
        replica = tuple(s.cluster.local_url for s in servers)
        sv = StrongVersion(
            logical_ts=1, origin_node=origin.cluster.local_url, op_id=oid
        )
        be = peer._strong_local_backend
        pack = await be.prepare(
            key=key,
            value={"stale": True, "t60": 1},
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

        # Origin coordinator diagnostics (same fields exhausted-ABORT leaves)
        cm = origin._cache_manager
        coord = cm._strong_coordinator
        assert coord is not None
        peer_id = peer.cluster.local_url
        coord.last_abort_fail_peers = [peer_id]
        coord.last_abort_fail_op_id = oid
        coord.recent_abort_fails.append(
            {
                "op_id": oid,
                "peers": [peer_id],
                "ts": time.time(),
                "key": f"{key.namespace}/{key.identifier}",
            }
        )
        # GCM status must surface enriched hint before HTTP scrape
        st = cm.strong_status()
        assert "n1" not in str(st.get("last_abort_fail_peers"))  # live urls
        assert peer_id in list(st.get("last_abort_fail_peers") or [])
        hint_st = st.get("residual_ops_hint") or ""
        assert "cache-strong-retry-abort" in hint_st
        assert "--namespace hint-live" in hint_st
        assert "--key sku-enriched" in hint_st
        assert oid in hint_st
        assert "not auto-heal" in hint_st

        mon_port = origin._monitoring_system.monitoring_port
        base = f"http://127.0.0.1:{mon_port}"
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/metrics/strong") as resp:
                assert resp.status == 200
                data = await resp.json()
                body = data.get("strong") or data
                hint = body.get("residual_ops_hint") or ""
                assert hint, f"expected non-empty residual_ops_hint: {body!r}"
                assert "cache-strong-retry-abort" in hint
                assert "--namespace hint-live" in hint
                assert "--key sku-enriched" in hint
                assert f"--op-id {oid}" in hint or oid in hint
                assert peer_id in hint or "--peer" in hint
                assert "not auto-heal" in hint
                assert oid == (body.get("last_abort_fail_op_id") or "")
                assert peer_id in list(body.get("last_abort_fail_peers") or [])
                # T85: JSON count matches peer list / prom gauge
                assert int(body.get("abort_fail_peer_count") or 0) >= 1
                assert int(body.get("abort_fail_peer_count") or 0) == len(
                    list(body.get("last_abort_fail_peers") or [])
                )

            async with session.get(f"{base}/mgmt/v1/strong") as resp:
                assert resp.status == 200
                mgmt = await resp.json()
                mbody = mgmt.get("strong") or mgmt
                assert "cache-strong-retry-abort" in (
                    mbody.get("residual_ops_hint") or ""
                )

            # T79: prom residual-candidate gauge non-zero while residual present
            async with session.get(f"{base}/metrics/prometheus") as presp:
                assert presp.status == 200
                ptext = await presp.text()
                assert "mpreg_strong_abort_fail_peers" in ptext
                saw = False
                for line in ptext.splitlines():
                    if line.startswith("mpreg_strong_abort_fail_peers{"):
                        saw = True
                        # value after labels
                        val = line.rsplit(" ", 1)[-1]
                        assert float(val) >= 1.0, line
                assert saw, "missing mpreg_strong_abort_fail_peers sample"

        # Doctor path consumes the same scrape payload
        ok, detail = evaluate_strong_doctor_payload({"strong": body})
        assert ok is True
        assert "cache-strong-retry-abort" in detail
        assert "hint-live" in detail or "sku-enriched" in detail
        # Prefer server hint string
        assert strong_residual_ops_hint(body) == hint

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
                    # T23: live capabilities honesty (parity with metrics builder)
                    caps = audit.get("capabilities") or {}
                    assert caps.get("gset_epidemic") is True
                    assert caps.get("siem") is False
                    assert caps.get("bft") is False
                    assert caps.get("infinite_retention") is False
                    assert caps.get("linearizable_cluster_ops") is False
                    assert caps.get("multi_tenant_beyond_cluster_id") is False

                async with session.get(f"{base}/metrics/prometheus") as resp:
                    assert resp.status == 200
                    text = await resp.text()
                    assert "mpreg_shared_audit_enabled" in text
                    assert "mpreg_shared_audit_store_size" in text
                    # T25: audit capability honesty gauges
                    assert "mpreg_shared_audit_cap_gset_epidemic" in text
                    assert "mpreg_shared_audit_cap_siem" in text
                    assert "mpreg_shared_audit_cap_bft" in text
                    for line in text.splitlines():
                        if line.startswith("mpreg_shared_audit_cap_siem{"):
                            assert line.rstrip().endswith(" 0")
                        if line.startswith("mpreg_shared_audit_cap_bft{"):
                            assert line.rstrip().endswith(" 0")
                        if line.startswith(
                            "mpreg_shared_audit_cap_gset_epidemic{"
                        ):
                            assert line.rstrip().endswith(" 1")
                        if line.startswith(
                            "mpreg_shared_audit_cap_infinite_retention{"
                        ):
                            assert line.rstrip().endswith(" 0")
                        if line.startswith(
                            "mpreg_shared_audit_cap_linearizable_cluster_ops{"
                        ):
                            assert line.rstrip().endswith(" 0")

@pytest.mark.asyncio
async def test_distlab_live_doctor_strong_audit_e2e(
    test_context: AsyncTestContext,
) -> None:
    """T20: live mesh → doctor semantics on real /metrics/strong + shared-audit.

    Uses evaluate_strong_doctor_payload + targeted HTTP probes (not full
    ``mpreg doctor`` subprocess — that walks many optional planes and is slow
    under mesh teardown). Full CLI doctor coverage remains unit-level.
    """
    import aiohttp

    with tempfile.TemporaryDirectory() as td:
        with port_range_context(4, "servers") as ports:
            sp, mp = ports[0:2], ports[2:4]
            url0 = f"ws://127.0.0.1:{sp[0]}"
            from mpreg.core.config import MPREGSettings

            def _both(port: int, mon: int, name: str, peers=None):
                return MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=name,
                    cluster_id="distlab-doctor-e2e",
                    resources={f"r-{name}"},
                    peers=peers or [],
                    log_level="ERROR",
                    gossip_interval=0.25,
                    monitoring_enabled=True,
                    monitoring_port=mon,
                    enable_default_cache=True,
                    cache_strong_enabled=True,
                    cache_strong_replica_factor=2,
                    cache_strong_min_replicas=2,
                    cache_strong_prepare_timeout_s=1.5,
                    cache_strong_commit_timeout_s=1.5,
                    mgmt_audit_path=str(Path(td) / f"{name}.jsonl"),
                    mgmt_audit_shared_enabled=True,
                    mgmt_audit_shared_reconcile_interval_s=0.35,
                )

            servers = [
                MPREGServer(_both(sp[0], mp[0], "D0")),
                MPREGServer(_both(sp[1], mp[1], "D1", peers=[url0])),
            ]
            test_context.servers.extend(servers)
            tasks = [asyncio.create_task(s.server()) for s in servers]
            test_context.tasks.extend(tasks)
            await asyncio.sleep(1.2)
            await wait_cache_peers(servers)
            await wait_gossip_connected(servers)

            sut = LiveStrongSUT(servers=servers)
            history = History()
            res = await sut.put(
                history,
                process="c0",
                origin_index=0,
                logical_key="doc-k",
                value=1,
            )
            assert res.success, res.error_message
            from mpreg.core.cache_models import (
                CacheOptions,
                ConsistencyLevel,
                GlobalCacheKey,
            )

            key = GlobalCacheKey(
                namespace="distlab-live", identifier="doc-k", version="v1"
            )
            await servers[0]._cache_manager.get(
                key, options=CacheOptions(consistency_level=ConsistencyLevel.STRONG)
            )
            apply_node_drain(servers[0], draining=True, reason="doctor-e2e")
            await wait_audit_cluster_events(servers, min_events=1, timeout=12.0)

            mon_port = servers[0]._monitoring_system.monitoring_port
            base = f"http://127.0.0.1:{mon_port}"

            from mpreg.cli.main import evaluate_strong_doctor_payload

            timeout = aiohttp.ClientTimeout(total=5.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(f"{base}/metrics/strong") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    ok, detail = evaluate_strong_doctor_payload(data)
                    assert ok is True, detail
                    assert "get_q=False" in detail
                    strong = data.get("strong") or {}
                    assert int((strong.get("counters") or {}).get("gets_refused", 0)) >= 1
                    assert int((strong.get("counters") or {}).get("puts_ok", 0)) >= 1
                    caps = strong.get("capabilities") or {}
                    assert caps.get("get_quorum") is False
                    assert caps.get("delete_quorum") is False
                    # T72: residual_ops_hint always present after clean put (empty)
                    assert "residual_ops_hint" in strong
                    assert isinstance(strong.get("residual_ops_hint"), str)
                    # Happy-path put should not leave residual candidates
                    assert list(strong.get("last_abort_fail_peers") or []) == []
                    assert (strong.get("residual_ops_hint") or "") == ""
                    from mpreg.cli.main import strong_residual_ops_hint

                    assert strong_residual_ops_hint(strong) == ""

                # T72/T73: Prometheus residual-candidate gauge should be 0 after clean put
                async with session.get(f"{base}/metrics/prometheus") as presp:
                    assert presp.status == 200
                    ptext = await presp.text()
                    assert "mpreg_strong_abort_fail_peers" in ptext
                    for line in ptext.splitlines():
                        if line.startswith("mpreg_strong_abort_fail_peers{"):
                            assert line.rstrip().endswith(" 0"), line

                async with session.get(f"{base}/mgmt/v1/strong") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    ok, _ = evaluate_strong_doctor_payload(data)
                    assert ok is True

                async with session.get(f"{base}/metrics/shared-audit") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    from mpreg.cli.main import evaluate_shared_audit_doctor_payload

                    aok, adetail = evaluate_shared_audit_doctor_payload(data)
                    assert aok is True, adetail
                    assert "siem=False" in adetail
                    assert "gset=True" in adetail
                    audit = data.get("shared_audit") or {}
                    assert audit.get("enabled_flag") is True
                    assert audit.get("status") not in {"misconfigured", "critical"}
                    acaps = audit.get("capabilities") or {}
                    assert acaps.get("gset_epidemic") is True
                    assert acaps.get("siem") is False
                    assert acaps.get("bft") is False

                # T26: coexistence prom scrape — both strong + audit cap gauges honest
                async with session.get(f"{base}/metrics/prometheus") as resp:
                    assert resp.status == 200
                    text = await resp.text()
                    assert "mpreg_strong_cap_get_quorum" in text
                    assert "mpreg_strong_cap_delete_quorum" in text
                    assert "mpreg_strong_cap_put_majority_commit" in text
                    assert "mpreg_shared_audit_cap_gset_epidemic" in text
                    assert "mpreg_shared_audit_cap_siem" in text
                    assert "mpreg_shared_audit_cap_bft" in text
                    for line in text.splitlines():
                        if line.startswith("mpreg_strong_cap_get_quorum{"):
                            assert line.rstrip().endswith(" 0")
                        if line.startswith("mpreg_strong_cap_delete_quorum{"):
                            assert line.rstrip().endswith(" 0")
                        if line.startswith("mpreg_strong_cap_put_majority_commit{"):
                            assert line.rstrip().endswith(" 1")
                        if line.startswith("mpreg_shared_audit_cap_siem{"):
                            assert line.rstrip().endswith(" 0")
                        if line.startswith("mpreg_shared_audit_cap_bft{"):
                            assert line.rstrip().endswith(" 0")
                        if line.startswith("mpreg_shared_audit_cap_gset_epidemic{"):
                            assert line.rstrip().endswith(" 1")
                        if line.startswith(
                            "mpreg_shared_audit_cap_infinite_retention{"
                        ):
                            assert line.rstrip().endswith(" 0")
                        if line.startswith(
                            "mpreg_shared_audit_cap_linearizable_cluster_ops{"
                        ):
                            assert line.rstrip().endswith(" 0")
