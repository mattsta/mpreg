"""Register built-in DistLab scenarios into DEFAULT_REGISTRY."""

from __future__ import annotations

from mpreg.testing.distlab.checker import (
    NoOpenInvokeChecker,
    default_audit_checkers,
    default_strong_checkers,
)
from mpreg.testing.distlab.generator import AuditBurst, ConcurrentPuts, SequentialPuts
from mpreg.testing.distlab.history import History
from mpreg.testing.distlab.models import OpKind
from mpreg.testing.distlab.nemesis import (
    FaultInjectorNemesisTarget,
    Nemesis,
    NemesisAction,
)
from mpreg.testing.distlab.registry import DEFAULT_REGISTRY, get_registry
from mpreg.testing.distlab.scenario import Scenario

_REGISTERED = False

def _strong_happy(n: int, key: str) -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(n, prepare_timeout_s=0.5, commit_timeout_s=0.5)

    async def body(history: History, s: object) -> None:
        res = await sut.put(
            history, process="c0", origin="n0", logical_key=key, value={"n": n}
        )
        assert res.success, res.error_message

    return Scenario(
        name=f"strong.happy_{n}",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key=key),
        meta={"n": n, "track": "T2"},
    )

def _strong_partition_majority() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    hooks = sut.nemesis_hooks()
    target = FaultInjectorNemesisTarget(
        injector=hooks["injector"],
        nodes=list(hooks["nodes"]),
        on_partition=hooks["on_partition"],
        on_heal=hooks["on_heal"],
        on_crash=hooks["on_crash"],
        on_recover=hooks["on_recover"],
        on_drop_rate=hooks["on_drop_rate"],
        on_delay=hooks["on_delay"],
    )

    async def body(history: History, s: object) -> None:
        target.apply_partition_groups([{"n0"}, {"n1", "n2"}])
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="pm", value="x"
        )
        assert res.success is False
        target.heal_network()

    return Scenario(
        name="strong.partition_majority",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="pm"),
        meta={"track": "T2"},
    )

def _strong_heal() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    hooks = sut.nemesis_hooks()
    target = FaultInjectorNemesisTarget(
        injector=hooks["injector"],
        nodes=list(hooks["nodes"]),
        on_partition=hooks["on_partition"],
        on_heal=hooks["on_heal"],
        on_crash=hooks["on_crash"],
        on_recover=hooks["on_recover"],
        on_drop_rate=hooks["on_drop_rate"],
        on_delay=hooks["on_delay"],
    )

    async def body(history: History, s: object) -> None:
        target.apply_partition_groups([{"n0"}, {"n1", "n2"}])
        bad = await sut.put(
            history, process="c0", origin="n0", logical_key="heal", value="no"
        )
        assert bad.success is False
        target.heal_network()
        good = await sut.put(
            history, process="c0", origin="n0", logical_key="heal", value="yes"
        )
        assert good.success is True

    return Scenario(
        name="strong.heal",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="heal"),
        meta={"track": "T2"},
    )

def _strong_concurrent() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    clients = ConcurrentPuts(sut=sut, n_clients=4, logical_key="ck").as_clients()
    return Scenario(
        name="strong.concurrent_same_key",
        setup=lambda: sut,
        clients=clients,
        checker=default_strong_checkers(key="ck"),
        meta={"track": "T2"},
    )

def _strong_multi_key() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    clients = ConcurrentPuts(
        sut=sut, n_clients=6, logical_key="mk", multi_key=True
    ).as_clients()
    return Scenario(
        name="strong.concurrent_multi_key",
        setup=lambda: sut,
        clients=clients,
        checker=default_strong_checkers(),
        meta={"track": "T2"},
    )

def _strong_drop_prepare() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    sut.transport.drop_prepare |= {"n1", "n2"}

    async def body(history: History, s: object) -> None:
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="dp", value=1
        )
        assert res.success is False

    return Scenario(
        name="strong.drop_prepare",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="dp"),
        meta={"track": "T2"},
    )

def _strong_drop_commit() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    sut.transport.drop_commit |= {"n1", "n2"}

    async def body(history: History, s: object) -> None:
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="dc", value=1
        )
        assert res.success is False

    return Scenario(
        name="strong.drop_commit",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="dc"),
        meta={"track": "T2"},
    )

def _strong_drop_abort() -> Scenario:
    """Drop ABORT after prepare+failed commit — purge TTL clears pending residual.

    Peers prepare successfully; commit is dropped so the put fails; ABORT is also
    dropped so pending would linger. Short pending_ttl + purge makes the
    residual-free contract hold without claiming BFT or infinite pending retention.
    """
    import asyncio

    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(
        3,
        prepare_timeout_s=0.5,
        commit_timeout_s=0.25,
        pending_ttl_s=0.15,
    )
    # Prepare reaches peers; commit fails; abort never delivered.
    sut.transport.drop_commit |= {"n1", "n2"}
    sut.transport.drop_abort |= {"n1", "n2"}

    async def body(history: History, s: object) -> None:
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="da", value=1
        )
        assert res.success is False
        # Wait past pending_ttl then purge so ResidualFreeChecker sees zero pending.
        await asyncio.sleep(0.25)
        for be in sut.backends.values():
            if hasattr(be, "purge_expired_pending"):
                be.purge_expired_pending()

    return Scenario(
        name="strong.drop_abort",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="da"),
        meta={"track": "T13", "fault": "drop_abort"},
    )

def _strong_refuse_get_delete() -> Scenario:
    """T19: STRONG get/delete always 1012; EVENTUAL RYW after majority put.

    Uses GlobalCacheManager (product surface) with lab single-node coordinator.
    Not a quorum get/delete product — design-correct refuse only.
    """
    from mpreg.core.cache_models import (
        CacheMetadata,
        CacheOptions,
        ConsistencyLevel,
        GlobalCacheKey,
    )
    from mpreg.core.cache_strong import (
        InProcessStrongTransport,
        StrongLocalBackend,
        StrongPutCoordinator,
    )
    from mpreg.core.errors import MpregErrorCode
    from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager

    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="refuse",
        )
    )
    be = StrongLocalBackend(node_id="origin")
    tr = InProcessStrongTransport()
    tr.register(be)
    gcm.attach_strong_coordinator(
        StrongPutCoordinator(
            origin_id="origin",
            local=be,
            transport=tr,
            lab_single_node=True,
            min_replicas=1,
            replica_factor=1,
        )
    )
    key = GlobalCacheKey(namespace="distlab", identifier="refuse-gd", version="v1")

    async def body(history: History, s: object) -> None:
        history.invoke("c0", OpKind.PUT, key="refuse-gd", value=42)
        put = await gcm.put(
            key,
            42,
            metadata=CacheMetadata(),
            options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
        )
        if put.success:
            history.ok("c0", OpKind.PUT, key="refuse-gd", value=42, op_id=put.operation_id)
        else:
            history.fail(
                "c0",
                OpKind.PUT,
                key="refuse-gd",
                value=42,
                error_code=put.error_code,
                error_message=put.error_message,
            )
        assert put.success, put.error_message

        history.invoke("c0", OpKind.GET, key="refuse-gd")
        bad_g = await gcm.get(
            key, options=CacheOptions(consistency_level=ConsistencyLevel.STRONG)
        )
        history.fail(
            "c0",
            OpKind.GET,
            key="refuse-gd",
            error_code=bad_g.error_code,
            error_message=bad_g.error_message,
        )
        assert bad_g.success is False
        assert bad_g.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY)

        history.invoke("c0", OpKind.DELETE, key="refuse-gd")
        bad_d = await gcm.delete(
            key, options=CacheOptions(consistency_level=ConsistencyLevel.STRONG)
        )
        history.fail(
            "c0",
            OpKind.DELETE,
            key="refuse-gd",
            error_code=bad_d.error_code,
            error_message=bad_d.error_message,
        )
        assert bad_d.success is False
        assert bad_d.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY)

        # EVENTUAL RYW still works
        history.invoke("c0", OpKind.GET, key="refuse-gd")
        ryw = await gcm.get(key)
        if ryw.success:
            history.ok("c0", OpKind.GET, key="refuse-gd", value=ryw.entry.value if ryw.entry else None)
        else:
            history.fail(
                "c0",
                OpKind.GET,
                key="refuse-gd",
                error_code=ryw.error_code,
                error_message=ryw.error_message,
            )
        assert ryw.success and ryw.entry is not None
        assert ryw.entry.value == 42
        st = gcm.strong_status()
        assert st["gets_refused"] >= 1
        assert st["deletes_refused"] >= 1
        caps = st.get("capabilities") or {}
        assert caps.get("get_quorum") is False
        assert caps.get("delete_quorum") is False
        assert caps.get("put_majority_commit") is True

    async def cleanup(_s: object) -> None:
        await gcm.shutdown()

    return Scenario(
        name="strong.refuse_get_delete",
        setup=lambda: gcm,
        body=body,
        teardown=cleanup,
        checker=NoOpenInvokeChecker(),
        meta={"track": "T18", "fault": "refuse"},
    )

def _strong_lie_prepare() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    sut.transport.lie_prepare_ok |= {"n1", "n2"}

    async def body(history: History, s: object) -> None:
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="lie", value=1
        )
        assert res.success is False

    return Scenario(
        name="strong.lie_prepare",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="lie"),
        meta={"track": "T3"},
    )

def _strong_nemesis() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    hooks = sut.nemesis_hooks()
    target = FaultInjectorNemesisTarget(
        injector=hooks["injector"],
        nodes=list(hooks["nodes"]),
        on_partition=hooks["on_partition"],
        on_heal=hooks["on_heal"],
        on_crash=hooks["on_crash"],
        on_recover=hooks["on_recover"],
        on_drop_rate=hooks["on_drop_rate"],
        on_delay=hooks["on_delay"],
    )
    nem = Nemesis(
        target=target,
        seed=42,
        interval_s=0.08,
        actions=[
            NemesisAction.PARTITION_ONE,
            NemesisAction.HEAL,
            NemesisAction.CLEAR_RATES,
        ],
    )
    clients = ConcurrentPuts(sut=sut, n_clients=8, logical_key="nem").as_clients()
    return Scenario(
        name="strong.nemesis_concurrent",
        setup=lambda: sut,
        clients=clients,
        nemesis=nem,
        checker=default_strong_checkers(key="nem"),
        meta={"track": "T2"},
    )

def _strong_interleaved() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)

    async def body(history: History, s: object) -> None:
        sut.transport.drop_prepare |= {"n1", "n2"}
        bad = await sut.put(
            history, process="c0", origin="n0", logical_key="inter", value="bad"
        )
        assert bad.success is False
        sut.transport.clear_malice()
        good = await sut.put(
            history, process="c0", origin="n0", logical_key="inter", value="good"
        )
        assert good.success is True

    return Scenario(
        name="strong.interleaved_fault_success",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="inter"),
        meta={"track": "T2"},
    )

def _strong_duplicate_commit() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    sut.transport.duplicate_commit = True

    async def body(history: History, s: object) -> None:
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="dup", value="d"
        )
        assert res.success

    return Scenario(
        name="strong.duplicate_commit",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="dup"),
        meta={"track": "T2"},
    )

def _strong_fail_prepare() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    sut.transport.fail_prepare |= {"n1", "n2"}

    async def body(history: History, s: object) -> None:
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="fp", value=1
        )
        assert res.success is False

    return Scenario(
        name="strong.fail_prepare",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="fp"),
        meta={"track": "T3"},
    )

def _strong_wrong_cluster() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    sut.transport.wrong_cluster |= {"n1", "n2"}

    async def body(history: History, s: object) -> None:
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="wc", value=1
        )
        assert res.success is False

    return Scenario(
        name="strong.wrong_cluster",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="wc"),
        meta={"track": "T3"},
    )

def _strong_delay_ok() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3, prepare_timeout_s=1.0, commit_timeout_s=1.0)
    sut.transport.delay_override_s = 0.05

    async def body(history: History, s: object) -> None:
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="dlay", value=1
        )
        assert res.success

    return Scenario(
        name="strong.delay_within_timeout",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="dlay"),
        meta={"track": "T2"},
    )

def _strong_not_bft_lie_commit() -> Scenario:
    """Both peers lie on COMMIT_ACK — DistLab documents not_bft boundary.

    With Q=2, origin can still majority-commit if it believes peers applied.
    Residual-free may not hold under Byzantine COMMIT lies — that is explicit
    non-claim, not a product failure. Scenario only asserts history closes.
    """
    from mpreg.testing.distlab.adapters.strong import StrongSUT
    from mpreg.testing.distlab.checker import NoOpenInvokeChecker

    sut = StrongSUT.create(3)
    sut.transport.lie_commit_applied |= {"n1", "n2"}

    async def body(history: History, s: object) -> None:
        # Outcome may be success (false majority) under lie — not BFT
        await sut.put(
            history, process="c0", origin="n0", logical_key="bft", value="lie"
        )

    return Scenario(
        name="strong.not_bft_lie_commit_both",
        setup=lambda: sut,
        body=body,
        # Only structural history check — residual-free is not claimed under BFT lies
        checker=NoOpenInvokeChecker(),
        meta={"track": "T3", "not_bft": True},
    )

def _strong_cft_partial_commit_lost_abort() -> Scenario:
    """T27 honesty: partial peer COMMIT + lost ABORT can leave peer L1.

    n=5, Q=3, need_peers=2. Only n1 receives COMMIT; put fails. ABORT to n1 is
    dropped so uncommit never lands. Documents CFT best-effort ABORT limit —
    **not** residual-free (not BFT). Origin remains residual-free.
    """
    from mpreg.core.cache_models import GlobalCacheKey
    from mpreg.core.cache_strong import _entry_op_id
    from mpreg.testing.distlab.adapters.strong import StrongSUT
    from mpreg.testing.distlab.checker import NoOpenInvokeChecker

    sut = StrongSUT.create(
        5,
        prepare_timeout_s=0.4,
        commit_timeout_s=0.25,
        pending_ttl_s=30.0,
    )
    # Only n1 can apply COMMIT; n2–n4 drop → need_peers=2 not met → fail
    sut.transport.drop_commit |= {"n2", "n3", "n4"}
    # Lost ABORT on the peer that did apply COMMIT
    sut.transport.drop_abort |= {"n1"}

    async def body(history: History, s: object) -> None:
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="cft", value={"cft": 1}
        )
        assert res.success is False, "put must fail without peer commit quorum"
        oid = res.operation_id or ""
        key = GlobalCacheKey(
            namespace="distlab", identifier="cft", version="v1"
        )
        # Origin residual-free (local abort always runs)
        o_ent = sut.backends["n0"].get_visible(key)
        assert o_ent is None or _entry_op_id(o_ent) != oid
        # Peer n1 may still hold L1 — CFT limit (document, do not "fix")
        n1_ent = sut.backends["n1"].get_visible(key)
        assert n1_ent is not None and _entry_op_id(n1_ent) == oid, (
            "expected CFT residual on n1 after partial commit + lost abort "
            f"(got {n1_ent!r})"
        )
        # Abort failure counter should have moved (best-effort attempts exhausted)
        coord = sut.coords["n0"]
        assert int(getattr(coord, "aborts_peer_fail", 0) or 0) >= 1

    return Scenario(
        name="strong.cft_partial_commit_lost_abort",
        setup=lambda: sut,
        body=body,
        # Structural only — residual-free is intentionally NOT claimed
        checker=NoOpenInvokeChecker(),
        meta={
            "track": "T27",
            "cft_limit": True,
            "not_residual_free": True,
            "fault": "partial_commit_lost_abort",
        },
    )

def _strong_cft_residual_survives_pending_purge() -> Scenario:
    """T29 honesty: residual L1 survives pending TTL purge after COMMIT apply.

    After partial COMMIT + lost ABORT, peer holds visible L1 with pending=0.
    Waiting past ``pending_ttl_s`` and calling ``purge_expired_pending`` must
    **not** clear that residual — purge only drops uncommitted prepares.
    """
    from mpreg.core.cache_models import GlobalCacheKey
    from mpreg.core.cache_strong import _entry_op_id
    from mpreg.testing.distlab.adapters.strong import StrongSUT
    from mpreg.testing.distlab.checker import NoOpenInvokeChecker

    sut = StrongSUT.create(
        5,
        prepare_timeout_s=0.4,
        commit_timeout_s=0.25,
        pending_ttl_s=0.05,
    )
    sut.transport.drop_commit |= {"n2", "n3", "n4"}
    sut.transport.drop_abort |= {"n1"}

    async def body(history: History, s: object) -> None:
        import asyncio

        res = await sut.put(
            history,
            process="c0",
            origin="n0",
            logical_key="ttl",
            value={"stale": True},
        )
        assert res.success is False
        oid = res.operation_id or ""
        key = GlobalCacheKey(
            namespace="distlab", identifier="ttl", version="v1"
        )
        n1 = sut.backends["n1"]
        ent = n1.get_visible(key)
        assert ent is not None and _entry_op_id(ent) == oid
        assert n1.pending_count() == 0, "COMMIT apply already cleared pending"
        await asyncio.sleep(0.08)
        purged = n1.purge_expired_pending()
        assert purged == 0  # nothing pending to purge
        ent2 = n1.get_visible(key)
        assert ent2 is not None and _entry_op_id(ent2) == oid, (
            "pending purge must not clear residual L1 after COMMIT apply"
        )
        # backups may still hold pre-commit snapshot for a future ABORT
        assert n1.backups_count() >= 0

    return Scenario(
        name="strong.cft_residual_survives_pending_purge",
        setup=lambda: sut,
        body=body,
        checker=NoOpenInvokeChecker(),
        meta={
            "track": "T29",
            "cft_limit": True,
            "not_residual_free": True,
            "pending_ttl_not_residual_gc": True,
            "fault": "partial_commit_lost_abort_then_purge",
        },
    )

def _strong_cft_residual_healed_by_lww() -> Scenario:
    """T28 honesty: later successful put can LWW-overwrite a CFT residual L1.

    Same setup as partial-commit+lost-abort (peer n1 holds failed op), then
    clear drop sets and put a new value. Peer L1 advances to the success op —
    **not** reliable ABORT; LWW heal only.
    """
    from mpreg.core.cache_models import GlobalCacheKey
    from mpreg.core.cache_strong import _entry_op_id
    from mpreg.testing.distlab.adapters.strong import StrongSUT
    from mpreg.testing.distlab.checker import NoOpenInvokeChecker

    sut = StrongSUT.create(
        5,
        prepare_timeout_s=0.4,
        commit_timeout_s=0.25,
        pending_ttl_s=30.0,
    )
    sut.transport.drop_commit |= {"n2", "n3", "n4"}
    sut.transport.drop_abort |= {"n1"}

    async def body(history: History, s: object) -> None:
        res_fail = await sut.put(
            history,
            process="c0",
            origin="n0",
            logical_key="heal",
            value={"stale": True},
        )
        assert res_fail.success is False
        fail_oid = res_fail.operation_id or ""
        key = GlobalCacheKey(
            namespace="distlab", identifier="heal", version="v1"
        )
        n1_stale = sut.backends["n1"].get_visible(key)
        assert n1_stale is not None and _entry_op_id(n1_stale) == fail_oid

        # Heal path: clear transport faults; majority put overwrites residual
        sut.transport.drop_commit.clear()
        sut.transport.drop_abort.clear()
        res_ok = await sut.put(
            history,
            process="c0",
            origin="n0",
            logical_key="heal",
            value={"healed": True},
        )
        assert res_ok.success is True, res_ok.error_message
        ok_oid = res_ok.operation_id or ""
        assert ok_oid and ok_oid != fail_oid
        for nid in ("n0", "n1", "n2", "n3", "n4"):
            ent = sut.backends[nid].get_visible(key)
            assert ent is not None, f"{nid} missing healed value"
            assert _entry_op_id(ent) == ok_oid, (
                f"{nid} still on residual op {_entry_op_id(ent)!r} "
                f"want {ok_oid!r}"
            )
            assert ent.value == {"healed": True}

    return Scenario(
        name="strong.cft_residual_healed_by_lww",
        setup=lambda: sut,
        body=body,
        checker=NoOpenInvokeChecker(),
        meta={
            "track": "T28",
            "cft_limit": True,
            "lww_heal": True,
            "not_reliable_abort": True,
            "fault": "partial_commit_lost_abort_then_lww",
        },
    )

def _strong_soak_n(n_puts: int) -> Scenario:
    import time

    from mpreg.testing.distlab.adapters.strong import StrongSUT
    from mpreg.testing.distlab.models import OpKind, OpStatus
    from mpreg.testing.distlab.sli import DEFAULT_STRONG_SOAK_BUDGET

    sut = StrongSUT.create(3)
    gen = SequentialPuts(sut=sut, n=n_puts, logical_key="soak")

    async def body(history: History, s: object) -> None:
        t0 = time.perf_counter()
        await gen.as_body()(history, sut)
        duration_s = time.perf_counter() - t0
        fails = history.failed_puts("soak")
        assert not fails, f"soak failures: {fails}"
        latencies: list[float] = []
        for inv, term in history.pairs():
            if inv.kind is not OpKind.PUT or inv.key != "soak":
                continue
            if term is None or term.status is not OpStatus.OK:
                continue
            latencies.append((term.wall_time - inv.wall_time) * 1000.0)
        if not latencies and n_puts > 0:
            latencies.append((duration_s * 1000.0) / n_puts)
        budget = DEFAULT_STRONG_SOAK_BUDGET.check(
            latency_ms=latencies,
            duration_s=duration_s,
            success=n_puts - len(fails),
            total=n_puts,
        )
        assert budget["ok"], budget["violations"]

    return Scenario(
        name=f"strong.soak_{n_puts}",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="soak"),
        meta={"track": "T2", "sli": True, "n_puts": n_puts},
    )

def _audit_multi() -> Scenario:
    from mpreg.testing.distlab.adapters.audit import AuditSUT

    sut = AuditSUT.create(3)
    return Scenario(
        name="audit.multi_origin",
        setup=lambda: sut,
        body=AuditBurst(sut=sut, n=6, prefix="m").as_body(),
        checker=default_audit_checkers(min_ids=6),
        meta={"track": "T4"},
    )

def _audit_burst() -> Scenario:
    from mpreg.testing.distlab.adapters.audit import AuditSUT

    sut = AuditSUT.create(3)
    return Scenario(
        name="audit.burst_30",
        setup=lambda: sut,
        body=AuditBurst(sut=sut, n=30, prefix="b").as_body(),
        checker=default_audit_checkers(min_ids=30),
        meta={"track": "T4"},
    )

def _audit_partition_heal() -> Scenario:
    from mpreg.testing.distlab.adapters.audit import AuditSUT

    sut = AuditSUT.create(3)
    hooks = sut.nemesis_hooks()
    target = FaultInjectorNemesisTarget(
        injector=hooks["injector"],
        nodes=list(hooks["nodes"]),
        on_partition=hooks["on_partition"],
        on_heal=hooks["on_heal"],
        on_crash=hooks["on_crash"],
        on_recover=hooks["on_recover"],
        on_drop_rate=hooks["on_drop_rate"],
        on_delay=hooks["on_delay"],
    )

    async def body(history: History, s: object) -> None:
        target.apply_partition_groups([{"a0"}, {"a1", "a2"}])
        for i in range(4):
            await sut.publish(
                history, process="c0", origin="a0", event=f"p{i}"
            )
        target.heal_network()
        await sut.flush_all()
        await sut.reconcile_all()
        await sut.reconcile_all()

    return Scenario(
        name="audit.partition_heal",
        setup=lambda: sut,
        body=body,
        checker=default_audit_checkers(min_ids=4),
        meta={"track": "T4"},
    )

def _audit_digest_repair() -> Scenario:
    from mpreg.testing.distlab.adapters.audit import AuditSUT

    sut = AuditSUT.create(2)
    sut.transport.drop_types.add("mgmt_audit_delta")

    async def body(history: History, s: object) -> None:
        await sut.publish(history, process="c0", origin="a0", event="repair")
        assert sut.stores["a1"].size() == 0
        sut.transport.drop_types.clear()
        await sut.reconcile_all()

    return Scenario(
        name="audit.digest_repair",
        setup=lambda: sut,
        body=body,
        checker=default_audit_checkers(min_ids=1),
        meta={"track": "T4"},
    )

def _audit_ineligible() -> Scenario:
    from mpreg.testing.distlab.adapters.audit import AuditSUT

    sut = AuditSUT.create(2)

    async def body(history: History, s: object) -> None:
        await sut.publish(
            history,
            process="c0",
            origin="a0",
            event="secret",
            eligible=False,
        )
        await sut.flush_all()
        await sut.reconcile_all()
        assert sut.stores["a1"].size() == 0

    return Scenario(
        name="audit.ineligible_local",
        setup=lambda: sut,
        body=body,
        checker=default_audit_checkers(min_ids=0),
        meta={"track": "T4"},
    )

def _audit_nemesis() -> Scenario:
    from mpreg.testing.distlab.adapters.audit import AuditSUT

    sut = AuditSUT.create(3)
    hooks = sut.nemesis_hooks()
    target = FaultInjectorNemesisTarget(
        injector=hooks["injector"],
        nodes=list(hooks["nodes"]),
        on_partition=hooks["on_partition"],
        on_heal=hooks["on_heal"],
        on_crash=hooks["on_crash"],
        on_recover=hooks["on_recover"],
        on_drop_rate=hooks["on_drop_rate"],
        on_delay=hooks["on_delay"],
    )
    nem = Nemesis(
        target=target,
        seed=7,
        interval_s=0.05,
        actions=[NemesisAction.PARTITION_ONE, NemesisAction.HEAL],
    )

    async def body(history: History, s: object) -> None:
        for i in range(12):
            await sut.publish(
                history,
                process=f"c{i}",
                origin=sut.node_ids[i % 3],
                event=f"n{i}",
            )
            await sut.flush_all()
        await sut.flush_all()
        await sut.reconcile_all()
        await sut.reconcile_all()

    return Scenario(
        name="audit.nemesis",
        setup=lambda: sut,
        body=body,
        nemesis=nem,
        checker=default_audit_checkers(min_ids=12),
        meta={"track": "T4"},
    )

def _audit_burst_n(n: int) -> Scenario:
    from mpreg.testing.distlab.adapters.audit import AuditSUT

    sut = AuditSUT.create(3)
    return Scenario(
        name=f"audit.burst_{n}",
        setup=lambda: sut,
        body=AuditBurst(sut=sut, n=n, prefix="b").as_body(),
        checker=default_audit_checkers(min_ids=n),
        meta={"track": "T4"},
    )

def _audit_duplicate() -> Scenario:
    from mpreg.testing.distlab.adapters.audit import AuditSUT

    sut = AuditSUT.create(2)

    async def body(history: History, s: object) -> None:
        # same event id twice via explicit publish then re-reconcile
        await sut.publish(history, process="c0", origin="a0", event="dup-ev")
        await sut.publish(history, process="c1", origin="a0", event="dup-ev")
        await sut.flush_all()
        await sut.reconcile_all()

    return Scenario(
        name="audit.duplicate_idempotent",
        setup=lambda: sut,
        body=body,
        checker=default_audit_checkers(min_ids=1),
        meta={"track": "T4"},
    )

def _strong_single_node() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(1, min_replicas=1, replica_factor=1)

    async def body(history: History, s: object) -> None:
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="solo", value=1
        )
        assert res.success, res.error_message

    return Scenario(
        name="strong.single_node",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="solo"),
        meta={"track": "T2"},
    )

def _strong_partition_one_peer() -> Scenario:
    """Isolate one peer; origin+other peer can still form Q=2."""
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    hooks = sut.nemesis_hooks()
    target = FaultInjectorNemesisTarget(
        injector=hooks["injector"],
        nodes=list(hooks["nodes"]),
        on_partition=hooks["on_partition"],
        on_heal=hooks["on_heal"],
        on_crash=hooks["on_crash"],
        on_recover=hooks["on_recover"],
        on_drop_rate=hooks["on_drop_rate"],
        on_delay=hooks["on_delay"],
    )

    async def body(history: History, s: object) -> None:
        # Isolate n2 only — n0/n1 still connected
        target.apply_partition_groups([{"n0", "n1"}, {"n2"}])
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="p1", value="ok"
        )
        assert res.success is True, res.error_message
        target.heal_network()

    return Scenario(
        name="strong.partition_one_peer",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="p1"),
        meta={"track": "T2"},
    )

def _strong_crash_recover() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    hooks = sut.nemesis_hooks()
    target = FaultInjectorNemesisTarget(
        injector=hooks["injector"],
        nodes=list(hooks["nodes"]),
        on_partition=hooks["on_partition"],
        on_heal=hooks["on_heal"],
        on_crash=hooks["on_crash"],
        on_recover=hooks["on_recover"],
        on_drop_rate=hooks["on_drop_rate"],
        on_delay=hooks["on_delay"],
    )

    async def body(history: History, s: object) -> None:
        target.crash_node("n2")
        # Q=2 still possible with n0+n1
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="cr", value=1
        )
        # May succeed (2 live) or fail depending on replica selection — residual free either way
        target.recover_node("n2")
        target.heal_network()
        good = await sut.put(
            history, process="c1", origin="n0", logical_key="cr", value=2
        )
        assert good.success is True

    return Scenario(
        name="strong.crash_recover",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="cr"),
        meta={"track": "T2"},
    )

def _strong_delay_beyond_timeout() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3, prepare_timeout_s=0.08, commit_timeout_s=0.08)
    sut.transport.delay_override_s = 0.25

    async def body(history: History, s: object) -> None:
        res = await sut.put(
            history, process="c0", origin="n0", logical_key="dto", value=1
        )
        assert res.success is False

    return Scenario(
        name="strong.delay_beyond_timeout",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="dto"),
        meta={"track": "T2"},
    )

def _strong_lie_commit_single() -> Scenario:
    """One peer lies on COMMIT_ACK — Q=2 may still form honestly with other peer."""
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    sut.transport.lie_commit_applied |= {"n2"}

    async def body(history: History, s: object) -> None:
        # Success is allowed; residual-free still required for any fails
        await sut.put(
            history, process="c0", origin="n0", logical_key="lc1", value="x"
        )

    return Scenario(
        name="strong.lie_commit_single_peer",
        setup=lambda: sut,
        body=body,
        # Only structural if success under partial lie; residual if fail
        checker=default_strong_checkers(key="lc1"),
        meta={"track": "T3"},
        strict=False,  # partial BFT edge — record outcome; registry test checks residual on fail
    )

def _strong_sequential_lww() -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)

    async def body(history: History, s: object) -> None:
        for i in range(5):
            res = await sut.put(
                history,
                process=f"c{i}",
                origin=f"n{i % 3}",
                logical_key="lww",
                value=i,
                op_id=f"lww-{i}",
            )
            assert res.success, res.error_message
        # last successful value must be final
        assert sut.snapshot_state().final_value("lww") == 4

    return Scenario(
        name="strong.sequential_lww",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="lww"),
        meta={"track": "T2"},
    )

def _strong_pending_full() -> Scenario:
    """max_pending exhaustion surfaces fail without residual dirty apply."""
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3, max_pending=1)
    # Hold a pending by dropping prepare so slot stays full, then second put
    sut.transport.drop_prepare |= {"n1", "n2"}

    async def body(history: History, s: object) -> None:
        bad1 = await sut.put(
            history, process="c0", origin="n0", logical_key="pf1", value=1
        )
        assert bad1.success is False
        # After fail, pending should abort; second put with malice cleared
        sut.transport.clear_malice()
        good = await sut.put(
            history, process="c1", origin="n0", logical_key="pf2", value=2
        )
        assert good.success is True

    return Scenario(
        name="strong.pending_full_recover",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(),
        meta={"track": "T6", "reg": "pending_full"},
    )

def _reg_expired_commit() -> Scenario:
    """Regression: expired pending rejects commit (reason=expired path)."""
    import time

    from mpreg.core.cache_models import CacheMetadata, GlobalCacheKey
    from mpreg.core.cache_strong import StrongVersion
    from mpreg.testing.distlab.adapters.strong import StrongSUT
    from mpreg.testing.distlab.checker import NoOpenInvokeChecker
    from mpreg.testing.distlab.models import OpKind

    sut = StrongSUT.create(1, min_replicas=1, replica_factor=1, pending_ttl_s=0.05)
    be = sut.backends["n0"]
    key = GlobalCacheKey(namespace="distlab", identifier="exp", version="v1")

    async def body(history: History, s: object) -> None:
        history.invoke("c0", OpKind.PUT, key="exp", value=1, op_id="exp-op")
        ver = StrongVersion(
            logical_ts=1,
            origin_node="n0",
            op_id="exp-op",
        )
        ack = await be.prepare(
            key=key,
            value=1,
            metadata=CacheMetadata(),
            strong_version=ver,
            replica_set=("n0",),
            quorum=1,
            ttl_s=0.05,
        )
        assert ack.ok
        # Force expiry without relying solely on purge removing slot first
        pending = be._pending.get("exp-op")
        if pending is not None:
            pending.expires_at = time.time() - 1.0
        cack = await be.commit(op_id="exp-op", key=key)
        assert cack.ok is False
        assert (cack.reason or "") == "expired"
        history.fail(
            "c0",
            OpKind.PUT,
            key="exp",
            value=1,
            op_id="exp-op",
            error_message=cack.reason or "expired",
        )

    return Scenario(
        name="reg_expired_commit",
        setup=lambda: sut,
        body=body,
        checker=NoOpenInvokeChecker(),
        meta={"track": "T6", "reg": "expired_commit"},
    )

def register_builtins(registry=None) -> int:
    """Idempotent registration of all built-in scenarios. Returns count.

    Always fills in any missing names (additive), so new scenarios ship without
    requiring a process restart after an earlier ensure_builtins() call.
    """
    global _REGISTERED
    reg = registry if registry is not None else DEFAULT_REGISTRY

    specs: list[tuple[str, object, str, str, tuple[str, ...]]] = [
        ("strong.happy_3", lambda: _strong_happy(3, "k3"), "T2", "3-node happy", ("strong", "happy")),
        ("strong.happy_5", lambda: _strong_happy(5, "k5"), "T2", "5-node Q=3", ("strong", "happy")),
        ("strong.happy_7", lambda: _strong_happy(7, "k7"), "T2", "7-node Q=4", ("strong", "happy")),
        ("strong.single_node", _strong_single_node, "T2", "min_replicas=1 lab", ("strong", "happy")),
        ("strong.soak_20", lambda: _strong_soak_n(20), "T2", "20 multi-origin soak", ("strong", "soak")),
        ("strong.soak_50", lambda: _strong_soak_n(50), "T2", "50 multi-origin soak", ("strong", "soak")),
        ("strong.sequential_lww", _strong_sequential_lww, "T2", "sequential LWW last wins", ("strong", "lww")),
        ("strong.partition_majority", _strong_partition_majority, "T2", "partition residual", ("strong", "fault")),
        ("strong.partition_one_peer", _strong_partition_one_peer, "T2", "isolate one peer still Q", ("strong", "fault")),
        ("strong.heal", _strong_heal, "T2", "heal then success", ("strong", "fault")),
        ("strong.concurrent_same_key", _strong_concurrent, "T2", "concurrent LWW", ("strong", "conc")),
        ("strong.concurrent_multi_key", _strong_multi_key, "T2", "multi-key", ("strong", "conc")),
        ("strong.drop_prepare", _strong_drop_prepare, "T2", "drop prepare", ("strong", "fault")),
        ("strong.drop_commit", _strong_drop_commit, "T2", "drop commit", ("strong", "fault")),
        ("strong.drop_abort", _strong_drop_abort, "T13", "drop abort residual-free", ("strong", "fault")),
        (
            "strong.refuse_get_delete",
            _strong_refuse_get_delete,
            "T18",
            "STRONG get/delete 1012 refuse + RYW",
            ("strong", "refuse"),
        ),
        ("strong.fail_prepare", _strong_fail_prepare, "T3", "fail prepare residual", ("strong", "adv")),
        ("strong.wrong_cluster", _strong_wrong_cluster, "T3", "wrong cluster residual", ("strong", "adv")),
        ("strong.delay_within_timeout", _strong_delay_ok, "T2", "delay within timeout", ("strong", "fault")),
        ("strong.delay_beyond_timeout", _strong_delay_beyond_timeout, "T2", "delay beyond timeout", ("strong", "fault")),
        ("strong.crash_recover", _strong_crash_recover, "T2", "crash one then recover", ("strong", "fault")),
        ("strong.lie_prepare", _strong_lie_prepare, "T3", "lie prepare residual", ("strong", "adv")),
        ("strong.lie_commit_single_peer", _strong_lie_commit_single, "T3", "single peer COMMIT lie", ("strong", "adv")),
        ("strong.not_bft_lie_commit_both", _strong_not_bft_lie_commit, "T3", "BFT boundary demo", ("strong", "not_bft")),
        (
            "strong.cft_partial_commit_lost_abort",
            _strong_cft_partial_commit_lost_abort,
            "T27",
            "CFT limit: partial COMMIT + lost ABORT peer L1",
            ("strong", "cft_limit", "fault"),
        ),
        (
            "strong.cft_residual_healed_by_lww",
            _strong_cft_residual_healed_by_lww,
            "T28",
            "CFT residual then LWW heal (not reliable ABORT)",
            ("strong", "cft_limit", "lww_heal"),
        ),
        (
            "strong.cft_residual_survives_pending_purge",
            _strong_cft_residual_survives_pending_purge,
            "T29",
            "CFT residual survives pending TTL purge (not residual GC)",
            ("strong", "cft_limit", "fault"),
        ),
        ("strong.nemesis_concurrent", _strong_nemesis, "T2", "nemesis concurrent", ("strong", "nemesis")),
        ("strong.interleaved_fault_success", _strong_interleaved, "T2", "fault then ok", ("strong", "fault")),
        ("strong.duplicate_commit", _strong_duplicate_commit, "T2", "dup commit", ("strong", "fault")),
        ("strong.pending_full_recover", _strong_pending_full, "T6", "pending full then recover", ("strong", "reg")),
        ("reg_expired_commit", _reg_expired_commit, "T6", "expired commit reject", ("strong", "reg")),
        ("audit.multi_origin", _audit_multi, "T4", "multi-origin", ("audit",)),
        ("audit.burst_30", lambda: _audit_burst_n(30), "T4", "burst 30", ("audit", "soak")),
        ("audit.burst_100", lambda: _audit_burst_n(100), "T4", "burst 100", ("audit", "soak")),
        ("audit.partition_heal", _audit_partition_heal, "T4", "part heal", ("audit", "fault")),
        ("audit.digest_repair", _audit_digest_repair, "T4", "digest repair", ("audit", "fault")),
        ("audit.ineligible_local", _audit_ineligible, "T4", "ineligible", ("audit",)),
        ("audit.duplicate_idempotent", _audit_duplicate, "T4", "duplicate idempotent", ("audit",)),
        ("audit.nemesis", _audit_nemesis, "T4", "nemesis audit", ("audit", "nemesis")),
    ]
    for name, factory, track, desc, tags in specs:
        if name in reg.list():
            continue
        reg.register(name, factory, track=track, description=desc, tags=tags)  # type: ignore[arg-type]

    if registry is None:
        _REGISTERED = True
    return len(reg.list())

def ensure_builtins() -> None:
    register_builtins()
