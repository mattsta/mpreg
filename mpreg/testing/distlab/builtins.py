"""Register built-in DistLab scenarios into DEFAULT_REGISTRY."""

from __future__ import annotations

from mpreg.testing.distlab.checker import default_audit_checkers, default_strong_checkers
from mpreg.testing.distlab.generator import AuditBurst, ConcurrentPuts, SequentialPuts
from mpreg.testing.distlab.history import History
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

def _strong_soak_n(n_puts: int) -> Scenario:
    from mpreg.testing.distlab.adapters.strong import StrongSUT

    sut = StrongSUT.create(3)
    gen = SequentialPuts(sut=sut, n=n_puts, logical_key="soak")

    async def body(history: History, s: object) -> None:
        await gen.as_body()(history, sut)
        fails = history.failed_puts("soak")
        assert not fails, f"soak failures: {fails}"

    return Scenario(
        name=f"strong.soak_{n_puts}",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="soak"),
        meta={"track": "T2"},
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

def register_builtins(registry=None) -> int:
    """Idempotent registration of all built-in scenarios. Returns count."""
    global _REGISTERED
    reg = registry if registry is not None else DEFAULT_REGISTRY
    if registry is None and _REGISTERED:
        return len(reg.list())

    specs: list[tuple[str, object, str, str, tuple[str, ...]]] = [
        ("strong.happy_3", lambda: _strong_happy(3, "k3"), "T2", "3-node happy", ("strong", "happy")),
        ("strong.happy_5", lambda: _strong_happy(5, "k5"), "T2", "5-node Q=3", ("strong", "happy")),
        ("strong.happy_7", lambda: _strong_happy(7, "k7"), "T2", "7-node Q=4", ("strong", "happy")),
        ("strong.soak_20", lambda: _strong_soak_n(20), "T2", "20 multi-origin soak", ("strong", "soak")),
        ("strong.soak_50", lambda: _strong_soak_n(50), "T2", "50 multi-origin soak", ("strong", "soak")),
        ("strong.partition_majority", _strong_partition_majority, "T2", "partition residual", ("strong", "fault")),
        ("strong.heal", _strong_heal, "T2", "heal then success", ("strong", "fault")),
        ("strong.concurrent_same_key", _strong_concurrent, "T2", "concurrent LWW", ("strong", "conc")),
        ("strong.concurrent_multi_key", _strong_multi_key, "T2", "multi-key", ("strong", "conc")),
        ("strong.drop_prepare", _strong_drop_prepare, "T2", "drop prepare", ("strong", "fault")),
        ("strong.drop_commit", _strong_drop_commit, "T2", "drop commit", ("strong", "fault")),
        ("strong.fail_prepare", _strong_fail_prepare, "T3", "fail prepare residual", ("strong", "adv")),
        ("strong.wrong_cluster", _strong_wrong_cluster, "T3", "wrong cluster residual", ("strong", "adv")),
        ("strong.delay_within_timeout", _strong_delay_ok, "T2", "delay within timeout", ("strong", "fault")),
        ("strong.lie_prepare", _strong_lie_prepare, "T3", "lie prepare residual", ("strong", "adv")),
        ("strong.not_bft_lie_commit_both", _strong_not_bft_lie_commit, "T3", "BFT boundary demo", ("strong", "not_bft")),
        ("strong.nemesis_concurrent", _strong_nemesis, "T2", "nemesis concurrent", ("strong", "nemesis")),
        ("strong.interleaved_fault_success", _strong_interleaved, "T2", "fault then ok", ("strong", "fault")),
        ("strong.duplicate_commit", _strong_duplicate_commit, "T2", "dup commit", ("strong", "fault")),
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
