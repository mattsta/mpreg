"""STRONG platform validation via first-party DistLab scenarios."""

from __future__ import annotations

import asyncio

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.testing.distlab import (
    FaultInjectorNemesisTarget,
    History,
    Nemesis,
    NemesisAction,
    Scenario,
    StrongSUT,
    default_strong_checkers,
)

def _sut(n: int = 3, **kw) -> StrongSUT:
    return StrongSUT.create(n, **kw)

@pytest.mark.asyncio
async def test_distlab_strong_happy_3() -> None:
    sut = _sut(3)

    async def body(history: History, s: StrongSUT) -> None:
        res = await s.put(
            history, process="c0", origin="n0", logical_key="k", value={"v": 1}
        )
        assert res.success

    r = await Scenario(
        name="strong-happy-3",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="k"),
        strict=True,
    ).run()
    assert r.ok
    assert r.history_len >= 2

@pytest.mark.asyncio
async def test_distlab_strong_happy_5_quorum() -> None:
    sut = _sut(5, prepare_timeout_s=0.6, commit_timeout_s=0.6)

    async def body(history: History, s: StrongSUT) -> None:
        res = await s.put(
            history, process="c0", origin="n0", logical_key="k5", value=5
        )
        assert res.success
        assert res.quorum_info and res.quorum_info["quorum"] == 3

    await Scenario(
        name="strong-happy-5",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="k5"),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_strong_partition_majority_residual_free() -> None:
    sut = _sut(3)
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
    # Isolate n0 from others
    target.apply_partition_groups([{"n0"}, {"n1", "n2"}])

    async def body(history: History, s: StrongSUT) -> None:
        res = await s.put(
            history, process="c0", origin="n0", logical_key="part", value="x"
        )
        assert res.success is False

    await Scenario(
        name="strong-part-maj",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="part"),
        strict=True,
    ).run()
    target.heal_network()

@pytest.mark.asyncio
async def test_distlab_strong_heal_then_success() -> None:
    sut = _sut(3)
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

    async def body(history: History, s: StrongSUT) -> None:
        target.apply_partition_groups([{"n0"}, {"n1", "n2"}])
        bad = await s.put(
            history, process="c0", origin="n0", logical_key="heal", value="no"
        )
        assert bad.success is False
        target.heal_network()
        good = await s.put(
            history, process="c0", origin="n0", logical_key="heal", value="yes"
        )
        assert good.success is True

    await Scenario(
        name="strong-heal",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="heal"),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_strong_concurrent_same_key() -> None:
    sut = _sut(3)

    async def client(history: History, idx: int) -> None:
        origin = f"n{idx % 3}"
        await sut.put(
            history,
            process=f"c{idx}",
            origin=origin,
            logical_key="ck",
            value=idx,
            op_id=f"op-{idx}",
        )

    await Scenario(
        name="strong-conc",
        setup=lambda: sut,
        clients=[client, client, client, client],
        checker=default_strong_checkers(key="ck"),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_strong_multi_key_concurrent() -> None:
    sut = _sut(3)

    async def client(history: History, idx: int) -> None:
        await sut.put(
            history,
            process=f"c{idx}",
            origin=f"n{idx % 3}",
            logical_key=f"mk{idx}",
            value=idx,
            op_id=f"mk-op-{idx}",
        )

    # Multi-key: residual + agreement only (LWW per key still ok)
    await Scenario(
        name="strong-mk",
        setup=lambda: sut,
        clients=[client] * 6,
        checker=default_strong_checkers(),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_strong_soak_multi_origin() -> None:
    sut = _sut(3)

    async def body(history: History, s: StrongSUT) -> None:
        for i in range(20):
            res = await s.put(
                history,
                process=f"c{i}",
                origin=f"n{i % 3}",
                logical_key="soak",
                value=i,
            )
            assert res.success, res.error_message

    await Scenario(
        name="strong-soak",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="soak"),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_strong_drop_prepare_residual() -> None:
    sut = _sut(3)
    sut.transport.drop_prepare |= {"n1", "n2"}

    async def body(history: History, s: StrongSUT) -> None:
        res = await s.put(
            history, process="c0", origin="n0", logical_key="dp", value=1
        )
        assert res.success is False

    await Scenario(
        name="strong-drop-prep",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="dp"),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_strong_nemesis_during_puts() -> None:
    sut = _sut(3)
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

    async def client(history: History, idx: int) -> None:
        await sut.put(
            history,
            process=f"c{idx}",
            origin=f"n{idx % 3}",
            logical_key="nem",
            value=idx,
            op_id=f"nem-{idx}",
        )

    # After nemesis stop, heal — checkers must pass residual/LWW
    r = await Scenario(
        name="strong-nemesis",
        setup=lambda: sut,
        clients=[client] * 8,
        nemesis=nem,
        checker=default_strong_checkers(key="nem"),
        strict=True,
    ).run()
    assert r.nemesis_actions >= 1

@given(drops=st.lists(st.sampled_from(["n1", "n2"]), max_size=2, unique=True))
@settings(max_examples=15, deadline=None)
def test_distlab_hypothesis_prepare_drops(drops: list[str]) -> None:
    async def _run() -> None:
        sut = _sut(3)
        sut.transport.drop_prepare |= set(drops)

        async def body(history: History, s: StrongSUT) -> None:
            await s.put(
                history, process="c0", origin="n0", logical_key="hyp", value=1
            )

        await Scenario(
            name="hyp-drop",
            setup=lambda: sut,
            body=body,
            checker=default_strong_checkers(key="hyp"),
            strict=True,
        ).run()

    asyncio.run(_run())

@given(values=st.lists(st.integers(0, 200), min_size=2, max_size=5))
@settings(max_examples=12, deadline=None)
def test_distlab_hypothesis_concurrent_lww(values: list[int]) -> None:
    async def _run() -> None:
        sut = _sut(3)

        async def client(history: History, idx: int) -> None:
            v = values[idx % len(values)]
            await sut.put(
                history,
                process=f"c{idx}",
                origin=f"n{idx % 3}",
                logical_key="hlww",
                value=v,
                op_id=f"h-{idx}",
            )

        await Scenario(
            name="hyp-lww",
            setup=lambda: sut,
            clients=[client] * len(values),
            checker=default_strong_checkers(key="hlww"),
            strict=True,
        ).run()

    asyncio.run(_run())

@pytest.mark.asyncio
async def test_distlab_strong_lie_prepare_residual() -> None:
    sut = _sut(3)
    sut.transport.lie_prepare_ok |= {"n1", "n2"}

    async def body(history: History, s: StrongSUT) -> None:
        res = await s.put(
            history, process="c0", origin="n0", logical_key="lie", value=1
        )
        assert res.success is False

    await Scenario(
        name="strong-lie-prep",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="lie"),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_strong_not_bft_lie_commit_documented() -> None:
    """CFT trusts COMMIT_ACK applied — lying peers may leave empty L1.

    This is an honest non_claim surface, not a product bug. DistLab records
    the history so claims can point at a concrete demonstration.
    """
    sut = _sut(3)
    sut.transport.lie_commit_applied |= {"n1", "n2"}
    history = History()
    res = await sut.put(
        history, process="c0", origin="n0", logical_key="bft", value="lie"
    )
    if res.success:
        # Origin has value; at least one peer empty → not BFT
        assert sut.backends["n0"].get_visible(sut.key("bft")) is not None
        miss = sum(
            1
            for n in ("n1", "n2")
            if sut.backends[n].get_visible(sut.key("bft")) is None
        )
        assert miss >= 1
        # Do not run replica-agreement requiring all nodes — CFT non_claim
        from mpreg.testing.distlab import ResidualFreeChecker, NoOpenInvokeChecker
        from mpreg.testing.distlab.checker import CompositeChecker

        # Residual-free only applies to failures; successes may diverge under lies
        r = CompositeChecker(
            name="not_bft",
            checkers=[NoOpenInvokeChecker()],
        ).check(history, state=sut.snapshot_state())
        assert r.ok
    else:
        r = default_strong_checkers(key="bft").check(
            history, state=sut.snapshot_state()
        )
        assert r.ok, r.violations

@pytest.mark.asyncio
async def test_distlab_strong_happy_7() -> None:
    sut = _sut(7, prepare_timeout_s=0.8, commit_timeout_s=0.8)

    async def body(history: History, s: StrongSUT) -> None:
        res = await s.put(
            history, process="c0", origin="n0", logical_key="k7", value=7
        )
        assert res.success
        assert res.quorum_info and res.quorum_info["quorum"] == 4

    await Scenario(
        name="strong-happy-7",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="k7"),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_strong_drop_commit_residual() -> None:
    sut = _sut(3)
    sut.transport.drop_commit |= {"n1", "n2"}

    async def body(history: History, s: StrongSUT) -> None:
        res = await s.put(
            history, process="c0", origin="n0", logical_key="dc", value=1
        )
        assert res.success is False

    await Scenario(
        name="strong-drop-commit",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="dc"),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_strong_fail_prepare_and_wrong_cluster() -> None:
    for malice, key in (
        ("fail_prepare", "fp"),
        ("wrong_cluster", "wc"),
    ):
        sut = _sut(3)
        getattr(sut.transport, malice).update({"n1", "n2"})

        async def body(history: History, s: StrongSUT, k=key) -> None:
            res = await s.put(
                history, process="c0", origin="n0", logical_key=k, value=1
            )
            assert res.success is False

        await Scenario(
            name=f"strong-{malice}",
            setup=lambda s=sut: s,
            body=body,
            checker=default_strong_checkers(key=key),
            strict=True,
        ).run()

@pytest.mark.asyncio
async def test_distlab_strong_duplicate_commit_idempotent() -> None:
    sut = _sut(3)
    sut.transport.duplicate_commit = True

    async def body(history: History, s: StrongSUT) -> None:
        res = await s.put(
            history, process="c0", origin="n0", logical_key="dup", value="d"
        )
        assert res.success

    await Scenario(
        name="strong-dup",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="dup"),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_strong_interleaved_fault_success() -> None:
    sut = _sut(3)

    async def body(history: History, s: StrongSUT) -> None:
        s.transport.drop_prepare |= {"n1", "n2"}
        bad = await s.put(
            history, process="c0", origin="n0", logical_key="inter", value="bad"
        )
        assert bad.success is False
        s.transport.clear_malice()
        good = await s.put(
            history, process="c0", origin="n0", logical_key="inter", value="good"
        )
        assert good.success is True

    await Scenario(
        name="strong-inter",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="inter"),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_strong_delay_within_timeout() -> None:
    sut = _sut(3, prepare_timeout_s=1.0, commit_timeout_s=1.0)
    sut.transport.delay_override_s = 0.05

    async def body(history: History, s: StrongSUT) -> None:
        res = await s.put(
            history, process="c0", origin="n0", logical_key="dlay", value=1
        )
        assert res.success

    await Scenario(
        name="strong-delay",
        setup=lambda: sut,
        body=body,
        checker=default_strong_checkers(key="dlay"),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_registry_strong_subset() -> None:
    from mpreg.testing.distlab import ensure_builtins, get_registry

    ensure_builtins()
    reg = get_registry()
    for name in (
        "strong.happy_3",
        "strong.partition_majority",
        "strong.heal",
        "strong.drop_prepare",
        "strong.interleaved_fault_success",
        "strong.lie_prepare",
        "strong.not_bft_lie_commit_both",
    ):
        r = await reg.run(name)
        assert r.ok, f"{name} failed: {r.check.violations}"

@given(seed=st.integers(0, 50))
@settings(max_examples=8, deadline=None)
def test_distlab_hypothesis_partition_heal(seed: int) -> None:
    async def _run() -> None:
        sut = _sut(3, seed=seed)
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
        target.apply_partition_groups([{"n0"}, {"n1", "n2"}])
        history = History()
        bad = await sut.put(
            history, process="c0", origin="n0", logical_key="ph", value=0
        )
        assert bad.success is False
        target.heal_network()
        good = await sut.put(
            history, process="c0", origin="n0", logical_key="ph", value=1
        )
        assert good.success is True
        r = default_strong_checkers(key="ph").check(
            history, state=sut.snapshot_state()
        )
        assert r.ok, r.violations

    asyncio.run(_run())
