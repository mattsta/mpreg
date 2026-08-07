"""Unit tests for DistLab itself (platform testing framework)."""

from __future__ import annotations

import asyncio

import pytest

from mpreg.testing.distlab import (
    CompositeChecker,
    History,
    Nemesis,
    NemesisAction,
    NoOpenInvokeChecker,
    NullNemesisTarget,
    OpKind,
    OpStatus,
    ResidualFreeChecker,
    Scenario,
    default_strong_checkers,
)
from mpreg.testing.distlab.checker import LWWRegisterChecker
from mpreg.testing.distlab.models import CheckResult, CheckViolation


def test_history_monotonic_and_pairing() -> None:
    h = History()
    h.invoke("c0", OpKind.PUT, key="k", value=1, op_id="a")
    h.ok("c0", OpKind.PUT, key="k", value=1, op_id="a")
    h.invoke("c1", OpKind.PUT, key="k", value=2, op_id="b")
    h.fail("c1", OpKind.PUT, key="k", value=2, op_id="b", error_code=1016)
    assert len(h) == 4
    pairs = h.pairs()
    assert len(pairs) == 2
    assert pairs[0][1] is not None and pairs[0][1].status is OpStatus.OK
    assert pairs[1][1] is not None and pairs[1][1].status is OpStatus.FAIL
    assert h.successful_puts("k")[0].op_id == "a"
    assert h.failed_puts("k")[0].error_code == 1016


def test_no_open_invoke_checker() -> None:
    h = History()
    h.invoke("c0", OpKind.PUT, key="k", value=1)
    r = NoOpenInvokeChecker().check(h)
    assert r.ok is False
    h.ok("c0", OpKind.PUT, key="k", value=1, op_id="x")
    r2 = NoOpenInvokeChecker().check(h)
    assert r2.ok is True


def test_composite_merges_failures() -> None:
    h = History()
    h.invoke("c", OpKind.PUT, key="k", value=1)
    # leave open
    bad = CompositeChecker(
        name="c",
        checkers=[NoOpenInvokeChecker(), ResidualFreeChecker()],
    )
    r = bad.check(h, state=None)
    assert r.ok is False


class _FakeState:
    def pending_count(self) -> int:
        return 0

    def visible_op_ids(self, key: str) -> set[str]:
        return {"bad-op"} if key == "k" else set()

    def replica_views(self, key: str):
        return {"n0": ("v", "good"), "n1": ("v", "good"), "n2": None}

    def final_op_id(self, key: str) -> str | None:
        return "good"

    def final_value(self, key: str):
        return "v"


def test_residual_and_lww_checkers() -> None:
    h = History()
    h.invoke("c", OpKind.PUT, key="k", value="v", op_id="good")
    h.ok("c", OpKind.PUT, key="k", value="v", op_id="good")
    h.invoke("c2", OpKind.PUT, key="k", value="x", op_id="bad-op")
    h.fail("c2", OpKind.PUT, key="k", value="x", op_id="bad-op")
    st = _FakeState()
    # residual: bad-op still "visible" in fake state
    rr = ResidualFreeChecker().check(h, state=st)
    assert rr.ok is False
    # fix state
    st2 = _FakeState()
    st2.visible_op_ids = lambda key: {"good"}  # type: ignore[method-assign]
    assert ResidualFreeChecker().check(h, state=st2).ok is True
    assert LWWRegisterChecker(key="k").check(h, state=st2).ok is True
    assert default_strong_checkers(key="k").check(h, state=st2).ok is True


def test_nemesis_step_and_stop_heals() -> None:
    target = NullNemesisTarget(nodes=["n0", "n1", "n2"])
    h = History()
    nem = Nemesis(
        target=target, history=h, seed=1, actions=[NemesisAction.PARTITION_ONE]
    )
    act = nem.step_once()
    assert act is NemesisAction.PARTITION_ONE
    assert any("partition" in x for x in target.log)
    assert nem.action_count == 1
    assert any(e.kind is OpKind.FAULT for e in h.snapshot())

    async def _stop() -> None:
        await nem.stop()

    asyncio.run(_stop())
    assert any(x == "heal" for x in target.log)


@pytest.mark.asyncio
async def test_scenario_runs_clients_and_checks() -> None:
    seen: list[int] = []

    async def client(history: History, idx: int) -> None:
        history.invoke(f"c{idx}", OpKind.BARRIER)
        history.ok(f"c{idx}", OpKind.BARRIER)
        seen.append(idx)

    sc = Scenario(
        name="unit-sc",
        clients=[client, client, client],
        checker=NoOpenInvokeChecker(),
        strict=True,
    )
    result = await sc.run()
    assert result.ok
    assert len(seen) == 3
    assert result.history_len == 6


@pytest.mark.asyncio
async def test_scenario_body_and_failure_raises() -> None:
    async def body(history: History, sut: object) -> None:
        history.invoke("c", OpKind.PUT, key="k", value=1)
        # open invoke → fail

    sc = Scenario(
        name="fail-sc",
        body=body,
        checker=NoOpenInvokeChecker(),
        strict=True,
    )
    with pytest.raises(AssertionError):
        await sc.run()


def test_check_result_raise() -> None:
    r = CheckResult(
        name="x",
        ok=False,
        violations=[CheckViolation(checker="x", message="boom")],
    )
    with pytest.raises(AssertionError, match="boom"):
        r.raise_if_failed()


def test_history_by_key_and_unmatched() -> None:
    h = History()
    h.invoke("c0", OpKind.PUT, key="a", value=1)
    h.ok("c0", OpKind.PUT, key="a", value=1, op_id="a1")
    h.invoke("c1", OpKind.PUT, key="b", value=2)
    # unmatched invoke on b
    assert [e.key for e in h.by_key("a")] == ["a", "a"]
    pairs = h.pairs()
    open_pairs = [p for p in pairs if p[1] is None]
    assert len(open_pairs) == 1
    assert open_pairs[0][0].key == "b"


def test_replica_agreement_majority_null_ok() -> None:
    from mpreg.testing.distlab.checker import ReplicaAgreementChecker

    class St:
        def replica_views(self, key: str):
            return {"n0": ("v", "op"), "n1": ("v", "op"), "n2": None}

    h = History()
    h.invoke("c", OpKind.PUT, key="k", value="v", op_id="op")
    h.ok("c", OpKind.PUT, key="k", value="v", op_id="op")
    r = ReplicaAgreementChecker().check(h, state=St())
    assert r.ok is True


def test_replica_agreement_conflict_fail() -> None:
    from mpreg.testing.distlab.checker import ReplicaAgreementChecker

    class St:
        def replica_views(self, key: str):
            return {"n0": ("v1", "op1"), "n1": ("v2", "op2"), "n2": ("v1", "op1")}

    h = History()
    h.invoke("c", OpKind.PUT, key="k", value="v1", op_id="op1")
    h.ok("c", OpKind.PUT, key="k", value="v1", op_id="op1")
    r = ReplicaAgreementChecker().check(h, state=St())
    assert r.ok is False


def test_gset_checker_eligible_false_ignored() -> None:
    from mpreg.testing.distlab.checker import GSetConvergenceChecker

    class St:
        def gset_ids_by_node(self):
            return {"a0": {"e1"}, "a1": {"e1"}}

    h = History()
    h.invoke("c", OpKind.AUDIT_PUBLISH, meta={"eligible": False})
    h.ok("c", OpKind.AUDIT_PUBLISH, op_id="secret", meta={"eligible": False})
    h.invoke("c2", OpKind.AUDIT_PUBLISH, meta={"eligible": True})
    h.ok("c2", OpKind.AUDIT_PUBLISH, op_id="e1", meta={"eligible": True})
    r = GSetConvergenceChecker(min_ids=1).check(h, state=St())
    assert r.ok is True


def test_callable_checker_plugin() -> None:
    from mpreg.testing.distlab import CallableChecker

    def fn(history: History, state=None) -> CheckResult:
        return CheckResult(name="plugin", ok=True, stats={"n": len(history)})

    h = History()
    h.invoke("c", OpKind.BARRIER)
    h.ok("c", OpKind.BARRIER)
    r = CallableChecker(name="plugin", fn=fn).check(h)
    assert r.ok and r.stats["n"] == 2


def test_nemesis_actions_matrix() -> None:
    target = NullNemesisTarget(nodes=["n0", "n1", "n2"])
    h = History()
    for act in (
        NemesisAction.PARTITION_ONE,
        NemesisAction.PARTITION_MAJORITY,
        NemesisAction.HEAL,
        NemesisAction.DROP_RATE,
        NemesisAction.CLEAR_RATES,
        NemesisAction.DELAY,
        NemesisAction.CRASH_ONE,
        NemesisAction.RECOVER_ALL,
    ):
        nem = Nemesis(target=target, history=h, seed=3, actions=[act])
        assert nem.step_once() is act


@pytest.mark.asyncio
async def test_scenario_non_strict_returns_ok_false() -> None:
    async def body(history: History, sut: object) -> None:
        history.invoke("c", OpKind.PUT, key="k", value=1)

    sc = Scenario(
        name="ns",
        body=body,
        checker=NoOpenInvokeChecker(),
        strict=False,
    )
    r = await sc.run()
    assert r.ok is False


@pytest.mark.asyncio
async def test_scenario_suite_stop_on_fail() -> None:
    from mpreg.testing.distlab import ScenarioSuite

    async def bad(history: History, sut: object) -> None:
        history.invoke("c", OpKind.PUT, key="k", value=1)

    async def good(history: History, sut: object) -> None:
        history.invoke("c", OpKind.BARRIER)
        history.ok("c", OpKind.BARRIER)

    suite = ScenarioSuite(name="s")
    suite.add(
        Scenario(name="bad", body=bad, checker=NoOpenInvokeChecker(), strict=True)
    )
    suite.add(
        Scenario(name="good", body=good, checker=NoOpenInvokeChecker(), strict=True)
    )
    with pytest.raises(AssertionError):
        await suite.run_all(stop_on_fail=True)


@pytest.mark.asyncio
async def test_history_concurrent_append() -> None:
    h = History()

    async def worker(n: int) -> None:
        for i in range(20):
            h.invoke(f"p{n}", OpKind.BARRIER)
            h.ok(f"p{n}", OpKind.BARRIER)

    await asyncio.gather(*[worker(i) for i in range(4)])
    assert len(h) == 160


def test_history_error_code_and_outcome_counts() -> None:
    from mpreg.testing.distlab.history import History
    from mpreg.testing.distlab.models import OpKind

    h = History()
    h.invoke("c0", OpKind.PUT, key="k", value=1)
    h.fail("c0", OpKind.PUT, key="k", value=1, error_code=1015, error_message="q")
    h.invoke("c1", OpKind.PUT, key="k", value=2)
    h.ok("c1", OpKind.PUT, key="k", value=2)
    codes = h.error_code_counts()
    assert codes.get("1015") == 1
    outcomes = h.outcome_counts()
    assert outcomes.get("invoke") == 2
    assert outcomes.get("fail") == 1
    assert outcomes.get("ok") == 1
