"""Shared-audit platform validation via first-party DistLab scenarios."""

from __future__ import annotations

import pytest

from mpreg.testing.distlab import (
    AuditSUT,
    FaultInjectorNemesisTarget,
    History,
    Nemesis,
    NemesisAction,
    Scenario,
    default_audit_checkers,
)

@pytest.mark.asyncio
async def test_distlab_audit_multi_origin_converge() -> None:
    sut = AuditSUT.create(3)

    async def body(history: History, s: AuditSUT) -> None:
        for i, origin in enumerate(s.node_ids):
            await s.publish(
                history,
                process=f"c{i}",
                origin=origin,
                event=f"e{i}",
            )
        await s.flush_all()
        await s.reconcile_all()

    await Scenario(
        name="audit-multi",
        setup=lambda: sut,
        body=body,
        checker=default_audit_checkers(min_ids=3),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_audit_partition_heal_converge() -> None:
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

    async def body(history: History, s: AuditSUT) -> None:
        target.apply_partition_groups([{"a0"}, {"a1", "a2"}])
        for i in range(4):
            await s.publish(
                history, process="c0", origin="a0", event=f"p{i}"
            )
        # Isolated — a1/a2 should not have a0's events yet
        assert s.stores["a1"].size() == 0
        target.heal_network()
        await s.flush_all()
        await s.reconcile_all()
        await s.reconcile_all()

    await Scenario(
        name="audit-part-heal",
        setup=lambda: sut,
        body=body,
        checker=default_audit_checkers(min_ids=4),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_audit_drop_delta_digest_repair() -> None:
    sut = AuditSUT.create(2)
    sut.transport.drop_types.add("mgmt_audit_delta")

    async def body(history: History, s: AuditSUT) -> None:
        await s.publish(history, process="c0", origin="a0", event="repair")
        assert s.stores["a1"].size() == 0
        s.transport.drop_types.clear()
        await s.reconcile_all()

    await Scenario(
        name="audit-digest-repair",
        setup=lambda: sut,
        body=body,
        checker=default_audit_checkers(min_ids=1),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_audit_ineligible_local_only() -> None:
    sut = AuditSUT.create(2)

    async def body(history: History, s: AuditSUT) -> None:
        await s.publish(
            history,
            process="c0",
            origin="a0",
            event="secret",
            eligible=False,
        )
        await s.flush_all()
        await s.reconcile_all()
        # Eligible set empty on both for gossip path — checker min_ids=0
        assert s.stores["a1"].size() == 0
        assert s.stores["a0"].size() == 1

    await Scenario(
        name="audit-ineligible",
        setup=lambda: sut,
        body=body,
        checker=default_audit_checkers(min_ids=0),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_audit_duplicate_idempotent() -> None:
    sut = AuditSUT.create(2)
    sut.transport.duplicate = True

    async def body(history: History, s: AuditSUT) -> None:
        await s.publish(history, process="c0", origin="a0", event="dup")
        await s.flush_all()
        assert s.stores["a1"].size() == 1

    await Scenario(
        name="audit-dup",
        setup=lambda: sut,
        body=body,
        checker=default_audit_checkers(min_ids=1),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_audit_burst_converge() -> None:
    sut = AuditSUT.create(3)

    async def body(history: History, s: AuditSUT) -> None:
        for i in range(30):
            origin = s.node_ids[i % 3]
            await s.publish(
                history, process=f"c{i}", origin=origin, event=f"b{i}"
            )
        await s.flush_all()
        await s.reconcile_all()

    await Scenario(
        name="audit-burst",
        setup=lambda: sut,
        body=body,
        checker=default_audit_checkers(min_ids=30),
        strict=True,
    ).run()

@pytest.mark.asyncio
async def test_distlab_audit_nemesis_then_converge() -> None:
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

    async def body(history: History, s: AuditSUT) -> None:
        for i in range(12):
            await s.publish(
                history,
                process=f"c{i}",
                origin=s.node_ids[i % 3],
                event=f"n{i}",
            )
            await s.flush_all()
        # After scenario stops nemesis (heal), force anti-entropy
        await s.flush_all()
        await s.reconcile_all()
        await s.reconcile_all()

    await Scenario(
        name="audit-nemesis",
        setup=lambda: sut,
        body=body,
        nemesis=nem,
        checker=default_audit_checkers(min_ids=12),
        strict=True,
    ).run()
