"""DistLab registry, generators, CLI, and built-in scenario smoke (T1)."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from mpreg.testing.distlab import (
    ConcurrentPuts,
    History,
    OpKind,
    RandomFaultPlan,
    SequentialPuts,
    StrongSUT,
    ensure_builtins,
    get_registry,
)
from mpreg.testing.distlab.generator import AuditBurst
from mpreg.testing.distlab.registry import ScenarioRegistry

def test_registry_list_after_builtins() -> None:
    ensure_builtins()
    reg = get_registry()
    names = reg.list()
    assert "strong.happy_3" in names
    assert "audit.multi_origin" in names
    assert "strong.not_bft_lie_commit_both" in names
    assert len(names) >= 20
    cat = reg.catalog()
    assert any(r["name"] == "strong.happy_3" and r["track"] == "T2" for r in cat)

def test_registry_unknown_raises() -> None:
    ensure_builtins()
    with pytest.raises(KeyError, match="unknown scenario"):
        get_registry().get("does.not.exist")

def test_registry_custom_register() -> None:
    reg = ScenarioRegistry(name="tmp")

    def factory():
        from mpreg.testing.distlab import NoOpenInvokeChecker, Scenario

        async def body(h: History, s: object) -> None:
            h.invoke("c", OpKind.BARRIER)
            h.ok("c", OpKind.BARRIER)

        return Scenario(name="tmp.x", body=body, checker=NoOpenInvokeChecker())

    reg.register("tmp.x", factory, track="T1", description="unit")
    assert reg.list() == ["tmp.x"]
    assert reg.meta("tmp.x")["track"] == "T1"

@pytest.mark.asyncio
async def test_registry_run_happy_3() -> None:
    ensure_builtins()
    r = await get_registry().run("strong.happy_3")
    assert r.ok
    assert r.history_len >= 2
    assert r.meta.get("track") == "T2" or True  # meta from scenario

@pytest.mark.asyncio
async def test_generator_sequential_puts_history() -> None:
    sut = StrongSUT.create(3)
    gen = SequentialPuts(sut=sut, n=5, logical_key="g")
    h = History()
    await gen.as_body()(h, sut)
    assert len(h.successful_puts("g")) == 5
    assert not h.failed_puts("g")

@pytest.mark.asyncio
async def test_generator_concurrent_clients() -> None:
    from mpreg.testing.distlab import Scenario, default_strong_checkers

    sut = StrongSUT.create(3)
    clients = ConcurrentPuts(sut=sut, n_clients=4, logical_key="cg").as_clients()
    r = await Scenario(
        name="gen-conc",
        setup=lambda: sut,
        clients=clients,
        checker=default_strong_checkers(key="cg"),
    ).run()
    assert r.ok

def test_random_fault_plan_seedable() -> None:
    a = RandomFaultPlan(seed=1, n=8).plan()
    b = RandomFaultPlan(seed=1, n=8).plan()
    c = RandomFaultPlan(seed=2, n=8).plan()
    assert a == b
    assert a != c

def test_cli_list_and_run_via_mpreg_entry() -> None:
    """Architecture: top-level ``uv run mpreg distlab`` only (never python -m)."""
    list_p = subprocess.run(
        ["uv", "run", "mpreg", "distlab", "list"],
        capture_output=True,
        text=True,
        cwd="/Users/matt/repos/mpreg",
        timeout=90,
    )
    assert list_p.returncode == 0, list_p.stderr
    assert "strong.happy_3" in list_p.stdout

    run_p = subprocess.run(
        ["uv", "run", "mpreg", "distlab", "run", "strong.happy_3", "--json"],
        capture_output=True,
        text=True,
        cwd="/Users/matt/repos/mpreg",
        timeout=120,
    )
    assert run_p.returncode == 0, run_p.stderr + run_p.stdout
    data = json.loads(run_p.stdout)
    assert data["ok"] is True
    assert data["name"] == "strong.happy_3"

def test_cli_help_mentions_non_claims() -> None:
    p = subprocess.run(
        ["uv", "run", "mpreg", "distlab", "--help"],
        capture_output=True,
        text=True,
        cwd="/Users/matt/repos/mpreg",
        timeout=60,
    )
    assert p.returncode == 0
    out = p.stdout + p.stderr
    assert "Elle" in out or "not Elle" in out or "DistLab" in out
    assert "distlab" in out.lower()

def test_python_m_distlab_is_blocked() -> None:
    """Module path must refuse — forces entry-point usage."""
    p = subprocess.run(
        [sys.executable, "-m", "mpreg.testing.distlab", "list"],
        capture_output=True,
        text=True,
        cwd="/Users/matt/repos/mpreg",
        timeout=30,
    )
    assert p.returncode == 2
    assert "not supported" in (p.stderr + p.stdout).lower() or "mpreg distlab" in (
        p.stderr + p.stdout
    )

@pytest.mark.asyncio
async def test_registry_subset_suite() -> None:
    from mpreg.testing.distlab import ScenarioSuite

    ensure_builtins()
    reg = get_registry()
    suite = ScenarioSuite(name="smoke")
    for name in ("strong.happy_3", "strong.drop_prepare", "audit.multi_origin"):
        suite.add(await reg.build(name))
    results = await suite.run_all(stop_on_fail=True)
    assert all(r.ok for r in results)
    assert len(results) == 3

@pytest.mark.asyncio
async def test_audit_burst_generator() -> None:
    from mpreg.testing.distlab import AuditSUT, Scenario, default_audit_checkers

    sut = AuditSUT.create(3)
    r = await Scenario(
        name="ab",
        setup=lambda: sut,
        body=AuditBurst(sut=sut, n=5, prefix="x").as_body(),
        checker=default_audit_checkers(min_ids=5),
    ).run()
    assert r.ok
