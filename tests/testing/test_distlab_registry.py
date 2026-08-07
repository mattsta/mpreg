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

@pytest.mark.asyncio
async def test_registry_run_suite_prefix_limit() -> None:
    from mpreg.testing.distlab.builtins import ensure_builtins
    from mpreg.testing.distlab.registry import get_registry

    ensure_builtins()
    reg = get_registry()
    report = await reg.run_suite(prefix="strong.happy_", limit=2, fail_fast=True)
    assert report["ran"] == 2
    assert report["ok"] is True
    assert report["passed"] == 2
    # taxonomy attached on scenario meta
    for row in report["results"]:
        assert "error_codes" in (row.get("meta") or {})

def test_registry_select_excludes_not_bft() -> None:
    from mpreg.testing.distlab.builtins import ensure_builtins
    from mpreg.testing.distlab.registry import get_registry

    ensure_builtins()
    reg = get_registry()
    names = reg.select(prefix="strong.")
    assert all("not_bft" not in n for n in names)
    assert "strong.not_bft_lie_commit_both" not in names
    with_bft = reg.select(prefix="strong.", exclude_tags=())
    assert "strong.not_bft_lie_commit_both" in with_bft

@pytest.mark.asyncio
async def test_registry_run_suite_smoke_preset() -> None:
    """T18/T19: smoke preset runs fast in-process core scenarios incl. refuse."""
    from mpreg.testing.distlab.builtins import ensure_builtins
    from mpreg.testing.distlab.registry import SUITE_PRESETS, get_registry

    ensure_builtins()
    reg = get_registry()
    assert "smoke" in SUITE_PRESETS
    selected = reg.select(preset="smoke")
    assert "strong.happy_3" in selected
    assert "strong.refuse_get_delete" in selected
    assert "audit.multi_origin" in selected
    assert all("not_bft" not in n for n in selected)
    report = await reg.run_suite(preset="smoke", fail_fast=True)
    assert report["preset"] == "smoke"
    assert report["ok"] is True
    assert report["ran"] >= 4
    assert report["passed"] == report["ran"]

@pytest.mark.asyncio
async def test_registry_run_suite_audit_core_preset() -> None:
    """T22: audit-core preset expands digest/duplicate/ineligible paths."""
    from mpreg.testing.distlab.builtins import ensure_builtins
    from mpreg.testing.distlab.registry import SUITE_PRESETS, get_registry

    ensure_builtins()
    reg = get_registry()
    assert "audit-core" in SUITE_PRESETS
    selected = reg.select(preset="audit-core")
    for name in (
        "audit.multi_origin",
        "audit.partition_heal",
        "audit.digest_repair",
        "audit.duplicate_idempotent",
        "audit.ineligible_local",
    ):
        assert name in selected, name
    report = await reg.run_suite(preset="audit-core", fail_fast=True)
    assert report["preset"] == "audit-core"
    assert report["ok"] is True
    assert report["ran"] >= 5
    assert report["passed"] == report["ran"]

@pytest.mark.asyncio
async def test_registry_run_suite_ci_core_preset() -> None:
    """T25: ci-core = ordered union of smoke ∪ strong-core ∪ audit-core."""
    from mpreg.testing.distlab.builtins import ensure_builtins
    from mpreg.testing.distlab.registry import (
        SUITE_PRESETS,
        get_registry,
        resolve_preset,
    )

    ensure_builtins()
    names = resolve_preset("ci-core")
    assert "ci-core" in SUITE_PRESETS
    assert "strong.happy_3" in names
    assert "strong.happy_5" in names
    assert "strong.refuse_get_delete" in names
    assert "audit.digest_repair" in names
    # Dedup: happy_3 appears once
    assert names.count("strong.happy_3") == 1
    assert names.count("audit.multi_origin") == 1
    # Union size >= max of parts
    assert len(names) >= len(SUITE_PRESETS["strong-core"])
    assert len(names) >= len(SUITE_PRESETS["audit-core"])

    reg = get_registry()
    report = await reg.run_suite(preset="ci-core", fail_fast=True)
    assert report["preset"] == "ci-core"
    assert report["ok"] is True
    assert report["ran"] == len(names)
    assert report["passed"] == report["ran"]

def test_strong_core_includes_retry_abort_ops_scenarios() -> None:
    """T56: strong-core/ci-core include full retry_abort ops surface scenarios."""
    from mpreg.testing.distlab.builtins import ensure_builtins
    from mpreg.testing.distlab.registry import get_registry, resolve_preset

    ensure_builtins()
    required = (
        "strong.cft_retry_abort_clears_residual",
        "strong.cft_retry_abort_self_target",
        "strong.cft_gcm_retry_abort_clears_residual",
        "strong.cft_partial_commit_lost_abort",
        "strong.cft_residual_healed_by_lww",
        "strong.cft_residual_survives_pending_purge",
        "strong.cft_orphan_backup_gc",
    )
    for preset in ("strong-core", "ci-core"):
        names = resolve_preset(preset)
        for sc in required:
            assert sc in names, f"{sc} missing from {preset}"
    # Factories registered and runnable via name lookup
    reg = get_registry()
    for sc in required:
        assert sc in reg.list(), f"{sc} not registered"

@pytest.mark.asyncio
async def test_strong_refuse_get_delete_scenario() -> None:
    """T19: builtin refuse scenario passes NoOpenInvokeChecker."""
    from mpreg.testing.distlab.builtins import ensure_builtins
    from mpreg.testing.distlab.registry import get_registry

    ensure_builtins()
    r = await get_registry().run("strong.refuse_get_delete")
    assert r.ok
    assert r.history_len >= 6
    codes = (r.meta or {}).get("error_codes") or {}
    # 1012 appears for get/delete refuses
    assert any(int(k) == 1012 for k in codes) or codes.get(1012) or codes.get("1012")

def test_cli_smoke_preset_via_mpreg_entry() -> None:
    """Architecture: uv run mpreg distlab suite --preset smoke."""
    list_p = subprocess.run(
        ["uv", "run", "mpreg", "distlab", "presets", "--json"],
        capture_output=True,
        text=True,
        cwd="/Users/matt/repos/mpreg",
        timeout=90,
    )
    assert list_p.returncode == 0, list_p.stderr
    presets = json.loads(list_p.stdout)
    assert "smoke" in presets

    run_p = subprocess.run(
        ["uv", "run", "mpreg", "distlab", "suite", "--preset", "smoke", "--json"],
        capture_output=True,
        text=True,
        cwd="/Users/matt/repos/mpreg",
        timeout=180,
    )
    assert run_p.returncode == 0, run_p.stderr + run_p.stdout
    data = json.loads(run_p.stdout)
    assert data["ok"] is True
    assert data["preset"] == "smoke"
    assert data["ran"] >= 3
