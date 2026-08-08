"""T25 residual closeout: ci-core preset composition + prom cap name contract."""

from __future__ import annotations

from mpreg.testing.distlab.registry import SUITE_PRESETS, resolve_preset


def test_t25_ci_core_preset_is_deduped_union() -> None:
    smoke = set(SUITE_PRESETS["smoke"])
    strong = set(SUITE_PRESETS["strong-core"])
    audit = set(SUITE_PRESETS["audit-core"])
    raft = set(SUITE_PRESETS["raft-core"])
    names = resolve_preset("ci-core")
    # ci-core = ordered union of smoke ∪ strong-core ∪ audit-core ∪ raft-core
    assert set(names) == smoke | strong | audit | raft
    assert len(names) == len(set(names))
    # Order: smoke first, then strong-only, then audit-only, then raft-only
    assert names[0] == SUITE_PRESETS["smoke"][0]
    # Refuse + digest + raft elect present for honesty coverage
    assert "strong.refuse_get_delete" in names
    assert "audit.digest_repair" in names
    assert "raft.elect_3" in names
    assert "raft.partition_heal" in names


def test_t25_unknown_preset_empty() -> None:
    assert resolve_preset("no-such-preset") == []
    assert resolve_preset("") == []


def test_t25_list_presets_includes_ci_core() -> None:
    assert "ci-core" in SUITE_PRESETS
    assert "smoke" in SUITE_PRESETS


def test_t25_list_presets_cli_expands_ci_core() -> None:
    """``mpreg distlab presets --json`` expands composed ci-core names."""
    from click.testing import CliRunner

    from mpreg.cli.main import cli

    r = CliRunner().invoke(cli, ["distlab", "presets", "--json"])
    assert r.exit_code == 0
    import json

    data = json.loads(r.output)
    assert "ci-core" in data
    assert "strong.refuse_get_delete" in data["ci-core"]
    assert "audit.digest_repair" in data["ci-core"]
    assert len(data["ci-core"]) >= 10
