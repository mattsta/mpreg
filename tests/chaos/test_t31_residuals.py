"""T31 residual closeout: CFT ops surface polish for T29/T30."""

from __future__ import annotations

from mpreg.cli.main import evaluate_strong_doctor_payload
from mpreg.server_pkg.openapi_surface import build_monitoring_openapi
from mpreg.testing.distlab.builtins import ensure_builtins
from mpreg.testing.distlab.registry import resolve_preset

def test_t31_doctor_detail_includes_abort_fail() -> None:
    ok, detail = evaluate_strong_doctor_payload(
        {
            "strong": {
                "health": "ok",
                "capabilities": {
                    "get_quorum": False,
                    "cft_only": True,
                    "abort_best_effort": True,
                    "pending_ttl_clears_residual_l1": False,
                },
                "counters": {"aborts_peer_fail": 4},
            }
        }
    )
    assert ok is True
    assert "abort_fail=4" in detail
    assert "cft=True" in detail

def test_t31_openapi_has_ttl_and_counts() -> None:
    doc = build_monitoring_openapi()
    schemas = (doc.get("components") or {}).get("schemas") or {}
    strong = schemas["StrongMetricsResponse"]
    body = (strong.get("properties") or {}).get("strong") or {}
    props = body.get("properties") or {}
    assert "visible_count" in props
    assert "backups_count" in props
    caps = (props.get("capabilities") or {}).get("properties") or {}
    assert caps.get("pending_ttl_clears_residual_l1", {}).get("enum") == [False]

def test_t31_ci_core_includes_all_cft_scenarios() -> None:
    ensure_builtins()
    ci = set(resolve_preset("ci-core"))
    for name in (
        "strong.cft_partial_commit_lost_abort",
        "strong.cft_residual_healed_by_lww",
        "strong.cft_residual_survives_pending_purge",
        "strong.cft_orphan_backup_gc",
    ):
        assert name in ci, name

def test_t31_prometheus_alerts_include_ttl_honesty() -> None:
    from pathlib import Path

    path = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "ops"
        / "prometheus_alerts.yml"
    )
    text = path.read_text(encoding="utf-8")
    assert "MPREGStrongCapPendingTtlClearsResidualClaimed" in text
    assert "mpreg_strong_cap_pending_ttl_clears_residual_l1" in text
