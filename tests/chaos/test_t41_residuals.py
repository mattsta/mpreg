"""T41 residual closeout: retry_abort_ops_driven honesty cap."""

from __future__ import annotations

from pathlib import Path

from mpreg.cli.main import evaluate_strong_doctor_payload
from mpreg.server_pkg.monitoring_metrics import build_strong_metrics
from mpreg.server_pkg.openapi_surface import _strong_metrics_schema


def test_t41_doctor_fails_closed_if_retry_not_ops_driven() -> None:
    ok, detail = evaluate_strong_doctor_payload(
        {
            "strong": {
                "health": "ok",
                "coordinator_bound": True,
                "capabilities": {
                    "get_quorum": False,
                    "delete_quorum": False,
                    "cft_only": True,
                    "abort_best_effort": True,
                    "pending_ttl_clears_residual_l1": False,
                    "retry_abort_ops_driven": False,
                },
                "counters": {},
            }
        }
    )
    assert ok is False
    assert "retry_abort_ops_driven" in detail


def test_t41_doctor_ok_shows_retry_ops() -> None:
    ok, detail = evaluate_strong_doctor_payload(
        {
            "strong": {
                "health": "ok",
                "coordinator_bound": True,
                "capabilities": {
                    "put_majority_commit": True,
                    "get_quorum": False,
                    "delete_quorum": False,
                    "local_ryw_after_put": True,
                    "cft_only": True,
                    "abort_best_effort": True,
                    "pending_ttl_clears_residual_l1": False,
                    "retry_abort_ops_driven": True,
                },
                "counters": {"puts_ok": 1},
            }
        }
    )
    assert ok is True
    assert "retry_ops=" in detail


def test_t41_openapi_retry_ops_driven_cap() -> None:
    props = _strong_metrics_schema()["properties"]["strong"]["properties"]
    caps = props["capabilities"]["properties"]
    assert "retry_abort_ops_driven" in caps
    assert caps["retry_abort_ops_driven"]["enum"] == [True]


def test_t41_prom_cap_and_alert() -> None:
    mon = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "fabric"
        / "monitoring_endpoints.py"
    ).read_text(encoding="utf-8")
    assert "mpreg_strong_cap_retry_abort_ops_driven" in mon
    yml = (
        Path(__file__).resolve().parents[2] / "mpreg" / "ops" / "prometheus_alerts.yml"
    ).read_text(encoding="utf-8")
    assert "MPREGStrongCapRetryAbortOpsDrivenMissing" in yml
    assert "mpreg_strong_cap_retry_abort_ops_driven == 0" in yml
    slo = (
        Path(__file__).resolve().parents[2]
        / "mpreg"
        / "core"
        / "observability"
        / "slo.py"
    ).read_text(encoding="utf-8")
    assert "MPREGStrongCapRetryAbortOpsDrivenMissing" in slo


def test_t41_build_metrics_default_cap() -> None:
    class _Srv:
        settings = type("S", (), {"cache_strong_enabled": False})()
        _cache_manager = None
        _strong_local_backend = None
        _strong_pending_purge_task = None

    payload = build_strong_metrics(_Srv())
    caps = payload.get("capabilities") or {}
    assert caps.get("retry_abort_ops_driven") is True
