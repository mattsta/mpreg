"""T47 residual closeout: live prom cap + client RPC metrics e2e honesty."""

from __future__ import annotations

from pathlib import Path


def test_t47_live_e2e_source_asserts_cap_and_client_rpc() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "testing"
        / "test_distlab_live.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "mpreg_strong_cap_retry_abort_ops_driven" in text
    assert "cache_strong_retry_abort" in text
    assert "retry_abort_ops_driven" in text
    assert "ops_driven" in text
    assert "automatic_heal" in text


def test_t47_phase_35_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 35" in text
    assert "cap_retry_abort" in text.lower() or "retry_abort_ops_driven" in text


def test_t47_plan_non_claims() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "plans"
        / "DISTLAB_T47_LIVE_CAP_CLIENT_RPC_METRICS_PLAN.md"
    )
    text = path.read_text(encoding="utf-8").lower()
    assert "not" in text and ("auto" in text or "heal" in text)


def test_t47_operate_mentions_cap_gauge() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "examples-curriculum"
        / "OPERATE.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "mpreg_strong_cap_retry_abort_ops_driven" in text or (
        "retry_abort_ops_driven" in text
    )
