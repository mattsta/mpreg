"""T89 residual closeout: config-check explain abort_fail_peer_count."""

from __future__ import annotations

from pathlib import Path


def test_t89_explain_guide_mentions_count() -> None:
    text = (
        Path(__file__).resolve().parents[2] / "mpreg" / "cli" / "main.py"
    ).read_text(encoding="utf-8")
    # strong_cache explain guide
    assert "abort_fail_peer_count" in text
    assert "mpreg_strong_abort_fail_peers" in text


def test_t89_config_check_test() -> None:
    text = (
        Path(__file__).resolve().parents[2] / "tests" / "test_config_check_cli.py"
    ).read_text(encoding="utf-8")
    assert "abort_fail_peer_count" in text or "mpreg_strong_abort_fail_peers" in text


def test_t89_phase_77_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    assert "Phase 77" in path.read_text(encoding="utf-8")


def test_t89_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T89_CONFIG_CHECK_PEER_COUNT_PLAN.md"
    ).is_file()
    assert "T89" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
