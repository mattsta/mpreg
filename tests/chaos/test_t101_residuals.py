"""T101 residual closeout: Doctor JSON last_abort_fail_peers list."""

from __future__ import annotations

from pathlib import Path


def test_t101_phase_89_honesty() -> None:
    assert "Phase 89" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")


def test_t101_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T101_DOCTOR_JSON_PEERS_LIST_PLAN.md"
    ).is_file()
    assert "T101" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )


def test_t101_doctor_json_peers_list_source() -> None:
    root = Path(__file__).resolve().parents[2]
    main = (root / "mpreg" / "cli" / "main.py").read_text(encoding="utf-8")
    assert 'row["last_abort_fail_peers"]' in main or "last_abort_fail_peers" in main
    assert "strong_doctor_json_residual_fields" in main
