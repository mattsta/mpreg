"""T100 residual closeout: Doctor JSON abort_fail_peer_count as int."""

from __future__ import annotations

from pathlib import Path


def test_t100_phase_88_honesty() -> None:
    assert "Phase 88" in (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")


def test_t100_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T100_DOCTOR_JSON_PEER_COUNT_INT_PLAN.md"
    ).is_file()
    assert "T100" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )


def test_t100_doctor_json_int_source() -> None:
    root = Path(__file__).resolve().parents[2]
    main = (root / "mpreg" / "cli" / "main.py").read_text(encoding="utf-8")
    assert "strong_doctor_json_residual_fields" in main
    assert "abort_fail_peer_count" in main
    # Must not stringify peer count for JSON rows
    assert 'row["abort_fail_peer_count"] = str(' not in main
