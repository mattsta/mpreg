"""T87 residual closeout: doctor abort_fail_peer_count detail + JSON."""

from __future__ import annotations

from pathlib import Path

from mpreg.cli.main import (
    _strong_abort_fail_peer_count,
    evaluate_strong_doctor_payload,
)

def test_t87_doctor_detail_and_helper() -> None:
    body = {
        "health": "ok",
        "capabilities": {
            "get_quorum": False,
            "delete_quorum": False,
            "cft_only": True,
            "abort_best_effort": True,
            "pending_ttl_clears_residual_l1": False,
            "retry_abort_ops_driven": True,
        },
        "counters": {},
        "last_abort_fail_peers": ["p1"],
        "last_abort_fail_op_id": "x",
    }
    assert _strong_abort_fail_peer_count(body) == 1
    ok, detail = evaluate_strong_doctor_payload({"strong": body})
    assert ok and "abort_fail_peer_count=1" in detail

def test_t87_main_wires_json_row() -> None:
    text = (
        Path(__file__).resolve().parents[2] / "mpreg" / "cli" / "main.py"
    ).read_text(encoding="utf-8")
    assert 'row["abort_fail_peer_count"]' in text
    assert "_strong_abort_fail_peer_count" in text

def test_t87_phase_75_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    assert "Phase 75" in path.read_text(encoding="utf-8")

def test_t87_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T87_DOCTOR_PEER_COUNT_PLAN.md"
    ).is_file()
    assert "T87" in (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
