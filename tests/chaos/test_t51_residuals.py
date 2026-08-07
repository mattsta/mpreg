"""T51 residual closeout: doctor abort_fail_op_id + ops remediation hint."""

from __future__ import annotations

from pathlib import Path

from mpreg.cli.main import (
    evaluate_strong_doctor_payload,
    strong_residual_ops_hint,
)

def _ok_body(**extra: object) -> dict:
    body: dict = {
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
        "counters": {"puts_ok": 1, "aborts_peer_fail": 1},
        "visible_count": 0,
        "backups_count": 0,
        "backups_pruned_total": 0,
    }
    body.update(extra)
    return body

def test_t51_doctor_shows_abort_fail_op_id() -> None:
    ok, detail = evaluate_strong_doctor_payload(
        {
            "strong": _ok_body(
                last_abort_fail_peers=["n1", "n2"],
                last_abort_fail_op_id="oid-abc",
            )
        }
    )
    assert ok is True
    assert "abort_fail_op_id=oid-abc" in detail
    assert "abort_fail_peers=['n1', 'n2']" in detail or "n1" in detail

def test_t51_doctor_hint_when_residual_candidates() -> None:
    ok, detail = evaluate_strong_doctor_payload(
        {
            "strong": _ok_body(
                last_abort_fail_peers=["n1"],
                last_abort_fail_op_id="oid-xyz",
            )
        }
    )
    assert ok is True
    assert "cache-strong-retry-abort" in detail
    assert "--op-id oid-xyz" in detail
    assert "--peer n1" in detail
    assert "not auto-heal" in detail
    # Still ok=True — residual candidates are CFT honesty, not doctor fail
    assert "hint:" in detail

def test_t51_doctor_no_hint_when_no_fail_peers() -> None:
    ok, detail = evaluate_strong_doctor_payload(
        {"strong": _ok_body(last_abort_fail_peers=[], last_abort_fail_op_id="")}
    )
    assert ok is True
    assert "hint:" not in detail
    assert "cache-strong-retry-abort" not in detail
    assert "abort_fail_op_id=-" in detail or "abort_fail_op_id=" in detail

def test_t51_hint_from_nested_coordinator() -> None:
    body = _ok_body(
        coordinator={
            "last_abort_fail_peers": ["n3"],
            "last_abort_fail_op_id": "nested-oid",
        }
    )
    # Top-level empty; nested must still resolve
    body.pop("last_abort_fail_peers", None)
    body.pop("last_abort_fail_op_id", None)
    hint = strong_residual_ops_hint(body)
    assert "nested-oid" in hint
    assert "--peer n3" in hint
    ok, detail = evaluate_strong_doctor_payload({"strong": body})
    assert ok and "nested-oid" in detail

def test_t51_phase_39_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 39" in text
    assert "abort_fail_op_id" in text or "ops hint" in text.lower()

def test_t51_runbook_hint() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "ops"
        / "STRONG_AND_SHARED_AUDIT_RUNBOOK.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "abort_fail_op_id" in text or "hint" in text.lower()
    assert "cache-strong-retry-abort" in text

def test_t51_claims_hint_non_claim() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "invariants"
        / "claims.yaml"
    )
    text = path.read_text(encoding="utf-8")
    assert "hint" in text.lower() or "remediation" in text.lower()
