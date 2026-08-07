"""T58 residual closeout: doctor prefers server residual_ops_hint."""

from __future__ import annotations

from pathlib import Path

from mpreg.cli.main import evaluate_strong_doctor_payload, strong_residual_ops_hint

def test_t58_prefer_server_hint() -> None:
    body = {
        "last_abort_fail_peers": ["n1"],
        "last_abort_fail_op_id": "oid-x",
        "residual_ops_hint": "SERVER_HINT_MARKER not auto-heal",
    }
    assert strong_residual_ops_hint(body) == "SERVER_HINT_MARKER not auto-heal"

def test_t58_fallback_when_empty_server_hint() -> None:
    body = {
        "last_abort_fail_peers": ["n9"],
        "last_abort_fail_op_id": "oid-fb",
        "residual_ops_hint": "",
    }
    h = strong_residual_ops_hint(body)
    assert "cache-strong-retry-abort" in h
    assert "--peer n9" in h
    assert "oid-fb" in h

def test_t58_doctor_uses_server_hint() -> None:
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
                "counters": {"aborts_peer_fail": 1},
                "last_abort_fail_peers": ["n1"],
                "last_abort_fail_op_id": "oid-d",
                "residual_ops_hint": "CUSTOM_SERVER_HINT not auto-heal",
                "visible_count": 0,
                "backups_count": 0,
                "backups_pruned_total": 0,
            }
        }
    )
    assert ok is True
    assert "CUSTOM_SERVER_HINT" in detail

def test_t58_phase_46_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 46" in text

def test_t58_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T58_DOCTOR_PREFER_HINT_PLAN.md"
    ).is_file()
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T58" in ledger
