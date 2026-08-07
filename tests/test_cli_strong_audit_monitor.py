"""CLI help surfaces for STRONG + shared audit monitor/doctor flags."""

from __future__ import annotations

from click.testing import CliRunner

from mpreg.cli.main import cli

def test_monitor_strong_help() -> None:
    r = CliRunner().invoke(cli, ["monitor", "strong", "--help"])
    assert r.exit_code == 0
    assert "STRONG" in r.output or "strong" in r.output.lower()

def test_monitor_audit_help() -> None:
    r = CliRunner().invoke(cli, ["monitor", "audit", "--help"])
    assert r.exit_code == 0
    assert "audit" in r.output.lower()

def test_doctor_strong_audit_flags_help() -> None:
    r = CliRunner().invoke(cli, ["doctor", "--help"])
    assert r.exit_code == 0
    assert "--strong" in r.output
    assert "--audit" in r.output

def test_distlab_suite_help() -> None:
    r = CliRunner().invoke(cli, ["distlab", "suite", "--help"])
    assert r.exit_code == 0
    assert "--track" in r.output
    assert "--limit" in r.output
    assert "--preset" in r.output

def test_distlab_presets_help() -> None:
    r = CliRunner().invoke(cli, ["distlab", "presets", "--help"])
    assert r.exit_code == 0
    assert "preset" in r.output.lower() or r.exit_code == 0

def test_doctor_strong_evaluate_payload_honesty() -> None:
    """T19: evaluate_strong_doctor_payload fails closed on dishonest caps."""
    from mpreg.cli.main import evaluate_strong_doctor_payload

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
                },
                "counters": {
                    "puts_ok": 3,
                    "gets_refused": 1,
                    "deletes_refused": 2,
                },
            }
        }
    )
    assert ok is True
    assert "get_q=False" in detail
    assert "gets_ref=1" in detail

    bad, bdetail = evaluate_strong_doctor_payload(
        {
            "strong": {
                "health": "ok",
                "capabilities": {"get_quorum": True, "delete_quorum": False},
                "counters": {},
            }
        }
    )
    assert bad is False
    assert "dishonest" in bdetail

    dis_ok, dis_d = evaluate_strong_doctor_payload(
        {"strong": {"health": "disabled", "capabilities": {}, "counters": {}}}
    )
    assert dis_ok is True
    assert "disabled" in dis_d

    mis_ok, _ = evaluate_strong_doctor_payload(
        {"strong": {"health": "misconfigured", "capabilities": {}, "counters": {}}}
    )
    assert mis_ok is False

def test_doctor_shared_audit_evaluate_payload_honesty() -> None:
    """T22: evaluate_shared_audit_doctor_payload fails closed on dishonest caps."""
    from mpreg.cli.main import evaluate_shared_audit_doctor_payload

    ok, detail = evaluate_shared_audit_doctor_payload(
        {
            "shared_audit": {
                "status": "ok",
                "store_size": 2,
                "capabilities": {
                    "gset_epidemic": True,
                    "siem": False,
                    "bft": False,
                    "infinite_retention": False,
                    "linearizable_cluster_ops": False,
                    "multi_tenant_beyond_cluster_id": False,
                },
                "counters": {"deltas_recv": 1, "publish_dropped": 0},
            }
        }
    )
    assert ok is True
    assert "gset=True" in detail
    assert "siem=False" in detail

    bad, bdetail = evaluate_shared_audit_doctor_payload(
        {
            "shared_audit": {
                "status": "ok",
                "capabilities": {"siem": True},
                "counters": {},
            }
        }
    )
    assert bad is False
    assert "dishonest" in bdetail
