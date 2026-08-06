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
                    "cft_only": True,
                    "abort_best_effort": True,
                },
                "counters": {
                    "puts_ok": 3,
                    "gets_refused": 1,
                    "deletes_refused": 2,
                    "aborts_peer_fail": 0,
                },
            }
        }
    )
    assert ok is True
    assert "get_q=False" in detail
    assert "gets_ref=1" in detail
    assert "cft=True" in detail
    assert "abort_be=True" in detail
    assert "ttl_gc=False" in detail

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

    cft_bad, cft_d = evaluate_strong_doctor_payload(
        {
            "strong": {
                "health": "ok",
                "capabilities": {
                    "get_quorum": False,
                    "cft_only": False,
                    "abort_best_effort": True,
                },
                "counters": {},
            }
        }
    )
    assert cft_bad is False
    assert "cft_only" in cft_d

    abort_bad, abort_d = evaluate_strong_doctor_payload(
        {
            "strong": {
                "health": "ok",
                "capabilities": {
                    "get_quorum": False,
                    "cft_only": True,
                    "abort_best_effort": False,
                },
                "counters": {},
            }
        }
    )
    assert abort_bad is False
    assert "abort_best_effort" in abort_d

    dis_ok, dis_d = evaluate_strong_doctor_payload(
        {"strong": {"health": "disabled", "capabilities": {}, "counters": {}}}
    )
    assert dis_ok is True
    assert "disabled" in dis_d

    mis_ok, _ = evaluate_strong_doctor_payload(
        {"strong": {"health": "misconfigured", "capabilities": {}, "counters": {}}}
    )
    assert mis_ok is False

def test_doctor_strong_row_residual_ops_hint_field() -> None:
    """T71: doctor strong checks expose residual_ops_hint for JSON consumers.

    Mirrors the row-building rule in ``doctor`` when metrics_strong/mgmt_strong
    succeed: always include the key (empty when no residual candidates).
    """
    from mpreg.cli.main import (
        evaluate_strong_doctor_payload,
        strong_residual_ops_hint,
    )

    payload = {
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
            "counters": {"puts_ok": 1, "aborts_peer_fail": 1},
            "last_abort_fail_peers": ["n1"],
            "last_abort_fail_op_id": "oid-t71",
            "recent_abort_fails": [
                {
                    "op_id": "oid-t71",
                    "key": "ns-t71/key-t71",
                    "peers": ["n1"],
                }
            ],
        }
    }
    ok, detail = evaluate_strong_doctor_payload(payload)
    assert ok is True
    assert "cache-strong-retry-abort" in detail
    body = payload["strong"]
    hint = strong_residual_ops_hint(body)
    assert hint
    assert "--namespace ns-t71" in hint
    assert "--key key-t71" in hint
    # Same shape doctor JSON rows use for metrics_strong / mgmt_strong
    from mpreg.cli.main import _strong_abort_fail_peer_count

    n = _strong_abort_fail_peer_count(body)
    assert n >= 1
    assert "abort_fail_peer_count=" in detail
    row = {
        "check": "metrics_strong",
        "status": "OK",
        "detail": detail,
        "residual_ops_hint": hint,
        "abort_fail_peer_count": str(n),
    }
    assert row["residual_ops_hint"]
    assert "not auto-heal" in row["residual_ops_hint"]
    assert row["abort_fail_peer_count"] == str(n)
    # Empty when no residual candidates
    empty_body = {
        "health": "ok",
        "capabilities": {
            "get_quorum": False,
            "delete_quorum": False,
            "cft_only": True,
            "abort_best_effort": True,
        },
        "counters": {},
        "last_abort_fail_peers": [],
    }
    assert strong_residual_ops_hint(empty_body) == ""
    assert _strong_abort_fail_peer_count(empty_body) == 0

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

def test_doctor_residual_hint_hypothesis() -> None:
    """T74: property — residual peers ⇒ hint; empty peers ⇒ no CLI template."""
    from hypothesis import given, settings, strategies as st

    from mpreg.cli.main import (
        evaluate_strong_doctor_payload,
        strong_residual_ops_hint,
    )

    peer_st = st.lists(
        st.text(
            alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="-_"),
            min_size=1,
            max_size=8,
        ).filter(lambda s: s.strip() != ""),
        max_size=4,
        unique=True,
    )
    oid_st = st.one_of(st.just(""), st.text(min_size=1, max_size=12).filter(str.strip))

    @given(peers=peer_st, oid=oid_st)
    @settings(max_examples=40, deadline=None)
    def _prop(peers: list[str], oid: str) -> None:
        body = {
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
            "counters": {"puts_ok": 1, "aborts_peer_fail": 1 if peers else 0},
            "last_abort_fail_peers": list(peers),
            "last_abort_fail_op_id": oid,
        }
        hint = strong_residual_ops_hint(body)
        ok, detail = evaluate_strong_doctor_payload({"strong": body})
        assert ok is True
        if peers:
            assert hint
            assert "cache-strong-retry-abort" in hint
            assert "not auto-heal" in hint
            assert "cache-strong-retry-abort" in detail
            if oid.strip():
                assert f"--op-id {oid.strip()}" in hint
            for p in peers:
                assert f"--peer {p}" in hint
        else:
            assert hint == ""
            assert "cache-strong-retry-abort" not in detail

    _prop()

def test_doctor_dishonest_caps_hypothesis() -> None:
    """T74: property — dishonest get/delete quorum always fails doctor."""
    from hypothesis import given, settings, strategies as st

    from mpreg.cli.main import evaluate_strong_doctor_payload

    @given(
        get_q=st.booleans(),
        del_q=st.booleans(),
        cft=st.booleans(),
        abort_be=st.booleans(),
        ttl_gc=st.booleans(),
        retry_ops=st.booleans(),
    )
    @settings(max_examples=32, deadline=None)
    def _prop(
        get_q: bool,
        del_q: bool,
        cft: bool,
        abort_be: bool,
        ttl_gc: bool,
        retry_ops: bool,
    ) -> None:
        body = {
            "health": "ok",
            "capabilities": {
                "get_quorum": get_q,
                "delete_quorum": del_q,
                "cft_only": cft,
                "abort_best_effort": abort_be,
                "pending_ttl_clears_residual_l1": ttl_gc,
                "retry_abort_ops_driven": retry_ops,
            },
            "counters": {},
        }
        ok, detail = evaluate_strong_doctor_payload({"strong": body})
        dishonest = get_q or del_q or (not cft) or (not abort_be) or ttl_gc or (not retry_ops)
        if dishonest:
            assert ok is False
            assert "dishonest" in detail
        else:
            assert ok is True

    _prop()

def test_count_abort_fail_peers_helper() -> None:
    """T80: count_abort_fail_peers dedupes and reads nested coordinator."""
    from mpreg.core.cache_strong import count_abort_fail_peers

    assert count_abort_fail_peers([]) == 0
    assert count_abort_fail_peers(None) == 0
    assert count_abort_fail_peers(["n1", "n1", "n2"]) == 2
    assert count_abort_fail_peers(body={"last_abort_fail_peers": ["a", "b"]}) == 2
    assert (
        count_abort_fail_peers(
            body={"coordinator": {"last_abort_fail_peers": ["x"]}}
        )
        == 1
    )
    assert count_abort_fail_peers(body={}) == 0

def test_count_abort_fail_peers_hypothesis() -> None:
    """T84: property — count equals unique non-empty peers; body path matches."""
    from hypothesis import given, settings, strategies as st

    from mpreg.core.cache_strong import count_abort_fail_peers

    peer = st.text(
        alphabet=st.characters(
            whitelist_categories=("L", "N"), whitelist_characters="-_:"
        ),
        min_size=1,
        max_size=10,
    ).filter(lambda s: s.strip() != "")

    @given(peers=st.lists(peer, max_size=6))
    @settings(max_examples=50, deadline=None)
    def _prop(peers: list[str]) -> None:
        expected = len(list(dict.fromkeys(peers)))
        assert count_abort_fail_peers(peers) == expected
        assert count_abort_fail_peers(body={"last_abort_fail_peers": peers}) == expected
        assert (
            count_abort_fail_peers(
                body={"coordinator": {"last_abort_fail_peers": peers}}
            )
            == expected
        )
        # explicit peers wins over body
        assert count_abort_fail_peers(["solo"], body={"last_abort_fail_peers": peers}) == 1

    _prop()

def test_doctor_detail_abort_fail_peer_count() -> None:
    """T87: doctor detail and helper surface abort_fail_peer_count."""
    from mpreg.cli.main import (
        _strong_abort_fail_peer_count,
        evaluate_strong_doctor_payload,
    )

    body = {
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
        "counters": {"puts_ok": 1, "aborts_peer_fail": 2},
        "last_abort_fail_peers": ["n1", "n2"],
        "last_abort_fail_op_id": "oid-count",
        "abort_fail_peer_count": 2,
    }
    assert _strong_abort_fail_peer_count(body) == 2
    ok, detail = evaluate_strong_doctor_payload({"strong": body})
    assert ok is True
    assert "abort_fail_peer_count=2" in detail
    assert "abort_fail_peers=" in detail
    # Server count 0 but peers present → still report peers length
    body2 = dict(body)
    body2["abort_fail_peer_count"] = 0
    assert _strong_abort_fail_peer_count(body2) == 2

