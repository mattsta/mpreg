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
        strong_doctor_json_residual_fields,
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
    # T100/T101: doctor JSON residual fields — JSON-native int + list
    fields = strong_doctor_json_residual_fields(body)
    assert fields["residual_ops_hint"] == hint
    assert "not auto-heal" in str(fields["residual_ops_hint"])
    assert isinstance(fields["abort_fail_peer_count"], int)
    assert fields["abort_fail_peer_count"] >= 1
    assert isinstance(fields["last_abort_fail_peers"], list)
    assert fields["last_abort_fail_peers"] == ["n1"]
    assert fields["abort_fail_peer_count"] == len(fields["last_abort_fail_peers"])
    assert "abort_fail_peer_count=" in detail
    row: dict[str, object] = {
        "check": "metrics_strong",
        "status": "OK",
        "detail": detail,
        **fields,
    }
    assert isinstance(row["abort_fail_peer_count"], int)
    assert isinstance(row["last_abort_fail_peers"], list)
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
    empty_fields = strong_doctor_json_residual_fields(empty_body)
    assert empty_fields["residual_ops_hint"] == ""
    assert empty_fields["abort_fail_peer_count"] == 0
    assert isinstance(empty_fields["abort_fail_peer_count"], int)
    assert empty_fields["last_abort_fail_peers"] == []
    assert isinstance(empty_fields["last_abort_fail_peers"], list)

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

def test_strong_abort_fail_peer_count_max_hypothesis() -> None:
    """T97: property — peer-count helper never under-reports non-empty peers."""
    from hypothesis import given, settings, strategies as st

    from mpreg.cli.main import _strong_abort_fail_peer_count

    peer = st.text(
        alphabet=st.characters(
            whitelist_categories=("L", "N"), whitelist_characters="-_"
        ),
        min_size=1,
        max_size=8,
    ).filter(lambda s: s.strip() != "")

    @given(
        peers=st.lists(peer, max_size=5, unique=True),
        reported=st.integers(min_value=0, max_value=10),
    )
    @settings(max_examples=40, deadline=None)
    def _prop(peers: list[str], reported: int) -> None:
        body = {
            "last_abort_fail_peers": list(peers),
            "abort_fail_peer_count": reported,
        }
        n = _strong_abort_fail_peer_count(body)
        if peers:
            assert n >= len(peers)
            assert n >= reported or n == len(peers)
            assert n == max(reported, len(peers))
        else:
            assert n == max(reported, 0)

    _prop()

def test_strong_doctor_json_residual_fields_types() -> None:
    """T100/T101/T110: doctor JSON residual fields use int + list + op_id str."""
    from mpreg.cli.main import strong_doctor_json_residual_fields

    residual = {
        "last_abort_fail_peers": ["peer-a", "peer-b"],
        "last_abort_fail_op_id": "oid-json",
        "recent_abort_fails": [
            {"op_id": "oid-json", "key": "orders/cart-1", "peers": ["peer-a", "peer-b"]}
        ],
        "abort_fail_peer_count": 2,
    }
    fields = strong_doctor_json_residual_fields(residual)
    assert set(fields) == {
        "residual_ops_hint",
        "abort_fail_peer_count",
        "last_abort_fail_peers",
        "last_abort_fail_op_id",
    }
    assert isinstance(fields["abort_fail_peer_count"], int)
    assert fields["abort_fail_peer_count"] == 2
    assert isinstance(fields["last_abort_fail_peers"], list)
    assert fields["last_abort_fail_peers"] == ["peer-a", "peer-b"]
    assert isinstance(fields["residual_ops_hint"], str)
    assert fields["residual_ops_hint"]
    assert "cache-strong-retry-abort" in fields["residual_ops_hint"]
    assert fields["last_abort_fail_op_id"] == "oid-json"
    assert isinstance(fields["last_abort_fail_op_id"], str)
    # Clean path
    clean = strong_doctor_json_residual_fields({})
    assert clean["abort_fail_peer_count"] == 0
    assert isinstance(clean["abort_fail_peer_count"], int)
    assert clean["last_abort_fail_peers"] == []
    assert isinstance(clean["last_abort_fail_peers"], list)
    assert clean["residual_ops_hint"] == ""
    assert clean["last_abort_fail_op_id"] == ""
    assert isinstance(clean["last_abort_fail_op_id"], str)

def test_strong_doctor_json_residual_fields_hypothesis() -> None:
    """T113: property — residual doctor JSON fields keep JSON-native types."""
    from hypothesis import given, settings, strategies as st

    from mpreg.cli.main import strong_doctor_json_residual_fields

    peer = st.text(
        alphabet=st.characters(
            whitelist_categories=("L", "N"), whitelist_characters="-_"
        ),
        min_size=1,
        max_size=8,
    ).filter(lambda s: s.strip() != "")
    oid = st.text(
        alphabet=st.characters(
            whitelist_categories=("L", "N"), whitelist_characters="-_"
        ),
        min_size=0,
        max_size=12,
    )

    @given(
        peers=st.lists(peer, max_size=5, unique=True),
        op_id=oid,
        reported=st.integers(min_value=0, max_value=8),
    )
    @settings(max_examples=40, deadline=None)
    def _prop(peers: list[str], op_id: str, reported: int) -> None:
        body = {
            "last_abort_fail_peers": list(peers),
            "last_abort_fail_op_id": op_id,
            "abort_fail_peer_count": reported,
        }
        fields = strong_doctor_json_residual_fields(body)
        assert isinstance(fields["abort_fail_peer_count"], int)
        assert isinstance(fields["last_abort_fail_peers"], list)
        assert isinstance(fields["last_abort_fail_op_id"], str)
        assert isinstance(fields["residual_ops_hint"], str)
        assert fields["last_abort_fail_peers"] == list(peers)
        assert fields["last_abort_fail_op_id"] == str(op_id or "")
        if peers:
            assert fields["abort_fail_peer_count"] == max(reported, len(peers))
            assert fields["residual_ops_hint"]
        else:
            assert fields["abort_fail_peer_count"] == max(reported, 0)
            assert fields["residual_ops_hint"] == ""

    _prop()

def test_openapi_abort_fail_peer_count_example() -> None:
    """T102/T123/T124: OpenAPI residual fields document typed examples."""
    import json

    from mpreg.server_pkg.openapi_surface import build_monitoring_openapi

    doc = build_monitoring_openapi()
    blob = json.dumps(doc)
    assert "abort_fail_peer_count" in blob
    assert "last_abort_fail_op_id" in blob
    assert "last_abort_fail_peers" in blob

    def _find_prop(obj: object, name: str) -> dict | None:
        if isinstance(obj, dict):
            if name in obj and isinstance(obj[name], dict) and "type" in obj[name]:
                return obj[name]  # type: ignore[return-value]
            for v in obj.values():
                found = _find_prop(v, name)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for v in obj:
                found = _find_prop(v, name)
                if found is not None:
                    return found
        return None

    count_node = _find_prop(doc, "abort_fail_peer_count")
    assert count_node is not None
    assert count_node.get("type") == "integer"
    assert count_node.get("example") == 1
    assert "doctor" in str(count_node.get("description", "")).lower() or "integer" in str(
        count_node.get("description", "")
    ).lower()

    oid_node = _find_prop(doc, "last_abort_fail_op_id")
    assert oid_node is not None
    assert oid_node.get("type") == "string"
    assert oid_node.get("example") == "op-abc123"

    peers_node = _find_prop(doc, "last_abort_fail_peers")
    assert peers_node is not None
    assert peers_node.get("type") == "array"
    assert isinstance(peers_node.get("example"), list)
    assert peers_node.get("example")

