import json
from pathlib import Path

from click.testing import CliRunner

from mpreg.cli.main import cli


def test_config_check_dev_profile() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli, ["config-check", "mpreg/profiles/dev.toml", "--format", "json"]
    )
    # dev profile may warn about missing monitoring auth → exit 2
    assert result.exit_code in (0, 2)
    assert "groups" in result.output or "identity" in result.output


def test_config_check_explain_includes_guide() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "config-check",
            "mpreg/profiles/dev.toml",
            "--format",
            "json",
            "--explain",
        ],
    )
    assert result.exit_code in (0, 2)
    assert "guide" in result.output or "## identity" in result.output
    assert "four-plane" in result.output.lower() or "systems" in result.output
    # --explain appends a human field guide after JSON; parse the JSON object only
    out = result.output
    brace = out.find("{")
    assert brace >= 0
    depth = 0
    end = None
    for i, ch in enumerate(out[brace:], start=brace):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    assert end is not None
    data = json.loads(out[brace:end])
    assert "strong_cache" in data["groups"]
    assert "shared_audit" in data["groups"]
    assert data["groups"]["strong_cache"]["capabilities"]["get_quorum"] is False
    assert data["groups"]["strong_cache"]["capabilities"]["delete_quorum"] is False
    guide = data.get("guide") or {}
    assert "strong_cache" in guide
    assert "shared_audit" in guide
    assert "strong_cache" in out  # field guide text after JSON
    # T69/T89/T120: strong_cache explain documents residual_ops_hint ops loop
    # and doctor JSON residual field types (not auto-heal)
    sc_guide = str(guide.get("strong_cache") or "")
    assert "residual_ops_hint" in sc_guide
    assert "cache-strong-retry-abort" in sc_guide
    assert (
        "abort_fail_peer_count" in sc_guide
        or "mpreg_strong_abort_fail_peers" in sc_guide
    )
    assert "doctor" in sc_guide.lower()
    assert "strong_doctor_json_residual_fields" in sc_guide or (
        "int" in sc_guide and "list" in sc_guide
    )
    assert "last_abort_fail_op_id" in sc_guide
    assert "not" in sc_guide.lower() and (
        "auto-heal" in sc_guide.lower() or "ops-driven" in sc_guide.lower()
    )


def test_config_check_strong_enabled_honesty_warnings(tmp_path: Path) -> None:
    """T20: enabling STRONG surfaces put-only MVP honesty + mon/cache deps."""
    cfg = tmp_path / "strong.toml"
    cfg.write_text(
        """
name = "t20-strong"
cluster_id = "t20"
host = "127.0.0.1"
port = 19001
enable_default_cache = true
enable_default_queue = true
cache_strong_enabled = true
cache_strong_replica_factor = 3
cache_strong_min_replicas = 3
monitoring_enabled = false
"""
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["config-check", str(cfg), "--format", "json"])
    assert result.exit_code in (0, 2)
    data = json.loads(result.output)
    assert data["groups"]["strong_cache"]["enabled"] is True
    caps = data["groups"]["strong_cache"]["capabilities"]
    assert caps["put_majority_commit"] is True
    assert caps["get_quorum"] is False
    assert caps["delete_quorum"] is False
    # T32/T41: CFT / TTL / retry honesty caps on config-check
    assert caps.get("cft_only") is True
    assert caps.get("abort_best_effort") is True
    assert caps.get("pending_ttl_clears_residual_l1") is False
    assert caps.get("retry_abort_ops_driven") is True
    warns = " ".join(data["warnings"]).lower()
    assert "put-only" in warns or "1012" in warns
    assert "metrics/strong" in warns or "monitoring_enabled" in warns


def test_config_check_shared_audit_honesty_warnings(tmp_path: Path) -> None:
    """T20: shared audit without path / mon warns honestly."""
    cfg = tmp_path / "audit.toml"
    cfg.write_text(
        """
name = "t20-audit"
cluster_id = "t20a"
host = "127.0.0.1"
port = 19002
enable_default_cache = true
enable_default_queue = true
mgmt_audit_shared_enabled = true
monitoring_enabled = false
"""
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["config-check", str(cfg), "--format", "json"])
    assert result.exit_code in (0, 2)
    data = json.loads(result.output)
    assert data["groups"]["shared_audit"]["enabled"] is True
    caps = data["groups"]["shared_audit"]["capabilities"]
    assert caps["gset_epidemic"] is True
    assert caps["siem"] is False
    assert caps["bft"] is False
    assert caps["infinite_retention"] is False
    assert caps["linearizable_cluster_ops"] is False
    assert caps["multi_tenant_beyond_cluster_id"] is False
    warns = " ".join(data["warnings"]).lower()
    assert "mgmt_audit_path" in warns or "jsonl" in warns
    assert "siem" in warns or "g-set" in warns or "bft" in warns


def test_config_check_report_includes_critical_warnings_field() -> None:
    """R3: JSON report always includes critical_warnings list."""
    runner = CliRunner()
    result = runner.invoke(
        cli, ["config-check", "mpreg/profiles/dev.toml", "--format", "json"]
    )
    assert result.exit_code in (0, 2)
    data = json.loads(result.output)
    assert "critical_warnings" in data
    assert isinstance(data["critical_warnings"], list)


def test_config_check_federated_profile_strict_exits_2() -> None:
    """R3: stock federated.toml still has change-me → strict fails."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "config-check",
            "mpreg/profiles/federated.toml",
            "--format",
            "json",
            "--strict",
        ],
    )
    assert result.exit_code == 2
    # non-strict still parses
    lab = runner.invoke(
        cli, ["config-check", "mpreg/profiles/federated.toml", "--format", "json"]
    )
    data = json.loads(lab.output)
    assert data.get("critical_warnings")
    assert any("change-me" in w.lower() for w in data["critical_warnings"])
