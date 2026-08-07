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
