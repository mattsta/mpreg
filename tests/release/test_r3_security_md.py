"""R3: SECURITY.md + config-check critical_warnings."""

import json
from pathlib import Path

from click.testing import CliRunner

from mpreg.cli.main import cli

ROOT = Path(__file__).resolve().parents[2]


def test_r3_security_md_exists() -> None:
    text = (ROOT / "SECURITY.md").read_text()
    assert "Threat model" in text or "threat model" in text.lower()
    assert "CFT" in text
    assert "Reporting" in text or "report" in text.lower()
    assert "BFT" in text


def test_r3_config_check_critical_warnings_change_me(tmp_path: Path) -> None:
    cfg = tmp_path / "fed.toml"
    cfg.write_text(
        """
name = "federated-prod-test"
cluster_id = "c1"
host = "0.0.0.0"
port = 19050
monitoring_enabled = true
monitoring_host = "0.0.0.0"
monitoring_enable_cors = true
enable_default_cache = true
enable_default_queue = true
enable_cache_federation = true
peers = ["ws://127.0.0.1:19051"]
fabric_routing_enabled = true
discovery_summary_signing_secret = "change-me-secret"
fabric_gossip_hmac_secret = "change-me-gossip"
fabric_gossip_require_hmac = true
"""
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["config-check", str(cfg), "--format", "json"])
    assert result.exit_code in (0, 2)
    data = json.loads(result.output)
    assert "critical_warnings" in data
    crit = " ".join(data["critical_warnings"]).lower()
    assert "change-me" in crit or "cors" in crit
    # strict must fail
    strict = runner.invoke(
        cli, ["config-check", str(cfg), "--format", "json", "--strict"]
    )
    assert strict.exit_code == 2


def test_r3_config_check_cors_warning(tmp_path: Path) -> None:
    cfg = tmp_path / "cors.toml"
    cfg.write_text(
        """
name = "cors-node"
cluster_id = "c"
host = "127.0.0.1"
port = 19052
monitoring_enabled = true
monitoring_host = "127.0.0.1"
monitoring_enable_cors = true
monitoring_auth_token = "lab-token"
enable_default_cache = true
enable_default_queue = true
"""
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["config-check", str(cfg), "--format", "json"])
    data = json.loads(result.output)
    warns = " ".join(data["warnings"]).lower()
    assert "cors" in warns
    crit = " ".join(data.get("critical_warnings") or []).lower()
    assert "cors" in crit
