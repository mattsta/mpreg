"""T23 residual closeout: config-check audit caps + live e2e capability asserts."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from mpreg.cli.main import cli
from mpreg.server_pkg.openapi_surface import (
    build_monitoring_openapi,
    openapi_path_set,
    route_table_path_set,
)

def test_t23_config_check_shared_audit_capabilities_parity(tmp_path: Path) -> None:
    """config-check shared_audit.capabilities mirrors metrics honesty contract."""
    cfg = tmp_path / "t23-audit.toml"
    cfg.write_text(
        """
name = "t23-audit"
cluster_id = "t23a"
host = "127.0.0.1"
port = 19023
enable_default_cache = true
enable_default_queue = true
mgmt_audit_shared_enabled = true
mgmt_audit_path = "/tmp/t23-audit.jsonl"
monitoring_enabled = true
monitoring_port = 19024
"""
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["config-check", str(cfg), "--format", "json"])
    assert result.exit_code in (0, 2)
    data = json.loads(result.output)
    sa = data["groups"]["shared_audit"]
    assert sa["enabled"] is True
    caps = sa["capabilities"]
    assert caps["gset_epidemic"] is True
    for k in (
        "siem",
        "bft",
        "infinite_retention",
        "linearizable_cluster_ops",
        "multi_tenant_beyond_cluster_id",
    ):
        assert caps[k] is False, k

    # Disabled → gset_epidemic false
    cfg2 = tmp_path / "t23-off.toml"
    cfg2.write_text(
        """
name = "t23-off"
cluster_id = "t23b"
host = "127.0.0.1"
port = 19025
enable_default_cache = true
enable_default_queue = true
mgmt_audit_shared_enabled = false
monitoring_enabled = true
monitoring_port = 19026
"""
    )
    r2 = runner.invoke(cli, ["config-check", str(cfg2), "--format", "json"])
    assert r2.exit_code in (0, 2)
    d2 = json.loads(r2.output)
    assert d2["groups"]["shared_audit"]["capabilities"]["gset_epidemic"] is False
    assert d2["groups"]["shared_audit"]["capabilities"]["siem"] is False

def test_t23_config_check_explain_mentions_audit_caps() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli, ["config-check", "mpreg/profiles/dev.toml", "--format", "json", "--explain"]
    )
    assert result.exit_code in (0, 2)
    out = result.output
    assert "shared_audit" in out
    # guide text after JSON
    assert "SIEM" in out or "siem" in out.lower() or "BFT" in out or "bft" in out.lower()

def test_t23_openapi_still_matches_route_table() -> None:
    assert openapi_path_set() == route_table_path_set()
    doc = build_monitoring_openapi()
    schemas = (doc.get("components") or {}).get("schemas") or {}
    assert "SharedAuditMetricsResponse" in schemas
    assert "StrongMetricsResponse" in schemas
