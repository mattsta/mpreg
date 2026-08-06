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
