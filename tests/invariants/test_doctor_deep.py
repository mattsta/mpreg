"""E3: doctor --deep includes raft and link-state checks (INV-E3)."""

from __future__ import annotations

import click
from click.testing import CliRunner

from mpreg.cli.main import cli

def test_doctor_deep_option_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["doctor", "--help"])
    assert result.exit_code == 0
    assert "--deep" in result.output

def test_doctor_requires_url() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["doctor", "--deep"])
    assert result.exit_code != 0
