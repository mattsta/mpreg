"""Package metadata, public exports, and CLI command registration checks."""

from __future__ import annotations

import mpreg
from mpreg.cli.main import cli


def test_version_matches_package_metadata() -> None:
    from importlib.metadata import version

    assert mpreg.__version__ == version("mpreg")


def test_license_is_apache() -> None:
    assert mpreg.__license__ == "Apache-2.0"


def test_public_exports() -> None:
    assert hasattr(mpreg, "MPREGServer")
    assert hasattr(mpreg, "MPREGClusterClient")
    assert hasattr(mpreg, "allocate_port")
    assert "MPREGServer" in mpreg.__all__


def test_cli_has_federation_metrics_not_duplicate_top_level_metrics() -> None:
    cmd_names = set(cli.commands)
    assert "federation-metrics" in cmd_names
    monitor = cli.commands.get("monitor")
    assert monitor is not None
    monitor_cmds = set(monitor.commands)
    assert "metrics" in monitor_cmds
    assert "prometheus" in monitor_cmds


def test_doctor_command_registered() -> None:
    assert "doctor" in cli.commands
    assert "profile" in cli.commands
