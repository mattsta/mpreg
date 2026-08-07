"""T43 residual closeout: CLI cache-strong-retry-abort surface."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

def test_t43_cli_help_registered() -> None:
    from mpreg.cli.main import cli

    r = CliRunner().invoke(cli, ["client", "--help"])
    assert r.exit_code == 0
    assert "cache-strong-retry-abort" in r.output

    r2 = CliRunner().invoke(cli, ["client", "cache-strong-retry-abort", "--help"])
    assert r2.exit_code == 0
    assert "--op-id" in r2.output
    assert "--peer" in r2.output
    # Honesty in help text
    help_l = r2.output.lower()
    assert "not automatic" in help_l or "ops-driven" in help_l or "cft" in help_l

def test_t43_cli_requires_url() -> None:
    from mpreg.cli.main import cli

    r = CliRunner().invoke(
        cli,
        [
            "client",
            "cache-strong-retry-abort",
            "--namespace",
            "ns",
            "--key",
            "k",
            "--op-id",
            "oid",
        ],
        env={"MPREG_URL": ""},
    )
    assert r.exit_code != 0
    assert "url" in (r.output + str(r.exception)).lower()

def test_t43_cli_invokes_client_retry() -> None:
    from mpreg.cli.main import cli
    from mpreg.client.unified_client import StrongRetryAbortResult

    result = StrongRetryAbortResult(
        success=True,
        cleared=True,
        ok_peers=["n1"],
        fail_peers=[],
        op_id="oid-1",
        attempts=2,
        ops_driven=True,
        automatic_heal=False,
    )

    mock_client = MagicMock()
    mock_client.cache_strong_retry_abort = AsyncMock(return_value=result)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    with patch("mpreg.client.unified_client.MPREGClient", return_value=mock_client):
        r = CliRunner().invoke(
            cli,
            [
                "client",
                "cache-strong-retry-abort",
                "--url",
                "ws://127.0.0.1:9",
                "--namespace",
                "ns",
                "--key",
                "id",
                "--op-id",
                "oid-1",
                "--version",
                "v1",
                "--peer",
                "n1",
                "--peer",
                "n2",
            ],
        )
    assert r.exit_code == 0, r.output
    assert "cleared" in r.output
    assert "ops_driven=True" in r.output
    assert "automatic_heal=False" in r.output
    assert "not automatic" in r.output.lower() or "cft" in r.output.lower()
    mock_client.cache_strong_retry_abort.assert_awaited_once()
    kwargs = mock_client.cache_strong_retry_abort.await_args
    assert kwargs.args[0] == "ns"
    assert kwargs.args[1] == "id"
    assert kwargs.args[2] == "oid-1"
    assert kwargs.kwargs.get("version") == "v1"
    assert kwargs.kwargs.get("peers") == ["n1", "n2"]

def test_t43_cli_json_and_fail_exit() -> None:
    from mpreg.cli.main import cli
    from mpreg.client.unified_client import StrongRetryAbortResult

    result = StrongRetryAbortResult(
        success=False,
        cleared=False,
        ok_peers=[],
        fail_peers=["n1"],
        op_id="oid-x",
        attempts=1,
        ops_driven=True,
        automatic_heal=False,
        error_message="retry_abort_still_fail",
    )
    mock_client = MagicMock()
    mock_client.cache_strong_retry_abort = AsyncMock(return_value=result)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    with patch("mpreg.client.unified_client.MPREGClient", return_value=mock_client):
        r = CliRunner().invoke(
            cli,
            [
                "client",
                "cache-strong-retry-abort",
                "--url",
                "ws://127.0.0.1:9",
                "--namespace",
                "ns",
                "--key",
                "k",
                "--op-id",
                "oid-x",
                "--json",
            ],
        )
    assert r.exit_code == 1, r.output
    assert "still_fail" in r.output or '"cleared": false' in r.output.lower() or (
        '"success": false' in r.output.lower()
    )
    assert "ops_driven" in r.output

def test_t43_docs_honesty() -> None:
    root = Path(__file__).resolve().parents[2]
    runbook = (
        root / "docs" / "ops" / "STRONG_AND_SHARED_AUDIT_RUNBOOK.md"
    ).read_text(encoding="utf-8")
    assert "cache-strong-retry-abort" in runbook
    residual = (
        root / "docs" / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")
    assert "Phase 31" in residual
    guide = (root / "docs" / "MPREG_CLIENT_GUIDE.md").read_text(encoding="utf-8")
    assert "cache-strong-retry-abort" in guide
    plan = (
        root / "docs" / "plans" / "DISTLAB_T43_CLI_RETRY_ABORT_PLAN.md"
    ).read_text(encoding="utf-8")
    assert "not automatic" in plan.lower() or "ops-driven" in plan.lower()

def test_t43_platform_honesty_lists_command() -> None:
    """Keep ERG-05 plane smoke list aware of retry-abort CLI."""
    from mpreg.cli.main import cli

    r = CliRunner().invoke(cli, ["client", "--help"])
    for name in (
        "queue-send",
        "cache-get",
        "cache-put",
        "cache-strong-retry-abort",
        "publish",
    ):
        assert name in r.output, name
