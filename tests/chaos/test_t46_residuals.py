"""T46 residual closeout: client/CLI locs pin for strong_retry_abort."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner


def test_t46_cli_help_has_loc() -> None:
    from mpreg.cli.main import cli

    r = CliRunner().invoke(cli, ["client", "cache-strong-retry-abort", "--help"])
    assert r.exit_code == 0
    assert "--loc" in r.output


def test_t46_cli_forwards_loc() -> None:
    from mpreg.cli.main import cli
    from mpreg.client.unified_client import StrongRetryAbortResult

    result = StrongRetryAbortResult(
        success=True,
        cleared=True,
        ok_peers=["n1"],
        fail_peers=[],
        op_id="oid",
        attempts=1,
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
                "k",
                "--op-id",
                "oid",
                "--loc",
                "cache",
            ],
        )
    assert r.exit_code == 0, r.output
    kwargs = mock_client.cache_strong_retry_abort.await_args.kwargs
    assert kwargs.get("locs") == frozenset({"cache"})


def test_t46_docs_honesty() -> None:
    root = Path(__file__).resolve().parents[2]
    guide = (root / "docs" / "MPREG_CLIENT_GUIDE.md").read_text(encoding="utf-8")
    assert "locs" in guide.lower() or "resource" in guide.lower()
    residual = (root / "docs" / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md").read_text(
        encoding="utf-8"
    )
    assert "Phase 34" in residual
    plan = (
        root / "docs" / "plans" / "DISTLAB_T46_CLIENT_LOCS_RETRY_PLAN.md"
    ).read_text(encoding="utf-8")
    assert "not" in plan.lower() and ("bft" in plan.lower() or "auto" in plan.lower())


def test_t46_runbook_locs() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "ops"
        / "STRONG_AND_SHARED_AUDIT_RUNBOOK.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "--loc" in text or "locs=" in text or "locs" in text
