"""CLI / entrypoint wiring for curriculum apps (no live servers)."""

from __future__ import annotations

import json
import subprocess

import pytest

from mpreg.examples.apps._shared.runner import main as examples_main


@pytest.mark.example_apps
@pytest.mark.unit
def test_runner_list_table(capsys: pytest.CaptureFixture[str]) -> None:
    examples_main(["list"])
    out = capsys.readouterr().out
    assert "hello_rpc" in out
    assert "order_intake" in out
    assert "multi_region_shop" in out
    assert "global_edge_control_plane" in out
    assert "Total:" in out


@pytest.mark.example_apps
@pytest.mark.unit
def test_runner_list_json(capsys: pytest.CaptureFixture[str]) -> None:
    examples_main(["list", "--format", "json"])
    data = json.loads(capsys.readouterr().out)
    ids = {row["id"] for row in data}
    assert "hello_rpc" in ids
    assert "plane_rpc" in ids
    assert "signed_route_border" in ids
    assert all("systems" in row and "kind" in row for row in data)
    assert len(data) >= 30


@pytest.mark.example_apps
@pytest.mark.unit
def test_runner_describe(capsys: pytest.CaptureFixture[str]) -> None:
    examples_main(["describe", "hello_rpc"])
    out = capsys.readouterr().out
    assert "hello_rpc" in out
    assert "mpreg-example run hello_rpc" in out
    assert "python -m" not in out
    assert "uv run python" not in out


@pytest.mark.example_apps
@pytest.mark.unit
def test_runner_path(capsys: pytest.CaptureFixture[str]) -> None:
    examples_main(["path", "hello_rpc"])
    path = capsys.readouterr().out.strip()
    assert path.endswith("hello_rpc/")


@pytest.mark.example_apps
@pytest.mark.unit
def test_runner_bundles(capsys: pytest.CaptureFixture[str]) -> None:
    examples_main(["bundles"])
    out = capsys.readouterr().out
    assert "tier1:" in out
    assert "tier2:" in out
    assert "product_vertical:" in out


@pytest.mark.example_apps
@pytest.mark.unit
def test_mpreg_example_console_script_list() -> None:
    """Installed entrypoint must work via uv (not python -m)."""
    proc = subprocess.run(
        ["uv", "run", "mpreg-example", "list", "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout
    data = json.loads(proc.stdout)
    assert any(row["id"] == "hello_rpc" for row in data)
    assert len(data) >= 30


@pytest.mark.example_apps
@pytest.mark.unit
def test_mpreg_examples_group_list() -> None:
    """``mpreg examples`` group shares the same runner."""
    proc = subprocess.run(
        ["uv", "run", "mpreg", "examples", "list", "--format", "json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout
    data = json.loads(proc.stdout)
    assert any(row["id"] == "hello_rpc" for row in data)


@pytest.mark.example_apps
@pytest.mark.unit
def test_mpreg_demo_delegates() -> None:
    """``mpreg demo list`` routes through unified runner."""
    proc = subprocess.run(
        ["uv", "run", "mpreg", "demo", "list"],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout
    assert "tier1" in proc.stdout


@pytest.mark.example_apps
@pytest.mark.unit
def test_no_python_dash_m_in_help() -> None:
    proc = subprocess.run(
        ["uv", "run", "mpreg-example", "--help"],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    blob = (proc.stdout or "") + (proc.stderr or "")
    assert "python -m" not in blob
