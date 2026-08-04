"""Unit tests for concurrent suite harness (limits + hang breadcrumbs)."""

from __future__ import annotations

from pathlib import Path

from mpreg.testing.hang_observe import HangStateDir, enable_faulthandler
from mpreg.testing.resource_limits import NoFileLimit, raise_open_file_limit
from mpreg.fabric.hop_headers import advance_fabric_headers
from mpreg.core.errors import MpregError, MpregErrorCode
from mpreg.fabric.message import MessageHeaders
import pytest

def test_raise_open_file_limit_is_self_managing() -> None:
    before = NoFileLimit.current()
    after = raise_open_file_limit(1_048_576)
    assert after.soft >= min(before.soft, 8192)
    assert after.soft >= before.soft or after.soft >= 8192

def test_hang_state_breadcrumbs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MPREG_TEST_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw99")
    enable_faulthandler()
    state = HangStateDir()
    state.write_pid(12345)
    state.write_current("tests/x.py::test_y", pid=12345)
    crumbs = state.breadcrumbs()
    assert len(crumbs) == 1
    assert crumbs[0].worker == "gw99"
    assert crumbs[0].pid == 12345
    assert crumbs[0].nodeid == "tests/x.py::test_y"
    state.clear_current()
    assert state.breadcrumbs() == []

def test_advance_fabric_headers_fail_closed_loop() -> None:
    with pytest.raises(MpregError) as ei:
        advance_fabric_headers(
            correlation_id="c",
            headers=MessageHeaders(
                correlation_id="c",
                routing_path=("node-a",),
            ),
            node_id="node-a",
            cluster_id="cluster-a",
            max_hops=5,
        )
    assert ei.value.code == int(MpregErrorCode.ROUTE_LOOP)

def test_advance_fabric_headers_fail_closed_budget() -> None:
    with pytest.raises(MpregError) as ei:
        advance_fabric_headers(
            correlation_id="c",
            headers=MessageHeaders(
                correlation_id="c",
                routing_path=("n1", "n2", "n3"),
                hop_budget=1,
            ),
            node_id="n4",
            cluster_id="c1",
            max_hops=1,
        )
    assert ei.value.code == int(MpregErrorCode.HOP_BUDGET_EXCEEDED)
