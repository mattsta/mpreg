"""B8: Dual-consensus footgun guard — facade is canonical."""

from __future__ import annotations

import ast
from pathlib import Path

import mpreg.consensus as facade
from mpreg.core import consensus_api


def test_facade_is_single_authority() -> None:
    assert facade.ProductionRaft is consensus_api.ProductionRaft
    assert facade.LightweightConsensusManager is not None
    # Documented footgun name still exists but status_dict is Raft-only helper
    assert callable(facade.status_dict)


def test_no_new_direct_consensus_manager_in_server_pkg() -> None:
    """server_pkg must not import ConsensusManager for strong consensus paths."""
    root = Path(__file__).resolve().parents[2] / "mpreg" / "server_pkg"
    offenders: list[str] = []
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module and "fabric.consensus" in node.module:
                    offenders.append(str(path))
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if "fabric.consensus" in alias.name:
                        offenders.append(str(path))
    assert not offenders, f"server_pkg imports fabric.consensus: {offenders}"
