"""U8 / 0.3.2: Unified Correctness closeout package version + artifacts."""

from pathlib import Path

import mpreg

ROOT = Path(__file__).resolve().parents[2]


def test_u8_pyproject_version_032() -> None:
    text = (ROOT / "pyproject.toml").read_text()
    assert 'version = "0.3.2"' in text


def test_u8_package_version() -> None:
    assert mpreg.__version__ == "0.3.2"


def test_u8_changelog_032() -> None:
    text = (ROOT / "CHANGELOG.md").read_text()
    assert "## [0.3.2]" in text
    assert "Unified Correctness" in text or "unified correctness" in text.lower()


def test_u8_claims_release_032() -> None:
    text = (ROOT / "tests/invariants/claims.yaml").read_text()
    assert "release_0_3_2:" in text
    assert "REL-0.3.2-RAFT-TASK-LIFECYCLE" in text
    assert "REL-0.3.2-FULL-SUITE-MULTIRUN" in text


def test_u8_pending_destroy_gate_exists() -> None:
    path = ROOT / "tests/test_raft_no_pending_destroy.py"
    assert path.is_file()
    body = path.read_text()
    assert "Task was destroyed but it is pending" in body


def test_u8_ci_raft_in_release_gate() -> None:
    gate = (ROOT / "scripts/release_gate.sh").read_text()
    assert "ci_raft.sh" in gate
