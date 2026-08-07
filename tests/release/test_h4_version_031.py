"""H4: package version is 0.3.1."""

from pathlib import Path

import mpreg

ROOT = Path(__file__).resolve().parents[2]

def test_h4_pyproject_version_031() -> None:
    text = (ROOT / "pyproject.toml").read_text()
    assert 'version = "0.3.1"' in text

def test_h4_package_version() -> None:
    assert mpreg.__version__ == "0.3.1"

def test_h4_changelog_031() -> None:
    text = (ROOT / "CHANGELOG.md").read_text()
    assert "## [0.3.1]" in text
    assert "Hardening" in text or "hardening" in text

def test_h4_claims_release_031() -> None:
    text = (ROOT / "tests/invariants/claims.yaml").read_text()
    assert "release_0_3_1:" in text
    assert "REL-0.3.1-CI-SURFACE" in text
