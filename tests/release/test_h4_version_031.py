"""H4: 0.3.1 Production Hardening historical artifacts remain in tree.

Package version has moved forward (see test_u8_version_032); this file locks
the 0.3.1 CHANGELOG section and claims so the hardening story stays auditable.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_h4_changelog_031() -> None:
    text = (ROOT / "CHANGELOG.md").read_text()
    assert "## [0.3.1]" in text
    assert "Hardening" in text or "hardening" in text


def test_h4_claims_release_031() -> None:
    text = (ROOT / "tests/invariants/claims.yaml").read_text()
    assert "release_0_3_1:" in text
    assert "REL-0.3.1-CI-SURFACE" in text
