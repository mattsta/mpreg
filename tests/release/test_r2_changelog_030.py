"""R2: CHANGELOG has 0.3.0 Production Snapshot section."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_r2_changelog_has_030_section() -> None:
    text = (ROOT / "CHANGELOG.md").read_text()
    assert "## [0.3.0]" in text
    assert "Production Snapshot" in text
    assert "release_gate" in text or "CI quality matrix" in text
    assert "non-goals" in text.lower() or "Non-goals" in text
