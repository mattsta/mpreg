"""H3: SUPPORT.md exists with help and security pointers."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_h3_support_md() -> None:
    text = (ROOT / "SUPPORT.md").read_text()
    assert "SECURITY.md" in text
    assert "config-check" in text or "doctor" in text.lower()
