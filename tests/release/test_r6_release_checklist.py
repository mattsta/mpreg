"""R6: release checklist + golden path."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_r6_release_checklist() -> None:
    text = (ROOT / "docs/ops/RELEASE_CHECKLIST.md").read_text()
    assert "release_gate.sh" in text
    assert "config-check" in text
    assert "0.3.0" in text
    assert "monitoring_auth_token" in text or "Bearer" in text
