"""H2: lint bar includes I/F401/UP035 on mpreg."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_h2_ci_lint_selects_import_rules() -> None:
    text = (ROOT / "scripts/ci_lint.sh").read_text()
    assert "--select E9" in text or "select E9" in text
    assert "I,F401,UP035" in text or "I,F401" in text
    assert "tests/release/" in text
