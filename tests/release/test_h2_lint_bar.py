"""H2: lint bar is full-tree ruff (project rules)."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_h2_ci_lint_runs_full_tree_ruff() -> None:
    text = (ROOT / "scripts/ci_lint.sh").read_text()
    assert "ruff check" in text
    # Full product + tests + tools surfaces (not a narrow select-only gate)
    assert "mpreg" in text
    assert "tests" in text
    assert "tools" in text
    # Must not be stuck on the old scoped E9-only / import-only bar alone
    assert (
        "full-tree" in text
        or "full tree" in text.lower()
        or ("mpreg tests tools" in text.replace("\\\n", " "))
    )
