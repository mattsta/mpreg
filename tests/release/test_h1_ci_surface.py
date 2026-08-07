"""H1: CI unit-fast and typecheck surfaces expanded for 0.3.1."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_h1_unit_fast_includes_expanded_paths() -> None:
    text = (ROOT / "scripts/ci_unit_fast.sh").read_text()
    for needle in (
        "test_cache_strong_gcm.py",
        "test_unified_client.py",
        "tests/release/",
    ):
        assert needle in text, needle


def test_h1_typecheck_import_smoke_broader() -> None:
    text = (ROOT / "scripts/ci_typecheck.sh").read_text()
    assert "mpreg.client.client_api" in text
    assert "mpreg.core.errors" in text
    # version not hardcoded to a single patch only
    assert '== "0.3.0"' not in text
