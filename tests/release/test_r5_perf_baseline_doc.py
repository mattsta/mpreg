"""R5: PERF_BASELINE doc exists and is honest."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_r5_perf_baseline_doc() -> None:
    path = ROOT / "docs/ops/PERF_BASELINE.md"
    text = path.read_text()
    assert path.stat().st_size > 200
    assert "not" in text.lower() and "wan" in text.lower()
    assert "ci_perf_smoke" in text or "pytest" in text
