"""R7: planning artifacts complete for 0.3.0."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

REQUIRED_DOCS = [
    "docs/RELEASE_0_3_PRODUCTION_SNAPSHOT_ARCHITECTURE.md",
    "docs/plans/RELEASE_0_3_PRODUCTION_SNAPSHOT_MASTER_PLAN.md",
    "docs/plans/RELEASE_0_3_BURNDOWN.md",
    "docs/plans/RELEASE_0_3_PROOF_LEDGER.md",
    "docs/ops/RELEASE_CHECKLIST.md",
    "docs/ops/PERF_BASELINE.md",
    "SECURITY.md",
    "CHANGELOG.md",
]

def test_r7_required_docs_exist() -> None:
    for rel in REQUIRED_DOCS:
        assert (ROOT / rel).is_file(), rel

def test_r7_claims_release_section() -> None:
    text = (ROOT / "tests/invariants/claims.yaml").read_text()
    assert "release_0_3:" in text
    assert "REL-0.3.0-CI-MATRIX" in text
    assert "0.3.0 Production Snapshot CI green" in text

def test_r7_architecture_defines_done() -> None:
    text = (ROOT / "docs/RELEASE_0_3_PRODUCTION_SNAPSHOT_ARCHITECTURE.md").read_text()
    assert "Definition of done" in text
    assert "R1" in text and "R7" in text
