"""R1: CI scripts and workflow exist for 0.3.0 matrix."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

REQUIRED_SCRIPTS = [
    "scripts/ci_lint.sh",
    "scripts/ci_typecheck.sh",
    "scripts/ci_unit_fast.sh",
    "scripts/ci_invariants.sh",
    "scripts/ci_distlab_core.sh",
    "scripts/ci_security_deps.sh",
    "scripts/ci_package_smoke.sh",
    "scripts/ci_perf_smoke.sh",
    "scripts/release_gate.sh",
    "scripts/run_demo_smoke.sh",
]

def test_r1_ci_scripts_exist_and_executable() -> None:
    for rel in REQUIRED_SCRIPTS:
        path = ROOT / rel
        assert path.is_file(), f"missing {rel}"
        assert path.stat().st_mode & 0o111, f"not executable: {rel}"

def test_r1_workflow_has_release_jobs() -> None:
    text = (ROOT / ".github/workflows/ci.yml").read_text()
    for job in (
        "lint:",
        "typecheck:",
        "unit-fast:",
        "invariants:",
        "distlab-core:",
        "security-deps:",
        "demo-smoke:",
        "package-smoke:",
    ):
        assert job in text, f"workflow missing job key {job!r}"
    assert "scripts/ci_lint.sh" in text
    # release_gate.sh is the local full orchestrator; optional in GHA
    assert "package-smoke" in text
