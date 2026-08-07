"""R2: README must not claim unshipped enterprise auth as done."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

def test_r2_oauth_not_listed_as_shipped_feature() -> None:
    readme = (ROOT / "README.md").read_text()
    # OAuth2 may appear only under roadmap with explicit not-in-0.3.0 language
    for i, line in enumerate(readme.splitlines(), 1):
        if "OAuth2" not in line and "OIDC" not in line:
            continue
        lower = line.lower()
        assert "roadmap" in lower or "not in 0.3" in lower or "not shipped" in lower, (
            f"L{i}: OAuth/OIDC must be roadmap-qualified: {line!r}"
        )

def test_r2_no_uncaveated_million_msg() -> None:
    readme = (ROOT / "README.md").read_text()
    for i, line in enumerate(readme.splitlines(), 1):
        if "Million+" not in line and "million+" not in line:
            continue
        lower = line.lower()
        assert (
            "lab" in lower
            or "hardware" in lower
            or "perf_baseline" in lower
            or "not a wan" in lower
        ), f"L{i}: throughput claim needs lab caveat: {line!r}"

def test_r2_readme_points_at_security_and_release_arch() -> None:
    readme = (ROOT / "README.md").read_text()
    assert "SECURITY.md" in readme
    assert "RELEASE_0_3_PRODUCTION_SNAPSHOT_ARCHITECTURE" in readme
    assert "PERF_BASELINE" in readme
