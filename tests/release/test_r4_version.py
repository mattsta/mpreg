"""R4: package version tracks the current release (0.3.2)."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_r4_pyproject_version_current() -> None:
    text = (ROOT / "pyproject.toml").read_text()
    assert 'version = "0.3.2"' in text
    assert "[project.urls]" in text


def test_r4_package_version_string() -> None:
    import mpreg

    # editable install may still resolve metadata
    assert mpreg.__version__ == "0.3.2"
