"""R4: package version is 0.3.1."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_r4_pyproject_version_030() -> None:
    text = (ROOT / "pyproject.toml").read_text()
    assert 'version = "0.3.1"' in text
    assert "[project.urls]" in text


def test_r4_package_version_string() -> None:
    import mpreg

    # editable install may still resolve metadata
    assert mpreg.__version__ == "0.3.1"
