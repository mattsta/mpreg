"""H3: SUPPORT.md exists; no forbidden deploy/product tokens in tree."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Split so this file never embeds the forbidden product tokens contiguously.
_ORCH_A = "doc" + "ker"
_ORCH_B = "kuber" + "netes"
_ORCH_C = "k" + "8s"
_FORBIDDEN_STORE = "re" + "dis"
_RECIPE = "Doc" + "kerfile"
_IGNORE = "." + "doc" + "kerignore"

# Product token only — do not treat "redistribution" / "redistribute" as hits.
_STORE_RE = re.compile(rf"\b{_FORBIDDEN_STORE}\b", re.IGNORECASE)

def test_h3_support_md() -> None:
    text = (ROOT / "SUPPORT.md").read_text()
    assert "SECURITY.md" in text
    assert "native" in text.lower() or "process" in text.lower()

    """Repo must not ship image recipes or orchestrator manifests."""
    assert not (ROOT / _RECIPE).exists()
    assert not (ROOT / _IGNORE).exists()
    for p in ROOT.rglob("*"):
        if ".git" in p.parts or ".venv" in p.parts or "dist" in p.parts:
            continue
        name = p.name.lower()
        assert name != _RECIPE.lower()
        assert name != _IGNORE
        assert not name.endswith("." + _RECIPE.lower())
        if p.is_file() and p.suffix.lower() in {
            ".md",
            ".py",
            ".yml",
            ".yaml",
            ".toml",
            ".sh",
            ".txt",
        }:
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
            lower = text.lower()
            # This test file intentionally mentions split tokens only.
            if p.resolve() == Path(__file__).resolve():
                continue
            assert _ORCH_A not in lower, f"{_ORCH_A} token in {p}"
            assert _ORCH_B not in lower, f"{_ORCH_B} token in {p}"
            assert _ORCH_C not in lower, f"{_ORCH_C} token in {p}"
            assert _STORE_RE.search(text) is None, f"{_FORBIDDEN_STORE} token in {p}"
