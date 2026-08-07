"""T45 residual closeout: product honesty scan + CLI curriculum."""

from __future__ import annotations

from pathlib import Path

# Product-facing paths that must not reintroduce unqualified residual-free /
# auto-heal marketing for STRONG CFT limits.
_PRODUCT_DOC_GLOBS = (
    "docs/MPREG_CLIENT_GUIDE.md",
    "docs/CACHING_SYSTEM.md",
    "docs/ops/STRONG_AND_SHARED_AUDIT_RUNBOOK.md",
    "docs/SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md",
    "docs/examples-curriculum/OPERATE.md",
    "docs/examples-curriculum/FEATURE_CATALOG.md",
    "docs/examples-curriculum/APP_CATALOG.md",
    "mpreg/examples/apps/02_moderate/cache_strong_quorum/README.md",
    "mpreg/examples/apps/02_moderate/ops_cli_tour/README.md",
)

# Phrases that previously caused overclaims (T35/T36).
_BANNED_PHRASES = (
    "residual-free failures",
    "residual-free 1015",
    "residual-free with rollback",
    "guarantees residual-free",
    "always residual-free",
    "automatic residual heal",
    "automatically heals residual",
    "auto-heal residual",
)


def test_t45_product_docs_no_unqualified_residual_free() -> None:
    root = Path(__file__).resolve().parents[2]
    hits: list[str] = []
    for rel in _PRODUCT_DOC_GLOBS:
        path = root / rel
        assert path.is_file(), f"missing product doc: {rel}"
        text = path.read_text(encoding="utf-8")
        lower = text.lower()
        for phrase in _BANNED_PHRASES:
            if phrase in lower:
                # Allow explicit negation nearby (defense in depth)
                idx = lower.find(phrase)
                window = lower[max(0, idx - 40) : idx + len(phrase) + 40]
                if "not " in window or "≠" in window or "never " in window:
                    continue
                hits.append(f"{rel}: {phrase!r}")
    assert not hits, "unqualified residual-free/auto-heal phrases:\n" + "\n".join(hits)


def test_t45_product_docs_qualify_cft_or_ops_retry() -> None:
    root = Path(__file__).resolve().parents[2]
    guide = (root / "docs" / "MPREG_CLIENT_GUIDE.md").read_text(encoding="utf-8")
    assert "cache_strong_retry_abort" in guide or "cache-strong-retry-abort" in guide
    assert "ops_driven" in guide.lower() or "ops-driven" in guide.lower()
    caching = (root / "docs" / "CACHING_SYSTEM.md").read_text(encoding="utf-8")
    assert "not residual-free" in caching.lower() or "cft" in caching.lower()
    assert "cache-strong-retry-abort" in caching or "strong_retry_abort" in caching


def test_t45_ops_cli_curriculum_mentions_retry_abort() -> None:
    root = Path(__file__).resolve().parents[2]
    run_py = (
        root / "mpreg" / "examples" / "apps" / "02_moderate" / "ops_cli_tour" / "run.py"
    ).read_text(encoding="utf-8")
    assert "cache-strong-retry-abort" in run_py
    readme = (
        root
        / "mpreg"
        / "examples"
        / "apps"
        / "02_moderate"
        / "ops_cli_tour"
        / "README.md"
    ).read_text(encoding="utf-8")
    assert "cache-strong-retry-abort" in readme


def test_t45_phase_33_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 33" in text
    assert "T45" in text or "honesty scan" in text.lower()


def test_t45_claims_scanner_non_claim() -> None:
    path = Path(__file__).resolve().parents[2] / "tests" / "invariants" / "claims.yaml"
    text = path.read_text(encoding="utf-8")
    assert "honesty scan" in text.lower() or "product-doc residual" in text.lower()
