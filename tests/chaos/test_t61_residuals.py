"""T61 residual closeout: OpenAPI residual_ops_hint example."""

from __future__ import annotations

from pathlib import Path

from mpreg.server_pkg.openapi_surface import (
    _strong_metrics_schema,
    build_monitoring_openapi,
)

def test_t61_openapi_residual_ops_hint_example() -> None:
    props = _strong_metrics_schema()["properties"]["strong"]["properties"]
    hint = props["residual_ops_hint"]
    ex = hint.get("example") or ""
    assert "cache-strong-retry-abort" in ex
    assert "--namespace orders" in ex
    assert "--key cart-42" in ex
    assert "not auto-heal" in ex
    assert "op-abc123" in ex

def test_t61_openapi_recent_abort_fails_example() -> None:
    props = _strong_metrics_schema()["properties"]["strong"]["properties"]
    raf = props["recent_abort_fails"]
    assert "example" in raf
    ex = raf["example"]
    assert isinstance(ex, list) and ex
    assert ex[0].get("key") == "orders/cart-42"
    assert "op_id" in ex[0]
    items = raf.get("items") or {}
    assert (items.get("properties") or {}).get("key")

def test_t61_full_openapi_doc_includes_example() -> None:
    doc = build_monitoring_openapi()
    blob = str(doc)
    assert "cache-strong-retry-abort" in blob
    assert "orders/cart-42" in blob or "cart-42" in blob

def test_t61_phase_49_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 49" in text

def test_t61_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T61_OPENAPI_HINT_EXAMPLE_PLAN.md"
    ).is_file()
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T61" in ledger
