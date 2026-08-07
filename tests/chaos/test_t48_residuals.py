"""T48 residual closeout: OpenAPI platform cache RPC catalog."""

from __future__ import annotations

from pathlib import Path

from mpreg.core.rpc_naming import PlatformRpc
from mpreg.server_pkg.openapi_surface import build_monitoring_openapi


def test_t48_openapi_platform_cache_rpc_catalog() -> None:
    doc = build_monitoring_openapi()
    schemas = (doc.get("components") or {}).get("schemas") or {}
    assert "PlatformCacheRpcCatalog" in schemas
    cat = schemas["PlatformCacheRpcCatalog"]
    cmds = cat["properties"]["commands"]["properties"]
    assert "strong_retry_abort" in cmds
    fqn = cmds["strong_retry_abort"]["properties"]["fqn"]["enum"]
    assert PlatformRpc.CACHE_STRONG_RETRY_ABORT in fqn
    honesty = cmds["strong_retry_abort"]["properties"]["result_honesty"]["properties"]
    assert honesty["ops_driven"]["enum"] == [True]
    assert honesty["automatic_heal"]["enum"] == [False]
    assert honesty["cft_best_effort"]["enum"] == [True]
    # All cache plane commands present
    for name in ("get", "put", "invalidate", "strong_retry_abort"):
        assert name in cmds
    assert cmds["get"]["properties"]["fqn"]["enum"] == [PlatformRpc.CACHE_GET]
    assert cmds["put"]["properties"]["fqn"]["enum"] == [PlatformRpc.CACHE_PUT]
    assert cmds["invalidate"]["properties"]["fqn"]["enum"] == [
        PlatformRpc.CACHE_INVALIDATE
    ]


def test_t48_openapi_tag_and_x_extension() -> None:
    doc = build_monitoring_openapi()
    tags = {t["name"] for t in doc.get("tags") or []}
    assert "platform-rpc" in tags
    xrpc = (doc.get("components") or {}).get("x-mpreg-platform-rpc") or {}
    assert "cache" in xrpc
    desc = cat_desc = ""
    for t in doc.get("tags") or []:
        if t.get("name") == "strong":
            desc = (t.get("description") or "").lower()
        if t.get("name") == "platform-rpc":
            cat_desc = (t.get("description") or "").lower()
    assert "strong_retry_abort" in desc or "retry" in desc
    assert "not http" in cat_desc or "wire" in cat_desc or "rpc" in cat_desc


def test_t48_catalog_honesty_text() -> None:
    doc = build_monitoring_openapi()
    cat = doc["components"]["schemas"]["PlatformCacheRpcCatalog"]
    blob = str(cat).lower()
    assert "not automatic" in blob or "ops-driven" in blob
    assert "not bft" in blob or "cft" in blob


def test_t48_phase_36_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 36" in text
    assert "PlatformCacheRpcCatalog" in text or "platform-rpc" in text.lower()


def test_t48_runbook_openapi_catalog() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "ops"
        / "STRONG_AND_SHARED_AUDIT_RUNBOOK.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "PlatformCacheRpcCatalog" in text or "strong_retry_abort" in text
