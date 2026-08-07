"""T59 residual closeout: residual_ops_hint enriches ns/key from recent fails."""

from __future__ import annotations

from pathlib import Path

import pytest

from mpreg.cli.main import strong_residual_ops_hint
from mpreg.core.cache_models import GlobalCacheKey
from mpreg.core.cache_strong import (
    InProcessStrongTransport,
    StrongLocalBackend,
    StrongPutCoordinator,
    format_residual_ops_hint,
)
from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager

def test_t59_format_enriches_from_recent() -> None:
    h = format_residual_ops_hint(
        ["n1"],
        "oid-1",
        recent_abort_fails=[
            {"op_id": "oid-1", "peers": ["n1"], "key": "orders/cart-42"}
        ],
    )
    assert "--namespace orders" in h
    assert "--key cart-42" in h
    assert "--op-id oid-1" in h
    assert "--peer n1" in h

def test_t59_format_explicit_ns_wins() -> None:
    h = format_residual_ops_hint(
        ["n1"],
        "oid-1",
        namespace="explicit",
        key_id="k",
        recent_abort_fails=[
            {"op_id": "oid-1", "peers": ["n1"], "key": "other/x"}
        ],
    )
    assert "--namespace explicit" in h
    assert "--key k" in h

def test_t59_doctor_enriches_placeholders() -> None:
    body = {
        "last_abort_fail_peers": ["n1"],
        "last_abort_fail_op_id": "oid-z",
        "residual_ops_hint": (
            "hint: after network recovery, ops re-ABORT (not auto-heal): "
            "uv run mpreg client cache-strong-retry-abort --url <ws> "
            "--op-id oid-z --namespace <ns> --key <id> --peer n1 "
            "(CFT best-effort; still fails while ABORT dropped)"
        ),
        "recent_abort_fails": [
            {"op_id": "oid-z", "peers": ["n1"], "key": "nsA/idB"}
        ],
    }
    h = strong_residual_ops_hint(body)
    assert "--namespace nsA" in h
    assert "--key idB" in h

@pytest.mark.asyncio
async def test_t59_gcm_status_enriched_after_cft() -> None:
    tr = InProcessStrongTransport()
    backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(5)}
    for be in backends.values():
        tr.register(be)
    tr.drop_commit |= {"n2", "n3", "n4"}
    tr.drop_abort |= {"n1"}
    coord = StrongPutCoordinator(
        origin_id="n0",
        local=backends["n0"],
        transport=tr,
        replica_factor=5,
        min_replicas=5,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.2,
        abort_attempts=2,
    )
    gcm = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
            local_cluster_id="t59",
        )
    )
    gcm.attach_strong_coordinator(coord)
    try:
        key = GlobalCacheKey(namespace="shop", identifier="sku-9", version="v1")
        res = await coord.strong_put(key, {"v": 1}, eligible_peers=list(backends))
        assert res.success is False
        st = gcm.strong_status()
        hint = st.get("residual_ops_hint") or ""
        assert "cache-strong-retry-abort" in hint
        assert "--namespace shop" in hint
        assert "--key sku-9" in hint
        assert res.operation_id in hint or "--op-id" in hint
    finally:
        await gcm.shutdown()

def test_t59_phase_47_honesty() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    assert "Phase 47" in text
    assert "recent_abort_fails" in text

def test_t59_plan_and_ledger() -> None:
    root = Path(__file__).resolve().parents[2]
    assert (
        root / "docs" / "plans" / "DISTLAB_T59_HINT_KEY_ENRICH_PLAN.md"
    ).is_file()
    ledger = (root / "docs" / "plans" / "DISTLAB_PROOF_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "T59" in ledger

def test_t59_claims() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "invariants"
        / "claims.yaml"
    )
    text = path.read_text(encoding="utf-8")
    assert "recent_abort_fails" in text or "key enrichment" in text.lower()
