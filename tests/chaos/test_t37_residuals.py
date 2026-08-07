"""T37 residual closeout: retry_abort clears CFT residual when ABORT can land."""

from __future__ import annotations

from pathlib import Path

import pytest

from mpreg.core.cache_models import GlobalCacheKey
from mpreg.core.cache_strong import (
    InProcessStrongTransport,
    StrongLocalBackend,
    StrongPutCoordinator,
    _entry_op_id,
)
from mpreg.testing.distlab.builtins import ensure_builtins
from mpreg.testing.distlab.registry import get_registry, resolve_preset


def _cft_residual_cluster() -> tuple[
    StrongPutCoordinator,
    InProcessStrongTransport,
    dict[str, StrongLocalBackend],
]:
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
    return coord, tr, backends


@pytest.mark.asyncio
async def test_t37_retry_abort_clears_residual_when_network_recovers() -> None:
    coord, tr, backends = _cft_residual_cluster()
    key = GlobalCacheKey(namespace="t37", identifier="k", version="v1")
    res = await coord.strong_put(key, {"v": 1}, eligible_peers=list(backends))
    assert res.success is False
    oid = res.operation_id or ""
    assert backends["n1"].get_visible(key) is not None
    assert _entry_op_id(backends["n1"].get_visible(key)) == oid
    assert "n1" in coord.last_abort_fail_peers

    tr.drop_abort.clear()
    tr.drop_commit.clear()
    out = await coord.retry_abort(key, oid)
    assert out.get("cleared") is True, out
    assert "n1" in list(out.get("ok_peers") or [])
    assert list(out.get("fail_peers") or []) == []
    assert coord.last_abort_fail_peers == []
    ent = backends["n1"].get_visible(key)
    assert ent is None or _entry_op_id(ent) != oid


@pytest.mark.asyncio
async def test_t37_retry_abort_still_fails_while_drop_abort() -> None:
    """Honesty: retry while still dropping ABORT does not clear residual."""
    coord, _tr, backends = _cft_residual_cluster()
    key = GlobalCacheKey(namespace="t37", identifier="still", version="v1")
    res = await coord.strong_put(key, {"v": 1}, eligible_peers=list(backends))
    assert res.success is False
    oid = res.operation_id or ""
    # Keep drop_abort on n1
    out = await coord.retry_abort(key, oid, peers=["n1"])
    assert out.get("cleared") is False
    assert "n1" in list(out.get("fail_peers") or [])
    assert "n1" in coord.last_abort_fail_peers
    ent = backends["n1"].get_visible(key)
    assert ent is not None and _entry_op_id(ent) == oid


@pytest.mark.asyncio
async def test_t37_retry_abort_empty_peers_noop() -> None:
    coord, _tr, _backends = _cft_residual_cluster()
    key = GlobalCacheKey(namespace="t37", identifier="empty", version="v1")
    out = await coord.retry_abort(key, "no-op-id", peers=[])
    assert out.get("ok_peers") == []
    assert out.get("fail_peers") == []
    assert out.get("attempts") == 0


@pytest.mark.asyncio
async def test_t37_retry_abort_self_target_clears_local() -> None:
    """RPC may land on residual peer: peers=[self] must local-abort, not no-op."""
    tr = InProcessStrongTransport()
    backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(3)}
    for be in backends.values():
        tr.register(be)
    # Residual lives on n1; coordinate *as* n1 (simulates RPC fan-in to residual)
    coord = StrongPutCoordinator(
        origin_id="n1",
        local=backends["n1"],
        transport=tr,
        replica_factor=3,
        min_replicas=2,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.2,
        abort_attempts=2,
    )
    key = GlobalCacheKey(namespace="t37", identifier="self", version="v1")
    from mpreg.core.cache_models import CacheMetadata
    from mpreg.core.cache_strong import StrongVersion

    oid = "self-residual"
    sv = StrongVersion(logical_ts=1, origin_node="n0", op_id=oid)
    await backends["n1"].prepare(
        key=key,
        value={"stale": True},
        metadata=CacheMetadata(),
        strong_version=sv,
        replica_set=("n0", "n1", "n2"),
        quorum=2,
        ttl_s=30.0,
    )
    await backends["n1"].commit(op_id=oid, key=key)
    assert backends["n1"].get_visible(key) is not None

    out = await coord.retry_abort(key, oid, peers=["n1"])
    assert out.get("cleared") is True, out
    assert "n1" in list(out.get("ok_peers") or [])
    ent = backends["n1"].get_visible(key)
    assert ent is None or _entry_op_id(ent) != oid


@pytest.mark.asyncio
async def test_t37_distlab_retry_abort_scenario() -> None:
    ensure_builtins()
    r = await get_registry().run("strong.cft_retry_abort_clears_residual")
    assert r.ok, r
    assert (r.meta or {}).get("product_fix") is True


def test_t37_preset_includes_retry_abort() -> None:
    ensure_builtins()
    assert "strong.cft_retry_abort_clears_residual" in resolve_preset("strong-core")
    assert "strong.cft_retry_abort_clears_residual" in resolve_preset("ci-core")


def test_t37_residual_honesty_phase() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    )
    text = path.read_text(encoding="utf-8")
    lower = text.lower()
    assert "phase 25" in lower or "retry_abort" in lower
    assert "best-effort" in lower
    assert "automatic background heal" in lower or "not automatic" in lower


def test_t37_claims_retry_abort_non_claim() -> None:
    path = Path(__file__).resolve().parents[2] / "tests" / "invariants" / "claims.yaml"
    text = path.read_text(encoding="utf-8")
    assert "retry_abort" in text
