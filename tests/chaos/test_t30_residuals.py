"""T30 residual closeout: orphan pre-commit backup GC (product fix)."""

from __future__ import annotations

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


@pytest.mark.asyncio
async def test_t30_orphan_backups_pruned_on_repeated_cft_residual() -> None:
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
    key = GlobalCacheKey(namespace="t30", identifier="k", version="v1")
    peers = list(backends)
    for i in range(8):
        res = await coord.strong_put(key, {"i": i}, eligible_peers=peers)
        assert res.success is False
        # Live backups: at most one (current residual op with prior value)
        assert backends["n1"].backups_count() <= 1
        assert backends["n1"].get_visible(key) is not None

    tr.drop_commit.clear()
    tr.drop_abort.clear()
    ok = await coord.strong_put(key, {"healed": True}, eligible_peers=peers)
    assert ok.success is True
    assert backends["n1"].backups_count() <= 1
    ent = backends["n1"].get_visible(key)
    assert ent is not None and _entry_op_id(ent) == ok.operation_id
    assert ent.value == {"healed": True}


@pytest.mark.asyncio
async def test_t30_distlab_orphan_backup_gc_scenario() -> None:
    ensure_builtins()
    r = await get_registry().run("strong.cft_orphan_backup_gc")
    assert r.ok, r
    assert (r.meta or {}).get("product_fix") is True


def test_t30_preset_includes_orphan_backup_gc() -> None:
    ensure_builtins()
    assert "strong.cft_orphan_backup_gc" in resolve_preset("strong-core")
    assert "strong.cft_orphan_backup_gc" in resolve_preset("ci-core")


@pytest.mark.asyncio
async def test_t30_abort_still_uncommits_current_residual() -> None:
    """Backup for the *current* residual op must still enable ABORT uncommit."""
    tr = InProcessStrongTransport()
    backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(5)}
    for be in backends.values():
        tr.register(be)
    # First put succeeds so n1 has a real prior value for backup
    coord = StrongPutCoordinator(
        origin_id="n0",
        local=backends["n0"],
        transport=tr,
        replica_factor=5,
        min_replicas=5,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.2,
    )
    key = GlobalCacheKey(namespace="t30", identifier="u", version="v1")
    peers = list(backends)
    ok1 = await coord.strong_put(key, {"v": 1}, eligible_peers=peers)
    assert ok1.success is True
    # Fail: only n1 applies peer COMMIT; Q=3 needs 2 peers → fail; lose ABORT on n1
    tr.drop_commit |= {"n2", "n3", "n4"}
    tr.drop_abort |= {"n1"}
    fail = await coord.strong_put(key, {"v": 2}, eligible_peers=peers)
    assert fail.success is False
    oid = fail.operation_id or ""
    n1 = backends["n1"]
    assert n1.get_visible(key) is not None
    assert _entry_op_id(n1.get_visible(key)) == oid
    assert n1.backups_count() == 1  # prior value kept for this residual op
    # Manual ABORT delivery (simulating late / repaired abort)
    await n1.abort(op_id=oid, key=key)
    restored = n1.get_visible(key)
    assert restored is not None
    assert restored.value == {"v": 1}
    assert n1.backups_count() == 0
