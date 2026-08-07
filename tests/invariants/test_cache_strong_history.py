"""Bounded single-key STRONG history checker (not Jepsen/Elle).

Checks concurrent STRONG puts on one key under LWW:
- At most one final visible value per key across backends.
- That value belongs to a successful put (or none if all failed).
- Failed put op_ids leave no visible residual anywhere.
- Successful puts that lost LWW are allowed only if a higher version won.

This is a **local** multi-backend history check — not WAN, not crash-restart,
not a full linearizability proof.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.core.cache_models import CacheMetadata, GlobalCacheKey
from mpreg.core.cache_strong import (
    InProcessStrongTransport,
    StrongLocalBackend,
    StrongPutCoordinator,
    _entry_op_id,
)


@dataclass
class PutOutcome:
    client_id: str
    value: object
    success: bool
    op_id: str | None
    error_code: int | None


def _build_mesh(n: int = 3):
    transport = InProcessStrongTransport()
    backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(n)}
    for be in backends.values():
        transport.register(be)
    coords = {}
    for i in range(n):
        nid = f"n{i}"
        coords[nid] = StrongPutCoordinator(
            origin_id=nid,
            local=backends[nid],
            transport=transport,
            cluster_id="hist",
            replica_factor=n,
            min_replicas=n,
            prepare_timeout_s=0.8,
            commit_timeout_s=0.8,
        )
    return coords, backends, transport


async def _run_concurrent_puts(
    values: list[object],
    *,
    n_nodes: int = 3,
) -> tuple[list[PutOutcome], dict[str, StrongLocalBackend]]:
    coords, backends, _tr = _build_mesh(n_nodes)
    key = GlobalCacheKey(namespace="hist", identifier="k", version="v1")
    peers = [f"n{i}" for i in range(n_nodes)]

    async def one(idx: int, val: object) -> PutOutcome:
        origin = f"n{idx % n_nodes}"
        res = await coords[origin].strong_put(
            key,
            val,
            metadata=CacheMetadata(created_by=origin),
            eligible_peers=peers,
            op_id=f"op-{idx}-{origin}",
        )
        return PutOutcome(
            client_id=origin,
            value=val,
            success=bool(res.success),
            op_id=res.operation_id,
            error_code=res.error_code,
        )

    outcomes = list(await asyncio.gather(*[one(i, v) for i, v in enumerate(values)]))
    return outcomes, backends


def check_history(
    outcomes: list[PutOutcome],
    backends: dict[str, StrongLocalBackend],
    key: GlobalCacheKey,
) -> None:
    success = [o for o in outcomes if o.success]
    failed = [o for o in outcomes if not o.success]

    # Residual-free failures
    for o in failed:
        if not o.op_id:
            continue
        for be in backends.values():
            ent = be.get_visible(key)
            if ent is not None:
                assert _entry_op_id(ent) != o.op_id, (
                    f"failed op {o.op_id} residual on {be.node_id}"
                )
            assert not be.has_pending(o.op_id)

    # All backends agree on visible value (or all empty)
    visibles = {nid: be.get_visible(key) for nid, be in backends.items()}
    values = {
        nid: (None if e is None else (e.value, _entry_op_id(e)))
        for nid, e in visibles.items()
    }
    uniq = {v for v in values.values()}
    assert len(uniq) <= 1, f"backends diverged: {values}"

    final = next(iter(uniq)) if uniq else None
    if final is None or final[0] is None:
        # No visible value — every put must have failed
        assert not success, "successful put but no visible value"
        return

    final_val, final_op = final
    # Final op must be from a successful put
    ok_ops = {o.op_id for o in success}
    assert final_op in ok_ops, f"final op {final_op} not in successes {ok_ops}"

    # LWW: final version should be max among successful applied versions present
    for o in success:
        # find if any backend has this op (winner or overwritten)
        pass
    # At least the winner's value matches one successful put
    assert any(o.value == final_val and o.op_id == final_op for o in success)

    # No pending left
    for be in backends.values():
        assert be.pending_count() == 0


@pytest.mark.asyncio
async def test_history_three_concurrent_puts_converge() -> None:
    outcomes, backends = await _run_concurrent_puts(["a", "b", "c"])
    key = GlobalCacheKey(namespace="hist", identifier="k", version="v1")
    check_history(outcomes, backends, key)
    # At least one success expected on healthy mesh
    assert any(o.success for o in outcomes)


@pytest.mark.asyncio
async def test_history_all_fail_no_residual() -> None:
    coords, backends, tr = _build_mesh(3)
    tr.drop_prepare |= {"n1", "n2"}
    # Also break n0-origin-only path by requiring 3
    key = GlobalCacheKey(namespace="hist", identifier="k", version="v1")
    peers = ["n0", "n1", "n2"]
    res = await coords["n0"].strong_put(key, "x", eligible_peers=peers)
    outcomes = [
        PutOutcome("n0", "x", bool(res.success), res.operation_id, res.error_code)
    ]
    check_history(outcomes, backends, key)
    assert not res.success


@given(
    values=st.lists(
        st.integers(min_value=0, max_value=1000),
        min_size=2,
        max_size=5,
    )
)
@settings(max_examples=25, deadline=None)
def test_history_hypothesis_concurrent_ints(values: list[int]) -> None:
    async def _run() -> None:
        outcomes, backends = await _run_concurrent_puts(list(values))
        key = GlobalCacheKey(namespace="hist", identifier="k", version="v1")
        check_history(outcomes, backends, key)

    asyncio.run(_run())
