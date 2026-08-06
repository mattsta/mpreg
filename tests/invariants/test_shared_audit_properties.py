"""Property-based invariants for SharedAudit G-Set + watermarks (INV-SHARED-AUDIT-01).

Hypotheses under test:
  H1  merge is commutative, associative (via fold), and idempotent on same identity
  H2  G-Set insert is monotonic: once present, identity stays until watermark drop
  H3  watermarks are monotonic; below-watermark inserts never resurrect
  H4  anti-entropy (DELTA / digest+PULL) converges all gossip_eligible records
      across N nodes under drop/reorder of DELTA
  H5  cross-cluster_id records are rejected
  H6  gossip_eligible=False never leaves origin via pull/delta filter
"""

from __future__ import annotations

import itertools

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from mpreg.server_pkg.shared_audit import (
    InProcessSharedAuditTransport,
    SharedAuditRecord,
    SharedAuditReplicator,
    SharedAuditStore,
    merge_records,
    record_from_mgmt_entry,
    stable_canonical_json,
)

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_SAFE_TEXT = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="-_"),
    min_size=1,
    max_size=12,
)

@st.composite
def audit_records(draw: st.DrawFn, *, entry_id: str | None = None) -> SharedAuditRecord:
    eid = entry_id or draw(_SAFE_TEXT.map(lambda s: f"e-{s}"))
    return SharedAuditRecord(
        schema_version=1,
        entry_id=eid,
        cluster_id="c1",
        origin_node=draw(st.sampled_from(["n0", "n1", "n2", "n3"])),
        origin_url=draw(_SAFE_TEXT.map(lambda s: f"ws://{s}")),
        event=draw(st.sampled_from(["node_drain", "node_detach", "policy_apply", "x"])),
        timestamp=draw(
            st.floats(
                min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False
            )
        ),
        actor=draw(st.one_of(st.none(), _SAFE_TEXT)),
        success=draw(st.booleans()),
        detail=draw(
            st.fixed_dictionaries(
                {
                    "k": st.integers(min_value=-100, max_value=100),
                }
            )
        ),
        gossip_eligible=True,
    )

def _same_id_variants(base: SharedAuditRecord, n: int = 3) -> list[SharedAuditRecord]:
    """Build n same-identity records with differing detail for conflict merge."""
    out = []
    for i in range(n):
        out.append(
            SharedAuditRecord(
                schema_version=base.schema_version,
                entry_id=base.entry_id,
                cluster_id=base.cluster_id,
                origin_node=base.origin_node,
                origin_url=base.origin_url,
                event=base.event,
                timestamp=base.timestamp,
                actor=base.actor,
                success=base.success,
                detail={"variant": i, "pad": "x" * i},
                gossip_eligible=True,
            )
        )
    return out

# ---------------------------------------------------------------------------
# H1 — merge lattice
# ---------------------------------------------------------------------------

@given(base=audit_records())
@settings(max_examples=80, deadline=None)
def test_merge_idempotent(base: SharedAuditRecord) -> None:
    w, c = merge_records(base, base)
    assert c is False
    assert stable_canonical_json(w.payload_for_merge()) == stable_canonical_json(
        base.payload_for_merge()
    )

@given(base=audit_records())
@settings(max_examples=60, deadline=None)
def test_merge_commutative_on_conflicts(base: SharedAuditRecord) -> None:
    a, b = _same_id_variants(base, 2)
    w1, c1 = merge_records(a, b)
    w2, c2 = merge_records(b, a)
    assert c1 is c2
    assert stable_canonical_json(w1.payload_for_merge()) == stable_canonical_json(
        w2.payload_for_merge()
    )

@given(base=audit_records())
@settings(max_examples=40, deadline=None)
def test_merge_associative_fold(base: SharedAuditRecord) -> None:
    variants = _same_id_variants(base, 3)
    winners: list[str] = []
    for order in itertools.permutations(variants):
        acc = order[0]
        for nxt in order[1:]:
            acc, _ = merge_records(acc, nxt)
        winners.append(stable_canonical_json(acc.payload_for_merge()))
    assert len(set(winners)) == 1

@given(a=audit_records(), b=audit_records())
@settings(max_examples=40, deadline=None)
def test_merge_rejects_distinct_identity(
    a: SharedAuditRecord, b: SharedAuditRecord
) -> None:
    assume(a.entry_id != b.entry_id)
    with pytest.raises(ValueError, match="distinct identities"):
        merge_records(a, b)

# ---------------------------------------------------------------------------
# H2 / H3 — store monotonicity + watermark anti-resurrection
# ---------------------------------------------------------------------------

@given(
    records=st.lists(audit_records(), min_size=1, max_size=25),
    max_entries=st.integers(min_value=3, max_value=15),
)
@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_store_size_bounded_and_watermarks_monotonic(
    records: list[SharedAuditRecord], max_entries: int
) -> None:
    store = SharedAuditStore(max_entries=max_entries, cluster_id="c1")
    prev_wm: dict[str, tuple[float, str]] = {}
    for r in records:
        store.insert(r)
        assert store.size() <= max_entries
        for origin, wm in store.watermarks_snapshot().items():
            key = wm.sort_key()
            if origin in prev_wm:
                assert key >= prev_wm[origin], (
                    "watermark must be monotonic non-decreasing"
                )
            prev_wm[origin] = key
        # Every retained record for an origin is >= that origin's watermark
        for origin, wm in store.watermarks_snapshot().items():
            for kept in store.snapshot(origin_node=origin):
                assert wm.covers(kept.timestamp, kept.entry_id)

@given(
    seed=st.lists(audit_records(), min_size=5, max_size=20),
    max_entries=st.integers(min_value=2, max_value=6),
)
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_no_resurrection_below_watermark(
    seed: list[SharedAuditRecord], max_entries: int
) -> None:
    # Force single origin so compaction advances one watermark
    fixed = [
        SharedAuditRecord(
            schema_version=1,
            entry_id=f"id-{i}-{r.entry_id}",
            cluster_id="c1",
            origin_node="n0",
            origin_url="ws://n0",
            event=r.event,
            timestamp=float(i),
            actor=r.actor,
            success=r.success,
            detail=dict(r.detail),
            gossip_eligible=True,
        )
        for i, r in enumerate(seed)
    ]
    store = SharedAuditStore(max_entries=max_entries, cluster_id="c1")
    for r in fixed:
        store.insert(r)
    wm = store.watermark_for("n0")
    assume(wm is not None)
    assert wm is not None
    # Re-insert every original; those below watermark must stay out
    for r in fixed:
        store.insert(r)
    for r in fixed:
        if not wm.covers(r.timestamp, r.entry_id):
            assert store.get("c1", r.entry_id) is None

@given(r=audit_records())
@settings(max_examples=30, deadline=None)
def test_cross_cluster_rejected(r: SharedAuditRecord) -> None:
    store = SharedAuditStore(cluster_id="home")
    foreign = SharedAuditRecord(
        schema_version=r.schema_version,
        entry_id=r.entry_id,
        cluster_id="other-cluster",
        origin_node=r.origin_node,
        origin_url=r.origin_url,
        event=r.event,
        timestamp=r.timestamp,
        actor=r.actor,
        success=r.success,
        detail=dict(r.detail),
        gossip_eligible=True,
    )
    assert store.insert(foreign) is None
    assert store.size() == 0
    assert store.rejected_cross_cluster >= 1

# ---------------------------------------------------------------------------
# H4 / H6 — anti-entropy convergence (async property)
# ---------------------------------------------------------------------------

def _mesh(
    n: int,
) -> tuple[
    InProcessSharedAuditTransport, list[SharedAuditStore], list[SharedAuditReplicator]
]:
    transport = InProcessSharedAuditTransport()
    stores: list[SharedAuditStore] = []
    reps: list[SharedAuditReplicator] = []
    for i in range(n):
        nid = f"n{i}"
        store = SharedAuditStore(cluster_id="c1", local_node=nid, max_entries=500)

        def peers(me: str = nid) -> list[str]:
            return [p for p in transport.peers if p != me]

        rep = SharedAuditReplicator(
            store=store,
            node_id=nid,
            cluster_id="c1",
            transport=transport,
            peer_list=peers,
            reconcile_interval_s=60.0,
            gossip_targets=n,
        )
        transport.register(rep)
        stores.append(store)
        reps.append(rep)
    return transport, stores, reps

@pytest.mark.asyncio
@given(
    payloads=st.lists(
        st.tuples(
            st.sampled_from(["node_drain", "detach", "policy"]),
            st.floats(
                min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False
            ),
            st.booleans(),
        ),
        min_size=1,
        max_size=12,
    ),
    drop_delta=st.booleans(),
)
@settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_property_mesh_converges_eligible(
    payloads: list[tuple[str, float, bool]], drop_delta: bool
) -> None:
    transport, stores, reps = _mesh(3)
    origin, peers = reps[0], reps[1:]
    if drop_delta:
        transport.drop_types.add("mgmt_audit_delta")

    expected_ids: set[str] = set()
    for i, (event, ts, ok) in enumerate(payloads):
        rec = record_from_mgmt_entry(
            event=event,
            timestamp=float(ts) + i * 0.001,
            actor="prop",
            success=ok,
            detail={"i": i},
            cluster_id="c1",
            origin_node="n0",
            origin_url="ws://n0",
            entry_id=f"prop-{i}",
        )
        stores[0].insert(rec)
        origin.publish(rec)
        expected_ids.add(rec.entry_id)

    await origin._flush_outbound()

    if drop_delta:
        # Repair via digest/PULL from each peer
        transport.drop_types.clear()
        digest = origin.build_digest()
        for peer in peers:
            await peer._on_digest(digest)
    else:
        # Extra digest round for completeness
        await origin._exchange_digests()

    for store in stores:
        have = {r.entry_id for r in store.snapshot(gossip_eligible_only=True)}
        missing = expected_ids - have
        assert not missing, f"node {store.local_node} missing {missing}"

@pytest.mark.asyncio
async def test_property_legacy_never_gossips() -> None:
    _transport, stores, reps = _mesh(2)
    legacy = SharedAuditRecord(
        schema_version=1,
        entry_id="legacy:deadbeef",
        cluster_id="c1",
        origin_node="n0",
        origin_url="ws://n0",
        event="old",
        timestamp=1.0,
        actor=None,
        success=True,
        detail={},
        gossip_eligible=False,
    )
    stores[0].insert(legacy)
    reps[0].publish(legacy)
    await reps[0]._flush_outbound()
    # Even after digest pull, peer must not receive non-gossip
    await reps[1]._on_digest(reps[0].build_digest())
    assert stores[1].get("c1", "legacy:deadbeef") is None
    pulled = stores[0].records_for_pull(requester_watermarks={}, limit=100)
    assert all(r.gossip_eligible for r in pulled)
    assert all(r.entry_id != "legacy:deadbeef" for r in pulled)

@pytest.mark.asyncio
async def test_reorder_epidemic_still_converges() -> None:
    transport, stores, reps = _mesh(3)
    transport.hold_reorder = True
    ids = []
    for i in range(5):
        rec = record_from_mgmt_entry(
            event="e",
            timestamp=float(i),
            actor=None,
            success=True,
            detail={"i": i},
            cluster_id="c1",
            origin_node="n0",
            entry_id=f"ro-{i}",
        )
        stores[0].insert(rec)
        reps[0].publish(rec)
        await reps[0]._flush_outbound()
        ids.append(rec.entry_id)
    await transport.flush_reorder()
    for store in stores[1:]:
        for eid in ids:
            assert store.get("c1", eid) is not None

# ---------------------------------------------------------------------------
# Response contract
# ---------------------------------------------------------------------------

@given(limit=st.integers(min_value=0, max_value=20))
@settings(max_examples=20, deadline=None)
def test_cluster_scope_requires_shared(limit: int) -> None:
    from mpreg.server_pkg.shared_audit import build_audit_response

    out = build_audit_response(
        store=None,
        local_entries=[{"event": "x", "timestamp": 1.0}],
        scope="cluster",
        shared_enabled=False,
        limit=limit,
    )
    assert out.get("error") == "shared_audit_disabled"
    assert out["mutations"] == []

@given(
    n=st.integers(min_value=0, max_value=30),
    limit=st.integers(min_value=0, max_value=15),
)
@settings(max_examples=30, deadline=None)
def test_local_scope_respects_limit(n: int, limit: int) -> None:
    from mpreg.server_pkg.shared_audit import build_audit_response

    local = [{"event": f"e{i}", "timestamp": float(i)} for i in range(n)]
    out = build_audit_response(
        store=None,
        local_entries=local,
        scope="local",
        limit=limit,
        shared_enabled=False,
    )
    assert out["scope"] == "local"
    if limit >= 0:
        assert len(out["mutations"]) <= limit
        assert out["mutation_count"] == len(out["mutations"])

# ---------------------------------------------------------------------------
# T15 — partition isolate + heal converges (DistLab Hypothesis expand)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(n_events=st.integers(min_value=1, max_value=8))
@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
async def test_partition_pair_heal_converges(n_events: int) -> None:
    """Isolate n1 from n0 and n2 while n0 publishes; heal + digest → equal G-Sets."""
    transport, stores, reps = _mesh(3)
    # Fully isolate n1
    transport.partition("n0", "n1")
    transport.partition("n1", "n2")

    expected: set[str] = set()
    for i in range(n_events):
        rec = record_from_mgmt_entry(
            event=f"e{i}",
            timestamp=float(i + 1),
            actor="prop",
            success=True,
            detail={"i": i},
            cluster_id="c1",
            origin_node="n0",
            origin_url="ws://n0",
            entry_id=f"part-{i}",
        )
        stores[0].insert(rec)
        reps[0].publish(rec)
        expected.add(rec.entry_id)

    await reps[0]._flush_outbound()
    n1_ids = {r.entry_id for r in stores[1].snapshot(gossip_eligible_only=True)}
    assert expected.isdisjoint(n1_ids), n1_ids

    transport.heal()
    # Multi-round digest anti-entropy after heal
    for _ in range(3):
        for r in reps:
            d = r.build_digest()
            for other in reps:
                if other is r:
                    continue
                await other._on_digest(d)

    for store in stores:
        have = {r.entry_id for r in store.snapshot(gossip_eligible_only=True)}
        missing = expected - have
        assert not missing, f"node {store.local_node} missing {missing}"
