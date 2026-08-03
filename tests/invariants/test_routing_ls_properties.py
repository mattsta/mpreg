"""A3: Link-state TTL, sequence, area (INV-R4, R5, R6)."""

from __future__ import annotations

import time

from mpreg.fabric.link_state import (
    LinkStateAreaPolicy,
    LinkStateNeighbor,
    LinkStateTable,
    LinkStateUpdate,
)

def test_expired_update_rejected() -> None:
    table = LinkStateTable(local_cluster="a")
    now = 1000.0
    upd = LinkStateUpdate(
        origin="b",
        neighbors=(LinkStateNeighbor(cluster_id="c"),),
        advertised_at=now - 100,
        ttl_seconds=10.0,
        sequence=1,
    )
    assert table.apply_update(upd, now=now) is False

def test_stale_sequence_rejected() -> None:
    table = LinkStateTable(local_cluster="a")
    now = time.time()
    n = (LinkStateNeighbor(cluster_id="c"),)
    assert table.apply_update(
        LinkStateUpdate(origin="b", neighbors=n, advertised_at=now, sequence=5),
        now=now,
    )
    assert (
        table.apply_update(
            LinkStateUpdate(origin="b", neighbors=n, advertised_at=now + 1, sequence=4),
            now=now + 1,
        )
        is False
    )

def test_purge_expired_removes_spf_inputs() -> None:
    table = LinkStateTable(local_cluster="a")
    now = 100.0
    table.apply_update(
        LinkStateUpdate(
            origin="b",
            neighbors=(LinkStateNeighbor(cluster_id="c"),),
            advertised_at=now,
            ttl_seconds=5.0,
            sequence=1,
        ),
        now=now,
    )
    assert table.has_origin("b")
    purged = table.purge_expired(now=now + 10)
    assert purged
    assert not table.has_origin("b")

def test_newer_sequence_accepted() -> None:
    table = LinkStateTable(local_cluster="a")
    now = time.time()
    n1 = (LinkStateNeighbor(cluster_id="c"),)
    n2 = (LinkStateNeighbor(cluster_id="d"),)
    assert table.apply_update(
        LinkStateUpdate(origin="b", neighbors=n1, advertised_at=now, sequence=1),
        now=now,
    )
    assert table.apply_update(
        LinkStateUpdate(origin="b", neighbors=n2, advertised_at=now + 1, sequence=2),
        now=now + 1,
    )
    assert table.neighbor_clusters("b") == frozenset({"d"})

def test_area_policy_exists() -> None:
    # Smoke: policy type is constructible for area isolation tests.
    policy = LinkStateAreaPolicy()
    assert policy is not None
