"""INV-P7: Federated announcement / RPC u-id dedup under retry."""

from __future__ import annotations

import time

from mpreg.datastructures.federated_types import (
    FederatedAnnouncementTracker,
    FederatedRPCAnnouncement,
)

def test_announcement_dedup_same_id() -> None:
    tracker = FederatedAnnouncementTracker(ttl_seconds=60.0)
    ann = FederatedRPCAnnouncement.create_initial(
        functions=("echo",),
        resources=(),
        cluster_id="c1",
        original_source="ws://origin:1",
        max_hops=3,
    )
    local = "ws://local:1"
    assert ann.should_process(local, tracker) is True
    tracker.mark_seen(ann.propagation.announcement_id, time.time())
    # Retry / duplicate of same announcement_id must not re-process
    assert ann.should_process(local, tracker) is False
    # Forwarded copy shares announcement_id → still deduped
    fwd = ann.create_forwarded()
    assert fwd.propagation.announcement_id == ann.propagation.announcement_id
    assert fwd.should_process(local, tracker) is False

def test_distinct_announcement_ids_process_independently() -> None:
    tracker = FederatedAnnouncementTracker(ttl_seconds=60.0)
    a1 = FederatedRPCAnnouncement.create_initial(
        functions=("f1",),
        resources=(),
        cluster_id="c1",
        original_source="ws://o1:1",
    )
    a2 = FederatedRPCAnnouncement.create_initial(
        functions=("f2",),
        resources=(),
        cluster_id="c2",
        original_source="ws://o2:1",
    )
    assert a1.propagation.announcement_id != a2.propagation.announcement_id
    local = "ws://local:1"
    assert a1.should_process(local, tracker)
    tracker.mark_seen(a1.propagation.announcement_id, time.time())
    assert a2.should_process(local, tracker)

def test_hop_limit_stops_forwarding() -> None:
    tracker = FederatedAnnouncementTracker(ttl_seconds=60.0)
    ann = FederatedRPCAnnouncement.create_initial(
        functions=("echo",),
        resources=(),
        cluster_id="c1",
        original_source="ws://origin:1",
        max_hops=1,
    )
    # hop 0 can process; after forward hop_count=1 cannot forward further
    assert ann.can_forward is True
    fwd = ann.create_forwarded()
    assert fwd.propagation.hop_count == 1
    assert fwd.can_forward is False
    # Exhausted hop budget ⇒ should_process False even if unseen
    assert fwd.should_process("ws://remote:1", tracker) is False

def test_tracker_ttl_expiry_allows_reprocess() -> None:
    tracker = FederatedAnnouncementTracker(ttl_seconds=5.0)
    ann = FederatedRPCAnnouncement.create_initial(
        functions=("echo",),
        resources=(),
        cluster_id="c1",
        original_source="ws://origin:1",
    )
    now = 1000.0
    tracker.mark_seen(ann.propagation.announcement_id, now)
    assert ann.should_process("ws://x:1", tracker) is False
    removed = tracker.cleanup_expired(now + 10.0)
    assert removed >= 1
    assert ann.should_process("ws://x:1", tracker) is True
