import pytest

from mpreg.fabric.route_announcer import RouteAnnouncementProcessor
from mpreg.fabric.route_control import (
    RouteAnnouncement,
    RouteDestination,
    RouteMetrics,
    RoutePath,
    RoutePolicy,
    RouteTable,
    RouteWithdrawal,
)
from mpreg.fabric.route_keys import RouteKeyRegistry
from mpreg.fabric.route_security import (
    RouteAnnouncementSigner,
    RouteSecurityConfig,
    verify_route_announcement,
    verify_route_withdrawal,
)


def _base_announcement() -> RouteAnnouncement:
    return RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-c"),
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(hop_count=1, latency_ms=5.0),
        advertiser="cluster-b",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
    )


def _base_withdrawal() -> RouteWithdrawal:
    return RouteWithdrawal(
        destination=RouteDestination(cluster_id="cluster-c"),
        path=RoutePath(("cluster-b", "cluster-c")),
        advertiser="cluster-b",
        withdrawn_at=101.0,
        epoch=2,
    )


def test_route_announcement_signature_roundtrip() -> None:
    signer = RouteAnnouncementSigner.create()
    announcement = _base_announcement()
    signed = signer.sign(announcement)

    assert signed.signature
    assert signed.public_key == signer.public_key
    assert verify_route_announcement(signed, public_key=signer.public_key)

    restored = RouteAnnouncement.from_dict(signed.to_dict())
    assert verify_route_announcement(restored, public_key=signer.public_key)


def test_route_withdrawal_signature_roundtrip() -> None:
    signer = RouteAnnouncementSigner.create()
    withdrawal = _base_withdrawal()
    signed = signer.sign_withdrawal(withdrawal)

    assert signed.signature
    assert signed.public_key == signer.public_key
    assert verify_route_withdrawal(signed, public_key=signer.public_key)

    restored = RouteWithdrawal.from_dict(signed.to_dict())
    assert verify_route_withdrawal(restored, public_key=signer.public_key)


@pytest.mark.asyncio
async def test_route_processor_requires_signature() -> None:
    table = RouteTable(local_cluster="cluster-a")
    signer = RouteAnnouncementSigner.create()
    processor = RouteAnnouncementProcessor(
        local_cluster="cluster-a",
        route_table=table,
        sender_cluster_resolver=lambda _: "cluster-b",
        security_config=RouteSecurityConfig(
            require_signatures=True,
            allow_unsigned=False,
            signature_algorithm=signer.algorithm,
        ),
        public_key_resolver=lambda cluster_id: (
            signer.public_key if cluster_id == "cluster-b" else None
        ),
    )

    signed = signer.sign(_base_announcement())
    updated = await processor.handle_announcement(signed, sender_id="node-b", now=100.0)
    assert updated is True

    unsigned = _base_announcement()
    updated = await processor.handle_announcement(
        unsigned, sender_id="node-b", now=100.0
    )
    assert updated is False

    withdrawal = signer.sign_withdrawal(_base_withdrawal())
    updated = await processor.handle_withdrawal(
        withdrawal, sender_id="node-b", now=102.0
    )
    assert updated is True

    unsigned_withdrawal = _base_withdrawal()
    updated = await processor.handle_withdrawal(
        unsigned_withdrawal, sender_id="node-b", now=102.0
    )
    assert updated is False


@pytest.mark.asyncio
async def test_route_processor_accepts_rotated_keys() -> None:
    table = RouteTable(local_cluster="cluster-a")
    signer_old = RouteAnnouncementSigner.create()
    signer_new = RouteAnnouncementSigner.create()
    registry = RouteKeyRegistry()
    registry.register_key(
        cluster_id="cluster-b", public_key=signer_old.public_key, now=100.0
    )
    registry.rotate_key(
        cluster_id="cluster-b",
        public_key=signer_new.public_key,
        overlap_seconds=20.0,
        now=120.0,
    )

    now_state = {"now": 130.0}
    processor = RouteAnnouncementProcessor(
        local_cluster="cluster-a",
        route_table=table,
        sender_cluster_resolver=lambda _: "cluster-b",
        security_config=RouteSecurityConfig(
            require_signatures=True,
            allow_unsigned=False,
            signature_algorithm=signer_old.algorithm,
        ),
        public_key_resolver=lambda cluster_id: registry.resolve_public_keys(
            cluster_id, now=now_state["now"]
        ),
    )

    announcement = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-c"),
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(),
        advertiser="cluster-b",
        advertised_at=130.0,
        ttl_seconds=60.0,
        epoch=1,
    )
    signed_old = signer_old.sign(announcement)
    updated = await processor.handle_announcement(
        signed_old, sender_id="node-b", now=now_state["now"]
    )
    assert updated is True

    now_state["now"] = 150.0
    later = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-c"),
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(),
        advertiser="cluster-b",
        advertised_at=150.0,
        ttl_seconds=60.0,
        epoch=2,
    )
    signed_old_late = signer_old.sign(later)
    updated = await processor.handle_announcement(
        signed_old_late, sender_id="node-b", now=now_state["now"]
    )
    assert updated is False


def test_route_policy_tag_filters() -> None:
    policy = RoutePolicy(allowed_tags={"gold"}, deny_tags={"blocked"})
    table = RouteTable(local_cluster="cluster-a", policy=policy)
    allowed = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-c"),
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(),
        advertiser="cluster-b",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
        route_tags=("gold", "fast"),
    )

    denied = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-c"),
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(),
        advertiser="cluster-b",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=2,
        route_tags=("blocked",),
    )

    assert (
        table.apply_announcement(allowed, received_from="cluster-b", now=100.0) is True
    )
    assert (
        table.apply_announcement(denied, received_from="cluster-b", now=100.0) is False
    )


@pytest.mark.asyncio
async def test_neighbor_policy_filters_announcement() -> None:
    table = RouteTable(local_cluster="cluster-a")
    policy = RoutePolicy(allowed_tags={"gold"})
    processor = RouteAnnouncementProcessor(
        local_cluster="cluster-a",
        route_table=table,
        sender_cluster_resolver=lambda _: "cluster-b",
        neighbor_policy_resolver=lambda _: policy,
    )

    blocked = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-c"),
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(),
        advertiser="cluster-b",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=1,
        route_tags=("silver",),
    )

    allowed = RouteAnnouncement(
        destination=RouteDestination(cluster_id="cluster-c"),
        path=RoutePath(("cluster-b", "cluster-c")),
        metrics=RouteMetrics(),
        advertiser="cluster-b",
        advertised_at=100.0,
        ttl_seconds=30.0,
        epoch=2,
        route_tags=("gold",),
    )

    updated = await processor.handle_announcement(
        blocked, sender_id="node-b", now=100.0
    )
    assert updated is False

    updated = await processor.handle_announcement(
        allowed, sender_id="node-b", now=100.0
    )
    assert updated is True
