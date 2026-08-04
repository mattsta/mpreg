"""A6: Signed routes under key rotation overlap (INV-R8)."""

from __future__ import annotations

import time

from mpreg.fabric.route_control import (
    RouteAnnouncement,
    RouteDestination,
    RouteMetrics,
    RoutePath,
)
from mpreg.fabric.route_keys import RouteKeyRegistry
from mpreg.fabric.route_security import (
    RouteAnnouncementSigner,
    RouteSecurityConfig,
    verify_route_announcement,
)

def _announcement(advertiser: str = "adv") -> RouteAnnouncement:
    return RouteAnnouncement(
        destination=RouteDestination(cluster_id="dest"),
        path=RoutePath(hops=(advertiser, "dest")),
        metrics=RouteMetrics(hop_count=1),
        advertiser=advertiser,
        advertised_at=time.time(),
        ttl_seconds=60.0,
    )

def test_unsigned_rejected_when_required() -> None:
    cfg = RouteSecurityConfig(require_signatures=True, allow_unsigned=False)
    ann = _announcement()
    assert not ann.signature
    # Authorization path mirrors route_announcer
    if not ann.signature:
        assert cfg.require_signatures
        authorized = cfg.allow_unsigned
    else:
        authorized = True
    assert authorized is False

def test_wrong_key_rejected() -> None:
    signer_a = RouteAnnouncementSigner.create()
    signer_b = RouteAnnouncementSigner.create()
    signed = signer_a.sign(_announcement())
    assert verify_route_announcement(signed, public_key=signer_a.public_key)
    assert not verify_route_announcement(signed, public_key=signer_b.public_key)

def test_key_rotation_overlap_accepts_both() -> None:
    now = time.time()
    registry = RouteKeyRegistry()
    old = RouteAnnouncementSigner.create()
    new = RouteAnnouncementSigner.create()
    registry.register_key(
        cluster_id="adv", public_key=old.public_key, make_primary=True, now=now
    )
    registry.rotate_key(
        cluster_id="adv",
        public_key=new.public_key,
        overlap_seconds=120.0,
        now=now,
    )
    keys = registry.resolve_public_keys("adv", now=now + 1)
    assert old.public_key in keys
    assert new.public_key in keys

    signed_old = old.sign(_announcement())
    signed_new = new.sign(_announcement())
    assert any(verify_route_announcement(signed_old, public_key=k) for k in keys)
    assert any(verify_route_announcement(signed_new, public_key=k) for k in keys)

def test_after_overlap_expiry_old_key_gone() -> None:
    now = 1_000.0
    registry = RouteKeyRegistry()
    old = RouteAnnouncementSigner.create()
    new = RouteAnnouncementSigner.create()
    registry.register_key(
        cluster_id="adv", public_key=old.public_key, make_primary=True, now=now
    )
    registry.rotate_key(
        cluster_id="adv",
        public_key=new.public_key,
        overlap_seconds=10.0,
        now=now,
    )
    registry.purge_expired(now=now + 11)
    keys = registry.resolve_public_keys("adv", now=now + 11)
    assert new.public_key in keys
    assert old.public_key not in keys

def test_processor_rejects_unsigned_when_required() -> None:
    """COR-T10-09 / INV-R9: processor path refuses unsigned under require_signatures."""
    from mpreg.fabric.route_announcer import RouteAnnouncementProcessor
    from mpreg.fabric.route_control import RouteTable

    cfg = RouteSecurityConfig(require_signatures=True, allow_unsigned=False)
    proc = RouteAnnouncementProcessor(
        local_cluster="local",
        route_table=RouteTable(local_cluster="local"),
        sender_cluster_resolver=lambda _url: "adv",
        security_config=cfg,
    )
    ann = _announcement(advertiser="adv")
    assert proc._is_announcement_authorized(ann, sender_cluster="adv") is False

def test_processor_accepts_valid_signature_with_resolver() -> None:
    """COR-T10-09: signed announcement authorized via public_key_resolver pin."""
    from mpreg.fabric.route_announcer import RouteAnnouncementProcessor
    from mpreg.fabric.route_control import RouteTable

    signer = RouteAnnouncementSigner.create()
    cfg = RouteSecurityConfig(require_signatures=True, allow_unsigned=False)
    proc = RouteAnnouncementProcessor(
        local_cluster="local",
        route_table=RouteTable(local_cluster="local"),
        sender_cluster_resolver=lambda _url: "adv",
        security_config=cfg,
        public_key_resolver=lambda cluster: (
            signer.public_key if cluster == "adv" else None
        ),
    )
    signed = signer.sign(_announcement(advertiser="adv"))
    assert proc._is_announcement_authorized(signed, sender_cluster="adv") is True

def test_processor_rejects_self_attested_key_when_required() -> None:
    """COR-T10-09: self-attested public_key is ignored when require_signatures."""
    from mpreg.fabric.route_announcer import RouteAnnouncementProcessor
    from mpreg.fabric.route_control import RouteTable

    signer = RouteAnnouncementSigner.create()
    cfg = RouteSecurityConfig(require_signatures=True, allow_unsigned=False)
    # No resolver / registry — only self-attested key on the announcement.
    proc = RouteAnnouncementProcessor(
        local_cluster="local",
        route_table=RouteTable(local_cluster="local"),
        sender_cluster_resolver=lambda _url: "adv",
        security_config=cfg,
        public_key_resolver=None,
    )
    signed = signer.sign(_announcement(advertiser="adv"))
    # Even if announcement carries public_key, COR-04 ignores it under require_signatures.
    assert proc._is_announcement_authorized(signed, sender_cluster="adv") is False
