from mpreg.fabric.route_keys import RouteKeyRegistry
from mpreg.fabric.route_security import RouteAnnouncementSigner


def test_route_key_registry_rotation_overlap() -> None:
    registry = RouteKeyRegistry()
    signer_old = RouteAnnouncementSigner.create()
    signer_new = RouteAnnouncementSigner.create()

    registry.register_key(
        cluster_id="cluster-b",
        public_key=signer_old.public_key,
        key_id="old-key",
        now=100.0,
    )
    registry.rotate_key(
        cluster_id="cluster-b",
        public_key=signer_new.public_key,
        overlap_seconds=20.0,
        now=120.0,
    )

    keys_during_overlap = registry.resolve_public_keys("cluster-b", now=130.0)
    assert signer_new.public_key in keys_during_overlap
    assert signer_old.public_key in keys_during_overlap

    keys_after_overlap = registry.resolve_public_keys("cluster-b", now=150.0)
    assert keys_after_overlap == (signer_new.public_key,)
