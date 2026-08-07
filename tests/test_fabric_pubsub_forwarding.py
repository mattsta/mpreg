from mpreg.fabric.pubsub_forwarding import (
    FABRIC_PUBSUB_FORWARDING_KEY,
    PubSubForwardingMetadata,
)


def test_pubsub_forwarding_metadata_roundtrip() -> None:
    metadata = PubSubForwardingMetadata(
        origin_node="node-a",
        routing_path=("node-a", "node-b"),
        max_hops=3,
    )
    headers = {"trace_id": "trace-1"}

    updated = metadata.with_headers(headers)
    parsed = PubSubForwardingMetadata.from_headers(updated)

    assert updated["trace_id"] == "trace-1"
    assert parsed == metadata


def test_pubsub_forwarding_metadata_missing_payload() -> None:
    assert PubSubForwardingMetadata.from_headers({}) is None
    assert (
        PubSubForwardingMetadata.from_headers({FABRIC_PUBSUB_FORWARDING_KEY: "invalid"})
        is None
    )


def test_pubsub_forwarding_metadata_hops() -> None:
    metadata = PubSubForwardingMetadata(
        origin_node="node-a", routing_path=("node-a",), max_hops=1
    )

    assert metadata.hop_count == 0
    assert metadata.can_forward() is True

    hopped = metadata.with_hop("node-b")
    assert hopped.hop_count == 1
    assert hopped.can_forward() is False
    assert hopped.has_visited("node-a") is True
    assert hopped.has_visited("node-b") is True
