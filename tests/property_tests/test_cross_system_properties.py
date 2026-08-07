#!/usr/bin/env python3
"""
Property-Based Tests for Fabric Cross-System Behaviors.

These tests verify invariants of the fabric message envelope and routing helpers.
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.fabric.message import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    RoutingPriority,
    UnifiedMessage,
)
from mpreg.fabric.message_codec import (
    unified_message_from_dict,
    unified_message_to_dict,
)
from mpreg.fabric.router import is_control_plane_message, is_federation_message


def correlation_ids() -> st.SearchStrategy[str]:
    return st.text(
        min_size=8,
        max_size=64,
        alphabet=st.characters(whitelist_categories=("L", "N")),
    ).map(lambda s: f"corr-{s}")


def message_ids() -> st.SearchStrategy[str]:
    return st.text(
        min_size=8,
        max_size=64,
        alphabet=st.characters(whitelist_categories=("L", "N")),
    ).map(lambda s: f"msg-{s}")


def topics() -> st.SearchStrategy[str]:
    segments = st.text(
        min_size=1,
        max_size=10,
        alphabet=st.characters(whitelist_categories=("L", "N")),
    )
    return st.lists(segments, min_size=2, max_size=6).map(".".join)


def headers() -> st.SearchStrategy[MessageHeaders]:
    return st.builds(
        MessageHeaders,
        correlation_id=correlation_ids(),
        source_cluster=st.one_of(st.none(), st.text(min_size=3, max_size=12)),
        target_cluster=st.one_of(st.none(), st.text(min_size=3, max_size=12)),
        routing_path=st.lists(
            st.text(min_size=1, max_size=12), min_size=0, max_size=4
        ).map(tuple),
        federation_path=st.lists(
            st.text(min_size=1, max_size=12), min_size=0, max_size=4
        ).map(tuple),
        hop_budget=st.one_of(st.none(), st.integers(min_value=0, max_value=10)),
        priority=st.sampled_from(list(RoutingPriority)),
        metadata=st.dictionaries(
            st.text(min_size=1, max_size=10), st.text(), max_size=5
        ),
    )


def unified_messages() -> st.SearchStrategy[UnifiedMessage]:
    return st.builds(
        UnifiedMessage,
        message_id=message_ids(),
        topic=topics(),
        message_type=st.sampled_from(list(MessageType)),
        delivery=st.sampled_from(list(DeliveryGuarantee)),
        payload=st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.one_of(
                st.text(), st.integers(), st.booleans(), st.floats(allow_nan=False)
            ),
        ),
        headers=headers(),
        timestamp=st.floats(min_value=1600000000, max_value=2000000000),
    )


class TestMessageIntegrityProperties:
    @given(message=unified_messages())
    @settings(max_examples=200, deadline=3000)
    def test_message_codec_roundtrip(self, message: UnifiedMessage) -> None:
        """
        Property: UnifiedMessage survives codec round-trip without mutation.
        """
        payload = unified_message_to_dict(message)
        reconstructed = unified_message_from_dict(payload)

        assert reconstructed.message_id == message.message_id
        assert reconstructed.topic == message.topic
        assert reconstructed.message_type == message.message_type
        assert reconstructed.delivery == message.delivery
        assert reconstructed.payload == message.payload
        assert reconstructed.headers.correlation_id == message.headers.correlation_id

    @given(message=unified_messages())
    @settings(max_examples=200, deadline=3000)
    def test_control_plane_detection(self, message: UnifiedMessage) -> None:
        """Property: Control plane detection only true for mpreg.* + CONTROL."""
        if (
            message.topic.startswith("mpreg.")
            and message.message_type == MessageType.CONTROL
        ):
            assert is_control_plane_message(message)
        if not message.topic.startswith("mpreg."):
            assert not is_control_plane_message(message)

    @given(message=unified_messages())
    @settings(max_examples=200, deadline=3000)
    def test_federation_detection(self, message: UnifiedMessage) -> None:
        """Property: Federation detection matches headers or topic prefix."""
        if message.headers.target_cluster or message.headers.federation_path:
            assert is_federation_message(message)
        if message.topic.startswith("mpreg.fabric."):
            assert is_federation_message(message)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
