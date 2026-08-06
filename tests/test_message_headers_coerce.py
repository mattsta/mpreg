"""Unit coverage for MessageHeaders.coerce (F22) and catalog entry_type default (F23)."""

from __future__ import annotations

import pytest

from mpreg.core.cluster_map import CatalogQueryRequest
from mpreg.core.statistics import MessageHeaders
from mpreg.fabric.pubsub_forwarding import PubSubForwardingMetadata

def test_message_headers_coerce_none() -> None:
    assert MessageHeaders.coerce(None) is None

def test_message_headers_coerce_dataclass_passthrough() -> None:
    h = MessageHeaders(correlation_id="c1", custom_headers={"x": "1"})
    assert MessageHeaders.coerce(h) is h

def test_message_headers_coerce_mapping() -> None:
    h = MessageHeaders.coerce(
        {
            "correlation_id": "corr",
            "content_type": "application/json",
            "priority": "2",
            "x-trace": "abc",
            "reply_to": "r.topic",
        }
    )
    assert h is not None
    assert h.correlation_id == "corr"
    assert h.content_type == "application/json"
    assert h.priority == 2
    assert h.reply_to == "r.topic"
    assert h.custom_headers == {"x-trace": "abc"}
    d = h.to_dict()
    assert d["correlation_id"] == "corr"
    assert d["x-trace"] == "abc"

def test_message_headers_coerce_rejects_bad_type() -> None:
    with pytest.raises(TypeError, match="headers must be"):
        MessageHeaders.coerce("not-headers")  # type: ignore[arg-type]

def test_catalog_query_request_default_entry_type() -> None:
    assert CatalogQueryRequest().entry_type == "functions"
    assert CatalogQueryRequest.from_dict({}).entry_type == "functions"
    assert CatalogQueryRequest.from_dict({"entry_type": ""}).entry_type == "functions"
    assert CatalogQueryRequest.from_dict({"entry_type": None}).entry_type == "functions"
    assert CatalogQueryRequest.from_dict({"entry_type": "Nodes"}).entry_type == "nodes"

def test_pubsub_forwarding_max_hops_bad_type() -> None:
    """except (TypeError, ValueError) path for non-int max_hops."""
    from mpreg.fabric.pubsub_forwarding import FABRIC_PUBSUB_FORWARDING_KEY

    meta = PubSubForwardingMetadata.from_headers(
        {
            FABRIC_PUBSUB_FORWARDING_KEY: {
                "origin_node": "n1",
                "routing_path": ["n1"],
                "max_hops": object(),
            }
        }
    )
    assert meta is not None
    assert meta.max_hops is None

    meta2 = PubSubForwardingMetadata.from_headers(
        {
            FABRIC_PUBSUB_FORWARDING_KEY: {
                "origin_node": "n1",
                "routing_path": ["n1"],
                "max_hops": "nope",
            }
        }
    )
    assert meta2 is not None
    assert meta2.max_hops is None
