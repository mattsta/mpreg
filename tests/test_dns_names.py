from mpreg.dns.names import decode_node_id, encode_node_id, parse_node_labels


def test_encode_decode_node_id_roundtrip() -> None:
    node_id = "ws://127.0.0.1:10000"
    label = encode_node_id(node_id)
    assert label.startswith("b32-")
    assert decode_node_id(label) == node_id


def test_parse_node_labels_accepts_dns_safe_label() -> None:
    label = "node-1"
    assert parse_node_labels((label, "node")) == label


def test_parse_node_labels_rejects_invalid_label() -> None:
    assert parse_node_labels(("bad_label", "node")) is None
