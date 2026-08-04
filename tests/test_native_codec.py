"""Native codec: no nested str/repr blow-ups; orjson canonical digests."""

from __future__ import annotations

import time

from mpreg.core.native_codec import (
    canonical_dumps,
    dumps,
    estimate_size_bytes,
    loads,
    payload_fingerprint_hex,
)
from mpreg.core.serialization import JsonSerializer
from mpreg.fabric.gossip import GossipMessage, GossipMessageType

def test_roundtrip_and_canonical_sort() -> None:
    data = {"b": 1, "a": {"z": 3, "y": 2}}
    assert loads(dumps(data)) == data
    assert canonical_dumps(data) == b'{"a":{"y":2,"z":3},"b":1}'
    ser = JsonSerializer()
    assert ser.serialize_canonical(data) == canonical_dumps(data)

def test_estimate_size_fast_on_huge_nested() -> None:
    nested = {
        "update_id": "u1",
        "nodes": [
            {"node_id": f"n{i}", "functions": [{"name": f"f{j}"} for j in range(30)]}
            for i in range(400)
        ],
    }
    t0 = time.perf_counter()
    size = estimate_size_bytes(nested)
    dt = time.perf_counter() - t0
    assert size > 0
    assert dt < 0.05, f"estimate too slow: {dt:.3f}s"

def test_payload_fingerprint_catalog_fast_path() -> None:
    payload = {
        "update_id": "catalog-abc",
        "cluster_id": "c1",
        "functions": [{"x": i} for i in range(5000)],
        "nodes": [{"n": i} for i in range(5000)],
    }
    t0 = time.perf_counter()
    fp = payload_fingerprint_hex(payload, truncate=16)
    dt = time.perf_counter() - t0
    assert len(fp) == 16
    assert dt < 0.02, f"fingerprint too slow: {dt:.3f}s"
    # Same identity → same fingerprint regardless of body churn shape lengths
    payload2 = dict(payload)
    payload2["functions"] = payload2["functions"] + [{"x": -1}]
    fp2 = payload_fingerprint_hex(payload2, truncate=16)
    assert fp != fp2  # length changed

def test_gossip_checksum_avoids_str_payload() -> None:
    huge = {
        "update_id": "delta-1",
        "cluster_id": "c",
        "functions": [{"name": f"fn-{i}", "meta": {"k": "v" * 20}} for i in range(2000)],
        "nodes": [{"node_id": f"n-{i}"} for i in range(500)],
    }
    t0 = time.perf_counter()
    msg = GossipMessage(
        message_id="m1",
        message_type=GossipMessageType.CATALOG_UPDATE,
        sender_id="ws://127.0.0.1:1",
        payload=huge,
        sequence_number=1,
    )
    dt = time.perf_counter() - t0
    assert len(msg.checksum) == 16
    assert len(msg.digest) == 8
    assert dt < 0.05, f"GossipMessage init too slow under huge payload: {dt:.3f}s"

def test_bigint_canonical_and_text_helpers(tmp_path) -> None:
    """orjson accepts full u64 as numbers; only >u64 becomes decimal str."""
    from mpreg.core.native_codec import (
        JSONDecodeError,
        dumps_pretty_text,
        dumps_text,
        load_path,
        loads_text,
    )

    # Full unsigned 64-bit range stays a JSON number (no walk / no quotes).
    u64_max = (1 << 64) - 1
    raw_u64 = canonical_dumps({"n": u64_max, "ok": 1})
    assert b'"n":"' not in raw_u64
    assert loads(raw_u64)["n"] == u64_max

    # True bigint: orjson TypeError → slow-path stringify for stable digests.
    huge = 1 << 64  # one past u64 max
    data = {"n": huge, "ok": 1}
    raw = canonical_dumps(data)
    assert b'"n":"' in raw  # bigint as decimal string
    assert loads(raw)["ok"] == 1
    assert loads(raw)["n"] == str(huge)

    # Non-str keys: OPT_NON_STR_KEYS, still no mandatory pre-walk.
    assert loads(dumps({1: "a", "b": 2})) == {"1": "a", "b": 2}

    assert loads_text(dumps_text({"a": 1})) == {"a": 1}
    path = tmp_path / "x.json"
    path.write_text(dumps_pretty_text({"z": 2, "a": 1}))
    loaded = load_path(path)
    assert loaded["a"] == 1
    try:
        loads_text("{not-json")
        raise AssertionError("expected JSONDecodeError")
    except JSONDecodeError:
        pass

def test_dump_path_and_canonical_hash(tmp_path) -> None:
    from mpreg.core.native_codec import canonical_hash_hex, dump_path, load_path

    path = tmp_path / "nested" / "cfg.json"
    dump_path(path, {"b": 2, "a": 1}, pretty=True)
    assert path.is_file()
    assert load_path(path) == {"b": 2, "a": 1}
    dump_path(path, {"x": True}, pretty=False)
    assert load_path(path) == {"x": True}
    h = canonical_hash_hex({"z": 1, "a": 2}, truncate=16)
    assert len(h) == 16
    assert h == canonical_hash_hex({"a": 2, "z": 1}, truncate=16)

def test_estimate_size_cycle_and_primitives() -> None:
    assert estimate_size_bytes(None) == 0
    assert estimate_size_bytes(b"abcd") == 4
    assert estimate_size_bytes("hi") == 2
    cyclic: dict = {"a": 1}
    cyclic["self"] = cyclic
    assert estimate_size_bytes(cyclic) > 0

def test_backend_registry_roundtrip() -> None:
    from mpreg.core.native_codec import get_codec_backend, set_codec_backend

    prev = get_codec_backend()
    try:
        backend = set_codec_backend("orjson")
        assert backend.name == "orjson"
        assert loads(dumps({"k": 1})) == {"k": 1}
    finally:
        set_codec_backend(prev)

def test_message_queue_fingerprint_avoids_str_payload() -> None:
    """Queue dedup fingerprint must not nested-repr large payloads."""
    from mpreg.core.message_queue import (
        DeliveryGuarantee,
        MessageQueue,
        QueueConfiguration,
        QueuedMessage,
    )
    from mpreg.datastructures import MessageId

    huge = {"blob": [{"x": i, "y": "z" * 40} for i in range(3000)]}
    q = MessageQueue(QueueConfiguration(name="fp-test"))
    msg = QueuedMessage(
        id=MessageId(source_node="fp-test"),
        topic="t.1",
        payload=huge,
        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
        headers={"h": "1"},
    )
    t0 = time.perf_counter()
    fp = q._create_message_fingerprint(msg)
    dt = time.perf_counter() - t0
    assert isinstance(fp, str) and len(fp) == 64
    assert dt < 0.05, f"fingerprint too slow: {dt:.3f}s"
    fp2 = q._create_message_fingerprint(msg)
    assert fp == fp2
