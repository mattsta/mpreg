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
    """orjson rejects >i64; codec coerces to decimal str for stable digests."""
    from mpreg.core.native_codec import (
        JSONDecodeError,
        dumps_pretty_text,
        dumps_text,
        load_path,
        loads_text,
    )

    huge = (1 << 63) + 99
    data = {"n": huge, "ok": 1}
    raw = canonical_dumps(data)
    assert b'"n":"' in raw  # bigint as decimal string
    assert loads(raw)["ok"] == 1
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
