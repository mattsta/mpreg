"""Phase S: RPCResponse converters preserve W3C fields."""

from mpreg.core.enhanced_rpc import TopicAwareRPCResponse
from mpreg.core.intermediate_results import EnhancedRPCResponse
from mpreg.core.model import RPCResponse

TP = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"

def test_rpc_response_roundtrip_w3c() -> None:
    r = RPCResponse(
        r=1,
        u="u",
        traceparent=TP,
        tracestate="v=1",
        headers={"traceparent": TP},
    )
    r2 = RPCResponse.model_validate(r.model_dump())
    assert r2.traceparent == TP
    assert r2.tracestate == "v=1"

def test_topic_aware_to_rpc_response_preserves_w3c() -> None:
    obj = TopicAwareRPCResponse(
        r={"ok": True},
        request_id="u-1",
        traceparent=TP,
        tracestate="x=1",
        headers={"traceparent": TP},
    )
    out = obj.to_rpc_response()
    assert isinstance(out, RPCResponse)
    assert out.u == "u-1"
    assert out.traceparent == TP
    assert out.tracestate == "x=1"

def test_topic_aware_from_rpc_response_copies_w3c() -> None:
    base = RPCResponse(r=2, u="u-2", traceparent=TP, tracestate="y=2", headers={"traceparent": TP})
    obj = TopicAwareRPCResponse.from_rpc_response(base)
    assert obj.traceparent == TP
    assert obj.to_rpc_response().traceparent == TP

def test_enhanced_roundtrip_preserves_w3c() -> None:
    base = RPCResponse(r=3, u="u-3", traceparent=TP, tracestate="z=3", headers={"traceparent": TP})
    enh = EnhancedRPCResponse.from_rpc_response(base)
    assert enh.traceparent == TP
    back = enh.to_rpc_response()
    assert back.traceparent == TP
    assert back.tracestate == "z=3"
    assert back.u == "u-3"
