from mpreg.client.client import Client
from mpreg.core.observability.trace_context import TRACEPARENT_KEY


def test_inject_outbound_trace_adds_traceparent() -> None:
    # Client needs url - construct minimally
    c = Client(url="ws://127.0.0.1:1")
    out = c._inject_outbound_trace({"role": "rpc", "u": "1", "cmds": []})
    assert TRACEPARENT_KEY in out
    assert out[TRACEPARENT_KEY].startswith("00-")
    assert "headers" in out
