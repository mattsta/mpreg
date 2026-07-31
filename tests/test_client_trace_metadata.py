from mpreg.client.client import Client

def test_record_trace_from_top_level_and_nested() -> None:
    client = Client(url="ws://127.0.0.1:1", full_log=False)
    client._record_trace_from_message(
        {
            "role": "rpc-response",
            "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
            "headers": {
                "metadata": {
                    "tracestate": "vendor=1",
                }
            },
        }
    )
    assert client._last_trace_metadata is not None
    assert client._last_trace_metadata["traceparent"].startswith("00-")
    assert client._last_trace_metadata["tracestate"] == "vendor=1"

def test_record_trace_ignores_non_dict() -> None:
    client = Client(url="ws://127.0.0.1:1", full_log=False)
    client._record_trace_from_message("nope")
    assert client._last_trace_metadata is None
