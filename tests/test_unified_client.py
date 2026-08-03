"""Unified client façade unit coverage."""

from __future__ import annotations

from types import MethodType

import pytest

from mpreg.client import CacheOpResult, MPREGClient, QueueSendResult
from mpreg.client.unified_client import UnifiedMPREGClient
from mpreg.core.model import RPCCommand

def test_exports_and_aliases() -> None:
    assert MPREGClient is UnifiedMPREGClient
    r = QueueSendResult.from_raw(
        {"success": True, "message_id": "m1", "error_message": None}
    )
    assert r.success and r.message_id == "m1"
    c = CacheOpResult.from_raw({"success": True, "value": 42})
    assert c.success and c.value == 42

@pytest.mark.asyncio
async def test_unified_client_composes_api() -> None:
    client = MPREGClient(url="ws://127.0.0.1:9")
    assert client.api.url == "ws://127.0.0.1:9"
    assert client.pubsub is not None

    async def fake_call(self, fun, *args, **kwargs):
        if fun == "queue_send":
            return {"success": True, "message_id": "q1"}
        if fun == "cache_put":
            return {"success": True}
        if fun == "cache_get":
            return {"success": True, "value": "v"}
        return {"ok": True}

    async def fake_connect(self):
        self._connected = True

    async def fake_request(self, commands, timeout=None):
        return {"r": len(commands)}

    # slots=True makes method attributes read-only on the instance; bind via type
    object.__setattr__(client.api, "_connected", True)
    # Patch on the class for this test process — restore after
    original_call = MPREGClientAPI_call = None
    from mpreg.client.client_api import MPREGClientAPI

    orig_call = MPREGClientAPI.call
    orig_connect = MPREGClientAPI.connect
    orig_request = MPREGClientAPI.request
    try:
        MPREGClientAPI.call = fake_call  # type: ignore[method-assign]
        MPREGClientAPI.connect = fake_connect  # type: ignore[method-assign]
        MPREGClientAPI.request = fake_request  # type: ignore[method-assign]

        qs = await client.queue_send("jobs", {"a": 1})
        assert qs.success and qs.message_id == "q1"
        put = await client.cache_put("ns", "id1", {"x": 1})
        assert put.success
        got = await client.cache_get("ns", "id1")
        assert got.value == "v"
        out = await client.request(
            [RPCCommand(name="e", fun="echo", args=("x",), kwargs={})]
        )
        assert out["r"] == 1
    finally:
        MPREGClientAPI.call = orig_call  # type: ignore[method-assign]
        MPREGClientAPI.connect = orig_connect  # type: ignore[method-assign]
        MPREGClientAPI.request = orig_request  # type: ignore[method-assign]
