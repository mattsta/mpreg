"""Unified client façade unit coverage."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from mpreg.client import (
    CacheOpResult,
    MPREGClient,
    QueueSendResult,
    StrongRetryAbortResult,
)
from mpreg.client.unified_client import UnifiedMPREGClient
from mpreg.core.errors import MpregError, MpregErrorCode
from mpreg.core.model import RPCCommand

def test_exports_and_aliases() -> None:
    assert MPREGClient is UnifiedMPREGClient
    r = QueueSendResult.from_raw(
        {"success": True, "message_id": "m1", "error_message": None}
    )
    assert r.success and r.message_id == "m1"
    c = CacheOpResult.from_raw({"success": True, "value": 42})
    assert c.success and c.value == 42
    # T42: STRONG diagnostics + retry result types exported
    c2 = CacheOpResult.from_raw(
        {
            "success": False,
            "operation_id": "o1",
            "quorum_info": {"abort_fail_peers": ["n1"]},
        }
    )
    assert c2.operation_id == "o1"
    assert c2.quorum_info and "n1" in c2.quorum_info["abort_fail_peers"]
    sr = StrongRetryAbortResult.from_raw(
        {"success": True, "cleared": True, "ok_peers": [], "fail_peers": []}
    )
    assert sr.cleared and sr.ops_driven and not sr.automatic_heal

@pytest.mark.asyncio
async def test_unified_client_composes_api() -> None:
    client = MPREGClient(url="ws://127.0.0.1:9")
    assert client.api.url == "ws://127.0.0.1:9"
    assert client.pubsub is not None

    async def fake_call(self, fun, *args, **kwargs):
        from mpreg.core.rpc_naming import PlatformRpc

        if fun in ("queue_send", PlatformRpc.QUEUE_SEND):
            return {"success": True, "message_id": "q1"}
        if fun in ("cache_put", PlatformRpc.CACHE_PUT):
            return {"success": True}
        if fun in ("cache_get", PlatformRpc.CACHE_GET):
            return {"success": True, "value": "v"}
        return {"ok": True}

    async def fake_connect(self):
        self._connected = True

    async def fake_request(self, commands, timeout=None):
        return {"r": len(commands)}

    # slots=True makes method attributes read-only on the instance; bind via type
    object.__setattr__(client.api, "_connected", True)
    # Patch on the class for this test process — restore after
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
            [RPCCommand(name="e", fun="mpreg.system.echo", args=("x",), kwargs={})]
        )
        assert out["r"] == 1
    finally:
        MPREGClientAPI.call = orig_call  # type: ignore[method-assign]
        MPREGClientAPI.connect = orig_connect  # type: ignore[method-assign]
        MPREGClientAPI.request = orig_request  # type: ignore[method-assign]

@pytest.mark.asyncio
async def test_unified_publish_fail_closed_by_default() -> None:
    """MPREGClient.publish raises on negative ack (ERG-05); soft path opt-in."""
    client = MPREGClient(url="ws://127.0.0.1:9")
    client._pubsub_started = True

    async def soft_fail(*_a, **_k):
        return {"role": "pubsub-ack", "success": False, "message": "no"}

    # Drive through pubsub client path with a stubbed transport
    transport = MagicMock()
    transport.send_raw_message = AsyncMock(
        return_value={"role": "other", "success": False}
    )
    client.api._client = transport  # type: ignore[attr-defined]
    client.api._connected = True  # type: ignore[attr-defined]
    object.__setattr__(client.api, "_connected", True)
    client.pubsub.base_client = client.api

    with pytest.raises(MpregError) as ei:
        await client.publish("t.topic", {"x": 1})
    assert ei.value.code == int(MpregErrorCode.UNAVAILABLE)

    ok = await client.publish("t.topic", {"x": 1}, raise_on_failure=False)
    assert ok is False

@pytest.mark.asyncio
async def test_pubsub_publish_soft_bool_default() -> None:
    """Low-level pubsub client keeps legacy soft-bool unless raise_on_failure."""
    from mpreg.client.pubsub_client import MPREGPubSubClient

    base = MagicMock()
    base._client = MagicMock()
    base._client.send_raw_message = AsyncMock(return_value={"role": "other"})
    ps = MPREGPubSubClient(base_client=base)
    assert await ps.publish("t", 1) is False
