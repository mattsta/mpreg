"""Phase P: RPCResponse carries W3C trace; client last_trace always populated."""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from mpreg.client.client import Client
from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCResponse
from mpreg.core.observability.trace_context import TRACEPARENT_KEY
from mpreg.core.port_allocator import port_range_context
from mpreg.server import MPREGServer
from mpreg.server_pkg.rpc_responses import w3c_trace_fields


def test_rpc_response_accepts_w3c_fields() -> None:
    r = RPCResponse(
        r={"ok": True},
        u="u-1",
        traceparent="00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
        tracestate="vendor=1",
        headers={
            "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
        },
    )
    d = r.model_dump()
    assert d["traceparent"].startswith("00-")
    assert d["tracestate"] == "vendor=1"
    assert "traceparent" in d["headers"]


def test_w3c_trace_fields_from_bind() -> None:
    from mpreg.core.observability.trace_context import bind_current_trace

    with bind_current_trace(
        "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01",
        tracestate="s=1",
    ):
        kw = w3c_trace_fields()
    assert kw["traceparent"].startswith("00-c")
    assert kw["tracestate"] == "s=1"
    assert "headers" in kw


def test_inject_seeds_last_trace() -> None:
    c = Client(url="ws://127.0.0.1:1")
    out = c._inject_outbound_trace({"role": "rpc", "u": "1", "cmds": []})
    assert TRACEPARENT_KEY in out
    assert c._last_trace_metadata is not None
    assert TRACEPARENT_KEY in c._last_trace_metadata


@pytest.mark.asyncio
async def test_live_rpc_echoes_trace_to_client() -> None:
    with port_range_context(1, "servers") as ports:
        settings = MPREGSettings(
            port=ports[0],
            name="TraceEcho",
            resources={"t"},
            log_level="ERROR",
            gossip_interval=30.0,
        )
        server = MPREGServer(settings=settings)

        def echo(msg: str) -> str:
            return msg

        server.register_command("echo", echo, ["t"])
        task = asyncio.create_task(server.server())
        try:
            await asyncio.sleep(0.15)
            url = f"ws://127.0.0.1:{ports[0]}"
            async with MPREGClientAPI(url) as client:
                out = await client.call("echo", "hi", locs=frozenset(["t"]))
                assert out == "hi"
                ctx = client.last_trace_context()
                assert isinstance(ctx, dict)
                assert "traceparent" in ctx
                assert str(ctx["traceparent"]).startswith("00-")
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
            with contextlib.suppress(Exception):
                await server.shutdown()
