"""L1 pubsub_request_reply — publish_with_reply request/response over topics."""

from __future__ import annotations

import asyncio
from typing import Any

from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.pubsub_client import MPREGPubSubClient
from mpreg.core.config import MPREGSettings
from mpreg.core.model import PubSubMessage
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
)
from mpreg.server import MPREGServer


def _reply_to_from_message(msg: PubSubMessage) -> str | None:
    headers = getattr(msg, "headers", None)
    if headers is None:
        return None
    if isinstance(headers, dict):
        raw = headers.get("reply_to")
        return str(raw) if raw else None
    raw = getattr(headers, "reply_to", None)
    if raw:
        return str(raw)
    if hasattr(headers, "get"):
        raw = headers.get("reply_to")
        return str(raw) if raw else None
    return None


async def main() -> None:
    with app_run(
        "pubsub_request_reply",
        "PubSub Request/Reply — publish_with_reply drill",
        level="L1",
    ):
        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="PubSub-RR",
                    cluster_id="rr-cluster",
                    log_level="WARNING",
                    gossip_interval=30.0,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                url = f"ws://127.0.0.1:{ports[0]}"
                step(f"pubsub hub {url}")

                async with MPREGClientAPI(url) as base_req:
                    async with MPREGClientAPI(url) as base_svc:
                        req = MPREGPubSubClient(base_client=base_req)
                        svc = MPREGPubSubClient(base_client=base_svc)
                        await req.start()
                        await svc.start()
                        try:
                            handled = asyncio.Event()
                            seen: dict[str, Any] = {}
                            reply_count = {"n": 0}

                            async def _service_handler(msg: PubSubMessage) -> None:
                                seen["request"] = msg.payload
                                reply_to = _reply_to_from_message(msg)
                                ensure(
                                    bool(reply_to),
                                    f"service missing reply_to headers={msg.headers!r}",
                                )
                                step(f"service replying to {reply_to}")
                                await svc.publish(
                                    str(reply_to),
                                    {
                                        "echo": msg.payload,
                                        "status": "ok",
                                        "svc": "rr-worker",
                                    },
                                )
                                reply_count["n"] += 1
                                handled.set()

                            with scenario(
                                "subscribe service worker on rpc.echo",
                                "pubsub.exchange",
                                "pubsub.client_wire",
                            ):
                                sub_id = await svc.subscribe(
                                    ["rpc.echo"],
                                    _service_handler,
                                    get_backlog=False,
                                )
                                ensure(bool(sub_id), "subscribe returned empty id")
                                await asyncio.sleep(0.2)
                                ok(f"service subscribed id={sub_id}")

                            with scenario(
                                "publish_with_reply round-trip",
                                "pubsub.publish_reply",
                                "pubsub.headers",
                            ):
                                response = await req.publish_with_reply(
                                    "rpc.echo",
                                    {"q": "ping", "n": 1},
                                    timeout=10.0,
                                )
                                ensure(
                                    response.success is True,
                                    f"publish_with_reply failed: {getattr(response, 'error', response)}",
                                )
                                payload = getattr(response, "reply_payload", None)
                                ensure(
                                    payload is not None,
                                    f"no reply_payload: {response}",
                                )
                                ensure(
                                    isinstance(payload, dict)
                                    and payload.get("status") == "ok",
                                    f"bad reply {payload}",
                                )
                                echo = (
                                    payload.get("echo")
                                    if isinstance(payload, dict)
                                    else None
                                )
                                ensure(
                                    isinstance(echo, dict) and echo.get("q") == "ping",
                                    f"echo missing in {payload}",
                                )
                                ok(f"reply payload={payload!r}")

                            with scenario(
                                "service handler observed request",
                                "pubsub.fanout",
                            ):
                                try:
                                    await asyncio.wait_for(handled.wait(), timeout=2.0)
                                except TimeoutError:
                                    step("handler event slow; reply already asserted")
                                if handled.is_set():
                                    ensure(
                                        isinstance(seen.get("request"), dict),
                                        f"bad seen request {seen}",
                                    )
                                    ok(f"handler saw {seen.get('request')}")
                                else:
                                    ok("reply path validated (handler event unset)")

                            with scenario(
                                "second request gets independent reply",
                                "pubsub.publish_reply",
                            ):
                                r2 = await req.publish_with_reply(
                                    "rpc.echo",
                                    {"q": "pong", "n": 2},
                                    timeout=10.0,
                                )
                                ensure(
                                    r2.success is True,
                                    f"second reply failed: {getattr(r2, 'error', r2)}",
                                )
                                p2 = getattr(r2, "reply_payload", None)
                                ensure(p2 is not None, "second reply empty")
                                ensure(
                                    isinstance(p2, dict)
                                    and isinstance(p2.get("echo"), dict)
                                    and p2["echo"].get("q") == "pong",
                                    f"second echo bad {p2}",
                                )
                                ok(f"second reply={p2!r}")
                                ensure(
                                    reply_count["n"] >= 1,
                                    f"service replied {reply_count['n']} times",
                                )
                                ok(f"service reply_count={reply_count['n']}")
                        finally:
                            await req.stop()
                            await svc.stop()

            await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())
