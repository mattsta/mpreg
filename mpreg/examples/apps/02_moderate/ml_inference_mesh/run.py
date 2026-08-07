"""L2 ml_inference_mesh — router + vision/NLP specialized workers (prod.ml)."""

from __future__ import annotations

import asyncio
from typing import Any

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
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


async def main() -> None:
    with app_run("ml_inference_mesh", "ML Inference Mesh — locs workers", level="L2"):
        with port_range_context(3, "servers") as ports:
            hub = f"ws://127.0.0.1:{ports[0]}"
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Router",
                    resources={"router", "api"},
                    log_level="WARNING",
                ),
                MPREGSettings(
                    port=ports[1],
                    name="Vision",
                    resources={"vision", "gpu"},
                    peers=[hub],
                    log_level="WARNING",
                ),
                MPREGSettings(
                    port=ports[2],
                    name="NLP",
                    resources={"nlp", "cpu"},
                    peers=[hub],
                    log_level="WARNING",
                ),
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                router, vision, nlp = servers

                def route_request(kind: str, payload: str) -> dict[str, Any]:
                    target = "vision" if kind == "image" else "nlp"
                    return {"kind": kind, "payload": payload, "target": target}

                def classify_image(req: dict[str, Any]) -> dict[str, Any]:
                    return {
                        "kind": "image",
                        "label": "cat"
                        if "cat" in str(req.get("payload", ""))
                        else "other",
                        "confidence": 0.91,
                    }

                def analyze_text(req: dict[str, Any]) -> dict[str, Any]:
                    text = str(req.get("payload", ""))
                    return {
                        "kind": "text",
                        "sentiment": "pos" if "love" in text else "neu",
                        "tokens": len(text.split()),
                    }

                router.register_command(
                    "route_request", route_request, ["router", "api"]
                )
                vision.register_command(
                    "classify_image", classify_image, ["vision", "gpu"]
                )
                nlp.register_command("analyze_text", analyze_text, ["nlp", "cpu"])
                step("router + vision + nlp workers ready")

                with scenario(
                    "image path: route → vision",
                    "prod.ml",
                    "rpc.dag",
                    "rpc.locs",
                ):
                    async with MPREGClientAPI(hub) as client:
                        img = await client.request(
                            [
                                RPCCommand(
                                    name="routed",
                                    fun="route_request",
                                    args=("image", "cat photo"),
                                    locs=frozenset(["router", "api"]),
                                ),
                                RPCCommand(
                                    name="pred",
                                    fun="classify_image",
                                    args=("routed",),
                                    locs=frozenset(["vision", "gpu"]),
                                ),
                            ]
                        )
                    ensure(
                        isinstance(img, dict)
                        and img.get("pred", {}).get("label") == "cat",
                        f"img {img}",
                    )
                    ok(f"vision pred={img.get('pred')}")

                with scenario("text path: route → nlp", "rpc.dag", "rpc.locs"):
                    async with MPREGClientAPI(hub) as client:
                        txt = await client.request(
                            [
                                RPCCommand(
                                    name="routed",
                                    fun="route_request",
                                    args=("text", "I love mpreg"),
                                    locs=frozenset(["router", "api"]),
                                ),
                                RPCCommand(
                                    name="pred",
                                    fun="analyze_text",
                                    args=("routed",),
                                    locs=frozenset(["nlp", "cpu"]),
                                ),
                            ]
                        )
                    ensure(
                        isinstance(txt, dict)
                        and txt.get("pred", {}).get("sentiment") == "pos",
                        f"txt {txt}",
                    )
                    ensure(
                        txt.get("pred", {}).get("tokens") == 3,
                        f"token count {txt.get('pred')}",
                    )
                    ok(f"nlp pred={txt.get('pred')}")

                with scenario(
                    "direct worker locs bypass router", "rpc.call", "rpc.locs"
                ):
                    async with MPREGClientAPI(hub) as client:
                        direct = await client.call(
                            "classify_image",
                            {"payload": "dog photo"},
                            locs=frozenset(["vision", "gpu"]),
                        )
                    ensure(
                        isinstance(direct, dict) and direct.get("label") == "other",
                        f"direct {direct}",
                    )
                    ok(f"direct vision={direct}")

                with scenario("concurrent vision + nlp", "rpc.call"):
                    async with MPREGClientAPI(hub) as client:
                        v, t = await asyncio.gather(
                            client.call(
                                "classify_image",
                                {"payload": "cat"},
                                locs=frozenset(["vision", "gpu"]),
                            ),
                            client.call(
                                "analyze_text",
                                {"payload": "love this"},
                                locs=frozenset(["nlp", "cpu"]),
                            ),
                        )
                    ensure(
                        v.get("label") == "cat" and t.get("sentiment") == "pos",
                        f"{v} {t}",
                    )
                    ok(f"concurrent v={v} t={t}")
                    step("non-claim: not model serving platform; routing demo only")

            await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())
