"""L2 media_pipeline — 4-stage ETL RPC DAG across specialized nodes (prod.media)."""

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
    with (
        app_run("media_pipeline", "Media Pipeline — 4-stage DAG", level="L2"),
        port_range_context(4, "servers") as ports,
    ):
        hub = f"ws://127.0.0.1:{ports[0]}"
        settings = [
            MPREGSettings(
                port=ports[0],
                name="Ingest",
                resources={"ingestion", "raw"},
                log_level="WARNING",
            ),
            MPREGSettings(
                port=ports[1],
                name="Process",
                resources={"processing", "etl"},
                peers=[hub],
                log_level="WARNING",
            ),
            MPREGSettings(
                port=ports[2],
                name="Analytics",
                resources={"analytics", "ml"},
                peers=[hub],
                log_level="WARNING",
            ),
            MPREGSettings(
                port=ports[3],
                name="Storage",
                resources={"storage", "db"},
                peers=[hub],
                log_level="WARNING",
            ),
        ]

        async def _run(servers: list[MPREGServer]) -> None:
            ingest, process, analytics, storage = servers

            def ingest_frame(media_id: str, bytes_hint: int) -> dict[str, Any]:
                return {
                    "media_id": media_id,
                    "bytes": bytes_hint,
                    "stage": "ingested",
                }

            def transcode(frame: dict[str, Any]) -> dict[str, Any]:
                return {**frame, "codec": "h264", "stage": "transcoded"}

            def analyze(frame: dict[str, Any]) -> dict[str, Any]:
                score = min(1.0, float(frame.get("bytes", 0)) / 1_000_000.0)
                return {**frame, "quality": score, "stage": "analyzed"}

            def store(frame: dict[str, Any]) -> dict[str, Any]:
                return {
                    "media_id": frame["media_id"],
                    "stored": True,
                    "quality": frame.get("quality"),
                    "codec": frame.get("codec"),
                    "stage": "stored",
                }

            ingest.register_command("ingest_frame", ingest_frame, ["ingestion", "raw"])
            process.register_command("transcode", transcode, ["processing", "etl"])
            analytics.register_command("analyze", analyze, ["analytics", "ml"])
            storage.register_command("store", store, ["storage", "db"])
            step("4-stage media pipeline registered")

            def _pipeline_cmds(media_id: str, nbytes: int) -> list[RPCCommand]:
                return [
                    RPCCommand(
                        name="ingested",
                        fun="ingest_frame",
                        args=(media_id, nbytes),
                        locs=frozenset(["ingestion", "raw"]),
                    ),
                    RPCCommand(
                        name="transcoded",
                        fun="transcode",
                        args=("ingested",),
                        locs=frozenset(["processing", "etl"]),
                    ),
                    RPCCommand(
                        name="analyzed",
                        fun="analyze",
                        args=("transcoded",),
                        locs=frozenset(["analytics", "ml"]),
                    ),
                    RPCCommand(
                        name="stored",
                        fun="store",
                        args=("analyzed",),
                        locs=frozenset(["storage", "db"]),
                    ),
                ]

            with scenario(
                "full 4-stage DAG",
                "prod.media",
                "rpc.dag",
                "rpc.locs",
                "rpc.register",
            ):
                async with MPREGClientAPI(hub) as client:
                    result = await client.request(_pipeline_cmds("vid-1", 500_000))
                ensure(isinstance(result, dict), "expected dict")
                stored = result.get("stored")
                ensure(isinstance(stored, dict), f"missing stored {result}")
                ensure(stored.get("stored") is True, f"not stored {stored}")
                ensure(stored.get("media_id") == "vid-1", f"bad id {stored}")
                ensure(
                    abs(float(stored.get("quality", 0)) - 0.5) < 1e-6,
                    f"bad quality {stored}",
                )
                ensure(stored.get("codec") == "h264", f"codec missing {stored}")
                ok(f"stored={stored}")

            with scenario("second media independent DAG", "rpc.dag", "rpc.locs"):
                async with MPREGClientAPI(hub) as client:
                    r2 = await client.request(_pipeline_cmds("vid-2", 1_000_000))
                s2 = r2.get("stored") if isinstance(r2, dict) else None
                ensure(isinstance(s2, dict) and s2.get("media_id") == "vid-2", f"{s2}")
                ensure(
                    abs(float(s2.get("quality", 0)) - 1.0) < 1e-6,
                    f"quality should cap at 1.0 got {s2}",
                )
                ok(f"vid-2 stored quality={s2.get('quality')}")

            with scenario("direct stage call via locs", "rpc.call", "rpc.locs"):
                async with MPREGClientAPI(hub) as client:
                    only = await client.call(
                        "analyze",
                        {
                            "media_id": "x",
                            "bytes": 250_000,
                            "stage": "transcoded",
                            "codec": "h264",
                        },
                        locs=frozenset(["analytics", "ml"]),
                    )
                ensure(
                    isinstance(only, dict)
                    and abs(float(only.get("quality", 0)) - 0.25) < 1e-6,
                    f"direct analyze {only}",
                )
                ok(f"direct analyze={only}")
                step("non-claim: not durable media store; in-memory stages only")

        await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())
