"""L2 media_pipeline — multi-stage ETL-style RPC across specialized nodes."""

from __future__ import annotations

import asyncio
from typing import Any

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import ensure, ok, run_with_servers, step
from mpreg.server import MPREGServer

async def main() -> None:
    with port_range_context(4, "servers") as ports:
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
                return {"media_id": media_id, "bytes": bytes_hint, "stage": "ingested"}

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
                    "stage": "stored",
                }

            ingest.register_command("ingest_frame", ingest_frame, ["ingestion", "raw"])
            process.register_command("transcode", transcode, ["processing", "etl"])
            analytics.register_command("analyze", analyze, ["analytics", "ml"])
            storage.register_command("store", store, ["storage", "db"])
            step("4-stage media pipeline registered")

            async with MPREGClientAPI(hub) as client:
                result = await client._client.request(
                    [
                        RPCCommand(
                            name="ingested",
                            fun="ingest_frame",
                            args=("vid-1", 500_000),
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
                )

            ensure(isinstance(result, dict), "expected dict")
            stored = result.get("stored")
            ensure(isinstance(stored, dict), f"missing stored {result}")
            ensure(stored.get("stored") is True, f"not stored {stored}")
            ensure(stored.get("media_id") == "vid-1", f"bad id {stored}")
            ensure(abs(float(stored.get("quality", 0)) - 0.5) < 1e-6, f"bad quality {stored}")
            ok(f"media pipeline stored={stored}")

        await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
