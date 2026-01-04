"""Auto-port cluster bootstrap demo.

This example captures auto-assigned ports and feeds them into cluster formation
without fixed port numbers.
"""

from __future__ import annotations

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def main() -> None:
    assigned: dict[str, int] = {}

    def _capture(name: str):
        def _cb(port: int) -> None:
            assigned[name] = port

        return _cb

    server_a = MPREGServer(
        MPREGSettings(name="node-a", port=None, on_port_assigned=_capture("a"))
    )
    server_a_task = asyncio.create_task(server_a.server())

    server_a_url = f"ws://{server_a.settings.host}:{server_a.settings.port}"

    server_b = MPREGServer(
        MPREGSettings(
            name="node-b",
            port=None,
            peers=[server_a_url],
            on_port_assigned=_capture("b"),
        )
    )
    server_b_task = asyncio.create_task(server_b.server())

    try:
        await asyncio.sleep(1.0)
        server_b_url = f"ws://{server_b.settings.host}:{server_b.settings.port}"

        async with MPREGClientAPI(server_b_url) as client:
            peers = await client.list_peers()
            print("node-a url:", server_a_url)
            print("node-b url:", server_b_url)
            print("node-b peers:", peers)
    finally:
        await server_b.shutdown_async()
        await server_a.shutdown_async()
        server_b_task.cancel()
        server_a_task.cancel()
        await asyncio.gather(server_a_task, server_b_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
