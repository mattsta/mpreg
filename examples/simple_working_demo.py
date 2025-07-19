#!/usr/bin/env python3
"""Simple working demo that definitely works."""

import asyncio

from mpreg.client_api import MPREGClientAPI
from mpreg.config import MPREGSettings
from mpreg.model import RPCCommand
from mpreg.server import MPREGServer


async def main():
    print("🚀 Simple MPREG Demo")
    print("=" * 30)

    # Start a simple server
    server = MPREGServer(
        MPREGSettings(
            port=9001, name="Demo-Server", resources={"demo"}, log_level="WARNING"
        )
    )

    # Register simple functions
    def add_numbers(a: int, b: int) -> int:
        return a + b

    def multiply_by_two(x: int) -> int:
        return x * 2

    server.register_command("add", add_numbers, ["demo"])
    server.register_command("double", multiply_by_two, ["demo"])

    # Start server
    server_task = asyncio.create_task(server.server())
    await asyncio.sleep(0.5)

    try:
        async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
            # Simple dependency chain
            print("🔗 Testing dependency resolution...")

            result = await client._client.request(
                [
                    RPCCommand(
                        name="sum", fun="add", args=(10, 20), locs=frozenset(["demo"])
                    ),
                    RPCCommand(
                        name="final",
                        fun="double",
                        args=("sum",),
                        locs=frozenset(["demo"]),
                    ),
                ]
            )

            print(f"✅ Result: {result}")
            print(f"✅ Final value: {result.get('final', 'Not found')}")

            # Test concurrent calls
            print("\n⚡ Testing concurrent execution...")

            tasks = []
            for i in range(5):
                task = client.call("add", i, i + 1, locs=frozenset(["demo"]))
                tasks.append(task)

            results = await asyncio.gather(*tasks)
            print(f"✅ Concurrent results: {results}")

            print("\n🎉 Demo completed successfully!")

    finally:
        server._shutdown_event.set()
        await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main())
