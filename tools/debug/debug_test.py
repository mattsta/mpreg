#!/usr/bin/env python3
"""Simple debug test to check server startup and shutdown."""

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def test_simple_server():
    """Test basic server startup and shutdown."""
    print("Creating server...")
    server = MPREGServer(
        MPREGSettings(
            port=9999, name="Debug-Server", resources={"test"}, log_level="INFO"
        )
    )

    print("Starting server...")
    server_task = asyncio.create_task(server.server())

    # Wait for server to start
    await asyncio.sleep(0.5)

    try:
        print("Testing client connection...")
        async with MPREGClientAPI("ws://127.0.0.1:9999") as client:
            result = await client.call("echo", "test")
            print(f"Echo result: {result}")
            assert result == "test"

        print("Test passed!")

    finally:
        print("Shutting down server...")
        server.shutdown()

        # Cancel task
        server_task.cancel()

        try:
            await asyncio.wait_for(server_task, timeout=2.0)
        except TimeoutError, asyncio.CancelledError:
            print("Server shutdown completed")


if __name__ == "__main__":
    asyncio.run(test_simple_server())
