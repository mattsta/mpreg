#!/usr/bin/env python3
"""
Debug script for intermediate results timeout issue.
"""

import asyncio
import contextlib
import logging

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand, RPCRequest
from mpreg.server import MPREGServer

# Set up logging
logging.basicConfig(level=logging.INFO)


async def test_minimal_intermediate_results():
    """Test minimal intermediate results to isolate timeout issue."""
    port = 29500

    settings = MPREGSettings(
        host="127.0.0.1",
        port=port,
        name="Debug-Server",
        cluster_id="debug-cluster",
        resources={"cpu"},
        gossip_interval=1.0,
    )

    server = MPREGServer(settings=settings)

    # Register simple function
    def simple_func(data: str) -> str:
        return f"processed_{data}"

    server.register_command("simple_func", simple_func, ["cpu"])

    # Start server
    server_task = asyncio.create_task(server.server())
    await asyncio.sleep(0.5)  # Short startup time

    try:
        # Create client
        client = MPREGClientAPI(f"ws://127.0.0.1:{port}")
        await client.connect()

        print("Client connected, testing basic request...")

        # Test basic request first (no enhanced features)
        basic_request = RPCRequest(
            cmds=(
                RPCCommand(
                    name="test1",
                    fun="simple_func",
                    args=("basic_test",),
                    locs=frozenset(["cpu"]),
                ),
            ),
            u="basic_test_request",
        )

        basic_response = await client._client.request_enhanced(basic_request)
        print(f"Basic request successful: {basic_response.r}")

        print("Testing enhanced request with timeout...")

        # Test enhanced request with short timeout
        enhanced_request = RPCRequest(
            cmds=(
                RPCCommand(
                    name="test2",
                    fun="simple_func",
                    args=("enhanced_test",),
                    locs=frozenset(["cpu"]),
                ),
            ),
            u="enhanced_test_request",
            return_intermediate_results=True,
        )

        # Use explicit timeout to prevent hanging
        enhanced_response = await client._client.request_enhanced(
            enhanced_request, timeout=10.0
        )
        print(f"Enhanced request successful: {enhanced_response.r}")
        print(f"Intermediate results: {len(enhanced_response.intermediate_results)}")

        await client.disconnect()

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        server_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await server_task


if __name__ == "__main__":
    asyncio.run(test_minimal_intermediate_results())
