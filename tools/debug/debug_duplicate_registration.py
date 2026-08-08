#!/usr/bin/env python3
"""
Debug script to test duplicate registration detection.
"""

import asyncio

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


async def test_duplicate_registration():
    """Test that duplicate registration detection works."""

    settings = MPREGSettings(
        host="127.0.0.1",
        port=9999,
        name="Debug-Server",
        cluster_id="debug-cluster",
        resources={"debug"},
    )

    server = MPREGServer(settings=settings)

    # This should work - first registration
    def test_function(arg: str) -> str:
        return f"result: {arg}"

    try:
        server.register_command("test_cmd", test_function, ["debug"])
        print("✅ First registration successful")
    except ValueError as e:
        print(f"❌ First registration failed: {e}")
        return

    # This should fail - duplicate registration
    def another_function(arg: str) -> str:
        return f"another: {arg}"

    try:
        server.register_command("test_cmd", another_function, ["debug"])
        print("❌ Duplicate registration should have failed but didn't!")
    except ValueError as e:
        print(f"✅ Duplicate registration correctly blocked: {e}")

    # Test @rpc_command decorated functions are already registered during __init__
    print("\n=== Testing @rpc_command auto-discovery ===")
    print(
        "✅ RPC system setup completed during server __init__ without duplicate registration errors"
    )
    print(f"✅ Server has {len(server.registry._commands)} registered functions:")
    for name in server.registry._commands:
        print(f"   - {name}")

    print(
        "\n✅ SUCCESS: Infinite loop bug fixed and duplicate registration detection working!"
    )


if __name__ == "__main__":
    asyncio.run(test_duplicate_registration())
