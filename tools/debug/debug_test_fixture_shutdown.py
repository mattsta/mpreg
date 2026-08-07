#!/usr/bin/env python3
"""
Test that simulates exactly how the test fixtures shut down servers.

This reproduces the exact sequence that was causing task destruction warnings.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from mpreg.server import MPREGServer, MPREGSettings


async def test_fixture_shutdown_sequence():
    """Test the exact shutdown sequence used by pytest fixtures."""

    print("=== TEST FIXTURE SHUTDOWN SIMULATION ===")
    print()

    # Create server exactly like the fixtures do
    settings = MPREGSettings(
        host="127.0.0.1", port=8777, name="fixture-test", cluster_id="test-cluster"
    )

    server = MPREGServer(settings)
    servers = [server]  # Simulate list like in fixtures

    try:
        print("1. Starting server background task...")
        # This simulates server.server() task creation in fixtures
        server_task = asyncio.create_task(server.server())
        tasks = [server_task]  # Simulate task list like in fixtures

        print("2. Letting server run briefly...")
        await asyncio.sleep(0.3)

        print("3. Simulating test fixture shutdown sequence...")

        # This is the EXACT sequence from conftest.py lines 58-74
        shutdown_tasks = []
        for srv in servers:
            try:
                # The fixtures call shutdown_async() DIRECTLY without shutdown() first
                shutdown_tasks.append(asyncio.create_task(srv.shutdown_async()))
            except Exception as e:
                print(f"Error initiating server shutdown: {e}")

        # Wait for all servers to shut down properly
        if shutdown_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*shutdown_tasks, return_exceptions=True), timeout=5.0
                )
                print("✅ All servers shut down within timeout")
            except TimeoutError:
                print("⚠️  Some servers did not shut down within timeout")
            except Exception as e:
                print(f"⚠️  Error during server shutdown: {e}")

        # Cancel remaining tasks (this is also from conftest.py)
        if tasks:
            for task in tasks:
                if not task.done():
                    task.cancel()

            # Wait for cancelled tasks
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), timeout=5.0
                )
                print("✅ All tasks completed/cancelled within timeout")
            except TimeoutError:
                print("⚠️  Some tasks did not complete within timeout")
            except Exception as e:
                print(f"⚠️  Error during task cleanup: {e}")

        # Check final state
        print("4. Final verification:")
        print(f"   Server background tasks: {len(server._background_tasks)}")
        print(f"   Server task done: {server_task.done()}")
        print(f"   Shutdown event set: {server._shutdown_event.is_set()}")

        if len(server._background_tasks) == 0 and server_task.done():
            print("✅ SUCCESS: Complete clean shutdown!")
            return True
        else:
            print("❌ FAILURE: Incomplete shutdown")
            return False

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(test_fixture_shutdown_sequence())
        if success:
            print("\n🎉 FIXTURE SHUTDOWN TEST PASSED!")
        else:
            print("\n❌ FIXTURE SHUTDOWN TEST FAILED!")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ TEST SCRIPT FAILED: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
