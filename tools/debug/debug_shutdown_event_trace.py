#!/usr/bin/env python3
"""
Deep dive debug script to trace exactly what happens to the shutdown event
and why tasks get stuck at line 2231.
"""

import asyncio
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from mpreg.server import MPREGServer, MPREGSettings


class DebugMPREGServer(MPREGServer):
    """Instrumented version of MPREGServer to trace shutdown event behavior."""

    def __post_init__(self) -> None:
        super().__post_init__()
        self._debug_start_time = time.time()
        print(
            f"[{self.settings.name}] DEBUG: Server created at t={self._debug_start_time}"
        )
        print(
            f"[{self.settings.name}] DEBUG: Shutdown event initial state: {self._shutdown_event.is_set()}"
        )

    def shutdown(self) -> None:
        """Debug version of shutdown with detailed logging."""
        current_time = time.time()
        elapsed = current_time - self._debug_start_time
        print(f"[{self.settings.name}] DEBUG: shutdown() called at t={elapsed:.3f}s")
        print(
            f"[{self.settings.name}] DEBUG: Shutdown event before set: {self._shutdown_event.is_set()}"
        )

        self._shutdown_event.set()

        print(
            f"[{self.settings.name}] DEBUG: Shutdown event after set: {self._shutdown_event.is_set()}"
        )
        print(
            f"[{self.settings.name}] DEBUG: Background tasks count: {len(self._background_tasks)}"
        )

        # Check if any tasks are waiting on the shutdown event
        for i, task in enumerate(self._background_tasks):
            task_name = getattr(task.get_coro(), "__name__", "unknown")
            task_status = "done" if task.done() else "pending"
            print(
                f"[{self.settings.name}] DEBUG: Task {i}: {task_name} ({task_status})"
            )

    async def shutdown_async(self) -> None:
        """Debug version of shutdown_async with detailed tracing."""
        current_time = time.time()
        elapsed = current_time - self._debug_start_time
        print(
            f"[{self.settings.name}] DEBUG: shutdown_async() called at t={elapsed:.3f}s"
        )
        print(
            f"[{self.settings.name}] DEBUG: Shutdown event state: {self._shutdown_event.is_set()}"
        )
        print(
            f"[{self.settings.name}] DEBUG: Background tasks before cleanup: {len(self._background_tasks)}"
        )

        # Call the parent implementation
        await super().shutdown_async()

        final_time = time.time()
        final_elapsed = final_time - self._debug_start_time
        print(
            f"[{self.settings.name}] DEBUG: shutdown_async() completed at t={final_elapsed:.3f}s"
        )

    async def _manage_peer_connections(self) -> None:
        """Debug version of _manage_peer_connections with detailed tracing."""
        current_time = time.time()
        elapsed = current_time - self._debug_start_time
        print(
            f"[{self.settings.name}] DEBUG: _manage_peer_connections() started at t={elapsed:.3f}s"
        )

        try:
            while True:
                # Check shutdown event state before waiting
                current_time = time.time()
                elapsed = current_time - self._debug_start_time
                print(
                    f"[{self.settings.name}] DEBUG: Loop iteration at t={elapsed:.3f}s, shutdown_event.is_set()={self._shutdown_event.is_set()}"
                )

                # Send gossip messages first
                await self._send_gossip_messages()

                # Now check for shutdown
                try:
                    print(
                        f"[{self.settings.name}] DEBUG: About to wait for shutdown event with timeout={self.settings.gossip_interval}"
                    )

                    # This is line 2231 where tasks get stuck!
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=self.settings.gossip_interval,
                    )

                    # If we get here, shutdown was signaled
                    current_time = time.time()
                    elapsed = current_time - self._debug_start_time
                    print(
                        f"[{self.settings.name}] DEBUG: Shutdown event triggered at t={elapsed:.3f}s - exiting cleanly"
                    )
                    break

                except TimeoutError:
                    current_time = time.time()
                    elapsed = current_time - self._debug_start_time
                    print(
                        f"[{self.settings.name}] DEBUG: Timeout at t={elapsed:.3f}s - continuing loop"
                    )
                    continue

                except asyncio.CancelledError:
                    current_time = time.time()
                    elapsed = current_time - self._debug_start_time
                    print(
                        f"[{self.settings.name}] DEBUG: Task cancelled at t={elapsed:.3f}s - exiting"
                    )
                    break

        except Exception as e:
            current_time = time.time()
            elapsed = current_time - self._debug_start_time
            print(
                f"[{self.settings.name}] DEBUG: Exception in _manage_peer_connections at t={elapsed:.3f}s: {e}"
            )
            raise
        finally:
            current_time = time.time()
            elapsed = current_time - self._debug_start_time
            print(
                f"[{self.settings.name}] DEBUG: _manage_peer_connections() exiting at t={elapsed:.3f}s"
            )


async def debug_shutdown_event_trace():
    """Test what exactly happens to the shutdown event during task destruction."""

    print("=== SHUTDOWN EVENT DEEP DIVE DEBUG ===")
    print()

    # Create instrumented server
    settings = MPREGSettings(
        host="127.0.0.1", port=8888, name="debug-server", cluster_id="debug"
    )

    server = DebugMPREGServer(settings)

    try:
        print("1. Starting peer connection manager task...")
        peer_task = asyncio.create_task(server._manage_peer_connections())
        server._background_tasks.add(peer_task)
        peer_task.add_done_callback(server._background_tasks.discard)

        print("2. Letting task run for 0.2 seconds...")
        await asyncio.sleep(0.2)

        print("3. Calling server.shutdown()...")
        server.shutdown()

        print("4. Brief delay to let shutdown event propagate...")
        await asyncio.sleep(0.05)

        print("5. Calling server.shutdown_async()...")
        await server.shutdown_async()

        print("6. Final verification...")
        print(f"   Final background tasks: {len(server._background_tasks)}")
        print(f"   Peer task done: {peer_task.done()}")

        if peer_task.done():
            if peer_task.exception():
                print(f"   Peer task exception: {peer_task.exception()}")
            else:
                print(f"   Peer task result: {peer_task.result()}")
        else:
            print("   Peer task still pending!")

        return True

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        # Force cleanup
        try:
            server.shutdown()
            await server.shutdown_async()
        except TimeoutError:
            pass


if __name__ == "__main__":
    try:
        success = asyncio.run(debug_shutdown_event_trace())
        if success:
            print("\n✅ DEBUG TRACE COMPLETED")
        else:
            print("\n❌ DEBUG TRACE FAILED")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ SCRIPT FAILED: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
