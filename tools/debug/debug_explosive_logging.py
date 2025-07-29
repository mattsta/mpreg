#!/usr/bin/env python3
"""
DEEP DIVE DEBUG: Find exactly where system logging explodes instead of staying controlled.

This script creates minimal clusters with VERY LIMITED logging to identify:
1. At what exact point does logging become explosive?
2. What specific component is generating the log explosion?
3. What triggers the uncontrolled behavior?
"""

import asyncio
import logging
import time

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer
from tests.port_allocator import PortAllocator


class LogExplosionDetector:
    """Detects where logging becomes explosive instead of controlled."""

    def __init__(self):
        self.port_allocator = PortAllocator()
        self.servers: list[MPREGServer] = []
        self.tasks: list[asyncio.Task] = []
        self.log_counts: dict[str, int] = {}
        self.original_log_level = None

        # Disable ALL logging initially
        self.suppress_all_logging()

    def suppress_all_logging(self):
        """Completely suppress all logging to see basic behavior."""
        self.original_log_level = logging.getLogger().level
        logging.getLogger().setLevel(logging.CRITICAL + 1)  # Above CRITICAL

        # Specifically disable mpreg logging
        logging.getLogger("mpreg").setLevel(logging.CRITICAL + 1)
        logging.getLogger("mpreg.server").setLevel(logging.CRITICAL + 1)
        logging.getLogger("mpreg.core").setLevel(logging.CRITICAL + 1)

    def enable_minimal_logging(self):
        """Enable only ERROR level logging."""
        logging.getLogger().setLevel(logging.ERROR)
        logging.getLogger("mpreg").setLevel(logging.ERROR)
        logging.getLogger("mpreg.server").setLevel(logging.ERROR)
        logging.getLogger("mpreg.core").setLevel(logging.ERROR)

    def enable_targeted_logging(self, component: str):
        """Enable logging for a specific component only."""
        self.suppress_all_logging()
        logging.getLogger(f"mpreg.{component}").setLevel(logging.INFO)

    async def test_minimal_cluster(self, size: int, description: str) -> dict:
        """Test creating a minimal cluster with zero logging."""
        print(f"\n🧪 TEST: {description} ({size} nodes)")

        start_time = time.time()
        servers = []

        try:
            # Phase 1: Create servers (no connections yet)
            ports = self.port_allocator.allocate_port_range(size, "testing")
            print(f"   📍 Ports allocated: {ports[0]}-{ports[-1]}")

            for i, port in enumerate(ports):
                settings = MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"debug-{i}",
                    cluster_id="debug-cluster",
                    resources={f"res-{i}"},
                    peers=None,
                    connect=None,  # NO AUTO-CONNECT initially
                    advertised_urls=None,
                    gossip_interval=2.0,  # Slow down gossip
                )

                server = MPREGServer(settings=settings)
                servers.append(server)

                # Start server with NO connections
                task = asyncio.create_task(server.server())
                self.tasks.append(task)

                # NO delay - see if this causes issues

            self.servers.extend(servers)
            print(f"   ✅ Created {len(servers)} isolated servers")

            # Phase 2: Wait and see if isolated servers are stable
            await asyncio.sleep(0.5)
            print("   ✅ Isolated servers stable")

            # Phase 3: Create ONE connection and see what happens
            if len(servers) > 1:
                print(
                    f"   🔗 Creating single connection: {servers[1].settings.name} -> {servers[0].settings.name}"
                )
                await servers[1]._establish_peer_connection(
                    f"ws://127.0.0.1:{servers[0].settings.port}"
                )
                await asyncio.sleep(0.2)
                print("   ✅ Single connection established")

            # Phase 4: Create second connection and observe
            if len(servers) > 2:
                print(
                    f"   🔗 Creating second connection: {servers[2].settings.name} -> {servers[0].settings.name}"
                )
                await servers[2]._establish_peer_connection(
                    f"ws://127.0.0.1:{servers[0].settings.port}"
                )
                await asyncio.sleep(0.2)
                print("   ✅ Second connection established")

            # Phase 5: Wait and see if system remains stable
            await asyncio.sleep(1.0)

            # Check system state
            total_connections = sum(len(s.peer_connections) for s in servers)
            setup_time = (time.time() - start_time) * 1000

            result = {
                "success": True,
                "size": size,
                "setup_time_ms": setup_time,
                "total_connections": total_connections,
                "servers_created": len(servers),
                "error": None,
            }

            print(f"   ✅ SUCCESS: {total_connections} connections, {setup_time:.0f}ms")
            return result

        except Exception as e:
            error_time = (time.time() - start_time) * 1000
            print(f"   ❌ FAILED: {e}")

            return {
                "success": False,
                "size": size,
                "setup_time_ms": error_time,
                "total_connections": 0,
                "servers_created": len(servers),
                "error": str(e),
            }

    async def test_connection_explosion(self):
        """Test if connections cause explosive logging."""
        print("\n🔥 TESTING: Connection-driven log explosion")

        # Create 3 servers
        ports = self.port_allocator.allocate_port_range(3, "testing")
        servers = []

        for i, port in enumerate(ports):
            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"conn-test-{i}",
                cluster_id="connection-test",
                resources={f"conn-res-{i}"},
                peers=None,
                connect=None,
                advertised_urls=None,
                gossip_interval=5.0,  # Very slow gossip
            )

            server = MPREGServer(settings=settings)
            servers.append(server)

            task = asyncio.create_task(server.server())
            self.tasks.append(task)

        self.servers.extend(servers)
        await asyncio.sleep(0.3)

        # Now enable INFO logging and make ONE connection
        print("   🔍 Enabling INFO logging and creating connection...")
        self.enable_minimal_logging()

        # Create connection and measure log output
        log_start = time.time()
        await servers[1]._establish_peer_connection(
            f"ws://127.0.0.1:{servers[0].settings.port}"
        )
        await asyncio.sleep(2.0)  # Wait for potential log explosion
        log_end = time.time()

        print(f"   ⏱️  Log observation period: {(log_end - log_start):.1f}s")
        print("   📊 If logs exploded, they should be visible above")

        self.suppress_all_logging()

    async def test_function_registration_explosion(self):
        """Test if function registration causes explosive logging."""
        print("\n📝 TESTING: Function registration log explosion")

        # Create 2 connected servers
        ports = self.port_allocator.allocate_port_range(2, "testing")
        servers = []

        for i, port in enumerate(ports):
            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"func-test-{i}",
                cluster_id="function-test",
                resources={f"func-res-{i}"},
                peers=None,
                connect=f"ws://127.0.0.1:{ports[0]}" if i > 0 else None,
                advertised_urls=None,
                gossip_interval=10.0,  # Very slow gossip
            )

            server = MPREGServer(settings=settings)
            servers.append(server)

            task = asyncio.create_task(server.server())
            self.tasks.append(task)

        self.servers.extend(servers)
        await asyncio.sleep(1.0)

        # Enable logging and register a function
        print("   🔍 Enabling INFO logging and registering function...")
        self.enable_minimal_logging()

        def test_function(data: str) -> str:
            return f"processed: {data}"

        # Register function and watch for log explosion
        log_start = time.time()
        servers[0].register_command("test_func", test_function, ["test-resource"])
        await asyncio.sleep(3.0)  # Wait for potential federation explosion
        log_end = time.time()

        print(f"   ⏱️  Log observation period: {(log_end - log_start):.1f}s")
        print("   📊 Function registration logs should be visible above")

        self.suppress_all_logging()

    async def cleanup(self):
        """Clean up all resources."""
        print("   🧹 Cleaning up...")

        # Cancel tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

        self.servers.clear()
        self.tasks.clear()

        # Port allocator doesn't have release_all_ports method
        # Individual ports are released automatically

        # Restore original logging
        if self.original_log_level is not None:
            logging.getLogger().setLevel(self.original_log_level)

    async def run_debug_investigation(self):
        """Run systematic debug investigation with controlled logging."""
        print("🔬 EXPLOSIVE LOGGING DEBUG INVESTIGATION")
        print("=" * 60)
        print("⚠️  All logging initially SUPPRESSED to observe basic behavior")

        # Test 1: Minimal isolated servers
        await self.test_minimal_cluster(3, "Isolated servers (no connections)")
        await self.cleanup()

        # Test 2: Connection behavior
        await self.test_connection_explosion()
        await self.cleanup()

        # Test 3: Function registration behavior
        await self.test_function_registration_explosion()
        await self.cleanup()

        # Test 4: Gradual size increase with controlled logging
        for size in [5, 8, 10]:
            result = await self.test_minimal_cluster(size, "Controlled cluster growth")
            await self.cleanup()

            if not result["success"]:
                print(
                    f"   💡 System fails at {size} nodes even with controlled logging"
                )
                break

        print("\n✅ DEBUG INVESTIGATION COMPLETE")
        print(
            "📋 If no explosive logging appeared above, the issue is in normal operation"
        )
        print(
            "📋 If explosive logging appeared, it should be clearly visible in the phases above"
        )


async def main():
    """Run the explosive logging debug investigation."""
    detector = LogExplosionDetector()
    try:
        await detector.run_debug_investigation()
    finally:
        await detector.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
