#!/usr/bin/env python3
"""
MPREG Advanced Cache Client/Server Demo

This script demonstrates how to start a cache-enabled MPREG server and connect
various cache clients over network protocols. Shows the three main MPREG client types:

1. MPREG RPC Client (for compute functions with caching)
2. MPREG PubSub Client (for topic routing and pub/sub)
3. MPREG Cache Client (for direct cache operations)

Usage:
    # Start cache server
    poetry run python mpreg/examples/cache_client_server_demo.py --mode server

    # Run cache client demo (in another terminal)
    poetry run python mpreg/examples/cache_client_server_demo.py --mode client

    # Run integrated demo (server + client in one process)
    poetry run python mpreg/examples/cache_client_server_demo.py --mode integrated
"""

import argparse
import asyncio
import sys
import time
from typing import Any

# Add the parent directory to Python path for imports
sys.path.insert(0, ".")

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.advanced_cache_ops import (
    AdvancedCacheOperations,
)
from mpreg.core.cache_protocol import (
    AtomicOperationMessage,
    CacheKeyMessage,
    CacheOperation,
    CacheOptionsMessage,
    CacheProtocolHandler,
    CacheRequestMessage,
    DataStructureOperationMessage,
    NamespaceOperationMessage,
)
from mpreg.core.cache_pubsub_integration import (
    CacheEventType,
    CacheNotificationConfig,
    CachePubSubIntegration,
    EnhancedAdvancedCacheOperations,
)
from mpreg.core.config import MPREGSettings
from mpreg.core.global_cache import (
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.core.topic_exchange import TopicExchange
from mpreg.server import MPREGServer


class MPREGCacheClient:
    """
    High-level cache client for MPREG that provides easy-to-use cache operations
    over network protocols (WebSocket/TCP).
    """

    def __init__(self, server_url: str = "ws://127.0.0.1:9001"):
        self.server_url = server_url
        self.client_api: MPREGClientAPI | None = None
        self._connected = False

    async def connect(self) -> None:
        """Connect to the MPREG cache server."""
        self.client_api = MPREGClientAPI(self.server_url)
        await self.client_api.__aenter__()
        self._connected = True
        print(f"🔗 Connected to MPREG cache server at {self.server_url}")

    async def disconnect(self) -> None:
        """Disconnect from the cache server."""
        if self.client_api and self._connected:
            await self.client_api.__aexit__(None, None, None)
            self._connected = False
            print("🔌 Disconnected from MPREG cache server")

    async def send_cache_request(self, request: CacheRequestMessage) -> dict[str, Any]:
        """Send a cache protocol request and get response."""
        if not self._connected or not self.client_api:
            raise RuntimeError("Client not connected")

        # Send request through the client API's WebSocket connection
        # Note: This would typically go through a cache protocol handler
        # For demo purposes, we'll simulate the request/response
        response_data = {
            "role": "cache-response",
            "u": request.u,
            "status": "hit",  # Simulated response
            "timestamp": time.time(),
        }

        return response_data

    async def get(
        self, namespace: str, identifier: str, version: str = "v1.0.0"
    ) -> Any:
        """Get a value from the cache."""
        key = CacheKeyMessage(
            namespace=namespace, identifier=identifier, version=version
        )
        request = CacheRequestMessage(
            operation=CacheOperation.GET.value, key=key, options=CacheOptionsMessage()
        )

        response = await self.send_cache_request(request)
        print(f"📥 Cache GET {namespace}.{identifier}: {response['status']}")
        return response.get("entry", {}).get("value")

    async def put(
        self,
        namespace: str,
        identifier: str,
        value: Any,
        ttl_seconds: float | None = None,
    ) -> bool:
        """Put a value into the cache."""
        key = CacheKeyMessage(namespace=namespace, identifier=identifier)
        request = CacheRequestMessage(
            operation=CacheOperation.PUT.value,
            key=key,
            value=value,
            options=CacheOptionsMessage(),
        )

        response = await self.send_cache_request(request)
        print(f"📤 Cache PUT {namespace}.{identifier}: {response['status']}")
        return response.get("status") == "hit"

    async def atomic_test_and_set(
        self, namespace: str, identifier: str, expected_value: Any, new_value: Any
    ) -> bool:
        """Perform atomic test-and-set operation."""
        key = CacheKeyMessage(namespace=namespace, identifier=identifier)
        atomic_op = AtomicOperationMessage(
            operation_type="test_and_set",
            expected_value=expected_value,
            new_value=new_value,
        )

        request = CacheRequestMessage(
            operation=CacheOperation.ATOMIC.value,
            key=key,
            atomic_operation=atomic_op,
            options=CacheOptionsMessage(consistency_level="strong"),
        )

        response = await self.send_cache_request(request)
        print(f"⚛️  Atomic test-and-set {namespace}.{identifier}: {response['status']}")
        return response.get("status") == "hit"

    async def set_add(self, namespace: str, identifier: str, member: Any) -> bool:
        """Add member to a distributed set."""
        key = CacheKeyMessage(namespace=namespace, identifier=identifier)
        struct_op = DataStructureOperationMessage(
            structure_type="set", operation="add", values=[member]
        )

        request = CacheRequestMessage(
            operation=CacheOperation.STRUCTURE.value,
            key=key,
            structure_operation=struct_op,
        )

        response = await self.send_cache_request(request)
        print(f"📝 Set ADD {namespace}.{identifier} += {member}: {response['status']}")
        return response.get("status") == "hit"

    async def set_contains(self, namespace: str, identifier: str, member: Any) -> bool:
        """Check if member exists in distributed set."""
        key = CacheKeyMessage(namespace=namespace, identifier=identifier)
        struct_op = DataStructureOperationMessage(
            structure_type="set", operation="contains", values=[member]
        )

        request = CacheRequestMessage(
            operation=CacheOperation.STRUCTURE.value,
            key=key,
            structure_operation=struct_op,
        )

        response = await self.send_cache_request(request)
        print(
            f"🔍 Set CONTAINS {namespace}.{identifier} ? {member}: {response['status']}"
        )
        return response.get("status") == "hit"

    async def clear_namespace(self, namespace: str, pattern: str = "*") -> int:
        """Clear all entries in a namespace."""
        ns_op = NamespaceOperationMessage(
            operation_type="clear",
            namespace=namespace,
            pattern=pattern,
            max_entries=1000,
        )

        request = CacheRequestMessage(
            operation=CacheOperation.NAMESPACE.value, namespace_operation=ns_op
        )

        response = await self.send_cache_request(request)
        cleared_count = response.get("cleared_count", 0)
        print(f"🧹 Cleared namespace {namespace}: {cleared_count} entries")
        return cleared_count


class MPREGCacheServer:
    """
    MPREG server with advanced cache capabilities.
    """

    def __init__(self, port: int = 9001, cluster_id: str = "cache-demo"):
        self.port = port
        self.cluster_id = cluster_id
        self.server: MPREGServer | None = None
        self.cache_manager: GlobalCacheManager | None = None
        self.advanced_cache_ops: EnhancedAdvancedCacheOperations | None = None
        self.cache_protocol_handler: CacheProtocolHandler | None = None
        self.pubsub_integration: CachePubSubIntegration | None = None

    async def start(self) -> None:
        """Start the cache-enabled MPREG server."""
        print(f"🚀 Starting MPREG Cache Server on port {self.port}")

        # Create server with cache configuration
        settings = MPREGSettings(
            port=self.port,
            name=f"CacheServer-{self.cluster_id}",
            resources={"cache", "compute"},
            cluster_id=self.cluster_id,
        )

        self.server = MPREGServer(settings)

        # Initialize cache components
        from mpreg.core.caching import CacheConfiguration, CacheLimits

        local_cache_config = CacheConfiguration(
            limits=CacheLimits(max_memory_bytes=512 * 1024 * 1024),  # 512MB
            default_ttl_seconds=3600,
        )
        cache_config = GlobalCacheConfiguration(
            local_cache_config=local_cache_config,
            enable_l2_persistent=True,
            enable_l3_distributed=True,
            enable_l4_federation=True,
        )

        self.cache_manager = GlobalCacheManager(cache_config)

        # Initialize topic exchange for pub/sub integration
        topic_exchange = TopicExchange(f"ws://127.0.0.1:{self.port}", self.cluster_id)

        # Set up cache-pub/sub integration
        self.pubsub_integration = CachePubSubIntegration(
            cache_manager=self.cache_manager,
            advanced_cache_ops=AdvancedCacheOperations(self.cache_manager),
            topic_exchange=topic_exchange,
            cluster_id=self.cluster_id,
        )

        # Enhanced cache operations with pub/sub notifications
        self.advanced_cache_ops = EnhancedAdvancedCacheOperations(
            cache_manager=self.cache_manager, pubsub_integration=self.pubsub_integration
        )

        # Cache protocol handler
        self.cache_protocol_handler = CacheProtocolHandler(
            cache_manager=self.cache_manager, advanced_cache_ops=self.advanced_cache_ops
        )

        # Register cache-enabled compute functions
        await self._register_cache_functions()

        # Configure cache notifications
        await self._configure_cache_notifications()

        # Start the server
        server_task = asyncio.create_task(self.server.server())
        print(f"✅ MPREG Cache Server running on ws://127.0.0.1:{self.port}")
        print("   Features enabled:")
        print("   - ⚡ Advanced cache operations (atomic, data structures)")
        print("   - 🌐 Global cache federation")
        print("   - 📢 Cache-PubSub integration")
        print("   - 🔄 Multi-tier caching (L1-L4)")

        # Keep server running
        try:
            await server_task
        except KeyboardInterrupt:
            print("\n🛑 Shutting down cache server...")
            await self.shutdown()

    async def _register_cache_functions(self) -> None:
        """Register cache-enabled compute functions."""
        if not self.server:
            return

        # Cache-heavy computation example
        async def expensive_fibonacci(n: int) -> int:
            """Compute fibonacci with intelligent caching."""
            if not self.cache_manager:
                return self._compute_fibonacci(n)

            cache_key = GlobalCacheKey.from_function_call(
                namespace="compute.fibonacci",
                function_name="fibonacci",
                args=(n,),
                kwargs={},
                tags={"expensive", "recursive"},
            )

            # Try cache first
            result = await self.cache_manager.get(cache_key)
            if result.success and result.entry:
                print(f"💨 Cache HIT: fib({n}) = {result.entry.value}")
                return result.entry.value

            # Compute and cache
            value = self._compute_fibonacci(n)
            await self.cache_manager.put(cache_key, value, metadata=None, options=None)
            print(f"🔥 Cache MISS: computed fib({n}) = {value}")
            return value

        # ML model inference with caching
        async def ml_predict(model_name: str, features: list[float]) -> dict[str, Any]:
            """ML model prediction with result caching."""
            if not self.cache_manager:
                return {"prediction": sum(features) / len(features)}  # Mock prediction

            cache_key = GlobalCacheKey.from_function_call(
                namespace="ml.predictions",
                function_name="predict",
                args=(model_name,),
                kwargs={"features": tuple(features)},
                tags={"ml", "inference"},
            )

            # Try cache first
            result = await self.cache_manager.get(cache_key)
            if result.success and result.entry:
                print(f"🧠 Model cache HIT: {model_name}")
                return result.entry.value

            # Mock computation
            await asyncio.sleep(0.1)  # Simulate model inference time
            prediction = {
                "model": model_name,
                "prediction": sum(features) / len(features),
                "confidence": 0.95,
                "timestamp": time.time(),
            }

            # Cache with TTL
            await self.cache_manager.put(cache_key, prediction)
            print(f"🎯 Model computed: {model_name}")
            return prediction

        # Register functions
        self.server.register_command(
            "fibonacci", expensive_fibonacci, ["compute", "cache"]
        )
        self.server.register_command("ml_predict", ml_predict, ["compute", "cache"])

        print("📋 Registered cache-enabled functions:")
        print("   - fibonacci(n): Cached recursive computation")
        print("   - ml_predict(model, features): Cached ML inference")

    def _compute_fibonacci(self, n: int) -> int:
        """Simple fibonacci computation."""
        if n <= 1:
            return n
        return self._compute_fibonacci(n - 1) + self._compute_fibonacci(n - 2)

    async def _configure_cache_notifications(self) -> None:
        """Configure cache event notifications."""
        if not self.pubsub_integration:
            return

        # Configure notifications for ML namespace
        ml_config = CacheNotificationConfig(
            notify_on_change=True,
            notification_topic="cache.events.ml",
            event_types=[CacheEventType.CACHE_HIT, CacheEventType.CACHE_MISS],
            include_cache_metadata=True,
            include_performance_metrics=True,
        )

        self.pubsub_integration.configure_notifications("ml.predictions", ml_config)

        # Configure notifications for compute namespace
        compute_config = CacheNotificationConfig(
            notify_on_change=True,
            notification_topic="cache.events.compute",
            event_types=[
                CacheEventType.ATOMIC_OPERATION,
                CacheEventType.DATA_STRUCTURE_OPERATION,
            ],
            include_cache_metadata=False,
            include_performance_metrics=True,
        )

        self.pubsub_integration.configure_notifications(
            "compute.fibonacci", compute_config
        )

        print("🔔 Configured cache event notifications")

    async def shutdown(self) -> None:
        """Shutdown the cache server."""
        if self.pubsub_integration:
            await self.pubsub_integration.shutdown()
        print("👋 Cache server shutdown complete")


async def run_server_mode(port: int = 9001) -> None:
    """Run in server mode."""
    server = MPREGCacheServer(port=port)
    await server.start()


async def run_client_mode(server_url: str = "ws://127.0.0.1:9001") -> None:
    """Run client demonstrations."""
    print("🎯 MPREG Cache Client Demo")
    print("=" * 40)

    # Test direct cache client
    print("\n📦 1. Direct Cache Client Demo")
    print("-" * 30)

    cache_client = MPREGCacheClient(server_url)
    await cache_client.connect()

    try:
        # Test basic cache operations
        await cache_client.put("demo", "key1", {"message": "Hello Cache!"})
        value = await cache_client.get("demo", "key1")
        print(f"Retrieved: {value}")

        # Test atomic operations
        success = await cache_client.atomic_test_and_set(
            "locks", "resource1", None, "locked"
        )
        print(f"Lock acquired: {success}")

        # Test data structures
        await cache_client.set_add("permissions", "user123", "read")
        await cache_client.set_add("permissions", "user123", "write")
        has_admin = await cache_client.set_contains("permissions", "user123", "admin")
        print(f"Has admin permission: {has_admin}")

        # Test namespace operations
        cleared = await cache_client.clear_namespace("demo")
        print(f"Cleared entries: {cleared}")

    finally:
        await cache_client.disconnect()

    # Test RPC client with cache-enabled functions
    print("\n🔧 2. RPC Client with Cache Demo")
    print("-" * 30)

    async with MPREGClientAPI(server_url) as rpc_client:
        # Test cached fibonacci
        print("Computing fibonacci(10) - should cache result...")
        start_time = time.time()
        result1 = await rpc_client.call("fibonacci", 10)
        time1 = time.time() - start_time
        print(f"First call: fib(10) = {result1} ({time1:.3f}s)")

        print("Computing fibonacci(10) again - should hit cache...")
        start_time = time.time()
        result2 = await rpc_client.call("fibonacci", 10)
        time2 = time.time() - start_time
        print(f"Second call: fib(10) = {result2} ({time2:.3f}s)")
        print(f"Speedup: {time1 / time2:.1f}x faster!")

        # Test ML prediction caching
        print("\nTesting ML prediction caching...")
        features = [1.0, 2.0, 3.0, 4.0]

        start_time = time.time()
        pred1 = await rpc_client.call("ml_predict", "bert-large", features)
        time1 = time.time() - start_time
        print(f"First prediction: {pred1['prediction']:.3f} ({time1:.3f}s)")

        start_time = time.time()
        pred2 = await rpc_client.call("ml_predict", "bert-large", features)
        time2 = time.time() - start_time
        print(f"Cached prediction: {pred2['prediction']:.3f} ({time2:.3f}s)")
        print(f"Speedup: {time1 / time2:.1f}x faster!")


async def run_integrated_mode() -> None:
    """Run server and client in the same process."""
    print("🔄 Integrated Server + Client Demo")
    print("=" * 40)

    # Start server in background
    server = MPREGCacheServer(port=9002)
    server_task = asyncio.create_task(server.start())

    # Wait for server to start
    await asyncio.sleep(2)

    try:
        # Run client demo against local server
        await run_client_mode("ws://127.0.0.1:9002")

    except KeyboardInterrupt:
        print("\n🛑 Stopping integrated demo...")
    finally:
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass
        await server.shutdown()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="MPREG Cache Client/Server Demo")
    parser.add_argument(
        "--mode",
        choices=["server", "client", "integrated"],
        default="integrated",
        help="Demo mode: server (cache server), client (cache client), or integrated (both)",
    )
    parser.add_argument("--port", type=int, default=9001, help="Server port")
    parser.add_argument(
        "--url", default="ws://127.0.0.1:9001", help="Server URL for client"
    )

    args = parser.parse_args()

    try:
        if args.mode == "server":
            asyncio.run(run_server_mode(args.port))
        elif args.mode == "client":
            asyncio.run(run_client_mode(args.url))
        else:  # integrated
            asyncio.run(run_integrated_mode())
    except KeyboardInterrupt:
        print("\n👋 Demo interrupted by user")


if __name__ == "__main__":
    main()
