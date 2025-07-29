#!/usr/bin/env python3
"""Debug script to isolate cache test hanging issue."""

import asyncio
import sys

from mpreg.core.advanced_cache_ops import AdvancedCacheOperations
from mpreg.core.cache_pubsub_integration import CachePubSubIntegration
from mpreg.core.caching import CacheConfiguration
from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager
from mpreg.core.topic_exchange import TopicExchange


async def debug_cache_setup():
    """Debug the cache setup to see where it hangs."""
    print("🔍 Starting cache debug...")

    try:
        print("1. Creating cache configuration...")
        local_config = CacheConfiguration()
        cache_config = GlobalCacheConfiguration(
            local_cache_config=local_config,
            enable_l2_persistent=False,
            enable_l3_distributed=False,
            enable_l4_federation=False,
        )
        print("✅ Cache configuration created")

        print("2. Creating cache manager...")
        cache_manager = GlobalCacheManager(cache_config)
        print("✅ Cache manager created")

        print("3. Creating topic exchange...")
        # Use a fake URL since we're just testing creation
        topic_exchange = TopicExchange("ws://127.0.0.1:9999", "debug-cluster")
        print("✅ Topic exchange created")

        print("4. Creating advanced cache ops...")
        advanced_ops = AdvancedCacheOperations(cache_manager)
        print("✅ Advanced cache ops created")

        print("5. Creating cache-pubsub integration...")
        integration = CachePubSubIntegration(
            cache_manager=cache_manager,
            advanced_cache_ops=advanced_ops,
            topic_exchange=topic_exchange,
            cluster_id="debug-cluster",
        )
        print("✅ Cache-pubsub integration created")

        print("6. Waiting 2 seconds to see if background tasks cause issues...")
        await asyncio.sleep(2)
        print("✅ No hanging after 2 seconds")

        print("7. Shutting down integration...")
        await integration.shutdown()
        print("✅ Integration shutdown complete")

        print("8. Shutting down cache manager...")
        await cache_manager.shutdown()
        print("✅ Cache manager shutdown complete")

        print("🎉 Debug completed successfully - no hanging detected!")

    except Exception as e:
        print(f"❌ Error during debug: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(debug_cache_setup())
        print("✅ Script completed normally")
    except KeyboardInterrupt:
        print("❌ Script was interrupted (possible hang)")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Script failed: {e}")
        sys.exit(1)
