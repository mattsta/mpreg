#!/usr/bin/env python3
"""Simple test of edge computing example."""

import asyncio

from examples.advanced_cluster_examples import EdgeComputingExample


async def main():
    print("🧪 Testing Edge Computing Example...")
    edge_example = EdgeComputingExample()
    try:
        await edge_example.run_edge_computing_demo()
        print("✅ Edge computing example completed successfully!")
    except Exception as e:
        print(f"❌ Edge computing example failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
