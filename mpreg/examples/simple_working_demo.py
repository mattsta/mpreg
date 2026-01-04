from __future__ import annotations

#!/usr/bin/env python3
"""Simple working demo that uses the modern tiered examples."""

import asyncio

from mpreg.examples.tier1_single_system_full import demo_rpc


async def main() -> None:
    print("🚀 Simple MPREG Demo (Tier 1 RPC)")
    print("=" * 30)
    await demo_rpc()


if __name__ == "__main__":
    asyncio.run(main())
