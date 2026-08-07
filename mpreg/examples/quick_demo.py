from __future__ import annotations

#!/usr/bin/env python3
"""Quick demonstration of MPREG's modern example tiers.

Run with: uv run python mpreg/examples/quick_demo.py
"""

import asyncio

from mpreg.examples.tier1_single_system_full import demo_rpc
from mpreg.examples.tier2_integrations import main as integrations_main
from mpreg.examples.tier3_full_system_expansion import main as full_system_main


async def main() -> None:
    print("=" * 60)
    print("🚀 MPREG Quick Demo - Tiered Examples")
    print("=" * 60)
    print("Tier 1: Single system")
    await demo_rpc()
    print("\nTier 2: Two-system integrations")
    await integrations_main()
    print("\nTier 3: Full system expansion")
    await full_system_main()


if __name__ == "__main__":
    asyncio.run(main())
