"""L2 rpc_plus_cache — unified from legacy tier2 integrations."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import ok, step
from mpreg.examples.tier2_integrations import rpc_plus_cache

async def main() -> None:
    step("running integration tour: rpc_plus_cache")
    await rpc_plus_cache()
    ok("rpc_plus_cache completed")

if __name__ == "__main__":
    asyncio.run(main())
