"""L2 pubsub_plus_queue — unified from legacy tier2 integrations."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import ok, step
from mpreg.examples.tier2_integrations import pubsub_plus_queue

async def main() -> None:
    step("running integration tour: pubsub_plus_queue")
    await pubsub_plus_queue()
    ok("pubsub_plus_queue completed")

if __name__ == "__main__":
    asyncio.run(main())
