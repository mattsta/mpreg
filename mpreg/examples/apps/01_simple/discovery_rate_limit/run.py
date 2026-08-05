"""L1 discovery_rate_limit — DiscoveryRateLimiter sliding-window allow/deny."""

from __future__ import annotations

import asyncio

from mpreg.core.discovery_rate_limit import (
    DiscoveryRateLimitConfig,
    DiscoveryRateLimiter,
    DiscoveryRateLimitKey,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

async def main() -> None:
    with app_run(
        "discovery_rate_limit",
        "Discovery Rate Limit — sliding window per viewer/command",
        level="L1",
    ):
        with scenario("disabled config always allows", "disco.rate_limit"):
            lim = DiscoveryRateLimiter(
                DiscoveryRateLimitConfig(
                    enabled=False, max_requests=1, window_seconds=60.0
                )
            )
            key = DiscoveryRateLimitKey(viewer_id="v1", command="list_peers")
            for i in range(5):
                ensure(lim.allow(key, now=float(i)) is True, f"disabled deny at {i}")
            ok("disabled=always allow")

        with scenario("max_requests=0 hard deny", "disco.rate_limit"):
            lim = DiscoveryRateLimiter(
                DiscoveryRateLimitConfig(
                    enabled=True, max_requests=0, window_seconds=10.0
                )
            )
            key = DiscoveryRateLimitKey(viewer_id="v2", command="catalog_query")
            ensure(lim.allow(key, now=1.0) is False, "zero max should deny")
            ok("max_requests=0 deny")

        with scenario("window allows N then denies", "disco.rate_limit"):
            lim = DiscoveryRateLimiter(
                DiscoveryRateLimitConfig(
                    enabled=True, max_requests=3, window_seconds=10.0
                )
            )
            key = DiscoveryRateLimitKey(
                viewer_id="v3", command="summary_query", namespace="ns.a"
            )
            t0 = 1000.0
            ensure(lim.allow(key, now=t0) is True, "1st")
            ensure(lim.allow(key, now=t0 + 0.1) is True, "2nd")
            ensure(lim.allow(key, now=t0 + 0.2) is True, "3rd")
            ensure(lim.allow(key, now=t0 + 0.3) is False, "4th should deny")
            ok("burst 3 then deny")

        with scenario("window expiry re-allows", "disco.rate_limit"):
            lim = DiscoveryRateLimiter(
                DiscoveryRateLimitConfig(
                    enabled=True, max_requests=2, window_seconds=5.0
                )
            )
            key = DiscoveryRateLimitKey(viewer_id="v4", command="dns_resolve")
            ensure(lim.allow(key, now=2000.0) is True, "a")
            ensure(lim.allow(key, now=2001.0) is True, "b")
            ensure(lim.allow(key, now=2002.0) is False, "still in window")
            # Events at 2000/2001 expire after window_seconds from now
            ensure(lim.allow(key, now=2006.1) is True, "after window should allow")
            ok("sliding window re-allow")

        with scenario("keys isolated by viewer/command/ns", "disco.rate_limit"):
            lim = DiscoveryRateLimiter(
                DiscoveryRateLimitConfig(
                    enabled=True, max_requests=1, window_seconds=30.0
                )
            )
            k1 = DiscoveryRateLimitKey(viewer_id="va", command="list")
            k2 = DiscoveryRateLimitKey(viewer_id="vb", command="list")
            k3 = DiscoveryRateLimitKey(viewer_id="va", command="watch")
            k4 = DiscoveryRateLimitKey(
                viewer_id="va", command="list", namespace="other"
            )
            ensure(lim.allow(k1, now=3000.0) is True, "k1")
            ensure(lim.allow(k1, now=3000.1) is False, "k1 second")
            ensure(lim.allow(k2, now=3000.2) is True, "viewer isolation")
            ensure(lim.allow(k3, now=3000.3) is True, "command isolation")
            ensure(lim.allow(k4, now=3000.4) is True, "namespace isolation")
            ok("key dimensions isolated")

        with scenario("max_keys prune under pressure", "disco.rate_limit"):
            lim = DiscoveryRateLimiter(
                DiscoveryRateLimitConfig(
                    enabled=True,
                    max_requests=10,
                    window_seconds=60.0,
                    max_keys=2,
                )
            )
            now = 4000.0
            for i in range(5):
                k = DiscoveryRateLimitKey(viewer_id=f"p{i}", command="c")
                ensure(lim.allow(k, now=now + i) is True, f"key {i}")
            # Prune runs when len >= max_keys before insert, then inserts —
            # steady state can be max_keys+1 (F20), not a hard cap at max_keys.
            n = len(lim._events)
            ensure(n <= 3, f"keys unbounded: {n}")
            ensure(n >= 1, "all keys pruned unexpectedly")
            step(
                f"friction F20: max_keys=2 retained={n} (cap is soft; "
                "prune-then-insert → up to max_keys+1)"
            )
            ok("max_keys soft prune")

        await asyncio.sleep(0)

if __name__ == "__main__":
    asyncio.run(main())
