"""L1 discovery_rate_limit — DiscoveryRateLimiter sliding-window allow/deny."""

from __future__ import annotations

import asyncio

from mpreg.core.discovery_rate_limit import (
    DiscoveryRateLimitConfig,
    DiscoveryRateLimiter,
    DiscoveryRateLimitKey,
)
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    get_probe,
    ok,
    scenario,
    step,
)


async def main() -> None:
    with app_run(
        "discovery_rate_limit",
        "Discovery Rate Limit — sliding window per viewer/command",
        level="L1",
        probe=True,
    ):
        probe = get_probe()
        assert probe is not None

        with scenario("disabled config always allows", "disco.rate_limit"):
            lim = DiscoveryRateLimiter(
                DiscoveryRateLimitConfig(
                    enabled=False, max_requests=1, window_seconds=60.0
                )
            )
            key = DiscoveryRateLimitKey(viewer_id="v1", command="list_peers")
            for i in range(5):
                with probe.measure("rate.allow"):
                    ensure(
                        lim.allow(key, now=float(i)) is True, f"disabled deny at {i}"
                    )
            ok("disabled=always allow")

        with scenario("max_requests=0 hard deny", "disco.rate_limit"):
            lim = DiscoveryRateLimiter(
                DiscoveryRateLimitConfig(
                    enabled=True, max_requests=0, window_seconds=10.0
                )
            )
            key = DiscoveryRateLimitKey(viewer_id="v2", command="catalog_query")
            with probe.measure("rate.deny"):
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
            with probe.measure("rate.allow"):
                ensure(lim.allow(key, now=t0) is True, "1st")
            with probe.measure("rate.allow"):
                ensure(lim.allow(key, now=t0 + 0.1) is True, "2nd")
            with probe.measure("rate.allow"):
                ensure(lim.allow(key, now=t0 + 0.2) is True, "3rd")
            with probe.measure("rate.deny"):
                ensure(lim.allow(key, now=t0 + 0.3) is False, "4th should deny")
            ok("burst 3 then deny")

        with scenario("window expiry re-allows", "disco.rate_limit"):
            lim = DiscoveryRateLimiter(
                DiscoveryRateLimitConfig(
                    enabled=True, max_requests=2, window_seconds=5.0
                )
            )
            key = DiscoveryRateLimitKey(viewer_id="v4", command="dns_resolve")
            with probe.measure("rate.allow"):
                ensure(lim.allow(key, now=2000.0) is True, "a")
            with probe.measure("rate.allow"):
                ensure(lim.allow(key, now=2001.0) is True, "b")
            with probe.measure("rate.deny"):
                ensure(lim.allow(key, now=2002.0) is False, "still in window")
            with probe.measure("rate.allow"):
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
            with probe.measure("rate.allow"):
                ensure(lim.allow(k1, now=3000.0) is True, "k1")
            with probe.measure("rate.deny"):
                ensure(lim.allow(k1, now=3000.1) is False, "k1 second")
            with probe.measure("rate.allow"):
                ensure(lim.allow(k2, now=3000.2) is True, "viewer isolation")
            with probe.measure("rate.allow"):
                ensure(lim.allow(k3, now=3000.3) is True, "command isolation")
            with probe.measure("rate.allow"):
                ensure(lim.allow(k4, now=3000.4) is True, "namespace isolation")
            ok("key dimensions isolated")

        with scenario("max_keys hard prune under pressure", "disco.rate_limit"):
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
                with probe.measure("rate.allow"):
                    ensure(lim.allow(k, now=now + i) is True, f"key {i}")
            # Phase G F20 fix: prune to max_keys-1 before insert → hard cap.
            n = len(lim._events)
            ensure(n <= 2, f"keys unbounded: {n}")
            ensure(n >= 1, "all keys pruned unexpectedly")
            step(f"F20 fixed: max_keys=2 hard cap retained={n}")
            ok("max_keys hard prune")

        with scenario("rate-limit latency/throughput probe", "mon.slo"):
            allow_op = probe.op("rate.allow")
            deny_op = probe.op("rate.deny")
            ensure(allow_op.count >= 10, f"allows {allow_op.count}")
            ensure(deny_op.count >= 3, f"denies {deny_op.count}")
            ensure(allow_op.p95_ms < 1000.0, f"allow p95 {allow_op.p95_ms}")
            ensure(probe.throughput_ops_s > 0.0, "throughput")
            ok(
                f"probe ops={probe.total_ops} allow_p95={allow_op.p95_ms:.4f} "
                f"throughput_ops_s={probe.throughput_ops_s:.1f}"
            )

        await asyncio.sleep(0)


if __name__ == "__main__":
    asyncio.run(main())
