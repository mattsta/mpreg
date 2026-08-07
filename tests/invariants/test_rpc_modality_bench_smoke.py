"""C5: Lightweight modality micro-benchmarks (assert budgets, not wall SLOs)."""

from __future__ import annotations

import asyncio
import time

import pytest

from mpreg.client.call_policy import (
    ClientCallPolicy,
    RpcExecutionMode,
    call_with_policy,
)


@pytest.mark.asyncio
async def test_m1_throughput_smoke() -> None:
    policy = ClientCallPolicy.for_mode(RpcExecutionMode.M1_ASYNC)
    policy = ClientCallPolicy(
        max_attempts=1,
        mode=RpcExecutionMode.M1_ASYNC,
        base_backoff_seconds=0.0,
        jitter_seconds=0.0,
    )
    n = 50
    start = time.perf_counter()

    async def op() -> int:
        return 1

    results = await asyncio.gather(*[call_with_policy(op, policy) for _ in range(n)])
    elapsed = time.perf_counter() - start
    assert sum(results) == n
    # Sanity: should complete quickly in-process
    assert elapsed < 5.0


@pytest.mark.asyncio
async def test_m2_deadline_miss_rate_bounded() -> None:
    policy = ClientCallPolicy.for_mode(
        RpcExecutionMode.M2_SOFT_RT, deadline_seconds=0.05, max_attempts=2
    )
    misses = 0
    total = 10
    for _ in range(total):

        async def slow() -> str:
            await asyncio.sleep(0.2)
            return "late"

        try:
            await call_with_policy(slow, policy)
        except Exception:
            misses += 1
    # Soft-RT must fail closed on slow ops
    assert misses == total


@pytest.mark.asyncio
async def test_m3_time_to_first_intermediate_budget() -> None:
    # Collector-level TTFB proxy
    from mpreg.core.intermediate_results import IntermediateResultCollector

    c = IntermediateResultCollector(request_id="b", total_levels=3)
    t0 = time.perf_counter()
    c.start_level(0)
    mid = c.complete_level(0, {"a": 1}, {"a": 1})
    ttfb_ms = (time.perf_counter() - t0) * 1000
    assert mid.level_index == 0
    assert ttfb_ms < 100.0
