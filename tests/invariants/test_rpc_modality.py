"""C0–C3: RpcExecutionMode, deadlines, stream oracle (INV-P1–P5)."""

from __future__ import annotations

import asyncio

import pytest

from mpreg.client.call_policy import (
    ClientCallPolicy,
    RpcExecutionMode,
    call_with_policy,
)
from mpreg.core.errors import MpregError, MpregErrorCode
from mpreg.testing.oracles import RpcOracle, RpcStreamEvent

def test_for_mode_defaults() -> None:
    m1 = ClientCallPolicy.for_mode(RpcExecutionMode.M1_ASYNC)
    assert m1.mode is RpcExecutionMode.M1_ASYNC
    assert m1.max_attempts >= 1
    m2 = ClientCallPolicy.for_mode(RpcExecutionMode.M2_SOFT_RT, deadline_seconds=0.5)
    assert m2.share_deadline_across_attempts is True
    assert m2.deadline_seconds == 0.5
    m3 = ClientCallPolicy.for_mode(RpcExecutionMode.M3_STREAMING)
    assert m3.retry_on_timeout is False

@pytest.mark.asyncio
async def test_soft_rt_deadline_fail_closed() -> None:
    policy = ClientCallPolicy.for_mode(
        RpcExecutionMode.M2_SOFT_RT, deadline_seconds=0.15, max_attempts=5
    )

    async def slow() -> str:
        await asyncio.sleep(0.08)
        raise TimeoutError("still slow")

    with pytest.raises(MpregError) as ei:
        await call_with_policy(slow, policy)
    assert ei.value.code == int(MpregErrorCode.TIMEOUT)

@pytest.mark.asyncio
async def test_shared_deadline_prevents_late_success() -> None:
    """Retries must not succeed after the original budget (INV-P2/P3)."""
    policy = ClientCallPolicy(
        max_attempts=10,
        deadline_seconds=0.12,
        share_deadline_across_attempts=True,
        base_backoff_seconds=0.0,
        jitter_seconds=0.0,
        retry_on_timeout=True,
        mode=RpcExecutionMode.M2_SOFT_RT,
    )
    calls = {"n": 0}

    async def eventually() -> str:
        calls["n"] += 1
        await asyncio.sleep(0.05)
        if calls["n"] < 20:
            raise TimeoutError("nope")
        return "late"

    with pytest.raises(MpregError) as ei:
        await call_with_policy(eventually, policy)
    assert ei.value.code == int(MpregErrorCode.TIMEOUT)
    assert calls["n"] < 20  # never reached late success path

@pytest.mark.asyncio
async def test_async_mode_can_retry_to_success() -> None:
    policy = ClientCallPolicy.for_mode(RpcExecutionMode.M1_ASYNC)
    policy = ClientCallPolicy(
        max_attempts=4,
        base_backoff_seconds=0.0,
        jitter_seconds=0.0,
        mode=RpcExecutionMode.M1_ASYNC,
    )
    n = {"c": 0}

    async def flaky() -> str:
        n["c"] += 1
        if n["c"] < 3:
            raise TimeoutError("x")
        return "ok"

    assert await call_with_policy(flaky, policy) == "ok"

def test_stream_oracle_contracts() -> None:
    o = RpcOracle()
    o.observe_many(
        [
            RpcStreamEvent(kind="intermediate", level=0),
            RpcStreamEvent(kind="intermediate", level=1),
            RpcStreamEvent(kind="final"),
        ]
    )
    RpcOracle.assert_timeout_code(1006)
