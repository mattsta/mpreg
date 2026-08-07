"""Retry and deadline policies for MPREG clients."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeVar

from mpreg.core.errors import MpregError, MpregErrorCode, map_exception

T = TypeVar("T")

class RpcExecutionMode(StrEnum):
    """Product RPC evaluation modalities (architecture claims INV-P*).

    M1_ASYNC: throughput-oriented retries; wall deadline optional.
    M2_SOFT_RT: end-to-end deadline budget; fail closed; no retry past remaining.
    M3_STREAMING: progressive/intermediate; same deadline rules as M2 by default.
    """

    M1_ASYNC = "async"
    M2_SOFT_RT = "soft_rt"
    M3_STREAMING = "streaming"

@dataclass(frozen=True, slots=True)
class ClientCallPolicy:
    """Controls retries and deadlines for RPC-style calls."""

    max_attempts: int = 1
    base_backoff_seconds: float = 0.05
    max_backoff_seconds: float = 2.0
    jitter_seconds: float = 0.05
    deadline_seconds: float | None = None
    retry_on_timeout: bool = True
    retry_on_unavailable: bool = True
    mode: RpcExecutionMode = RpcExecutionMode.M1_ASYNC
    # When True (default for M2/M3 factories), remaining deadline is shared
    # across attempts so retries cannot exceed the original budget.
    share_deadline_across_attempts: bool = False

    @classmethod
    def for_mode(
        cls,
        mode: RpcExecutionMode,
        *,
        deadline_seconds: float | None = None,
        max_attempts: int | None = None,
    ) -> ClientCallPolicy:
        """Build a policy with modality-appropriate defaults."""
        if mode is RpcExecutionMode.M1_ASYNC:
            return cls(
                mode=mode,
                max_attempts=max_attempts if max_attempts is not None else 3,
                deadline_seconds=deadline_seconds,
                retry_on_timeout=True,
                retry_on_unavailable=True,
                share_deadline_across_attempts=False,
            )
        if mode is RpcExecutionMode.M2_SOFT_RT:
            return cls(
                mode=mode,
                max_attempts=max_attempts if max_attempts is not None else 2,
                deadline_seconds=deadline_seconds
                if deadline_seconds is not None
                else 1.0,
                retry_on_timeout=True,
                retry_on_unavailable=True,
                share_deadline_across_attempts=True,
                base_backoff_seconds=0.01,
                max_backoff_seconds=0.2,
                jitter_seconds=0.01,
            )
        # M3_STREAMING
        return cls(
            mode=mode,
            max_attempts=max_attempts if max_attempts is not None else 1,
            deadline_seconds=deadline_seconds if deadline_seconds is not None else 30.0,
            retry_on_timeout=False,
            retry_on_unavailable=True,
            share_deadline_across_attempts=True,
            base_backoff_seconds=0.0,
            max_backoff_seconds=0.0,
            jitter_seconds=0.0,
        )

    def should_retry(self, exc: BaseException, attempt: int) -> bool:
        if attempt >= self.max_attempts:
            return False
        mapped = map_exception(exc)
        if mapped is None:
            return False
        if mapped.code == int(MpregErrorCode.TIMEOUT) and self.retry_on_timeout:
            return True
        if mapped.code == int(MpregErrorCode.UNAVAILABLE) and self.retry_on_unavailable:
            return True
        return bool(mapped.retryable)

    def backoff_for_attempt(self, attempt: int) -> float:
        # attempt is 1-based after a failure
        exp = self.base_backoff_seconds * (2 ** max(0, attempt - 1))
        delay = min(self.max_backoff_seconds, exp)
        if self.jitter_seconds > 0:
            delay += random.uniform(0, self.jitter_seconds)
        return float(delay)

async def call_with_policy[T](
    operation: Callable[[], Awaitable[T]],
    policy: ClientCallPolicy,
) -> T:
    """Execute async operation with retry/deadline policy.

    Soft-RT and streaming modes share one wall-clock deadline across attempts
    so a retry cannot succeed after the original budget (INV-P2 / INV-P3).
    """
    attempt = 0
    last_exc: BaseException | None = None
    deadline_mono: float | None = None
    if policy.deadline_seconds is not None and policy.share_deadline_across_attempts:
        deadline_mono = time.monotonic() + float(policy.deadline_seconds)

    while attempt < max(1, policy.max_attempts):
        attempt += 1
        try:
            if deadline_mono is not None:
                remaining = deadline_mono - time.monotonic()
                if remaining <= 0:
                    raise MpregError.of(
                        MpregErrorCode.TIMEOUT,
                        details="deadline exhausted before attempt",
                    )
                return await asyncio.wait_for(operation(), timeout=remaining)
            if policy.deadline_seconds is not None:
                return await asyncio.wait_for(
                    operation(), timeout=policy.deadline_seconds
                )
            return await operation()
        except Exception as exc:
            last_exc = exc
            mapped = map_exception(exc)
            if deadline_mono is not None and (deadline_mono - time.monotonic()) <= 0:
                raise MpregError.of(
                    MpregErrorCode.TIMEOUT,
                    details="deadline exhausted",
                ) from exc
            if not policy.should_retry(exc, attempt):
                raise mapped from exc
            delay = policy.backoff_for_attempt(attempt)
            if deadline_mono is not None:
                remaining = deadline_mono - time.monotonic()
                if remaining <= 0:
                    raise MpregError.of(
                        MpregErrorCode.TIMEOUT,
                        details="deadline exhausted during backoff",
                    ) from exc
                delay = min(delay, max(0.0, remaining))
            await asyncio.sleep(delay)
    assert last_exc is not None
    raise map_exception(last_exc) from last_exc

def default_ha_policy() -> ClientCallPolicy:
    """Sensible defaults for multi-endpoint clients (retry only retryable codes)."""
    return ClientCallPolicy(
        max_attempts=3,
        base_backoff_seconds=0.05,
        max_backoff_seconds=2.0,
        jitter_seconds=0.05,
        retry_on_timeout=True,
        retry_on_unavailable=True,
    )
