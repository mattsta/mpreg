"""Retry and deadline policies for MPREG clients."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

from mpreg.core.errors import MpregError, MpregErrorCode, map_exception

T = TypeVar("T")

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
        if mapped.retryable:
            return True
        return False

    def backoff_for_attempt(self, attempt: int) -> float:
        # attempt is 1-based after a failure
        exp = self.base_backoff_seconds * (2 ** max(0, attempt - 1))
        delay = min(self.max_backoff_seconds, exp)
        if self.jitter_seconds > 0:
            delay += random.uniform(0, self.jitter_seconds)
        return delay

async def call_with_policy(
    operation: Callable[[], Awaitable[T]],
    policy: ClientCallPolicy,
) -> T:
    """Execute async operation with retry/deadline policy."""
    attempt = 0
    last_exc: BaseException | None = None
    while attempt < max(1, policy.max_attempts):
        attempt += 1
        try:
            if policy.deadline_seconds is not None:
                return await asyncio.wait_for(
                    operation(), timeout=policy.deadline_seconds
                )
            return await operation()
        except Exception as exc:
            last_exc = exc
            if not policy.should_retry(exc, attempt):
                raise map_exception(exc) from exc
            await asyncio.sleep(policy.backoff_for_attempt(attempt))
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
