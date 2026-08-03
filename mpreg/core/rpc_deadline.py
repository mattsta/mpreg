"""Soft-RT deadline propagation helpers for fabric RPC hops (INV-P2 / C1).

Deadline is carried on ``MessageHeaders.deadline_remaining_ms`` and decremented
by measured hop latency so multi-hop RPCs fail closed before silent stalls.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from mpreg.core.errors import MpregError, MpregErrorCode
from mpreg.fabric.message import MessageHeaders

DEADLINE_METADATA_KEY = "mpreg.deadline_remaining_ms"

@dataclass(frozen=True, slots=True)
class DeadlineBudget:
    """Wall-clock deadline budget tracked across hops."""

    remaining_ms: float
    started_mono: float

    @classmethod
    def from_seconds(cls, seconds: float) -> DeadlineBudget:
        return cls(remaining_ms=max(0.0, float(seconds) * 1000.0), started_mono=time.monotonic())

    @classmethod
    def from_headers(cls, headers: MessageHeaders) -> DeadlineBudget | None:
        if headers.deadline_remaining_ms is not None:
            return cls(
                remaining_ms=float(headers.deadline_remaining_ms),
                started_mono=time.monotonic(),
            )
        raw = headers.metadata.get(DEADLINE_METADATA_KEY)
        if raw is None:
            return None
        try:
            return cls(remaining_ms=float(raw), started_mono=time.monotonic())
        except (TypeError, ValueError):
            return None

    def elapsed_ms(self) -> float:
        return max(0.0, (time.monotonic() - self.started_mono) * 1000.0)

    def remaining_after_elapsed(self) -> float:
        return max(0.0, self.remaining_ms - self.elapsed_ms())

    def exhausted(self) -> bool:
        return self.remaining_after_elapsed() <= 0.0

    def raise_if_exhausted(self, *, details: str = "deadline exhausted on hop") -> None:
        if self.exhausted():
            raise MpregError.of(MpregErrorCode.TIMEOUT, details=details)

    def apply_to_headers(self, headers: MessageHeaders) -> MessageHeaders:
        remaining = self.remaining_after_elapsed()
        meta = dict(headers.metadata)
        meta[DEADLINE_METADATA_KEY] = remaining
        return MessageHeaders(
            correlation_id=headers.correlation_id,
            source_cluster=headers.source_cluster,
            target_cluster=headers.target_cluster,
            routing_path=headers.routing_path,
            federation_path=headers.federation_path,
            hop_budget=headers.hop_budget,
            priority=headers.priority,
            metadata=meta,
            deadline_remaining_ms=remaining,
        )

def decrement_deadline_headers(
    headers: MessageHeaders, *, hop_latency_ms: float
) -> MessageHeaders:
    """Subtract hop latency from remaining deadline (if any)."""
    if headers.deadline_remaining_ms is None:
        raw = headers.metadata.get(DEADLINE_METADATA_KEY)
        if raw is None:
            return headers
        try:
            current = float(raw)
        except (TypeError, ValueError):
            return headers
    else:
        current = float(headers.deadline_remaining_ms)
    remaining = max(0.0, current - max(0.0, float(hop_latency_ms)))
    meta = dict(headers.metadata)
    meta[DEADLINE_METADATA_KEY] = remaining
    return MessageHeaders(
        correlation_id=headers.correlation_id,
        source_cluster=headers.source_cluster,
        target_cluster=headers.target_cluster,
        routing_path=headers.routing_path,
        federation_path=headers.federation_path,
        hop_budget=headers.hop_budget,
        priority=headers.priority,
        metadata=meta,
        deadline_remaining_ms=remaining,
    )

def headers_with_deadline_seconds(
    headers: MessageHeaders, deadline_seconds: float | None
) -> MessageHeaders:
    """Stamp headers with an absolute remaining budget from seconds."""
    if deadline_seconds is None:
        return headers
    remaining_ms = max(0.0, float(deadline_seconds) * 1000.0)
    meta = dict(headers.metadata)
    meta[DEADLINE_METADATA_KEY] = remaining_ms
    return MessageHeaders(
        correlation_id=headers.correlation_id,
        source_cluster=headers.source_cluster,
        target_cluster=headers.target_cluster,
        routing_path=headers.routing_path,
        federation_path=headers.federation_path,
        hop_budget=headers.hop_budget,
        priority=headers.priority,
        metadata=meta,
        deadline_remaining_ms=remaining_ms,
    )
