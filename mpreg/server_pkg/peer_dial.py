"""Peer dial types and pure policy math extracted from MPREGServer.

Server methods remain thin wrappers that supply live cluster context
(diagnostics, gossip interval, seed peers) while decision math lives here
for unit testing without a running server.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass(slots=True)
class PeerDialState:
    """Adaptive dial scheduling state for a peer URL."""

    consecutive_failures: int = 0
    last_attempt_at: float = 0.0
    last_success_at: float | None = None
    next_attempt_at: float = 0.0

    def can_attempt(self, now: float) -> bool:
        return now >= self.next_attempt_at

    def record_attempt(self, now: float) -> None:
        self.last_attempt_at = now

    def record_success(self, now: float) -> None:
        self.consecutive_failures = 0
        self.last_success_at = now
        self.next_attempt_at = now

    def record_failure(
        self,
        *,
        now: float,
        base_delay_seconds: float,
        max_delay_seconds: float,
        spread_fraction: float,
    ) -> None:
        self.consecutive_failures += 1
        exponent = max(self.consecutive_failures - 1, 0)
        if base_delay_seconds <= 0:
            delay = max_delay_seconds
        else:
            delay = base_delay_seconds
            max_exponent = 0
            while delay < max_delay_seconds:
                delay *= 2
                max_exponent += 1
            exponent = min(exponent, max_exponent)
            delay = min(max_delay_seconds, base_delay_seconds * (2**exponent))
        jitter_seconds = delay * max(min(spread_fraction, 0.25), 0.0)
        self.next_attempt_at = now + delay + jitter_seconds


@dataclass(frozen=True, slots=True)
class PeerDialConnectionPolicy:
    """Connection attempt policy chosen for a peer dial."""

    max_retries: int
    base_delay_seconds: float
    connect_timeout_seconds: float
    open_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class PeerDialDiagnosticSnapshot:
    """Structured dial attempt snapshot for diagnostics."""

    peer_url: str
    dial_url: str
    context: str
    fast_connect: bool
    peer_target_count: int
    connected_ratio: float
    consecutive_failures: int
    policy: PeerDialConnectionPolicy
    attempt_epoch_seconds: float


@dataclass(frozen=True, slots=True)
class PeerDialLoopSnapshot:
    """Structured scheduler loop snapshot for diagnostics."""

    peer_target_count: int
    connected_ratio: float
    discovery_ratio: float
    pressure: float
    desired_connected: int
    exploration_slots: int
    connected_candidates: int
    due_candidates: int
    selected_candidates: int
    parallelism: int
    dial_budget: int
    not_due_candidates: int
    reconcile_interval_seconds: float


@dataclass(frozen=True, slots=True)
class PeerDialBackoff:
    base_seconds: float
    cap_seconds: float

    def delay_for_failures(self, failures: int) -> float:
        if failures <= 0:
            return 0.0
        return min(self.cap_seconds, self.base_seconds * (2 ** (failures - 1)))


def select_peer_connection_policy(
    *,
    fast_connect: bool,
    peer_target_count: int,
    consecutive_failures: int,
    connected_ratio: float = 1.0,
) -> PeerDialConnectionPolicy:
    """Choose retries/timeouts for a peer dial (pure)."""
    target_count = max(peer_target_count, 1)
    failure_count = max(consecutive_failures, 0)
    connectivity = min(max(connected_ratio, 0.0), 1.0)
    target_factor = target_count**0.5
    retry_bonus = min(4, failure_count // 2)
    connectivity_pressure = max(0.0, 0.45 - connectivity) / 0.45

    def _adaptive_retry_cap(*, fast_path: bool) -> int:
        if fast_path:
            if target_count >= 20:
                if connectivity < 0.15:
                    return 0
                if connectivity < 0.45:
                    return 1
                return 2
            base_cap = max(2, int(6.0 / max(target_factor, 1.0)))
            if connectivity < 0.5:
                base_cap += 1
            return base_cap + min(1, failure_count // 5)
        if target_count >= 20:
            if connectivity < 0.15:
                return 0
            if connectivity < 0.45:
                return 1
            return 2
        base_cap = max(2, int(8.0 / max(target_factor, 1.0)))
        return base_cap + min(2, failure_count // 5)

    def _pressure_timeout_floor(*, fast_path: bool) -> float:
        if target_count < 20:
            if fast_path:
                return 1.4 + (connectivity_pressure * 1.4)
            return 2.0 + (connectivity_pressure * 1.8)
        if fast_path:
            return 1.8 + (connectivity_pressure * 1.2)
        return 2.4 + (connectivity_pressure * 1.6)

    if fast_connect:
        max_retries = min(6, max(1, 1 + int(target_factor // 2)) + retry_bonus)
        base_delay_seconds = min(
            1.5, 0.08 + (target_factor * 0.035) + (min(failure_count, 6) * 0.04)
        )
        connect_timeout_seconds = min(
            8.0, 1.1 + (target_factor * 0.16) + (min(failure_count, 8) * 0.18)
        )
        connect_timeout_seconds = max(
            connect_timeout_seconds, _pressure_timeout_floor(fast_path=True)
        )
        max_retries = min(max_retries, _adaptive_retry_cap(fast_path=True))
        return PeerDialConnectionPolicy(
            max_retries=max_retries,
            base_delay_seconds=base_delay_seconds,
            connect_timeout_seconds=connect_timeout_seconds,
            open_timeout_seconds=connect_timeout_seconds,
        )

    max_retries = min(8, max(2, 1 + int(target_factor)) + retry_bonus)
    base_delay_seconds = min(
        2.2, 0.3 + (target_factor * 0.05) + (min(failure_count, 6) * 0.07)
    )
    connect_timeout_seconds = min(
        10.0, 2.4 + (target_factor * 0.10) + (min(failure_count, 10) * 0.22)
    )
    connect_timeout_seconds = max(
        connect_timeout_seconds, _pressure_timeout_floor(fast_path=False)
    )
    max_retries = min(max_retries, _adaptive_retry_cap(fast_path=False))
    return PeerDialConnectionPolicy(
        max_retries=max_retries,
        base_delay_seconds=base_delay_seconds,
        connect_timeout_seconds=connect_timeout_seconds,
        open_timeout_seconds=connect_timeout_seconds,
    )


def dial_pressure(*, peer_target_count: int, connected_ratio: float = 1.0) -> float:
    target_count = max(peer_target_count, 1)
    connectivity = min(max(connected_ratio, 0.0), 1.0)
    connectivity_deficit = 1.0 - connectivity
    size_factor = max(target_count**0.5 - 2.0, 0.0) / 2.0
    return connectivity_deficit * size_factor


def dial_parallelism(*, peer_target_count: int, connected_ratio: float = 1.0) -> int:
    target_count = max(peer_target_count, 1)
    connectivity = min(max(connected_ratio, 0.0), 1.0)
    pressure = dial_pressure(
        peer_target_count=target_count, connected_ratio=connectivity
    )
    parallelism_cap = 4
    if pressure >= 1.2:
        parallelism_cap = 1
    elif pressure >= 0.8:
        parallelism_cap = 2
    elif pressure >= 0.4:
        parallelism_cap = 3
    parallelism = max(1, min(parallelism_cap, int(target_count**0.5)))
    if target_count >= 24 and connectivity < 0.20:
        parallelism = max(parallelism, 3)
    elif target_count >= 20 and connectivity < 0.15:
        recovery_parallelism = 2 if target_count < 36 else 3
        parallelism = max(parallelism, recovery_parallelism)
    return parallelism


def dial_exploration_slots(
    *,
    peer_target_count: int,
    connected_ratio: float,
    discovery_ratio: float,
) -> int:
    target_count = max(peer_target_count, 1)
    if target_count < 20:
        return 0
    if discovery_ratio >= 0.95:
        return 0
    if connected_ratio < 0.30:
        return max(2, int(target_count**0.5))
    if connected_ratio < 0.50:
        return max(2, int(target_count**0.5) // 2)
    return max(1, int(target_count**0.5) // 2)


def target_connection_count(
    *,
    peer_target_count: int,
    connected_ratio: float = 1.0,
    discovery_ratio: float = 1.0,
) -> int:
    target_count = max(peer_target_count, 1)
    if target_count <= 20:
        return target_count
    baseline = max(6, int(target_count**0.5) + 4)
    connectivity = min(max(connected_ratio, 0.0), 1.0)
    discovery = min(max(discovery_ratio, 0.0), 1.0)

    if discovery >= 0.90:
        stability_target = max(6, int(target_count**0.5) + 2)
        if connectivity < 0.30:
            return min(target_count, stability_target + 1)
        return min(target_count, stability_target)

    if connectivity < 0.3:
        deficit_bonus = max(6, int(target_count**0.5))
    elif connectivity < 0.5:
        deficit_bonus = max(4, int(target_count**0.4))
    elif connectivity < 0.7:
        deficit_bonus = 2
    else:
        deficit_bonus = 0

    discovery_bonus = 0
    if target_count >= 24:
        if discovery < 0.5:
            discovery_bonus = max(discovery_bonus, int(target_count * 0.35))
        elif discovery < 0.7:
            discovery_bonus = max(discovery_bonus, int(target_count * 0.25))
        elif discovery < 0.85:
            discovery_bonus = max(discovery_bonus, int(target_count * 0.15))
        elif discovery < 0.95:
            discovery_bonus = max(discovery_bonus, int(target_count * 0.08))

    return min(target_count, baseline + max(deficit_bonus, discovery_bonus))


def backoff_base_seconds(
    *,
    peer_target_count: int,
    connected_ratio: float = 1.0,
    gossip_interval: float = 1.0,
) -> float:
    target_count = max(peer_target_count, 1)
    by_cluster_scale = gossip_interval / max(target_count**0.5, 1.0)
    startup_pressure_floor = gossip_interval * min(target_count / 100.0, 0.5)
    pressure = dial_pressure(
        peer_target_count=target_count, connected_ratio=connected_ratio
    )
    pressure_multiplier = 1.0 + (min(pressure, 2.5) * 1.5)
    return max(0.2, by_cluster_scale, startup_pressure_floor) * pressure_multiplier


def backoff_cap_seconds(
    *, peer_target_count: int, gossip_interval: float = 1.0
) -> float:
    target_count = max(peer_target_count, 1)
    return max(gossip_interval, gossip_interval * (target_count**0.5))


def reconcile_interval_seconds(
    *,
    peer_target_count: int,
    connected_ratio: float = 1.0,
    gossip_interval: float = 1.0,
) -> float:
    if peer_target_count <= 0:
        return max(gossip_interval, 0.2)
    scaled_interval = gossip_interval / max(peer_target_count**0.5, 1.0)
    adaptive_floor = min(0.75, 0.2 + (peer_target_count / 200.0))
    base_interval = max(adaptive_floor, scaled_interval)
    pressure = dial_pressure(
        peer_target_count=peer_target_count, connected_ratio=connected_ratio
    )
    pressure_multiplier = 1.0 + (min(pressure, 2.0) * 1.25)
    interval_cap = max(gossip_interval * 4.0, 1.5)
    return min(interval_cap, base_interval * pressure_multiplier)


def spread_fraction_for_url(peer_url: str) -> float:
    """Deterministic jitter prevents synchronized redials without global randomness."""
    checksum = sum(ord(char) for char in peer_url) % 1000
    return checksum / 4000.0


def selection_spread(*, local_url: str, peer_url: str) -> float:
    """Per-node deterministic spread for dial target ordering under budget pressure."""
    basis = f"{local_url}|{peer_url}"
    digest = hashlib.blake2s(basis.encode("utf-8"), digest_size=8).digest()
    spread_value = int.from_bytes(digest, "big")
    return spread_value / float((1 << 64) - 1)


# Back-compat aliases used by early unit tests / docs
def dial_pressure_from_counts(*, recent_failures: int, recent_successes: int) -> float:
    total = recent_failures + recent_successes
    if total <= 0:
        return 0.0
    return min(1.0, recent_failures / total)


def spread_fraction(*, attempt: int, window: int = 8) -> float:
    if window <= 0:
        return 0.0
    return min(1.0, (attempt % window) / float(window))
