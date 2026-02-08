"""Route control plane data structures for path-vector fabric routing."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, replace

from loguru import logger

from mpreg.datastructures.type_aliases import (
    BandwidthMbps,
    ClusterId,
    DurationSeconds,
    HopCount,
    JsonDict,
    NetworkLatencyMs,
    ReliabilityScore,
    RouteCostScore,
    Timestamp,
)


def _as_dict(value: object) -> JsonDict:
    if isinstance(value, dict):
        return {str(key): val for key, val in value.items()}
    return {}


def _as_str_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    return ()


def _as_int(value: object, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, (int, float, str)):
        try:
            return int(value)
        except TypeError, ValueError:
            return default
    return default


def _as_float(value: object, default: float) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float, str)):
        try:
            return float(value)
        except TypeError, ValueError:
            return default
    return default


@dataclass(frozen=True, slots=True)
class RouteDestination:
    """Typed destination for route advertisements."""

    cluster_id: ClusterId

    def to_dict(self) -> dict[str, str]:
        return {"cluster_id": self.cluster_id}

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RouteDestination:
        return cls(cluster_id=str(payload.get("cluster_id", "")))


@dataclass(frozen=True, slots=True)
class RoutePath:
    """Ordered cluster hops for a route."""

    hops: tuple[ClusterId, ...]

    def contains(self, cluster_id: ClusterId) -> bool:
        return cluster_id in self.hops

    def with_prefix(self, cluster_id: ClusterId) -> RoutePath:
        return RoutePath((cluster_id,) + self.hops)

    def next_hop(self, local_cluster: ClusterId) -> ClusterId | None:
        if not self.hops or self.hops[0] != local_cluster:
            return None
        if len(self.hops) < 2:
            return None
        return self.hops[1]

    def contains_any(self, clusters: set[ClusterId]) -> bool:
        return any(cluster in clusters for cluster in self.hops)

    def to_dict(self) -> dict[str, list[str]]:
        return {"hops": list(self.hops)}

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RoutePath:
        return cls(hops=_as_str_tuple(payload.get("hops", ())))


@dataclass(frozen=True, slots=True)
class RouteMetrics:
    """Metrics used to evaluate route quality."""

    hop_count: HopCount = 0
    latency_ms: NetworkLatencyMs = 0.0
    bandwidth_mbps: BandwidthMbps = 0
    reliability_score: ReliabilityScore = 1.0
    cost_score: RouteCostScore = 0.0

    def with_added_hop(
        self,
        *,
        latency_ms: NetworkLatencyMs = 0.0,
        bandwidth_mbps: BandwidthMbps | None = None,
        reliability_score: ReliabilityScore = 1.0,
        cost_score: RouteCostScore = 0.0,
    ) -> RouteMetrics:
        bandwidth = self._combine_bandwidth(bandwidth_mbps)
        reliability = max(
            0.0,
            min(self.reliability_score, float(reliability_score), 1.0),
        )
        return RouteMetrics(
            hop_count=self.hop_count + 1,
            latency_ms=self.latency_ms + float(latency_ms),
            bandwidth_mbps=bandwidth,
            reliability_score=reliability,
            cost_score=self.cost_score + float(cost_score),
        )

    def _combine_bandwidth(self, bandwidth_mbps: BandwidthMbps | None) -> BandwidthMbps:
        if bandwidth_mbps is None or bandwidth_mbps <= 0:
            return self.bandwidth_mbps
        if self.bandwidth_mbps <= 0:
            return bandwidth_mbps
        return min(self.bandwidth_mbps, bandwidth_mbps)

    def to_dict(self) -> dict[str, float | int]:
        return {
            "hop_count": int(self.hop_count),
            "latency_ms": float(self.latency_ms),
            "bandwidth_mbps": int(self.bandwidth_mbps),
            "reliability_score": float(self.reliability_score),
            "cost_score": float(self.cost_score),
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RouteMetrics:
        return cls(
            hop_count=_as_int(payload.get("hop_count"), 0),
            latency_ms=_as_float(payload.get("latency_ms"), 0.0),
            bandwidth_mbps=_as_int(payload.get("bandwidth_mbps"), 0),
            reliability_score=_as_float(payload.get("reliability_score"), 1.0),
            cost_score=_as_float(payload.get("cost_score"), 0.0),
        )


@dataclass(frozen=True, slots=True)
class RouteAnnouncement:
    """Path-vector route advertisement for a destination."""

    destination: RouteDestination
    path: RoutePath
    metrics: RouteMetrics
    advertiser: ClusterId
    advertised_at: Timestamp = field(default_factory=time.time)
    ttl_seconds: DurationSeconds = 30.0
    epoch: int = 0
    route_tags: tuple[str, ...] = field(default_factory=tuple)
    signature: bytes = b""
    public_key: bytes = b""
    signature_algorithm: str = "ed25519"

    def is_expired(self, now: Timestamp | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.advertised_at + self.ttl_seconds)

    def signature_payload(self) -> JsonDict:
        return {
            "destination": self.destination.to_dict(),
            "path": self.path.to_dict(),
            "metrics": self.metrics.to_dict(),
            "advertiser": self.advertiser,
            "advertised_at": float(self.advertised_at),
            "ttl_seconds": float(self.ttl_seconds),
            "epoch": int(self.epoch),
            "route_tags": sorted(self.route_tags),
        }

    def to_bytes(self) -> bytes:
        return json.dumps(
            self.signature_payload(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()

    def with_signature(
        self, *, signature: bytes, public_key: bytes, algorithm: str
    ) -> RouteAnnouncement:
        return replace(
            self,
            signature=signature,
            public_key=public_key,
            signature_algorithm=algorithm,
        )

    def to_dict(self) -> JsonDict:
        return {
            "destination": self.destination.to_dict(),
            "path": self.path.to_dict(),
            "metrics": self.metrics.to_dict(),
            "advertiser": self.advertiser,
            "advertised_at": float(self.advertised_at),
            "ttl_seconds": float(self.ttl_seconds),
            "epoch": int(self.epoch),
            "route_tags": list(self.route_tags),
            "signature": self.signature.hex() if self.signature else "",
            "public_key": self.public_key.hex() if self.public_key else "",
            "signature_algorithm": self.signature_algorithm,
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RouteAnnouncement:
        signature_hex = str(payload.get("signature", ""))
        public_key_hex = str(payload.get("public_key", ""))
        try:
            signature = bytes.fromhex(signature_hex) if signature_hex else b""
        except ValueError:
            signature = b""
        try:
            public_key = bytes.fromhex(public_key_hex) if public_key_hex else b""
        except ValueError:
            public_key = b""
        route_tags = _as_str_tuple(payload.get("route_tags", ()))
        return cls(
            destination=RouteDestination.from_dict(
                _as_dict(payload.get("destination", {}))
            ),
            path=RoutePath.from_dict(_as_dict(payload.get("path", {}))),
            metrics=RouteMetrics.from_dict(_as_dict(payload.get("metrics", {}))),
            advertiser=str(payload.get("advertiser", "")),
            advertised_at=_as_float(payload.get("advertised_at"), time.time()),
            ttl_seconds=_as_float(payload.get("ttl_seconds"), 30.0),
            epoch=_as_int(payload.get("epoch"), 0),
            route_tags=route_tags,
            signature=signature,
            public_key=public_key,
            signature_algorithm=str(payload.get("signature_algorithm", "ed25519")),
        )


@dataclass(frozen=True, slots=True)
class RouteWithdrawal:
    """Withdrawal for a previously advertised route."""

    destination: RouteDestination
    path: RoutePath
    advertiser: ClusterId
    withdrawn_at: Timestamp = field(default_factory=time.time)
    epoch: int = 0
    route_tags: tuple[str, ...] = field(default_factory=tuple)
    signature: bytes = b""
    public_key: bytes = b""
    signature_algorithm: str = "ed25519"

    def signature_payload(self) -> JsonDict:
        return {
            "destination": self.destination.to_dict(),
            "path": self.path.to_dict(),
            "advertiser": self.advertiser,
            "withdrawn_at": float(self.withdrawn_at),
            "epoch": int(self.epoch),
            "route_tags": sorted(self.route_tags),
        }

    def to_bytes(self) -> bytes:
        return json.dumps(
            self.signature_payload(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()

    def with_signature(
        self, *, signature: bytes, public_key: bytes, algorithm: str
    ) -> RouteWithdrawal:
        return replace(
            self,
            signature=signature,
            public_key=public_key,
            signature_algorithm=algorithm,
        )

    def to_dict(self) -> JsonDict:
        return {
            "destination": self.destination.to_dict(),
            "path": self.path.to_dict(),
            "advertiser": self.advertiser,
            "withdrawn_at": float(self.withdrawn_at),
            "epoch": int(self.epoch),
            "route_tags": list(self.route_tags),
            "signature": self.signature.hex() if self.signature else "",
            "public_key": self.public_key.hex() if self.public_key else "",
            "signature_algorithm": self.signature_algorithm,
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RouteWithdrawal:
        signature_hex = str(payload.get("signature", ""))
        public_key_hex = str(payload.get("public_key", ""))
        try:
            signature = bytes.fromhex(signature_hex) if signature_hex else b""
        except ValueError:
            signature = b""
        try:
            public_key = bytes.fromhex(public_key_hex) if public_key_hex else b""
        except ValueError:
            public_key = b""
        route_tags = _as_str_tuple(payload.get("route_tags", ()))
        return cls(
            destination=RouteDestination.from_dict(
                _as_dict(payload.get("destination", {}))
            ),
            path=RoutePath.from_dict(_as_dict(payload.get("path", {}))),
            advertiser=str(payload.get("advertiser", "")),
            withdrawn_at=_as_float(payload.get("withdrawn_at"), time.time()),
            epoch=_as_int(payload.get("epoch"), 0),
            route_tags=route_tags,
            signature=signature,
            public_key=public_key,
            signature_algorithm=str(payload.get("signature_algorithm", "ed25519")),
        )


@dataclass(frozen=True, slots=True)
class RouteRecord:
    """A learned route entry stored in the local table."""

    destination: RouteDestination
    path: RoutePath
    next_hop: ClusterId
    metrics: RouteMetrics
    advertiser: ClusterId
    learned_from: ClusterId
    advertised_at: Timestamp
    ttl_seconds: DurationSeconds
    epoch: int
    route_tags: tuple[str, ...] = field(default_factory=tuple)

    def is_expired(self, now: Timestamp | None = None) -> bool:
        timestamp = now if now is not None else time.time()
        return timestamp > (self.advertised_at + self.ttl_seconds)


@dataclass(frozen=True, slots=True)
class RouteTiebreaker:
    """Tiebreaker value used for route selection ordering."""

    name: str
    value: float | str


@dataclass(frozen=True, slots=True)
class RouteCandidateTrace:
    """Trace data for a candidate route selection."""

    destination: RouteDestination
    next_hop: ClusterId
    advertiser: ClusterId
    path: RoutePath
    metrics: RouteMetrics
    score: float
    tiebreakers: tuple[RouteTiebreaker, ...]
    expired: bool
    filtered_reason: str | None = None

    def to_dict(self) -> JsonDict:
        return {
            "destination": self.destination.to_dict(),
            "next_hop": self.next_hop,
            "advertiser": self.advertiser,
            "path": self.path.to_dict(),
            "metrics": self.metrics.to_dict(),
            "score": float(self.score),
            "tiebreakers": [
                {"name": item.name, "value": item.value} for item in self.tiebreakers
            ],
            "expired": self.expired,
            "filtered_reason": self.filtered_reason,
        }


@dataclass(frozen=True, slots=True)
class RouteSelectionTrace:
    """Explain how a route selection was made."""

    destination: RouteDestination
    evaluated_at: Timestamp
    avoid_clusters: tuple[ClusterId, ...]
    candidates: tuple[RouteCandidateTrace, ...]
    selected: RouteCandidateTrace | None

    def to_dict(self) -> JsonDict:
        return {
            "destination": self.destination.to_dict(),
            "evaluated_at": float(self.evaluated_at),
            "avoid_clusters": list(self.avoid_clusters),
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "selected": self.selected.to_dict() if self.selected else None,
        }


@dataclass(frozen=True, slots=True)
class RouteBestKey:
    """Key used to detect best-route changes for convergence tracking."""

    advertiser: ClusterId
    next_hop: ClusterId
    path: RoutePath
    epoch: int


@dataclass(frozen=True, slots=True)
class RouteHoldDownKey:
    """Key used to track route hold-down windows."""

    destination: RouteDestination
    advertiser: ClusterId


@dataclass(frozen=True, slots=True)
class RouteFlapState:
    """Track route flap state for dampening."""

    last_change: Timestamp
    flap_count: int
    suppressed_until: Timestamp | None = None


@dataclass(slots=True)
class RouteControlStats:
    """Counters for route control events."""

    announcements_accepted: int = 0
    announcements_rejected: int = 0
    withdrawals_received: int = 0
    withdrawals_applied: int = 0
    hold_down_rejects: int = 0
    suppression_rejects: int = 0
    routes_purged: int = 0
    rejection_reasons: dict[str, int] = field(default_factory=dict)

    def record_reject(self, reason: str) -> None:
        self.announcements_rejected += 1
        self.rejection_reasons[reason] = self.rejection_reasons.get(reason, 0) + 1

    def to_dict(self) -> JsonDict:
        return {
            "announcements_accepted": self.announcements_accepted,
            "announcements_rejected": self.announcements_rejected,
            "withdrawals_received": self.withdrawals_received,
            "withdrawals_applied": self.withdrawals_applied,
            "hold_down_rejects": self.hold_down_rejects,
            "suppression_rejects": self.suppression_rejects,
            "routes_purged": self.routes_purged,
            "rejection_reasons": dict(self.rejection_reasons),
        }


@dataclass(frozen=True, slots=True)
class RouteStabilityPolicy:
    """Stability controls for route management."""

    hold_down_seconds: DurationSeconds = 0.0
    flap_threshold: int = 0
    suppression_window_seconds: DurationSeconds = 0.0


@dataclass(slots=True)
class RouteConvergenceState:
    """Track convergence timing per destination."""

    first_seen_at: Timestamp
    last_change_at: Timestamp
    last_best: RouteBestKey


@dataclass(frozen=True, slots=True)
class RoutePolicy:
    """Policy for scoring and selecting routes."""

    weight_latency: float = 1.0
    weight_hops: float = 10.0
    weight_cost: float = 1.0
    weight_reliability: float = 50.0
    weight_bandwidth: float = 0.05
    max_hops: HopCount | None = None
    allowed_advertisers: set[ClusterId] | None = None
    allowed_destinations: set[ClusterId] | None = None
    allowed_tags: set[str] | None = None
    deny_tags: set[str] | None = None
    deterministic_tiebreakers: tuple[str, ...] = (
        "hop_count",
        "latency_ms",
        "reliability_score",
        "advertiser",
    )

    def accepts(self, metrics: RouteMetrics) -> bool:
        if self.max_hops is None:
            return True
        return metrics.hop_count <= self.max_hops

    def accepts_announcement(
        self, announcement: RouteAnnouncement, *, received_from: ClusterId
    ) -> bool:
        if (
            self.allowed_advertisers is not None
            and announcement.advertiser not in self.allowed_advertisers
        ):
            return False
        if (
            self.allowed_destinations is not None
            and announcement.destination.cluster_id not in self.allowed_destinations
        ):
            return False
        if self.allowed_tags is not None:
            if not set(announcement.route_tags).intersection(self.allowed_tags):
                return False
        if self.deny_tags is not None:
            if set(announcement.route_tags).intersection(self.deny_tags):
                return False
        return True

    def accepts_withdrawal(
        self, withdrawal: RouteWithdrawal, *, received_from: ClusterId
    ) -> bool:
        if (
            self.allowed_advertisers is not None
            and withdrawal.advertiser not in self.allowed_advertisers
        ):
            return False
        if (
            self.allowed_destinations is not None
            and withdrawal.destination.cluster_id not in self.allowed_destinations
        ):
            return False
        if self.allowed_tags is not None:
            if not set(withdrawal.route_tags).intersection(self.allowed_tags):
                return False
        if self.deny_tags is not None:
            if set(withdrawal.route_tags).intersection(self.deny_tags):
                return False
        return True

    def score(self, metrics: RouteMetrics) -> float:
        reliability_penalty = (
            1.0 - metrics.reliability_score
        ) * self.weight_reliability
        bandwidth_bonus = metrics.bandwidth_mbps * self.weight_bandwidth
        return (
            metrics.latency_ms * self.weight_latency
            + metrics.hop_count * self.weight_hops
            + metrics.cost_score * self.weight_cost
            + reliability_penalty
            - bandwidth_bonus
        )

    def sort_key(self, record: RouteRecord) -> tuple[object, ...]:
        base = self.score(record.metrics)
        tiebreakers = self._tiebreaker_values(record)
        return (base, *tiebreakers)

    def tiebreakers(self, record: RouteRecord) -> tuple[RouteTiebreaker, ...]:
        return tuple(
            RouteTiebreaker(name=rule, value=value)
            for rule, value in zip(
                self.deterministic_tiebreakers, self._tiebreaker_values(record)
            )
        )

    def _tiebreaker_values(self, record: RouteRecord) -> tuple[object, ...]:
        values: list[object] = []
        for rule in self.deterministic_tiebreakers:
            if rule == "hop_count":
                values.append(float(record.metrics.hop_count))
            elif rule == "latency_ms":
                values.append(float(record.metrics.latency_ms))
            elif rule == "reliability_score":
                values.append(-float(record.metrics.reliability_score))
            elif rule == "advertiser":
                values.append(record.advertiser)
        return tuple(values)


@dataclass(slots=True)
class RouteTable:
    """Maintain learned routes and select next hops."""

    local_cluster: ClusterId
    policy: RoutePolicy = field(default_factory=RoutePolicy)
    stability_policy: RouteStabilityPolicy = field(default_factory=RouteStabilityPolicy)
    max_routes_per_destination: int = 4
    routes: dict[RouteDestination, list[RouteRecord]] = field(default_factory=dict)
    hold_downs: dict[RouteHoldDownKey, Timestamp] = field(default_factory=dict)
    flap_states: dict[RouteHoldDownKey, RouteFlapState] = field(default_factory=dict)
    convergence_state: dict[RouteDestination, RouteConvergenceState] = field(
        default_factory=dict
    )
    stats: RouteControlStats = field(default_factory=RouteControlStats)

    def build_local_announcement(
        self,
        *,
        ttl_seconds: DurationSeconds = 30.0,
        epoch: int = 0,
        now: Timestamp | None = None,
        route_tags: tuple[str, ...] = (),
    ) -> RouteAnnouncement:
        timestamp = now if now is not None else time.time()
        destination = RouteDestination(cluster_id=self.local_cluster)
        path = RoutePath((self.local_cluster,))
        return RouteAnnouncement(
            destination=destination,
            path=path,
            metrics=RouteMetrics(),
            advertiser=self.local_cluster,
            advertised_at=timestamp,
            ttl_seconds=ttl_seconds,
            epoch=epoch,
            route_tags=route_tags,
        )

    def apply_announcement(
        self,
        announcement: RouteAnnouncement,
        *,
        received_from: ClusterId,
        now: Timestamp | None = None,
        link_latency_ms: NetworkLatencyMs = 0.0,
        link_bandwidth_mbps: BandwidthMbps | None = None,
        link_reliability: ReliabilityScore = 1.0,
        link_cost: RouteCostScore = 0.0,
    ) -> bool:
        timestamp = now if now is not None else time.time()
        self._purge_hold_downs(timestamp)
        if announcement.is_expired(timestamp):
            self.stats.record_reject("expired")
            logger.debug(
                "Route announcement rejected: expired destination={} advertiser={} received_from={}",
                announcement.destination.cluster_id,
                announcement.advertiser,
                received_from,
            )
            return False
        if received_from == self.local_cluster:
            self.stats.record_reject("self")
            logger.debug(
                "Route announcement rejected: self destination={} advertiser={} received_from={}",
                announcement.destination.cluster_id,
                announcement.advertiser,
                received_from,
            )
            return False
        if not self.policy.accepts_announcement(
            announcement, received_from=received_from
        ):
            self.stats.record_reject("policy")
            logger.debug(
                "Route announcement rejected: policy destination={} advertiser={} received_from={}",
                announcement.destination.cluster_id,
                announcement.advertiser,
                received_from,
            )
            return False
        if not announcement.path.hops:
            self.stats.record_reject("empty_path")
            logger.debug(
                "Route announcement rejected: empty_path destination={} advertiser={} received_from={}",
                announcement.destination.cluster_id,
                announcement.advertiser,
                received_from,
            )
            return False
        if announcement.path.hops[0] != announcement.advertiser:
            self.stats.record_reject("advertiser_mismatch")
            logger.debug(
                "Route announcement rejected: advertiser_mismatch destination={} advertiser={} received_from={}",
                announcement.destination.cluster_id,
                announcement.advertiser,
                received_from,
            )
            return False
        if announcement.destination.cluster_id != announcement.path.hops[-1]:
            self.stats.record_reject("destination_mismatch")
            logger.debug(
                "Route announcement rejected: destination_mismatch destination={} advertiser={} received_from={}",
                announcement.destination.cluster_id,
                announcement.advertiser,
                received_from,
            )
            return False
        if announcement.path.contains(self.local_cluster):
            self.stats.record_reject("loop")
            logger.debug(
                "Route announcement rejected: loop destination={} advertiser={} received_from={}",
                announcement.destination.cluster_id,
                announcement.advertiser,
                received_from,
            )
            return False
        hold_down_key = RouteHoldDownKey(
            destination=announcement.destination,
            advertiser=announcement.advertiser,
        )
        if self._is_hold_down_active(hold_down_key, timestamp):
            self.stats.hold_down_rejects += 1
            self.stats.record_reject("hold_down")
            logger.debug(
                "Route announcement rejected: hold_down destination={} advertiser={} received_from={}",
                announcement.destination.cluster_id,
                announcement.advertiser,
                received_from,
            )
            return False
        if self._is_suppressed(hold_down_key, timestamp):
            self.stats.suppression_rejects += 1
            self.stats.record_reject("suppressed")
            logger.debug(
                "Route announcement rejected: suppressed destination={} advertiser={} received_from={}",
                announcement.destination.cluster_id,
                announcement.advertiser,
                received_from,
            )
            return False

        new_path = announcement.path.with_prefix(self.local_cluster)
        metrics = announcement.metrics.with_added_hop(
            latency_ms=link_latency_ms,
            bandwidth_mbps=link_bandwidth_mbps,
            reliability_score=link_reliability,
            cost_score=link_cost,
        )
        if not self.policy.accepts(metrics):
            self.stats.record_reject("policy_metrics")
            logger.debug(
                "Route announcement rejected: policy_metrics destination={} advertiser={} received_from={}",
                announcement.destination.cluster_id,
                announcement.advertiser,
                received_from,
            )
            return False

        record = RouteRecord(
            destination=announcement.destination,
            path=new_path,
            next_hop=received_from,
            metrics=metrics,
            advertiser=announcement.advertiser,
            learned_from=received_from,
            advertised_at=announcement.advertised_at,
            ttl_seconds=announcement.ttl_seconds,
            epoch=announcement.epoch,
            route_tags=announcement.route_tags,
        )
        updated = self._upsert(record, timestamp)
        if updated:
            self.stats.announcements_accepted += 1
            logger.debug(
                "Route announcement accepted: destination={} advertiser={} received_from={} hop_count={}",
                record.destination.cluster_id,
                record.advertiser,
                record.learned_from,
                record.metrics.hop_count,
            )
        else:
            self.stats.record_reject("stale")
            logger.debug(
                "Route announcement rejected: stale destination={} advertiser={} received_from={}",
                record.destination.cluster_id,
                record.advertiser,
                record.learned_from,
            )
        return updated

    def apply_withdrawal(
        self,
        withdrawal: RouteWithdrawal,
        *,
        received_from: ClusterId,
        now: Timestamp | None = None,
    ) -> tuple[RouteRecord, ...]:
        timestamp = now if now is not None else time.time()
        self._purge_hold_downs(timestamp)
        self.stats.withdrawals_received += 1
        if withdrawal.advertiser != received_from:
            logger.debug(
                "Route withdrawal rejected: advertiser_mismatch destination={} advertiser={} received_from={}",
                withdrawal.destination.cluster_id,
                withdrawal.advertiser,
                received_from,
            )
            return ()
        if not withdrawal.path.hops:
            logger.debug(
                "Route withdrawal rejected: empty_path destination={} advertiser={} received_from={}",
                withdrawal.destination.cluster_id,
                withdrawal.advertiser,
                received_from,
            )
            return ()
        if withdrawal.path.hops[0] != withdrawal.advertiser:
            logger.debug(
                "Route withdrawal rejected: advertiser_mismatch destination={} advertiser={} received_from={}",
                withdrawal.destination.cluster_id,
                withdrawal.advertiser,
                received_from,
            )
            return ()
        if withdrawal.destination.cluster_id != withdrawal.path.hops[-1]:
            logger.debug(
                "Route withdrawal rejected: destination_mismatch destination={} advertiser={} received_from={}",
                withdrawal.destination.cluster_id,
                withdrawal.advertiser,
                received_from,
            )
            return ()

        records = list(self.routes.get(withdrawal.destination, []))
        removed = [
            record
            for record in records
            if record.advertiser == withdrawal.advertiser
            and record.learned_from == received_from
        ]
        if not removed:
            logger.debug(
                "Route withdrawal ignored: no matching routes destination={} advertiser={} received_from={}",
                withdrawal.destination.cluster_id,
                withdrawal.advertiser,
                received_from,
            )
            return ()
        remaining = [record for record in records if record not in removed]
        if remaining:
            self.routes[withdrawal.destination] = remaining
        else:
            self.routes.pop(withdrawal.destination, None)
        self.stats.withdrawals_applied += 1
        for record in removed:
            key = RouteHoldDownKey(
                destination=record.destination,
                advertiser=record.advertiser,
            )
            self._apply_hold_down(key, timestamp)
            self._record_flap_event(key, timestamp)
        logger.debug(
            "Route withdrawal applied: destination={} advertiser={} received_from={} removed={}",
            withdrawal.destination.cluster_id,
            withdrawal.advertiser,
            received_from,
            len(removed),
        )
        self._refresh_convergence_state(withdrawal.destination, timestamp)
        return tuple(removed)

    def remove_routes_for_neighbor(
        self, *, learned_from: ClusterId, now: Timestamp | None = None
    ) -> tuple[RouteRecord, ...]:
        """Remove routes learned from a neighbor cluster."""
        timestamp = now if now is not None else time.time()
        removed: list[RouteRecord] = []
        for destination, records in list(self.routes.items()):
            remaining: list[RouteRecord] = []
            removed_for_destination = 0
            for record in records:
                if record.learned_from == learned_from:
                    removed.append(record)
                    removed_for_destination += 1
                    key = RouteHoldDownKey(
                        destination=record.destination,
                        advertiser=record.advertiser,
                    )
                    self._apply_hold_down(key, timestamp)
                    self._record_flap_event(key, timestamp)
                else:
                    remaining.append(record)
            if remaining:
                self.routes[destination] = remaining
            else:
                self.routes.pop(destination, None)
            if removed_for_destination:
                self._refresh_convergence_state(destination, timestamp)

        if removed:
            self.stats.withdrawals_applied += len(removed)
            logger.debug(
                "Routes removed for neighbor: learned_from={} removed={}",
                learned_from,
                len(removed),
            )
        return tuple(removed)

    def select_route(
        self,
        destination: RouteDestination,
        *,
        avoid_clusters: tuple[ClusterId, ...] = (),
        now: Timestamp | None = None,
    ) -> RouteRecord | None:
        candidates = [
            record
            for record in self.routes.get(destination, [])
            if not record.is_expired(now)
        ]
        if avoid_clusters:
            avoid_set = set(avoid_clusters)
            candidates = [
                record
                for record in candidates
                if not record.path.contains_any(avoid_set)
            ]
        if not candidates:
            return None
        candidates.sort(key=self.policy.sort_key)
        return candidates[0]

    def explain_selection(
        self,
        destination: RouteDestination,
        *,
        avoid_clusters: tuple[ClusterId, ...] = (),
        now: Timestamp | None = None,
    ) -> RouteSelectionTrace:
        timestamp = now if now is not None else time.time()
        avoid_set = set(avoid_clusters)
        raw_candidates = list(self.routes.get(destination, []))
        traces: list[RouteCandidateTrace] = []
        valid: list[tuple[RouteRecord, RouteCandidateTrace]] = []

        for record in raw_candidates:
            expired = record.is_expired(timestamp)
            filtered_reason = None
            if expired:
                filtered_reason = "expired"
            elif avoid_set and record.path.contains_any(avoid_set):
                filtered_reason = "avoid_clusters"
            trace = RouteCandidateTrace(
                destination=record.destination,
                next_hop=record.next_hop,
                advertiser=record.advertiser,
                path=record.path,
                metrics=record.metrics,
                score=self.policy.score(record.metrics),
                tiebreakers=self.policy.tiebreakers(record),
                expired=expired,
                filtered_reason=filtered_reason,
            )
            traces.append(trace)
            if filtered_reason is None:
                valid.append((record, trace))

        selected = None
        if valid:
            valid.sort(key=lambda entry: self.policy.sort_key(entry[0]))
            selected = valid[0][1]

        return RouteSelectionTrace(
            destination=destination,
            evaluated_at=timestamp,
            avoid_clusters=avoid_clusters,
            candidates=tuple(traces),
            selected=selected,
        )

    def routes_for(
        self, destination: RouteDestination, *, now: Timestamp | None = None
    ) -> tuple[RouteRecord, ...]:
        return tuple(
            record
            for record in self.routes.get(destination, [])
            if not record.is_expired(now)
        )

    def metrics_snapshot(self, *, now: Timestamp | None = None) -> JsonDict:
        timestamp = now if now is not None else time.time()
        snapshot = dict(self.stats.to_dict())
        routes_active_total = 0
        routes_per_destination: dict[str, int] = {}
        for destination, records in self.routes.items():
            active = [record for record in records if not record.is_expired(timestamp)]
            if not active:
                continue
            routes_per_destination[destination.cluster_id] = len(active)
            routes_active_total += len(active)
        snapshot["routes_active_total"] = routes_active_total
        snapshot["destinations_tracked"] = len(routes_per_destination)
        snapshot["routes_per_destination"] = routes_per_destination

        convergence_values: list[float] = []
        for destination, state in self.convergence_state.items():
            if destination.cluster_id not in routes_per_destination:
                continue
            convergence_values.append(
                max(0.0, state.last_change_at - state.first_seen_at)
            )
        if convergence_values:
            snapshot["convergence_seconds_avg"] = sum(convergence_values) / len(
                convergence_values
            )
            snapshot["convergence_seconds_max"] = max(convergence_values)
        else:
            snapshot["convergence_seconds_avg"] = 0.0
            snapshot["convergence_seconds_max"] = 0.0
        return snapshot

    def purge_expired(self, *, now: Timestamp | None = None) -> int:
        timestamp = now if now is not None else time.time()
        removed = 0
        for destination, records in list(self.routes.items()):
            removed_for_destination = 0
            active = []
            for record in records:
                if record.is_expired(timestamp):
                    removed += 1
                    removed_for_destination += 1
                    key = RouteHoldDownKey(
                        destination=record.destination,
                        advertiser=record.advertiser,
                    )
                    self._record_flap_event(key, timestamp)
                else:
                    active.append(record)
            if active:
                active.sort(key=self.policy.sort_key)
                self.routes[destination] = active
            else:
                self.routes.pop(destination, None)
            if removed_for_destination:
                self._refresh_convergence_state(destination, timestamp)
        if removed:
            self.stats.routes_purged += removed
            logger.debug("Expired routes purged: removed={}", removed)
        return removed

    def _apply_hold_down(self, key: RouteHoldDownKey, now: Timestamp) -> None:
        if self.stability_policy.hold_down_seconds <= 0:
            return
        self.hold_downs[key] = now + self.stability_policy.hold_down_seconds
        logger.debug(
            "Route hold-down applied: destination={} advertiser={} until={}",
            key.destination.cluster_id,
            key.advertiser,
            self.hold_downs[key],
        )

    def _purge_hold_downs(self, now: Timestamp) -> None:
        expired = [key for key, until in self.hold_downs.items() if now >= until]
        for key in expired:
            self.hold_downs.pop(key, None)

        expired_flaps = [
            key
            for key, state in self.flap_states.items()
            if state.suppressed_until is not None and now >= state.suppressed_until
        ]
        for key in expired_flaps:
            self.flap_states.pop(key, None)

    def _is_hold_down_active(self, key: RouteHoldDownKey, now: Timestamp) -> bool:
        until = self.hold_downs.get(key)
        if until is None:
            return False
        if now >= until:
            self.hold_downs.pop(key, None)
            return False
        return True

    def _is_suppressed(self, key: RouteHoldDownKey, now: Timestamp) -> bool:
        state = self.flap_states.get(key)
        if not state or state.suppressed_until is None:
            return False
        if now >= state.suppressed_until:
            self.flap_states.pop(key, None)
            return False
        return True

    def _record_flap_event(self, key: RouteHoldDownKey, now: Timestamp) -> None:
        policy = self.stability_policy
        if policy.flap_threshold <= 0 or policy.suppression_window_seconds <= 0:
            return
        state = self.flap_states.get(key)
        if state is None:
            flap_count = 1
        else:
            if now - state.last_change > policy.suppression_window_seconds:
                flap_count = 1
            else:
                flap_count = state.flap_count + 1
        suppressed_until = None
        if flap_count >= policy.flap_threshold:
            suppressed_until = now + policy.suppression_window_seconds
        self.flap_states[key] = RouteFlapState(
            last_change=now,
            flap_count=flap_count,
            suppressed_until=suppressed_until,
        )
        if suppressed_until is not None and (
            state is None or state.suppressed_until is None
        ):
            logger.debug(
                "Route suppression activated: destination={} advertiser={} until={} flap_count={}",
                key.destination.cluster_id,
                key.advertiser,
                suppressed_until,
                flap_count,
            )

    def _refresh_convergence_state(
        self, destination: RouteDestination, now: Timestamp
    ) -> None:
        records = [
            record
            for record in self.routes.get(destination, [])
            if not record.is_expired(now)
        ]
        if not records:
            self.convergence_state.pop(destination, None)
            return
        records.sort(key=self.policy.sort_key)
        self.routes[destination] = records
        best = records[0]
        best_key = RouteBestKey(
            advertiser=best.advertiser,
            next_hop=best.next_hop,
            path=best.path,
            epoch=best.epoch,
        )
        existing = self.convergence_state.get(destination)
        if existing is None:
            self.convergence_state[destination] = RouteConvergenceState(
                first_seen_at=now,
                last_change_at=now,
                last_best=best_key,
            )
            return
        if existing.last_best != best_key:
            existing.last_best = best_key
            existing.last_change_at = now

    def _upsert(self, record: RouteRecord, now: Timestamp) -> bool:
        records = list(self.routes.get(record.destination, []))
        updated = False
        for idx, existing in enumerate(records):
            if existing.next_hop == record.next_hop and existing.path == record.path:
                if record.epoch < existing.epoch or (
                    record.epoch == existing.epoch
                    and record.advertised_at <= existing.advertised_at
                ):
                    return False
                records[idx] = record
                updated = True
                break
        if not updated:
            records.append(record)
            updated = True

        records = [entry for entry in records if not entry.is_expired(now)]
        records.sort(key=self.policy.sort_key)
        if len(records) > self.max_routes_per_destination:
            records = records[: self.max_routes_per_destination]
        self.routes[record.destination] = records
        self._refresh_convergence_state(record.destination, now)
        return updated
