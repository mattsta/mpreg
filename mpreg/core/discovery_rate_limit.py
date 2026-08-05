from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from threading import RLock

from mpreg.datastructures.type_aliases import (
    DurationSeconds,
    NamespaceName,
    Timestamp,
    ViewerId,
)

@dataclass(frozen=True, slots=True)
class DiscoveryRateLimitKey:
    viewer_id: ViewerId
    command: str
    namespace: NamespaceName | None = None

@dataclass(frozen=True, slots=True)
class DiscoveryRateLimitConfig:
    enabled: bool
    max_requests: int
    window_seconds: DurationSeconds
    max_keys: int = 1000

@dataclass(slots=True)
class DiscoveryRateLimiter:
    config: DiscoveryRateLimitConfig
    _events: dict[DiscoveryRateLimitKey, deque[Timestamp]] = field(default_factory=dict)
    _lock: RLock = field(default_factory=RLock)

    def allow(self, key: DiscoveryRateLimitKey, *, now: Timestamp) -> bool:
        if not self.config.enabled:
            return True
        if self.config.max_requests <= 0:
            return False
        window_seconds = float(self.config.window_seconds)
        if window_seconds <= 0:
            return True
        with self._lock:
            events = self._events.get(key)
            if events is None:
                # F20: hard cap — make room for this new key before insert.
                if (
                    self.config.max_keys > 0
                    and len(self._events) >= self.config.max_keys
                ):
                    self._prune(
                        now=now,
                        window_seconds=window_seconds,
                        target_size=max(0, self.config.max_keys - 1),
                    )
                events = self._events.get(key)
            if events is None:
                events = deque()
                self._events[key] = events
            self._prune_events(events, now=now, window_seconds=window_seconds)
            if len(events) >= self.config.max_requests:
                return False
            events.append(now)
            return True

    def _prune(
        self,
        *,
        now: Timestamp,
        window_seconds: float,
        target_size: int | None = None,
    ) -> None:
        expired_keys: list[DiscoveryRateLimitKey] = []
        for key, events in self._events.items():
            self._prune_events(events, now=now, window_seconds=window_seconds)
            if not events:
                expired_keys.append(key)
        for key in expired_keys:
            self._events.pop(key, None)
        if self.config.max_keys <= 0:
            return
        cap = self.config.max_keys if target_size is None else target_size
        if len(self._events) <= cap:
            return
        ordered = sorted(
            self._events.items(),
            key=lambda item: item[1][-1] if item[1] else 0.0,
        )
        excess = len(self._events) - cap
        for key, _ in ordered[:excess]:
            self._events.pop(key, None)

    @staticmethod
    def _prune_events(
        events: deque[Timestamp], *, now: Timestamp, window_seconds: float
    ) -> None:
        expired_before = now - window_seconds
        while events and events[0] <= expired_before:
            events.popleft()
