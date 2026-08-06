from mpreg.server_pkg.peer_dial import (
    PeerDialBackoff,
    PeerDialState,
    backoff_base_seconds,
    backoff_cap_seconds,
    dial_exploration_slots,
    dial_parallelism,
    dial_pressure,
    dial_pressure_from_counts,
    reconcile_interval_seconds,
    select_peer_connection_policy,
    selection_spread,
    spread_fraction,
    spread_fraction_for_url,
    target_connection_count,
)

def test_backoff_and_pressure_counts() -> None:
    b = PeerDialBackoff(base_seconds=0.1, cap_seconds=1.0)
    assert b.delay_for_failures(0) == 0.0
    assert b.delay_for_failures(1) == 0.1
    assert b.delay_for_failures(10) == 1.0
    assert dial_pressure_from_counts(recent_failures=0, recent_successes=0) == 0.0
    assert dial_pressure_from_counts(recent_failures=3, recent_successes=1) == 0.75
    assert 0.0 <= spread_fraction(attempt=3) <= 1.0

def test_select_peer_connection_policy_fast_vs_steady() -> None:
    fast = select_peer_connection_policy(
        fast_connect=True,
        peer_target_count=8,
        consecutive_failures=0,
        connected_ratio=1.0,
    )
    steady = select_peer_connection_policy(
        fast_connect=False,
        peer_target_count=8,
        consecutive_failures=0,
        connected_ratio=1.0,
    )
    assert fast.connect_timeout_seconds <= steady.connect_timeout_seconds
    assert fast.max_retries >= 0
    assert steady.max_retries >= 0

def test_large_fabric_near_isolation_caps_retries() -> None:
    policy = select_peer_connection_policy(
        fast_connect=True,
        peer_target_count=40,
        consecutive_failures=5,
        connected_ratio=0.05,
    )
    assert policy.max_retries == 0

def test_dial_pressure_parallelism_exploration() -> None:
    p = dial_pressure(peer_target_count=36, connected_ratio=0.1)
    assert p > 0.0
    parallel = dial_parallelism(peer_target_count=36, connected_ratio=0.1)
    assert parallel >= 2
    slots = dial_exploration_slots(
        peer_target_count=36, connected_ratio=0.2, discovery_ratio=0.5
    )
    assert slots >= 2
    assert (
        dial_exploration_slots(
            peer_target_count=8, connected_ratio=0.2, discovery_ratio=0.5
        )
        == 0
    )

def test_target_connection_and_backoff() -> None:
    assert target_connection_count(peer_target_count=10) == 10
    large = target_connection_count(
        peer_target_count=40, connected_ratio=0.2, discovery_ratio=0.4
    )
    assert 6 <= large <= 40
    base = backoff_base_seconds(
        peer_target_count=16, connected_ratio=0.5, gossip_interval=1.0
    )
    cap = backoff_cap_seconds(peer_target_count=16, gossip_interval=1.0)
    assert 0.2 <= base <= cap
    interval = reconcile_interval_seconds(
        peer_target_count=16, connected_ratio=0.5, gossip_interval=1.0
    )
    assert interval > 0

def test_peer_dial_state_failure_backoff() -> None:
    state = PeerDialState()
    now = 1000.0
    state.record_attempt(now)
    state.record_failure(
        now=now,
        base_delay_seconds=0.5,
        max_delay_seconds=8.0,
        spread_fraction=0.1,
    )
    assert state.consecutive_failures == 1
    assert state.next_attempt_at > now
    state.record_success(now + 10)
    assert state.consecutive_failures == 0

def test_spread_helpers_deterministic() -> None:
    a = spread_fraction_for_url("ws://a:1")
    b = spread_fraction_for_url("ws://a:1")
    assert a == b
    s1 = selection_spread(local_url="ws://local:1", peer_url="ws://peer:2")
    s2 = selection_spread(local_url="ws://local:1", peer_url="ws://peer:2")
    assert s1 == s2
    assert 0.0 <= s1 <= 1.0
