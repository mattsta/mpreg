import time

from mpreg.core.model import GoodbyeReason
from mpreg.server_pkg.types import (
    CommandExecutionResult,
    DepartedPeer,
    MessageStats,
    RemoteCommandStats,
)

def test_message_and_remote_stats() -> None:
    stats = MessageStats()
    stats.total_processed = 3
    remote = RemoteCommandStats()
    remote.record("echo")
    assert remote.total == 1
    assert remote.last_command == "echo"

def test_departed_peer_expiry() -> None:
    now = time.time()
    peer = DepartedPeer(
        node_url="ws://x:1",
        instance_id="i",
        cluster_id="c",
        reason=GoodbyeReason.GRACEFUL_SHUTDOWN,
        departed_at=now - 10,
        ttl_seconds=1.0,
    )
    assert peer.is_expired(now)

def test_command_execution_result() -> None:
    result = CommandExecutionResult(name="a", value=1)
    assert result.name == "a"
    assert result.value == 1
