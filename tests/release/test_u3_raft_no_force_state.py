"""U3: production tests must not assign RaftState on live ProductionRaft nodes.

MagicMock harnesses may still set attributes. Real modules under tests/ that
import ProductionRaft should use reset_to_follower / testing_set_state / elections.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"

# Allowlist: pure mock construction only (attribute on MagicMock)
_ALLOW_SUBSTRINGS = (
    "MagicMock()",
    "raft_node = MagicMock",
    "node = MagicMock",
)


def test_no_raw_current_state_assignment_on_raft_nodes() -> None:
    import re

    # Real assignment: identifier.current_state = RaftState.X
    assign_re = re.compile(
        r"(?<![\"'])\b\w+\.current_state\s*=\s*RaftState\.(FOLLOWER|LEADER|CANDIDATE)\b"
    )
    offenders: list[str] = []
    for path in TESTS.rglob("*.py"):
        if path.name == "test_u3_raft_no_force_state.py":
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for i, line in enumerate(text.splitlines(), 1):
            if not assign_re.search(line):
                continue
            start = max(0, i - 8)
            ctx = "\n".join(text.splitlines()[start:i])
            if any(a in ctx for a in _ALLOW_SUBSTRINGS):
                continue
            if "testing_set_state" in line:
                continue
            offenders.append(f"{path.relative_to(ROOT)}:{i}:{line.strip()}")
    assert not offenders, "Use reset_to_follower/testing_set_state:\n" + "\n".join(
        offenders
    )
