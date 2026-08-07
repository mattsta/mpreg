"""Process resource limits for high-concurrency test runs.

macOS ships with a launchctl soft ``maxfiles`` of 256. Concurrent xdist workers
that spin multi-node clusters open hundreds of sockets and will thrash or hang
unless the process soft RLIMIT_NOFILE is raised before pytest starts.
"""

from __future__ import annotations

import resource
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class NoFileLimit:
    """Snapshot of the process open-file soft/hard limits."""

    soft: int
    hard: int

    @classmethod
    def current(cls) -> NoFileLimit:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        return cls(soft=soft, hard=hard)


def raise_open_file_limit(target: int = 1_048_576) -> NoFileLimit:
    """Raise soft RLIMIT_NOFILE toward ``target`` (capped by hard limit).

    Returns the limit in effect after the attempt. Never raises on failure to
    raise — callers log the returned snapshot and proceed.
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    # hard may be resource.RLIM_INFINITY on some platforms
    if hard == resource.RLIM_INFINITY:
        new_soft = max(soft, target)
        new_hard = hard
    else:
        new_soft = min(max(soft, target), hard)
        new_hard = hard
    if new_soft > soft:
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, new_hard))
        except ValueError, OSError:
            # Best-effort stepwise fallback (some kernels reject large jumps).
            for candidate in (65536, 16384, 8192):
                if candidate <= soft or (
                    hard != resource.RLIM_INFINITY and candidate > hard
                ):
                    continue
                try:
                    resource.setrlimit(resource.RLIMIT_NOFILE, (candidate, new_hard))
                    break
                except ValueError, OSError:
                    continue
    return NoFileLimit.current()
