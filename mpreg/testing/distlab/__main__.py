"""Blocked module path — use the pyproject entry point instead.

Architecture rule: never ``python -m``. Use::

    uv run mpreg distlab list
    uv run mpreg distlab run strong.happy_3
"""

from __future__ import annotations

import sys

_MSG = (
    "error: python -m mpreg.testing.distlab is not supported.\n"
    "Use the top-level entry point:\n"
    "  uv run mpreg distlab list\n"
    "  uv run mpreg distlab catalog\n"
    "  uv run mpreg distlab run strong.happy_3\n"
)


def main() -> int:
    sys.stderr.write(_MSG)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
