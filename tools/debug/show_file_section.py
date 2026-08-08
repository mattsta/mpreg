#!/usr/bin/env python3
"""Print numbered file sections for deep-dive audits."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

type LineNumber = int


@dataclass(frozen=True, slots=True)
class SectionRequest:
    path: Path
    start: LineNumber
    end: LineNumber


def _parse_args() -> SectionRequest:
    parser = argparse.ArgumentParser(
        description="Print a numbered line section from a file"
    )
    parser.add_argument("--file", required=True, help="Path to file")
    parser.add_argument("--start", required=True, type=int, help="Start line (1-based)")
    parser.add_argument("--end", required=True, type=int, help="End line (1-based)")
    args = parser.parse_args()

    start = max(1, int(args.start))
    end = max(start, int(args.end))
    return SectionRequest(path=Path(str(args.file)), start=start, end=end)


def main() -> int:
    request = _parse_args()
    if not request.path.exists():
        print(f"missing_file={request.path}")
        return 2

    lines = request.path.read_text(encoding="utf-8").splitlines()
    max_line = len(lines)
    start = min(request.start, max_line)
    end = min(request.end, max_line)

    print(f"file={request.path}")
    print(f"range={start}:{end}")
    for line_no in range(start, end + 1):
        print(f"{line_no:5d}: {lines[line_no - 1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
