"""DistLab CLI implementation — invoked only via ``uv run mpreg distlab …``.

Never document or call ``python -m``. Architecture requires top-level
pyproject entry points (``mpreg`` / ``mpreg-example``).
"""

from __future__ import annotations

import asyncio
import json
import sys
from typing import Any

NON_CLAIMS = (
    "not Elle linearizability",
    "not WAN / geo generators",
    "not BFT / Byzantine proofs",
    "not kernel/iptables partitions",
    "not JVM Jepsen port",
    "not fsync / kill-9 cold restart guarantees",
)

def _ensure_registry():
    from mpreg.testing.distlab.builtins import ensure_builtins
    from mpreg.testing.distlab.registry import get_registry

    ensure_builtins()
    return get_registry()

def list_scenarios(*, track: str = "", as_json: bool = False) -> int:
    """Print registered scenario names. Exit 0."""
    reg = _ensure_registry()
    names = reg.list()
    if track:
        names = [n for n in names if reg.meta(n).get("track") == track]
    if as_json:
        rows = [{"name": n, **reg.meta(n)} for n in names]
        print(json.dumps(rows, indent=2, default=str))
        return 0
    for n in names:
        m = reg.meta(n)
        desc = m.get("description") or ""
        tr = m.get("track") or ""
        print(f"{n:40s}  {tr:4s}  {desc}")
    return 0

def catalog(*, as_json: bool = False) -> int:
    """Print full scenario catalog with metadata."""
    reg = _ensure_registry()
    cat = reg.catalog()
    if as_json:
        print(json.dumps(cat, indent=2, default=str))
    else:
        for row in cat:
            tags = ",".join(row.get("tags") or [])
            print(
                f"{row['name']:40s}  track={row.get('track', ''):4s}  "
                f"tags={tags:24s}  {row.get('description', '')}"
            )
    return 0

def run_scenario(name: str, *, as_json: bool = False) -> int:
    """Run one in-process scenario by name. Exit 0 pass / 1 fail / 2 unknown."""
    reg = _ensure_registry()

    async def _run():
        return await reg.run(name)

    try:
        result = asyncio.run(_run())
    except KeyError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except AssertionError as e:
        payload: dict[str, Any] = {"name": name, "ok": False, "error": str(e)}
        if as_json:
            print(json.dumps(payload, indent=2))
        else:
            print(f"FAIL {name}: {e}", file=sys.stderr)
        return 1

    if as_json:
        print(json.dumps(result.to_dict(), indent=2, default=str))
    else:
        status = "PASS" if result.ok else "FAIL"
        print(
            f"{status} {result.name} duration={result.duration_s:.3f}s "
            f"history={result.history_len} nemesis={result.nemesis_actions}"
        )
        if not result.ok and result.check.violations:
            for v in result.check.violations[:12]:
                print(f"  - [{v.checker}] {v.message}", file=sys.stderr)
    return 0 if result.ok else 1

def main(argv: list[str] | None = None) -> int:
    """Argparse entry for tests / thin wrappers. Prefer ``uv run mpreg distlab``."""
    import argparse

    p = argparse.ArgumentParser(
        prog="mpreg distlab",
        description=(
            "MPREG DistLab — first-party distributed testing lab. "
            "Jepsen-inspired; honest non-claims: " + "; ".join(NON_CLAIMS)
        ),
        epilog=(
            "Entry point: uv run mpreg distlab …  "
            "See docs/plans/DISTLAB_SEVEN_TRACK_MASTER_PLAN.md"
        ),
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    pl = sub.add_parser("list", help="List registered scenario names")
    pl.add_argument("--track", default="", help="Filter by track id (T1..T7)")
    pl.add_argument("--json", action="store_true")
    pl.set_defaults(
        func=lambda a: list_scenarios(track=a.track or "", as_json=a.json)
    )

    pc = sub.add_parser("catalog", help="Show scenario catalog with metadata")
    pc.add_argument("--json", action="store_true")
    pc.set_defaults(func=lambda a: catalog(as_json=a.json))

    pr = sub.add_parser("run", help="Run one in-process scenario by name")
    pr.add_argument("name", help="Scenario name (e.g. strong.happy_3)")
    pr.add_argument("--json", action="store_true", help="Emit ScenarioResult JSON")
    pr.set_defaults(func=lambda a: run_scenario(a.name, as_json=a.json))

    args = p.parse_args(argv)
    return int(args.func(args))
