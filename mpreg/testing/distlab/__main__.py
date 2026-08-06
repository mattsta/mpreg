"""CLI for DistLab: list and run registered scenarios.

Honesty banner (always printed on help):
  DistLab is Jepsen-inspired history/checker/nemesis — not Elle, not WAN,
  not BFT proofs, not kernel partitions, not a JVM Jepsen port.

Usage::

    python -m mpreg.testing.distlab list
    python -m mpreg.testing.distlab run strong.happy_3
    python -m mpreg.testing.distlab run strong.happy_3 --json
    python -m mpreg.testing.distlab catalog
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

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

def cmd_list(args: argparse.Namespace) -> int:
    reg = _ensure_registry()
    names = reg.list()
    if args.track:
        names = [n for n in names if reg.meta(n).get("track") == args.track]
    for n in names:
        m = reg.meta(n)
        desc = m.get("description") or ""
        track = m.get("track") or ""
        print(f"{n:40s}  {track:4s}  {desc}")
    return 0

def cmd_catalog(args: argparse.Namespace) -> int:
    reg = _ensure_registry()
    cat = reg.catalog()
    if args.json:
        print(json.dumps(cat, indent=2, default=str))
    else:
        for row in cat:
            print(
                f"{row['name']}\ttrack={row.get('track','')}\t"
                f"tags={','.join(row.get('tags') or [])}\t{row.get('description','')}"
            )
    return 0

def cmd_run(args: argparse.Namespace) -> int:
    reg = _ensure_registry()
    name = args.name

    async def _run():
        return await reg.run(name)

    try:
        result = asyncio.run(_run())
    except KeyError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except AssertionError as e:
        payload = {
            "name": name,
            "ok": False,
            "error": str(e),
        }
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print(f"FAIL {name}: {e}", file=sys.stderr)
        return 1

    if args.json:
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

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m mpreg.testing.distlab",
        description=(
            "MPREG DistLab — first-party distributed testing lab. "
            "Jepsen-inspired; honest non-claims: " + "; ".join(NON_CLAIMS)
        ),
        epilog="See docs/plans/DISTLAB_SEVEN_TRACK_MASTER_PLAN.md",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    pl = sub.add_parser("list", help="List registered scenario names")
    pl.add_argument("--track", default="", help="Filter by track id (T1..T7)")
    pl.set_defaults(func=cmd_list)

    pc = sub.add_parser("catalog", help="Show scenario catalog with metadata")
    pc.add_argument("--json", action="store_true")
    pc.set_defaults(func=cmd_catalog)

    pr = sub.add_parser("run", help="Run one in-process scenario by name")
    pr.add_argument("name", help="Scenario name (e.g. strong.happy_3)")
    pr.add_argument("--json", action="store_true", help="Emit ScenarioResult JSON")
    pr.set_defaults(func=cmd_run)

    return p

def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))

if __name__ == "__main__":
    raise SystemExit(main())
