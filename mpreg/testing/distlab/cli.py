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


def list_presets(*, as_json: bool = False) -> int:
    """Print named suite presets (composed presets expand via resolve_preset)."""
    from mpreg.testing.distlab.registry import SUITE_PRESETS, resolve_preset

    resolved = {k: resolve_preset(k) for k in sorted(SUITE_PRESETS)}
    if as_json:
        print(json.dumps(resolved, indent=2))
        return 0
    for name, scenarios in resolved.items():
        print(f"{name:16s}  {', '.join(scenarios)}")
    return 0


def run_suite(
    *,
    track: str = "",
    prefix: str = "",
    tag: str = "",
    names: list[str] | None = None,
    preset: str = "",
    include_not_bft: bool = False,
    limit: int = 0,
    fail_fast: bool = False,
    as_json: bool = False,
) -> int:
    """Run a filtered DistLab suite. Exit 0 all pass / 1 any fail / 2 empty selection."""
    reg = _ensure_registry()
    exclude = () if include_not_bft else ("not_bft",)

    async def _run():
        return await reg.run_suite(
            track=track,
            prefix=prefix,
            tag=tag,
            names=names,
            preset=preset,
            exclude_tags=exclude,
            limit=limit,
            fail_fast=fail_fast,
        )

    try:
        report = asyncio.run(_run())
    except KeyError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    if not report.get("selected"):
        print("error: no scenarios selected", file=sys.stderr)
        return 2
    if as_json:
        print(json.dumps(report, indent=2, default=str))
    else:
        status = "PASS" if report["ok"] else "FAIL"
        preset_s = f" preset={report['preset']}" if report.get("preset") else ""
        print(
            f"{status} suite{preset_s} ran={report['ran']} passed={report['passed']} "
            f"failed={len(report['failed'])} "
            f"duration={report['total_duration_s']:.3f}s"
        )
        for name in report.get("failed") or []:
            print(f"  FAIL {name}", file=sys.stderr)
        # compact per-scenario lines
        for row in report.get("results") or []:
            st = "PASS" if row.get("ok") else "FAIL"
            print(
                f"  {st} {row.get('name')} "
                f"duration={float(row.get('duration_s') or 0):.3f}s "
                f"history={row.get('history_len')}"
            )
    return 0 if report["ok"] else 1


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
    pl.set_defaults(func=lambda a: list_scenarios(track=a.track or "", as_json=a.json))

    pc = sub.add_parser("catalog", help="Show scenario catalog with metadata")
    pc.add_argument("--json", action="store_true")
    pc.set_defaults(func=lambda a: catalog(as_json=a.json))

    pr = sub.add_parser("run", help="Run one in-process scenario by name")
    pr.add_argument("name", help="Scenario name (e.g. strong.happy_3)")
    pr.add_argument("--json", action="store_true", help="Emit ScenarioResult JSON")
    pr.set_defaults(func=lambda a: run_scenario(a.name, as_json=a.json))

    ps = sub.add_parser("suite", help="Run a filtered scenario suite")
    ps.add_argument("--track", default="", help="Filter by track id")
    ps.add_argument("--prefix", default="", help="Name prefix filter")
    ps.add_argument("--tag", default="", help="Require tag")
    ps.add_argument(
        "--preset",
        default="",
        help="Named suite preset (smoke, strong-core, audit-core, ci-core)",
    )
    ps.add_argument(
        "--name",
        action="append",
        default=None,
        dest="names",
        help="Explicit scenario name (repeatable)",
    )
    ps.add_argument("--limit", type=int, default=0, help="Max scenarios (0=all)")
    ps.add_argument("--fail-fast", action="store_true")
    ps.add_argument(
        "--include-not-bft",
        action="store_true",
        help="Include not_bft boundary demos",
    )
    ps.add_argument("--json", action="store_true")
    ps.set_defaults(
        func=lambda a: run_suite(
            track=a.track or "",
            prefix=a.prefix or "",
            tag=a.tag or "",
            names=a.names,
            preset=a.preset or "",
            include_not_bft=bool(a.include_not_bft),
            limit=int(a.limit or 0),
            fail_fast=bool(a.fail_fast),
            as_json=bool(a.json),
        )
    )

    pp = sub.add_parser("presets", help="List named suite presets")
    pp.add_argument("--json", action="store_true")
    pp.set_defaults(func=lambda a: list_presets(as_json=bool(a.json)))

    args = p.parse_args(argv)
    return int(args.func(args))
