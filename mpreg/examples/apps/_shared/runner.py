"""Orchestrate curriculum example apps (list / run / smoke / suite / demo)."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence

from mpreg.examples.apps._shared.registry import (
    DEMO_BUNDLES,
    AppLevel,
    ExampleApp,
    app_to_dict,
    get_app,
    list_apps,
)
from mpreg.examples.apps._shared.runtime import (
    RunReport,
    banner,
    ok,
    run_app_main,
    step,
)

# Default per-command ceilings (individual apps may finish faster).
_DEFAULT_TIMEOUT = {
    "run": 180.0,
    "smoke": 180.0,
    "suite": 600.0,
    "demo": 600.0,
}

async def run_one(app: ExampleApp, *, timeout_s: float | None = 180.0) -> RunReport:
    banner(app.title, level=app.level.value, app_id=app.id)
    step(app.summary)
    step(f"systems: {', '.join(app.systems)}  kind={app.kind}")
    main = app.load_main()
    report = await run_app_main(app.id, main, timeout_s=timeout_s)
    if report.ok:
        ok(f"{app.id} passed ({report.duration_s:.2f}s)")
    else:
        print(f"  ✗ {app.id} failed ({report.duration_s:.2f}s)")
        if report.error:
            print(report.error)
    return report

async def run_many(
    apps: Sequence[ExampleApp],
    *,
    timeout_s: float | None = 180.0,
    fail_fast: bool = True,
) -> list[RunReport]:
    reports: list[RunReport] = []
    for app in apps:
        report = await run_one(app, timeout_s=timeout_s)
        reports.append(report)
        if not report.ok and fail_fast:
            break
    return reports

def print_list(apps: Sequence[ExampleApp], *, fmt: str = "table") -> None:
    if fmt == "json":
        print(json.dumps([app_to_dict(a) for a in apps], indent=2))
        return
    print(f"{'ID':<26} {'LVL':<4} {'KIND':<12} {'SMOKE':<6} {'SUITE':<6} SYSTEMS")
    print("-" * 88)
    for a in apps:
        print(
            f"{a.id:<26} {a.level.value:<4} "
            f"{a.kind:<12} "
            f"{'yes' if a.smoke else '':<6} "
            f"{'yes' if a.suite else '':<6} "
            f"{','.join(a.systems)}"
        )
        print(f"  {a.title}: {a.summary}")
    print()
    print(f"Total: {len(apps)} apps")
    print("Bundles: " + ", ".join(sorted(DEMO_BUNDLES)))

def print_describe(app: ExampleApp) -> None:
    data = app_to_dict(app)
    print(json.dumps(data, indent=2))
    print()
    print(f"README: {app.path}README.md")
    print(f"Run:    uv run mpreg-example run {app.id}")
    print(f"Also:   uv run mpreg examples run {app.id}")

async def _run_bundle(
    title: str,
    apps: Sequence[ExampleApp],
    *,
    timeout_s: float,
    fail_fast: bool,
) -> int:
    banner(title)
    reports = await run_many(apps, timeout_s=timeout_s, fail_fast=fail_fast)
    passed = sum(1 for r in reports if r.ok)
    failed = [r for r in reports if not r.ok]
    print()
    print(f"Result: {passed}/{len(reports)} passed")
    if failed:
        for r in failed:
            print(f"  FAIL {r.app_id}: {r.error}")
        return 1
    ok("all selected apps passed")
    return 0

def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mpreg-example",
        description=(
            "MPREG unified example apps runner "
            "(curriculum + capability planes + legacy demos)"
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="List curriculum apps")
    p_list.add_argument("--level", choices=[e.value for e in AppLevel], default=None)
    p_list.add_argument("--smoke", action="store_true", help="Only smoke apps")
    p_list.add_argument("--suite", action="store_true", help="Only suite apps")
    p_list.add_argument(
        "--kind",
        choices=("product", "plane", "integration", "legacy"),
        default=None,
        help="Filter by app kind",
    )
    p_list.add_argument(
        "--format", choices=("table", "json"), default="table", dest="fmt"
    )

    p_desc = sub.add_parser("describe", help="Describe one app")
    p_desc.add_argument("app_id")

    p_path = sub.add_parser("path", help="Print on-disk path for an app")
    p_path.add_argument("app_id")

    p_run = sub.add_parser("run", help="Run one app")
    p_run.add_argument("app_id")
    p_run.add_argument("--timeout", type=float, default=_DEFAULT_TIMEOUT["run"])

    p_smoke = sub.add_parser("smoke", help="Run smoke bundle (fast CI path)")
    p_smoke.add_argument("--timeout", type=float, default=_DEFAULT_TIMEOUT["smoke"])
    p_smoke.add_argument("--no-fail-fast", action="store_true")

    p_suite = sub.add_parser("suite", help="Run full shipped curriculum suite")
    p_suite.add_argument("--timeout", type=float, default=_DEFAULT_TIMEOUT["suite"])
    p_suite.add_argument("--no-fail-fast", action="store_true")

    p_demo = sub.add_parser(
        "demo",
        help="Run a named bundle (tier1/tier2/tier3/quick/all_planes/product_vertical)",
    )
    p_demo.add_argument(
        "bundle",
        nargs="?",
        default="quick",
        choices=sorted(DEMO_BUNDLES.keys()),
        help="Bundle name",
    )
    p_demo.add_argument("--timeout", type=float, default=_DEFAULT_TIMEOUT["demo"])
    p_demo.add_argument("--no-fail-fast", action="store_true")

    p_bundles = sub.add_parser("bundles", help="List demo bundles")
    p_bundles.add_argument(
        "--format", choices=("table", "json"), default="table", dest="fmt"
    )

    return parser.parse_args(list(argv) if argv is not None else None)

def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)

    if args.command == "list":
        apps = list_apps(
            level=args.level,
            smoke_only=args.smoke,
            suite_only=args.suite,
            kind=args.kind,
        )
        print_list(apps, fmt=args.fmt)
        return

    if args.command == "describe":
        print_describe(get_app(args.app_id))
        return

    if args.command == "path":
        print(get_app(args.app_id).path)
        return

    if args.command == "bundles":
        if args.fmt == "json":
            print(json.dumps(DEMO_BUNDLES, indent=2))
        else:
            for name, ids in sorted(DEMO_BUNDLES.items()):
                print(f"{name}: {', '.join(ids)}")
        return

    if args.command == "run":
        app = get_app(args.app_id)

        async def _run() -> int:
            report = await run_one(app, timeout_s=args.timeout)
            return 0 if report.ok else 1

        raise SystemExit(asyncio.run(_run()))

    if args.command == "smoke":
        apps = list_apps(smoke_only=True)
        raise SystemExit(
            asyncio.run(
                _run_bundle(
                    "Curriculum smoke",
                    apps,
                    timeout_s=args.timeout,
                    fail_fast=not args.no_fail_fast,
                )
            )
        )

    if args.command == "suite":
        apps = list_apps(suite_only=True)
        raise SystemExit(
            asyncio.run(
                _run_bundle(
                    "Curriculum suite",
                    apps,
                    timeout_s=args.timeout,
                    fail_fast=not args.no_fail_fast,
                )
            )
        )

    if args.command == "demo":
        ids = DEMO_BUNDLES[args.bundle]
        apps = [get_app(i) for i in ids]
        raise SystemExit(
            asyncio.run(
                _run_bundle(
                    f"Demo bundle: {args.bundle}",
                    apps,
                    timeout_s=args.timeout,
                    fail_fast=not args.no_fail_fast,
                )
            )
        )

    raise SystemExit(2)

if __name__ == "__main__":
    main(sys.argv[1:])
