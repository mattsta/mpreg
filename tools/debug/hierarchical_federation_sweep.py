from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean

from hierarchical_federation_probe import ProbeConfig, _run_probe

type RunIndex = int
type Seconds = float


@dataclass(frozen=True, slots=True)
class SweepRunResult:
    run_index: RunIndex
    report_path: str
    exit_code: int
    propagation_peak: float
    post_fault_peak: float
    initial_unreachable_from_global: int
    post_fault_unreachable_from_global: int
    surviving_component_sizes: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class ScenarioSummary:
    initial_convergence_seconds: Seconds
    propagation_threshold: float
    post_fault_threshold: float
    run_count: int
    propagation_pass_count: int
    post_fault_pass_count: int
    propagation_min: float
    propagation_avg: float
    propagation_max: float
    post_fault_min: float
    post_fault_avg: float
    post_fault_max: float
    max_initial_unreachable: int
    max_post_fault_unreachable: int
    runs: tuple[SweepRunResult, ...]


@dataclass(frozen=True, slots=True)
class SweepReport:
    generated_at_unix: float
    generated_at_utc: str
    output_dir: str
    runs_per_convergence: int
    propagation_duration_seconds: Seconds
    post_fault_duration_seconds: Seconds
    sample_interval_seconds: Seconds
    failure_mode: str
    scenarios: tuple[ScenarioSummary, ...]


def _configure_runtime_logging() -> None:
    # Keep probe output focused on metrics; warnings are captured in JSON artifacts.
    logging.getLogger("websockets.server").setLevel(logging.ERROR)
    logging.getLogger("websockets.client").setLevel(logging.ERROR)
    logging.getLogger("asyncio").setLevel(logging.ERROR)
    try:
        from loguru import logger as loguru_logger
    except Exception:
        return
    loguru_logger.remove()
    loguru_logger.add(sys.stderr, level="ERROR")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Repeat hierarchical federation probe runs across convergence settings "
            "and summarize propagation stability."
        )
    )
    parser.add_argument(
        "--convergence-seconds",
        action="append",
        type=float,
        required=True,
        help="Initial convergence sleep before propagation sampling (repeatable)",
    )
    parser.add_argument(
        "--runs-per-convergence",
        type=int,
        default=4,
        help="Number of probe runs for each convergence value",
    )
    parser.add_argument(
        "--sample-interval-seconds",
        type=float,
        default=0.5,
    )
    parser.add_argument(
        "--propagation-duration-seconds",
        type=float,
        default=8.0,
    )
    parser.add_argument(
        "--post-fault-duration-seconds",
        type=float,
        default=8.0,
    )
    parser.add_argument(
        "--failure-mode",
        choices=("disconnect", "shutdown"),
        default="disconnect",
    )
    parser.add_argument(
        "--propagation-threshold",
        type=float,
        default=0.45,
    )
    parser.add_argument(
        "--post-fault-threshold",
        type=float,
        default=0.6,
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/evidence/hierarchical_probe_sweep",
    )
    return parser.parse_args()


def _timestamp_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _load_probe_metrics(report_path: Path) -> SweepRunResult:
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    return SweepRunResult(
        run_index=0,
        report_path=str(report_path),
        exit_code=0,
        propagation_peak=float(payload["propagation_peak"]),
        post_fault_peak=float(payload["post_fault_peak"]),
        initial_unreachable_from_global=int(payload["initial_unreachable_from_global"]),
        post_fault_unreachable_from_global=int(
            payload["post_fault_unreachable_from_global"]
        ),
        surviving_component_sizes=tuple(
            int(value) for value in payload["surviving_component_sizes"]
        ),
    )


async def _run_scenario(
    *,
    scenario_dir: Path,
    convergence_seconds: Seconds,
    runs_per_convergence: int,
    sample_interval_seconds: Seconds,
    propagation_duration_seconds: Seconds,
    post_fault_duration_seconds: Seconds,
    failure_mode: str,
    propagation_threshold: float,
    post_fault_threshold: float,
) -> ScenarioSummary:
    run_results: list[SweepRunResult] = []
    probe_config = ProbeConfig(
        initial_convergence_seconds=convergence_seconds,
        sample_interval_seconds=sample_interval_seconds,
        propagation_duration_seconds=propagation_duration_seconds,
        post_fault_duration_seconds=post_fault_duration_seconds,
        failure_mode=failure_mode,
    )

    for run_index in range(1, runs_per_convergence + 1):
        report_path = scenario_dir / f"run_{run_index:03d}.json"
        started_at = time.time()
        exit_code = await _run_probe(config=probe_config, output_json=report_path)
        elapsed_seconds = time.time() - started_at

        if exit_code == 0 and report_path.exists():
            metrics = _load_probe_metrics(report_path)
            run_results.append(
                SweepRunResult(
                    run_index=run_index,
                    report_path=str(report_path),
                    exit_code=exit_code,
                    propagation_peak=metrics.propagation_peak,
                    post_fault_peak=metrics.post_fault_peak,
                    initial_unreachable_from_global=metrics.initial_unreachable_from_global,
                    post_fault_unreachable_from_global=metrics.post_fault_unreachable_from_global,
                    surviving_component_sizes=metrics.surviving_component_sizes,
                )
            )
            print(
                "scenario_result "
                f"convergence={convergence_seconds:.2f}s "
                f"run={run_index}/{runs_per_convergence} "
                f"elapsed={elapsed_seconds:.2f}s "
                f"prop_peak={metrics.propagation_peak:.4f} "
                f"post_fault_peak={metrics.post_fault_peak:.4f} "
                f"initial_unreachable={metrics.initial_unreachable_from_global} "
                f"post_fault_unreachable={metrics.post_fault_unreachable_from_global}"
            )
            continue

        run_results.append(
            SweepRunResult(
                run_index=run_index,
                report_path=str(report_path),
                exit_code=exit_code,
                propagation_peak=0.0,
                post_fault_peak=0.0,
                initial_unreachable_from_global=-1,
                post_fault_unreachable_from_global=-1,
                surviving_component_sizes=tuple(),
            )
        )
        print(
            "scenario_result "
            f"convergence={convergence_seconds:.2f}s "
            f"run={run_index}/{runs_per_convergence} "
            f"elapsed={elapsed_seconds:.2f}s "
            f"exit_code={exit_code} report_missing={not report_path.exists()}"
        )

    successful_runs = [run for run in run_results if run.exit_code == 0]
    if not successful_runs:
        return ScenarioSummary(
            initial_convergence_seconds=convergence_seconds,
            propagation_threshold=propagation_threshold,
            post_fault_threshold=post_fault_threshold,
            run_count=0,
            propagation_pass_count=0,
            post_fault_pass_count=0,
            propagation_min=0.0,
            propagation_avg=0.0,
            propagation_max=0.0,
            post_fault_min=0.0,
            post_fault_avg=0.0,
            post_fault_max=0.0,
            max_initial_unreachable=-1,
            max_post_fault_unreachable=-1,
            runs=tuple(run_results),
        )

    propagation_peaks = [run.propagation_peak for run in successful_runs]
    post_fault_peaks = [run.post_fault_peak for run in successful_runs]
    return ScenarioSummary(
        initial_convergence_seconds=convergence_seconds,
        propagation_threshold=propagation_threshold,
        post_fault_threshold=post_fault_threshold,
        run_count=len(successful_runs),
        propagation_pass_count=sum(
            1 for value in propagation_peaks if value >= propagation_threshold
        ),
        post_fault_pass_count=sum(
            1 for value in post_fault_peaks if value >= post_fault_threshold
        ),
        propagation_min=min(propagation_peaks),
        propagation_avg=mean(propagation_peaks),
        propagation_max=max(propagation_peaks),
        post_fault_min=min(post_fault_peaks),
        post_fault_avg=mean(post_fault_peaks),
        post_fault_max=max(post_fault_peaks),
        max_initial_unreachable=max(
            run.initial_unreachable_from_global for run in successful_runs
        ),
        max_post_fault_unreachable=max(
            run.post_fault_unreachable_from_global for run in successful_runs
        ),
        runs=tuple(run_results),
    )


async def _run() -> int:
    args = _parse_args()
    _configure_runtime_logging()
    base_output_dir = Path(args.output_dir).resolve()
    session_dir = base_output_dir / _timestamp_id()
    session_dir.mkdir(parents=True, exist_ok=True)

    scenarios: list[ScenarioSummary] = []
    convergence_values = tuple(
        sorted({float(value) for value in args.convergence_seconds})
    )
    for convergence_seconds in convergence_values:
        scenario_dir = session_dir / f"convergence_{convergence_seconds:.2f}s"
        scenario_dir.mkdir(parents=True, exist_ok=True)
        print(
            "scenario_start "
            f"convergence={convergence_seconds:.2f}s "
            f"runs={args.runs_per_convergence}"
        )
        scenario_summary = await _run_scenario(
            scenario_dir=scenario_dir,
            convergence_seconds=convergence_seconds,
            runs_per_convergence=max(1, int(args.runs_per_convergence)),
            sample_interval_seconds=max(0.1, float(args.sample_interval_seconds)),
            propagation_duration_seconds=max(
                0.1, float(args.propagation_duration_seconds)
            ),
            post_fault_duration_seconds=max(
                0.1, float(args.post_fault_duration_seconds)
            ),
            failure_mode=str(args.failure_mode),
            propagation_threshold=float(args.propagation_threshold),
            post_fault_threshold=float(args.post_fault_threshold),
        )
        scenarios.append(scenario_summary)
        print(
            "scenario_summary "
            f"convergence={convergence_seconds:.2f}s "
            f"prop_pass={scenario_summary.propagation_pass_count}/{scenario_summary.run_count} "
            f"prop_min={scenario_summary.propagation_min:.4f} "
            f"prop_avg={scenario_summary.propagation_avg:.4f} "
            f"prop_max={scenario_summary.propagation_max:.4f} "
            f"post_fault_pass={scenario_summary.post_fault_pass_count}/{scenario_summary.run_count} "
            f"post_fault_min={scenario_summary.post_fault_min:.4f}"
        )

    report = SweepReport(
        generated_at_unix=time.time(),
        generated_at_utc=datetime.now(UTC).isoformat(),
        output_dir=str(session_dir),
        runs_per_convergence=max(1, int(args.runs_per_convergence)),
        propagation_duration_seconds=max(0.1, float(args.propagation_duration_seconds)),
        post_fault_duration_seconds=max(0.1, float(args.post_fault_duration_seconds)),
        sample_interval_seconds=max(0.1, float(args.sample_interval_seconds)),
        failure_mode=str(args.failure_mode),
        scenarios=tuple(scenarios),
    )
    report_path = session_dir / "sweep_report.json"
    report_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")
    print(f"wrote_report={report_path}")
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
