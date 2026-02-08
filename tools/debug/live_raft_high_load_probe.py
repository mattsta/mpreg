#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from mpreg.datastructures.production_raft import RaftState
from mpreg.datastructures.production_raft_implementation import ProductionRaft
from tests.conftest import AsyncTestContext
from tests.test_helpers import wait_for_condition
from tests.test_live_raft_integration import TestLiveRaftIntegration

type Seconds = float
type NodeId = str


@dataclass(frozen=True, slots=True)
class ProbeConfig:
    cluster_sizes: tuple[int, ...]
    commands_per_batch: int
    command_timeout_seconds: Seconds
    leader_lookup_timeout_seconds: Seconds
    post_load_replication_wait_seconds: Seconds
    consistency_gap_tolerance: int
    consistency_threshold: float
    success_threshold: float
    simulate_xdist: bool


@dataclass(frozen=True, slots=True)
class BatchStats:
    batch_id: int
    total_commands: int
    success_count: int
    none_result_count: int
    timeout_count: int
    no_leader_count: int
    exception_count: int


@dataclass(frozen=True, slots=True)
class ClusterHighLoadResult:
    cluster_size: int
    concurrency_factor: float
    leader_id: NodeId | None
    election_duration_seconds: Seconds
    total_commands: int
    successful_commands: int
    success_rate: float
    throughput_per_second: float
    consistency_rate: float
    consistency_error_count: int
    timeout_failures: int
    no_leader_failures: int
    none_result_failures: int
    exception_failures: int
    mean_command_duration_seconds: Seconds
    p95_command_duration_seconds: Seconds
    max_command_duration_seconds: Seconds
    batch_stats: tuple[BatchStats, ...]
    failed: bool
    failure_reason: str | None


@dataclass(frozen=True, slots=True)
class ProbeReport:
    generated_at_unix: float
    config: ProbeConfig
    results: tuple[ClusterHighLoadResult, ...]


def _parse_cluster_sizes(raw: str) -> tuple[int, ...]:
    values: list[int] = []
    for part in raw.split(","):
        cleaned = part.strip()
        if not cleaned:
            continue
        values.append(max(1, int(cleaned)))
    if not values:
        raise ValueError("At least one cluster size is required")
    return tuple(values)


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * percentile))
    index = max(0, min(index, len(ordered) - 1))
    return float(ordered[index])


def _parse_args() -> tuple[ProbeConfig, Path | None]:
    parser = argparse.ArgumentParser(
        description="Probe live raft high-concurrency behavior with structured evidence"
    )
    parser.add_argument("--cluster-sizes", type=str, default="5,7,11")
    parser.add_argument("--commands-per-batch", type=int, default=10)
    parser.add_argument("--command-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--leader-lookup-timeout-seconds", type=float, default=1.0)
    parser.add_argument("--post-load-replication-wait-seconds", type=float, default=2.0)
    parser.add_argument("--consistency-gap-tolerance", type=int, default=5)
    parser.add_argument("--consistency-threshold", type=float, default=0.8)
    parser.add_argument("--success-threshold", type=float, default=0.7)
    parser.add_argument("--simulate-xdist", action="store_true")
    parser.add_argument("--output-json", type=str, default="")
    args = parser.parse_args()

    config = ProbeConfig(
        cluster_sizes=_parse_cluster_sizes(str(args.cluster_sizes)),
        commands_per_batch=max(1, int(args.commands_per_batch)),
        command_timeout_seconds=max(0.2, float(args.command_timeout_seconds)),
        leader_lookup_timeout_seconds=max(
            0.1, float(args.leader_lookup_timeout_seconds)
        ),
        post_load_replication_wait_seconds=max(
            0.0, float(args.post_load_replication_wait_seconds)
        ),
        consistency_gap_tolerance=max(0, int(args.consistency_gap_tolerance)),
        consistency_threshold=max(0.0, min(float(args.consistency_threshold), 1.0)),
        success_threshold=max(0.0, min(float(args.success_threshold), 1.0)),
        simulate_xdist=bool(args.simulate_xdist),
    )
    output_path = (
        Path(args.output_json).resolve() if str(args.output_json).strip() else None
    )
    return config, output_path


async def _run_cluster_probe(
    *,
    helper: TestLiveRaftIntegration,
    config: ProbeConfig,
    cluster_size: int,
) -> ClusterHighLoadResult:
    command_durations: list[float] = []
    timeout_failures = 0
    no_leader_failures = 0
    none_result_failures = 0
    exception_failures = 0
    election_started_at = time.monotonic()
    failure_reason: str | None = None

    async with AsyncTestContext() as test_context:
        with tempfile.TemporaryDirectory() as temp_dir:
            nodes = await helper.create_live_raft_cluster(
                cluster_size,
                Path(temp_dir),
                test_context,
            )
            try:
                for node in nodes.values():
                    await node.start()

                leader: ProductionRaft | None = None

                def leader_ready() -> bool:
                    nonlocal leader
                    leaders = [
                        node
                        for node in nodes.values()
                        if node.current_state == RaftState.LEADER
                    ]
                    if leaders:
                        leader = leaders[0]
                        return True
                    return False

                concurrency_factor = (
                    4.0 if os.environ.get("PYTEST_XDIST_WORKER") else 1.0
                )
                election_timeout = max(10.0, cluster_size * 2.0) * concurrency_factor
                await wait_for_condition(
                    leader_ready,
                    timeout=election_timeout,
                    interval=0.1,
                    error_message=(
                        f"No leader in {cluster_size}-node cluster after {election_timeout:.1f}s"
                    ),
                )
                election_duration = time.monotonic() - election_started_at

                async def current_leader(timeout: float) -> ProductionRaft | None:
                    deadline = asyncio.get_running_loop().time() + timeout
                    while asyncio.get_running_loop().time() < deadline:
                        leaders = [
                            node
                            for node in nodes.values()
                            if node.current_state == RaftState.LEADER
                        ]
                        if leaders:
                            return leaders[0]
                        await asyncio.sleep(0.05)
                    return None

                async def high_load_batch(
                    batch_id: int,
                ) -> tuple[BatchStats, list[float]]:
                    local_durations: list[float] = []
                    local_success_count = 0
                    local_none_result_count = 0
                    local_timeout_count = 0
                    local_no_leader_count = 0
                    local_exception_count = 0
                    for command_index in range(config.commands_per_batch):
                        started_at = time.monotonic()
                        success = False
                        try:
                            attempt = 0
                            command_result = None
                            while attempt < 2:
                                attempt += 1
                                target = await current_leader(
                                    timeout=config.leader_lookup_timeout_seconds
                                )
                                if target is None:
                                    local_no_leader_count += 1
                                    continue
                                command_result = await asyncio.wait_for(
                                    target.submit_command(
                                        f"load_test_b{batch_id}_c{command_index}"
                                    ),
                                    timeout=config.command_timeout_seconds,
                                )
                                if command_result is not None:
                                    success = True
                                    break
                            if not success and command_result is None:
                                local_none_result_count += 1
                        except TimeoutError:
                            local_timeout_count += 1
                        except Exception:
                            local_exception_count += 1
                        finally:
                            duration = time.monotonic() - started_at
                            local_durations.append(duration)
                            if success:
                                local_success_count += 1
                    return (
                        BatchStats(
                            batch_id=batch_id,
                            total_commands=config.commands_per_batch,
                            success_count=local_success_count,
                            none_result_count=local_none_result_count,
                            timeout_count=local_timeout_count,
                            no_leader_count=local_no_leader_count,
                            exception_count=local_exception_count,
                        ),
                        local_durations,
                    )

                concurrent_batches = cluster_size
                load_started_at = time.monotonic()
                batch_outputs = await asyncio.gather(
                    *[
                        asyncio.create_task(high_load_batch(batch_id))
                        for batch_id in range(concurrent_batches)
                    ]
                )
                load_duration = time.monotonic() - load_started_at

                batch_stats: list[BatchStats] = []
                total_commands = 0
                successful_commands = 0
                for batch_stat, durations in batch_outputs:
                    batch_stats.append(batch_stat)
                    command_durations.extend(durations)
                    total_commands += batch_stat.total_commands
                    successful_commands += batch_stat.success_count
                    timeout_failures += batch_stat.timeout_count
                    no_leader_failures += batch_stat.no_leader_count
                    none_result_failures += batch_stat.none_result_count
                    exception_failures += batch_stat.exception_count

                success_rate = (
                    successful_commands / total_commands if total_commands > 0 else 0.0
                )
                throughput = (
                    successful_commands / load_duration if load_duration > 0 else 0.0
                )

                await asyncio.sleep(config.post_load_replication_wait_seconds)
                refreshed_leader = await current_leader(timeout=2.0)
                if refreshed_leader is not None:
                    leader = refreshed_leader
                if leader is None:
                    failure_reason = "leader_lost_after_load"
                    return ClusterHighLoadResult(
                        cluster_size=cluster_size,
                        concurrency_factor=concurrency_factor,
                        leader_id=None,
                        election_duration_seconds=election_duration,
                        total_commands=total_commands,
                        successful_commands=successful_commands,
                        success_rate=success_rate,
                        throughput_per_second=throughput,
                        consistency_rate=0.0,
                        consistency_error_count=cluster_size - 1,
                        timeout_failures=timeout_failures,
                        no_leader_failures=no_leader_failures,
                        none_result_failures=none_result_failures,
                        exception_failures=exception_failures,
                        mean_command_duration_seconds=(
                            sum(command_durations) / len(command_durations)
                            if command_durations
                            else 0.0
                        ),
                        p95_command_duration_seconds=_percentile(
                            command_durations, 0.95
                        ),
                        max_command_duration_seconds=max(
                            command_durations, default=0.0
                        ),
                        batch_stats=tuple(batch_stats),
                        failed=True,
                        failure_reason=failure_reason,
                    )

                leader_log_length = len(leader.persistent_state.log_entries)
                consistency_errors = 0
                for node in nodes.values():
                    if node is leader:
                        continue
                    node_log_length = len(node.persistent_state.log_entries)
                    if (
                        abs(node_log_length - leader_log_length)
                        > config.consistency_gap_tolerance
                    ):
                        consistency_errors += 1
                consistency_rate = 1.0 - (consistency_errors / max(cluster_size - 1, 1))

                failed = False
                if success_rate < config.success_threshold:
                    failed = True
                    failure_reason = (
                        f"success_rate_below_threshold:{success_rate:.3f}<"
                        f"{config.success_threshold:.3f}"
                    )
                elif consistency_rate < config.consistency_threshold:
                    failed = True
                    failure_reason = (
                        f"consistency_rate_below_threshold:{consistency_rate:.3f}<"
                        f"{config.consistency_threshold:.3f}"
                    )

                return ClusterHighLoadResult(
                    cluster_size=cluster_size,
                    concurrency_factor=concurrency_factor,
                    leader_id=leader.node_id,
                    election_duration_seconds=election_duration,
                    total_commands=total_commands,
                    successful_commands=successful_commands,
                    success_rate=success_rate,
                    throughput_per_second=throughput,
                    consistency_rate=consistency_rate,
                    consistency_error_count=consistency_errors,
                    timeout_failures=timeout_failures,
                    no_leader_failures=no_leader_failures,
                    none_result_failures=none_result_failures,
                    exception_failures=exception_failures,
                    mean_command_duration_seconds=(
                        sum(command_durations) / len(command_durations)
                        if command_durations
                        else 0.0
                    ),
                    p95_command_duration_seconds=_percentile(command_durations, 0.95),
                    max_command_duration_seconds=max(command_durations, default=0.0),
                    batch_stats=tuple(batch_stats),
                    failed=failed,
                    failure_reason=failure_reason,
                )
            finally:
                for node in nodes.values():
                    try:
                        await asyncio.wait_for(node.stop(), timeout=2.0)
                    except TimeoutError:
                        continue
                    except asyncio.CancelledError:
                        continue
                    except Exception:
                        continue


async def _run_probe(config: ProbeConfig) -> ProbeReport:
    if config.simulate_xdist:
        os.environ["PYTEST_XDIST_WORKER"] = "probe"
    else:
        os.environ.pop("PYTEST_XDIST_WORKER", None)

    helper = TestLiveRaftIntegration()
    results: list[ClusterHighLoadResult] = []
    for cluster_size in config.cluster_sizes:
        print(f"\n[cluster={cluster_size}] running high-load probe")
        result = await _run_cluster_probe(
            helper=helper,
            config=config,
            cluster_size=cluster_size,
        )
        results.append(result)
        print(
            f"[cluster={cluster_size}] success_rate={result.success_rate:.3f} "
            f"consistency_rate={result.consistency_rate:.3f} "
            f"throughput={result.throughput_per_second:.2f}/s "
            f"timeouts={result.timeout_failures} "
            f"no_leader={result.no_leader_failures} "
            f"none={result.none_result_failures} "
            f"exceptions={result.exception_failures} "
            f"failed={result.failed} reason={result.failure_reason}"
        )
    return ProbeReport(
        generated_at_unix=time.time(),
        config=config,
        results=tuple(results),
    )


def main() -> int:
    config, output_path = _parse_args()
    report = asyncio.run(_run_probe(config))
    payload = asdict(report)
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        print(f"wrote_report={output_path}")

    failing = [result for result in report.results if result.failed]
    print(
        f"probe_complete clusters={len(report.results)} failing_clusters={len(failing)}"
    )
    if failing:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
