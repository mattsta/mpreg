#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from mpreg.datastructures.production_raft import RaftState
from mpreg.datastructures.production_raft_implementation import ProductionRaft
from tests.conftest import AsyncTestContext
from tests.test_live_raft_integration import TestLiveRaftIntegration

type NodeId = str
type Seconds = float


@dataclass(frozen=True, slots=True)
class ProbeConfig:
    cluster_size: int
    startup_phase_timeout_seconds: Seconds
    election_timeout_seconds: Seconds
    command_count: int
    command_timeout_seconds: Seconds
    replication_wait_seconds: Seconds
    sample_interval_seconds: Seconds


@dataclass(frozen=True, slots=True)
class NodeStateSnapshot:
    node_id: NodeId
    state: str
    term: int
    log_length: int
    commit_index: int
    last_applied: int
    votes_received: int
    current_leader: NodeId | None


@dataclass(frozen=True, slots=True)
class ClusterSnapshot:
    elapsed_seconds: Seconds
    leader_count: int
    candidate_count: int
    follower_count: int
    leader_id: NodeId | None
    states: tuple[NodeStateSnapshot, ...]


@dataclass(frozen=True, slots=True)
class CommandResult:
    command_index: int
    success: bool
    duration_seconds: Seconds
    error_message: str | None


@dataclass(frozen=True, slots=True)
class ConsistencyIssue:
    node_id: NodeId
    leader_log_length: int
    node_log_length: int
    state: str


@dataclass(frozen=True, slots=True)
class ProbeReport:
    generated_at_unix: float
    config: ProbeConfig
    startup_succeeded: bool
    election_succeeded: bool
    startup_duration_seconds: Seconds
    election_duration_seconds: Seconds
    leader_id: NodeId | None
    command_results: tuple[CommandResult, ...]
    replication_consistent: bool
    consistency_issues: tuple[ConsistencyIssue, ...]
    final_snapshot: ClusterSnapshot
    snapshots: tuple[ClusterSnapshot, ...]


def _capture_snapshot(
    nodes: dict[NodeId, ProductionRaft],
    started_at: float,
) -> ClusterSnapshot:
    state_rows: list[NodeStateSnapshot] = []
    leader_id: NodeId | None = None
    leader_count = 0
    candidate_count = 0
    follower_count = 0

    for node_id in sorted(nodes):
        node = nodes[node_id]
        current_state = node.current_state
        state_value = current_state.value
        if current_state == RaftState.LEADER:
            leader_count += 1
            leader_id = node_id
        elif current_state == RaftState.CANDIDATE:
            candidate_count += 1
        elif current_state == RaftState.FOLLOWER:
            follower_count += 1

        state_rows.append(
            NodeStateSnapshot(
                node_id=node_id,
                state=state_value,
                term=node.persistent_state.current_term,
                log_length=len(node.persistent_state.log_entries),
                commit_index=node.volatile_state.commit_index,
                last_applied=node.volatile_state.last_applied,
                votes_received=len(node.votes_received),
                current_leader=node.current_leader,
            )
        )

    return ClusterSnapshot(
        elapsed_seconds=time.monotonic() - started_at,
        leader_count=leader_count,
        candidate_count=candidate_count,
        follower_count=follower_count,
        leader_id=leader_id,
        states=tuple(state_rows),
    )


def _find_leader(nodes: dict[NodeId, ProductionRaft]) -> ProductionRaft | None:
    leaders = [
        node for node in nodes.values() if node.current_state == RaftState.LEADER
    ]
    if not leaders:
        return None
    return leaders[0]


async def _stop_nodes(nodes: dict[NodeId, ProductionRaft]) -> None:
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
    snapshots: list[ClusterSnapshot] = []
    command_results: list[CommandResult] = []
    consistency_issues: list[ConsistencyIssue] = []
    startup_succeeded = False
    election_succeeded = False
    leader_id: NodeId | None = None
    startup_duration_seconds = 0.0
    election_duration_seconds = 0.0

    helper = TestLiveRaftIntegration()

    async with AsyncTestContext() as test_context:
        with tempfile.TemporaryDirectory() as temp_dir:
            nodes = await helper.create_live_raft_cluster(
                config.cluster_size, Path(temp_dir), test_context
            )
            started_at = time.monotonic()
            try:
                start_tasks = [
                    asyncio.create_task(node.start()) for node in nodes.values()
                ]
                startup_started_at = time.monotonic()
                await asyncio.wait_for(
                    asyncio.gather(*start_tasks),
                    timeout=config.startup_phase_timeout_seconds,
                )
                startup_duration_seconds = time.monotonic() - startup_started_at
                startup_succeeded = True
                snapshots.append(_capture_snapshot(nodes, started_at))

                election_started_at = time.monotonic()
                while True:
                    snapshot = _capture_snapshot(nodes, started_at)
                    snapshots.append(snapshot)
                    leader = _find_leader(nodes)
                    if leader is not None:
                        election_succeeded = True
                        election_duration_seconds = (
                            time.monotonic() - election_started_at
                        )
                        leader_id = leader.node_id
                        break
                    if (
                        time.monotonic() - election_started_at
                        > config.election_timeout_seconds
                    ):
                        break
                    await asyncio.sleep(config.sample_interval_seconds)

                leader = _find_leader(nodes)
                if leader is not None:
                    for command_index in range(config.command_count):
                        command_start = time.monotonic()
                        command_error: str | None = None
                        command_success = False
                        try:
                            result = await asyncio.wait_for(
                                leader.submit_command(f"probe_cmd_{command_index}"),
                                timeout=config.command_timeout_seconds,
                            )
                            command_success = result is not None
                            if not command_success:
                                command_error = "submit_command returned None"
                        except TimeoutError:
                            command_error = "submit_command timeout"
                        except Exception as exc:
                            command_error = f"{type(exc).__name__}: {exc}"

                        command_results.append(
                            CommandResult(
                                command_index=command_index,
                                success=command_success,
                                duration_seconds=time.monotonic() - command_start,
                                error_message=command_error,
                            )
                        )
                        snapshots.append(_capture_snapshot(nodes, started_at))
                        if not command_success:
                            break

                await asyncio.sleep(config.replication_wait_seconds)
                final_snapshot = _capture_snapshot(nodes, started_at)
                snapshots.append(final_snapshot)

                if leader_id is not None:
                    leader = nodes[leader_id]
                    leader_log_length = len(leader.persistent_state.log_entries)
                    for node_id, node in nodes.items():
                        if node_id == leader_id:
                            continue
                        node_log_length = len(node.persistent_state.log_entries)
                        if node_log_length != leader_log_length:
                            consistency_issues.append(
                                ConsistencyIssue(
                                    node_id=node_id,
                                    leader_log_length=leader_log_length,
                                    node_log_length=node_log_length,
                                    state=node.current_state.value,
                                )
                            )
                else:
                    final_snapshot = _capture_snapshot(nodes, started_at)
            finally:
                await _stop_nodes(nodes)

    replication_consistent = len(consistency_issues) == 0
    final_snapshot = snapshots[-1]
    return ProbeReport(
        generated_at_unix=time.time(),
        config=config,
        startup_succeeded=startup_succeeded,
        election_succeeded=election_succeeded,
        startup_duration_seconds=startup_duration_seconds,
        election_duration_seconds=election_duration_seconds,
        leader_id=leader_id,
        command_results=tuple(command_results),
        replication_consistent=replication_consistent,
        consistency_issues=tuple(consistency_issues),
        final_snapshot=final_snapshot,
        snapshots=tuple(snapshots),
    )


def _parse_args() -> tuple[ProbeConfig, Path | None]:
    parser = argparse.ArgumentParser(
        description="Probe the live raft cluster-size path with structured phase evidence"
    )
    parser.add_argument("--cluster-size", type=int, default=13)
    parser.add_argument("--startup-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--election-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--command-count", type=int, default=20)
    parser.add_argument("--command-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--replication-wait-seconds", type=float, default=3.0)
    parser.add_argument("--sample-interval-seconds", type=float, default=0.2)
    parser.add_argument("--output-json", type=str, default="")
    args = parser.parse_args()

    config = ProbeConfig(
        cluster_size=max(1, int(args.cluster_size)),
        startup_phase_timeout_seconds=max(1.0, float(args.startup_timeout_seconds)),
        election_timeout_seconds=max(1.0, float(args.election_timeout_seconds)),
        command_count=max(1, int(args.command_count)),
        command_timeout_seconds=max(0.5, float(args.command_timeout_seconds)),
        replication_wait_seconds=max(0.0, float(args.replication_wait_seconds)),
        sample_interval_seconds=max(0.05, float(args.sample_interval_seconds)),
    )
    output_json = (
        Path(args.output_json).resolve() if str(args.output_json).strip() else None
    )
    return config, output_json


def main() -> int:
    config, output_json = _parse_args()
    report = asyncio.run(_run_probe(config))
    payload = asdict(report)
    print(
        "raft_probe_result "
        f"startup={report.startup_succeeded} "
        f"election={report.election_succeeded} "
        f"leader={report.leader_id} "
        f"commands={sum(1 for item in report.command_results if item.success)}/"
        f"{len(report.command_results)} "
        f"replication_consistent={report.replication_consistent}"
    )
    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        print(f"wrote_report={output_json}")

    if not report.startup_succeeded:
        return 1
    if not report.election_succeeded:
        return 1
    if not report.command_results:
        return 1
    if any(not result.success for result in report.command_results):
        return 1
    if not report.replication_consistent:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
