"""ProductionRaft system-under-test adapter for DistLab.

First-class lab surface for Raft election, log replication, and partition
scenarios. Uses in-process ``ProductionRaft`` + memory storage + a controllable
``RaftLabNetwork`` (not Fabric live sockets). Honest scope:

* **Is:** unique-leader election, command commit under majority, partition
  isolation, heal + re-elect, SM agreement on committed keys.
* **Is not:** Fabric wire transport, multi-process live mesh, BFT, dynamic
  membership changes, WAN latency models.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from mpreg.core.errors import OPERATIONAL_EXCEPTIONS
from mpreg.datastructures.production_raft import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    RaftState,
    RequestVoteRequest,
    RequestVoteResponse,
)
from mpreg.datastructures.production_raft_implementation import (
    ProductionRaft,
    RaftConfiguration,
    RaftLeadershipError,
)
from mpreg.datastructures.raft_storage_adapters import RaftStorageFactory
from mpreg.testing.distlab.history import History
from mpreg.testing.distlab.models import OpKind
from mpreg.testing.faults import FaultInjector


class RaftLabStateMachine:
    """Deterministic KV state machine for DistLab Raft scenarios."""

    def __init__(self) -> None:
        self.state: dict[str, Any] = {}
        self.applied_commands: list[tuple[Any, int]] = []
        self.apply_count: int = 0

    async def apply_command(self, command: Any, index: int) -> Any:
        self.apply_count += 1
        self.applied_commands.append((command, index))
        if isinstance(command, str) and "=" in command:
            key, value = command.split("=", 1)
            key = key.strip().removeprefix("set_")
            value = value.strip()
            self.state[key] = int(value) if value.isdigit() else value
            return f"set_{key}_{value}"
        if isinstance(command, dict):
            # Structured put: {"op":"put","key":k,"value":v}
            if command.get("op") == "put":
                k = str(command.get("key", ""))
                self.state[k] = command.get("value")
                return f"put_{k}"
        return f"applied_{index}"

    async def create_snapshot(self) -> bytes:
        return json.dumps(
            {"state": self.state, "apply_count": self.apply_count},
            sort_keys=True,
            default=str,
        ).encode()

    async def restore_from_snapshot(self, snapshot_data: bytes) -> None:
        data = json.loads(snapshot_data.decode())
        self.state = dict(data.get("state") or {})
        self.apply_count = int(data.get("apply_count") or 0)


@dataclass
class RaftLabNetwork:
    """Controllable in-process network for DistLab Raft (partition + drop)."""

    nodes: dict[str, ProductionRaft] = field(default_factory=dict)
    partitions: set[frozenset[str]] = field(default_factory=set)
    edge_cuts: set[frozenset[str]] = field(default_factory=set)
    crashed: set[str] = field(default_factory=set)
    message_delay_s: float = 0.0
    message_loss_rate: float = 0.0
    injector: FaultInjector = field(default_factory=FaultInjector)
    sent_messages: list[tuple[str, str, str]] = field(default_factory=list)
    dropped_messages: int = 0

    def register_node(self, node_id: str, node: ProductionRaft) -> None:
        self.nodes[node_id] = node

    def cut_edge(self, a: str, b: str) -> None:
        self.edge_cuts.add(frozenset({a, b}))

    def heal_edges(self) -> None:
        self.edge_cuts.clear()

    def create_partition(
        self, *groups: set[str] | frozenset[str] | Sequence[str]
    ) -> None:
        self.partitions = {frozenset(g) for g in groups}

    def heal_partition(self) -> None:
        self.partitions.clear()
        self.heal_edges()
        self.injector.heal()
        self.message_loss_rate = 0.0
        self.message_delay_s = 0.0

    def clear_malice(self) -> None:
        self.heal_partition()
        self.crashed.clear()
        self.injector.control_drop_rate = 0.0
        self.injector.data_drop_rate = 0.0

    def can_communicate(self, source: str, target: str) -> bool:
        if source in self.crashed or target in self.crashed:
            return False
        if frozenset({source, target}) in self.edge_cuts:
            return False
        if not self.injector.can_deliver(source, target, plane="control"):
            return False
        if not self.partitions:
            return True
        source_p: frozenset[str] | None = None
        target_p: frozenset[str] | None = None
        for part in self.partitions:
            if source in part:
                source_p = part
            if target in part:
                target_p = part
        return source_p is not None and source_p == target_p


class RaftLabTransport:
    """Transport that respects :class:`RaftLabNetwork` conditions.

    Handlers run on sibling tasks (same pattern as integration MockNetwork) so
    nested await under heartbeat gather cannot RecursionError on cancel.
    """

    def __init__(self, node_id: str, network: RaftLabNetwork) -> None:
        self.node_id = node_id
        self.network = network

    async def _deliver(self, handler_coro: Any, timeout: float = 5.0) -> Any:
        from mpreg.datastructures.raft_task_manager import _retain_task

        task = asyncio.create_task(handler_coro)
        _retain_task(task)  # survive GC if waiter is cancelled mid-flight
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
        except TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
            return None
        except asyncio.CancelledError:
            # Must settle sibling before re-raise — never drop a pending task.
            if not task.done():
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
            raise
        except OPERATIONAL_EXCEPTIONS:
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            return None

    async def _maybe_delay(self) -> None:
        d = self.network.message_delay_s or self.network.injector.delay_for(
            plane="control"
        )
        if d > 0:
            await asyncio.sleep(d)

    def _blocked(self, target: str) -> bool:
        if not self.network.can_communicate(self.node_id, target):
            self.network.dropped_messages += 1
            return True
        if self.network.message_loss_rate > 0:
            # Deterministic-ish drop via injector control plane rate if set
            if self.network.injector.control_drop_rate > 0 and not (
                self.network.injector.can_deliver(self.node_id, target, plane="control")
            ):
                self.network.dropped_messages += 1
                return True
        return False

    async def send_request_vote(
        self, target: str, request: RequestVoteRequest
    ) -> RequestVoteResponse | None:
        if self._blocked(target):
            return None
        self.network.sent_messages.append(("request_vote", self.node_id, target))
        await self._maybe_delay()
        node = self.network.nodes.get(target)
        if node is None:
            return None
        return await self._deliver(node.handle_request_vote(request))

    async def send_append_entries(
        self, target: str, request: AppendEntriesRequest
    ) -> AppendEntriesResponse | None:
        if self._blocked(target):
            return None
        self.network.sent_messages.append(("append_entries", self.node_id, target))
        await self._maybe_delay()
        node = self.network.nodes.get(target)
        if node is None:
            return None
        return await self._deliver(node.handle_append_entries(request))

    async def send_install_snapshot(
        self, target: str, request: InstallSnapshotRequest
    ) -> InstallSnapshotResponse | None:
        if self._blocked(target):
            return None
        self.network.sent_messages.append(("install_snapshot", self.node_id, target))
        await self._maybe_delay()
        node = self.network.nodes.get(target)
        if node is None:
            return None
        return await self._deliver(node.handle_install_snapshot(request))


@dataclass
class RaftStateSnapshot:
    """Checker-facing snapshot of a Raft cluster."""

    nodes: dict[str, ProductionRaft]
    state_machines: dict[str, RaftLabStateMachine]
    peer_ids: list[str]

    def leaders(self) -> list[str]:
        return [
            nid
            for nid, n in self.nodes.items()
            if n.current_state == RaftState.LEADER and not n._stopped
        ]

    def leader_count(self) -> int:
        return len(self.leaders())

    def sm_value(self, key: str) -> dict[str, Any]:
        """Per-node SM value for ``key`` (missing → None)."""
        out: dict[str, Any] = {}
        for nid, sm in self.state_machines.items():
            out[nid] = sm.state.get(key)
        return out

    def committed_values(self, key: str) -> set[Any]:
        """Distinct non-None SM values for key across live nodes."""
        vals: set[Any] = set()
        for sm in self.state_machines.values():
            if key in sm.state:
                vals.add(sm.state[key])
        return vals

    def statuses(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for n in self.nodes.values():
            st = n.get_status()
            out.append(
                {
                    "node_id": st.node_id,
                    "state": str(st.state),
                    "term": st.term,
                    "elections_started": st.elections_started,
                    "coordinator_active": st.coordinator_active,
                }
            )
        return out


@dataclass
class RaftSUT:
    """In-process N-node ProductionRaft system under DistLab control."""

    n: int
    network: RaftLabNetwork
    nodes: dict[str, ProductionRaft]
    state_machines: dict[str, RaftLabStateMachine]
    peer_ids: list[str]
    config: RaftConfiguration
    _started: bool = False

    @classmethod
    def create(
        cls,
        n: int = 3,
        *,
        election_timeout_min: float = 0.25,
        election_timeout_max: float = 0.45,
        heartbeat_interval: float = 0.06,
        rpc_timeout: float = 0.15,
        command_apply_timeout_seconds: float = 5.0,
        pre_vote_enabled: bool = True,
        seed: int = 0,
        node_prefix: str = "n",
    ) -> RaftSUT:
        if n < 1:
            raise ValueError("n must be >= 1")
        peer_ids = [f"{node_prefix}{i}" for i in range(n)]
        members = set(peer_ids)
        inj = FaultInjector(seed=seed)
        network = RaftLabNetwork(injector=inj)
        config = RaftConfiguration(
            election_timeout_min=election_timeout_min,
            election_timeout_max=election_timeout_max,
            heartbeat_interval=heartbeat_interval,
            rpc_timeout=rpc_timeout,
            command_apply_timeout_seconds=command_apply_timeout_seconds,
            pre_vote_enabled=pre_vote_enabled,
        )
        nodes: dict[str, ProductionRaft] = {}
        sms: dict[str, RaftLabStateMachine] = {}
        for nid in peer_ids:
            storage = RaftStorageFactory.create_memory_storage(f"distlab_raft_{nid}")
            transport = RaftLabTransport(nid, network)
            sm = RaftLabStateMachine()
            node = ProductionRaft(
                node_id=nid,
                cluster_members=members,
                storage=storage,
                transport=transport,
                state_machine=sm,
                config=config,
            )
            nodes[nid] = node
            sms[nid] = sm
            network.register_node(nid, node)
        return cls(
            n=n,
            network=network,
            nodes=nodes,
            state_machines=sms,
            peer_ids=peer_ids,
            config=config,
        )

    def snapshot_state(self) -> RaftStateSnapshot:
        return RaftStateSnapshot(
            nodes=self.nodes,
            state_machines=self.state_machines,
            peer_ids=list(self.peer_ids),
        )

    async def start(self) -> None:
        if self._started:
            return
        for node in self.nodes.values():
            await node.start()
        self._started = True

    async def stop(self) -> None:
        """Stop all nodes sequentially, then drain any retained sibling tasks.

        Concurrent stop + loop teardown can RecursionError inside Task.cancel
        when heartbeat gathers nest deeply. Sequential bounded stops keep the
        cancel graph shallow; drain_retained_tasks settles RaftLabTransport
        deliver tasks that outlived their waiters.
        """
        from mpreg.datastructures.raft_task_manager import drain_retained_tasks

        for node in list(self.nodes.values()):
            with contextlib.suppress(
                TimeoutError, asyncio.CancelledError, Exception
            ):
                await asyncio.wait_for(node.stop(), timeout=3.0)
        with contextlib.suppress(TimeoutError, asyncio.CancelledError, Exception):
            await drain_retained_tasks(timeout=2.0)
        self._started = False

    async def wait_for_leader(
        self,
        *,
        among: Sequence[str] | None = None,
        timeout_seconds: float | None = None,
        poll_interval: float = 0.05,
        require_unique: bool = True,
    ) -> ProductionRaft:
        """Wait for a unique leader (optionally restricted to a subset)."""
        if among is None:
            pool = self.nodes
        else:
            pool = {nid: self.nodes[nid] for nid in among if nid in self.nodes}
        try:
            return await ProductionRaft.wait_for_leader(
                pool,
                timeout_seconds=timeout_seconds,
                poll_interval=poll_interval,
                require_unique=require_unique,
            )
        except RaftLeadershipError as exc:
            detail = ", ".join(
                f"{s.node_id}:{s.state}/t{s.term}/e{s.elections_started}"
                f"/skip_contact={s.elections_skipped_recent_contact}"
                for s in exc.statuses
            )
            raise AssertionError(f"{exc}; statuses=[{detail}]") from exc

    def current_leader(self) -> ProductionRaft | None:
        leaders = [
            n
            for n in self.nodes.values()
            if n.current_state == RaftState.LEADER and not n._stopped
        ]
        if len(leaders) == 1:
            return leaders[0]
        return None

    async def wait_sm_key(
        self,
        key: str,
        expected: Any,
        *,
        among: Sequence[str] | None = None,
        timeout_seconds: float = 3.0,
        poll_interval: float = 0.05,
    ) -> None:
        """Wait until all selected nodes have ``key == expected`` in SM."""
        ids = list(among) if among is not None else list(self.peer_ids)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            ok = all(self.state_machines[nid].state.get(key) == expected for nid in ids)
            if ok:
                return
            if asyncio.get_running_loop().time() >= deadline:
                views = {nid: self.state_machines[nid].state.get(key) for nid in ids}
                raise AssertionError(
                    f"SM key {key!r} did not reach {expected!r} within "
                    f"{timeout_seconds}s; views={views}"
                )
            await asyncio.sleep(poll_interval)

    async def put(
        self,
        history: History,
        *,
        process: str,
        key: str,
        value: Any,
        leader: ProductionRaft | None = None,
        op_id: str | None = None,
        wait_replicate: bool = True,
        among: Sequence[str] | None = None,
    ) -> Any:
        """Submit a put via the current (or given) leader; record history."""
        history.invoke(process, OpKind.PUT, key=key, value=value, op_id=op_id)
        try:
            ldr = leader or self.current_leader()
            if ldr is None:
                ldr = await self.wait_for_leader(among=among)
            command = f"{key}={value}"
            result = await ldr.submit_command(command, client_id=process)
        except Exception as exc:
            history.info(
                process,
                OpKind.PUT,
                key=key,
                value=value,
                op_id=op_id,
                error_message=type(exc).__name__,
            )
            raise
        if result is None:
            history.fail(
                process,
                OpKind.PUT,
                key=key,
                value=value,
                op_id=op_id,
                error_message="not_leader_or_replication_failed",
            )
            return None
        history.ok(
            process,
            OpKind.PUT,
            key=key,
            value=value,
            op_id=op_id or str(result),
            meta={"ack": result, "leader": ldr.node_id},
        )
        if wait_replicate:
            targets = list(among) if among is not None else list(self.peer_ids)
            # Only wait on nodes that can still see the leader's majority path
            reachable = [
                nid
                for nid in targets
                if nid == ldr.node_id or self.network.can_communicate(ldr.node_id, nid)
            ]
            if reachable:
                expected = (
                    int(value) if isinstance(value, str) and value.isdigit() else value
                )
                if isinstance(value, int) or (
                    isinstance(value, str) and value.isdigit()
                ):
                    expected = int(value)
                # History already OK (committed); lag is left for checkers.
                with contextlib.suppress(AssertionError):
                    await self.wait_sm_key(
                        key,
                        expected,
                        among=reachable,
                        timeout_seconds=self.config.command_apply_timeout_seconds,
                    )
        return result

    def partition_groups(self, groups: Sequence[Sequence[str] | set[str]]) -> None:
        self.network.create_partition(*[set(g) for g in groups])

    def heal(self) -> None:
        self.network.heal_partition()
        self.network.clear_malice()

    def nemesis_hooks(self) -> dict[str, Any]:
        """Callbacks for FaultInjectorNemesisTarget."""
        net = self.network
        nodes = list(self.peer_ids)

        def on_partition(groups: Sequence[set[str]]) -> None:
            net.heal_edges()
            labeled = [set(g) for g in groups]
            net.create_partition(*labeled)

        def on_heal() -> None:
            net.heal_partition()

        def on_crash(node_id: str) -> None:
            net.crashed.add(node_id)

        def on_recover(node_id: str) -> None:
            net.crashed.discard(node_id)

        def on_drop(rate: float) -> None:
            net.message_loss_rate = rate
            net.injector.control_drop_rate = rate

        def on_delay(seconds: float) -> None:
            net.message_delay_s = seconds

        return {
            "nodes": nodes,
            "on_partition": on_partition,
            "on_heal": on_heal,
            "on_crash": on_crash,
            "on_recover": on_recover,
            "on_drop_rate": on_drop,
            "on_delay": on_delay,
            "injector": net.injector,
        }
