"""L3 leader_election_lab — MetricBased + QuorumBased leader election."""

from __future__ import annotations

import asyncio

from mpreg.datastructures.leader_election import (
    LeaderElectionMetrics,
    MetricBasedLeaderElection,
    QuorumBasedLeaderElection,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step


def _m(
    cid: str,
    *,
    cpu: float = 0.2,
    mem: float = 0.2,
    hit: float = 0.9,
    conns: int = 100,
    lat: float = 10.0,
) -> LeaderElectionMetrics:
    return LeaderElectionMetrics(
        cluster_id=cid,
        cpu_usage=cpu,
        memory_usage=mem,
        cache_hit_rate=hit,
        active_connections=conns,
        network_latency_ms=lat,
    )


async def main() -> None:
    with app_run(
        "leader_election_lab",
        "Leader Election Lab — metric + quorum fitness",
        level="L3",
    ):
        with scenario("metrics validation bounds", "cons.leader_election"):
            bad = False
            try:
                LeaderElectionMetrics(cluster_id="", cpu_usage=0.1)
            except ValueError:
                bad = True
            ensure(bad, "empty cluster_id should raise")
            bad_cpu = False
            try:
                LeaderElectionMetrics(cluster_id="x", cpu_usage=1.5)
            except ValueError:
                bad_cpu = True
            ensure(bad_cpu, "cpu>1 should raise")
            m = _m("ok")
            score = m.fitness_score()
            ensure(0.0 <= score <= 100.0, f"score {score}")
            ok(f"fitness_score={score:.1f}")

        with scenario("metric elect prefers healthier peer", "cons.leader_election"):
            elec = MetricBasedLeaderElection(cluster_id="self")
            elec.update_metrics("self", _m("self", cpu=0.8, hit=0.5, conns=10))
            elec.update_metrics("peer-a", _m("peer-a", cpu=0.1, hit=0.99, conns=500))
            leader = await elec.elect_leader("cache-ns")
            ensure(leader == "peer-a", f"leader={leader}")
            ensure(await elec.get_current_leader("cache-ns") == "peer-a", "get")
            ensure(await elec.is_leader("cache-ns") is False, "self should not lead")
            ok(f"metric leader={leader}")

        with scenario("metric elect self when best", "cons.leader_election"):
            elec = MetricBasedLeaderElection(cluster_id="self")
            elec.update_metrics("self", _m("self", cpu=0.05, hit=0.99, conns=800))
            elec.update_metrics("peer-b", _m("peer-b", cpu=0.9, hit=0.2, conns=5))
            leader = await elec.elect_leader("ns2")
            ensure(leader == "self", f"leader={leader}")
            ensure(await elec.is_leader("ns2") is True, "should be leader")
            ok("self wins on fitness")

        with scenario("metric step_down clears namespace", "cons.leader_election"):
            elec = MetricBasedLeaderElection(cluster_id="n1")
            await elec.elect_leader("ns-step")
            ensure(await elec.get_current_leader("ns-step") is not None, "pre")
            await elec.step_down("ns-step")
            ensure(await elec.get_current_leader("ns-step") is None, "post step_down")
            ok("step_down")

        with scenario(
            "quorum elect with enough peers",
            "cons.leader_election",
            "cons.quorum_teach",
        ):
            q = QuorumBasedLeaderElection(cluster_id="q-self", quorum_size=3)
            q.update_metrics("q-self", _m("q-self", cpu=0.5))
            q.update_metrics("q-a", _m("q-a", cpu=0.1, hit=0.95, conns=200))
            q.update_metrics("q-b", _m("q-b", cpu=0.4))
            leader = await q.elect_leader("q-ns")
            ensure(leader in {"q-self", "q-a", "q-b"}, f"leader {leader}")
            # Best fitness should be q-a
            ensure(leader == "q-a", f"expected q-a got {leader}")
            ok(f"quorum leader={leader}")

        with scenario(
            "quorum below size elects self",
            "cons.leader_election",
            "cons.quorum_teach",
        ):
            q = QuorumBasedLeaderElection(cluster_id="solo", quorum_size=3)
            # only self in available set
            leader = await q.elect_leader("tiny")
            ensure(leader == "solo", f"leader={leader}")
            ensure(await q.is_leader("tiny") is True, "solo should lead")
            step("below quorum_size → elect self fail-open for availability")
            ok("sub-quorum self-elect")

        with scenario("independent namespaces", "cons.leader_election"):
            elec = MetricBasedLeaderElection(cluster_id="m1")
            elec.update_metrics("m1", _m("m1", cpu=0.1))
            elec.update_metrics("m2", _m("m2", cpu=0.9))
            a = await elec.elect_leader("ns-a")
            # degrade m1 so m2 wins ns-b
            elec.update_metrics("m1", _m("m1", cpu=0.95, hit=0.1, conns=1))
            elec.update_metrics("m2", _m("m2", cpu=0.05, hit=0.99, conns=400))
            b = await elec.elect_leader("ns-b")
            ensure(a == "m1", f"ns-a {a}")
            ensure(b == "m2", f"ns-b {b}")
            ensure(await elec.get_current_leader("ns-a") == "m1", "ns-a sticky")
            ok(f"ns-a={a} ns-b={b}")


if __name__ == "__main__":
    asyncio.run(main())
