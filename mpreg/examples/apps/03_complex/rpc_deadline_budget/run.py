"""L3 rpc_deadline_budget — M1 vs M2 shared wall deadline policies."""

from __future__ import annotations

import asyncio
import time

from mpreg.client.call_policy import ClientCallPolicy, RpcExecutionMode
from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
)
from mpreg.server import MPREGServer

async def main() -> None:
    with app_run(
        "rpc_deadline_budget",
        "RPC Deadline Budget — M1 retries vs M2 shared wall",
        level="L3",
    ):
        with scenario("M1 factory: optional deadline, no share", "client.policy.m1"):
            m1 = ClientCallPolicy.for_mode(
                RpcExecutionMode.M1_ASYNC, deadline_seconds=2.0
            )
            ensure(
                m1.share_deadline_across_attempts is False,
                "M1 should not share deadline across attempts by default",
            )
            ensure(m1.deadline_seconds == 2.0, f"M1 deadline {m1.deadline_seconds}")
            ok(
                f"M1 mode={m1.mode} share={m1.share_deadline_across_attempts} "
                f"deadline={m1.deadline_seconds}"
            )

        with scenario("M2 factory: shared wall budget", "client.policy.m2", "rpc.deadline"):
            m2 = ClientCallPolicy.for_mode(
                RpcExecutionMode.M2_SOFT_RT, deadline_seconds=1.5
            )
            ensure(m2.share_deadline_across_attempts is True, "M2 must share deadline")
            ensure(m2.deadline_seconds == 1.5, f"M2 deadline {m2.deadline_seconds}")
            ok(f"M2 share={m2.share_deadline_across_attempts} deadline=1.5s")

        with scenario("M3 streaming inherits soft-RT share", "client.policy.m3"):
            m3 = ClientCallPolicy.for_mode(
                RpcExecutionMode.M3_STREAMING, deadline_seconds=3.0
            )
            ensure(m3.share_deadline_across_attempts is True, "M3 should share")
            ok(f"M3 deadline={m3.deadline_seconds}")

        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="Deadline-Node",
                    resources={"work"},
                    log_level="WARNING",
                    gossip_interval=30.0,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]
                calls = {"n": 0}

                def fast(x: int) -> int:
                    calls["n"] += 1
                    return x * 2

                def slow(seconds: float) -> str:
                    calls["n"] += 1
                    time.sleep(float(seconds))
                    return "done"

                server.register_command("fast", fast, ["work"])
                server.register_command("slow", slow, ["work"])
                url = f"ws://127.0.0.1:{ports[0]}"

                with scenario(
                    "M2 happy path under budget",
                    "rpc.deadline",
                    "rpc.call",
                    "client.policy.m2",
                ):
                    policy = ClientCallPolicy.for_mode(
                        RpcExecutionMode.M2_SOFT_RT, deadline_seconds=5.0
                    )
                    async with MPREGClientAPI(url, call_policy=policy) as client:
                        out = await client.call(
                            "fast", 21, locs=frozenset(["work"]), timeout=5.0
                        )
                        ensure(out == 42, f"fast got {out}")
                    ok("M2 fast call under budget")

                with scenario(
                    "M2 exhausted by slow work fails closed",
                    "rpc.deadline",
                    "client.policy.m2",
                ):
                    policy = ClientCallPolicy.for_mode(
                        RpcExecutionMode.M2_SOFT_RT, deadline_seconds=0.35
                    )
                    failed = False
                    t0 = time.monotonic()
                    async with MPREGClientAPI(url, call_policy=policy) as client:
                        try:
                            await client.call(
                                "slow",
                                2.0,
                                locs=frozenset(["work"]),
                                timeout=0.35,
                            )
                        except Exception as exc:
                            failed = True
                            step(f"expected timeout/fail: {type(exc).__name__}: {exc}")
                    elapsed = time.monotonic() - t0
                    ensure(failed, "slow work should exhaust M2 deadline")
                    ensure(elapsed < 2.5, f"should fail closed quickly, took {elapsed:.2f}s")
                    ok(f"fail-closed in {elapsed:.2f}s")

                with scenario(
                    "policy fields inspectable for operators",
                    "client.policy.m1",
                    "rpc.deadline",
                ):
                    # Usability: operators can read policy without calling
                    p = ClientCallPolicy.for_mode(RpcExecutionMode.M1_ASYNC)
                    ensure(hasattr(p, "deadline_seconds"), "missing deadline_seconds")
                    ensure(
                        hasattr(p, "share_deadline_across_attempts"),
                        "missing share flag",
                    )
                    ok(
                        f"M1 defaults deadline={p.deadline_seconds} "
                        f"share={p.share_deadline_across_attempts}"
                    )

                with scenario(
                    "F15: client fail-closed ≠ server handler preemption",
                    "rpc.deadline",
                    "client.policy.m2",
                ):
                    # Honest platform limit: after the client abandons a call,
                    # a blocking server handler (time.sleep) may still run to
                    # completion. Cooperative cancel requires async handlers
                    # that await and check cancellation — not automatic
                    # thread-kill of sync work.
                    step(
                        "friction F15 (documented): M2/client timeout fail-closes "
                        "the caller; sync handlers are not preempted mid-body. "
                        "Prefer async handlers + cooperative checkpoints for "
                        "cancel-sensitive work."
                    )
                    ok("F15 non-claim logged for operators")

            await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
