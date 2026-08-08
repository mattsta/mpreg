"""L1 tx_circuit_breaker_lab — CircuitBreaker open/half-open/closed drill."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.fabric.federation_optimized import CircuitBreaker


async def main() -> None:
    with app_run(
        "tx_circuit_breaker_lab",
        "TX Circuit Breaker — open / half-open / closed",
        level="L1",
    ):
        with scenario(
            "F9 timeout_seconds aligns current_timeout",
            "tx.circuit_breaker",
        ):
            cb = CircuitBreaker(
                failure_threshold=3,
                success_threshold=2,
                timeout_seconds=0.25,
            )
            ensure(cb.current_timeout == 0.25, f"timeout={cb.current_timeout}")
            ensure(cb.state == "closed", f"state={cb.state}")
            ensure(cb.can_execute() is True, "closed should execute")
            ok(f"init closed timeout={cb.current_timeout}")

        with scenario(
            "failures open the breaker",
            "tx.circuit_breaker",
        ):
            cb = CircuitBreaker(
                failure_threshold=3,
                success_threshold=2,
                timeout_seconds=0.3,
            )
            for _ in range(3):
                cb.record_failure()
            ensure(cb.state == "open", f"expected open got {cb.state}")
            ensure(cb.can_execute() is False, "open must block")
            ok("opened after threshold failures")

        with scenario(
            "timeout → half_open → success closes",
            "tx.circuit_breaker",
        ):
            cb = CircuitBreaker(
                failure_threshold=2,
                success_threshold=2,
                timeout_seconds=0.15,
            )
            cb.record_failure()
            cb.record_failure()
            ensure(cb.state == "open", "should be open")
            await asyncio.sleep(0.2)
            ensure(cb.can_execute() is True, "should enter half_open")
            ensure(cb.state == "half_open", f"state={cb.state}")
            cb.record_success()
            ensure(cb.state == "half_open", "need success_threshold")
            cb.record_success()
            ensure(cb.state == "closed", f"expected closed got {cb.state}")
            ensure(cb.can_execute() is True, "closed execute")
            ok("half_open → closed after successes")

        with scenario(
            "half_open failure reopens with backoff",
            "tx.circuit_breaker",
        ):
            cb = CircuitBreaker(
                failure_threshold=1,
                success_threshold=2,
                timeout_seconds=0.1,
                max_timeout_seconds=10.0,
            )
            cb.record_failure()
            ensure(cb.state == "open", "open")
            await asyncio.sleep(0.12)
            ensure(cb.can_execute() is True, "half_open probe")
            before = cb.current_timeout
            cb.record_failure()
            ensure(cb.state == "open", f"reopen got {cb.state}")
            ensure(
                cb.current_timeout >= before, f"backoff {before}→{cb.current_timeout}"
            )
            step(f"backoff timeout {before:.3f} → {cb.current_timeout:.3f}")
            ok("reopen + exponential backoff")

        await asyncio.sleep(0)
        ok("tx_circuit_breaker_lab complete")


if __name__ == "__main__":
    asyncio.run(main())
