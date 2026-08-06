"""L1 namespace_policy_gate — validate / apply / status / export / audit (ns.*)."""

from __future__ import annotations

import asyncio

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
    with (
        app_run(
            "namespace_policy_gate",
            "Namespace Policy Gate — validate/apply/status/audit",
            level="L1",
        ),
        port_range_context(1, "servers") as ports,
    ):
        settings = [
            MPREGSettings(
                port=ports[0],
                name="NS-Policy-Admin",
                cluster_id="market",
                log_level="WARNING",
                gossip_interval=30.0,
            )
        ]

        async def _run(servers: list[MPREGServer]) -> None:
            url = f"ws://127.0.0.1:{ports[0]}"
            step(f"admin server {url}")

            rules_v1 = [
                {
                    "namespace": "svc.secret",
                    "visibility": ["secure-cluster"],
                    "policy_version": "v1",
                }
            ]
            rules_bad = [
                {
                    # Missing namespace should fail validation when invalid shape
                    "visibility": ["x"],
                    "policy_version": "bad",
                }
            ]

            async with MPREGClientAPI(url) as client:
                with scenario(
                    "validate well-formed rules",
                    "ns.validate",
                    "client.api",
                ):
                    valid = await client.namespace_policy_validate(
                        rules=rules_v1, actor="curriculum"
                    )
                    ensure(valid.valid is True, f"expected valid: {valid}")
                    ok("validate accepted svc.secret rule")

                with scenario("apply policy and enable engine", "ns.apply"):
                    applied = await client.namespace_policy_apply(
                        rules=rules_v1,
                        enabled=True,
                        actor="curriculum",
                    )
                    ensure(
                        applied.applied is True,
                        f"apply failed: {applied}",
                    )
                    ok("policy applied enabled=True")

                with scenario(
                    "namespace_status reflects deny-by-default viewer",
                    "ns.status",
                ):
                    status = await client.namespace_status(namespace="svc.secret")
                    # Local admin client is not secure-cluster → not allowed
                    ensure(
                        status.allowed is False,
                        f"expected deny for local viewer, got {status}",
                    )
                    ensure(
                        bool(status.reason),
                        "status reason should be non-empty",
                    )
                    ok(f"status allowed=False reason={status.reason}")

                with scenario("export current rules", "ns.export"):
                    exported = await client.namespace_policy_export()
                    ensure(
                        exported is not None,
                        "export returned None",
                    )
                    rules = getattr(exported, "rules", ()) or ()
                    ensure(len(rules) >= 1, f"export rules empty: {exported}")
                    names = {
                        getattr(r, "namespace", None)
                        if not isinstance(r, dict)
                        else r.get("namespace")
                        for r in rules
                    }
                    ensure(
                        "svc.secret" in names,
                        f"svc.secret missing from export {names}",
                    )
                    ok(f"exported {len(rules)} rule(s)")

                with scenario("audit log records apply", "ns.audit"):
                    audit = await client.namespace_policy_audit(limit=10)
                    entries = getattr(audit, "entries", ()) or ()
                    ensure(len(entries) >= 1, f"expected audit entries, got {audit}")
                    ok(f"audit entries={len(entries)}")

                with scenario(
                    "re-validate after apply still green",
                    "ns.validate",
                ):
                    again = await client.namespace_policy_validate(
                        rules=rules_v1, actor="curriculum"
                    )
                    ensure(again.valid is True, f"re-validate failed: {again}")
                    ok("validate remains green post-apply")

                # Soft check: malformed empty-namespace still returns a response
                step(f"optional bad-rule shape probe: {rules_bad[0].keys()}")
                try:
                    bad = await client.namespace_policy_validate(
                        rules=rules_bad, actor="curriculum"
                    )
                    # Either invalid or exception path — both teach honesty
                    ok(f"bad-rule validate valid={getattr(bad, 'valid', None)}")
                except Exception as exc:  # noqa: BLE001 — demo boundary
                    ok(f"bad-rule rejected via exception: {type(exc).__name__}")

        await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
