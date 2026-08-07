"""L1 ns_engine_direct — in-process NamespacePolicyEngine owner/viewer gates."""

from __future__ import annotations

import asyncio

from mpreg.core.namespace_policy import NamespacePolicyEngine, NamespacePolicyRule
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step


async def main() -> None:
    with app_run(
        "ns_engine_direct",
        "NS Engine Direct — owner/viewer decisions",
        level="L1",
    ):
        with scenario(
            "construct engine with rules",
            "ns.engine",
        ):
            # Inspect rule fields dynamically for version skew
            fields = getattr(NamespacePolicyRule, "__dataclass_fields__", {})
            step(f"NamespacePolicyRule fields={list(fields)}")
            kwargs: dict = {"namespace": "svc.orders"}
            if "owners" in fields:
                kwargs["owners"] = frozenset({"cluster-a"})
            if "visibility" in fields:
                kwargs["visibility"] = frozenset({"cluster-a", "cluster-b"})
            if "policy_version" in fields:
                kwargs["policy_version"] = "v1"
            rule = NamespacePolicyRule(**kwargs)
            engine = NamespacePolicyEngine(
                enabled=True,
                default_allow=False,
                rules=(rule,),
            )
            ensure(engine.enabled is True, "enabled")
            ensure(len(engine.rules) == 1, "rules")
            ok(f"engine rules={len(engine.rules)} default_allow=False")

        with scenario(
            "owner source allowed / denied",
            "ns.engine",
        ):
            allow = engine.allows_source("svc.orders", "cluster-a")
            deny = engine.allows_source("svc.orders", "cluster-z")
            ensure(allow.allowed is True, f"owner allow reason={allow.reason}")
            ensure(deny.allowed is False, f"foreign reason={deny.reason}")
            ok(f"source owner={allow.reason} foreign={deny.reason}")

        with scenario(
            "viewer visibility gate",
            "ns.engine",
        ):
            v_ok = engine.allows_viewer("svc.orders", "cluster-b")
            v_no = engine.allows_viewer("svc.orders", "cluster-z")
            ensure(v_ok.allowed is True, f"viewer b reason={v_ok.reason}")
            ensure(v_no.allowed is False, f"viewer z reason={v_no.reason}")
            ok(f"viewer b={v_ok.reason} z={v_no.reason}")

        with scenario(
            "disabled engine allows all",
            "ns.engine",
        ):
            off = NamespacePolicyEngine(enabled=False, default_allow=False, rules=())
            d = off.allows_source("anything", "x")
            ensure(d.allowed is True, f"disabled should allow got {d}")
            ensure(d.reason == "policy_disabled", f"reason={d.reason}")
            ok("policy_disabled short-circuit")

        with scenario(
            "default_allow when no rule",
            "ns.engine",
        ):
            open_e = NamespacePolicyEngine(enabled=True, default_allow=True, rules=())
            closed_e = NamespacePolicyEngine(
                enabled=True, default_allow=False, rules=()
            )
            ensure(
                open_e.allows_source("no.rule", "c").allowed is True,
                "default allow",
            )
            ensure(
                closed_e.allows_source("no.rule", "c").allowed is False,
                "default deny",
            )
            ok("default_allow true/false")

        with scenario(
            "data-plane write ownership",
            "ns.engine",
        ):
            if hasattr(engine, "allows_data_access"):
                w_ok = engine.allows_data_access(
                    "svc.orders", actor_cluster="cluster-a", write=True
                )
                w_no = engine.allows_data_access(
                    "svc.orders", actor_cluster="cluster-z", write=True
                )
                ensure(w_ok.allowed is True, f"write owner {w_ok.reason}")
                ensure(w_no.allowed is False, f"write foreign {w_no.reason}")
                ok(f"data write owner={w_ok.reason} foreign={w_no.reason}")
            else:
                ok("allows_data_access not on this build — skipped")

        await asyncio.sleep(0)
        ok("ns_engine_direct complete")


if __name__ == "__main__":
    asyncio.run(main())
