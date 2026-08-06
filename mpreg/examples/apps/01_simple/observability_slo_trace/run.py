"""L1 observability_slo_trace — golden signals + W3C traceparent helpers."""

from __future__ import annotations

import asyncio
import re

from mpreg.core.monitoring.server_monitoring import ServerMetricsTracker
from mpreg.core.observability.slo import GOLDEN_SIGNALS, prometheus_alert_rules_yaml
from mpreg.core.observability.trace_context import (
    TRACEPARENT_KEY,
    TRACESTATE_KEY,
    bind_current_trace,
    ensure_traceparent,
    extract_traceparent,
    generate_span_id,
    generate_trace_id,
    generate_traceparent,
    get_current_traceparent,
    inject_trace_metadata,
)
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    get_probe,
    ok,
    scenario,
    step,
)

_TP_RE = re.compile(r"^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$")

async def main() -> None:
    with app_run(
        "observability_slo_trace",
        "Observability SLO + Trace — golden signals and W3C context",
        level="L1",
        probe=True,
    ):
        probe = get_probe()
        assert probe is not None

        with scenario("golden signals catalog", "mon.slo", "mon.golden_signals"):
            with probe.measure("slo.catalog"):
                names = {s.name for s in GOLDEN_SIGNALS}
            ensure(names == {"traffic", "errors", "latency", "saturation"}, f"{names}")
            latency = next(s for s in GOLDEN_SIGNALS if s.name == "latency")
            ensure(
                "mpreg_rpc_latency_ms" in latency.prometheus_metric,
                latency.prometheus_metric,
            )
            ensure("250" in latency.warning_threshold, latency.warning_threshold)
            ok(f"signals={sorted(names)}")

        with scenario("prometheus alert rules yaml", "mon.slo"):
            with probe.measure("slo.yaml"):
                yaml_text = prometheus_alert_rules_yaml()
            ensure("MPREGFederationHealthDegraded" in yaml_text, "missing health alert")
            ensure("MPREGRPCLatencyP95High" in yaml_text, "missing latency alert")
            ensure("mpreg_golden_signals" in yaml_text, "missing group name")
            ensure(yaml_text.strip().startswith("groups:"), "not yaml-ish")
            ok(f"yaml_bytes={len(yaml_text)}")

        with scenario("generate traceparent shape", "mon.trace_context"):
            with probe.measure("trace.generate"):
                tid = generate_trace_id()
                sid = generate_span_id()
                tp = generate_traceparent(sampled=True)
                tp0 = generate_traceparent(sampled=False)
            ensure(len(tid) == 32 and all(c in "0123456789abcdef" for c in tid), tid)
            ensure(len(sid) == 16, sid)
            ensure(_TP_RE.match(tp) is not None, f"bad tp {tp}")
            ensure(tp0.endswith("-00"), tp0)
            ok(f"tp={tp[:20]}…")

        with scenario("ensure + extract metadata", "mon.trace_context"):
            meta: dict = {}
            with probe.measure("trace.ensure"):
                tp1 = ensure_traceparent(meta)
                tp2 = ensure_traceparent(meta)
            ensure(meta[TRACEPARENT_KEY] == tp1, "not stamped")
            ensure(tp2 == tp1, "should reuse existing")
            ensure(extract_traceparent(meta) == tp1, "extract miss")
            ensure(extract_traceparent(None) is None, "None should miss")
            ensure(extract_traceparent({}) is None, "empty should miss")
            ok("ensure/extract ok")

        with scenario("bind_current_trace contextvar", "mon.trace_context"):
            parent = generate_traceparent()
            ensure(get_current_traceparent() is None, "pre-bind dirty")
            with probe.measure("trace.bind"):
                with bind_current_trace(parent, tracestate="vendor=1"):
                    ensure(get_current_traceparent() == parent, "bind failed")
                    injected = inject_trace_metadata({"hop": "1"})
                    ensure(injected[TRACEPARENT_KEY] == parent, "inject ignored bind")
                    ensure(
                        injected.get(TRACESTATE_KEY) == "vendor=1", "tracestate miss"
                    )
                    ensure(injected.get("hop") == "1", "payload clobbered")
            ensure(get_current_traceparent() is None, "unbind failed")
            ok("bind/unbind + inject")

        with scenario("inject preference order", "mon.trace_context"):
            existing = generate_traceparent()
            explicit = generate_traceparent()
            with probe.measure("trace.inject"):
                out = inject_trace_metadata(
                    {TRACEPARENT_KEY: existing, "x": 1},
                    traceparent=explicit,
                )
                out2 = inject_trace_metadata({TRACEPARENT_KEY: existing})
                out3 = inject_trace_metadata({})
            ensure(out[TRACEPARENT_KEY] == explicit, "explicit should win")
            ensure(out["x"] == 1, "meta lost")
            ensure(out2[TRACEPARENT_KEY] == existing, "existing should stick")
            ensure(_TP_RE.match(out3[TRACEPARENT_KEY]) is not None, out3)
            step("preference: explicit → metadata → bind → generate")
            ok("inject preference order")

        with scenario(
            "ServerMetricsTracker snapshot + ExampleProbe",
            "mon.slo",
        ):
            tracker = ServerMetricsTracker()
            # Simulate a burst of RPC + pubsub samples (in-process, no HTTP scrape)
            for i in range(12):
                with probe.measure("metrics.record_rpc"):
                    tracker.record_rpc(1.5 + (i % 4) * 0.25, True)
            tracker.record_rpc(12.0, False, error_code="demo")
            for i in range(5):
                with probe.measure("metrics.record_pubsub"):
                    tracker.record_pubsub(0.8 + i * 0.1, True)
            snap = tracker.snapshot()
            ensure(snap["rpc"]["total"] >= 13, snap["rpc"])
            ensure(snap["rpc"]["errors"] >= 1, snap["rpc"])
            ensure(snap["rpc"]["p95_ms"] > 0.0, snap["rpc"])
            ensure(snap["rpc"]["rps"] >= 0.0, snap["rpc"])
            ensure(snap["pubsub"]["total"] >= 5, snap["pubsub"])
            probe.absorb_server_tracker(tracker, label="demo")
            step(
                f"tracker snapshot rpc.total={snap['rpc']['total']} "
                f"avg_ms={snap['rpc']['avg_ms']} p95_ms={snap['rpc']['p95_ms']} "
                f"rps={snap['rpc']['rps']} pubsub.total={snap['pubsub']['total']}"
            )
            ensure(probe.total_ops >= 10, f"ops {probe.total_ops}")
            ensure(probe.throughput_ops_s > 0.0, "throughput")
            ensure(probe.op("metrics.record_rpc").p95_ms < 5000.0, "record p95")
            ok(
                f"probe ops={probe.total_ops} tracker_rpc_p95={snap['rpc']['p95_ms']} "
                f"throughput_ops_s={probe.throughput_ops_s:.1f}"
            )

        await asyncio.sleep(0)

if __name__ == "__main__":
    asyncio.run(main())
