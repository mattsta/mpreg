"""L1 topic_taxonomy_tour — TopicValidator + TopicTemplateEngine + taxonomy constants."""

from __future__ import annotations

import asyncio

from mpreg.core.topic_taxonomy import (
    TopicAccessLevel,
    TopicTaxonomy,
    TopicTemplateEngine,
    TopicValidator,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

async def main() -> None:
    with app_run(
        "topic_taxonomy_tour",
        "Topic Taxonomy Tour — validate + generate control-plane topics",
        level="L1",
    ):
        v = TopicValidator()
        engine = TopicTemplateEngine()

        with scenario("validate good and bad patterns", "topic.taxonomy", "topic.validate"):
            ok_pat, ok_msg = v.validate_topic_pattern("mpreg.rpc.command.x.started")
            ensure(ok_pat is True, f"good pattern rejected: {ok_msg}")
            bad_pat, bad_msg = v.validate_topic_pattern("bad topic spaces")
            ensure(bad_pat is False, "spaces should fail validation")
            ensure(bool(bad_msg), "expected validation message")
            ok(f"good=True bad_msg={bad_msg!r}")

        with scenario("wildcard match helper", "topic.validate"):
            ensure(v.matches_pattern("a.b.c", "a.*.c") is True, "star match failed")
            ensure(v.matches_pattern("a.b.c", "a.#") is True, "hash match failed")
            ensure(v.matches_pattern("x.y", "a.*") is False, "should not match")
            ok("*, # matching ok")

        with scenario("control-plane access levels", "topic.taxonomy", "topic.access"):
            level = v.get_topic_access_level("mpreg.rpc.command.cmd1.started")
            ensure(level == TopicAccessLevel.CONTROL_PLANE, f"level {level}")
            ensure(v.is_internal_topic("mpreg.rpc.command.cmd1.started") is True, "internal")
            ok(f"access={level}")

        with scenario("template engine generators", "topic.generate"):
            rpc_t = engine.generate_rpc_command_topic("cmd-9", "started")
            ensure(rpc_t == "mpreg.rpc.command.cmd-9.started", f"rpc {rpc_t}")
            q_t = engine.generate_queue_topic("jobs", "enqueued")
            ensure(q_t == "mpreg.queue.jobs.enqueued", f"queue {q_t}")
            fed_t = engine.generate_federation_topic("us-east", "join")
            ensure("us-east" in fed_t and "mpreg." in fed_t, f"fed {fed_t}")
            ok(f"rpc={rpc_t} queue={q_t} fed={fed_t}")

        with scenario(
            "taxonomy constants + format templates",
            "topic.taxonomy",
            "topic.generate",
        ):
            pat = TopicTaxonomy.RPC_COMMAND_STARTED
            concrete = pat.generate_example_topic(command_id="abc")
            ensure(
                concrete == "mpreg.rpc.command.abc.started",
                f"format failed {concrete}",
            )
            # Phase H F17: {param} templates match as single-segment wildcards
            matched = pat.matches_topic(concrete)
            ensure(matched is True, f"template should match concrete: {matched}")
            ensure(
                pat.as_wildcard_pattern() == "mpreg.rpc.command.*.started",
                pat.as_wildcard_pattern(),
            )
            ensure(pat.matches_topic("mpreg.rpc.command.other.started") is True, "other id")
            ensure(pat.matches_topic("mpreg.rpc.command.x.failed") is False, "wrong suffix")
            step(
                f"F17 fixed: matches_topic on '{{param}}' → wildcard "
                f"{pat.as_wildcard_pattern()!r} matched={matched}"
            )
            ensure(
                v.matches_pattern(concrete, "mpreg.rpc.command.*.started") is True,
                "expanded wildcard should match",
            )
            ok(f"constant pattern={pat.pattern} example={concrete}")

        # touch a few more constants for breadth
        for name in (
            "QUEUE_MESSAGE_ENQUEUED",
            "CACHE_INVALIDATION",
            "DISCOVERY_SUMMARY",
        ):
            ensure(hasattr(TopicTaxonomy, name), f"missing {name}")
        ok("taxonomy constants present")
        await asyncio.sleep(0)

if __name__ == "__main__":
    asyncio.run(main())
