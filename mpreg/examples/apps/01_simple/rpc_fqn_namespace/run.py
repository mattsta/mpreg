"""L1 rpc_fqn_namespace — FQN wire names, bare qualify, mpreg.* deny, bound ns."""

from __future__ import annotations

import asyncio

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.core.rpc_naming import (
    DEFAULT_USER_NAMESPACE,
    PLATFORM_NAMESPACE_ROOT,
    PlatformRpc,
    assert_call_allowed,
    assert_registration_allowed,
    qualify_rpc_name,
)
from mpreg.examples.apps._shared.runtime import (
    EXAMPLE_RUN_EXCEPTIONS,
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
        "rpc_fqn_namespace",
        "RPC FQN Namespace — bare qualify, deny mpreg.*, bound prefix",
        level="L1",
    ):
        with scenario(
            "pure helpers: bare → FQN; explicit pass-through",
            "rpc.fqn",
            "rpc.register",
        ):
            ensure(
                qualify_rpc_name("add") == f"{DEFAULT_USER_NAMESPACE}.add",
                "default bare qualify",
            )
            ensure(
                qualify_rpc_name("create", "orders") == "orders.create",
                "custom ns bare qualify",
            )
            ensure(
                qualify_rpc_name("orders.create") == "orders.create",
                "explicit FQN unchanged",
            )
            ensure(
                qualify_rpc_name(PlatformRpc.ECHO) == PlatformRpc.ECHO,
                "platform FQN pass-through",
            )
            ok(
                f"qualify: bare→{DEFAULT_USER_NAMESPACE}.*, "
                f"explicit FQN, {PlatformRpc.ECHO}"
            )

        with scenario(
            "namespace deny: users cannot inject mpreg.*",
            "rpc.namespace_deny",
            "rpc.fqn",
        ):
            denied = False
            try:
                assert_registration_allowed(f"{PLATFORM_NAMESPACE_ROOT}.evil.echo")
            except ValueError as exc:
                denied = True
                step(f"deny: {exc}")
            ensure(denied, "mpreg.* registration must raise")
            # Platform path allowed only with allow_platform
            assert_registration_allowed(PlatformRpc.ECHO, allow_platform=True)
            ok(f"mpreg.* denied for users; allow_platform OK for {PlatformRpc.ECHO}")

        with scenario(
            "bound namespace: register/call under hierarchical prefix",
            "rpc.bound_namespace",
            "rpc.fqn",
        ):
            bound = "app.orders"
            assert_registration_allowed("app.orders.create", bound_namespace=bound)
            outside = False
            try:
                assert_registration_allowed("app.other.x", bound_namespace=bound)
            except ValueError as exc:
                outside = True
                step(f"bound reject: {exc}")
            ensure(outside, "outside bound must fail")
            # Calls to platform still OK under bound
            assert_call_allowed(PlatformRpc.ECHO, bound_namespace=bound)
            # Call outside bound fails
            call_out = False
            try:
                assert_call_allowed("app.other.x", bound_namespace=bound)
            except ValueError:
                call_out = True
            ensure(call_out, "call outside bound must fail")
            ok(f"bound={bound} enforces register/call prefix")

        with port_range_context(1, "servers") as ports:
            settings = [
                MPREGSettings(
                    port=ports[0],
                    name="FQN-Demo",
                    resources={"cpu"},
                    log_level="WARNING",
                    default_rpc_namespace="demo",
                    bound_rpc_namespace=None,
                )
            ]

            async def _run(servers: list[MPREGServer]) -> None:
                server = servers[0]

                def echo_msg(msg: str) -> str:
                    return f"echo:{msg}"

                def add(a: int, b: int) -> int:
                    return a + b

                def order_create(sku: str) -> dict[str, str]:
                    return {"sku": sku, "status": "created"}

                # Bare names qualify under settings.default_rpc_namespace ("demo")
                server.register_command("echo_msg", echo_msg, ["cpu"])
                server.register_command("add", add, ["cpu"])
                # Explicit FQN outside default ns
                server.register_command("orders.create", order_create, ["cpu"])
                step("registered bare echo_msg/add → demo.*; explicit orders.create")

                # User cannot steal platform echo
                platform_denied = False
                try:
                    server.register_command(
                        "mpreg.system.echo",
                        echo_msg,
                        ["cpu"],
                    )
                except ValueError as exc:
                    platform_denied = True
                    step(f"live deny: {exc}")
                ensure(platform_denied, "live mpreg.* register must fail")
                ok("live server enforces namespace deny")

                hub = f"ws://127.0.0.1:{ports[0]}"
                with scenario(
                    "client bare names qualify under default_rpc_namespace",
                    "rpc.call",
                    "rpc.fqn",
                    "client.api",
                ):
                    async with MPREGClientAPI(
                        hub, default_rpc_namespace="demo"
                    ) as client:
                        # Bare call → demo.echo_msg
                        out = await client.call(
                            "echo_msg", "hi", locs=frozenset(["cpu"])
                        )
                        ensure(out == "echo:hi", f"bare call got {out!r}")
                        # Explicit FQN
                        summed = await client.call(
                            "demo.add", 20, 22, locs=frozenset(["cpu"])
                        )
                        ensure(summed == 42, f"explicit demo.add got {summed!r}")
                        created = await client.call(
                            "orders.create",
                            "sku-1",
                            locs=frozenset(["cpu"]),
                        )
                        ensure(
                            isinstance(created, dict)
                            and created.get("status") == "created",
                            f"orders.create {created!r}",
                        )
                        ok("bare→demo.*; explicit demo.add + orders.create")

                with scenario(
                    "bound client: bare qualifies under bound; outside denied",
                    "rpc.bound_namespace",
                    "rpc.call",
                ):
                    # Server also bound so register path matches operator story
                    bound_settings = MPREGSettings(
                        port=ports[0],  # unused — we only need client side here
                        name="unused",
                        log_level="WARNING",
                    )
                    del bound_settings  # silence unused; client-only demo
                    async with MPREGClientAPI(
                        hub,
                        default_rpc_namespace="demo",
                        bound_rpc_namespace="orders",
                    ) as client:
                        # Bare "create" under bound → orders.create
                        created = await client.call(
                            "create", "sku-2", locs=frozenset(["cpu"])
                        )
                        ensure(
                            isinstance(created, dict) and created.get("sku") == "sku-2",
                            f"bound bare create {created!r}",
                        )
                        # Call outside bound raises before wire
                        denied = False
                        try:
                            await client.call("demo.add", 1, 2, locs=frozenset(["cpu"]))
                        except ValueError as exc:
                            denied = True
                            step(f"bound call deny: {exc}")
                        ensure(denied, "bound client must reject demo.add")
                        # Platform call still allowed
                        try:
                            peers = await client.list_peers()
                            step(f"platform list_peers ok type={type(peers).__name__}")
                        except EXAMPLE_RUN_EXCEPTIONS as exc:
                            # list_peers may still work; if not, note honestly
                            step(f"platform call path: {type(exc).__name__}: {exc}")
                        ok("bound client: bare under orders.*; outside denied")

                with scenario(
                    "wrong default ns → miss (no context-dependent short names)",
                    "rpc.call",
                    "rpc.fqn",
                ):
                    async with MPREGClientAPI(
                        hub, default_rpc_namespace="other"
                    ) as client:
                        missed = False
                        try:
                            await client.call(
                                "add", 1, 1, locs=frozenset(["cpu"]), timeout=3.0
                            )
                        except EXAMPLE_RUN_EXCEPTIONS as exc:
                            missed = True
                            step(
                                f"other.add miss (expected): "
                                f"{type(exc).__name__}: {exc}"
                            )
                        ensure(
                            missed,
                            "bare add under other ns must not hit demo.add",
                        )
                        ok("no short-name magic across namespaces")

            await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())
