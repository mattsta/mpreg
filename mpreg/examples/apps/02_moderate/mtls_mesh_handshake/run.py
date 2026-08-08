"""L2 mtls_mesh_handshake — CERT_REQUIRED mTLS wss mesh (Phase K depth)."""

from __future__ import annotations

import asyncio
import ssl

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.dev_certs import generate_dev_tls_material
from mpreg.core.port_allocator import port_range_context
from mpreg.core.transport.interfaces import (
    SecurityConfig,
    TransportConfig,
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


def _client_ctx(material, *, with_client_cert: bool) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(str(material.ca_cert))
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    if with_client_cert:
        ctx.load_cert_chain(str(material.client_cert), str(material.client_key))
    return ctx


async def main() -> None:
    with app_run(
        "mtls_mesh_handshake",
        "mTLS Mesh Handshake — CERT_REQUIRED server + client cert",
        level="L2",
    ):
        material = generate_dev_tls_material()
        try:
            with scenario(
                "dev material for mTLS mesh",
                "tx.tls",
                "tx.security",
            ):
                ensure(material.ca_cert.is_file(), "ca missing")
                ensure(material.client_cert.is_file(), "client cert missing")
                ok(f"material={material.directory}")

            with port_range_context(1, "servers") as ports:
                # Server requires client certs via tls_ca_file → CERT_REQUIRED
                settings = [
                    MPREGSettings(
                        host="127.0.0.1",
                        port=ports[0],
                        name="MTLS-Server",
                        resources={"secure"},
                        log_level="WARNING",
                        gossip_interval=30.0,
                        tls_cert_file=str(material.server_cert),
                        tls_key_file=str(material.server_key),
                        tls_ca_file=str(material.ca_cert),
                    )
                ]

                async def _run(servers: list[MPREGServer]) -> None:
                    server = servers[0]

                    def secure_ping(msg: str) -> str:
                        return f"mtls:{msg}"

                    server.register_command("secure_ping", secure_ping, ["secure"])
                    wss = f"wss://127.0.0.1:{ports[0]}"
                    step(f"mTLS listener {wss}")

                    with scenario(
                        "client WITH cert succeeds CERT_REQUIRED",
                        "tx.tls",
                        "tx.security",
                        "rpc.call",
                        "client.api",
                    ):
                        transport = TransportConfig(
                            security=SecurityConfig(
                                ssl_context=_client_ctx(
                                    material, with_client_cert=True
                                ),
                                cert_file=str(material.client_cert),
                                key_file=str(material.client_key),
                                ca_file=str(material.ca_cert),
                                verify_cert=True,
                            )
                        )
                        async with MPREGClientAPI(
                            wss, transport_config=transport
                        ) as client:
                            out = await client.call(
                                "secure_ping",
                                "ok",
                                locs=frozenset(["secure"]),
                                timeout=5.0,
                            )
                            ensure(out == "mtls:ok", f"got {out!r}")
                        ok(f"mTLS RPC ok → {out!r}")

                    with scenario(
                        "client WITHOUT cert rejected",
                        "tx.tls",
                        "tx.security",
                    ):
                        rejected = False
                        try:
                            transport = TransportConfig(
                                security=SecurityConfig(
                                    ssl_context=_client_ctx(
                                        material, with_client_cert=False
                                    ),
                                    ca_file=str(material.ca_cert),
                                    verify_cert=True,
                                )
                            )
                            async with asyncio.timeout(4.0):
                                async with MPREGClientAPI(
                                    wss, transport_config=transport
                                ) as client:
                                    await client.call(
                                        "secure_ping",
                                        "no",
                                        locs=frozenset(["secure"]),
                                        timeout=2.0,
                                    )
                        except EXAMPLE_RUN_EXCEPTIONS as exc:
                            rejected = True
                            step(f"no-cert rejected: {type(exc).__name__}")
                        ensure(rejected, "CERT_REQUIRED must reject bare client")
                        ok("missing client cert fail-closed")

                    with scenario(
                        "server SSL context is CERT_REQUIRED when ca set",
                        "tx.tls",
                    ):
                        sec = SecurityConfig(
                            cert_file=str(material.server_cert),
                            key_file=str(material.server_key),
                            ca_file=str(material.ca_cert),
                        )
                        sctx = sec.create_server_ssl_context()
                        ensure(sctx is not None, "server ctx None")
                        ensure(
                            sctx.verify_mode == ssl.CERT_REQUIRED,
                            f"verify_mode={sctx.verify_mode}",
                        )
                        ok("create_server_ssl_context CERT_REQUIRED")

                    with scenario(
                        "plain ws still fail-closed",
                        "tx.tls",
                    ):
                        rejected = False
                        try:
                            async with asyncio.timeout(3.0):
                                async with MPREGClientAPI(
                                    f"ws://127.0.0.1:{ports[0]}"
                                ) as client:
                                    await client.call(
                                        "secure_ping",
                                        "x",
                                        locs=frozenset(["secure"]),
                                        timeout=2.0,
                                    )
                        except EXAMPLE_RUN_EXCEPTIONS:
                            rejected = True
                        ensure(rejected, "ws must fail")
                        ok("ws:// fail-closed")

                await run_with_servers(settings, _run)
        finally:
            material.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
