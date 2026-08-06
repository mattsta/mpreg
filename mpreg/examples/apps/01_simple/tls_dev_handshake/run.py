"""L1 tls_dev_handshake — generate_dev_tls_material + wss:// RPC (Phase J F12)."""

from __future__ import annotations

import asyncio
import ssl

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.dev_certs import generate_dev_tls_material
from mpreg.core.port_allocator import port_range_context
from mpreg.core.transport.interfaces import SecurityConfig, TransportConfig
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
        "tls_dev_handshake",
        "TLS Dev Handshake — self-signed CA + wss:// RPC",
        level="L1",
    ):
        material = generate_dev_tls_material()
        try:
            with scenario(
                "generate_dev_tls_material writes PEM chain",
                "tx.tls",
                "tx.security",
            ):
                ensure(material.ca_cert.is_file(), "ca.pem missing")
                ensure(material.server_cert.is_file(), "server.pem missing")
                ensure(material.server_key.is_file(), "server.key missing")
                ensure(material.client_cert.is_file(), "client.pem missing")
                ensure(material.ca_cert.stat().st_size > 0, "ca empty")
                step(f"material dir={material.directory}")
                ok("CA + server + client PEMs present")

            with port_range_context(1, "servers") as ports:
                settings = [
                    MPREGSettings(
                        host="127.0.0.1",
                        port=ports[0],
                        name="TLS-Dev",
                        resources={"secure"},
                        log_level="WARNING",
                        gossip_interval=30.0,
                        tls_cert_file=str(material.server_cert),
                        tls_key_file=str(material.server_key),
                        tls_ca_file=None,
                    )
                ]

                async def _run(servers: list[MPREGServer]) -> None:
                    server = servers[0]

                    def secure_echo(msg: str) -> str:
                        return f"tls:{msg}"

                    server.register_command("secure_echo", secure_echo, ["secure"])
                    wss = f"wss://127.0.0.1:{ports[0]}"
                    step(f"listener wss on {wss}")

                    with scenario(
                        "wss client with CA trust + RPC",
                        "tx.tls",
                        "rpc.call",
                        "client.api",
                    ):
                        # IP URL vs DNS SAN: load CA but disable hostname pin
                        ctx = ssl.create_default_context()
                        ctx.load_verify_locations(str(material.ca_cert))
                        ctx.check_hostname = False
                        ctx.verify_mode = ssl.CERT_REQUIRED
                        transport = TransportConfig(
                            security=SecurityConfig(
                                ssl_context=ctx,
                                ca_file=str(material.ca_cert),
                                verify_cert=True,
                            )
                        )
                        async with MPREGClientAPI(
                            wss, transport_config=transport
                        ) as client:
                            out = await client.call(
                                "secure_echo",
                                "hello",
                                locs=frozenset(["secure"]),
                                timeout=5.0,
                            )
                            ensure(out == "tls:hello", f"got {out!r}")
                        ok(f"wss RPC ok → {out!r}")

                    with scenario(
                        "plain ws:// rejected when server is TLS-only",
                        "tx.tls",
                    ):
                        rejected = False
                        try:
                            async with asyncio.timeout(3.0):
                                async with MPREGClientAPI(
                                    f"ws://127.0.0.1:{ports[0]}"
                                ) as client:
                                    await client.call(
                                        "secure_echo",
                                        "x",
                                        locs=frozenset(["secure"]),
                                        timeout=2.0,
                                    )
                        except (Exception, asyncio.CancelledError, TimeoutError) as exc:
                            rejected = True
                            step(f"ws to tls port failed: {type(exc).__name__}")
                        ensure(rejected, "plain ws must not speak TLS server")
                        ok("ws:// fail-closed against TLS listener")

                    with scenario(
                        "client cert PEMs loadable for mTLS drills",
                        "tx.tls",
                        "tx.security",
                    ):
                        sec = SecurityConfig(
                            cert_file=str(material.client_cert),
                            key_file=str(material.client_key),
                            ca_file=str(material.ca_cert),
                            verify_cert=False,
                        )
                        ctx2 = sec.create_ssl_context()
                        ensure(ctx2 is not None, "client SSL context missing")
                        ok("client cert+key load into SecurityConfig")
                        step(
                            "full CERT_REQUIRED mTLS: set tls_ca_file on server + "
                            "client cert_file/key_file (same material)"
                        )

                await run_with_servers(settings, _run)
        finally:
            material.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
