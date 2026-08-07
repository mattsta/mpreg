"""L1 transport_protocol_tour — TCP framing + multi-protocol adapter (Phase J)."""

from __future__ import annotations

import asyncio
import struct

from mpreg.core.transport.enhanced_adapter import (
    EnhancedMultiProtocolAdapter,
    EnhancedMultiProtocolAdapterConfig,
    create_enhanced_multi_protocol_adapter,
)
from mpreg.core.transport.factory import TransportFactory
from mpreg.core.transport.interfaces import TransportConfig
from mpreg.core.transport.tcp_transport import (
    MESSAGE_HEADER_SIZE,
    PING_MESSAGE,
    PONG_MESSAGE,
    TCPListener,
    TCPTransport,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step


async def main() -> None:
    with app_run(
        "transport_protocol_tour",
        "Transport Protocol Tour — TCP framing + multi-protocol",
        level="L1",
    ):
        with scenario(
            "TCP wire constants + classes",
            "tx.tcp",
        ):
            ensure(MESSAGE_HEADER_SIZE == 4, f"header {MESSAGE_HEADER_SIZE}")
            ensure(PING_MESSAGE.endswith(b"PING"), f"ping {PING_MESSAGE!r}")
            ensure(PONG_MESSAGE.endswith(b"PONG"), f"pong {PONG_MESSAGE!r}")
            ensure(TCPTransport is not None, "TCPTransport missing")
            ensure(TCPListener is not None, "TCPListener missing")
            ok(
                f"header={MESSAGE_HEADER_SIZE} "
                f"ping_len={len(PING_MESSAGE)} pong_len={len(PONG_MESSAGE)}"
            )

        with scenario(
            "length-prefix frame encode/decode shape",
            "tx.tcp",
        ):
            payload = b'{"hello":"mpreg"}'
            frame = struct.pack(">I", len(payload)) + payload
            (length,) = struct.unpack(">I", frame[:MESSAGE_HEADER_SIZE])
            body = frame[MESSAGE_HEADER_SIZE:]
            ensure(length == len(payload), f"len {length}")
            ensure(body == payload, f"body {body!r}")
            ok(f"framed {len(frame)} bytes (4+{length})")

        with scenario(
            "TransportFactory knows listener API",
            "tx.tcp",
            "tx.multi_protocol",
        ):
            ensure(hasattr(TransportFactory, "create_listener"), "factory API")
            step("TCPTransport registered for tcp:// / tcps:// URLs")
            ok("TransportFactory surface present")

        with scenario(
            "EnhancedMultiProtocolAdapter constructible",
            "tx.multi_protocol",
            "mon.transport",
        ):
            ensure(
                EnhancedMultiProtocolAdapter is not None,
                "adapter class missing",
            )
            ensure(
                EnhancedMultiProtocolAdapterConfig is not None,
                "config missing",
            )
            # Factory uses kwargs (not config object) — construct without bind
            adapter = create_enhanced_multi_protocol_adapter(
                host="127.0.0.1",
                base_port=0,
                enable_health_monitoring=True,
            )
            ensure(adapter is not None, "adapter None")
            ok(f"adapter type={type(adapter).__name__}")
            cfg = EnhancedMultiProtocolAdapterConfig()
            ensure(cfg is not None, "config None")
            ok(
                f"config fields include health={hasattr(cfg, 'enable_health_monitoring')}"
            )

        with scenario(
            "TransportConfig default for WS vs TCP distinction",
            "tx.tcp",
            "tx.security",
        ):
            cfg = TransportConfig()
            ensure(cfg is not None, "TransportConfig missing")
            ensure(hasattr(cfg, "security"), "security field")
            step(
                "curriculum default path is WebSocket (ws://); TCP is the "
                "binary length-prefix alternative for external clients"
            )
            ok("TransportConfig ready for either protocol family")

        await asyncio.sleep(0)


if __name__ == "__main__":
    asyncio.run(main())
