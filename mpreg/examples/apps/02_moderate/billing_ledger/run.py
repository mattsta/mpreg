"""L2 billing_ledger — RPC charges + cache balances + queue settlements."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.global_cache import (
    CacheMetadata,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.core.message_queue import DeliveryGuarantee
from mpreg.core.message_queue_manager import create_reliable_queue_manager
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
)
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport
from mpreg.server import MPREGServer

async def main() -> None:
    with (
        app_run(
            "billing_ledger",
            "Billing Ledger — charge RPC + balance cache + settle queue",
            level="L2",
        ),
        port_range_context(1, "servers") as ports,
    ):
        settings = [
            MPREGSettings(
                port=ports[0],
                name="Billing-API",
                resources={"billing", "api"},
                log_level="WARNING",
                gossip_interval=30.0,
            )
        ]

        async def _run(servers: list[MPREGServer]) -> None:
            server = servers[0]
            ledger: dict[str, dict[str, Any]] = {}

            def charge(account: str, cents: int, idem: str) -> dict[str, Any]:
                if idem in ledger:
                    return {**ledger[idem], "replay": True}
                txn = {
                    "txn_id": f"txn-{uuid.uuid4().hex[:8]}",
                    "account": account,
                    "cents": cents,
                    "idem": idem,
                    "status": "authorized",
                    "replay": False,
                }
                ledger[idem] = txn
                return txn

            def get_txn(txn_id: str) -> dict[str, Any]:
                for t in ledger.values():
                    if t["txn_id"] == txn_id:
                        return {"found": True, **t}
                return {"found": False, "txn_id": txn_id}

            server.register_command("charge", charge, ["billing", "api"])
            server.register_command("get_txn", get_txn, ["billing", "api"])

            transport = InProcessCacheTransport()
            protocol = FabricCacheProtocol(
                "billing-cache", transport=transport, gossip_interval=60.0
            )
            cache = GlobalCacheManager(
                GlobalCacheConfiguration(
                    enable_l2_persistent=False,
                    enable_l3_distributed=False,
                    enable_l4_federation=False,
                ),
                cache_protocol=protocol,
            )
            qmgr = create_reliable_queue_manager()
            settlements: list[dict[str, Any]] = []

            def settle_worker(message: Any) -> None:
                payload = getattr(message, "payload", message)
                if isinstance(payload, dict):
                    settlements.append(payload)

            try:
                await qmgr.create_queue("settlements")
                qmgr.subscribe_to_queue(
                    "settlements",
                    "settle-worker",
                    "settle.*",
                    callback=settle_worker,
                )

                url = f"ws://127.0.0.1:{ports[0]}"
                async with MPREGClientAPI(url) as client:
                    with scenario(
                        "authorize charge via RPC",
                        "prod.billing",
                        "rpc.call",
                        "rpc.register",
                    ):
                        txn = await client.call(
                            "charge",
                            "acct-1",
                            2500,
                            "idem-1",
                            locs=frozenset(["billing", "api"]),
                        )
                        ensure(
                            isinstance(txn, dict) and txn.get("status") == "authorized",
                            f"charge failed {txn}",
                        )
                        ensure(txn.get("replay") is False, "first charge replay?")
                        ok(f"txn={txn['txn_id']} cents={txn['cents']}")

                    with scenario(
                        "idempotent replay same key",
                        "rpc.call",
                        "prod.billing",
                    ):
                        again = await client.call(
                            "charge",
                            "acct-1",
                            2500,
                            "idem-1",
                            locs=frozenset(["billing", "api"]),
                        )
                        ensure(again.get("replay") is True, f"expected replay {again}")
                        ensure(
                            again.get("txn_id") == txn["txn_id"],
                            "idempotency broke txn_id",
                        )
                        ok(f"replay txn_id={again['txn_id']}")

                    with scenario(
                        "cache running balance",
                        "cache.put_get",
                        "cache.l1",
                    ):
                        key = GlobalCacheKey.from_data(
                            "billing.balance", {"account": "acct-1"}
                        )
                        await cache.put(
                            key,
                            {"account": "acct-1", "cents": 2500},
                            CacheMetadata(computation_cost_ms=1.0, ttl_seconds=300.0),
                        )
                        got = await cache.get(key)
                        ensure(
                            got.success and got.entry is not None,
                            "balance cache miss",
                        )
                        ensure(
                            got.entry.value.get("cents") == 2500,
                            f"balance {got.entry.value}",
                        )
                        ok(f"balance cached={got.entry.value}")

                    with scenario(
                        "enqueue settlement job",
                        "queue.send",
                        "queue.alo",
                        "queue.subscribe",
                    ):
                        result = await qmgr.send_message(
                            "settlements",
                            "settle.capture",
                            {
                                "txn_id": txn["txn_id"],
                                "account": "acct-1",
                                "cents": 2500,
                            },
                            DeliveryGuarantee.AT_LEAST_ONCE,
                        )
                        ensure(result.success, f"settle send {result}")
                        for _ in range(40):
                            if settlements:
                                break
                            await asyncio.sleep(0.05)
                        ensure(len(settlements) >= 1, f"no settlement {settlements}")
                        ensure(
                            settlements[0].get("txn_id") == txn["txn_id"],
                            f"bad settle {settlements[0]}",
                        )
                        ok(f"settled={settlements[0]}")

                    with scenario("get_txn lookup", "rpc.call"):
                        found = await client.call(
                            "get_txn",
                            txn["txn_id"],
                            locs=frozenset(["billing", "api"]),
                        )
                        ensure(found.get("found") is True, f"get_txn {found}")
                        ok(f"found status={found.get('status')}")
            finally:
                await cache.shutdown()
                await protocol.shutdown()

        await run_with_servers(settings, _run)

if __name__ == "__main__":
    asyncio.run(main())
