import asyncio

from mpreg.core.config import MPREGSettings
from mpreg.core.model import GoodbyeReason
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager


async def main() -> None:
    async with AsyncTestContext() as ctx:
        port_manager = TestPortManager()

        server1_port = port_manager.get_server_port()
        server2_port = port_manager.get_server_port()
        server3_port = port_manager.get_server_port()

        server1 = MPREGServer(
            MPREGSettings(
                port=server1_port,
                name="DebugServer1",
                cluster_id="debug-reentry-test",
                log_level="INFO",
                gossip_interval=0.3,
            )
        )
        server2 = MPREGServer(
            MPREGSettings(
                port=server2_port,
                name="DebugServer2",
                cluster_id="debug-reentry-test",
                peers=[f"ws://127.0.0.1:{server1_port}"],
                log_level="INFO",
                gossip_interval=0.3,
            )
        )
        server3 = MPREGServer(
            MPREGSettings(
                port=server3_port,
                name="DebugServer3",
                cluster_id="debug-reentry-test",
                peers=[f"ws://127.0.0.1:{server2_port}"],
                log_level="INFO",
                gossip_interval=0.3,
            )
        )

        ctx.servers.extend([server1, server2, server3])

        tasks = [asyncio.create_task(s.server()) for s in (server1, server2, server3)]
        ctx.tasks.extend(tasks)

        await asyncio.sleep(3.0)

        server2_url = f"ws://127.0.0.1:{server2_port}"
        print("server1 peers keys before goodbye:", list(server1.cluster.peers_info))
        print(
            "server2 in server1 peers before:",
            server2_url in server1.cluster.peers_info,
        )

        await server2.send_goodbye(GoodbyeReason.CLUSTER_REBALANCE)
        await asyncio.sleep(1.0)

        print("server1 peers keys after goodbye:", list(server1.cluster.peers_info))
        print(
            "server2 in server1 peers after:", server2_url in server1.cluster.peers_info
        )
        print("server1 departed:", list(server1.cluster._departed_peers))


if __name__ == "__main__":
    asyncio.run(main())
