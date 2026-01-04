import asyncio

import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import MPREGException
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext
from tests.test_helpers import TestPortManager


@pytest.mark.asyncio
async def test_version_constraint_rejects_mismatch() -> None:
    async with AsyncTestContext() as ctx:
        port_manager = TestPortManager()
        server_port = port_manager.get_server_port()

        settings = MPREGSettings(
            host="127.0.0.1",
            port=server_port,
            name="VersionServer",
            cluster_id="version-test",
            fabric_routing_enabled=True,
        )
        server = MPREGServer(settings=settings)
        ctx.servers.append(server)

        server_task = asyncio.create_task(server.server())
        ctx.tasks.append(server_task)

        def versioned() -> str:
            return "ok"

        server.register_command(
            "versioned",
            versioned,
            ["versioned-resource"],
            function_id="func-versioned",
            version="2.1.0",
        )

        await asyncio.sleep(0.5)

        async with MPREGClientAPI(f"ws://127.0.0.1:{server_port}") as client:
            with pytest.raises(MPREGException):
                await client.call(
                    "versioned",
                    locs=frozenset({"versioned-resource"}),
                    function_id="func-versioned",
                    version_constraint=">=3.0",
                )
