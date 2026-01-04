import asyncio

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer
from tests.test_helpers import TestPortManager, wait_for_condition


async def _run_server(
    settings: MPREGSettings,
) -> tuple[MPREGServer, asyncio.Task[None]]:
    server = MPREGServer(settings=settings)
    task = asyncio.create_task(server.server())
    await asyncio.sleep(1.0)
    return server, task


async def _stop_server(server: MPREGServer, task: asyncio.Task[None]) -> None:
    await server.shutdown_async()
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except TimeoutError:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_persistence_config_file_restores_queues(tmp_path) -> None:
    config_path = tmp_path / "settings.toml"
    config_path.write_text(
        """
        [mpreg]
        host = "127.0.0.1"
        port = 0
        name = "Persist-Config"
        cluster_id = "persist-config"
        resources = ["queue"]
        monitoring_enabled = false
        enable_default_queue = true

        [mpreg.persistence_config]
        mode = "sqlite"
        data_dir = "{data_dir}"
        """.format(data_dir=str(tmp_path / "data"))
    )

    settings = MPREGSettings.from_path(config_path)

    with TestPortManager() as ports:
        settings.port = ports.get_server_port()
        server_a, task_a = await _run_server(settings)
        await server_a._queue_manager.create_queue("jobs")
        await _stop_server(server_a, task_a)

        settings.port = ports.get_server_port()
        server_b, task_b = await _run_server(settings)
        await wait_for_condition(
            lambda: "jobs" in server_b._queue_manager.list_queues(),
            timeout=5.0,
            interval=0.1,
            error_message="Queue not restored from config-based persistence",
        )
        await _stop_server(server_b, task_b)
