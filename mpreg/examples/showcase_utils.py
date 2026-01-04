from __future__ import annotations

#!/usr/bin/env python3
"""Utilities for MPREG example showcases."""


import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer


@dataclass(frozen=True, slots=True)
class ServerHandle:
    server: MPREGServer
    task: asyncio.Task[Any]


async def start_servers(settings_list: list[MPREGSettings]) -> list[ServerHandle]:
    """Start multiple MPREG servers with a brief stagger."""
    handles: list[ServerHandle] = []
    for settings in settings_list:
        server = MPREGServer(settings=settings)
        task = asyncio.create_task(server.server())
        handles.append(ServerHandle(server=server, task=task))
        await asyncio.sleep(0.1)
    await asyncio.sleep(1.0)
    return handles


async def stop_servers(handles: list[ServerHandle]) -> None:
    """Stop servers and wait a brief moment for cleanup."""
    for handle in handles:
        handle.server._shutdown_event.set()
    await asyncio.sleep(0.2)


async def run_with_servers(
    settings_list: list[MPREGSettings],
    runner: Callable[[list[MPREGServer]], Awaitable[Any]],
) -> Any:
    """Start servers, run a coroutine, and stop servers."""
    handles = await start_servers(settings_list)
    servers = [handle.server for handle in handles]
    try:
        return await runner(servers)
    finally:
        await stop_servers(handles)
