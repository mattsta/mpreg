"""Pytest configuration and fixtures for MPREG testing.

This module provides comprehensive async fixtures for setting up and tearing down
MPREG servers and clients cleanly. All fixtures ensure proper cleanup to prevent
tests from hanging or leaving orphaned processes.
"""

# ruff hates this file and the way imports are used as param names but that's how pytest works.

import asyncio
import gc
import time
import warnings
from collections.abc import AsyncGenerator
from typing import Any

import pytest
import pytest_asyncio
from loguru import logger

warnings.filterwarnings(
    "ignore",
    message=r"Exception ignored in.*gc_cumulative_time",
    category=pytest.PytestUnraisableExceptionWarning,
)

import contextlib

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings

# Import port allocation helpers explicitly
from mpreg.core.port_allocator import (  # noqa
    get_port_allocator,  # noqa
    port_context,  # noqa
    port_range_context,  # noqa
)
from mpreg.fabric.gossip_transport import InProcessGossipTransport
from mpreg.server import MPREGServer


@pytest.fixture
def test_port():
    """Pytest fixture for a single test port."""
    with port_context("testing") as port:
        yield port


@pytest.fixture
def server_port():
    """Pytest fixture for a server port."""
    with port_context("servers") as port:
        yield port


@pytest.fixture
def client_port():
    """Pytest fixture for a client port."""
    with port_context("clients") as port:
        yield port


@pytest.fixture
def federation_port():
    """Pytest fixture for a federation port."""
    with port_context("federation") as port:
        yield port


@pytest.fixture
def port_pair():
    """Pytest fixture for a pair of ports (e.g., server + client)."""
    with port_range_context(2, "testing") as ports:
        yield ports


@pytest.fixture
def server_cluster_ports():
    """Pytest fixture for a cluster of server ports."""
    with port_range_context(8, "servers") as ports:
        yield ports


@pytest.fixture
def port_allocator():
    """Pytest fixture for the port allocator instance."""
    return get_port_allocator()


@pytest.fixture(autouse=True)
def _cleanup_orphaned_event_loop() -> AsyncGenerator[None]:
    """Close any non-running event loop left behind by sync helpers."""
    yield
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        return
    if loop is not None and not loop.is_closed() and not loop.is_running():
        try:
            gc.collect()
            loop.run_until_complete(asyncio.sleep(0))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
            gc.collect()
        except Exception:
            pass
        loop.close()
        asyncio.set_event_loop(None)


@pytest.fixture
def gossip_transport() -> InProcessGossipTransport:
    """In-process transport shared by federation gossip tests."""
    return InProcessGossipTransport()


class AsyncTestContext:
    """Context manager for async test operations with automatic cleanup."""

    def __init__(self) -> None:
        self.servers: list[MPREGServer] = []
        self.clients: list[MPREGClientAPI] = []
        self.tasks: list[asyncio.Task[Any]] = []

    async def __aenter__(self) -> AsyncTestContext:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Ensure all resources are cleaned up properly."""
        # Disconnect all clients first
        for client in self.clients:
            try:
                await client.disconnect()
            except Exception as e:
                try:
                    logger.warning(f"Error disconnecting client: {e}")
                except RuntimeError:
                    # Event loop closed during logging
                    pass

        # Stop all servers using ASYNC shutdown method
        shutdown_tasks = []
        for server in self.servers:
            try:
                # Use async shutdown instead of sync shutdown
                shutdown_tasks.append(asyncio.create_task(server.shutdown_async()))
            except Exception as e:
                with contextlib.suppress(RuntimeError):
                    logger.warning(f"Error initiating server shutdown: {e}")

        # Wait for all servers to shut down properly
        if shutdown_tasks:
            shutdown_timeout = max(5.0, len(shutdown_tasks) * 0.75)
            try:
                await asyncio.wait_for(
                    asyncio.gather(*shutdown_tasks, return_exceptions=True),
                    timeout=shutdown_timeout,
                )
            except TimeoutError:
                with contextlib.suppress(RuntimeError):
                    logger.warning(
                        f"Some servers did not shut down within timeout ({shutdown_timeout:.1f}s)"
                    )
            except Exception as e:
                with contextlib.suppress(RuntimeError):
                    logger.warning(f"Error during server shutdown: {e}")

        # Give server tasks a moment to exit cleanly after shutdown
        if self.tasks:
            try:
                grace_period = max(1.0, min(5.0, len(self.tasks) * 0.1))
                await asyncio.wait(self.tasks, timeout=grace_period)
            except Exception:
                pass

        # Cancel remaining tracked tasks
        if self.tasks:
            for task in self.tasks:
                if not task.done():
                    task.cancel()

        # Give tasks time to respond to cancellation
        try:
            await asyncio.sleep(0.3)
        except RuntimeError:
            # Event loop might be closed
            pass

        # Wait for all tracked tasks to complete with timeout
        if self.tasks:
            task_timeout = max(5.0, len(self.tasks) * 0.3)
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.tasks, return_exceptions=True),
                    timeout=task_timeout,
                )
            except TimeoutError:
                with contextlib.suppress(RuntimeError):
                    logger.warning(
                        f"Some tasks did not complete within timeout during cleanup ({task_timeout:.1f}s)"
                    )
            except Exception as e:
                with contextlib.suppress(RuntimeError):
                    logger.warning(f"Error during task cleanup: {e}")

        # Additional cleanup delay to ensure all async operations complete
        try:
            await asyncio.sleep(0.2)
        except RuntimeError:
            # Event loop closed, skip final delay
            pass

        # NEW: Verify no leaked tasks
        try:
            await self._check_for_leaked_tasks()
        except RuntimeError:
            # Event loop closed, skip leak check
            pass

        # Clear all collections
        self.servers.clear()
        self.clients.clear()
        self.tasks.clear()

    async def cleanup(self) -> None:
        """Explicit cleanup method for compatibility."""
        await self.__aexit__(None, None, None)

    async def _check_for_leaked_tasks(self) -> None:
        """Check for leaked asyncio tasks and attempt cleanup."""
        remaining_tasks = [t for t in asyncio.all_tasks() if not t.done()]
        current_task = asyncio.current_task()

        # Filter out system tasks and expected tasks
        user_tasks = []
        for task in remaining_tasks:
            if current_task is not None and task is current_task:
                continue
            task_name = task.get_name()
            # Skip known system tasks
            if any(
                system_name in task_name.lower()
                for system_name in [
                    "_probe_",
                    "selector",
                    "reader",
                    "writer",
                    "signal",
                ]
            ):
                continue
            # Skip current cleanup task
            if "check_for_leaked_tasks" in task_name:
                continue
            user_tasks.append(task)

        if user_tasks:
            logger.warning(
                f"Found {len(user_tasks)} potentially leaked tasks during cleanup:"
            )
            for task in user_tasks:
                logger.warning(f"  Leaked task: {task.get_name()} - {task}")

            # Cancel leaked tasks
            for task in user_tasks:
                if not task.cancelled() and not task.done():
                    logger.warning(f"Cancelling leaked task: {task.get_name()}")
                    task.cancel()

            # Await cancelled tasks to avoid pending-task warnings
            if user_tasks:
                try:
                    cleanup_timeout = max(1.0, min(5.0, len(user_tasks) * 0.2))
                    await asyncio.wait_for(
                        asyncio.gather(*user_tasks, return_exceptions=True),
                        timeout=cleanup_timeout,
                    )
                except TimeoutError:
                    logger.warning(
                        f"Leaked tasks did not finish within {cleanup_timeout:.1f}s"
                    )
                except asyncio.CancelledError:
                    return


@pytest_asyncio.fixture
async def test_context() -> AsyncGenerator[AsyncTestContext]:
    """Provides a clean async test context with automatic resource cleanup."""
    async with AsyncTestContext() as ctx:
        yield ctx


@pytest_asyncio.fixture
async def single_server(
    test_context: AsyncTestContext,
    server_port: int,  # noqa
) -> AsyncGenerator[MPREGServer]:
    """Creates a single MPREG server for testing.

    Example Usage:
        async def test_basic_echo(single_server):
            # Server is automatically started and will be cleaned up
            client = MPREGClientAPI(f"ws://127.0.0.1:{single_server.settings.port}")
            await client.connect()
            result = await client.call("echo", "hello world")
            assert result == "hello world"
    """
    settings = MPREGSettings(
        host="127.0.0.1",
        port=server_port,  # Use allocated port for testing
        name="Test Server",
        cluster_id="test-cluster",
        resources={"test-resource-1", "test-resource-2"},
        peers=None,
        connect=None,
        advertised_urls=None,
        gossip_interval=5.0,
    )

    server = MPREGServer(settings=settings)
    test_context.servers.append(server)

    # Start server in background task
    server_task = asyncio.create_task(server.server())
    test_context.tasks.append(server_task)

    # Wait longer for server to start and be ready
    await asyncio.sleep(0.5)

    yield server


@pytest_asyncio.fixture
async def cluster_2_servers(
    test_context: AsyncTestContext,
    port_pair: list[int],  # noqa
) -> AsyncGenerator[tuple[MPREGServer, MPREGServer]]:
    """Creates a 2-server cluster for testing distributed operations.

    Example Usage:
        async def test_distributed_workflow(cluster_2_servers):
            server1, server2 = cluster_2_servers
            # Test cross-server RPC calls and dependency resolution
    """
    port1, port2 = port_pair

    # Primary server
    settings1 = MPREGSettings(
        host="127.0.0.1",
        port=port1,
        name="Primary Server",
        cluster_id="test-cluster",
        resources={"model-a", "dataset-1"},
        peers=None,
        connect=None,
        advertised_urls=None,
        gossip_interval=5.0,
    )

    # Secondary server that connects to primary
    settings2 = MPREGSettings(
        host="127.0.0.1",
        port=port2,
        name="Secondary Server",
        cluster_id="test-cluster",
        resources={"model-b", "dataset-2"},
        peers=None,
        connect=f"ws://127.0.0.1:{port1}",
        advertised_urls=None,
        gossip_interval=5.0,
    )

    server1 = MPREGServer(settings=settings1)
    server2 = MPREGServer(settings=settings2)

    test_context.servers.extend([server1, server2])

    # Start server1 first, then server2 after a delay to avoid race conditions
    task1 = asyncio.create_task(server1.server())
    test_context.tasks.append(task1)

    # Wait for server1 to be ready
    await asyncio.sleep(0.2)

    # Now start server2 which will connect to server1
    task2 = asyncio.create_task(server2.server())
    test_context.tasks.append(task2)

    # Wait for servers to establish connection
    await asyncio.sleep(0.5)

    yield server1, server2


@pytest_asyncio.fixture
async def cluster_3_servers(
    test_context: AsyncTestContext,
    server_cluster_ports: list[int],  # noqa
) -> AsyncGenerator[tuple[MPREGServer, MPREGServer, MPREGServer]]:
    """Creates a 3-server cluster for testing complex distributed scenarios.

    Example Usage:
        async def test_complex_workflow(cluster_3_servers):
            primary, secondary, tertiary = cluster_3_servers
            # Test multi-hop dependency resolution across cluster
    """
    port1, port2, port3 = server_cluster_ports[:3]  # Use first 3 ports

    settings = [
        MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Primary Server",
            cluster_id="test-cluster",
            resources={"model-a", "dataset-1"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=5.0,
        ),
        MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Secondary Server",
            cluster_id="test-cluster",
            resources={"model-b", "dataset-2"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=5.0,
        ),
        MPREGSettings(
            host="127.0.0.1",
            port=port3,
            name="Tertiary Server",
            cluster_id="test-cluster",
            resources={"model-c", "dataset-3"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=5.0,
        ),
    ]

    servers = [MPREGServer(settings=s) for s in settings]
    test_context.servers.extend(servers)

    # Start all servers concurrently
    tasks = [asyncio.create_task(server.server()) for server in servers]
    test_context.tasks.extend(tasks)

    # Wait for cluster formation (ensure all servers see each other)
    expected_peers = len(servers)
    deadline = time.time() + 15.0
    while time.time() < deadline:
        if all(
            len(server._peer_directory.nodes()) >= expected_peers
            if server._peer_directory
            else False
            for server in servers
        ):
            break
        await asyncio.sleep(0.2)
    else:
        raise AssertionError(
            "Cluster did not fully form before timeout in cluster_3_servers"
        )

    yield servers[0], servers[1], servers[2]


@pytest_asyncio.fixture
async def client_factory(test_context: AsyncTestContext) -> Any:
    """Factory function for creating test clients with automatic cleanup.

    Example Usage:
        async def test_multiple_clients(single_server, client_factory):
            client1 = await client_factory(single_server.settings.port)
            client2 = await client_factory(single_server.settings.port)
            # Both clients automatically cleaned up
    """

    async def _create_client(port: int, **kwargs: Any) -> MPREGClientAPI:
        client = MPREGClientAPI(f"ws://127.0.0.1:{port}", **kwargs)
        test_context.clients.append(client)
        await client.connect()
        return client

    return _create_client


# Custom server registration functions for testing
def data_processing_function(data: list[int]) -> int:
    """Example data processing function for testing workflows."""
    return sum(data)


def ml_inference_function(
    model_name: str, input_data: dict[str, Any]
) -> dict[str, Any]:
    """Example ML inference function for testing distributed AI workflows."""
    return {
        "model": model_name,
        "prediction": f"processed_{input_data.get('value', 'unknown')}",
        "confidence": 0.95,
    }


def format_results_function(
    raw_results: dict[str, Any], format_type: str = "json"
) -> str:
    """Example results formatting function for testing multi-step workflows."""
    if format_type == "json":
        import json

        return json.dumps(raw_results)
    elif format_type == "summary":
        return f"Result: {raw_results.get('prediction', 'N/A')}"
    else:
        return str(raw_results)


@pytest_asyncio.fixture
async def enhanced_server(
    test_context: AsyncTestContext,
    server_port: int,  # noqa
) -> AsyncGenerator[MPREGServer]:
    """Creates a server with additional test functions registered.

    Example Usage:
        async def test_workflow_pipeline(enhanced_server):
            # Server has data_processing, ml_inference, and format_results functions
            client = MPREGClientAPI(f"ws://127.0.0.1:{enhanced_server.settings.port}")
            await client.connect()

            # Create a multi-step workflow
            result = await client.request([
                RPCCommand(name="process", fun="data_processing", args=([1,2,3,4,5],)),
                RPCCommand(name="inference", fun="ml_inference", args=("my_model", {"value": "process"})),
                RPCCommand(name="final", fun="format_results", args=("inference", "summary"))
            ])
    """
    settings = MPREGSettings(
        host="127.0.0.1",
        port=server_port,  # Use allocated port for testing
        name="Enhanced Test Server",
        cluster_id="test-cluster",
        resources={"data-processor", "ml-model", "formatter"},
        peers=None,
        connect=None,
        advertised_urls=None,
        gossip_interval=5.0,
    )

    server = MPREGServer(settings=settings)

    # Register additional test functions
    server.register_command(
        "data_processing", data_processing_function, ["data-processor"]
    )
    server.register_command("ml_inference", ml_inference_function, ["ml-model"])
    server.register_command("format_results", format_results_function, ["formatter"])

    test_context.servers.append(server)

    # Start server in background
    server_task = asyncio.create_task(server.server())
    test_context.tasks.append(server_task)

    # Wait for server to start
    await asyncio.sleep(0.1)

    yield server


# Pytest configuration
def pytest_configure(config: Any) -> None:
    """Configure pytest for async testing."""
    # Ensure we're using the right event loop policy
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())


# Remove deprecated event_loop fixture - use pytest-asyncio defaults
