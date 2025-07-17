"""Pytest configuration and fixtures for MPREG testing.

This module provides comprehensive async fixtures for setting up and tearing down
MPREG servers and clients cleanly. All fixtures ensure proper cleanup to prevent
tests from hanging or leaving orphaned processes.
"""

import asyncio
from collections.abc import AsyncGenerator
from typing import Any

import pytest_asyncio
from loguru import logger

from mpreg.client_api import MPREGClientAPI
from mpreg.config import MPREGSettings
from mpreg.server import MPREGServer


class AsyncTestContext:
    """Context manager for async test operations with automatic cleanup."""

    def __init__(self) -> None:
        self.servers: list[MPREGServer] = []
        self.clients: list[MPREGClientAPI] = []
        self.tasks: list[asyncio.Task[Any]] = []

    async def __aenter__(self) -> "AsyncTestContext":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Ensure all resources are cleaned up properly."""
        # Cancel all running tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Disconnect all clients
        for client in self.clients:
            try:
                await client.disconnect()
            except Exception as e:
                logger.warning(f"Error disconnecting client: {e}")

        # Stop all servers by cancelling their tasks
        for server in self.servers:
            if hasattr(server, "_peer_connection_manager_task"):
                if (
                    server._peer_connection_manager_task
                    and not server._peer_connection_manager_task.done()
                ):
                    server._peer_connection_manager_task.cancel()
                    try:
                        await server._peer_connection_manager_task
                    except asyncio.CancelledError:
                        pass

        # Clear all collections
        self.servers.clear()
        self.clients.clear()
        self.tasks.clear()


@pytest_asyncio.fixture
async def test_context() -> AsyncGenerator[AsyncTestContext, None]:
    """Provides a clean async test context with automatic resource cleanup."""
    async with AsyncTestContext() as ctx:
        yield ctx


@pytest_asyncio.fixture
async def single_server(
    test_context: AsyncTestContext,
) -> AsyncGenerator[MPREGServer, None]:
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
        port=9999,  # Use fixed port for testing
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
) -> AsyncGenerator[tuple[MPREGServer, MPREGServer], None]:
    """Creates a 2-server cluster for testing distributed operations.

    Example Usage:
        async def test_distributed_workflow(cluster_2_servers):
            server1, server2 = cluster_2_servers
            # Test cross-server RPC calls and dependency resolution
    """
    # Primary server
    settings1 = MPREGSettings(
        host="127.0.0.1",
        port=9001,
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
        port=9002,
        name="Secondary Server",
        cluster_id="test-cluster",
        resources={"model-b", "dataset-2"},
        peers=None,
        connect="ws://127.0.0.1:9001",
        advertised_urls=None,
        gossip_interval=5.0,
    )

    server1 = MPREGServer(settings=settings1)
    server2 = MPREGServer(settings=settings2)

    test_context.servers.extend([server1, server2])

    # Start both servers concurrently
    tasks = [
        asyncio.create_task(server1.server()),
        asyncio.create_task(server2.server()),
    ]
    test_context.tasks.extend(tasks)

    # Wait for servers to establish connection
    await asyncio.sleep(0.5)

    yield server1, server2


@pytest_asyncio.fixture
async def cluster_3_servers(
    test_context: AsyncTestContext,
) -> AsyncGenerator[tuple[MPREGServer, MPREGServer, MPREGServer], None]:
    """Creates a 3-server cluster for testing complex distributed scenarios.

    Example Usage:
        async def test_complex_workflow(cluster_3_servers):
            primary, secondary, tertiary = cluster_3_servers
            # Test multi-hop dependency resolution across cluster
    """
    settings = [
        MPREGSettings(
            host="127.0.0.1",
            port=9011,
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
            port=9012,
            name="Secondary Server",
            cluster_id="test-cluster",
            resources={"model-b", "dataset-2"},
            peers=None,
            connect="ws://127.0.0.1:9011",
            advertised_urls=None,
            gossip_interval=5.0,
        ),
        MPREGSettings(
            host="127.0.0.1",
            port=9013,
            name="Tertiary Server",
            cluster_id="test-cluster",
            resources={"model-c", "dataset-3"},
            peers=None,
            connect="ws://127.0.0.1:9011",
            advertised_urls=None,
            gossip_interval=5.0,
        ),
    ]

    servers = [MPREGServer(settings=s) for s in settings]
    test_context.servers.extend(servers)

    # Start all servers concurrently
    tasks = [asyncio.create_task(server.server()) for server in servers]
    test_context.tasks.extend(tasks)

    # Wait for cluster formation
    await asyncio.sleep(1.0)

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
) -> AsyncGenerator[MPREGServer, None]:
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
        port=9999,
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
