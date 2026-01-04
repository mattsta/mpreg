"""
Demonstration of concurrent-safe port allocation for MPREG testing.

This test file showcases how to use the port allocation system
to enable safe concurrent testing with pytest-xdist.
"""

import asyncio
import socket
import time

import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.server import MPREGServer

from .test_helpers import TestPortManager, server_cluster_urls, test_server_url


@pytest.mark.unit
def test_port_allocator_basic(port_allocator):
    """Test basic port allocation functionality."""
    # Get some ports
    port1 = port_allocator.allocate_port("testing")
    port2 = port_allocator.allocate_port("servers")

    # Ports should be different
    assert port1 != port2

    # Get worker info and validate ranges
    info = port_allocator.get_worker_info()
    testing_range = info["port_ranges"]["testing"]
    servers_range = info["port_ranges"]["servers"]

    # Should be in valid ranges (accounting for worker offsets)
    assert testing_range["start"] <= port1 <= testing_range["end"]  # testing range
    assert servers_range["start"] <= port2 <= servers_range["end"]  # servers range
    assert "worker_id" in info
    assert "allocated_ports" in info
    assert port1 in info["allocated_ports"]
    assert port2 in info["allocated_ports"]


@pytest.mark.unit
def test_port_allocator_skips_in_use(port_allocator):
    """Ensure allocator does not hand out ports already in use."""
    port = port_allocator.allocate_port("testing")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", port))
        port_allocator.release_port(port)
        allocated = port_allocator.allocate_port("testing", preferred_port=port)
        assert allocated != port
    finally:
        sock.close()
        if "allocated" in locals():
            port_allocator.release_port(allocated)


@pytest.mark.unit
def test_port_callback_invoked_on_auto_assignment():
    """Verify auto-assigned ports trigger the port callback."""
    captured: dict[str, int] = {}

    def _callback(port: int) -> None:
        captured["port"] = port

    settings = MPREGSettings(
        port=None,
        on_port_assigned=_callback,
        monitoring_enabled=False,
    )
    server = MPREGServer(settings=settings)

    try:
        assert captured.get("port") == server.settings.port
    finally:
        if server._auto_allocated_port is not None:
            from mpreg.core.port_allocator import release_port

            release_port(server._auto_allocated_port)
            server._auto_allocated_port = None


@pytest.mark.unit
def test_port_range_validation(port_allocator):
    """Test that all port ranges are valid and within configured limits."""
    info = port_allocator.get_worker_info()

    # Get the actual configured ranges from the port allocator
    configured_ranges = port_allocator.RANGES

    # Validate each range
    for category, range_info in info["port_ranges"].items():
        start = range_info["start"]
        end = range_info["end"]
        available = range_info["available_ports"]

        # Get the configured range for this category
        configured_range = configured_ranges[category]

        # Basic range validation
        assert start <= end, f"Invalid range in {category}: {start} > {end}"
        assert available > 0, f"No available ports in {category}"
        assert available == (end - start + 1), f"Port count mismatch in {category}"

        # Range should be within the configured bounds for this category
        assert start >= configured_range.start, (
            f"Port range too low in {category}: {start} < {configured_range.start}"
        )
        assert end <= configured_range.end, (
            f"Port range too high in {category}: {end} > {configured_range.end}"
        )

        # Range should be reasonable (above privileged ports)
        assert start >= 10000, f"Port range in privileged range in {category}: {start}"


@pytest.mark.unit
def test_worker_id_detection(port_allocator):
    """Test worker ID detection and offset calculation."""
    info = port_allocator.get_worker_info()
    worker_id = info["worker_id"]
    worker_offset = info["worker_offset"]

    # Worker ID should be valid
    assert isinstance(worker_id, str)
    assert len(worker_id) > 0

    # Worker offset should be non-negative and reasonable
    assert worker_offset >= 0
    assert worker_offset < 10000  # Reasonable upper bound

    # Offset should be multiple of 200 for non-master workers
    if worker_id != "master":
        assert worker_offset % 200 == 0, f"Worker offset not aligned: {worker_offset}"


@pytest.mark.unit
def test_bulk_port_allocation(port_allocator):
    """Test efficient bulk port allocation."""
    # Test bulk allocation
    bulk_ports = port_allocator.allocate_port_range(5, "testing")

    assert len(bulk_ports) == 5
    assert len(set(bulk_ports)) == 5  # All unique

    # Ports should be in valid range
    info = port_allocator.get_worker_info()
    testing_range = info["port_ranges"]["testing"]
    for port in bulk_ports:
        assert testing_range["start"] <= port <= testing_range["end"]

    # Clean up
    for port in bulk_ports:
        port_allocator.release_port(port)


@pytest.mark.unit
def test_category_isolation(port_allocator):
    """Test that different categories get non-overlapping ports."""
    # Allocate one port from each category
    allocated_ports = {}
    categories = [
        "servers",
        "clients",
        "federation",
        "testing",
        "research",
        "monitoring",
    ]

    try:
        for category in categories:
            port = port_allocator.allocate_port(category)
            allocated_ports[category] = port

        # All ports should be unique
        ports = list(allocated_ports.values())
        assert len(set(ports)) == len(ports), "Categories have overlapping ports"

        # Verify each port is in correct category range
        info = port_allocator.get_worker_info()
        for category, port in allocated_ports.items():
            range_info = info["port_ranges"][category]
            assert range_info["start"] <= port <= range_info["end"], (
                f"Port {port} not in {category} range {range_info['start']}-{range_info['end']}"
            )

    finally:
        # Clean up
        for port in allocated_ports.values():
            port_allocator.release_port(port)


@pytest.mark.unit
def test_port_context_manager(test_port):
    """Test port allocation with context manager fixture."""
    # test_port is automatically allocated and cleaned up
    assert isinstance(test_port, int)
    assert test_port > 0


@pytest.mark.unit
def test_multiple_ports(port_pair, server_cluster_ports):
    """Test allocation of multiple ports."""
    assert len(port_pair) == 2
    assert len(server_cluster_ports) == 8

    # All ports should be unique
    all_ports = port_pair + server_cluster_ports
    assert len(set(all_ports)) == len(all_ports)


@pytest.mark.unit
def test_url_helpers():
    """Test URL generation helpers."""
    with test_server_url("servers") as server_url:
        assert server_url.startswith("ws://127.0.0.1:")
        port = int(server_url.split(":")[-1])
        assert 10000 <= port <= 15000


@pytest.mark.unit
def test_port_manager():
    """Test TestPortManager for complex scenarios."""
    with TestPortManager() as manager:
        # Get individual ports
        server_port = manager.get_server_port()
        client_port = manager.get_client_port()

        assert server_port != client_port

        # Get URLs (these allocate new ports, not the ones we just got)
        server_url = manager.get_server_url()
        client_url = manager.get_client_url()

        # Verify URLs are well-formed
        assert server_url.startswith("ws://127.0.0.1:")
        assert client_url.startswith("ws://127.0.0.1:")

        # Get a cluster
        cluster_urls = manager.get_server_cluster(3)
        assert len(cluster_urls) == 3

        # All should be unique
        ports = [int(url.split(":")[-1]) for url in cluster_urls]
        assert len(set(ports)) == 3


@pytest.mark.integration
@pytest.mark.slow
async def test_concurrent_servers(test_context):
    """Test multiple servers with allocated ports running concurrently."""
    with TestPortManager() as port_manager:
        # Create multiple servers with different ports
        ports = port_manager.get_port_range(3, "servers")
        servers = []
        tasks = []

        try:
            # Create and start servers
            for i, port in enumerate(ports):
                settings = MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"Test Server {i + 1}",
                    cluster_id=f"test-cluster-{i + 1}",
                    resources={f"resource-{i + 1}"},
                    log_level="ERROR",  # Reduce noise
                )

                server = MPREGServer(settings=settings)
                servers.append(server)
                test_context.servers.append(server)

                # Start server
                task = asyncio.create_task(server.server())
                tasks.append(task)
                test_context.tasks.append(task)

                await asyncio.sleep(0.1)  # Stagger startup

            # Test that all servers are responsive with retries.
            for port in ports:
                deadline = time.time() + 5.0
                last_error: Exception | None = None
                while time.time() < deadline:
                    try:
                        async with MPREGClientAPI(f"ws://127.0.0.1:{port}") as client:
                            result = await client.call("echo", "hello")
                            assert result == "hello"
                        last_error = None
                        break
                    except Exception as e:
                        last_error = e
                        await asyncio.sleep(0.2)
                if last_error is not None:
                    pytest.fail(f"Server on port {port} not responsive: {last_error}")

        except Exception as e:
            pytest.fail(f"Error in concurrent server test: {e}")


@pytest.mark.integration
async def test_server_cluster_startup(test_context):
    """Test that multiple servers can start with allocated ports."""
    with TestPortManager() as port_manager:
        port1 = port_manager.get_server_port()
        port2 = port_manager.get_server_port()

        # Primary server
        settings1 = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Primary Server",
            cluster_id="test-cluster",
            resources={"step1"},
            log_level="ERROR",
        )

        # Secondary server connects to primary
        settings2 = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Secondary Server",
            cluster_id="test-cluster",
            resources={"step2"},
            peers=[f"ws://127.0.0.1:{port1}"],
            log_level="ERROR",
        )

        server1 = MPREGServer(settings=settings1)
        server2 = MPREGServer(settings=settings2)
        test_context.servers.extend([server1, server2])

        # Start servers
        task1 = asyncio.create_task(server1.server())
        task2 = asyncio.create_task(server2.server())
        test_context.tasks.extend([task1, task2])

        await asyncio.sleep(1.0)  # Allow servers to start

        # Test that both servers are responsive
        async with MPREGClientAPI(f"ws://127.0.0.1:{port1}") as client1:
            result1 = await client1.call("echo", "hello1")
            assert result1 == "hello1"

        async with MPREGClientAPI(f"ws://127.0.0.1:{port2}") as client2:
            result2 = await client2.call("echo", "hello2")
            assert result2 == "hello2"


@pytest.mark.unit
def test_url_context_managers():
    """Test URL context managers for clean resource management."""
    with server_cluster_urls(3, "servers") as urls:
        assert len(urls) == 3
        for url in urls:
            assert url.startswith("ws://127.0.0.1:")

        # All URLs should have different ports
        ports = [int(url.split(":")[-1]) for url in urls]
        assert len(set(ports)) == 3
