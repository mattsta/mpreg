"""
Tests for TCP transport implementation.

This module contains comprehensive tests for both TCP and TCP+TLS transport,
including protocol compliance, error handling, and performance characteristics.
"""

import asyncio
import ssl
import struct

import pytest

from mpreg.core.transport import (
    SecurityConfig,
    TransportConfig,
    TransportConnectionError,
    TransportError,
    TransportFactory,
    TransportTimeoutError,
)
from mpreg.core.transport.interfaces import TransportProtocol
from mpreg.core.transport.tcp_transport import (
    MESSAGE_HEADER_SIZE,
    TCPTransport,
)

from .test_helpers import create_test_ssl_context


class TestTCPTransport:
    """Test cases for TCP transport implementation."""

    @pytest.mark.asyncio
    async def test_tcp_create_transport(self):
        """Test creating TCP transport via factory."""
        transport = TransportFactory.create("tcp://localhost:6668")
        assert isinstance(transport, TCPTransport)
        assert transport.protocol == TransportProtocol.TCP
        assert not transport.is_secure
        assert not transport.connected

    @pytest.mark.asyncio
    async def test_tcp_secure_create_transport(self):
        """Test creating TCP secure transport via factory."""
        config = TransportConfig(security=SecurityConfig(verify_cert=False))
        transport = TransportFactory.create("tcps://localhost:6669", config)
        assert isinstance(transport, TCPTransport)
        assert transport.protocol == TransportProtocol.TCP_SECURE
        assert transport.is_secure
        assert not transport.connected

    @pytest.mark.asyncio
    async def test_tcp_basic_communication(self, tcp_port):
        """Test basic TCP send/receive functionality."""
        config = TransportConfig()

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Create and connect client
            client = TransportFactory.create(f"tcp://127.0.0.1:{tcp_port}", config)

            # Test connection
            await client.connect()
            assert client.connected

            # Accept server connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=1.0)

            # Test bidirectional communication
            test_message = b"Hello, TCP World!"

            # Client -> Server
            await client.send(test_message)
            received = await server_transport.receive()
            assert received == test_message

            # Server -> Client
            response = b"Hello, TCP Client!"
            await server_transport.send(response)
            received = await client.receive()
            assert received == response

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_tcp_secure_communication(self, tcp_port):
        """Test secure TCP communication with TLS."""
        # Create SSL contexts
        server_ssl_context = create_test_ssl_context()
        client_ssl_context = ssl.create_default_context()
        client_ssl_context.check_hostname = False
        client_ssl_context.verify_mode = ssl.CERT_NONE

        server_config = TransportConfig(
            security=SecurityConfig(ssl_context=server_ssl_context)
        )
        client_config = TransportConfig(
            security=SecurityConfig(ssl_context=client_ssl_context)
        )

        # Start secure TCP listener
        listener = TransportFactory.create_listener(
            "tcps", "127.0.0.1", tcp_port, server_config
        )
        await listener.start()

        try:
            # Create and connect secure client
            client = TransportFactory.create(
                f"tcps://127.0.0.1:{tcp_port}", client_config
            )

            # Test secure connection
            await client.connect()
            assert client.connected
            assert client.is_secure

            # Accept server connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Test secure communication
            test_message = b"Secure Hello, TCP World!"

            await client.send(test_message)
            received = await server_transport.receive()
            assert received == test_message

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_tcp_wire_protocol_compliance(self, tcp_port):
        """Test TCP wire protocol compliance with length-prefixed framing."""
        config = TransportConfig()

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Connect with raw TCP client to test wire protocol
            reader, writer = await asyncio.open_connection("127.0.0.1", tcp_port)

            # Accept server connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=1.0)

            # Test outgoing message format (client -> server)
            test_message = b"Wire protocol test"
            expected_length = len(test_message)

            # Send manually framed message
            header = struct.pack(">I", expected_length)
            writer.write(header + test_message)
            await writer.drain()

            # Verify server receives correct message
            received = await server_transport.receive()
            assert received == test_message

            # Test incoming message format (server -> client)
            response = b"Response message"
            await server_transport.send(response)

            # Read manually framed response
            header_bytes = await reader.readexactly(MESSAGE_HEADER_SIZE)
            message_length = struct.unpack(">I", header_bytes)[0]
            assert message_length == len(response)

            message_bytes = await reader.readexactly(message_length)
            assert message_bytes == response

            # Cleanup
            writer.close()
            await writer.wait_closed()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_tcp_ping_pong(self, tcp_port):
        """Test TCP ping/pong functionality."""
        config = TransportConfig(heartbeat_timeout=1.0)

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Create client connection
            client = TransportFactory.create(f"tcp://127.0.0.1:{tcp_port}", config)
            await client.connect()

            # Accept server connection in background
            async def server_ping_handler():
                server_transport = await asyncio.wait_for(
                    listener.accept(), timeout=2.0
                )
                try:
                    # Wait for ping and respond with pong
                    ping_msg = await asyncio.wait_for(
                        server_transport.receive(), timeout=2.0
                    )
                    if ping_msg == b"PING":
                        await asyncio.wait_for(
                            server_transport.send(b"PONG"), timeout=2.0
                        )
                finally:
                    await server_transport.disconnect()

            server_task = asyncio.create_task(server_ping_handler())

            # Test ping measurement
            rtt = await client.ping()
            assert isinstance(rtt, float)
            assert rtt > 0
            assert rtt < 1.0  # Should be fast locally

            await server_task
            await client.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_tcp_large_messages(self, tcp_port):
        """Test TCP handling of large messages within limits."""
        config = TransportConfig(
            protocol_options={"max_message_size": 100 * 1024}  # 100KB limit
        )

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Create client connection
            client = TransportFactory.create(f"tcp://127.0.0.1:{tcp_port}", config)
            await client.connect()

            # Accept server connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=1.0)

            # Test large message (50KB - within limit)
            large_message = b"X" * (50 * 1024)

            await client.send(large_message)
            received = await server_transport.receive()
            assert received == large_message

            # Test oversized message (150KB - over limit)
            oversized_message = b"Y" * (150 * 1024)

            with pytest.raises(TransportError, match="Message too large"):
                await client.send(oversized_message)

            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_tcp_connection_errors(self):
        """Test TCP connection error handling."""
        # Test connection to non-existent server
        config = TransportConfig(connect_timeout=2.0)
        client = TransportFactory.create(
            "tcp://127.0.0.1:1", config
        )  # Port 1 is privileged

        with pytest.raises(TransportConnectionError):
            await client.connect()

        # Test operations on disconnected transport
        with pytest.raises(TransportConnectionError):
            await client.send(b"test")

        with pytest.raises(TransportConnectionError):
            await client.receive()

        with pytest.raises(TransportConnectionError):
            await client.ping()

    @pytest.mark.asyncio
    async def test_tcp_timeout_errors(self, tcp_port):
        """Test TCP timeout error handling."""
        config = TransportConfig(
            read_timeout=2.0,
            write_timeout=2.0,
            heartbeat_timeout=2.0,
        )

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Create client connection
            client = TransportFactory.create(f"tcp://127.0.0.1:{tcp_port}", config)
            await client.connect()

            # Accept server connection in background (but don't respond to anything)
            async def silent_server():
                try:
                    server_transport = await asyncio.wait_for(
                        listener.accept(), timeout=5.0
                    )
                    # Just keep the connection open but don't send anything
                    await asyncio.sleep(
                        10.0
                    )  # Let client timeout operations complete with increased buffer
                    await server_transport.disconnect()
                except TimeoutError:
                    pass

            server_task = asyncio.create_task(silent_server())

            # Test read timeout (no data available)
            with pytest.raises(TransportTimeoutError):
                await client.receive()

            # Test ping timeout (no server response)
            with pytest.raises(TransportTimeoutError):
                await client.ping()

            await client.disconnect()
            await server_task

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_tcp_malformed_protocol(self, tcp_port):
        """Test TCP handling of malformed protocol messages."""
        config = TransportConfig()

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Connect with raw TCP client
            reader, writer = await asyncio.open_connection("127.0.0.1", tcp_port)

            # Accept server connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=1.0)

            # Send malformed header (incomplete)
            writer.write(b"\x00\x00")  # Only 2 bytes instead of 4
            await writer.drain()
            writer.close()

            # Server should get connection closed error
            with pytest.raises(TransportConnectionError):
                await server_transport.receive()

            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_tcp_listener_lifecycle(self, tcp_port):
        """Test TCP listener start/stop lifecycle."""
        config = TransportConfig()
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )

        # Test initial state
        assert not listener.listening
        assert listener.endpoint == f"tcp://127.0.0.1:{tcp_port}"

        # Test start
        await listener.start()
        assert listener.listening

        # Test double start (should be no-op)
        await listener.start()
        assert listener.listening

        # Test stop
        await listener.stop()
        assert not listener.listening

        # Test double stop (should be no-op)
        await listener.stop()
        assert not listener.listening

    @pytest.mark.asyncio
    async def test_tcp_context_managers(self, tcp_port):
        """Test TCP transport and listener context managers."""
        config = TransportConfig()

        # Test listener context manager
        async with TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        ) as listener:
            assert listener.listening

            # Test transport context manager
            async with TransportFactory.create(
                f"tcp://127.0.0.1:{tcp_port}", config
            ) as client:
                assert client.connected

                # Accept connection
                server_transport = await listener.accept()

                # Test communication
                await client.send(b"context test")
                received = await server_transport.receive()
                assert received == b"context test"

                await server_transport.disconnect()

        # Should be cleaned up automatically
        assert not listener.listening

    @pytest.mark.asyncio
    async def test_tcp_protocol_specifications(self):
        """Test TCP protocol specification retrieval."""
        # Test TCP specification
        tcp_spec = TransportFactory.get_protocol_spec("tcp")
        assert tcp_spec.name == "TCP"
        assert tcp_spec.message_framing == "length-prefixed"
        assert tcp_spec.supports_binary
        assert tcp_spec.connection_oriented
        assert "none" in tcp_spec.security_schemes

        # Test TCP secure specification
        tcps_spec = TransportFactory.get_protocol_spec("tcps")
        assert tcps_spec.name == "TCP Secure"
        assert tcps_spec.message_framing == "length-prefixed"
        assert "TLS" in tcps_spec.security_schemes

        # Test all specs
        all_specs = TransportFactory.get_all_protocol_specs()
        assert "tcp" in all_specs
        assert "tcps" in all_specs

        # Test external client examples
        assert "python" in tcp_spec.metadata["external_client_examples"]
        assert "go" in tcp_spec.metadata["external_client_examples"]
        assert "c" in tcp_spec.metadata["external_client_examples"]

    @pytest.mark.asyncio
    async def test_tcp_multiple_connections(self, tcp_port):
        """Test TCP listener handling multiple simultaneous connections."""
        config = TransportConfig()

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Create multiple client connections
            num_clients = 5
            clients = []
            server_transports = []

            for i in range(num_clients):
                client = TransportFactory.create(f"tcp://127.0.0.1:{tcp_port}", config)
                await client.connect()
                clients.append(client)

                # Accept server connection
                server_transport = await listener.accept()
                server_transports.append(server_transport)

            # Test communication on all connections
            for i, (client, server) in enumerate(zip(clients, server_transports)):
                message = f"Message from client {i}".encode()
                await client.send(message)
                received = await server.receive()
                assert received == message

            # Cleanup all connections
            for client in clients:
                await client.disconnect()
            for server in server_transports:
                await server.disconnect()

        finally:
            await listener.stop()


@pytest.fixture
async def tcp_port(port_allocator):
    """Allocate a TCP port for testing."""
    return port_allocator.allocate_port()


class TestTCPStreaming:
    """Test cases for TCP streaming functionality."""

    @pytest.mark.asyncio
    async def test_tcp_streaming_large_data(self, tcp_port):
        """Test TCP streaming for large data transfers."""
        config = TransportConfig(
            protocol_options={
                "max_message_size": 1024,  # 1KB normal limit
                "max_stream_size": 10 * 1024 * 1024,  # 10MB stream limit
            }
        )

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Create client connection
            client = TransportFactory.create(f"tcp://127.0.0.1:{tcp_port}", config)
            await client.connect()

            # Accept server connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=1.0)

            # Test large data that exceeds normal message size
            large_data = b"STREAMING_TEST_DATA" * 1000  # ~19KB data

            # Send via streaming (should work)
            await client.send_stream(large_data, chunk_size=1024)
            received = await server_transport.receive_stream()
            assert received == large_data

            # Verify normal send would fail for this size
            with pytest.raises(TransportError, match="Message too large"):
                await client.send(large_data)

            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_tcp_streaming_file_like_object(self, tcp_port):
        """Test TCP streaming with file-like objects."""
        import io

        config = TransportConfig()

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Create client connection
            client = TransportFactory.create(f"tcp://127.0.0.1:{tcp_port}", config)
            await client.connect()

            # Accept server connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=1.0)

            # Test with BytesIO file-like object
            file_data = b"File content for streaming test" * 100  # ~3KB
            file_obj = io.BytesIO(file_data)

            # Send file via streaming
            await client.send_stream(file_obj, chunk_size=512)
            received = await server_transport.receive_stream()
            assert received == file_data

            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_tcp_streaming_async_iterator(self, tcp_port):
        """Test TCP streaming with async iterators."""
        config = TransportConfig()

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Create client connection
            client = TransportFactory.create(f"tcp://127.0.0.1:{tcp_port}", config)
            await client.connect()

            # Accept server connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=1.0)

            # Create async iterator
            async def data_generator():
                for i in range(10):
                    yield f"Chunk {i}: " + "x" * 100
                    # Remove sleep to avoid timing issues in tests
                    # await asyncio.sleep(0.01)  # Simulate async data source

            # Send via streaming async iterator with timeout
            await asyncio.wait_for(client.send_stream(data_generator()), timeout=5.0)
            received = await asyncio.wait_for(
                server_transport.receive_stream(), timeout=5.0
            )

            # Verify data
            expected = "".join(f"Chunk {i}: " + "x" * 100 for i in range(10)).encode()
            assert received == expected

            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_tcp_streaming_size_limits(self, tcp_port):
        """Test TCP streaming size limits."""
        config = TransportConfig(
            protocol_options={
                "max_stream_size": 1024,  # 1KB stream limit
            }
        )

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Create client connection
            client = TransportFactory.create(f"tcp://127.0.0.1:{tcp_port}", config)
            await client.connect()

            # Accept server connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=1.0)

            # Test oversized stream
            oversized_data = b"X" * 2048  # 2KB - exceeds 1KB limit

            # Send oversized stream with timeout
            await asyncio.wait_for(
                client.send_stream(oversized_data, chunk_size=256), timeout=5.0
            )

            # Server should reject due to size limit
            with pytest.raises(TransportError, match="Stream too large"):
                await asyncio.wait_for(server_transport.receive_stream(), timeout=5.0)

            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_tcp_mixed_regular_and_streaming(self, tcp_port):
        """Test mixing regular messages and streaming in same connection."""
        config = TransportConfig()

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Create client connection
            client = TransportFactory.create(f"tcp://127.0.0.1:{tcp_port}", config)
            await client.connect()

            # Accept server connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=1.0)

            # Send regular message
            regular_msg = b"Regular message"
            await client.send(regular_msg)
            received1 = await server_transport.receive()
            assert received1 == regular_msg

            # Send streaming message
            stream_data = b"Streaming message" * 100
            await asyncio.wait_for(client.send_stream(stream_data), timeout=5.0)
            received2 = await asyncio.wait_for(
                server_transport.receive_stream(), timeout=5.0
            )
            assert received2 == stream_data

            # Send another regular message
            regular_msg2 = b"Another regular message"
            await asyncio.wait_for(client.send(regular_msg2), timeout=2.0)
            received3 = await asyncio.wait_for(server_transport.receive(), timeout=2.0)
            assert received3 == regular_msg2

            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()


class TestTCPPerformance:
    """Performance tests for TCP transport."""

    @pytest.mark.asyncio
    async def test_tcp_throughput(self, tcp_port):
        """Test TCP transport throughput."""
        config = TransportConfig()

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Create client connection
            client = TransportFactory.create(f"tcp://127.0.0.1:{tcp_port}", config)
            await client.connect()

            # Server echo handler
            async def echo_server():
                server_transport = await asyncio.wait_for(
                    listener.accept(), timeout=5.0
                )
                try:
                    for _ in range(100):  # Match the reduced client message count
                        data = await asyncio.wait_for(
                            server_transport.receive(), timeout=5.0
                        )
                        await asyncio.wait_for(server_transport.send(data), timeout=5.0)
                finally:
                    await server_transport.disconnect()

            server_task = asyncio.create_task(echo_server())

            # Send fewer messages with timeouts for parallel test compatibility
            import time

            start_time = time.time()

            test_message = b"Performance test message" * 10  # ~250 bytes
            num_messages = 100  # Reduced for parallel test execution

            for i in range(num_messages):
                await asyncio.wait_for(client.send(test_message), timeout=1.0)
                response = await asyncio.wait_for(client.receive(), timeout=1.0)
                assert response == test_message

            end_time = time.time()
            duration = end_time - start_time

            # Calculate throughput
            messages_per_second = num_messages / duration
            bytes_per_second = (
                len(test_message) * 2 * num_messages
            ) / duration  # Send + receive

            print(
                f"TCP throughput: {messages_per_second:.1f} msgs/sec, {bytes_per_second / 1024 / 1024:.1f} MB/sec"
            )

            # Basic performance expectations (very conservative)
            assert messages_per_second > 100  # At least 100 msgs/sec
            assert bytes_per_second > 10 * 1024  # At least 10 KB/sec

            await server_task
            await client.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_tcp_latency(self, tcp_port):
        """Test TCP transport latency."""
        config = TransportConfig()

        # Start TCP listener
        listener = TransportFactory.create_listener(
            "tcp", "127.0.0.1", tcp_port, config
        )
        await listener.start()

        try:
            # Create client connection
            client = TransportFactory.create(f"tcp://127.0.0.1:{tcp_port}", config)
            await client.connect()

            # Server ping responder
            async def ping_server():
                server_transport = await asyncio.wait_for(
                    listener.accept(), timeout=2.0
                )
                try:
                    while True:
                        data = await asyncio.wait_for(
                            server_transport.receive(), timeout=2.0
                        )
                        if data == b"PING":
                            await asyncio.wait_for(
                                server_transport.send(b"PONG"), timeout=2.0
                            )
                        else:
                            break
                except TimeoutError:
                    # Expected when test completes
                    pass
                finally:
                    await server_transport.disconnect()

            server_task = asyncio.create_task(ping_server())

            # Measure multiple ping latencies
            latencies = []
            for _ in range(10):
                rtt = await client.ping()
                latencies.append(rtt)

            avg_latency = sum(latencies) / len(latencies)
            max_latency = max(latencies)

            print(
                f"TCP latency: avg={avg_latency * 1000:.2f}ms, max={max_latency * 1000:.2f}ms"
            )

            # Basic latency expectations (very conservative for localhost)
            assert avg_latency < 0.1  # Less than 100ms average
            assert max_latency < 0.2  # Less than 200ms max

            # Stop ping server
            await client.send(b"STOP")
            await server_task
            await client.disconnect()

        finally:
            await listener.stop()
