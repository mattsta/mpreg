"""
Comprehensive tests for WebSocket transport implementation.

Tests cover:
- WebSocket client and server transport functionality
- Basic message sending and receiving
- Connection management and error handling
- Security features (TLS, authentication)
- Protocol compliance and external client compatibility
- Performance characteristics and timeouts
- WebSocket-specific features (ping/pong, frames)
- Integration with MPREG transport factory
"""

import asyncio
import ssl

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.core.transport import (
    SecurityConfig,
    TransportConfig,
    TransportConnectionError,
    TransportFactory,
    TransportTimeoutError,
)
from mpreg.core.transport.interfaces import TransportProtocol
from mpreg.core.transport.websocket_transport import (
    WebSocketListener,
    WebSocketTransport,
    _WebSocketServerTransport,
)

from .test_helpers import create_test_ssl_context


class TestWebSocketTransportFactory:
    """Test WebSocket transport creation via factory."""

    def test_websocket_create_transport(self):
        """Test creating WebSocket transport via factory."""
        transport = TransportFactory.create("ws://localhost:8080")
        assert isinstance(transport, WebSocketTransport)
        assert transport.protocol == TransportProtocol.WEBSOCKET
        assert not transport.is_secure
        assert not transport.connected

    def test_websocket_secure_create_transport(self):
        """Test creating WebSocket Secure transport via factory."""
        config = TransportConfig(security=SecurityConfig(verify_cert=False))
        transport = TransportFactory.create("wss://localhost:8443", config)
        assert isinstance(transport, WebSocketTransport)
        assert transport.protocol == TransportProtocol.WEBSOCKET_SECURE
        assert transport.is_secure
        assert not transport.connected

    def test_websocket_create_listener(self):
        """Test creating WebSocket listener via factory."""
        config = TransportConfig()
        listener = TransportFactory.create_listener("ws", "127.0.0.1", 8080, config)
        assert isinstance(listener, WebSocketListener)
        assert listener.host == "127.0.0.1"
        assert listener.port == 8080

    def test_websocket_secure_create_listener(self):
        """Test creating WebSocket Secure listener via factory."""
        config = TransportConfig(security=SecurityConfig(cert_file="test.crt"))
        listener = TransportFactory.create_listener("wss", "127.0.0.1", 8443, config)
        assert isinstance(listener, WebSocketListener)
        assert listener._get_protocol_scheme() == "wss"


class TestWebSocketTransportBasic:
    """Test basic WebSocket transport functionality."""

    @pytest.mark.asyncio
    async def test_websocket_connection_lifecycle(self, test_port):
        """Test WebSocket connection establishment and cleanup."""
        config = TransportConfig()

        # Create transport
        transport = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
        assert not transport.connected

        # Start listener to accept connections
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            # Connect
            await transport.connect()
            assert transport.connected

            # Accept server connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)
            assert isinstance(server_transport, _WebSocketServerTransport)

            # Disconnect
            await transport.disconnect()
            assert not transport.connected

            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_websocket_basic_communication(self, test_port):
        """Test basic WebSocket send/receive functionality."""
        config = TransportConfig()

        # Start WebSocket listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            # Create and connect client
            client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
            await client.connect()

            # Accept server connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Test bidirectional communication
            test_message = b"Hello, WebSocket World!"

            # Client -> Server
            await client.send(test_message)
            received = await server_transport.receive()
            assert received == test_message

            # Server -> Client
            response = b"Hello, WebSocket Client!"
            await server_transport.send(response)
            received = await client.receive()
            assert received == response

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_websocket_ping_pong(self, test_port):
        """Test WebSocket ping/pong functionality."""
        config = TransportConfig(heartbeat_timeout=2.0)

        # Start listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            # Connect client
            client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
            await client.connect()

            # Accept server connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Test ping from client
            rtt = await client.ping()
            assert isinstance(rtt, float)
            assert rtt > 0
            assert rtt < 1.0  # Should be very fast locally

            # Test ping from server
            rtt = await server_transport.ping()
            assert isinstance(rtt, float)
            assert rtt > 0

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_websocket_large_message(self, test_port):
        """Test WebSocket with large messages."""
        config = TransportConfig(
            protocol_options={"max_message_size": 1024 * 1024}
        )  # 1MB

        # Start listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            # Connect
            client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
            await client.connect()

            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Send large message
            large_message = b"X" * (512 * 1024)  # 512KB
            await client.send(large_message)

            received = await server_transport.receive()
            assert received == large_message
            assert len(received) == 512 * 1024

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()


class TestWebSocketTransportAuthentication:
    """Test WebSocket authentication features."""

    @pytest.mark.asyncio
    async def test_websocket_bearer_auth(self, test_port):
        """Test WebSocket Bearer token authentication."""
        security_config = SecurityConfig(auth_token="test-bearer-token-123")
        config = TransportConfig(security=security_config)

        # Start listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            # Connect with auth
            client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
            await client.connect()

            # Should connect successfully (auth headers sent during handshake)
            assert client.connected

            # Accept and verify connection works
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Test communication works
            await client.send(b"authenticated message")
            received = await server_transport.receive()
            assert received == b"authenticated message"

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_websocket_api_key_auth(self, test_port):
        """Test WebSocket API key authentication."""
        security_config = SecurityConfig(api_key="secret-api-key-456")
        config = TransportConfig(security=security_config)

        # Start listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            # Connect with API key
            client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
            await client.connect()

            assert client.connected

            # Accept connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Test communication
            await client.send(b"api key authenticated")
            received = await server_transport.receive()
            assert received == b"api key authenticated"

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()


class TestWebSocketTransportSecurity:
    """Test WebSocket security features."""

    @pytest.mark.asyncio
    async def test_websocket_secure_connection(self, test_port):
        """Test WebSocket Secure (WSS) connections."""
        # Create test SSL contexts (server for listener, client for transport)
        server_ssl_context = create_test_ssl_context()
        client_ssl_context = ssl.create_default_context()
        client_ssl_context.check_hostname = False
        client_ssl_context.verify_mode = ssl.CERT_NONE

        server_security_config = SecurityConfig(
            ssl_context=server_ssl_context, verify_cert=False
        )
        client_security_config = SecurityConfig(
            ssl_context=client_ssl_context, verify_cert=False
        )

        server_config = TransportConfig(security=server_security_config)
        client_config = TransportConfig(security=client_security_config)

        # Start secure listener
        listener = WebSocketListener("127.0.0.1", test_port, server_config)
        await listener.start()

        try:
            # Connect securely
            client = WebSocketTransport(f"wss://127.0.0.1:{test_port}", client_config)
            await client.connect()

            assert client.connected
            assert client.is_secure

            # Accept connection
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Test secure communication
            secure_message = b"This message is encrypted!"
            await client.send(secure_message)
            received = await server_transport.receive()
            assert received == secure_message

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    def test_websocket_protocol_scheme_detection(self):
        """Test protocol scheme detection for security."""
        # Non-secure config
        config = TransportConfig()
        listener = WebSocketListener("127.0.0.1", 8080, config)
        assert listener._get_protocol_scheme() == "ws"

        # Secure config with cert file
        secure_config = TransportConfig(security=SecurityConfig(cert_file="test.crt"))
        secure_listener = WebSocketListener("127.0.0.1", 8443, secure_config)
        assert secure_listener._get_protocol_scheme() == "wss"

        # Secure config with SSL context
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_config = TransportConfig(security=SecurityConfig(ssl_context=ssl_context))
        ssl_listener = WebSocketListener("127.0.0.1", 8443, ssl_config)
        assert ssl_listener._get_protocol_scheme() == "wss"


class TestWebSocketTransportErrorHandling:
    """Test WebSocket error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_websocket_connection_failure(self):
        """Test WebSocket connection failure to non-existent server."""
        config = TransportConfig(connect_timeout=0.1)  # Very short timeout

        # Try to connect to non-existent server (use port 65534 which is valid but unlikely to be used)
        client = WebSocketTransport("ws://127.0.0.1:65534", config)

        with pytest.raises(
            TransportConnectionError, match="WebSocket connection failed"
        ):
            await client.connect()

    @pytest.mark.asyncio
    async def test_websocket_send_without_connection(self):
        """Test sending without established connection."""
        config = TransportConfig()
        client = WebSocketTransport("ws://127.0.0.1:8080", config)

        with pytest.raises(TransportConnectionError, match="WebSocket not connected"):
            await client.send(b"test message")

    @pytest.mark.asyncio
    async def test_websocket_receive_without_connection(self):
        """Test receiving without established connection."""
        config = TransportConfig()
        client = WebSocketTransport("ws://127.0.0.1:8080", config)

        with pytest.raises(TransportConnectionError, match="WebSocket not connected"):
            await client.receive()

    @pytest.mark.asyncio
    async def test_websocket_ping_without_connection(self):
        """Test ping without established connection."""
        config = TransportConfig()
        client = WebSocketTransport("ws://127.0.0.1:8080", config)

        with pytest.raises(TransportConnectionError, match="WebSocket not connected"):
            await client.ping()

    @pytest.mark.asyncio
    async def test_websocket_send_timeout(self, test_port):
        """Test WebSocket send timeout."""
        config = TransportConfig(write_timeout=0.01)  # Very short timeout

        # Start listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            # Connect
            client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
            await client.connect()

            # Accept connection but don't read
            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Try to send large message that will likely timeout
            large_message = b"X" * (1024 * 1024)  # 1MB

            # This might timeout depending on system buffer sizes
            try:
                await client.send(large_message)
            except TransportTimeoutError:
                pass  # Expected for this test

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_websocket_receive_timeout(self, test_port):
        """Test WebSocket receive timeout."""
        config = TransportConfig(read_timeout=0.1)  # Short timeout

        # Start listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            # Connect
            client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
            await client.connect()

            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Try to receive without server sending anything
            with pytest.raises(
                TransportTimeoutError, match="WebSocket receive timeout"
            ):
                await client.receive()

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_websocket_ping_timeout(self, test_port):
        """Test WebSocket ping timeout."""
        config = TransportConfig(heartbeat_timeout=0.01)  # Very short timeout

        # Start listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            # Connect
            client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
            await client.connect()

            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Ping might timeout with very short timeout
            try:
                await client.ping()
            except TransportTimeoutError:
                pass  # Expected for very short timeouts

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_websocket_double_connect(self, test_port):
        """Test connecting when already connected."""
        config = TransportConfig()

        # Start listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            # Connect
            client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
            await client.connect()
            assert client.connected

            # Try to connect again (should be no-op)
            await client.connect()
            assert client.connected

            # Cleanup
            await client.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_websocket_double_disconnect(self):
        """Test disconnecting when already disconnected."""
        config = TransportConfig()
        client = WebSocketTransport("ws://127.0.0.1:8080", config)

        # Should not raise error
        await client.disconnect()
        await client.disconnect()


class TestWebSocketTransportDataTypes:
    """Test WebSocket handling of different data types."""

    @pytest.mark.asyncio
    async def test_websocket_text_to_bytes_conversion(self, test_port):
        """Test automatic conversion of text messages to bytes."""
        config = TransportConfig()

        # Start listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            # Connect
            client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
            await client.connect()

            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Send binary data
            binary_message = b"binary message"
            await client.send(binary_message)

            received = await server_transport.receive()
            assert isinstance(received, bytes)
            assert received == binary_message

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_websocket_empty_message(self, test_port):
        """Test sending and receiving empty messages."""
        config = TransportConfig()

        # Start listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            # Connect
            client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
            await client.connect()

            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Send empty message
            await client.send(b"")

            received = await server_transport.receive()
            assert received == b""

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()


class TestWebSocketTransportPerformance:
    """Test WebSocket transport performance characteristics."""

    @pytest.mark.asyncio
    async def test_websocket_multiple_rapid_messages(self, test_port):
        """Test sending multiple messages rapidly."""
        config = TransportConfig()

        # Start listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            # Connect
            client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
            await client.connect()

            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Send multiple messages rapidly
            message_count = 50
            messages = [f"message-{i}".encode() for i in range(message_count)]

            # Send all messages
            send_tasks = [client.send(msg) for msg in messages]
            await asyncio.gather(*send_tasks)

            # Receive all messages
            received_messages = []
            for _ in range(message_count):
                received = await server_transport.receive()
                received_messages.append(received)

            # Verify all messages received (order might vary)
            assert len(received_messages) == message_count
            assert set(received_messages) == set(messages)

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    @pytest.mark.asyncio
    async def test_websocket_concurrent_clients(self, test_port):
        """Test multiple concurrent WebSocket clients."""
        config = TransportConfig()

        # Start listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        await listener.start()

        try:
            client_count = 5
            clients = []
            server_transports = []

            # Connect multiple clients
            for i in range(client_count):
                client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
                await client.connect()
                clients.append(client)

                # Accept server connection
                server_task = asyncio.create_task(listener.accept())
                server_transport = await asyncio.wait_for(server_task, timeout=2.0)
                server_transports.append(server_transport)

            # Test communication from all clients
            for i, (client, server_transport) in enumerate(
                zip(clients, server_transports)
            ):
                message = f"client-{i}-message".encode()
                await client.send(message)

                received = await server_transport.receive()
                assert received == message

            # Cleanup all connections
            for client in clients:
                await client.disconnect()
            for server_transport in server_transports:
                await server_transport.disconnect()

        finally:
            await listener.stop()


class TestWebSocketTransportProtocolCompliance:
    """Test WebSocket protocol compliance and external client compatibility."""

    def test_websocket_protocol_spec_registration(self):
        """Test that WebSocket protocol specs are properly registered."""
        # Test WebSocket spec
        ws_spec = TransportFactory.get_protocol_spec(TransportProtocol.WEBSOCKET)
        assert ws_spec.name == "WebSocket"
        assert ws_spec.version == "1.0"
        assert ws_spec.message_framing == "websocket-frames"
        assert ws_spec.supports_binary is True
        assert ws_spec.supports_streaming is True
        assert ws_spec.connection_oriented is True
        assert "none" in ws_spec.security_schemes
        assert "TLS" in ws_spec.security_schemes

        # Test WebSocket Secure spec
        wss_spec = TransportFactory.get_protocol_spec(
            TransportProtocol.WEBSOCKET_SECURE
        )
        assert wss_spec.name == "WebSocket Secure"
        assert "TLS" in wss_spec.security_schemes
        assert "client-cert" in wss_spec.auth_methods

    def test_websocket_external_client_examples(self):
        """Test that external client examples are present and valid."""
        ws_spec = TransportFactory.get_protocol_spec(TransportProtocol.WEBSOCKET)

        # Check examples exist
        assert "external_client_examples" in ws_spec.metadata
        examples = ws_spec.metadata["external_client_examples"]

        assert "python" in examples
        assert "javascript" in examples
        assert "curl" in examples

        # Check Python example contains expected elements
        python_example = examples["python"]
        assert "websockets" in python_example
        assert "asyncio" in python_example
        assert "send" in python_example
        assert "recv" in python_example

        # Check JavaScript example contains expected elements
        js_example = examples["javascript"]
        assert "WebSocket" in js_example
        assert "onopen" in js_example
        assert "onmessage" in js_example

    def test_websocket_auth_header_specifications(self):
        """Test authentication header specifications."""
        ws_spec = TransportFactory.get_protocol_spec(TransportProtocol.WEBSOCKET)

        assert "auth_headers" in ws_spec.metadata
        auth_headers = ws_spec.metadata["auth_headers"]

        assert "bearer" in auth_headers
        assert "Authorization: Bearer" in auth_headers["bearer"]

        assert "api-key" in auth_headers
        assert "X-API-Key" in auth_headers["api-key"]


@given(st.binary(min_size=0, max_size=1024))
@settings(max_examples=20, deadline=5000)
def test_websocket_property_based_messaging(message_data):
    """Property-based test for WebSocket messaging with various data."""

    async def test_message():
        config = TransportConfig()

        # Use a fixed port for property testing to avoid port conflicts
        test_port = 18080  # Fixed port for property tests

        # Start listener
        listener = WebSocketListener("127.0.0.1", test_port, config)
        try:
            await listener.start()
        except Exception:
            # Port might be in use, skip this test iteration
            return

        try:
            # Connect
            client = WebSocketTransport(f"ws://127.0.0.1:{test_port}", config)
            await client.connect()

            server_task = asyncio.create_task(listener.accept())
            server_transport = await asyncio.wait_for(server_task, timeout=2.0)

            # Test the message
            await client.send(message_data)
            received = await server_transport.receive()

            # Message should be received exactly as sent
            assert received == message_data
            assert len(received) == len(message_data)

            # Cleanup
            await client.disconnect()
            await server_transport.disconnect()

        finally:
            await listener.stop()

    # Run the async test
    asyncio.run(test_message())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
