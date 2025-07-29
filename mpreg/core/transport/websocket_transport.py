"""
WebSocket transport implementation for MPREG.

This module implements WebSocket (ws://) and WebSocket Secure (wss://) transport
with a well-defined protocol specification for external client development.

Protocol Specification:
- Message Framing: WebSocket frames (RFC 6455)
- Message Format: Binary frames containing serialized data
- Max Message Size: Configurable (default: 1MB)
- Connection: Persistent bidirectional connection
- Security: Optional TLS/SSL for wss:// URLs
- Authentication: Via HTTP headers during handshake
"""

import asyncio
import time

from websockets.client import WebSocketClientProtocol, connect
from websockets.exceptions import ConnectionClosed, WebSocketException
from websockets.server import WebSocketServer, WebSocketServerProtocol, serve

from .defaults import DEFAULT_MAX_MESSAGE_SIZE
from .factory import ProtocolSpec, register_transport
from .interfaces import (
    TransportConfig,
    TransportConnectionError,
    TransportError,
    TransportInterface,
    TransportListener,
    TransportProtocol,
    TransportTimeoutError,
)


class WebSocketTransport(TransportInterface):
    """WebSocket transport implementation.

    Supports both ws:// and wss:// protocols with optional authentication
    and configurable timeouts.

    Protocol Details:
    ================

    Connection Establishment:
    1. Client opens WebSocket connection to ws://host:port or wss://host:port
    2. Optional authentication via HTTP headers:
       - Authorization: Bearer <token>
       - X-API-Key: <api-key>
    3. Server accepts connection and both sides can send/receive

    Message Format:
    - All messages are sent as WebSocket binary frames
    - Message content is application-defined (typically serialized JSON/MessagePack)
    - No additional framing required - WebSocket handles message boundaries

    Ping/Pong:
    - Uses WebSocket ping/pong frames for health checking
    - Automatic pong responses handled by WebSocket implementation
    - Custom ping() method measures round-trip time

    Error Handling:
    - Connection errors raise TransportConnectionError
    - Timeout errors raise TransportTimeoutError
    - Protocol errors raise TransportError

    Example External Client (Python):
    ```python
    import asyncio
    import websockets

    async def client():
        uri = "ws://localhost:6666"
        async with websockets.connect(uri) as websocket:
            # Send message
            await websocket.send(b"Hello, MPREG!")

            # Receive response
            response = await websocket.recv()
            print(f"Received: {response}")

    asyncio.run(client())
    ```

    Example External Client (JavaScript):
    ```javascript
    const ws = new WebSocket('ws://localhost:6666');

    ws.onopen = function() {
        // Send message as ArrayBuffer
        const message = new TextEncoder().encode("Hello, MPREG!");
        ws.send(message);
    };

    ws.onmessage = function(event) {
        console.log('Received:', event.data);
    };
    ```
    """

    def __init__(self, url: str, config: TransportConfig) -> None:
        """Initialize WebSocket transport.

        Args:
            url: WebSocket URL (ws:// or wss://)
            config: Transport configuration
        """
        super().__init__(url, config)
        self._websocket: WebSocketClientProtocol | None = None
        self._connect_lock = asyncio.Lock()

    async def connect(self) -> None:
        """Establish WebSocket connection."""
        async with self._connect_lock:
            if self._connected:
                return

            try:
                # Prepare connection parameters
                extra_headers = {}

                # Add authentication headers if configured
                if self.config.security.auth_token:
                    extra_headers["Authorization"] = (
                        f"Bearer {self.config.security.auth_token}"
                    )

                if self.config.security.api_key:
                    extra_headers["X-API-Key"] = self.config.security.api_key

                # Get SSL context for wss:// connections
                ssl_context = None
                if self.is_secure:
                    ssl_context = self.config.security.create_ssl_context()

                # Connect with timeout
                self._websocket = await asyncio.wait_for(
                    connect(
                        self.url,
                        ssl=ssl_context,
                        extra_headers=extra_headers,
                        max_size=self.config.get_max_message_size(),
                        read_limit=self.config.read_buffer_size,
                        write_limit=self.config.write_buffer_size,
                    ),
                    timeout=self.config.connect_timeout,
                )

                self._connected = True

            except TimeoutError:
                raise TransportTimeoutError(f"WebSocket connection timeout: {self.url}")
            except (OSError, WebSocketException) as e:
                raise TransportConnectionError(f"WebSocket connection failed: {e}")

    async def disconnect(self) -> None:
        """Close WebSocket connection."""
        if self._websocket and not self._websocket.closed:
            await self._websocket.close()

        self._websocket = None
        self._connected = False

    async def send(self, data: bytes) -> None:
        """Send data via WebSocket.

        Args:
            data: Raw bytes to send

        Raises:
            TransportConnectionError: If not connected
            TransportTimeoutError: If send times out
            TransportError: If send fails
        """
        if not self._connected or not self._websocket:
            raise TransportConnectionError("WebSocket not connected")

        try:
            if self.config.write_timeout:
                await asyncio.wait_for(
                    self._websocket.send(data),
                    timeout=self.config.write_timeout,
                )
            else:
                await self._websocket.send(data)

        except TimeoutError:
            raise TransportTimeoutError("WebSocket send timeout")
        except ConnectionClosed:
            self._connected = False
            raise TransportConnectionError("WebSocket connection closed")
        except WebSocketException as e:
            raise TransportError(f"WebSocket send error: {e}")

    async def receive(self) -> bytes:
        """Receive data from WebSocket.

        Returns:
            Raw bytes received

        Raises:
            TransportConnectionError: If not connected
            TransportTimeoutError: If receive times out
            TransportError: If receive fails
        """
        if not self._connected or not self._websocket:
            raise TransportConnectionError("WebSocket not connected")

        try:
            if self.config.read_timeout:
                data = await asyncio.wait_for(
                    self._websocket.recv(),
                    timeout=self.config.read_timeout,
                )
            else:
                data = await self._websocket.recv()

            # Ensure we return bytes
            if isinstance(data, str):
                return data.encode("utf-8")
            return data

        except TimeoutError:
            raise TransportTimeoutError("WebSocket receive timeout")
        except ConnectionClosed:
            self._connected = False
            raise TransportConnectionError("WebSocket connection closed")
        except WebSocketException as e:
            raise TransportError(f"WebSocket receive error: {e}")

    async def ping(self) -> float:
        """Send WebSocket ping and measure round-trip time.

        Returns:
            Round-trip time in seconds

        Raises:
            TransportConnectionError: If not connected
            TransportTimeoutError: If ping times out
        """
        if not self._connected or not self._websocket:
            raise TransportConnectionError("WebSocket not connected")

        try:
            start_time = time.time()

            pong_waiter = await self._websocket.ping()
            await asyncio.wait_for(
                pong_waiter,
                timeout=self.config.heartbeat_timeout,
            )

            return time.time() - start_time

        except TimeoutError:
            raise TransportTimeoutError("WebSocket ping timeout")
        except ConnectionClosed:
            self._connected = False
            raise TransportConnectionError("WebSocket connection closed")
        except WebSocketException as e:
            raise TransportError(f"WebSocket ping error: {e}")


class WebSocketListener(TransportListener):
    """WebSocket listener for accepting incoming connections."""

    def __init__(self, host: str, port: int, config: TransportConfig) -> None:
        """Initialize WebSocket listener.

        Args:
            host: Host address to bind to
            port: Port to listen on
            config: Transport configuration
        """
        super().__init__(host, port, config)
        self._server: WebSocketServer | None = None
        self._accept_queue: asyncio.Queue | None = None

    def _get_protocol_scheme(self) -> str:
        """Get protocol scheme for WebSocket listener."""
        # Determine if we should use wss:// based on SSL configuration
        if self.config.security.ssl_context or self.config.security.cert_file:
            return "wss"
        return "ws"

    async def start(self) -> None:
        """Start WebSocket listener.

        Raises:
            TransportError: If listener fails to start
        """
        if self._listening:
            return

        try:
            # Get SSL context for secure connections
            ssl_context = None
            if self.config.security.ssl_context or self.config.security.cert_file:
                ssl_context = self.config.security.create_ssl_context()

            # Create accept queue for incoming connections
            self._accept_queue = asyncio.Queue()

            # Start WebSocket server
            self._server = await serve(
                self._handle_connection,
                self.host,
                self.port,
                ssl=ssl_context,
                max_size=self.config.get_max_message_size(),
                read_limit=self.config.read_buffer_size,
                write_limit=self.config.write_buffer_size,
            )

            self._listening = True

        except (OSError, WebSocketException) as e:
            raise TransportError(f"WebSocket listener start failed: {e}")

    async def stop(self) -> None:
        """Stop WebSocket listener."""
        if not self._listening:
            return

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        self._accept_queue = None
        self._listening = False

    async def accept(self) -> TransportInterface:
        """Accept incoming WebSocket connection.

        Returns:
            WebSocket transport for the accepted connection

        Raises:
            TransportError: If accept fails
        """
        if not self._listening or not self._accept_queue:
            raise TransportError("WebSocket listener not started")

        try:
            # Wait for incoming connection
            websocket = await self._accept_queue.get()

            # Create transport wrapper
            transport = _WebSocketServerTransport(websocket, self.config)
            return transport

        except Exception as e:
            raise TransportError(f"WebSocket accept failed: {e}")

    async def _handle_connection(
        self, websocket: WebSocketServerProtocol, path: str
    ) -> None:
        """Handle incoming WebSocket connection."""
        if self._accept_queue:
            await self._accept_queue.put(websocket)

            # Keep connection alive until it's closed
            try:
                await websocket.wait_closed()
            except Exception:
                pass


class _WebSocketServerTransport(TransportInterface):
    """WebSocket transport wrapper for server-side connections."""

    def __init__(
        self, websocket: WebSocketServerProtocol, config: TransportConfig
    ) -> None:
        """Initialize server-side WebSocket transport.

        Args:
            websocket: WebSocket server protocol instance
            config: Transport configuration
        """
        super().__init__(websocket.remote_address[0], config)
        self._websocket = websocket
        self._connected = True

    async def connect(self) -> None:
        """No-op for server-side connection (already connected)."""
        pass

    async def disconnect(self) -> None:
        """Close server-side WebSocket connection."""
        if self._websocket and not self._websocket.closed:
            await self._websocket.close()

        self._connected = False

    async def send(self, data: bytes) -> None:
        """Send data via server-side WebSocket."""
        if not self._connected or self._websocket.closed:
            raise TransportConnectionError("WebSocket not connected")

        try:
            if self.config.write_timeout:
                await asyncio.wait_for(
                    self._websocket.send(data),
                    timeout=self.config.write_timeout,
                )
            else:
                await self._websocket.send(data)

        except TimeoutError:
            raise TransportTimeoutError("WebSocket send timeout")
        except ConnectionClosed:
            self._connected = False
            raise TransportConnectionError("WebSocket connection closed")
        except WebSocketException as e:
            raise TransportError(f"WebSocket send error: {e}")

    async def receive(self) -> bytes:
        """Receive data from server-side WebSocket."""
        if not self._connected or self._websocket.closed:
            raise TransportConnectionError("WebSocket not connected")

        try:
            if self.config.read_timeout:
                data = await asyncio.wait_for(
                    self._websocket.recv(),
                    timeout=self.config.read_timeout,
                )
            else:
                data = await self._websocket.recv()

            # Ensure we return bytes
            if isinstance(data, str):
                return data.encode("utf-8")
            return data

        except TimeoutError:
            raise TransportTimeoutError("WebSocket receive timeout")
        except ConnectionClosed:
            self._connected = False
            raise TransportConnectionError("WebSocket connection closed")
        except WebSocketException as e:
            raise TransportError(f"WebSocket receive error: {e}")

    async def ping(self) -> float:
        """Send WebSocket ping from server side."""
        if not self._connected or self._websocket.closed:
            raise TransportConnectionError("WebSocket not connected")

        try:
            start_time = time.time()

            pong_waiter = await self._websocket.ping()
            await asyncio.wait_for(
                pong_waiter,
                timeout=self.config.heartbeat_timeout,
            )

            return time.time() - start_time

        except TimeoutError:
            raise TransportTimeoutError("WebSocket ping timeout")
        except ConnectionClosed:
            self._connected = False
            raise TransportConnectionError("WebSocket connection closed")
        except WebSocketException as e:
            raise TransportError(f"WebSocket ping error: {e}")


# Protocol specifications for external client development
_WEBSOCKET_SPEC = ProtocolSpec(
    name="WebSocket",
    version="1.0",
    description="WebSocket transport for MPREG RPC and pub/sub systems",
    message_framing="websocket-frames",
    max_message_size=DEFAULT_MAX_MESSAGE_SIZE,
    supports_binary=True,
    supports_streaming=True,
    connection_oriented=True,
    security_schemes=["none", "TLS"],
    auth_methods=["none", "bearer", "api-key"],
    metadata={
        "rfc": "RFC 6455",
        "subprotocols": [],
        "extensions": [],
        "auth_headers": {
            "bearer": "Authorization: Bearer <token>",
            "api-key": "X-API-Key: <key>",
        },
        "ping_pong": "WebSocket ping/pong frames",
        "external_client_examples": {
            "python": """
import asyncio
import websockets

async def client():
    uri = "ws://localhost:6666"
    async with websockets.connect(uri) as websocket:
        # Send binary message
        await websocket.send(b"Hello, MPREG!")
        
        # Receive response
        response = await websocket.recv()
        print(f"Received: {response}")

asyncio.run(client())
""",
            "javascript": """
const ws = new WebSocket('ws://localhost:6666');

ws.onopen = function() {
    // Send message as ArrayBuffer
    const message = new TextEncoder().encode("Hello, MPREG!");
    ws.send(message);
};

ws.onmessage = function(event) {
    console.log('Received:', event.data);
};
""",
            "curl": """
# WebSocket connections require special tools, not curl
# Use websocat instead:
echo "Hello, MPREG!" | websocat ws://localhost:6666
""",
        },
    },
)

_WEBSOCKET_SECURE_SPEC = ProtocolSpec(
    name="WebSocket Secure",
    version="1.0",
    description="Secure WebSocket transport with TLS/SSL encryption",
    message_framing="websocket-frames",
    max_message_size=DEFAULT_MAX_MESSAGE_SIZE,
    supports_binary=True,
    supports_streaming=True,
    connection_oriented=True,
    security_schemes=["TLS"],
    auth_methods=["none", "bearer", "api-key", "client-cert"],
    metadata={
        "rfc": "RFC 6455 + RFC 8446 (TLS 1.3)",
        "tls_versions": ["1.2", "1.3"],
        "cipher_suites": "Modern secure ciphers only",
        "auth_headers": {
            "bearer": "Authorization: Bearer <token>",
            "api-key": "X-API-Key: <key>",
        },
        "external_client_examples": {
            "python": """
import asyncio
import websockets
import ssl

async def secure_client():
    # For production, use proper certificate verification
    ssl_context = ssl.create_default_context()
    
    uri = "wss://localhost:6667"
    async with websockets.connect(uri, ssl=ssl_context) as websocket:
        await websocket.send(b"Secure Hello!")
        response = await websocket.recv()
        print(f"Secure response: {response}")

asyncio.run(secure_client())
""",
        },
    },
)

# Register WebSocket transports
register_transport(
    TransportProtocol.WEBSOCKET,
    WebSocketTransport,
    WebSocketListener,
    _WEBSOCKET_SPEC,
)

register_transport(
    TransportProtocol.WEBSOCKET_SECURE,
    WebSocketTransport,  # Same implementation, different config
    WebSocketListener,  # Same implementation, different config
    _WEBSOCKET_SECURE_SPEC,
)
