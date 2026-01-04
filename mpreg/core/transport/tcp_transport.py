"""
TCP transport implementation for MPREG.

This module implements TCP and TCP+TLS transport with a well-defined wire protocol
specification for external client development.

Protocol Specification:
- Message Framing: Length-prefixed binary protocol
- Frame Format: [4-byte length][message data]
- Length Encoding: Big-endian uint32
- Max Message Size: Configurable (default: 1MB)
- Connection: Persistent bidirectional TCP connection
- Security: Optional TLS/SSL for tcps:// URLs
- Keepalive: TCP SO_KEEPALIVE and application-level pings
"""

from __future__ import annotations

import asyncio
import contextlib
import struct
import time
from collections.abc import AsyncIterable

from .defaults import DEFAULT_MAX_MESSAGE_SIZE
from .factory import ProtocolSpec, register_transport
from .interfaces import (
    FileReader,
    StreamData,
    TransportConfig,
    TransportConnectionError,
    TransportError,
    TransportInterface,
    TransportListener,
    TransportProtocol,
    TransportTimeoutError,
)

# Wire protocol constants
MESSAGE_HEADER_SIZE = 4  # 4 bytes for uint32 length
MAX_SINGLE_MESSAGE_SIZE = 0xFFFFFFFF  # 4GB theoretical max for uint32
STREAMING_MARKER = 0xFFFFFFFF  # Special length value indicating streaming mode
PING_MESSAGE = b"\x00\x00\x00\x00PING"
PONG_MESSAGE = b"\x00\x00\x00\x00PONG"

# Streaming protocol constants
STREAM_CHUNK_HEADER_SIZE = 8  # 4 bytes chunk length + 4 bytes flags
STREAM_FLAG_CONTINUE = 0x01  # More chunks follow
STREAM_FLAG_END = 0x02  # Final chunk
STREAM_FLAG_ERROR = 0x04  # Error in stream


class TCPTransport(TransportInterface):
    """TCP transport implementation.

    Supports both tcp:// and tcps:// protocols with length-prefixed binary framing.

    Wire Protocol Specification:
    ============================

    Message Framing:
    All messages are framed with a 4-byte length prefix followed by message data:

    [4-byte length (big-endian uint32)][message data]

    The length field contains the size of the message data only (not including the header).
    Maximum message size is configurable (default 1MB).

    Connection Establishment:
    1. Client opens TCP connection to host:port
    2. For tcps://, TLS handshake is performed
    3. Both sides can immediately start sending framed messages

    Ping/Pong Protocol:
    - Ping: 9 bytes total: [0x00, 0x00, 0x00, 0x04, 'P', 'I', 'N', 'G']
    - Pong: 9 bytes total: [0x00, 0x00, 0x00, 0x04, 'P', 'O', 'N', 'G']
    - Used for connection health checking and latency measurement

    Error Handling:
    - Connection errors raise TransportConnectionError
    - Timeout errors raise TransportTimeoutError
    - Protocol errors (malformed frames) raise TransportError

    External Client Examples:
    ========================

    Python Client:
    ```python
    import asyncio
    import struct

    async def tcp_client():
        reader, writer = await asyncio.open_connection('localhost', <port>)

        # Send message
        message = b"Hello, MPREG!"
        length = struct.pack('>I', len(message))
        writer.write(length + message)
        await writer.drain()

        # Receive response
        header = await reader.readexactly(4)
        msg_len = struct.unpack('>I', header)[0]
        response = await reader.readexactly(msg_len)
        print(f"Received: {response}")

        writer.close()
        await writer.wait_closed()

    asyncio.run(tcp_client())
    ```

    Go Client:
    ```go
    package main

    import (
        "encoding/binary"
        "fmt"
        "net"
    )

    func main() {
        conn, err := net.Dial("tcp", "localhost:<port>")
        if err != nil {
            panic(err)
        }
        defer conn.Close()

        // Send message
        message := []byte("Hello, MPREG!")
        length := make([]byte, 4)
        binary.BigEndian.PutUint32(length, uint32(len(message)))

        conn.Write(length)
        conn.Write(message)

        // Receive response
        header := make([]byte, 4)
        conn.Read(header)
        msgLen := binary.BigEndian.Uint32(header)

        response := make([]byte, msgLen)
        conn.Read(response)
        fmt.Printf("Received: %s\\n", response)
    }
    ```

    C Client:
    ```c
    #include <stdio.h>
    #include <stdlib.h>
    #include <string.h>
    #include <unistd.h>
    #include <arpa/inet.h>

    int main() {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in server = {0};
        server.sin_family = AF_INET;
        server.sin_port = htons(<port>);
        inet_pton(AF_INET, "127.0.0.1", &server.sin_addr);

        connect(sock, (struct sockaddr*)&server, sizeof(server));

        // Send message
        char* message = "Hello, MPREG!";
        uint32_t length = htonl(strlen(message));

        send(sock, &length, 4, 0);
        send(sock, message, strlen(message), 0);

        // Receive response
        uint32_t response_length;
        recv(sock, &response_length, 4, 0);
        response_length = ntohl(response_length);

        char* response = malloc(response_length + 1);
        recv(sock, response, response_length, 0);
        response[response_length] = '\\0';

        printf("Received: %s\\n", response);

        free(response);
        close(sock);
        return 0;
    }
    ```
    """

    def __init__(self, url: str, config: TransportConfig) -> None:
        """Initialize TCP transport.

        Args:
            url: TCP URL (tcp:// or tcps://)
            config: Transport configuration
        """
        super().__init__(url, config)
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connect_lock = asyncio.Lock()

    async def connect(self) -> None:
        """Establish TCP connection."""
        async with self._connect_lock:
            if self._connected:
                return

            try:
                host = self.parsed_url.hostname
                port = self.parsed_url.port

                if not host or not port:
                    raise TransportConnectionError(f"Invalid TCP URL: {self.url}")

                # Get SSL context for secure connections
                ssl_context = None
                if self.is_secure:
                    ssl_context = self.config.security.create_ssl_context()

                # Connect with timeout
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(
                        host,
                        port,
                        ssl=ssl_context,
                        limit=self.config.read_buffer_size,
                    ),
                    timeout=self.config.connect_timeout,
                )

                # Configure TCP socket options
                if self._writer and self._writer.transport:
                    sock = self._writer.transport.get_extra_info("socket")
                    if sock:
                        # Enable TCP keepalive
                        sock.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)

                        # Platform-specific keepalive options
                        try:
                            import socket

                            if hasattr(socket, "TCP_KEEPIDLE"):
                                sock.setsockopt(
                                    socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 300
                                )  # 5 min
                            if hasattr(socket, "TCP_KEEPINTVL"):
                                sock.setsockopt(
                                    socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30
                                )  # 30 sec
                            if hasattr(socket, "TCP_KEEPCNT"):
                                sock.setsockopt(
                                    socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3
                                )  # 3 attempts
                        except (OSError, AttributeError):
                            # Ignore if platform doesn't support these options
                            pass

                self._connected = True

            except TimeoutError:
                raise TransportTimeoutError(f"TCP connection timeout: {self.url}")
            except (OSError, ConnectionError) as e:
                raise TransportConnectionError(f"TCP connection failed: {e}")

    async def disconnect(self) -> None:
        """Close TCP connection."""
        if self._writer:
            self._writer.close()
            with contextlib.suppress(Exception):
                await self._writer.wait_closed()

        self._reader = None
        self._writer = None
        self._connected = False

    async def send(self, data: bytes) -> None:
        """Send data via TCP with length framing.

        Args:
            data: Raw bytes to send

        Raises:
            TransportConnectionError: If not connected
            TransportTimeoutError: If send times out
            TransportError: If send fails or data too large
        """
        if not self._connected or not self._writer:
            raise TransportConnectionError("TCP not connected")

        # Check message size limit
        max_size = self.config.get_max_message_size()
        if len(data) > max_size:
            raise TransportError(f"Message too large: {len(data)} > {max_size}")

        try:
            # Create length-prefixed message
            header = struct.pack(">I", len(data))
            message = header + data

            # Send with optional timeout
            self._writer.write(message)

            if self.config.write_timeout:
                await asyncio.wait_for(
                    self._writer.drain(),
                    timeout=self.config.write_timeout,
                )
            else:
                await self._writer.drain()

        except TimeoutError:
            raise TransportTimeoutError("TCP send timeout")
        except (ConnectionError, BrokenPipeError):
            self._connected = False
            raise TransportConnectionError("TCP connection closed")
        except Exception as e:
            raise TransportError(f"TCP send error: {e}")

    async def receive(self) -> bytes:
        """Receive data from TCP with length framing.

        Returns:
            Raw bytes received

        Raises:
            TransportConnectionError: If not connected
            TransportTimeoutError: If receive times out
            TransportError: If receive fails or protocol error
        """
        if not self._connected or not self._reader:
            raise TransportConnectionError("TCP not connected")

        try:
            # Read message length header
            if self.config.read_timeout:
                header = await asyncio.wait_for(
                    self._reader.readexactly(MESSAGE_HEADER_SIZE),
                    timeout=self.config.read_timeout,
                )
            else:
                header = await self._reader.readexactly(MESSAGE_HEADER_SIZE)

            # Decode message length
            message_length = struct.unpack(">I", header)[0]

            # Validate message size
            max_size = self.config.get_max_message_size()
            if message_length > max_size:
                raise TransportError(
                    f"Message too large: {message_length} > {max_size}"
                )

            # Read message data
            if self.config.read_timeout:
                data = await asyncio.wait_for(
                    self._reader.readexactly(message_length),
                    timeout=self.config.read_timeout,
                )
            else:
                data = await self._reader.readexactly(message_length)

            return data

        except TimeoutError:
            raise TransportTimeoutError("TCP receive timeout")
        except asyncio.IncompleteReadError:
            self._connected = False
            raise TransportConnectionError("TCP connection closed")
        except struct.error as e:
            raise TransportError(f"TCP protocol error: invalid header: {e}")
        except Exception as e:
            raise TransportError(f"TCP receive error: {e}")

    async def send_stream(
        self, data_stream: StreamData, chunk_size: int = 64 * 1024
    ) -> None:
        """Send large data as a stream of chunks.

        This method allows sending data larger than the configured max_message_size
        by breaking it into smaller chunks with a streaming protocol extension.

        Wire Format for Streaming:
        1. Initial message: [0xFFFFFFFF] (STREAMING_MARKER)
        2. Chunk format: [4-byte chunk_length][4-byte flags][chunk_data]
        3. Final chunk: flags include STREAM_FLAG_END

        Args:
            data_stream: Bytes data, file-like object, or async iterator
            chunk_size: Size of each chunk (default 64KB)

        Raises:
            TransportConnectionError: If not connected
            TransportTimeoutError: If send times out
            TransportError: If send fails
        """
        if not self._connected or not self._writer:
            raise TransportConnectionError("TCP not connected")

        try:
            # Send streaming marker
            streaming_header = struct.pack(">I", STREAMING_MARKER)
            self._writer.write(streaming_header)

            # Handle different data source types
            if isinstance(data_stream, FileReader):
                # File-like object
                await self._send_file_stream(data_stream, chunk_size)
            elif isinstance(data_stream, AsyncIterable):
                # Async iterator
                await self._send_async_iterator_stream(data_stream)
            else:
                # Assume bytes
                await self._send_bytes_stream(data_stream, chunk_size)

            # Ensure all data is sent
            if self.config.write_timeout:
                await asyncio.wait_for(
                    self._writer.drain(),
                    timeout=self.config.write_timeout,
                )
            else:
                await self._writer.drain()

        except TimeoutError:
            raise TransportTimeoutError("TCP stream send timeout")
        except (ConnectionError, BrokenPipeError):
            self._connected = False
            raise TransportConnectionError("TCP connection closed")
        except Exception as e:
            # Send error chunk to notify receiver
            try:
                error_chunk = struct.pack(">II", 0, STREAM_FLAG_ERROR)
                self._writer.write(error_chunk)
                await self._writer.drain()
            except (OSError, ConnectionError, asyncio.CancelledError):
                # Expected during connection failures or cancellation - safe to ignore
                pass
            raise TransportError(f"TCP stream send error: {e}")

    async def _send_bytes_stream(self, data: bytes, chunk_size: int) -> None:
        """Send bytes data as stream chunks."""
        if not self._writer:
            raise TransportConnectionError("Transport is not connected")

        offset = 0
        total_size = len(data)

        while offset < total_size:
            # Calculate chunk size
            remaining = total_size - offset
            current_chunk_size = min(chunk_size, remaining)

            # Determine flags
            flags = STREAM_FLAG_CONTINUE
            if offset + current_chunk_size >= total_size:
                flags = STREAM_FLAG_END

            # Create chunk
            chunk_data = data[offset : offset + current_chunk_size]
            chunk_header = struct.pack(">II", current_chunk_size, flags)

            # Send chunk
            self._writer.write(chunk_header + chunk_data)
            offset += current_chunk_size

    async def _send_file_stream(self, file_obj, chunk_size: int) -> None:
        """Send file-like object as stream chunks."""
        if not self._writer:
            raise TransportConnectionError("Transport is not connected")

        while True:
            chunk_data = file_obj.read(chunk_size)
            if not chunk_data:
                # End of file - send final empty chunk
                final_chunk = struct.pack(">II", 0, STREAM_FLAG_END)
                self._writer.write(final_chunk)
                break

            # Send chunk with continue flag
            chunk_header = struct.pack(">II", len(chunk_data), STREAM_FLAG_CONTINUE)
            self._writer.write(chunk_header + chunk_data)

        # If we read less than chunk_size, it's the final chunk
        # This is handled by the empty chunk above

    async def _send_async_iterator_stream(self, async_iter) -> None:
        """Send async iterator as stream chunks."""
        if not self._writer:
            raise TransportConnectionError("Transport is not connected")

        try:
            async for chunk_data in async_iter:
                if isinstance(chunk_data, str):
                    chunk_data = chunk_data.encode("utf-8")

                # Send chunk with continue flag (we don't know if it's the last)
                chunk_header = struct.pack(">II", len(chunk_data), STREAM_FLAG_CONTINUE)
                self._writer.write(chunk_header + chunk_data)

            # Send final empty chunk to indicate end
            final_chunk = struct.pack(">II", 0, STREAM_FLAG_END)
            self._writer.write(final_chunk)

        except Exception:
            # Send error chunk
            error_chunk = struct.pack(">II", 0, STREAM_FLAG_ERROR)
            self._writer.write(error_chunk)
            raise

    async def receive_stream(self) -> bytes:
        """Receive streamed data and reassemble into complete message.

        This method handles the streaming protocol extension to receive
        data that was sent via send_stream().

        Returns:
            Complete reassembled message bytes

        Raises:
            TransportConnectionError: If not connected
            TransportTimeoutError: If receive times out
            TransportError: If receive fails or stream error
        """
        if not self._connected or not self._reader:
            raise TransportConnectionError("TCP not connected")

        try:
            # First, read the regular message header
            if self.config.read_timeout:
                header = await asyncio.wait_for(
                    self._reader.readexactly(MESSAGE_HEADER_SIZE),
                    timeout=self.config.read_timeout,
                )
            else:
                header = await self._reader.readexactly(MESSAGE_HEADER_SIZE)

            message_length = struct.unpack(">I", header)[0]

            # Check if this is a streaming message
            if message_length == STREAMING_MARKER:
                return await self._receive_stream_chunks()
            else:
                # Regular message - read the data
                if self.config.read_timeout:
                    data = await asyncio.wait_for(
                        self._reader.readexactly(message_length),
                        timeout=self.config.read_timeout,
                    )
                else:
                    data = await self._reader.readexactly(message_length)
                return data

        except TimeoutError:
            raise TransportTimeoutError("TCP stream receive timeout")
        except asyncio.IncompleteReadError:
            self._connected = False
            raise TransportConnectionError("TCP connection closed")
        except Exception as e:
            raise TransportError(f"TCP stream receive error: {e}")

    async def _receive_stream_chunks(self) -> bytes:
        """Receive and reassemble streaming chunks."""
        if not self._reader:
            raise TransportConnectionError("Transport is not connected")

        chunks = []
        total_size = 0

        while True:
            # Read chunk header
            if self.config.read_timeout:
                chunk_header = await asyncio.wait_for(
                    self._reader.readexactly(STREAM_CHUNK_HEADER_SIZE),
                    timeout=self.config.read_timeout,
                )
            else:
                chunk_header = await self._reader.readexactly(STREAM_CHUNK_HEADER_SIZE)

            chunk_length, flags = struct.unpack(">II", chunk_header)

            # Check for error flag
            if flags & STREAM_FLAG_ERROR:
                raise TransportError("Stream error received from sender")

            # Read chunk data if any
            if chunk_length > 0:
                if self.config.read_timeout:
                    chunk_data = await asyncio.wait_for(
                        self._reader.readexactly(chunk_length),
                        timeout=self.config.read_timeout,
                    )
                else:
                    chunk_data = await self._reader.readexactly(chunk_length)

                chunks.append(chunk_data)
                total_size += chunk_length

                # Check for reasonable total size limit
                max_stream_size = self.config.protocol_options.get(
                    "max_stream_size",
                    100 * 1024 * 1024,  # 100MB default
                )
                if total_size > max_stream_size:
                    raise TransportError(
                        f"Stream too large: {total_size} > {max_stream_size}"
                    )

            # Check if this is the final chunk
            if flags & STREAM_FLAG_END:
                break

        # Reassemble all chunks
        return b"".join(chunks)

    async def ping(self) -> float:
        """Send TCP ping and measure round-trip time.

        Uses a special ping/pong protocol over the TCP connection.

        Returns:
            Round-trip time in seconds

        Raises:
            TransportConnectionError: If not connected
            TransportTimeoutError: If ping times out
        """
        if not self._connected:
            raise TransportConnectionError("TCP not connected")

        try:
            start_time = time.time()

            # Send ping message
            await self.send(b"PING")

            # Wait for pong response with timeout
            response = await asyncio.wait_for(
                self.receive(),
                timeout=self.config.heartbeat_timeout,
            )

            if response != b"PONG":
                raise TransportError(f"Invalid ping response: {response!r}")

            return time.time() - start_time

        except TimeoutError:
            raise TransportTimeoutError("TCP ping timeout")
        except TransportError:
            raise
        except Exception as e:
            raise TransportError(f"TCP ping error: {e}")


class TCPListener(TransportListener):
    """TCP listener for accepting incoming connections."""

    def __init__(self, host: str, port: int, config: TransportConfig) -> None:
        """Initialize TCP listener.

        Args:
            host: Host address to bind to
            port: Port to listen on
            config: Transport configuration
        """
        super().__init__(host, port, config)
        self._server: asyncio.Server | None = None
        self._accept_queue: asyncio.Queue | None = None

    def _get_protocol_scheme(self) -> str:
        """Get protocol scheme for TCP listener."""
        # Determine if we should use tcps:// based on SSL configuration
        if self.config.security.ssl_context or self.config.security.cert_file:
            return "tcps"
        return "tcp"

    async def start(self) -> None:
        """Start TCP listener.

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

            # Start TCP server
            self._server = await asyncio.start_server(
                self._handle_connection,
                self.host,
                self.port,
                ssl=ssl_context,
                reuse_address=True,
                reuse_port=True,
                limit=self.config.read_buffer_size,
            )

            self._listening = True

        except (OSError, ConnectionError) as e:
            raise TransportError(f"TCP listener start failed: {e}")

    async def stop(self) -> None:
        """Stop TCP listener."""
        if not self._listening:
            return

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        self._accept_queue = None
        self._listening = False

    async def accept(self) -> TransportInterface:
        """Accept incoming TCP connection.

        Returns:
            TCP transport for the accepted connection

        Raises:
            TransportError: If accept fails
        """
        if not self._listening or not self._accept_queue:
            raise TransportError("TCP listener not started")

        try:
            # Wait for incoming connection
            reader, writer = await self._accept_queue.get()

            # Create transport wrapper
            transport = _TCPServerTransport(reader, writer, self.config)
            return transport

        except Exception as e:
            raise TransportError(f"TCP accept failed: {e}")

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle incoming TCP connection."""
        if self._accept_queue:
            await self._accept_queue.put((reader, writer))


class _TCPServerTransport(TransportInterface):
    """TCP transport wrapper for server-side connections."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        config: TransportConfig,
    ) -> None:
        """Initialize server-side TCP transport.

        Args:
            reader: TCP stream reader
            writer: TCP stream writer
            config: Transport configuration
        """
        # Get remote address for URL
        peername = writer.get_extra_info("peername")
        remote_host = peername[0] if peername else "unknown"

        super().__init__(f"tcp://{remote_host}", config)
        self._reader = reader
        self._writer = writer
        self._connected = True

        # Configure TCP socket options
        if self._writer.transport:
            sock = self._writer.transport.get_extra_info("socket")
            if sock:
                try:
                    import socket

                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                except (OSError, AttributeError):
                    pass

    async def connect(self) -> None:
        """No-op for server-side connection (already connected)."""
        pass

    async def disconnect(self) -> None:
        """Close server-side TCP connection."""
        if self._writer:
            self._writer.close()
            with contextlib.suppress(Exception):
                await self._writer.wait_closed()

        self._connected = False

    async def send(self, data: bytes) -> None:
        """Send data via server-side TCP."""
        if not self._connected or not self._writer:
            raise TransportConnectionError("TCP not connected")

        # Check message size limit
        max_size = self.config.get_max_message_size()
        if len(data) > max_size:
            raise TransportError(f"Message too large: {len(data)} > {max_size}")

        try:
            # Create length-prefixed message
            header = struct.pack(">I", len(data))
            message = header + data

            # Send with optional timeout
            self._writer.write(message)

            if self.config.write_timeout:
                await asyncio.wait_for(
                    self._writer.drain(),
                    timeout=self.config.write_timeout,
                )
            else:
                await self._writer.drain()

        except TimeoutError:
            raise TransportTimeoutError("TCP send timeout")
        except (ConnectionError, BrokenPipeError):
            self._connected = False
            raise TransportConnectionError("TCP connection closed")
        except Exception as e:
            raise TransportError(f"TCP send error: {e}")

    async def receive(self) -> bytes:
        """Receive data from server-side TCP."""
        if not self._connected or not self._reader:
            raise TransportConnectionError("TCP not connected")

        try:
            # Read message length header
            if self.config.read_timeout:
                header = await asyncio.wait_for(
                    self._reader.readexactly(MESSAGE_HEADER_SIZE),
                    timeout=self.config.read_timeout,
                )
            else:
                header = await self._reader.readexactly(MESSAGE_HEADER_SIZE)

            # Decode message length
            message_length = struct.unpack(">I", header)[0]

            # Validate message size
            max_size = self.config.get_max_message_size()
            if message_length > max_size:
                raise TransportError(
                    f"Message too large: {message_length} > {max_size}"
                )

            # Read message data
            if self.config.read_timeout:
                data = await asyncio.wait_for(
                    self._reader.readexactly(message_length),
                    timeout=self.config.read_timeout,
                )
            else:
                data = await self._reader.readexactly(message_length)

            return data

        except TimeoutError:
            raise TransportTimeoutError("TCP receive timeout")
        except asyncio.IncompleteReadError:
            self._connected = False
            raise TransportConnectionError("TCP connection closed")
        except struct.error as e:
            raise TransportError(f"TCP protocol error: invalid header: {e}")
        except Exception as e:
            raise TransportError(f"TCP receive error: {e}")

    async def ping(self) -> float:
        """Send TCP ping from server side."""
        if not self._connected:
            raise TransportConnectionError("TCP not connected")

        try:
            start_time = time.time()

            # Send ping message
            await self.send(b"PING")

            # Wait for pong response with timeout
            response = await asyncio.wait_for(
                self.receive(),
                timeout=self.config.heartbeat_timeout,
            )

            if response != b"PONG":
                raise TransportError(f"Invalid ping response: {response!r}")

            return time.time() - start_time

        except TimeoutError:
            raise TransportTimeoutError("TCP ping timeout")
        except TransportError:
            raise
        except Exception as e:
            raise TransportError(f"TCP ping error: {e}")

    async def send_stream(
        self, data_stream: StreamData, chunk_size: int = 64 * 1024
    ) -> None:
        """Send large data as a stream of chunks (server-side)."""
        if not self._connected or not self._writer:
            raise TransportConnectionError("TCP not connected")

        try:
            # Send streaming marker
            streaming_header = struct.pack(">I", STREAMING_MARKER)
            self._writer.write(streaming_header)

            # Handle different data source types
            if isinstance(data_stream, FileReader):
                # File-like object
                await self._send_file_stream(data_stream, chunk_size)
            elif isinstance(data_stream, AsyncIterable):
                # Async iterator
                await self._send_async_iterator_stream(data_stream)
            else:
                # Assume bytes
                await self._send_bytes_stream(data_stream, chunk_size)

            # Ensure all data is sent
            if self.config.write_timeout:
                await asyncio.wait_for(
                    self._writer.drain(),
                    timeout=self.config.write_timeout,
                )
            else:
                await self._writer.drain()

        except TimeoutError:
            raise TransportTimeoutError("TCP stream send timeout")
        except (ConnectionError, BrokenPipeError):
            self._connected = False
            raise TransportConnectionError("TCP connection closed")
        except Exception as e:
            # Send error chunk to notify receiver
            try:
                error_chunk = struct.pack(">II", 0, STREAM_FLAG_ERROR)
                self._writer.write(error_chunk)
                await self._writer.drain()
            except (OSError, ConnectionError, asyncio.CancelledError):
                # Expected during connection failures or cancellation - safe to ignore
                pass
            raise TransportError(f"TCP stream send error: {e}")

    async def _send_bytes_stream(self, data: bytes, chunk_size: int) -> None:
        """Send bytes data as stream chunks (server-side)."""
        offset = 0
        total_size = len(data)

        while offset < total_size:
            # Calculate chunk size
            remaining = total_size - offset
            current_chunk_size = min(chunk_size, remaining)

            # Determine flags
            flags = STREAM_FLAG_CONTINUE
            if offset + current_chunk_size >= total_size:
                flags = STREAM_FLAG_END

            # Create chunk
            chunk_data = data[offset : offset + current_chunk_size]
            chunk_header = struct.pack(">II", current_chunk_size, flags)

            # Send chunk
            self._writer.write(chunk_header + chunk_data)
            offset += current_chunk_size

    async def _send_file_stream(self, file_obj, chunk_size: int) -> None:
        """Send file-like object as stream chunks (server-side)."""
        while True:
            chunk_data = file_obj.read(chunk_size)
            if not chunk_data:
                # End of file - send final empty chunk
                final_chunk = struct.pack(">II", 0, STREAM_FLAG_END)
                self._writer.write(final_chunk)
                break

            # Send chunk with continue flag
            chunk_header = struct.pack(">II", len(chunk_data), STREAM_FLAG_CONTINUE)
            self._writer.write(chunk_header + chunk_data)

    async def _send_async_iterator_stream(self, async_iter) -> None:
        """Send async iterator as stream chunks (server-side)."""
        try:
            async for chunk_data in async_iter:
                if isinstance(chunk_data, str):
                    chunk_data = chunk_data.encode("utf-8")

                # Send chunk with continue flag (we don't know if it's the last)
                chunk_header = struct.pack(">II", len(chunk_data), STREAM_FLAG_CONTINUE)
                self._writer.write(chunk_header + chunk_data)

            # Send final empty chunk to indicate end
            final_chunk = struct.pack(">II", 0, STREAM_FLAG_END)
            self._writer.write(final_chunk)

        except Exception:
            # Send error chunk
            error_chunk = struct.pack(">II", 0, STREAM_FLAG_ERROR)
            self._writer.write(error_chunk)
            raise

    async def receive_stream(self) -> bytes:
        """Receive streamed data and reassemble into complete message (server-side)."""
        if not self._connected or not self._reader:
            raise TransportConnectionError("TCP not connected")

        try:
            # First, read the regular message header
            if self.config.read_timeout:
                header = await asyncio.wait_for(
                    self._reader.readexactly(MESSAGE_HEADER_SIZE),
                    timeout=self.config.read_timeout,
                )
            else:
                header = await self._reader.readexactly(MESSAGE_HEADER_SIZE)

            message_length = struct.unpack(">I", header)[0]

            # Check if this is a streaming message
            if message_length == STREAMING_MARKER:
                return await self._receive_stream_chunks()
            else:
                # Regular message - read the data
                if self.config.read_timeout:
                    data = await asyncio.wait_for(
                        self._reader.readexactly(message_length),
                        timeout=self.config.read_timeout,
                    )
                else:
                    data = await self._reader.readexactly(message_length)
                return data

        except TimeoutError:
            raise TransportTimeoutError("TCP stream receive timeout")
        except asyncio.IncompleteReadError:
            self._connected = False
            raise TransportConnectionError("TCP connection closed")
        except Exception as e:
            raise TransportError(f"TCP stream receive error: {e}")

    async def _receive_stream_chunks(self) -> bytes:
        """Receive and reassemble streaming chunks (server-side)."""
        chunks = []
        total_size = 0

        while True:
            # Read chunk header
            if self.config.read_timeout:
                chunk_header = await asyncio.wait_for(
                    self._reader.readexactly(STREAM_CHUNK_HEADER_SIZE),
                    timeout=self.config.read_timeout,
                )
            else:
                chunk_header = await self._reader.readexactly(STREAM_CHUNK_HEADER_SIZE)

            chunk_length, flags = struct.unpack(">II", chunk_header)

            # Check for error flag
            if flags & STREAM_FLAG_ERROR:
                raise TransportError("Stream error received from sender")

            # Read chunk data if any
            if chunk_length > 0:
                if self.config.read_timeout:
                    chunk_data = await asyncio.wait_for(
                        self._reader.readexactly(chunk_length),
                        timeout=self.config.read_timeout,
                    )
                else:
                    chunk_data = await self._reader.readexactly(chunk_length)

                chunks.append(chunk_data)
                total_size += chunk_length

                # Check for reasonable total size limit
                max_stream_size = self.config.protocol_options.get(
                    "max_stream_size",
                    100 * 1024 * 1024,  # 100MB default
                )
                if total_size > max_stream_size:
                    raise TransportError(
                        f"Stream too large: {total_size} > {max_stream_size}"
                    )

            # Check if this is the final chunk
            if flags & STREAM_FLAG_END:
                break

        # Reassemble all chunks
        return b"".join(chunks)


# Fix socket import for TCP keepalive
try:
    from socket import SO_KEEPALIVE, SOL_SOCKET
except ImportError:
    # Fallback constants if not available
    SOL_SOCKET = 1
    SO_KEEPALIVE = 9


# Protocol specifications for external client development
_TCP_SPEC = ProtocolSpec(
    name="TCP",
    version="1.0",
    description="TCP transport with length-prefixed binary framing",
    message_framing="length-prefixed",
    max_message_size=DEFAULT_MAX_MESSAGE_SIZE,
    supports_binary=True,
    supports_streaming=True,
    connection_oriented=True,
    security_schemes=["none"],
    auth_methods=["none"],
    metadata={
        "wire_protocol": {
            "frame_format": "[4-byte length (big-endian uint32)][message data]",
            "header_size": MESSAGE_HEADER_SIZE,
            "length_encoding": "big-endian uint32",
            "max_message_size": DEFAULT_MAX_MESSAGE_SIZE,
            "streaming_support": True,
            "streaming_marker": "0xFFFFFFFF",
            "streaming_format": "[STREAMING_MARKER][chunk_header][chunk_data]...[final_chunk]",
            "chunk_header": "[4-byte length][4-byte flags]",
            "stream_flags": {
                "CONTINUE": "0x01 - More chunks follow",
                "END": "0x02 - Final chunk",
                "ERROR": "0x04 - Error in stream",
            },
        },
        "ping_pong": {
            "ping_frame": "0x00000004 + 'PING'",
            "pong_frame": "0x00000004 + 'PONG'",
            "description": "4-byte length prefix + 4-byte message",
        },
        "keepalive": {
            "tcp_keepalive": "Enabled with SO_KEEPALIVE",
            "keepalive_idle": "300 seconds (5 minutes)",
            "keepalive_interval": "30 seconds",
            "keepalive_count": "3 attempts",
        },
        "external_client_examples": {
            "python": """
import asyncio
import struct

async def tcp_client():
    reader, writer = await asyncio.open_connection('localhost', <port>)
    
    # Send message
    message = b"Hello, MPREG!"
    length = struct.pack('>I', len(message))
    writer.write(length + message)
    await writer.drain()
    
    # Receive response
    header = await reader.readexactly(4)
    msg_len = struct.unpack('>I', header)[0]
    response = await reader.readexactly(msg_len)
    print(f"Received: {response}")
    
    writer.close()
    await writer.wait_closed()

asyncio.run(tcp_client())

# Streaming example for large data
async def tcp_streaming_client():
    reader, writer = await asyncio.open_connection('localhost', <port>)
    
    # Send streaming marker
    streaming_marker = struct.pack('>I', 0xFFFFFFFF)
    writer.write(streaming_marker)
    
    # Send large file as chunks
    large_data = b"X" * (10 * 1024 * 1024)  # 10MB data
    chunk_size = 64 * 1024  # 64KB chunks
    
    for offset in range(0, len(large_data), chunk_size):
        chunk = large_data[offset:offset + chunk_size]
        flags = 0x01  # CONTINUE
        if offset + chunk_size >= len(large_data):
            flags = 0x02  # END
        
        chunk_header = struct.pack('>II', len(chunk), flags)
        writer.write(chunk_header + chunk)
    
    await writer.drain()
    writer.close()
    await writer.wait_closed()

asyncio.run(tcp_streaming_client())
""",
            "go": """
package main

import (
    "encoding/binary"
    "fmt"
    "net"
)

func main() {
    conn, err := net.Dial("tcp", "localhost:<port>")
    if err != nil {
        panic(err)
    }
    defer conn.Close()
    
    // Send message
    message := []byte("Hello, MPREG!")
    length := make([]byte, 4)
    binary.BigEndian.PutUint32(length, uint32(len(message)))
    
    conn.Write(length)
    conn.Write(message)
    
    // Receive response
    header := make([]byte, 4)
    conn.Read(header)
    msgLen := binary.BigEndian.Uint32(header)
    
    response := make([]byte, msgLen)
    conn.Read(response)
    fmt.Printf("Received: %s\\n", response)
}
""",
            "c": """
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

int main() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in server = {0};
    server.sin_family = AF_INET;
    server.sin_port = htons(<port>);
    inet_pton(AF_INET, "127.0.0.1", &server.sin_addr);
    
    connect(sock, (struct sockaddr*)&server, sizeof(server));
    
    // Send message
    char* message = "Hello, MPREG!";
    uint32_t length = htonl(strlen(message));
    
    send(sock, &length, 4, 0);
    send(sock, message, strlen(message), 0);
    
    // Receive response
    uint32_t response_length;
    recv(sock, &response_length, 4, 0);
    response_length = ntohl(response_length);
    
    char* response = malloc(response_length + 1);
    recv(sock, response, response_length, 0);
    response[response_length] = '\\0';
    
    printf("Received: %s\\n", response);
    
    free(response);
    close(sock);
    return 0;
}
""",
        },
    },
)

_TCP_SECURE_SPEC = ProtocolSpec(
    name="TCP Secure",
    version="1.0",
    description="TCP transport with TLS/SSL encryption and length-prefixed framing",
    message_framing="length-prefixed",
    max_message_size=DEFAULT_MAX_MESSAGE_SIZE,
    supports_binary=True,
    supports_streaming=True,
    connection_oriented=True,
    security_schemes=["TLS"],
    auth_methods=["none", "client-cert"],
    metadata={
        "wire_protocol": {
            "frame_format": "[4-byte length (big-endian uint32)][message data]",
            "header_size": MESSAGE_HEADER_SIZE,
            "length_encoding": "big-endian uint32",
            "max_message_size": DEFAULT_MAX_MESSAGE_SIZE,
            "encryption": "TLS/SSL layer provides encryption",
        },
        "tls_config": {
            "tls_versions": ["1.2", "1.3"],
            "cipher_suites": "Modern secure ciphers only",
            "client_certificates": "Optional client certificate authentication",
        },
        "external_client_examples": {
            "python": """
import asyncio
import struct
import ssl

async def secure_tcp_client():
    # Create SSL context
    ssl_context = ssl.create_default_context()
    
    reader, writer = await asyncio.open_connection(
        'localhost', <port>, ssl=ssl_context
    )
    
    # Send secure message
    message = b"Secure Hello, MPREG!"
    length = struct.pack('>I', len(message))
    writer.write(length + message)
    await writer.drain()
    
    # Receive response
    header = await reader.readexactly(4)
    msg_len = struct.unpack('>I', header)[0]
    response = await reader.readexactly(msg_len)
    print(f"Secure response: {response}")
    
    writer.close()
    await writer.wait_closed()

asyncio.run(secure_tcp_client())
""",
        },
    },
)

# Register TCP transports
register_transport(
    TransportProtocol.TCP,
    TCPTransport,
    TCPListener,
    _TCP_SPEC,
)

register_transport(
    TransportProtocol.TCP_SECURE,
    TCPTransport,  # Same implementation, different config
    TCPListener,  # Same implementation, different config
    _TCP_SECURE_SPEC,
)
