"""Simple test client for an empty MPREG Cluster.

Every MPREG server instance in a cluster has "echo" and "echos" commands by default,
where "echo" accepts 1 argument and "echos" accepts a list of arguments, so we can always
test a cluster with echo commands for the processing/resolution/connection logic."""

from __future__ import annotations

import asyncio
import contextlib
import pprint as pp
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import ulid
from loguru import logger

from ..core.model import (
    MPREGException,
    PubSubNotification,
    RPCCommand,
    RPCRequest,
    RPCResponse,
)
from ..core.serialization import JsonSerializer
from ..core.statistics import RawMessageDict
from ..core.transport.factory import TransportFactory
from ..core.transport.interfaces import (
    TransportConfig,
    TransportError,
    TransportInterface,
)

client_log = logger

try:
    loop = asyncio.get_running_loop()
    loop.set_task_factory(asyncio.eager_task_factory)
except RuntimeError:
    # No event loop running, this is fine for imports
    pass


@dataclass
class Client:
    # websocket URL like: ws://127.0.0.1:<port>
    url: str

    # optionally disable big log printing to get more accurate timing measurements
    full_log: bool = True
    transport_config: TransportConfig = field(
        default_factory=TransportConfig, repr=False
    )

    _transport: TransportInterface | None = field(default=None, init=False)
    serializer: JsonSerializer = field(default_factory=JsonSerializer, init=False)
    _pending_requests: dict[str, asyncio.Future[RPCResponse | RawMessageDict]] = field(
        default_factory=dict, init=False
    )
    _listener_task: asyncio.Task[None] | None = field(default=None, init=False)
    # PubSub notification handling
    _notification_queue: asyncio.Queue[PubSubNotification] = field(
        default_factory=asyncio.Queue, init=False
    )
    _notification_handlers: dict[str, list[Callable[[Any], None]]] = field(
        default_factory=dict, init=False
    )

    async def request(
        self, cmds: list[RPCCommand], timeout: float | None = None
    ) -> Any:
        """Sends an RPC request to the server and waits for a response.

        Args:
            cmds: A list of RPCCommand objects representing the commands to execute.
            timeout: Optional timeout in seconds for the request.

        Returns:
            The result of the RPC call.

        Raises:
            asyncio.TimeoutError: If the request times out.
            Exception: For other RPC errors returned by the server.
        """
        req = RPCRequest(cmds=tuple(cmds), u=str(ulid.new()))

        send = req.model_dump_json()

        if self.full_log:
            client_log.info("====================== NEW REQUEST ======================")
            client_log.info("[{}] Sending:\n{}", req.u, pp.pformat(req.model_dump()))

        if not self._transport:
            raise ConnectionError("Transport connection is not established.")

        # Create a future for this request's response
        response_future: asyncio.Future[RPCResponse | RawMessageDict] = asyncio.Future()
        self._pending_requests[req.u] = response_future

        try:
            await self._transport.send(
                send.encode("utf-8") if isinstance(send, str) else send
            )

            # Wait for the response with timeout
            response = await asyncio.wait_for(response_future, timeout=timeout)

            # Ensure we got an RPCResponse
            if not isinstance(response, RPCResponse):
                raise Exception(f"Expected RPCResponse, got {type(response)}")
        except TimeoutError:
            client_log.error("[{}] Request timed out after {} seconds.", req.u, timeout)
            raise
        finally:
            # Clean up the pending request
            self._pending_requests.pop(req.u, None)

        if self.full_log:
            client_log.info(
                "[{}] Result:\n{}", response.u, pp.pformat(response.model_dump())
            )

        assert req.u == response.u

        if response.error:
            client_log.error(
                "RPC Error: {}: {}", response.error.code, response.error.message
            )
            raise MPREGException(rpc_error=response.error)

        return response.r

    async def request_enhanced(
        self, request: RPCRequest, timeout: float | None = None
    ) -> RPCResponse:
        """Sends an enhanced RPC request with debugging features and returns full response.

        Args:
            request: RPCRequest object with debugging options configured
            timeout: Optional timeout in seconds for the request.

        Returns:
            Full RPCResponse including intermediate results and execution summary.

        Raises:
            asyncio.TimeoutError: If the request times out.
            Exception: For other RPC errors returned by the server.
        """
        send = request.model_dump_json()
        if self.full_log:
            client_log.info("================= NEW ENHANCED REQUEST =================")
            client_log.info(
                "[{}] Sending:\n{}", request.u, pp.pformat(request.model_dump())
            )

        if not self._transport:
            raise ConnectionError("Transport connection is not established.")

        # Create a future for this request's response
        response_future: asyncio.Future[RPCResponse | RawMessageDict] = asyncio.Future()
        self._pending_requests[request.u] = response_future

        # Send the request
        await self._transport.send(
            send.encode("utf-8") if isinstance(send, str) else send
        )

        # Wait for the response with optional timeout
        try:
            if timeout:
                response = await asyncio.wait_for(response_future, timeout=timeout)
            else:
                response = await response_future
        finally:
            # Clean up the pending request
            self._pending_requests.pop(request.u, None)

        # Handle response
        if isinstance(response, dict):
            # Raw message dict, convert to RPCResponse
            response = RPCResponse(**response)

        if self.full_log:
            client_log.info("[{}] Enhanced Response received", request.u)
            if response.intermediate_results:
                client_log.info(
                    "  Intermediate results: {} levels",
                    len(response.intermediate_results),
                )
            if response.execution_summary:
                client_log.info(
                    "  Execution summary: {:.1f}ms total",
                    response.execution_summary.total_execution_time_ms,
                )

        if response.error:
            client_log.error(
                "Enhanced RPC Error: {}: {}",
                response.error.code,
                response.error.message,
            )
            raise MPREGException(rpc_error=response.error)

        return response

    async def _listen_for_responses(self) -> None:
        """Listen for incoming responses and route them to the correct pending request."""
        if not self._transport:
            return

        try:
            while self._transport and self._transport.connected:
                try:
                    raw_message = await self._transport.receive()
                    message_data = self.serializer.deserialize(raw_message)

                    # Check message type
                    message_role = message_data.get("role")

                    if message_role == "pubsub-notification":
                        # Handle PubSub notification
                        notification = PubSubNotification.model_validate(message_data)
                        await self._notification_queue.put(notification)

                    elif message_role == "pubsub-ack":
                        # Handle PubSub acknowledgment - route to pending requests
                        ack_data = message_data
                        request_id = ack_data.get("operation_id") or ack_data.get("u")

                        if request_id and request_id in self._pending_requests:
                            future = self._pending_requests[request_id]
                            if not future.done():
                                future.set_result(RawMessageDict(message_data))
                        else:
                            client_log.warning(
                                "Received PubSub ack for unknown request: {}",
                                request_id,
                            )

                    elif message_role == "rpc-response":
                        # Handle RPC response
                        response = RPCResponse.model_validate(message_data)

                        # Route the response to the correct pending request
                        if response.u in self._pending_requests:
                            future = self._pending_requests[response.u]
                            if not future.done():
                                future.set_result(response)
                        else:
                            client_log.warning(
                                "Received RPC response for unknown request: {}",
                                response.u,
                            )

                    else:
                        # Try to parse as generic RPC response for backward compatibility
                        try:
                            response = RPCResponse.model_validate(message_data)

                            # Route the response to the correct pending request
                            if response.u in self._pending_requests:
                                future = self._pending_requests[response.u]
                                if not future.done():
                                    future.set_result(response)
                            else:
                                client_log.warning(
                                    "Received response for unknown request: {}",
                                    response.u,
                                )
                        except Exception:
                            # If it's not a valid RPC response, check if it's a raw ack
                            request_id = message_data.get("u") or message_data.get(
                                "operation_id"
                            )

                            if request_id and request_id in self._pending_requests:
                                future = self._pending_requests[request_id]
                                if not future.done():
                                    future.set_result(RawMessageDict(message_data))
                            else:
                                client_log.warning(
                                    "Received unknown message type: {}", message_data
                                )

                except Exception as e:
                    client_log.error("Failed to process message: {}", e)

        except TransportError as e:
            client_log.error("Transport receive error: {}", e)
        except Exception as e:
            client_log.error("Error in response listener: {}", e)
        finally:
            # Cancel all pending requests
            for future in self._pending_requests.values():
                if not future.done():
                    future.cancel()

    async def connect(self) -> None:
        self._transport = TransportFactory.create(self.url, self.transport_config)
        await self._transport.connect()
        self._listener_task = asyncio.create_task(self._listen_for_responses())

    async def disconnect(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listener_task
        if self._transport:
            await self._transport.disconnect()
        # Cancel any remaining pending requests
        for future in self._pending_requests.values():
            if not future.done():
                future.cancel()
        self._pending_requests.clear()
        self._transport = None

    def get_notification_queue(self) -> asyncio.Queue[PubSubNotification]:
        """Get the notification queue for PubSub clients."""
        return self._notification_queue

    async def send_raw_message(self, message: dict[str, Any]) -> RawMessageDict:
        """Send a raw message and return the response."""
        if not self._transport:
            await self.connect()
        transport = self._transport
        if not transport:
            raise ConnectionError("Transport is not established.")

        # Create a unique request ID if not present
        request_id = message.get("u", str(ulid.new()))
        if "u" not in message:
            message["u"] = request_id

        # Serialize message
        serialized = self.serializer.serialize(message)

        # Create a future to wait for the response
        response_future: asyncio.Future[RPCResponse | RawMessageDict] = asyncio.Future()
        self._pending_requests[request_id] = response_future

        try:
            await transport.send(serialized)

            # Wait for response from the unified listener
            response_data = await asyncio.wait_for(response_future, timeout=10.0)
            # Convert to RawMessageDict if needed
            if isinstance(response_data, RawMessageDict):
                return response_data
            else:
                # Convert RPCResponse to RawMessageDict
                return RawMessageDict(response_data.model_dump())

        except TimeoutError:
            # If no response within timeout, return a basic acknowledgment
            return RawMessageDict(
                {
                    "status": "sent",
                    "message_id": request_id,
                    "timestamp": asyncio.get_running_loop().time(),
                }
            )
        finally:
            # Clean up the pending request
            self._pending_requests.pop(request_id, None)


@client_log.catch
def cmd() -> None:
    import jsonargparse

    jsonargparse.CLI(Client)  # type: ignore[no-untyped-call]
