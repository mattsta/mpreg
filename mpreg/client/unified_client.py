"""Unified MPREG client façade spanning RPC, pub/sub, queue, and cache.

Queue and cache operations use the same RPC transport as
:class:`MPREGClientAPI`, calling well-known server command names when
registered, or documenting the local manager path when operators attach
managers in-process. This is the single import path for application code
that needs more than one data plane.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from loguru import logger

from ..core.model import RPCCommand
from .call_policy import ClientCallPolicy
from .client_api import MPREGClientAPI
from .pubsub_client import MPREGPubSubClient, PubSubMessage
from ..core.transport.interfaces import TransportConfig

unified_log = logger

@dataclass(slots=True)
class QueueSendResult:
    """Normalized result from a queue send attempt."""

    success: bool
    message_id: str | None = None
    error_message: str | None = None
    raw: Any = None

    @classmethod
    def from_raw(cls, raw: Any) -> QueueSendResult:
        if isinstance(raw, dict):
            return cls(
                success=bool(raw.get("success", False)),
                message_id=(
                    str(raw["message_id"])
                    if raw.get("message_id") is not None
                    else None
                ),
                error_message=(
                    str(raw["error_message"])
                    if raw.get("error_message") is not None
                    else (
                        str(raw["error"]) if raw.get("error") is not None else None
                    )
                ),
                raw=raw,
            )
        if hasattr(raw, "success"):
            mid = getattr(raw, "message_id", None)
            return cls(
                success=bool(raw.success),
                message_id=str(mid) if mid is not None else None,
                error_message=getattr(raw, "error_message", None),
                raw=raw,
            )
        return cls(success=bool(raw), raw=raw)

@dataclass(slots=True)
class CacheOpResult:
    """Normalized result from a cache get/put attempt."""

    success: bool
    value: Any = None
    error_message: str | None = None
    raw: Any = None

    @classmethod
    def from_raw(cls, raw: Any, *, value_key: str = "value") -> CacheOpResult:
        if isinstance(raw, dict):
            return cls(
                success=bool(raw.get("success", value_key in raw or "entry" in raw)),
                value=raw.get(value_key, raw.get("entry", raw.get("data"))),
                error_message=(
                    str(raw["error_message"])
                    if raw.get("error_message") is not None
                    else (str(raw["error"]) if raw.get("error") is not None else None)
                ),
                raw=raw,
            )
        if hasattr(raw, "success"):
            entry = getattr(raw, "entry", None)
            value = getattr(entry, "value", None) if entry is not None else getattr(raw, "value", None)
            return cls(
                success=bool(raw.success),
                value=value,
                error_message=getattr(raw, "error_message", None),
                raw=raw,
            )
        return cls(success=raw is not None, value=raw, raw=raw)

@dataclass(slots=True)
class MPREGClient:
    """Single façade for RPC + pub/sub + queue + cache over one connection.

    Prefer this over composing lower-level clients in application code.
    ``MPREGClientAPI`` remains available for RPC-only callers; this class
    embeds it and adds plane helpers.
    """

    url: str
    full_log: bool = False
    auth_token: str | None = None
    api_key: str | None = None
    transport_config: TransportConfig | None = None
    call_policy: ClientCallPolicy | None = None
    default_timeout_seconds: float | None = 30.0
    notification_queue_maxsize: int = 1024

    api: MPREGClientAPI = field(init=False)
    pubsub: MPREGPubSubClient = field(init=False)
    _pubsub_started: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.api = MPREGClientAPI(
            url=self.url,
            full_log=self.full_log,
            auth_token=self.auth_token,
            api_key=self.api_key,
            transport_config=self.transport_config,
            call_policy=self.call_policy,
            default_timeout_seconds=self.default_timeout_seconds,
            notification_queue_maxsize=self.notification_queue_maxsize,
        )
        self.pubsub = MPREGPubSubClient(base_client=self.api)

    @property
    def _client(self) -> Any:
        return self.api._client

    async def connect(self) -> None:
        await self.api.connect()

    async def disconnect(self) -> None:
        if self._pubsub_started:
            await self.pubsub.stop()
            self._pubsub_started = False
        await self.api.disconnect()

    async def __aenter__(self) -> MPREGClient:
        await self.connect()
        await self.pubsub.start()
        self._pubsub_started = True
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.disconnect()

    # --- RPC ---
    async def call(self, fun: str, *args: Any, **kwargs: Any) -> Any:
        return await self.api.call(fun, *args, **kwargs)

    async def request(
        self,
        commands: list[RPCCommand] | tuple[RPCCommand, ...],
        *,
        timeout: float | None = None,
    ) -> Any:
        return await self.api.request(commands, timeout=timeout)

    async def call_dag(
        self,
        commands: list[RPCCommand] | tuple[RPCCommand, ...],
        *,
        timeout: float | None = None,
    ) -> Any:
        return await self.api.call_dag(commands, timeout=timeout)

    def last_trace_context(self) -> dict[str, str] | None:
        return self.api.last_trace_context()

    # --- Pub/sub ---
    async def publish(self, topic: str, payload: Any, headers: Any = None) -> bool:
        if not self._pubsub_started:
            await self.pubsub.start()
            self._pubsub_started = True
        return await self.pubsub.publish(topic, payload, headers)

    async def subscribe(
        self,
        patterns: list[str],
        callback: Callable[[PubSubMessage], None],
        get_backlog: bool = True,
        backlog_seconds: int = 300,
    ) -> str:
        if not self._pubsub_started:
            await self.pubsub.start()
            self._pubsub_started = True
        return await self.pubsub.subscribe(
            patterns, callback, get_backlog, backlog_seconds
        )

    async def unsubscribe(self, subscription_id: str) -> bool:
        return await self.pubsub.unsubscribe(subscription_id)

    # --- Queue (RPC command surface when server exposes it) ---
    async def queue_send(
        self,
        queue_name: str,
        payload: Any,
        *,
        topic: str | None = None,
        delivery_guarantee: str = "at_least_once",
        timeout: float | None = None,
    ) -> QueueSendResult:
        """Send a message to a named queue via the ``queue_send`` RPC command.

        Servers that attach a :class:`MessageQueueManager` may register
        ``queue_send``; when absent this raises the mapped command-not-found
        error so callers fail closed rather than silently dropping.
        """
        topic_value = topic or f"mpreg.queue.{queue_name}"
        raw = await self.api.call(
            "queue_send",
            {
                "queue_name": queue_name,
                "topic": topic_value,
                "payload": payload,
                "delivery_guarantee": delivery_guarantee,
            },
            timeout=timeout,
        )
        return QueueSendResult.from_raw(raw)

    async def queue_create(
        self,
        queue_name: str,
        *,
        timeout: float | None = None,
        **options: Any,
    ) -> Any:
        """Create a queue via the ``queue_create`` RPC command when available."""
        body: dict[str, Any] = {"queue_name": queue_name, **options}
        return await self.api.call("queue_create", body, timeout=timeout)

    # --- Cache (RPC command surface when server exposes it) ---
    async def cache_get(
        self,
        namespace: str,
        identifier: str,
        *,
        version: str | None = None,
        timeout: float | None = None,
    ) -> CacheOpResult:
        """Fetch a cache entry via the ``cache_get`` RPC command."""
        body: dict[str, Any] = {
            "namespace": namespace,
            "identifier": identifier,
        }
        if version is not None:
            body["version"] = version
        raw = await self.api.call("cache_get", body, timeout=timeout)
        return CacheOpResult.from_raw(raw)

    async def cache_put(
        self,
        namespace: str,
        identifier: str,
        value: Any,
        *,
        version: str | None = None,
        timeout: float | None = None,
        **options: Any,
    ) -> CacheOpResult:
        """Store a cache entry via the ``cache_put`` RPC command."""
        body: dict[str, Any] = {
            "namespace": namespace,
            "identifier": identifier,
            "value": value,
            **options,
        }
        if version is not None:
            body["version"] = version
        raw = await self.api.call("cache_put", body, timeout=timeout)
        return CacheOpResult.from_raw(raw)

# Back-compat alias used in some sketches / docs
UnifiedMPREGClient = MPREGClient

__all__ = [
    "MPREGClient",
    "UnifiedMPREGClient",
    "QueueSendResult",
    "CacheOpResult",
]
