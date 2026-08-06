"""
MPREG Core Module

Core functionality for the MPREG federation system including
models, registry, serialization, and connection management.
"""

from __future__ import annotations

from .config import MPREGSettings
from .connection import Connection
from .errors import MpregError, MpregErrorCode, error_code_catalog, map_exception
from .model import (
    CommandNotFoundException,
    MPREGException,
    PubSubAck,
    PubSubMessage,
    PubSubNotification,
    PubSubPublish,
    PubSubSubscribe,
    PubSubSubscription,
    PubSubUnsubscribe,
    RPCCommand,
    RPCError,
    RPCRequest,
    RPCResponse,
    RPCServerGoodbye,
    RPCServerMessage,
    RPCServerRequest,
    RPCServerStatus,
    TopicAdvertisement,
    TopicPattern,
)
from .rpc_registry import RpcRegistry
from .serialization import JsonSerializer
from .timer import Timer
from .topic_exchange import TopicExchange

__all__ = [
    "CommandNotFoundException",
    # Connection
    "Connection",
    # Serialization
    "JsonSerializer",
    # Model - Exceptions
    "MPREGException",
    # Config
    "MPREGSettings",
    "MpregError",
    "MpregErrorCode",
    "PubSubAck",
    # Model - PubSub
    "PubSubMessage",
    "PubSubNotification",
    "PubSubPublish",
    "PubSubSubscribe",
    "PubSubSubscription",
    "PubSubUnsubscribe",
    # Model - Core RPC
    "RPCCommand",
    "RPCError",
    "RPCRequest",
    "RPCResponse",
    "RPCServerGoodbye",
    "RPCServerMessage",
    "RPCServerRequest",
    "RPCServerStatus",
    # Registry
    "RpcRegistry",
    # Timer
    "Timer",
    "TopicAdvertisement",
    # Topic Exchange
    "TopicExchange",
    # Model - Other
    "TopicPattern",
    "error_code_catalog",
    "map_exception",
]
