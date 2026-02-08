"""
MPREG Core Module

Core functionality for the MPREG federation system including
models, registry, serialization, and connection management.
"""

from __future__ import annotations

from .config import MPREGSettings
from .connection import Connection
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
    # Config
    "MPREGSettings",
    # Connection
    "Connection",
    # Model - Core RPC
    "RPCCommand",
    "RPCRequest",
    "RPCResponse",
    "RPCError",
    "RPCServerGoodbye",
    "RPCServerStatus",
    "RPCServerMessage",
    "RPCServerRequest",
    # Model - PubSub
    "PubSubMessage",
    "PubSubSubscription",
    "PubSubPublish",
    "PubSubSubscribe",
    "PubSubUnsubscribe",
    "PubSubNotification",
    "PubSubAck",
    # Model - Other
    "TopicPattern",
    "TopicAdvertisement",
    # Model - Exceptions
    "MPREGException",
    "CommandNotFoundException",
    # Registry
    "RpcRegistry",
    # Serialization
    "JsonSerializer",
    # Timer
    "Timer",
    # Topic Exchange
    "TopicExchange",
]
