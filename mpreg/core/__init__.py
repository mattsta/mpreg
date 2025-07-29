"""
MPREG Core Module

Core functionality for the MPREG federation system including
models, registry, serialization, and connection management.
"""

from .config import MPREGSettings
from .connection import Connection
from .model import (
    CommandNotFoundException,
    GossipMessage,
    MPREGException,
    PeerInfo,
    PubSubAck,
    PubSubGossip,
    PubSubMessage,
    PubSubNotification,
    PubSubPublish,
    PubSubSubscribe,
    PubSubSubscription,
    PubSubUnsubscribe,
    RPCCommand,
    RPCError,
    RPCInternalAnswer,
    RPCInternalRequest,
    RPCRequest,
    RPCResponse,
    RPCServerGoodbye,
    RPCServerHello,
    RPCServerMessage,
    RPCServerRequest,
    RPCServerStatus,
    TopicAdvertisement,
    TopicPattern,
)
from .registry import Command, CommandRegistry
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
    "RPCInternalRequest",
    "RPCInternalAnswer",
    "RPCServerHello",
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
    "PubSubGossip",
    # Model - Other
    "TopicPattern",
    "TopicAdvertisement",
    "PeerInfo",
    "GossipMessage",
    # Model - Exceptions
    "MPREGException",
    "CommandNotFoundException",
    # Registry
    "Command",
    "CommandRegistry",
    # Serialization
    "JsonSerializer",
    # Timer
    "Timer",
    # Topic Exchange
    "TopicExchange",
]
