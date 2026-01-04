"""
MPREG Client Module

Client-side functionality for connecting to MPREG federation systems.
"""

from __future__ import annotations

from .client import Client
from .client_api import MPREGClientAPI
from .pubsub_client import (
    MPREGPubSubClient,
    MPREGPubSubExtendedClient,
    SubscriptionCallback,
)

__all__ = [
    "Client",
    "MPREGClientAPI",
    "MPREGPubSubClient",
    "MPREGPubSubExtendedClient",
    "SubscriptionCallback",
]
