"""
MPREG Client Module

Client-side functionality for connecting to MPREG federation systems.
"""

from __future__ import annotations

from .client import Client
from .client_api import MPREGClientAPI
from .call_policy import (
    ClientCallPolicy,
    RpcExecutionMode,
    call_with_policy,
    default_ha_policy,
)
from .cluster_client import MPREGClusterClient
from .dns_client import MPREGDnsClient
from .pubsub_client import (
    MPREGPubSubClient,
    MPREGPubSubExtendedClient,
    SubscriptionCallback,
)
from .unified_client import (
    CacheOpResult,
    MPREGClient,
    QueueSendResult,
    UnifiedMPREGClient,
)

__all__ = [
    "Client",
    "MPREGClientAPI",
    "MPREGClusterClient",
    "ClientCallPolicy",
    "RpcExecutionMode",
    "call_with_policy",
    "default_ha_policy",
    "MPREGDnsClient",
    "MPREGPubSubClient",
    "MPREGPubSubExtendedClient",
    "SubscriptionCallback",
    "MPREGClient",
    "UnifiedMPREGClient",
    "QueueSendResult",
    "CacheOpResult",
]
