"""
MPREG Client Module

Client-side functionality for connecting to MPREG federation systems.
"""

from __future__ import annotations

from .call_policy import (
    ClientCallPolicy,
    RpcExecutionMode,
    call_with_policy,
    default_ha_policy,
)
from .client import Client
from .client_api import MPREGClientAPI
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
    StrongRetryAbortResult,
    UnifiedMPREGClient,
)

__all__ = [
    "CacheOpResult",
    "Client",
    "ClientCallPolicy",
    "MPREGClient",
    "MPREGClientAPI",
    "MPREGClusterClient",
    "MPREGDnsClient",
    "MPREGPubSubClient",
    "MPREGPubSubExtendedClient",
    "QueueSendResult",
    "RpcExecutionMode",
    "StrongRetryAbortResult",
    "SubscriptionCallback",
    "UnifiedMPREGClient",
    "call_with_policy",
    "default_ha_policy",
]
