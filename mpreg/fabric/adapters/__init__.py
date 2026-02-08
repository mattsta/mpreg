"""Fabric adapters for existing subsystems."""

from __future__ import annotations

from .cache_federation import CacheFederationCatalogAdapter
from .function_registry import LocalFunctionCatalogAdapter
from .service_registry import LocalServiceCatalogAdapter
from .topic_exchange import TopicExchangeCatalogAdapter

__all__ = [
    "CacheFederationCatalogAdapter",
    "LocalFunctionCatalogAdapter",
    "LocalServiceCatalogAdapter",
    "TopicExchangeCatalogAdapter",
]
