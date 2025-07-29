"""
Federated pub/sub topic routing for MPREG.

This module provides production-ready federation components with comprehensive optimizations:
- Thread-safe concurrent operations
- Latency-based intelligent routing
- Comprehensive error handling and resilience
- Memory-efficient data structures
- Advanced monitoring and observability

Includes all optimizations from federation_optimized.py and federation_bridge_v2.py
consolidated into a single module for easier maintenance.
"""

# Re-export optimized components for compatibility
from .federation_bridge import (
    GraphAwareFederationBridge as FederationBridge,
)
from .federation_optimized import (
    CircuitBreaker,
    ClusterIdentity,
    ClusterStatus,
    IntelligentRoutingTable,
    LatencyMetrics,
    OptimizedBloomFilter,
    OptimizedClusterState,
)

# Legacy aliases for backward compatibility
BloomFilter = OptimizedBloomFilter
FederationBridge = FederationBridge

__all__ = [
    # Core components
    "ClusterStatus",
    "ClusterIdentity",
    "OptimizedBloomFilter",
    "BloomFilter",  # Legacy alias
    "LatencyMetrics",
    "CircuitBreaker",
    "IntelligentRoutingTable",
    "OptimizedClusterState",
    "FederationBridge",
]
