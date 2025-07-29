"""
Unified monitoring and observability system for MPREG.

This module provides comprehensive monitoring capabilities across all MPREG systems:
- RPC command execution monitoring
- Topic Pub/Sub event flow monitoring
- Message Queue processing monitoring
- Cache coordination monitoring
- Federation cross-cluster monitoring
- Transport layer health monitoring

Features:
- Cross-system correlation tracking
- Real-time performance metrics
- System health aggregation
- Event timeline reconstruction
- Performance trend analysis
"""

from .unified_monitoring import (
    CorrelationMetrics,
    CrossSystemEvent,
    MonitoringConfig,
    SystemHealthScore,
    SystemPerformanceMetrics,
    UnifiedSystemMetrics,
    UnifiedSystemMonitor,
    create_unified_system_monitor,
)

__all__ = [
    "UnifiedSystemMetrics",
    "CorrelationMetrics",
    "SystemHealthScore",
    "SystemPerformanceMetrics",
    "CrossSystemEvent",
    "MonitoringConfig",
    "UnifiedSystemMonitor",
    "create_unified_system_monitor",
]
