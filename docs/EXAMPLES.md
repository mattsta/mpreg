# MPREG Examples and Use Cases

This document showcases the comprehensive examples and test scenarios that demonstrate MPREG's unique capabilities in distributed computing environments.

Note: Examples use dynamic port allocation (via `allocate_port("servers")`) to avoid collisions. If you copy snippets from other docs, treat port numbers as placeholders and allocate ports at runtime.

For the full documentation index, see `docs/README.md`.

## 🎯 Quick Start Examples

### 1. Run the Quick Demo

The fastest way to see MPREG in action:

```bash
uv run python mpreg/examples/quick_demo.py
```

This 3-minute demo shows:

- ✨ Automatic dependency resolution
- 🎯 Intelligent resource routing
- ⚡ High-performance concurrency
- 🌐 Zero-configuration clustering

### 2. Performance Benchmarks

See MPREG's performance characteristics:

```bash
uv run python mpreg/examples/tier1_single_system_full.py --system rpc
```

Measure your own baseline with logging at INFO and a steady warm-up.

### 3. Real-World Applications

Comprehensive production examples:

```bash
uv run python mpreg/examples/real_world_examples.py
```

Features:

- 📊 Real-time data processing pipelines
- 🤖 Distributed ML inference systems
- 🔄 Event-driven architectures
- 📈 Analytics and monitoring workflows

### 4. Route Security + Policy Demo

Minimal live demo showing route key rotation and neighbor policy filtering:

```bash
uv run python mpreg/examples/fabric_route_security_demo.py
```

### 5. Auto-Port Cluster Bootstrap Demo

Auto-assign ports and capture endpoints via callbacks for cluster bootstrapping:

```bash
uv run python mpreg/examples/auto_port_cluster_bootstrap.py
```

### 6. Cache + Queue Integration Demo

Fabric-native cache + queue workflows in one run:

```bash
uv run python mpreg/examples/tier2_integrations.py
```

Highlights:

- RPC results cached and reused.
- Pub/sub fan-out feeding a durable queue.
- Cache federation using fabric transport.

### 7. Persistence Restart Demo

Unified persistence for cache + queue across restart:

```bash
uv run python mpreg/examples/persistence_restart_demo.py
```

Fabric catalog + route key snapshots across restart:

```bash
uv run python mpreg/examples/fabric_snapshot_restart_demo.py
```

This demo uses permissive federation bridging to allow a short-lived
cross-cluster announcement.

Use the settings file to launch a persistent server:

```bash
uv run mpreg server start-config mpreg/examples/persistence_settings.toml
```

## ⚙️ Performance and Correctness Tips

- Reuse `MPREGClientAPI` connections to avoid setup overhead per request.
- Use explicit `locs` to keep routing deterministic and reduce fan-out.
- When validating fabric cache federation, pass `CacheOptions(cache_levels=...)` to target L4.
- For queue demos, ensure enough subscribers exist before using `DeliveryGuarantee.QUORUM`.
- Keep logging at INFO for cleaner benchmark signals.

## 🔐 Optional Routing Controls

- **Route security**: signed announcements + key rotation (`docs/FABRIC_ROUTE_SECURITY.md`).
- **Route policies**: per-neighbor import filters and global export policy
  (`docs/FABRIC_ROUTE_POLICIES.md`).
- **Route trace**: debug next-hop selection via `RouteTable.explain_selection()`.

## 🧪 Comprehensive Test Suite

MPREG includes **2,000+ tests** covering unit, integration, and live topology
scenarios across RPC, pub/sub, queues, cache, and the fabric control plane.

```bash
# Run all tests
uv run pytest -v

# Run specific test categories
uv run pytest tests/test_advanced_cluster_scenarios.py -v
uv run pytest tests/test_production_examples.py -v
uv run pytest tests/test_integration_examples.py -v
```

### Test Categories

#### 1. **Core Functionality**

- Model validation and serialization
- Function identity + version constraint matching
- Dependency resolution and topological ordering

#### 2. **Fabric Control Plane**

- Catalog delta propagation and TTL expiry
- Path-vector route announcements and loop prevention
- Membership changes and GOODBYE handling

#### 3. **System Integrations**

- RPC, pub/sub, queue, and cache federation flows
- Multi-hop routing with hop budgets
- Resource-based routing and policy enforcement

#### 4. **Real-World Workflows**

- End-to-end data pipelines
- ML inference workflows
- Business process orchestration

## 🌟 Unique MPREG Capabilities Demonstrated

### 1. Automatic Dependency Resolution

```python
# Complex dependency chain resolved automatically
workflow = await client._client.request([
    RPCCommand(name="step1", fun="process_data", args=(raw_data,)),
    RPCCommand(name="step2", fun="analyze", args=("step1",)),  # Uses step1 result
    RPCCommand(name="step3", fun="store", args=("step2",)),    # Uses step2 result
    RPCCommand(name="final", fun="report", args=("step1", "step2", "step3"))  # Uses all
])
# MPREG handles topological sorting and execution order automatically!
```

### 2. Resource-Based Intelligent Routing

```python
# Functions automatically route to servers with matching resources
gpu_result = await client.call("train_model", model_data,
                              locs=frozenset(["gpu", "ml-models"]))

cpu_result = await client.call("heavy_compute", dataset,
                              locs=frozenset(["cpu-intensive"]))

db_result = await client.call("store_results", combined_data,
                             locs=frozenset(["database", "storage"]))

# No manual endpoint management - MPREG routes optimally!
```

### 3. Zero-Configuration Clustering

```python
# Servers automatically discover and join the cluster
from mpreg.core.port_allocator import port_range_context

with port_range_context(3, "servers") as ports:
    server1 = MPREGServer(MPREGSettings(port=ports[0], resources={"gpu"}))
    hub_url = f"ws://127.0.0.1:{ports[0]}"
    server2 = MPREGServer(
        MPREGSettings(port=ports[1], resources={"cpu"}, peers=[hub_url])
    )
    server3 = MPREGServer(
        MPREGSettings(port=ports[2], resources={"db"}, peers=[hub_url])
    )

# Fabric catalog gossip handles membership and routing automatically.
# Client connects to any node and accesses entire cluster
```

### 4. High-Performance Concurrency

```python
# Multiple concurrent requests over single connection
tasks = [
    client.call("function_a", data_a, locs=frozenset(["server_a"])),
    client.call("function_b", data_b, locs=frozenset(["server_b"])),
    client.call("function_c", data_c, locs=frozenset(["server_c"])),
]

results = await asyncio.gather(*tasks)  # All execute in parallel
# Sub-millisecond latencies with hundreds of concurrent operations!
```

## 📊 Performance Characteristics

Performance depends on workload, logging level, and network topology. For stable measurements:

- Warm up the cluster for 10-20 requests before sampling.
- Keep logging at INFO or lower.
- Reuse connections and batch RPC calls when possible.
- Increase concurrency gradually to find saturation points.

## 🏗️ Architecture Patterns Demonstrated

### 1. **Data Pipeline Pattern**

Real-time sensor data → processing → analytics → storage

- Automatic routing through specialized nodes
- Error handling and data validation
- Real-time anomaly detection
- Dashboard integration

### 2. **ML Inference Pattern**

Model routing → preprocessing → inference → post-processing

- Multi-model support (vision, NLP, etc.)
- Automatic model selection
- Feature extraction pipelines
- Batch and real-time inference

### 3. **Microservice Orchestration**

Authentication → validation → business logic → persistence

- Saga pattern implementation
- Circuit breaker behavior
- Event-driven workflows
- Cross-service dependencies

### 4. **High-Throughput Processing**

Bulk operations → parallel processing → aggregation

- Concurrent request handling
- Load balancing across nodes
- Performance optimization
- Resource utilization

## 🎯 Production Deployment Scenarios

### E-Commerce Platform

```
Frontend (API Gateway) → Auth Service → Payment Service → Inventory Service → Analytics
```

- 8-step purchase workflow
- Concurrent user sessions
- Inventory stress testing
- Real-time analytics

### Data Processing Pipeline

```
Ingestion → Validation → Processing → Analytics → Storage → Reporting
```

- Multi-stage ETL workflows
- Anomaly detection
- Real-time insights
- Dashboard summaries

### ML Inference Cluster

```
Router → Vision Models → NLP Models → Feature Processing → Results Aggregation
```

- Multi-model inference
- Preprocessing pipelines
- Parallel execution
- Model optimization

## 🚀 Getting Started

1. **Quick Demo** (3 minutes):

   ```bash
   uv run python mpreg/examples/quick_demo.py
   ```

2. **Performance Testing**:

   ```bash
   uv run python mpreg/examples/tier1_single_system_full.py --system rpc
   ```

3. **Real-World Examples**:

   ```bash
   uv run python mpreg/examples/real_world_examples.py
   ```

4. **Run Full Test Suite**:
   ```bash
   uv run pytest -v
   ```

## 🛡️ Defensive Function Design Patterns

### **Critical: Handling Dependency Resolution**

One of the most important aspects of MPREG function design is handling the fact that **dependency resolution transforms arguments**. Functions must work correctly whether called directly with simple arguments OR indirectly with resolved dependency objects.

### **The Problem**

```python
# ❌ This function will CRASH in dependency chains
def analytics_bad(dataset: str, metrics: list) -> dict:
    return {
        "results": {metric: f"{metric}_result" for metric in metrics}
        # FAILS: TypeError: unhashable type: 'dict' when metrics contains resolved objects
    }

# Direct call works fine:
analytics_bad("test", ["metric1", "metric2"])  # ✅ Works

# But dependency-resolved call crashes:
# metrics = [{"gpu_result": "data"}, {"cpu_result": "data"}]  # From resolved dependencies
analytics_bad("test", metrics)  # ❌ CRASHES: Can't use dict as dictionary key
```

### **The Solution: Safe Function Design**

```python
# ✅ This function handles both cases safely
def analytics_safe(dataset: str, metrics: list) -> dict:
    return {
        "dataset": dataset,
        "metrics": metrics,
        "results": {f"metric_{i}": f"result_for_metric_{i}" for i, metric in enumerate(metrics)},
        "processing_node": "Analytics-Server"
    }

# Works for both direct and dependency-resolved calls:
analytics_safe("test", ["metric1", "metric2"])  # ✅ Works
analytics_safe("test", [{"gpu": "data"}, {"cpu": "data"}])  # ✅ Also works
```

### **Real-World Example: Multi-Stage Data Pipeline**

```python
# Production example showing defensive design
def aggregate_results(operation: str, data_sources: list) -> dict:
    """
    Safely aggregates results from multiple data sources.

    data_sources can be:
    - Direct call: ["source1", "source2", "source3"]
    - Resolved dependencies: [{"query_result": "data1"}, {"computation": "data2"}]
    """
    aggregated_data = []
    source_types = []

    for i, source in enumerate(data_sources):
        if isinstance(source, str):
            # Direct string reference
            aggregated_data.append(f"processed_{source}")
            source_types.append("string_reference")
        elif isinstance(source, dict):
            # Resolved dependency object - extract meaningful data
            if "result" in source:
                aggregated_data.append(source["result"])
            elif "data" in source:
                aggregated_data.append(source["data"])
            else:
                # Fallback: use the whole object
                aggregated_data.append(f"complex_result_{i}")
            source_types.append("resolved_object")
        else:
            # Handle unexpected types gracefully
            aggregated_data.append(f"unknown_type_{i}")
            source_types.append(f"unknown_{type(source).__name__}")

    return {
        "operation": operation,
        "total_sources": len(data_sources),
        "source_types": source_types,
        "aggregated_data": aggregated_data,
        "timestamp": "2025-01-17T12:00:00Z"
    }

# Example usage in a dependency chain:
workflow = await client._client.request([
    # Stage 1: Multiple parallel data gathering
    RPCCommand(name="gpu_process", fun="run_inference", args=("ModelA", "input")),
    RPCCommand(name="cpu_process", fun="heavy_compute", args=("input", 100)),
    RPCCommand(name="db_query", fun="query_database", args=("SELECT * FROM metrics")),

    # Stage 2: Safe aggregation of all results
    RPCCommand(
        name="final_report",
        fun="aggregate_results",
        args=("comprehensive_analysis", ["gpu_process", "cpu_process", "db_query"])
        # aggregate_results safely handles the resolved objects from all three previous stages
    )
])
```

### **Type-Safe Function Design Pattern**

```python
from typing import Union, Dict, List, Any, Optional

def process_workflow_data(
    workflow_id: str,
    inputs: List[Union[str, Dict[str, Any]]],
    options: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Type-safe function that explicitly handles both direct and resolved arguments.

    Args:
        workflow_id: Unique identifier for this workflow
        inputs: Can be string references OR resolved dependency objects
        options: Optional configuration (also subject to dependency resolution)
    """
    processed_inputs = []
    input_metadata = []

    # Handle inputs safely
    for i, input_item in enumerate(inputs):
        if isinstance(input_item, str):
            # String reference - process as-is
            processed_inputs.append(input_item)
            input_metadata.append({"type": "reference", "value": input_item})
        elif isinstance(input_item, dict):
            # Resolved dependency object - extract relevant data
            if "processed_data" in input_item:
                processed_inputs.append(input_item["processed_data"])
                input_metadata.append({"type": "resolved", "keys": list(input_item.keys())})
            else:
                # Generic handling for complex objects
                processed_inputs.append(f"resolved_input_{i}")
                input_metadata.append({"type": "complex", "size": len(str(input_item))})
        else:
            # Fallback for unexpected types
            processed_inputs.append(str(input_item))
            input_metadata.append({"type": "converted", "original_type": type(input_item).__name__})

    # Handle options safely
    safe_options = options or {}
    if isinstance(safe_options, dict):
        # Already a dict, use directly
        processed_options = safe_options
    else:
        # Converted from dependency resolution
        processed_options = {"resolved_options": str(safe_options)}

    return {
        "workflow_id": workflow_id,
        "input_count": len(inputs),
        "processed_inputs": processed_inputs,
        "input_metadata": input_metadata,
        "options": processed_options,
        "processing_node": "WorkflowProcessor",
        "timestamp": "2025-01-17T12:00:00Z"
    }
```

### **Testing Pattern for Dependency-Safe Functions**

```python
import pytest

class TestDependencySafeFunctions:
    """Test suite ensuring functions work with both direct and resolved arguments"""

    def test_function_with_string_arguments(self):
        """Test normal direct function call"""
        result = aggregate_results("test_op", ["source1", "source2"])
        assert result["total_sources"] == 2
        assert all(t == "string_reference" for t in result["source_types"])

    def test_function_with_resolved_dependencies(self):
        """Test with complex resolved dependency objects (critical test!)"""
        resolved_sources = [
            {"query_result": "SELECT results", "rows": 150, "time": "45ms"},
            {"computation": "tensor_ops", "gpu_time": "12ms", "memory": "2GB"},
            {"storage_info": "saved to S3", "size": "500MB"}
        ]

        result = aggregate_results("production_op", resolved_sources)
        assert result["total_sources"] == 3
        assert all(t == "resolved_object" for t in result["source_types"])
        assert len(result["aggregated_data"]) == 3

    def test_function_with_mixed_arguments(self):
        """Test with both strings and resolved objects"""
        mixed_sources = [
            "simple_string",
            {"complex": "object", "with": {"nested": "data"}},
            "another_string"
        ]

        result = aggregate_results("mixed_op", mixed_sources)
        assert result["total_sources"] == 3
        expected_types = ["string_reference", "resolved_object", "string_reference"]
        assert result["source_types"] == expected_types

    def test_function_handles_empty_inputs(self):
        """Edge case: empty inputs"""
        result = aggregate_results("empty_op", [])
        assert result["total_sources"] == 0
        assert result["aggregated_data"] == []

    def test_function_handles_unexpected_types(self):
        """Edge case: unexpected argument types"""
        weird_sources = [123, None, {"valid": "dict"}]
        result = aggregate_results("weird_op", weird_sources)
        assert result["total_sources"] == 3
        # Function should handle gracefully without crashing
```

### **Key Defensive Design Principles**

1. **Never use list/dict arguments directly as dictionary keys**
2. **Always check argument types before processing**
3. **Provide fallbacks for unexpected types**
4. **Use index-based keys instead of value-based keys**
5. **Test functions with both simple and complex resolved arguments**
6. **Include type hints to document expected argument transformations**

### **Error Prevention in Production**

The MPREG server now includes enhanced error reporting:

```python
# ✅ With the new error handling, function crashes are caught and reported clearly:
#
# ERROR: Function execution failed: command=analytics, error=TypeError: unhashable type: 'dict',
#        args_types=['str', 'list']
#
# This immediately tells you:
# 1. Which function crashed (analytics)
# 2. What the error was (unhashable type)
# 3. What argument types were received (str, list)
#
# Instead of a mysterious 30-second timeout!
```

## 🎉 What Makes MPREG Special

1. **🔗 Automatic Dependency Resolution**
   - No manual dependency management
   - Topological sorting built-in
   - Late-binding parameter substitution

2. **🎯 Intelligent Resource Routing**
   - Functions route to optimal servers automatically
   - No hardcoded endpoints
   - Dynamic resource-based discovery

3. **⚡ High-Performance Concurrency**
   - Sub-millisecond local calls
   - Concurrent requests over single connections
   - Scales to hundreds of parallel operations

4. **🌐 Zero-Configuration Clustering**
   - Automatic peer discovery
   - Self-managing membership
   - No central coordination required

5. **🔧 Self-Managing Architecture**
   - Components handle their own lifecycle
   - Automatic connection pooling
   - Resilient error handling

6. **🏭 Production-Ready**
   - Comprehensive error handling
   - Timeout management
   - Connection pooling
   - Graceful degradation

Ready to build powerful distributed applications with MPREG! 🚀
