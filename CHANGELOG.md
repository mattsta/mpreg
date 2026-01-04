# CHANGELOG

## [Unreleased] - 2026-01-04

### 🌟 Major Architectural Transformation: Federation → Unified Fabric

This release represents a complete architectural rewrite of MPREG's distributed coordination layer, transforming from a "federation" model to a unified "fabric" routing platform. The fabric provides a single, coherent control plane for RPC, pub/sub, message queues, caching, and cross-cluster coordination.

---

## 🏗️ Core Architecture Changes

### Unified Fabric Routing System (`mpreg/fabric/`)

**Complete rewrite of distributed coordination** from `mpreg/federation/` to `mpreg/fabric/`:

- **Routing Catalog & Engine** (`catalog.py`, `engine.py`, `router.py`)
  - Single unified catalog for functions, topics, queues, and cache profiles
  - Path-vector routing with multi-hop forwarding support
  - Policy-based routing with export/import filtering
  - Catalog delta propagation via gossip protocol

- **Control Plane** (`control_plane.py`, `route_control.py`)
  - Link-state routing with topology awareness
  - Route announcement and withdrawal protocols
  - BGP-inspired path-vector routing with loop prevention
  - Route security with cryptographic verification

- **Federation Capabilities**
  - RPC forwarding across clusters (`rpc_messages.py`, `router.py`)
  - Pub/sub message forwarding (`pubsub_forwarding.py`, `pubsub_router.py`)
  - Queue federation with delivery guarantees (`queue_federation.py`, `queue_delivery.py`)
  - Cache federation with profile-based selection (`cache_federation.py`, `cache_selection.py`)

- **Transport Layer**
  - Unified message envelope codec (`message.py`, `message_codec.py`)
  - Server-to-server transport adapters (`server_transport.py`, `server_gossip_transport.py`)
  - Raft consensus transport (`raft_transport.py`, `raft_messages.py`)

- **Gossip & Membership**
  - Epidemic gossip protocol (`gossip.py`, `gossip_transport.py`)
  - Peer directory and discovery (`peer_directory.py`, `membership.py`)
  - Auto-discovery service (`auto_discovery.py`)
  - Hub hierarchy for regional topologies (`hubs.py`, `hub_registry.py`, `hub_hierarchy.py`)

- **Monitoring & Observability**
  - Comprehensive monitoring endpoints (`monitoring_endpoints.py`)
  - Performance metrics collection (`performance_metrics.py`)
  - Federation alerting system (`federation_alerting.py`)

### Persistence Framework (`mpreg/core/persistence/`)

**New pluggable persistence layer** for stateful components:

- **Core Abstractions** (`backend.py`, `config.py`, `registry.py`)
  - Generic key-value store interface
  - Pluggable backend registry (in-memory, file-based, future: Redis, PostgreSQL)
  - Configuration-driven persistence activation

- **Component Stores**
  - Cache persistence (`cache_store.py`)
  - Message queue persistence (`queue_store.py`)
  - Generic KV store (`kv_store.py`)

- **Blockchain Persistence**
  - Blockchain state storage (`blockchain_store.py` in `datastructures/`)
  - Cryptographic primitives (`blockchain_crypto.py`)
  - Block ledger management (`blockchain_ledger.py` in `core/`)

### Enhanced Server Architecture (`server.py`)

**Major refactoring of `MPREGServer`** (5,738 lines changed):

- **Fabric Integration**
  - Direct integration with `FabricRouter` for all routing decisions
  - Catalog-driven function/topic/queue registration
  - Unified message envelope handling
  - Multi-hop RPC forwarding with hop budget tracking

- **Connection Management**
  - Improved peer lifecycle management
  - Catalog snapshot synchronization on peer connect
  - Connection storm prevention
  - Graceful shutdown with goodbye protocol

- **Monitoring & Introspection**
  - Real-time server status endpoints
  - Distributed tracing integration
  - Performance metrics collection
  - Health check endpoints

### Transport Layer Enhancements (`mpreg/core/transport/`)

- **Adapter Registry** (`adapter_registry.py`)
  - Dynamic transport adapter registration
  - Protocol negotiation support
  - Future-proof for QUIC/HTTP3

- **Factory Improvements** (`factory.py`)
  - Unified transport factory for client/server
  - Connection pooling and reuse
  - Circuit breaker integration

- **Protocol Enhancements**
  - TCP transport improvements (`tcp_transport.py`)
  - WebSocket transport refinements (`websocket_transport.py`)
  - Server envelope transport (`server_envelope_transport.py`)

---

## 🔧 Core System Improvements

### Configuration & Settings (`mpreg/core/config.py`)

- **Fabric Routing Settings**
  - `fabric_routing_enabled`: Enable multi-hop forwarding
  - `fabric_routing_max_hops`: Hop budget for RPC requests
  - `persistence_enabled`: Activate persistence framework
  - `persistence_backend`: Select storage backend

- **Federation Configuration**
  - `cluster_id`: Unique cluster identifier for fabric routing
  - `region`: Geographic region for topology optimization
  - `federation_config`: Policy-based routing configuration

### Enhanced Type System

- **Function Identity** (`datastructures/function_identity.py`)
  - Versioned function identifiers with semantic versioning
  - Resource-based function routing
  - Function metadata and capability tracking

- **Cache Models** (`core/cache_models.py`, `core/cache_interfaces.py`)
  - Structured cache entry models
  - Cache profile definitions for federation
  - Multi-tier cache coordination

- **Message Structures** (`datastructures/message_structures.py`)
  - Enhanced RPC envelopes with routing metadata
  - Pub/sub message formats
  - Queue message structures with delivery guarantees

### Monitoring & Observability

- **Unified Monitoring** (`core/monitoring/unified_monitoring.py`)
  - ULID-based distributed tracing
  - Cross-system request tracking
  - Performance metric aggregation

- **System Adapters** (`core/monitoring/system_adapters.py`)
  - OS-level metrics collection
  - Resource utilization tracking
  - System health monitoring

- **Server Monitoring** (`core/monitoring/server_monitoring.py`)
  - Real-time server metrics
  - Connection pool statistics
  - Request latency histograms

### Logging Infrastructure (`core/logging.py`)

- **Centralized Logging**
  - Loguru-based logging with scoped filters
  - Debug scope activation (e.g., `DEBUG_SCOPES=fabric.router,fabric.gossip`)
  - Structured log context for distributed tracing

---

## 🎯 API & Client Changes

### Client API Improvements (`mpreg/client/`)

- **Simplified Client** (`client_api.py`, `client.py`)
  - Removed `client_peer.py` (integrated into main client)
  - Streamlined connection management
  - Better error handling and retries

- **Pub/Sub Client** (`pubsub_client.py`)
  - Fabric-aware topic routing
  - Multi-cluster pub/sub support
  - Improved subscription management

### CLI Enhancements (`mpreg/cli/`)

- **Unified CLI** (`main.py`)
  - `mpreg demo` command for running examples
  - `mpreg discover` for cluster discovery
  - `mpreg health` for federation monitoring
  - `mpreg topology` for fabric visualization

- **Federation CLI** (`federation_cli.py`)
  - Fabric configuration generation
  - Cluster deployment automation
  - Health monitoring and alerting

---

## 📚 Testing & Quality

### Test Suite Expansion

**2,000+ comprehensive tests** (up from 1,800+):

- **Fabric Test Coverage** (60+ new test files)
  - `test_fabric_router_impl.py`: Core routing engine tests
  - `test_fabric_catalog.py`: Catalog management tests
  - `test_fabric_route_control.py`: Route control plane tests
  - `test_fabric_link_state.py`: Link-state routing tests
  - `test_fabric_route_security.py`: Route security validation
  - Integration tests for RPC, pub/sub, queue, and cache federation

- **Persistence Tests**
  - `test_persistence_framework.py`: Backend abstraction tests
  - `test_persistence_restart.py`: Restart/recovery scenarios
  - `test_blockchain_persistence.py`: Blockchain state persistence

- **Property-Based Tests**
  - Enhanced RPC topology tests
  - Federation properties validation
  - Cross-system invariant checks

### Testing Infrastructure

- **Port Allocation** (`core/port_allocator.py`, removed `tests/port_allocator.py`)
  - Moved port allocation to core for reuse in examples
  - Improved concurrent test execution
  - Dynamic port assignment for all tests

- **Test Helpers** (`tests/conftest.py`, `tests/test_helpers.py`)
  - Simplified test fixtures
  - Better async context management
  - Improved cleanup and resource management

### CI/CD Pipeline (`.github/workflows/ci.yml`)

**New GitHub Actions workflows**:

- Demo smoke tests (15 min timeout)
- Full demo suite (30 min timeout)
- Automated validation on push/PR

**Demo Scripts**:

- `scripts/run_demo_smoke.sh`: Fast validation suite
- `scripts/run_demo_suite.sh`: Comprehensive demo execution

---

## 📖 Documentation Overhaul

### New Documentation

- **[docs/README.md](docs/README.md)**: Primary documentation index
- **[docs/UNIFIED_UNIFICATION_PLAN.md](docs/UNIFIED_UNIFICATION_PLAN.md)**: Fabric architecture vision and status
- **[docs/FABRIC_LINK_STATE_ROUTING.md](docs/FABRIC_LINK_STATE_ROUTING.md)**: Link-state routing protocol
- **[docs/FABRIC_ROUTE_POLICIES.md](docs/FABRIC_ROUTE_POLICIES.md)**: Policy-based routing guide
- **[docs/FABRIC_ROUTE_SECURITY.md](docs/FABRIC_ROUTE_SECURITY.md)**: Route security and verification
- **[docs/OBSERVABILITY_TROUBLESHOOTING.md](docs/OBSERVABILITY_TROUBLESHOOTING.md)**: Logging and debugging guide
- **[docs/MANAGEMENT_UI_CLI_NEXT_STEPS.md](docs/MANAGEMENT_UI_CLI_NEXT_STEPS.md)**: Management plane roadmap
- **[docs/PERSISTENCE_FRAMEWORK_PLAN.md](docs/PERSISTENCE_FRAMEWORK_PLAN.md)**: Persistence architecture
- **[docs/TEST_PARITY_MATRIX.md](docs/TEST_PARITY_MATRIX.md)**: Cross-system test coverage matrix

### Updated Documentation

**Architecture & Deployment**:

- `ARCHITECTURE.md`: Unified fabric architecture
- `PRODUCTION_DEPLOYMENT.md`: Fabric-based deployment patterns
- `GETTING_STARTED.md`: Updated for fabric APIs

**Federation Guides** (updated for fabric):

- `FEDERATION_ARCHITECTURE.md`: Fabric control plane architecture
- `FEDERATION_DEVELOPER_QUICK_REFERENCE.md`: Fabric API quick reference
- `FEDERATION_QUICK_START_EXAMPLES.md`: Fabric-based examples
- `FEDERATION_CLI.md` & `FEDERATION_CLI_QUICK_REFERENCE.md`: Updated CLI commands

**System Guides**:

- `MPREG_PROTOCOL_SPECIFICATION.md`: Unified message envelope protocol
- `MPREG_CLIENT_GUIDE.md`: Fabric-aware client usage
- `CACHE_FEDERATION_GUIDE.md`: Fabric-based cache federation
- `FEDERATED_MULTI_CLUSTER_RPC_IMPLEMENTATION.md`: Multi-hop RPC routing

**Examples & Tutorials**:

- `EXAMPLES.md`: Tier-based demo structure
- `WORKING_EXAMPLES.md`: Updated example code

### Documentation Archival (`docs/archive/`)

**Archived planning documents**:

- `FEDERATED_ARCHITECTURE_PROJECT_PLAN.md`
- `FEDERATION_PLANET_SCALE_ROADMAP.md`
- `FEDERATION_SYSTEM_ANALYSIS.md`
- `FABRIC_PATH_VECTOR_ROUTING_PLAN.md` (implemented)
- `UNIFIED_DISTRIBUTED_FABRIC_PLAN.md` (implemented)
- And more...

---

## 🎪 Examples & Demos

### Reorganized Example Structure

**New tier-based demo system**:

- **Tier 1: Single System** (`tier1_single_system_full.py`)
  - Full capability demonstration of each subsystem
  - Run with `--system rpc|pubsub|queue|cache|fabric|monitoring`
  - Quick validation of core functionality

- **Tier 2: Integrations** (`tier2_integrations.py`)
  - Two-system integration scenarios
  - Cross-system coordination examples
  - Real-world integration patterns

- **Tier 3: Full Expansion** (`tier3_full_system_expansion.py`)
  - Complete multi-cluster deployment
  - Planet-scale federation scenarios
  - End-to-end distributed workflows

### New Example Files

- `auto_port_cluster_bootstrap.py`: Dynamic port allocation demo
- `fabric_route_security_demo.py`: Route security validation
- `fabric_snapshot_restart_demo.py`: Persistence and recovery
- `persistence_restart_demo.py`: General persistence demo
- `showcase_utils.py`: Common utilities for examples
- `examples/README.md`: Comprehensive example documentation

### Removed Legacy Examples

**Deleted 15+ outdated example files**:

- Old federation demos (replaced by fabric-based examples)
- Legacy benchmarks (replaced by tier-based demos)
- Redundant cache/queue demos (consolidated)

---

## 🗑️ Removed Components

### Deprecated Federation Modules

**Removed `mpreg/federation/` (11,000+ lines)**:

- `federation_bridge.py`: Replaced by `fabric/router.py`
- `cache_federation_bridge.py`: Replaced by `fabric/cache_federation.py`
- `federated_topic_exchange.py`: Replaced by `fabric/pubsub_router.py`
- `unified_federation.py`: Replaced by `fabric/engine.py`
- `global_cache_federation.py`: Replaced by `fabric/cache_federation.py`
- `leader_election_network_adapter.py`: Integrated into `fabric/raft_transport.py`
- And more...

### Deprecated Core Modules

**Removed from `mpreg/core/`**:

- `unified_routing.py`: Replaced by `fabric/router.py`
- `unified_router_impl.py`: Replaced by `fabric/router.py`
- `federated_message_queue.py`: Replaced by `fabric/queue_federation.py`
- `federated_delivery_guarantees.py`: Integrated into `fabric/queue_delivery.py`
- `cache_gossip.py`: Replaced by `fabric/cache_federation.py`

### Removed Test Files

**Deleted legacy test files**:

- `test_unified_federation.py`
- `test_unified_router_impl.py`
- `test_federated_message_queue*.py` (3 files)
- `test_federated_topic_exchange.py`
- `test_graph_integration.py`
- Legacy debug scripts in `tools/debug/`

---

## 🔄 Migration Guide

### API Migration

#### Old Federation API

```python
from mpreg.federation.federated_topic_exchange import create_federated_cluster
from mpreg.federation.federation_graph import FederationGraph
from mpreg.federation.cache_federation_bridge import CacheFederationBridge

cluster = await create_federated_cluster("ws://...", cluster_id="us-1")
```

#### New Fabric API

```python
from mpreg.fabric.federation_config import create_permissive_bridging_config
from mpreg.fabric import RoutingCatalog, FabricRouter
from mpreg.core.config import MPREGSettings

config = create_permissive_bridging_config("us-1")
settings = MPREGSettings(
    cluster_id="us-1",
    federation_config=config,
    fabric_routing_enabled=True,
)
server = MPREGServer(settings=settings)
```

### Import Path Changes

| Old Path                             | New Path                        |
| ------------------------------------ | ------------------------------- |
| `mpreg.federation.federation_graph`  | `mpreg.fabric.federation_graph` |
| `mpreg.federation.federation_gossip` | `mpreg.fabric.gossip`           |
| `mpreg.federation.federation_hubs`   | `mpreg.fabric.hubs`             |
| `mpreg.federation.auto_discovery`    | `mpreg.fabric.auto_discovery`   |
| `mpreg.core.cache_gossip`            | `mpreg.fabric.cache_federation` |

### Configuration Migration

#### Old Settings

```python
settings = MPREGSettings(
    enable_federation=True,
    federation_cluster_id="us-1",
)
```

#### New Settings

```python
settings = MPREGSettings(
    cluster_id="us-1",
    fabric_routing_enabled=True,
    fabric_routing_max_hops=5,
    persistence_enabled=True,
    persistence_backend="file",
)
```

---

## 📊 Statistics

- **Lines Changed**: 50,293 insertions, 54,395 deletions across 473 files
- **Test Coverage**: 2,000+ tests (up from 1,800+)
- **New Modules**: 60+ new files in `mpreg/fabric/`
- **Documentation**: 40+ documentation files updated/created
- **Examples**: 15+ old examples removed, 10+ new tier-based demos added

---

## 🙏 Notes

This release represents months of architectural planning and implementation. The fabric routing system provides a clean, unified foundation for all distributed coordination in MPREG, enabling:

- **Single Control Plane**: One catalog, one router, one gossip protocol
- **Multi-Hop Routing**: RPC requests can traverse multiple clusters
- **Policy-Based Routing**: Export/import filters, route security, topology control
- **Persistence**: Stateful components survive restarts
- **Production Ready**: Comprehensive testing, monitoring, and operational tooling

All previous functionality is preserved and enhanced through the new fabric architecture. Existing RPC, pub/sub, queue, and cache operations work transparently with improved performance and reliability.

---

## 🔗 References

- **Architecture**: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Unified Plan**: [docs/UNIFIED_UNIFICATION_PLAN.md](docs/UNIFIED_UNIFICATION_PLAN.md)
- **Getting Started**: [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md)
- **Examples**: [mpreg/examples/README.md](mpreg/examples/README.md)
- **Test Coverage**: [docs/TEST_PARITY_MATRIX.md](docs/TEST_PARITY_MATRIX.md)
