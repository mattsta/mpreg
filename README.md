# mpreg: Matt's Protocol for Results Everywhere Guaranteed

**The distributed computing platform making hard distributed coordination simple**

Do you need results? Everywhere? Guaranteed? Then you need to MPREG!

What started as my solution for coordinating ML models across servers has evolved into a **comprehensive distributed computing platform** with sophisticated capabilities:

## Summary the First

- **Dependency-resolving RPC** that figures out function call ordering automatically across servers
- **Planet-scale federation** with gossip clustering and geographic routing (tested up to 100+ nodes)
- **AMQP-style topic exchange** for million+ message/second hierarchical pub/sub
- **SQS-like message queues** with multiple delivery guarantees and dead letter handling
- **Multi-tier caching** with intelligent eviction policies and dependency tracking
- **Production Raft consensus** for distributed coordination and state management
- **Blockchain components** for immutable state and transaction management
- **1,800+ comprehensive tests** with property-based validation and type safety

## Summary the Second

- 🎪 Distributed RPC (Multi-node dependency resolution for request routing)
- 🌐 Topic Exchange (AMQP-style pub/sub with million+ msg/sec)
- 📬 Message Queues (SQS-like with multiple delivery guarantees)
- 🗄️ Smart Caching (S4LRU, dependency-aware, cost-based eviction)
- 🌍 Planet-Scale Federation (geographic routing, hub-and-spoke)
- ⛓️ Blockchain & Consensus (production Raft, Byzantine fault tolerance)
- 🏭 Real-World Examples (8-stage data pipelines, distributed ML inference)

## Summary the Third: 🎯 **Core Capabilities Index**

### 🔄 **Distributed Computing Foundation**

- 🎪 **Dependency-Resolving RPC**: Automatic function call ordering across servers with resource-based routing
- 🌐 **Planet-Scale Federation**: Gossip clustering with geographic routing (tested to 100+ nodes)
- 📡 **Zero-Config Discovery**: Peer-to-peer mesh with automatic cluster formation and health monitoring

### 🚀 **Message & Communication Systems**

- 🌐 **Topic Exchange**: AMQP-style hierarchical pub/sub with million+ message/second throughput
- 📬 **Message Queues**: SQS-compatible queues with multiple delivery guarantees and dead letter handling
- 🔗 **WebSocket Transport**: Persistent connections with pooling, reconnection, and circuit breakers

### 🧠 **Intelligent Data Management**

- 🗄️ **Multi-Tier Caching**: S4LRU algorithm with dependency-aware eviction and cost-benefit analysis
- ⛓️ **Blockchain Components**: Immutable state management with transaction validation
- 📊 **Vector Clocks and Merkle Trees**: Distributed timestamp coordination for consistency guarantees

### 🏗️ **Production Infrastructure**

- 🔒 **Consensus Algorithms**: Production Raft implementation with Byzantine fault tolerance
- 🌍 **Geographic Routing**: Hub-and-spoke federation with Dijkstra/A\* path optimization
- 🔧 **Self-Healing Systems**: Automatic failure detection, recovery, and graceful degradation

### 📈 **Reliability & Monitoring**

- ✅ **Comprehensive Testing**: 1,800+ tests with unit, integration, and property/hypothesis-based validation and type safety
- 📊 **Real-Time Metrics**: Built-in monitoring endpoints with distributed tracing
- 🚨 **Fault Tolerance**: Circuit breakers, split-brain prevention, and network partition handling

### 🎭 **Real-World Applications**

- 🏭 **Data Pipelines**: 8-stage processing workflows with automatic server routing
- 🤖 **ML Inference**: Distributed model serving with preprocessing and post-processing chains
- 🏢 **Enterprise Integration**: Federation CLI and a dozen monitoring endpoints for production deployment

**Status**: This has evolved from a demo proof of concept into a **production-ready distributed computing platform** with comprehensive test coverage and robust architectural patterns. MPREG implements a network-enabled multi-function-call dependency resolver with custom function topologies on every request, now featuring concurrent request handling, self-managing components, automatic cluster discovery, and much more. I still haven't found anything else equivalent to the "gossip cluster group hierarchy low latency late-binding dependency resolution" approach explored here, plus all the additional distributed systems capabilities. A similar system is the nice https://github.com/pipefunc/pipefunc but it is designed around running local things or running in "big cluster mode" so it doesn't meet these distributed coordination ideas.

🎯 **The big idea**: Write functions, not infrastructure. MPREG handles the distributed coordination across all these systems.

## What is it?

`mpreg` allows you to define a distributed cluster multi-call function topology across multiple processes or servers so you can run your requests against one cluster endpoint and automatically receive results from your data anywhere in the cluster.

Basically, `mpreg` helps you decouple "run function X against data Y" without needing to know where "function X" and "data Y" exists in your cluster.

Why is this useful? I made this because I had some models with datasets I wanted to run across multiple servers/processes (they didn't work well multithreaded or forked due to GIL and COW issues), but then I had a problem where I needed 8 processes each with their own port numbers and datasets, but I didn't want to make a static mapping of "host, port, dataset, available functions" — so now, each process can register itself with (available functions, available datasets) and your clients just connect to the cluster and say "run function X on dataset Y" then the cluster auto-routes your requests to the processes having both the required data and functions available.

But it grew into much more than that. What started as simple RPC routing became a comprehensive distributed platform including pub/sub messaging, caching systems, message queues, federation capabilities, consensus mechanisms, and more — all working together seamlessly.

## The Magic: Dependency Resolution That Just Works

Of course, just a simple mapping of "lookup dataset, lookup function, run where both match" isn't entirely interesting.

To spice things up a bit, `mpreg` implements a fully resolvable function call hierarchy as your RPC mechanism. Basically: your RPC function calls can reference the output of other function calls and they all get resolved cluster-side before returned to your client.

This is **dependency resolution at cluster scale** — you describe complex workflows across multiple servers and MPREG figures out the execution order and routing automatically.

## Examples

### Simple Example: Call Things

```python
# Modern API
async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
    result = await client.call("echo", "hi there!")
    # Returns: "hi there!"

# Or using the lower-level client directly
from mpreg.core.model import RPCCommand
result = await client._client.request([
    RPCCommand(name="first", fun="echo", args=("hi there!",), locs=frozenset())
])
```

and it returns the function call value matched to your RPC request name for the function call:

```json
{ "first": "hi there!" }
```

### More Advanced: Use previous function as next argument

RPC requests have your RPC reference name, your target function name, and the function arguments. The trick here is: if your function arguments match the name of other RPC reference names, the other RPC is resolved first, then the RPC's return value is used in place of the name.

We can also call multiple functions at once with unique names:

```python
# Modern dependency resolution - these execute in proper order automatically
result = await client._client.request([
    RPCCommand(name="first", fun="echo", args=("hi there!",), locs=frozenset()),
    RPCCommand(name="second", fun="echo", args=("first",), locs=frozenset()),  # Uses result from "first"
])
```

and it returns the `first` RPC returned value as the parameter to the `second` name:

```python
{"second": "hi there!"}
```

### More Clear: Name RPC Names Better

Direct string matching on the function parameters can be confusing as above with "first" suddenly becoming a magic value, so let's name them better:

```python
result = await client._client.request([
    RPCCommand(name="|first", fun="echo", args=("hi there!",), locs=frozenset()),
    RPCCommand(name="|second", fun="echo", args=("|first",), locs=frozenset()),
    RPCCommand(name="|third", fun="echos", args=("|first", "AND ME TOO"), locs=frozenset()),
])
```

and this one returns:

```json
{ "|second": "hi there!", "|third": ["hi there!", "AND ME TOO"] }
```

Note how it returns all FINAL level RPCs having no further resolvable arguments (so `mpreg` supports one-call-with-multiple-return-values just fine).

### More Examples:

#### 3-returns-1 using multiple replacements

```python
result = await client._client.request([
    RPCCommand(name="|first", fun="echo", args=("hi there!",), locs=frozenset()),
    RPCCommand(name="|second", fun="echo", args=("|first",), locs=frozenset()),
    RPCCommand(name="|third", fun="echos", args=("|first", "|second", "AND ME TOO"), locs=frozenset()),
])
```

returns:

```json
{ "|third": ["hi there!", "hi there!", "AND ME TOO"] }
```

Note how here it returns only `|third` because `third` contains _both_ `|first` _and_ `|second` (so all return values have been resolved in the final result).

#### 4-returns-1 using multiple replacements

```python
result = await client._client.request([
    RPCCommand(name="|first", fun="echo", args=("hi there!",), locs=frozenset()),
    RPCCommand(name="|second", fun="echo", args=("|first",), locs=frozenset()),
    RPCCommand(name="|third", fun="echos", args=("|first", "|second", "AND ME TOO"), locs=frozenset()),
    RPCCommand(name="|4th", fun="echo", args=("|third",), locs=frozenset()),
])
```

returns:

```json
{ "|4th": ["hi there!", "hi there!", "AND ME TOO"] }
```

### Extra Argument

You may have noticed the `locs=frozenset()` parameter in all those `RPCCommand()` calls. For these `echo` tests there's no specific dataset to consult, but if your cluster had named datasets/resources registered, you'd provide your resource name(s) there:

```python
# Route to specific resources/datasets
result = await client.call("train_model", training_data, locs=frozenset(["gpu-cluster", "dataset-v2"]))
```

When called fully with `(name, function, args, locs)`, the cluster routes your request to the best matching cluster nodes having `(function, resource)` matches (because you may have common functions like "run model" but the output changes depending on _which model/dataset_ you are running against).

`mpreg` cluster nodes can register multiple datasets and your RPC requests can also provide multiple dataset requests per call. Your RPC request will only be sent to a cluster node matching _all_ your datasets requested (but the server can have _more_ datasets than your request, so it doesn't need to be a 100% server-dataset-match).

This quad tuple of `(name, function, args, dataset)` actually simplifies your workflows because now you don't need to make 20 different function names for running datasets — you just have common functions but custom data defined on each node, then the cluster knows how to route your requests based both on requests datasets and requested function name availability (if multiple cluster nodes have the same functions and datasets registered, matches are randomly load balanced when requested).

## Complete System Showcase

MPREG has grown into a comprehensive distributed platform with multiple production-ready systems working together:

### 🌐 Topic Exchange (AMQP-Style Pub/Sub)

```python
# Million+ message/second hierarchical topic routing
from mpreg.core.topic_exchange import TopicExchange

exchange = TopicExchange("ws://localhost:9001", "demo_cluster")

# AMQP-style hierarchical topics with wildcard matching
exchange.add_subscription("user_events", ["user.*.login", "user.*.logout"])
exchange.add_subscription("orders", ["order.#", "payment.*.completed"])  # # = multi-level wildcard

# Publish to specific topics - automatic routing with sub-millisecond latency
exchange.publish_message("user.123.login", {"username": "alice", "ip": "192.168.1.100"})
exchange.publish_message("order.us.12345.created", {"amount": 99.99, "region": "us"})

# Try it: poetry run python mpreg/examples/topic_exchange_demo.py
```

### 📬 Message Queues (SQS-Like Reliability)

```python
# Multiple delivery guarantees for different reliability needs
from mpreg.core.message_queue_manager import create_reliable_queue_manager
from mpreg.core.message_queue import DeliveryGuarantee

manager = create_reliable_queue_manager()

# Different delivery guarantees for different use cases
await manager.send_message("urgent_queue", data, DeliveryGuarantee.AT_LEAST_ONCE)  # Retry until ack
await manager.send_message("broadcast_queue", data, DeliveryGuarantee.BROADCAST)   # All subscribers
await manager.send_message("consensus_queue", data, DeliveryGuarantee.QUORUM)     # N acknowledgments

# Supports FIFO, Priority, and Delay queues with dead letter handling
# Try it: poetry run python mpreg/examples/message_queue_demo.py
```

### 🗄️ Smart Multi-Tier Caching

```python
# Intelligent caching with dependency tracking and multiple eviction policies
from mpreg.core.caching import create_performance_cache_manager, EvictionPolicy

cache = create_performance_cache_manager()

# Multiple intelligent eviction policies
cache.configure(eviction_policy=EvictionPolicy.DEPENDENCY_AWARE)  # Tracks function dependencies
cache.configure(eviction_policy=EvictionPolicy.COST_BASED)        # Cost-benefit analysis
cache.configure(eviction_policy=EvictionPolicy.S4LRU)            # Segmented LRU with promotion

# Automatic dependency tracking and cascade invalidation
key = CacheKey.create("expensive_function", args, kwargs)
cache.put(key, result, dependencies=["data_source_1", "model_v2"])

# Try it: poetry run python mpreg/examples/caching_demo.py
```

### 🌍 Planet-Scale Federation

```python
# Geographic federation with gossip clustering and intelligent routing
from mpreg.federation.federated_topic_exchange import create_federated_cluster, connect_clusters

# Create federated clusters across regions with automatic discovery
us_cluster = await create_federated_cluster("ws://us-west.company.com:9001",
                                            cluster_id="us-west-1", region="us-west")
eu_cluster = await create_federated_cluster("ws://eu-central.company.com:9001",
                                            cluster_id="eu-central-1", region="eu-central")

# Connect via federation bridges (hub-and-spoke topology, not full mesh)
await connect_clusters(us_cluster, eu_cluster)  # Cross-continental coordination

# Messages route intelligently across continents with bloom filter optimization
# Try it: poetry run python mpreg/examples/federation_demo.py
```

### ⛓️ Blockchain & Distributed Consensus

```python
# Built-in blockchain and production Raft consensus
from mpreg.datastructures.blockchain import Blockchain, ConsensusType
from mpreg.datastructures.production_raft import ProductionRaft

# Immutable blockchain with multiple consensus mechanisms
blockchain = Blockchain(consensus_type=ConsensusType.PROOF_OF_AUTHORITY)
transaction = blockchain.create_transaction("sender_node", "receiver_node", amount=100)

# Production Raft implementation for distributed state agreement
raft = ProductionRaft(node_id="node1", cluster_nodes=["node1", "node2", "node3"])
await raft.start()  # Handles leader election, log replication, membership changes

# Try it: poetry run python mpreg/examples/planet_scale_integration_example.py
```

### 🏭 Real-World Usage Examples

```python
# Data Processing Pipeline (8-stage workflow across specialized servers)
# Ingestion → Validation → Cleaning → Analytics → Insights → Storage → Dashboard
# Each stage automatically routes to servers with required resources (CPU/GPU/Database)

result = await client._client.request([
    # Stage 1: Data ingestion server
    RPCCommand(name="ingested", fun="ingest_sensor_data",
               args=(sensor_id, readings), locs=frozenset(["ingestion", "raw-data"])),

    # Stage 2: Processing server
    RPCCommand(name="cleaned", fun="clean_data",
               args=("ingested",), locs=frozenset(["processing", "etl"])),

    # Stage 3: Analytics server
    RPCCommand(name="analyzed", fun="detect_anomalies",
               args=("cleaned",), locs=frozenset(["analytics", "ml"])),

    # Stage 4: Storage server
    RPCCommand(name="stored", fun="store_data",
               args=("analyzed",), locs=frozenset(["storage", "database"])),
])

# MPREG automatically figures out the execution order and routes each
# function to the optimal server based on resource requirements
# Try it: poetry run python mpreg/examples/real_world_examples.py
```

### 🤖 Distributed ML Inference

```python
# Route ML inference to specialized model servers automatically
result = await client._client.request([
    # Route to image preprocessing server
    RPCCommand(name="preprocessed", fun="preprocess_image",
               args=(image_data,), locs=frozenset(["preprocessing"])),

    # Route to vision model server
    RPCCommand(name="classified", fun="classify_image",
               args=("preprocessed",), locs=frozenset(["vision", "gpu"])),

    # Route to NLP server for description
    RPCCommand(name="described", fun="generate_description",
               args=("classified",), locs=frozenset(["nlp", "text-generation"])),
])

# Client just describes the ML pipeline - MPREG routes to optimal servers
# Try it: poetry run python mpreg/examples/real_world_examples.py
```

## 📊 Additional Examples & Benchmarks

Beyond the core examples above, MPREG includes comprehensive demonstrations and benchmarking tools:

### 🎪 **Specialized Demos**

```bash
# Performance and monitoring demos
poetry run python mpreg/examples/unified_monitoring_demo.py      # ULID-based cross-system tracking
poetry run python mpreg/examples/benchmark_demo.py              # Performance benchmarking suite
poetry run python mpreg/examples/intermediate_results_demo.py   # Intermediate result handling

# Advanced federation examples
poetry run python mpreg/examples/federation_alerting_demo.py    # Federation alerting system
poetry run python mpreg/examples/federated_queue_examples.py    # Cross-cluster message queues
poetry run python mpreg/examples/planet_scale_integration_example.py # Complete planet-scale demo

# Specialized system demonstrations
poetry run python mpreg/examples/cache_client_server_demo.py    # Cache client/server architecture
poetry run python mpreg/examples/topic_exchange_benchmark.py   # Topic exchange performance testing
```

### 🔧 **Debug and Analysis Tools**

The `tools/debug/` directory contains 60+ specialized debugging scripts for deep system analysis:

```bash
# Federation and scalability analysis
tools/debug/debug_federation_scaling.py          # Federation scalability analysis
tools/debug/debug_auto_discovery_5node.py        # Auto-discovery mechanism testing
tools/debug/debug_planet_scale_deep_dive.py      # Planet-scale consensus debugging

# Performance and replication testing
tools/debug/benchmark_replication_performance.py # Replication performance testing
tools/debug/debug_append_entries_traffic.py      # Raft append entries analysis
tools/debug/debug_commit_timing.py               # Consensus commit timing analysis

# Consensus and voting debugging
tools/debug/debug_split_brain_prevention.py      # Split-brain prevention testing
tools/debug/debug_vote_network.py                # Vote network analysis
tools/debug/debug_leader_election_failures.py    # Leader election failure analysis
```

## 🧪 Testing Infrastructure

MPREG includes sophisticated testing infrastructure with 1,800+ tests supporting concurrent execution:

### 🔄 **Concurrent Testing Support**

```bash
# Automated concurrent testing
./run_concurrent_tests.sh                          # Run concurrent test demos

# Manual concurrent testing with pytest-xdist
poetry run pytest -n auto                          # Auto-detect CPU cores
poetry run pytest -n 20 -m "not slow"              # 20 workers, skip slow tests
poetry run pytest tests/integration/ -n 15         # Integration tests with 15 workers
```

### 📚 **Testing Documentation**

- **[Port Migration Guide](tests/README_PORT_MIGRATION.md)** - Essential for developers adding new tests with dynamic port allocation
- **[Concurrent Testing Status](tests/CONCURRENT_TESTING_STATUS.md)** - Current testing infrastructure status and migration progress
- **Property-based testing** with Hypothesis in `tests/property_tests/`
- **Integration test suites** in `tests/integration/`
- **Performance test framework** in `tests/performance/`

### 🏃‍♂️ **Test Categories**

```bash
# Run specific test categories
poetry run pytest -m "unit"              # Unit tests only
poetry run pytest -m "integration"       # Integration tests only
poetry run pytest -m "property"          # Property-based tests only
poetry run pytest -m "performance"       # Performance tests only
poetry run pytest -m "federation"        # Federation system tests
```

## Quick Demo Running

### Intial Setup

```bash
pip install poetry -U

[clone repo and use clone]

poetry install
```

### Terminal 1 (Server 1)

```bash
# Run a server with specific resources
poetry run python -c "
from mpreg.server import MPREGServer
from mpreg.core.config import MPREGSettings
import asyncio

server = MPREGServer(MPREGSettings(
    port=9001,
    name='Primary Server',
    resources={'model-a', 'dataset-1'}
))
asyncio.run(server.server())
"
```

### Terminal 2 (Server 2)

```bash
# Run a second server that connects to the first
poetry run python -c "
from mpreg.server import MPREGServer
from mpreg.core.config import MPREGSettings
import asyncio

server = MPREGServer(MPREGSettings(
    port=9002,
    name='Secondary Server',
    resources={'model-b', 'dataset-2'},
    peers=['ws://127.0.0.1:9001']
))
asyncio.run(server.server())
"
```

### Terminal 3 (Client)

```python
# Connect and make calls
from mpreg.client.client_api import MPREGClientAPI
import asyncio

async def main():
    async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
        # Simple call
        result = await client.call("echo", "Hello MPREG!")
        print(f"Result: {result}")

        # Multi-step workflow
        from mpreg.core.model import RPCCommand
        workflow = await client._client.request([
            RPCCommand(name="step1", fun="echo", args=("first step",), locs=frozenset()),
            RPCCommand(name="step2", fun="echo", args=("step1",), locs=frozenset()),
        ])
        print(f"Workflow result: {workflow}")

asyncio.run(main())
```

## 🛠️ Federation Management CLI

MPREG includes a comprehensive command-line interface for managing federated clusters in production environments.

### Quick CLI Examples

```bash
# Discover available clusters
poetry run mpreg-federation discover

# Generate configuration template
poetry run mpreg-federation generate-config federation.json

# Validate configuration
poetry run mpreg-federation validate-config federation.json

# Deploy federation from configuration
poetry run mpreg-federation deploy federation.json

# Monitor cluster health
poetry run mpreg-federation health
poetry run mpreg-federation monitor health-watch --interval 30

# Show federation topology
poetry run mpreg-federation topology
```

### Production Configuration Example

```json
{
  "version": "1.0",
  "federation": {
    "enabled": true,
    "health_check_interval": 30,
    "resilience": {
      "circuit_breaker": {
        "failure_threshold": 5,
        "success_threshold": 3,
        "timeout_seconds": 60
      }
    }
  },
  "clusters": [
    {
      "cluster_id": "prod-us-west",
      "cluster_name": "Production US West",
      "region": "us-west-2",
      "server_url": "ws://cluster.company.com:8000",
      "bridge_url": "ws://federation.company.com:9000",
      "resources": ["compute", "storage", "ml-inference"]
    }
  ]
}
```

### Complete Documentation

- **[Full CLI Documentation](docs/FEDERATION_CLI.md)** - Comprehensive guide with real-world scenarios
- **[Quick Reference](docs/FEDERATION_CLI_QUICK_REFERENCE.md)** - Essential commands and templates

## 📚 Comprehensive Documentation Library

### 🏗️ **System Architecture & Deployment**

- **[Production Deployment Guide](docs/PRODUCTION_DEPLOYMENT.md)** - Complete production setup with system requirements, scaling, and operational procedures
- **[Client Usage Guide](docs/MPREG_CLIENT_GUIDE.md)** - RPC, PubSub, and Cache clients with multi-language examples and protocol documentation
- **[Caching System Guide](docs/CACHING_SYSTEM.md)** - Multi-tier caching with S4LRU and dependency-aware eviction

### ⛓️ **Blockchain & Data Structures**

- **[Blockchain Implementation Guide](docs/BLOCKCHAIN_COMPLETE_GUIDE.md)** - Production-ready blockchain with multiple consensus mechanisms and federation integration
- **[Vector Clock Guide](docs/VECTOR_CLOCK_GUIDE.md)** - Distributed timestamp coordination for causality tracking
- **[Merkle Tree Guide](docs/MERKLE_TREE_GUIDE.md)** - Cryptographic data integrity with O(log n) verification proofs
- **[DAO Integration Guide](docs/DAO_COMPREHENSIVE_GUIDE.md)** - Decentralized governance patterns and voting mechanisms

### 📬 **Messaging & Communication Systems**

- **[SQS Message Queue System](docs/SQS_MESSAGE_QUEUE_SYSTEM.md)** - Multiple delivery guarantees and dead letter handling
- **[MPREG Protocol Specification](docs/MPREG_PROTOCOL_SPECIFICATION.md)** - Complete network protocol documentation

### 🌐 **Federation System Documentation**

**⚠️ Critical for Developers Working with Federation**:

- **[Federation Architecture & Fault Tolerance](docs/FEDERATION_ARCHITECTURE_AND_FAULT_TOLERANCE.md)** - Complete technical documentation of the federation system, including Byzantine fault tolerance and critical edge cases
- **[Byzantine Fault Detection Debug Guide](docs/BYZANTINE_FAULT_DETECTION_DEBUG_GUIDE.md)** - Essential debugging guide for consensus system issues, including a critical bug discovery and resolution
- **[Federation Developer Quick Reference](docs/FEDERATION_DEVELOPER_QUICK_REFERENCE.md)** - Quick reference guide for developers working with the federation system

These documents detail fragile components, edge cases, and architectural decisions discovered through deep debugging sessions. **Essential reading** before modifying federation or consensus code.

## 🏗️ Architecture & Module Organization

MPREG is organized into a clean, modular architecture with well-separated concerns:

### 📁 **Core Modules (`mpreg.core`)**

- **`config.py`** - Configuration management with `MPREGSettings`
- **`model.py`** - Data models, RPC commands, and PubSub messages
- **`registry.py`** - Command registry and function management
- **`serialization.py`** - JSON serialization with error handling
- **`connection.py`** - WebSocket connection management
- **`topic_exchange.py`** - Topic-based message routing
- **`caching.py`** - Multi-tier cache manager with S4LRU algorithm
- **`message_queue.py`** - SQS-compatible message queues with delivery guarantees
- **`enhanced_rpc.py`** - Advanced RPC features with circuit breakers
- **`statistics.py`** - Performance metrics and monitoring data structures

### 🌐 **Federation System (`mpreg.federation`)**

Planet-scale distributed coordination with:

- **`federation_graph.py`** - Graph-based routing with geographic optimization
- **`federation_hubs.py`** - Hub-and-spoke architecture (Local → Regional → Global)
- **`federation_gossip.py`** - Epidemic information propagation with vector clocks
- **`federation_consensus.py`** - Distributed state management with conflict resolution
- **`federation_membership.py`** - SWIM-based failure detection and membership
- **`federation_alerting.py`** - Real-time federation health monitoring and alerting
- **`auto_discovery.py`** - Automatic peer discovery and cluster formation
- **`federation_bridge.py`** - Cross-cluster communication bridges
- **`federation_registry.py`** - Federated service registry and discovery

### 👥 **Client APIs (`mpreg.client`)**

- **`client.py`** - Low-level WebSocket client
- **`client_api.py`** - High-level `MPREGClientAPI` with context manager support
- **`pubsub_client.py`** - Publish/subscribe messaging client

### 🗄️ **Data Structures (`mpreg.datastructures`)**

Advanced distributed data structures and algorithms:

- **`blockchain.py`** - Production blockchain with multiple consensus mechanisms
- **`production_raft.py`** - Complete Raft consensus implementation with safety guarantees
- **`merkle_tree.py`** - Cryptographic data integrity with O(log n) proofs
- **`vector_clock.py`** - Distributed timestamp coordination for causality
- **`trie.py`** - Efficient prefix matching for topic routing
- **`dao.py`** - Decentralized governance and voting mechanisms
- **`leader_election.py`** - Multiple leader election algorithms (Raft, Quorum, Metric-based)

### 🔧 **Tools & Development (`tools/`)**

Comprehensive debugging and analysis utilities:

- **`tools/debug/`** - 60+ specialized debugging scripts for federation, consensus, and performance analysis
- **Performance benchmarking** - `benchmark_replication_performance.py`, `debug_federation_scaling.py`
- **Federation testing** - `debug_auto_discovery_5node.py`, `debug_planet_scale_deep_dive.py`
- **Consensus debugging** - `debug_split_brain_prevention.py`, `debug_vote_network.py`

### 📚 **Comprehensive Import Examples**

```python
# Core functionality
from mpreg.core.model import RPCCommand, PubSubMessage
from mpreg.core.config import MPREGSettings

# Client APIs (see docs/MPREG_CLIENT_GUIDE.md for complete examples)
from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.pubsub_client import MPREGPubSubClient

# Advanced caching systems
from mpreg.core.caching import create_performance_cache_manager, EvictionPolicy
from mpreg.core.enhanced_caching_factories import CacheKey

# Message queue systems
from mpreg.core.message_queue_manager import create_reliable_queue_manager
from mpreg.core.message_queue import DeliveryGuarantee

# Topic exchange and routing
from mpreg.core.topic_exchange import TopicExchange
from mpreg.core.topic_dependency_resolver import TopicDependencyResolver

# Federation system (for advanced use cases)
from mpreg.federation import (
    FederationGraph,
    GeographicCoordinate,
    GossipProtocol,
    ConsensusManager
)

# Blockchain and consensus
from mpreg.datastructures.blockchain import Blockchain, ConsensusType
from mpreg.datastructures.production_raft import ProductionRaft
from mpreg.datastructures.dao import DAOGovernance, ProposalType

# Federation and distributed coordination
from mpreg.federation.federated_topic_exchange import create_federated_cluster
from mpreg.federation.federation_graph import FederationGraph
from mpreg.federation.auto_discovery import create_auto_discovery_service

# Monitoring and observability
from mpreg.core.monitoring.unified_monitoring import create_unified_system_monitor
from mpreg.federation.federation_alerting import FederationAlertingSystem

# Convenient top-level imports
from mpreg import FederationGraph, GeographicCoordinate, MPREGClientAPI
```

## Status

The above demos all work! The system has evolved significantly since the early prototype days and now includes:

✅ **Production-Ready**: Comprehensive test coverage (1,800+ tests) and robust error handling  
✅ **Modern Client API**: Easy-to-use `MPREGClientAPI` with context manager support  
✅ **Concurrent Requests**: Multiple simultaneous requests over single connections  
✅ **Self-Managing Components**: Automatic connection pooling, peer discovery, and cleanup  
✅ **Distributed Coordination**: Gossip protocol for cluster formation and function discovery  
✅ **Resource-Based Routing**: Intelligent function routing based on available datasets/resources

To register your own functions with custom datasets/resources, you can now easily do:

```python
# Register custom functions on your server
server.register_command("my_function", my_callable, ["my-dataset", "gpu-required"])

# Client automatically discovers and routes to the right server
result = await client.call("my_function", args, locs=frozenset(["my-dataset"]))
```

## 🏗️ System Architecture & Components

MPREG implements a sophisticated **multi-layer distributed architecture** designed for both development simplicity and production scalability:

### 🌟 **Core Architecture Principles**

- **Peer-to-Peer Design**: Every server can act as router, load balancer, or compute node
- **Resource-Based Routing**: Functions execute on servers with required datasets/compute resources
- **Gossip-Based Discovery**: Automatic peer discovery and function advertisement
- **Zero-Config Clustering**: Servers auto-discover and self-organize without central coordination
- **Geographic Federation**: Planet-scale routing with hub-and-spoke topology optimization

### 🔧 **Server Configuration**

Each MPREG server is configured with three core components:

```python
server = MPREGServer(MPREGSettings(
    name='Analytics Server',           # Human-readable server identity
    resources={'gpu', 'dataset-v2'},   # Available resources this server provides
    peers=['ws://hub.company.com:9001'] # Peer servers to connect with (optional)
))
```

**Resource Types**:

- **Compute Resources**: `gpu`, `high-memory`, `fast-cpu`, `distributed-storage`
- **Dataset Resources**: `customer-data`, `model-v3`, `real-time-feed`
- **Service Resources**: `database`, `cache`, `message-queue`, `analytics`
- **Geographic Resources**: `us-west`, `eu-central`, `asia-pacific`

Servers with **no resources** automatically become **load balancers** and **routers**, handling request distribution and cluster coordination.

### 📡 **Gossip Protocol & Discovery**

MPREG implements epidemic-style gossip for robust distributed coordination:

- **Function Advertisement**: Servers gossip available functions and resources every 30 seconds
- **Health Monitoring**: Vector clock-based failure detection with configurable timeouts
- **Membership Changes**: Dynamic cluster membership with automatic cleanup of failed nodes
- **Network Partitions**: Split-brain prevention through quorum-based decision making

### 🌍 **Federation Topology**

For planet-scale deployments, MPREG supports hierarchical federation:

```
Local Clusters → Regional Hubs → Global Federation
      ↓              ↓               ↓
   5-50 nodes    100-500 nodes   1000+ nodes
```

**Key Benefits**:

- **Reduced Latency**: Requests route to geographically closest resources
- **Fault Isolation**: Regional failures don't affect global availability
- **Efficient Scaling**: Hub-and-spoke prevents O(n²) connection growth
- **Cost Optimization**: Data gravity routing minimizes cross-region transfers

## 🚀 Production Deployment Status

MPREG has evolved from experimental prototype to **production-ready distributed platform** with enterprise-grade reliability:

### ✅ **Production Readiness Checklist**

**🏗️ Architecture & Scalability**

- ✅ **Tested to 100+ nodes** in federation scenarios
- ✅ **Planet-scale geographic routing** with hub-and-spoke optimization
- ✅ **Zero-downtime rolling updates** with graceful connection migration
- ✅ **Horizontal scaling** with automatic load balancing
- ✅ **Network partition tolerance** with split-brain prevention

**🔐 Reliability & Safety**

- ✅ **1,800+ comprehensive tests** covering edge cases and failure scenarios
- ✅ **Byzantine fault tolerance** with production Raft consensus
- ✅ **Circuit breaker patterns** for cascading failure prevention
- ✅ **Graceful degradation** under high load and network stress
- ✅ **Comprehensive error handling** with proper timeout management

**📊 Monitoring & Observability**

- ✅ **Real-time performance metrics** with built-in monitoring endpoints
- ✅ **Distributed tracing** for request flow analysis across clusters
- ✅ **Health checks** with configurable alerting thresholds
- ✅ **Federation topology visualization** for operational awareness
- ✅ **Comprehensive logging** with structured event correlation

**⚡ Performance & Efficiency**

- ✅ **Sub-millisecond routing decisions** with intelligent caching
- ✅ **Million+ message/second throughput** in topic exchange systems
- ✅ **Memory-efficient implementations** with configurable resource limits
- ✅ **Connection pooling** with persistent WebSocket optimization
- ✅ **Bloom filter optimization** for efficient federation routing

### 🎉 **Recent Major Improvements (v2.0)**

**🔄 Client & Connection Management**:

- **Concurrent Request Processing**: Multiple simultaneous requests over single connections using Future-based dispatching
- **Advanced Connection Pooling**: Persistent WebSocket connections with automatic reconnection and health monitoring
- **Modern Client API**: Easy `MPREGClientAPI` with context manager support and proper async/await patterns

**🌐 Federation & Geographic Scaling**:

- **Graph-Based Routing**: Dijkstra and A\* algorithms for optimal multi-hop federation routing with geographic heuristics
- **Dynamic Cluster Membership**: SWIM-based failure detection with automatic node discovery and removal
- **Hub-and-Spoke Optimization**: Hierarchical federation reducing connection complexity from O(n²) to O(log n)

**💾 Advanced Caching & Storage**:

- **S4LRU Algorithm**: Segmented LRU with intelligent promotion for better cache hit rates
- **Dependency-Aware Eviction**: Cascade invalidation based on function dependencies
- **Multi-Tier Architecture**: L1/L2 cache hierarchy with configurable eviction policies
- **Cost-Benefit Analysis**: Intelligent caching decisions based on computation cost vs. memory usage

**🔗 Message Queues & Pub/Sub**:

- **SQS-Compatible Queues**: Multiple delivery guarantees (fire-and-forget, at-least-once, broadcast, quorum)
- **AMQP-Style Topic Exchange**: Hierarchical topic routing with wildcard matching (`user.*.login`, `order.#`)
- **Dead Letter Handling**: Automatic retry and dead letter queue management for failed messages
- **Federated Pub/Sub**: Cross-cluster message routing with bloom filter optimization

## 🔮 Future Roadmap & Enhancements

MPREG continues evolving toward an even more comprehensive distributed computing platform:

### 🎯 **Roadmap**

**🔐 Security & Authentication**

- **OAuth2/OIDC Integration**: Enterprise-grade authentication and authorization
- **TLS Encryption**: End-to-end encryption for all cluster communication
- **RBAC (Role-Based Access Control)**: Fine-grained permissions for functions and resources
- **API Key Management**: Secure client authentication with rotation support

**📊 Enhanced Monitoring & Observability**

- **Prometheus Metrics**: Native Prometheus export for comprehensive monitoring
- **OpenTelemetry Integration**: Distributed tracing with industry-standard tooling
- **Real-time Dashboards**: Built-in web UI for cluster health and performance visualization
- **Automated Alerting**: Configurable alerts for performance anomalies and failures

**⚡ Performance & Optimization**

- **CloudPickle Support**: Binary serialization for complex Python objects beyond JSON
- **Adaptive Load Balancing**: ML-based routing decisions using historical performance data
- **Memory-Mapped Caching**: Zero-copy data sharing between processes on same nodes

**🤖 Intelligent Automation**

- **Auto-scaling Based on Load**: Dynamic cluster scaling with predictive capacity planning
- **Self-Healing Infrastructure**: Automatic recovery from node failures and network partitions
- **Performance-Driven Optimization**: AI-powered resource allocation and routing optimization
- **Cost-Aware Scaling**: Cloud cost optimization with intelligent instance management

**🌍 Multi-Cloud & Hybrid Deployments**

- **Cloud Provider Abstraction**: Deploy across AWS, GCP, Azure with federation bridges
- **Edge Computing Integration**: Extend federation to edge devices and IoT deployments

**🔬 Advanced Distributed Algorithms**

- **CRDT (Conflict-free Replicated Data Types)**: Eventually consistent distributed data structures
- **Vector Database Integration**: Native support for embedding search and similarity queries
- **Stream Processing**: Real-time event processing with windowing and aggregation
- **Distributed Machine Learning**: Federated learning with privacy-preserving computation

### ✅ **Recently Completed**

- ✅ ~~Add comprehensive automated test suite~~ **DONE!** (384+ tests covering distributed scenarios)
- ✅ ~~Modern client library with async/await~~ **DONE!** (MPREGClientAPI with context managers)
- ✅ ~~Easy server function registration~~ **DONE!** (server.register_command() interface)
- ✅ ~~Planet-scale federation capabilities~~ **DONE!** (Geographic routing with hub-and-spoke)
- ✅ ~~Production-ready consensus algorithms~~ **DONE!** (Raft implementation with safety guarantees)
- ✅ ~~Advanced caching with multiple eviction policies~~ **DONE!** (S4LRU, dependency-aware, cost-based)
- ✅ ~~Message queues and pub/sub systems~~ **DONE!** (SQS-like queues, AMQP-style topics)

**Contributing**: The codebase is well-documented with comprehensive tests. New features are designed with backwards compatibility and extensive error handling. See the `docs/`, `mpreg/examples/`, `tests/`, and `tools/` directories for architectural decision records and implementation guides.
