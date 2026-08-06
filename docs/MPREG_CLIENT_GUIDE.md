# MPREG Client Guide

This guide covers the four main types of MPREG clients and how to use them over network protocols.
Example URLs use placeholder ports; for live runs, allocate ports dynamically and share the
resulting `MPREG_URL` with clients.

## Overview

For most application code, start with the unified façade:

```python
from mpreg import MPREGClient  # also: UnifiedMPREGClient

async with MPREGClient("ws://127.0.0.1:<port>") as client:
    result = await client.call("echo", "hello")
    # Dev/single-node/cluster/federated profiles enable queue+cache by default.
    # await client.queue_send("jobs", payload={"x": 1})
    # await client.queue_ack("jobs", message_id, subscriber_id)
    # await client.queue_receive("jobs", timeout_seconds=5.0, auto_acknowledge=True)
    # await client.cache_put("ns", "key", value={"v": 1})
    # await client.cache_invalidate("ns*")
    # await client.publish_with_reply("topic", {"ping": True})
```

`MPREGClient` spans RPC, pub/sub, queue, and cache over the same transport.
Queue and cache RPCs are only available when the server has those managers
attached — `mpreg config-check` warns when both defaults are off.

Lower-level clients remain available for specialized use:

1. **MPREGClientAPI** — RPC-focused API (base of the unified façade)
2. **MPREG Cluster Client** — HA access with auto-discovery and failover
3. **MPREG PubSub Client** — topic-based messaging and event routing
4. **In-process cache/queue managers** — same-process attach path (not remote RPC)

All clients operate over standard network protocols (WebSocket, TCP).
External clients should follow the wire protocol in
`docs/MPREG_PROTOCOL_SPECIFICATION.md`.

### Structured errors and retries

See [Call retries and deadlines](#call-retries-and-deadlines) for
`ClientCallPolicy`, `MpregError` codes, and HA non-retryable short-circuit
behavior on `MPREGClusterClient`.

---

## 1. MPREG RPC Client

**Use Case**: Distributed computing, microservices, function orchestration

Use `target_cluster` to constrain execution to a federated cluster and
`routing_topic` to apply fabric routing policies when needed.

### Python Example

```python
from mpreg.client.client_api import MPREGClientAPI

async def main():
    async with MPREGClientAPI("ws://cache-server:<port>") as client:
        # Simple function call
        result = await client.call("fibonacci", 10)
        print(f"fib(10) = {result}")

        # Function with identity + version constraints
        result = await client.call(
            "fibonacci",
            10,
            function_id="math.fibonacci",
            version_constraint=">=1.0.0,<2.0.0",
        )
        print(f"fib(10) v1.x = {result}")

        # Function with caching
        ml_result = await client.call("ml_predict", "bert-large", [1.0, 2.0, 3.0])
        print(f"Prediction: {ml_result}")

        # Dependency chain execution (public request / call_dag — not private _client)
        from mpreg.core.model import RPCCommand

        result = await client.request(
            [
                RPCCommand(
                    name="step1",
                    fun="process_data",
                    args=(data,),
                    function_id="pipeline.process_data",
                    version_constraint=">=2.0.0,<3.0.0",
                ),
                RPCCommand(
                    name="step2", fun="analyze", args=("step1",)
                ),  # Uses step1 result
                RPCCommand(name="final", fun="summarize", args=("step2",)),
            ]
        )
```

### External Clients

For JavaScript/Go/Rust/etc., implement the MPREG wire protocol and message
envelopes. See `docs/MPREG_PROTOCOL_SPECIFICATION.md` for the exact payloads
and framing details.

---

## 2. MPREG Cluster Client (HA + Discovery)

**Use Case**: High availability client access with endpoint discovery and failover

The cluster client bootstraps from seed URLs, periodically refreshes the cluster map,
scores endpoints by load, and applies latency/error penalties from recent calls
for stable failover.

### Python Example

```python
from mpreg.client.cluster_client import MPREGClusterClient

async def main():
    client = MPREGClusterClient(seed_urls=("ws://hub-1:<port>", "ws://hub-2:<port>"))
    await client.connect()

    # Auto-selects the healthiest endpoint based on cluster map load metrics
    result = await client.call(
        "quote",
        "EURUSD",
        function_id="market.quote",
        version_constraint=">=1.0.0,<2.0.0",
    )
    print(result)

    # Refresh the discovery view explicitly (optional)
    await client.refresh_cluster_map()
    await client.disconnect()
```

### Cluster Map Snapshot

The cluster client consumes the `cluster_map` RPC, which returns node metadata,
transport endpoints, and load metrics. This is safe to call directly via
`MPREGClientAPI` as well.

```python
from mpreg.client.client_api import MPREGClientAPI

async with MPREGClientAPI("ws://hub-1:<port>") as client:
    snapshot = await client.cluster_map()
    print(snapshot["cluster_id"])
```

`MPREGClusterClient` can optionally prefer a region when selecting ingress
endpoints. If `preferred_region` is set, the client will select endpoints from
that region when available and fall back to other regions only when none match.

### Summary Redirects (Global Discovery)

Use summary exports to discover the authoritative cluster and prefer its
ingress endpoints when making calls:

```python
summary = await client.summary_query(
    namespace="svc.market",
    scope="global",
    include_ingress=True,
    ingress_limit=2,
    ingress_scope="zone",
    ingress_capabilities=["rpc"],
)
entry = summary.items[0]
result = await client.call_with_summary(
    entry,
    "svc.market.quote",
    "EURUSD",
    function_id="market.quote",
    version_constraint=">=1.0.0",
    ingress=summary.ingress,
)
```

You can also enable automatic summary redirects when a command is not found in
the local cluster map. This keeps the default call flow unchanged unless the
command is missing and a summary exists:

```python
client = MPREGClusterClient(
    seed_urls=("ws://hub-1:<port>",),
    auto_summary_redirect=True,
    summary_redirect_scope="global",
    summary_redirect_ingress_limit=2,
)
result = await client.call(
    "svc.market.quote",
    "EURUSD",
    function_id="market.quote",
    version_constraint=">=1.0.0",
)
```

For direct calls that already specify `target_cluster`, enable automatic
ingress hint usage:

```python
result = await client.call(
    "svc.market.quote",
    "EURUSD",
    function_id="market.quote",
    version_constraint=">=1.0.0",
    target_cluster=entry.source_cluster,
    use_ingress_hints=True,
)
```

### Scoped Discovery Queries

Use `cluster_map_v2` for scoped node snapshots and `catalog_query` for filtered
discovery queries with pagination. Use `scope` to enforce tier visibility and
`tags` to filter tagged endpoints.

```python
from mpreg.client.client_api import MPREGClientAPI

async with MPREGClientAPI("ws://hub-1:<port>") as client:
    scoped_map = await client.cluster_map_v2(scope="zone", limit=10)
    print(len(scoped_map["nodes"]))

    functions = await client.catalog_query(
        entry_type="functions",
        namespace="svc.market",
        scope="zone",
        tags=["alpha"],
        function_name="quote",
        version_constraint=">=1.0.0",
        limit=5,
    )
    print(functions["items"])
```

Clusters can also deploy resolver nodes that serve discovery from cache
(`discovery_resolver_mode=True`). Point `cluster_map_v2` and `catalog_query`
at a resolver endpoint when you want cached discovery responses.

### RPC Discovery Introspection

Use `rpc_list` to enumerate RPC endpoints with summaries and spec digests.
Use `rpc_describe` to fetch spec payloads (auto/local/catalog/scatter) with
`detail_level` controlling summary vs full specs, and use `rpc_report` for
aggregated counts.
The default `auto` mode uses gossipped catalog data and scatters only for
missing specs; set `detail_level="summary"` to omit full specs.

```python
from mpreg.client.client_api import MPREGClientAPI

async with MPREGClientAPI("ws://hub-1:<port>") as client:
    listing = await client.rpc_list(
        namespace="svc.market",
        scope="zone",
        query="quote",
        limit=20,
    )
    print(listing["items"])

    details = await client.rpc_describe(
        mode="auto",
        detail_level="full",
        namespace="svc.market",
        function_name="svc.market.quote",
        scope="zone",
        timeout_seconds=2.0,
    )
    print(details["items"][0]["spec"])

    report = await client.rpc_report(namespace="svc.market", scope="zone")
    print(report)
```

### Catalog Watch (Delta Stream)

Use `catalog_watch` to get the discovery delta topic, then subscribe via pub/sub.
When `namespace` is provided, the topic is namespaced (e.g.
`mpreg.discovery.delta.<namespace>`).

```python
from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.pubsub_client import MPREGPubSubClient

async with MPREGClientAPI("ws://hub-1:<port>") as client:
    watch_info = await client.catalog_watch(scope="zone", namespace="svc.market")

    pubsub = MPREGPubSubClient(base_client=client)
    await pubsub.start()

    def on_delta(message):
        payload = message.payload
        print(payload["delta"]["update_id"])

    await pubsub.subscribe([watch_info["topic"]], on_delta, get_backlog=False)
```

Delta payloads include `delta` (routing catalog delta), `counts`, `namespaces`,
`source_node`, and `source_cluster` for client-side filtering.

### Summary Query

Use `summary_query` to fetch summarized service entries from a catalog. When
summary resolvers are enabled, use `scope="region"` or `scope="global"` to
read from the summary cache.

```python
summary = await client.summary_query(namespace="svc.market", limit=10)
print(summary["items"])

global_summary = await client.summary_query(namespace="svc.market", scope="global")
print(global_summary["items"])
```

Summary items may include `source_cluster` when the server has export metadata;
use it as a redirect hint for region-level resolvers.

To request ingress hints, set `include_ingress=True`. Optional filters include
`ingress_scope` (strict), `ingress_capabilities`, and `ingress_tags`:

```python
global_summary = await client.summary_query(
    namespace="svc.market",
    scope="global",
    include_ingress=True,
    ingress_limit=2,
    ingress_scope="zone",
    ingress_capabilities=["rpc"],
    ingress_tags=["edge"],
)
print(global_summary.get("ingress", {}))
```

### Summary Watch (Export Stream)

Use `summary_watch` to subscribe to periodic summary exports via pub/sub.
When `namespace` is set, the server returns a namespaced topic like
`mpreg.discovery.summary.<namespace>`.

```python
from mpreg.client.pubsub_client import MPREGPubSubClient

watch_info = await client.summary_watch(scope="zone", namespace="svc.market")

pubsub = MPREGPubSubClient(base_client=client)
await pubsub.start()

def on_summary(message):
    payload = message.payload
    print(payload["summaries"])

await pubsub.subscribe([watch_info["topic"]], on_summary, get_backlog=False)
```

### Discovery CLI Helpers

The CLI exposes discovery helpers for quick inspection:

```bash
mpreg discovery query --url ws://hub:<port> --entry-type functions --namespace svc.market
mpreg discovery summary --url ws://hub:<port> --namespace svc.market --scope global \
  --include-ingress --ingress-scope zone --ingress-capability rpc --ingress-limit 2
mpreg discovery watch --url ws://hub:<port> --namespace svc.market --count 5
mpreg discovery status --url http://127.0.0.1:<monitoring-port>
mpreg report namespace-health --url http://127.0.0.1:<monitoring-port>
mpreg report export-lag --url http://127.0.0.1:<monitoring-port>
```

### Resolver Cache Ops

Resolvers expose cache stats and resync operations via RPC:

```python
stats = await client.resolver_cache_stats()
print(stats["entry_counts"])
print(stats.get("query_cache", {}))

resync = await client.resolver_resync()
print(resync["resynced"])
```

### Namespace Policy Status

If namespace policies are enabled, you can inspect viewer visibility:

```python
status = await client.namespace_status(namespace="svc.secret")
print(status["allowed"])

async with MPREGClientAPI("ws://secure-cluster:<port>") as secure_client:
    secure_status = await secure_client.namespace_status(namespace="svc.secret")
    print(secure_status["allowed"])
```

Viewer identity is derived from the server-side connection context; use a
client connected to the target cluster to evaluate its visibility.
When tenant-aware policy is enabled, include `viewer_tenant_id` in discovery
requests to evaluate tenant visibility. For authenticated tenant binding, use
`auth_token` or `api_key` on the client:

```python
async with MPREGClientAPI(
    "ws://secure-cluster:<port>",
    auth_token="tenant-secure-token",
) as secure_client:
    allowed = await secure_client.catalog_query(
        entry_type="functions",
        namespace="svc.secret",
    )
    print(allowed["items"])
```

### Namespace Policy Admin (Apply/Validate)

```python
validate = await client.namespace_policy_validate(
    rules=[{"namespace": "svc.secret", "visibility": ["secure-cluster"]}],
    actor="admin",
)
print(validate["valid"])

apply = await client.namespace_policy_apply(
    rules=[{"namespace": "svc.secret", "visibility": ["secure-cluster"]}],
    enabled=True,
    actor="admin",
)
print(apply["applied"])

audit = await client.namespace_policy_audit(limit=10)
print(audit["entries"])
```

---

## 3. MPREG PubSub Client

**Use Case**: Event-driven architecture, real-time notifications, message routing

Pub/sub integrates with the fabric control plane for discovery and
cross-cluster forwarding; no separate pub/sub gossip plane exists.

### Python Example

```python
from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.pubsub_client import MPREGPubSubClient

async def main():
    base_client = MPREGClientAPI("ws://cache-server:<port>")
    await base_client.connect()
    pubsub = MPREGPubSubClient(base_client=base_client)
    await pubsub.start()

    # Subscribe to cache events
    await pubsub.subscribe(
        patterns=["cache.events.#"],
        callback=handle_cache_event,
        get_backlog=True,
    )

    # Publish cache invalidation
    await pubsub.publish(
        topic="cache.invalidation.pattern.ml.*",
        payload={"pattern": "ml.*", "reason": "model_updated"},
    )

    await pubsub.stop()
    await base_client.disconnect()

async def handle_cache_event(message):
    print(f"Cache event: {message.topic}")
    print(f"Payload: {message.payload}")
```

---

## 4. Global Cache Manager (In-Process)

**Use Case**: Direct cache manipulation, cache federation, and cache-level control

### Python Example

```python
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport
from mpreg.core.global_cache import (
    CacheLevel,
    CacheMetadata,
    CacheOptions,
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)

async def main():
    cache_transport = InProcessCacheTransport()
    cache_protocol = FabricCacheProtocol("node-a", transport=cache_transport)

    cache = GlobalCacheManager(
        GlobalCacheConfiguration(
            enable_l2_persistent=False,
            enable_l3_distributed=True,
            enable_l4_federation=True,
            local_cluster_id="cluster-a",
        ),
        cache_protocol=cache_protocol,
    )

    key = GlobalCacheKey.from_data("user_sessions", {"session_id": "session_123"})
    options = CacheOptions(cache_levels=frozenset([CacheLevel.L1, CacheLevel.L4]))
    await cache.put(
        key, {"user_id": "user_456"}, CacheMetadata(ttl_seconds=300.0), options=options
    )

    result = await cache.get(key, options=options)
    print("Cache hit:", result.success)
    await cache.shutdown()
```

### Cache Protocol Messages

For implementing cache clients in other languages, here are the key message formats:

#### Cache GET Request

```json
{
  "role": "cache-request",
  "u": "cache-12345",
  "operation": "get",
  "key": {
    "namespace": "user_sessions",
    "identifier": "session_123",
    "version": "v1.0.0"
  },
  "options": {
    "consistency_level": "eventual",
    "timeout_ms": 5000
  }
}
```

#### Cache PUT Request

```json
{
  "role": "cache-request",
  "u": "cache-12346",
  "operation": "put",
  "key": {
    "namespace": "user_sessions",
    "identifier": "session_123"
  },
  "value": {
    "user_id": "user_456",
    "login_time": 1640995200.123
  },
  "options": {
    "consistency_level": "strong"
  }
}
```

#### Atomic Test-and-Set Request

```json
{
  "role": "cache-request",
  "u": "atomic-12347",
  "operation": "atomic",
  "key": {
    "namespace": "locks",
    "identifier": "resource_abc"
  },
  "atomic_operation": {
    "operation_type": "test_and_set",
    "expected_value": null,
    "new_value": {
      "owner": "worker_001",
      "acquired_at": 1640995200.123
    },
    "ttl_seconds": 300
  },
  "options": {
    "consistency_level": "strong"
  }
}
```

#### Data Structure Operation Request

```json
{
  "role": "cache-request",
  "u": "struct-12348",
  "operation": "structure",
  "key": {
    "namespace": "user_permissions",
    "identifier": "user_456"
  },
  "structure_operation": {
    "structure_type": "set",
    "operation": "add",
    "values": ["admin", "moderator"]
  }
}
```

#### Namespace Operation Request

```json
{
  "role": "cache-request",
  "u": "namespace-12349",
  "operation": "namespace",
  "namespace_operation": {
    "operation_type": "clear",
    "namespace": "temp_data",
    "pattern": "expired_*",
    "max_entries": 1000
  }
}
```

---

## Cache Demonstrations

Run the cache-focused demos:

```bash
uv run python mpreg/examples/tier1_single_system_full.py --system cache
uv run python mpreg/examples/tier2_integrations.py
```

---

## Client Comparison

| Feature           | RPC Client              | Cluster Client           | PubSub Client         | Cache Manager                  |
| ----------------- | ----------------------- | ------------------------ | --------------------- | ------------------------------ |
| **Primary Use**   | Function calls          | HA access + discovery    | Event messaging       | Data storage                   |
| **Communication** | Request/Response        | Request/Response         | Publish/Subscribe     | In-process API                 |
| **Data Model**    | Function arguments      | Function arguments       | Topic messages        | Key-value + structures         |
| **Consistency**   | Per-function            | Per-function             | Eventually consistent | Configurable (eventual/strong) |
| **Caching**       | Automatic (server-side) | Automatic (server-side)  | Message backlog       | Direct control                 |
| **Atomicity**     | Function-level          | Function-level           | Message-level         | Operation-level                |
| **Federation**    | Multi-cluster routing   | Multi-cluster + failover | Topic federation      | Geographic replication         |

---

## Network Protocols

All MPREG clients support multiple transport protocols:

### WebSocket (Recommended)

- **URL**: `ws://server:port` or `wss://server:port` (TLS)
- **Use Case**: Real-time applications, web browsers
- **Features**: Bi-directional, low latency, firewall friendly

### TCP

- **URL**: `tcp://server:port` or `tcps://server:port` (TLS)
- **Use Case**: High-throughput, server-to-server
- **Features**: Maximum performance, connection pooling

### Connection Example

```python
# WebSocket
client = MPREGClientAPI("ws://cache-server:<port>")

# WebSocket with TLS
client = MPREGClientAPI("wss://cache-server:<port>")

# TCP
client = MPREGClientAPI("tcp://cache-server:<port>")

# TCP with TLS
client = MPREGClientAPI("tcps://cache-server:<port>")
```

---

## Production Considerations

### High Availability

```python
from mpreg.client.cluster_client import MPREGClusterClient

client = MPREGClusterClient(
    seed_urls=(
        "ws://cache-1.example.com:<port>",
        "ws://cache-2.example.com:<port>",
        "ws://cache-3.example.com:<port>",
    )
)
await client.connect()
result = await client.call("echo", "ping")
await client.disconnect()
```

### Authentication

```python
# Bearer token authentication
headers = {"Authorization": "Bearer eyJhbGciOiJIUzI1NiIs..."}
client = MPREGClientAPI("wss://secure-cache:<port>", headers=headers)

# API key authentication
headers = {"X-API-Key": "mpreg_api_key_abc123..."}
client = MPREGClientAPI("wss://secure-cache:<port>", headers=headers)
```

### Connection Pooling

```python
from mpreg.core.transport.factory import create_transport_pool

# Create connection pool
pool = create_transport_pool(
    urls=["ws://cache-1:<port>", "ws://cache-2:<port>"],
    pool_size=10,
    max_lifetime_seconds=3600,
)

# Use pooled connections
async with pool.get_client() as client:
    result = await client.call("fibonacci", 10)
```

### Monitoring

```python
# Cross-system monitoring
from mpreg.core.monitoring.unified_monitoring import (
    EventType,
    SystemType,
    create_unified_system_monitor,
)

monitor = create_unified_system_monitor()
await monitor.start()
tracking_id = await monitor.record_cross_system_event(
    correlation_id="client-demo",
    event_type=EventType.REQUEST_START,
    source_system=SystemType.RPC,
)
await monitor.stop()
```

---

## 5. DNS Interoperability (Optional)

**Use Case**: Standard DNS clients resolving MPREG services without a custom SDK

Use the `dns_*` RPCs to register service endpoints into the catalog, then
query the DNS gateway over UDP/TCP.

### Python Example (Register)

```python
from mpreg.client.client_api import MPREGClientAPI

async def main():
    async with MPREGClientAPI("ws://ingress:<port>") as client:
        await client.dns_register(
            {
                "name": "tradefeed",
                "namespace": "market",
                "protocol": "tcp",
                "port": 9000,
                "targets": ["10.0.1.12", "10.0.1.13"],
                "tags": ["primary"],
                "capabilities": ["quotes"],
                "metadata": {"tier": "gold"},
                "priority": 10,
                "weight": 5,
            }
        )
```

### CLI Example

```bash
mpreg client dns-register --url ws://ingress:<port> \\
  --name tradefeed --namespace market --protocol tcp --port 9000 \\
  --target 10.0.1.12 --target 10.0.1.13 --tag primary \\
  --capability quotes --metadata tier=gold
```

### DNS Query Example

```bash
dig @<gateway-host> -p <udp-port> _svc._tcp.tradefeed.market.mpreg SRV
dig @<gateway-host> -p <udp-port> tradefeed.market.mpreg A
```

CLI helper:

```bash
mpreg client dns-resolve --host <gateway-host> --port <udp-port> \
  --qname _svc._tcp.tradefeed.market.mpreg --qtype SRV
```

### Python Example (Resolve)

```python
from mpreg.client.dns_client import MPREGDnsClient

async def main():
    dns = MPREGDnsClient(host="127.0.0.1", port=5353)
    result = await dns.resolve("_svc._tcp.tradefeed.market.mpreg", qtype="SRV")

print(result.to_dict())
```

Node label helpers:

```bash
mpreg client dns-node-encode "ws://127.0.0.1:10000"
mpreg client dns-node-decode b32-...
```

This guide provides everything needed to start using MPREG's RPC, cluster-aware, PubSub clients, the in-process cache manager, and the optional DNS gateway.

## Call retries and deadlines

```python
from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.call_policy import ClientCallPolicy
from mpreg.core.errors import MpregError, MpregErrorCode

policy = ClientCallPolicy(
    max_attempts=3,
    base_backoff_seconds=0.05,
    max_backoff_seconds=1.0,
    jitter_seconds=0.05,
    deadline_seconds=5.0,
    retry_on_timeout=True,
    retry_on_unavailable=True,
)
async with MPREGClientAPI("ws://127.0.0.1:<port>", call_policy=policy) as client:
    try:
        result = await client.call("echo", "hi")
    except MpregError as exc:
        # Branch on stable numeric codes (also on exc.rpc_error.code)
        if exc.code == int(MpregErrorCode.COMMAND_NOT_FOUND):
            ...
        elif exc.retryable:
            ...
        raise
    # Optional correlation aid when debugging multi-hop fabric paths:
    print(client.last_trace_context())
```

| Code      | Name                | Typical retry?                               |
| --------- | ------------------- | -------------------------------------------- |
| 1001      | COMMAND_NOT_FOUND   | No (cluster client may try summary redirect) |
| 1002      | VERSION_MISMATCH    | No                                           |
| 1003      | HOP_BUDGET_EXCEEDED | No                                           |
| 1004      | POLICY_DENIED       | No                                           |
| 1005      | ROUTE_NOT_FOUND     | Yes (default)                                |
| 1006      | TIMEOUT             | Yes                                          |
| 1007      | UNAVAILABLE         | Yes                                          |
| 1008      | INVALID_ARGUMENT    | No                                           |
| 1009–1010 | AUTH\_\*            | No                                           |
| 1099      | INTERNAL            | Operator-dependent                           |

### High-availability cluster client

```python
from mpreg.client.cluster_client import MPREGClusterClient

client = MPREGClusterClient(
    seed_urls=("ws://hub-a:<port>", "ws://hub-b:<port>"),
    preferred_region="us-east",
    auto_summary_redirect=True,
)
# Non-retryable MpregError stops endpoint rotation (except command-not-found
# when summary redirect is enabled).
```

Endpoint selection already weights latency and error rate. Keep
`auto_summary_redirect=False` (default) for single-cluster deployments.

### Trace correlation

Fabric messages carry W3C `traceparent` in header metadata. Correlate hops with:

```bash
mpreg monitor decisions --correlation-id <id> --url $MPREG_MONITORING_URL
```

## RPC execution modalities (async / soft real-time / streaming)

MPREG clients select evaluation semantics through `ClientCallPolicy` and
`RpcExecutionMode`:

| Mode              | Enum                            | Semantics                                                                                            |
| ----------------- | ------------------------------- | ---------------------------------------------------------------------------------------------------- |
| M1 Async          | `RpcExecutionMode.M1_ASYNC`     | Throughput-oriented retries; optional wall deadline per attempt                                      |
| M2 Soft real-time | `RpcExecutionMode.M2_SOFT_RT`   | Shared end-to-end deadline budget; fail closed with `TIMEOUT` (1006); no retry past remaining budget |
| M3 Streaming      | `RpcExecutionMode.M3_STREAMING` | Progressive / intermediate results; cancel stops further partials; default single attempt            |

```python
from mpreg.client.call_policy import (
    ClientCallPolicy,
    RpcExecutionMode,
    call_with_policy,
)

# Soft real-time: 200ms budget shared across retries
policy = ClientCallPolicy.for_mode(
    RpcExecutionMode.M2_SOFT_RT,
    deadline_seconds=0.2,
)

# Streaming / progressive
stream_policy = ClientCallPolicy.for_mode(RpcExecutionMode.M3_STREAMING)

# Async throughput with HA defaults
async_policy = ClientCallPolicy.for_mode(RpcExecutionMode.M1_ASYNC)
```

Fabric hops carry `MessageHeaders.deadline_remaining_ms` (also mirrored in
metadata key `mpreg.deadline_remaining_ms`). Forwarders should decrement the
budget by measured hop latency via `mpreg.core.rpc_deadline.decrement_deadline_headers`.
When remaining ≤ 0, servers return structured `TIMEOUT` rather than stalling.

Intermediate results (`return_intermediate_results`) emit monotonic level
indices; a final event implies no further partials. Under hop loss, clients may
observe a prefix of levels without a final — never a silent wrong success.

See architecture claims INV-P1–P5 in `tests/invariants/claims.yaml`.

## Four-plane HA limits (USE-T10-03 / ERG-T10-08)

`MPREGClusterClient` provides **RPC-path** endpoint failover via `ClientCallPolicy`.
It does **not** automatically HA-fanout queue, cache, or pub/sub operations across
endpoints.

- Use `cluster.plane_client(url)` (or `MPREGClient(url)`) for queue/cache/pubsub against
  **one** chosen endpoint.
- Bare servers without `--enable-cache` / `--enable-queue` (or a profile that sets them)
  return `COMMAND_NOT_FOUND` for plane RPCs — run `mpreg config-check` first.
- `EXACTLY_ONCE` delivery is **refused** (`MpregErrorCode` 1011). Prefer
  at-least-once + idempotent handlers.
- Cache `ConsistencyLevel.STRONG` **put** is available when the server sets
  `cache_strong_enabled` (majority-commit barrier, residual-free failures).
  Default **off** → `1012 UNSUPPORTED_CONSISTENCY`. STRONG **get** and
  **delete** always refuse with `1012`. Operational put failures use
  **1015–1018** (`INSUFFICIENT_QUORUM`, `QUORUM_TIMEOUT`, `STRONG_CONFLICT`,
  `STRONG_PENDING_FULL`). See `docs/CACHING_SYSTEM.md` and claim
  `INV-CACHE-STRONG-01`. Curriculum: `cache_strong_quorum`.
- `location_consistency.ConsistencyLevel.STRONG` remains fail-closed (separate
  plane; not the GlobalCacheManager product path).
- `CacheOpResult` / `QueueSendResult` expose `error_code` (ERG-T13-03) so callers
  need not dig into `.raw` for plane refusals. Missing `success` on dict payloads
  is fail-closed (`success=False`). STRONG results may also carry `quorum_info`.
- Soft plane failures also carry stable codes: `1007` unavailable, `1008` invalid
  argument (ERG-T13-04).
- Proof ledger: `tests/invariants/claims.yaml` (`non_claims` lists what is out of scope).

## Default HA call policy

`MPREGClusterClient` applies `default_ha_policy()` (3 attempts, retry timeout/unavailable only)
unless you pass `call_policy=ClientCallPolicy(...)` or `call_policy=ClientCallPolicy(max_attempts=1)`.

Outbound RPC JSON includes a W3C `traceparent` (and optional `tracestate`) injected by the
transport client. Read the last observed context with `MPREGClientAPI.last_trace_context()`.
