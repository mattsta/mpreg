# MPREG Fabric Federation Quick Start Examples

## 🚀 5-Minute Quick Start

Choose your use case and copy-paste the example to get started immediately:

### Helper Utilities (Port Allocation + Startup)

All examples avoid fixed ports. Use this helper to allocate free ports per run.

```python
from contextlib import contextmanager

from mpreg.core.port_allocator import port_range_context

@contextmanager
def reserve_ports(count: int, label: str = "servers"):
    with port_range_context(count, label) as ports:
        yield ports
```

### 1. Edge Computing Network (Dynamic Mesh)

```python
import asyncio
from mpreg.server import MPREGServer
from mpreg.core.config import MPREGSettings

async def create_edge_network():
    """Create a self-organizing edge computing network."""
    nodes = []
    node_tasks = []
    with reserve_ports(6) as ports:
        # Create 6 edge nodes that will auto-discover each other
        for i, port in enumerate(ports):
            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"edge-node-{i}",
                cluster_id="edge-network",
                resources={f"sensor-{i}", f"actuator-{i}"},
                connect=f"ws://127.0.0.1:{ports[0]}" if i > 0 else None,
                gossip_interval=0.5,  # Fast discovery for IoT
            )

            node = MPREGServer(settings=settings)
            nodes.append(node)

            # Register edge processing function
            def process_sensor_data(sensor_id: str, data: dict) -> dict:
                return {
                    "processed_by": settings.name,
                    "sensor_id": sensor_id,
                    "processed_data": data,
                    "timestamp": time.time(),
                }

            node.register_command(
                "process_sensor", process_sensor_data, [f"sensor-{i}"]
            )
            node_tasks.append(asyncio.create_task(node.server()))
            await asyncio.sleep(0.05)

    await asyncio.sleep(0.5)
    return nodes

# Usage
edge_nodes = await create_edge_network()
print(f"✅ Created {len(edge_nodes)} edge computing nodes")
```

### 2. Financial Trading System (Byzantine Tolerant)

```python
async def create_trading_system():
    """Create a Byzantine fault-tolerant trading system."""

    # 3 trading regions with 3 nodes each (tolerates 1 malicious node per region)
    regions = ["new-york", "london", "tokyo"]
    all_nodes = []
    node_tasks = []

    for region_idx, region in enumerate(regions):
        region_nodes = []
        leader_connect = None
        if region_idx > 0:
            leader_connect = (
                f"ws://127.0.0.1:{all_nodes[(region_idx - 1) * 3].settings.port}"
            )
        with reserve_ports(3) as region_ports:
            for i, port in enumerate(region_ports):
                settings = MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"{region}-trader-{i}",
                    cluster_id="trading-fabric",
                    resources={f"{region}-market-data", f"{region}-order-book"},
                    connect=(
                        leader_connect
                        if i == 0 and leader_connect
                        else f"ws://127.0.0.1:{region_ports[0]}"
                        if i > 0
                        else None
                    ),
                    gossip_interval=0.3,  # Fast for trading
                )

                node = MPREGServer(settings=settings)
                region_nodes.append(node)
                all_nodes.append(node)

                # Register trading functions
                def execute_trade(symbol: str, quantity: int, price: float) -> dict:
                    return {
                        "trade_id": f"{region}-{int(time.time() * 1000)}",
                        "symbol": symbol,
                        "quantity": quantity,
                        "price": price,
                        "executed_by": settings.name,
                        "region": region,
                    }

                node.register_command(
                    "execute_trade",
                    execute_trade,
                    [f"{region}-order-book"],
                )
                node_tasks.append(asyncio.create_task(node.server()))
                await asyncio.sleep(0.05)

    await asyncio.sleep(0.5)
    return all_nodes

# Usage
trading_nodes = await create_trading_system()
print(f"✅ Created Byzantine-tolerant trading system with {len(trading_nodes)} nodes")
```

### 3. Enterprise Service Mesh (Hierarchical)

```python
async def create_enterprise_mesh():
    """Create a hierarchical enterprise service mesh."""

    # 3-tier architecture: Local services → Regional aggregators → Global coordinator
    tiers = [
        {"name": "local", "count": 8},      # Microservices
        {"name": "regional", "count": 3},   # Regional aggregators
        {"name": "global", "count": 1},     # Global coordinator
    ]

    all_nodes = []
    tier_leaders = []
    node_tasks = []

    for tier in tiers:
        tier_nodes = []
        leader_connect = (
            f"ws://127.0.0.1:{tier_leaders[-1].settings.port}"
            if tier_leaders
            else None
        )
        with reserve_ports(tier["count"]) as tier_ports:
            for i in range(tier["count"]):
                port = tier_ports[i]
                settings = MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"{tier['name']}-service-{i}",
                    cluster_id="enterprise-mesh",
                    resources={f"{tier['name']}-compute", f"{tier['name']}-storage"},
                    connect=(
                        leader_connect
                        if i == 0 and leader_connect
                        else f"ws://127.0.0.1:{tier_ports[0]}"
                        if i > 0
                        else None
                    ),
                    gossip_interval=0.5,
                )

                node = MPREGServer(settings=settings)
                tier_nodes.append(node)
                all_nodes.append(node)

                # Register tier-specific functions
                if tier["name"] == "local":
                    def process_request(request_id: str, data: dict) -> dict:
                        return {
                            "request_id": request_id,
                            "processed_by": settings.name,
                            "tier": "local",
                            "result": f"Processed: {data}",
                        }
                    node.register_command(
                        "process_request",
                        process_request,
                        [f"{tier['name']}-compute"],
                    )

                elif tier["name"] == "regional":
                    def aggregate_results(results: list) -> dict:
                        return {
                            "aggregated_by": settings.name,
                            "tier": "regional",
                            "total_results": len(results),
                            "summary": f"Aggregated {len(results)} results",
                        }
                    node.register_command(
                        "aggregate_results",
                        aggregate_results,
                        [f"{tier['name']}-compute"],
                    )

                elif tier["name"] == "global":
                    def coordinate_global(operation: str) -> dict:
                        return {
                            "coordinated_by": settings.name,
                            "tier": "global",
                            "operation": operation,
                            "status": "coordinated",
                        }
                    node.register_command(
                        "coordinate_global",
                        coordinate_global,
                        [f"{tier['name']}-compute"],
                    )

                node_tasks.append(asyncio.create_task(node.server()))
                await asyncio.sleep(0.05)

            # Track tier leaders for inter-tier connections
            tier_leaders.append(tier_nodes[0])

    await asyncio.sleep(0.5)
    return all_nodes, tier_leaders

# Usage
enterprise_nodes, leaders = await create_enterprise_mesh()
print(f"✅ Created enterprise service mesh with {len(enterprise_nodes)} nodes across 3 tiers")
```

### 4. DNS Gateway + Ingress Node (Interop Demo)

Expose the service catalog via DNS while keeping MPREG RPC and pub/sub intact.

```python
import asyncio
from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context, port_context
from mpreg.server import MPREGServer

async def create_dns_gateway_demo():
    with port_range_context(2, "servers") as ports, port_context("dns-udp") as dns_port:
        # Ingress node with DNS gateway enabled
        settings = MPREGSettings(
            host="127.0.0.1",
            port=ports[0],
            name="dns-ingress",
            cluster_id="demo-cluster",
            dns_gateway_enabled=True,
            dns_zones=("mpreg",),
            dns_udp_port=dns_port,
        )
        ingress = MPREGServer(settings=settings)
        task = asyncio.create_task(ingress.server())

        # Feature node that registers a service
        feature_settings = MPREGSettings(
            host="127.0.0.1",
            port=ports[1],
            name="feature-node",
            cluster_id="demo-cluster",
            connect=f"ws://127.0.0.1:{ports[0]}",
        )
        feature = MPREGServer(settings=feature_settings)
        feature_task = asyncio.create_task(feature.server())

        await asyncio.sleep(0.5)
        return ingress, feature, dns_port, (task, feature_task)

# After startup, register a service and resolve via DNS:
# mpreg client dns-register --url ws://127.0.0.1:<port> --name tradefeed --namespace market \
#   --protocol tcp --port 9000 --target 127.0.0.1
# mpreg client dns-resolve --host 127.0.0.1 --port <dns-port> --qname _svc._tcp.tradefeed.market.mpreg --qtype SRV
```

### 5. Global CDN (Cross-Datacenter)

```python
async def create_global_cdn():
    """Create a global content delivery network."""

    # 3 geographic regions with realistic latencies
    datacenters = [
        {"name": "us-east", "nodes": 4, "latency": 5},
        {"name": "eu-west", "nodes": 4, "latency": 8},
        {"name": "asia-pacific", "nodes": 3, "latency": 12},
    ]

    all_nodes = []
    dc_leaders = []
    node_tasks = []

    total_nodes = sum(dc["nodes"] for dc in datacenters)
    with reserve_ports(total_nodes) as all_ports:
        offsets: dict[str, tuple[int, int]] = {}
        offset = 0
        for dc in datacenters:
            offsets[dc["name"]] = (offset, dc["nodes"])
            offset += dc["nodes"]
        leader_urls = {
            dc["name"]: f"ws://127.0.0.1:{all_ports[offsets[dc['name']][0]]}"
            for dc in datacenters
        }

        for dc in datacenters:
            dc_nodes = []
            start, count = offsets[dc["name"]]
            dc_ports = all_ports[start : start + count]
            leader_peers = [
                url for name, url in leader_urls.items() if name != dc["name"]
            ]

            for i, port in enumerate(dc_ports):
                settings = MPREGSettings(
                    host="127.0.0.1",
                    port=port,
                    name=f"{dc['name']}-cdn-{i}",
                    cluster_id="global-cdn",
                    resources={f"{dc['name']}-cache", f"{dc['name']}-origin"},
                    peers=leader_peers if i == 0 and leader_peers else None,
                    connect=leader_urls[dc["name"]] if i > 0 else None,
                    gossip_interval=0.5,
                )

                node = MPREGServer(settings=settings)
                dc_nodes.append(node)
                all_nodes.append(node)

                # Register CDN functions
                def serve_content(content_id: str, client_location: str) -> dict:
                    return {
                        "content_id": content_id,
                        "served_from": settings.name,
                        "datacenter": dc["name"],
                        "latency_ms": dc["latency"],
                        "client_location": client_location,
                        "cache_hit": True,  # Simulate cache
                    }

                def replicate_content(content_id: str, content_data: str) -> dict:
                    return {
                        "content_id": content_id,
                        "replicated_to": settings.name,
                        "datacenter": dc["name"],
                        "size_bytes": len(content_data),
                        "replication_time": time.time(),
                    }

                node.register_command(
                    "serve_content", serve_content, [f"{dc['name']}-cache"]
                )
                node.register_command(
                    "replicate_content",
                    replicate_content,
                    [f"{dc['name']}-origin"],
                )
                node_tasks.append(asyncio.create_task(node.server()))
                await asyncio.sleep(0.05)

            dc_leaders.append(dc_nodes[0])

    await asyncio.sleep(0.5)
    return all_nodes, dc_leaders

# Usage
cdn_nodes, dc_leaders = await create_global_cdn()
print(f"✅ Created global CDN with {len(cdn_nodes)} nodes across 3 datacenters")
```

### 6. Mission-Critical System (Self-Healing)

```python
async def create_mission_critical_system():
    """Create a self-healing mission-critical system."""

    # 10-node resilient mesh for maximum fault tolerance
    node_count = 10
    all_nodes = []
    node_tasks = []
    with reserve_ports(node_count) as ports:
        for i, port in enumerate(ports):
            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"critical-node-{i}",
                cluster_id="mission-critical",
                resources={f"critical-service-{i}", "backup-service"},
                connect=f"ws://127.0.0.1:{ports[0]}" if i > 0 else None,  # Star topology for resilience
                gossip_interval=0.2,  # Very fast failure detection
            )

            node = MPREGServer(settings=settings)
            all_nodes.append(node)

            # Register critical system functions with redundancy
            def execute_critical_operation(operation_id: str, params: dict) -> dict:
                return {
                    "operation_id": operation_id,
                    "executed_by": settings.name,
                    "node_id": i,
                    "params": params,
                    "backup_nodes": [
                        f"critical-node-{(i+1)%node_count}",
                        f"critical-node-{(i+2)%node_count}",
                    ],
                    "execution_time": time.time(),
                    "status": "completed",
                }

            def health_check() -> dict:
                return {
                    "node": settings.name,
                    "status": "healthy",
                    "connections": "will_be_measured",
                    "last_check": time.time(),
                }

            node.register_command(
                "execute_critical",
                execute_critical_operation,
                [f"critical-service-{i}"],
            )
            node.register_command("health_check", health_check, ["backup-service"])
            node_tasks.append(asyncio.create_task(node.server()))
            await asyncio.sleep(0.05)

    # Test partition recovery scenarios
    partition_scenarios = [
        {"name": "Split Brain", "partitions": [[0,1,2,3,4], [5,6,7,8,9]]},
        {"name": "Minority Failure", "partitions": [[0,1,2,3,4,5,6], [7,8,9]]},
        {"name": "Multiple Islands", "partitions": [[0,1,2,3,4,5], [6,7], [8,9]]},
    ]

    await asyncio.sleep(0.5)
    return all_nodes, partition_scenarios

# Usage
critical_nodes, scenarios = await create_mission_critical_system()
print(f"✅ Created mission-critical system with {len(critical_nodes)} self-healing nodes")
```

## 📊 Performance Benchmarking

Run benchmarks to compare topologies for your use case (test utility):

```python
from tests.performance_research_framework import ScalabilityBenchmark

async def benchmark_topologies():
    """Compare all topology patterns for your specific needs."""

    benchmark = ScalabilityBenchmark()

    # Test configurations
    cluster_sizes = [5, 8, 12, 16]
    topology_types = ["LINEAR_CHAIN", "STAR_HUB", "RING", "MULTI_HUB"]

    results = await benchmark.run_scalability_benchmark(
        topology_builder=None,  # Will be created automatically
        cluster_sizes=cluster_sizes,
        topology_types=topology_types,
        iterations=3,
    )

    # Generate performance report
    report = benchmark.generate_performance_report()
    print(report)

    # Export detailed results
    benchmark.export_benchmark_results("topology_benchmark_results.json")

    return results

# Usage
benchmark_results = await benchmark_topologies()
```

## 🔧 Configuration Templates

### Environment Variables

```bash
# Basic configuration
export MPREG_HOST=0.0.0.0
export MPREG_PORT=<port>
export MPREG_CLUSTER_ID=production
export MPREG_GOSSIP_INTERVAL=0.5

# Topology-specific settings
export MPREG_TOPOLOGY=hierarchical
export MPREG_BYZANTINE_TOLERANCE=true
export MPREG_SELF_HEALING=true
export MPREG_CROSS_DC_LATENCY_MS=100
```

### Docker Compose

```yaml
version: "3.8"
services:
  mpreg-node-1:
    image: mpreg:latest
    ports:
      - "<host-port>:<container-port>"
    environment:
      - MPREG_PORT=<container-port>
      - MPREG_CLUSTER_ID=docker-fabric
      - MPREG_TOPOLOGY=dynamic_mesh

  mpreg-node-2:
    image: mpreg:latest
    ports:
      - "<host-port>:<container-port>"
    environment:
      - MPREG_PORT=<container-port>
      - MPREG_CONNECT=ws://mpreg-node-1:<container-port>
      - MPREG_CLUSTER_ID=docker-fabric
      - MPREG_TOPOLOGY=dynamic_mesh
    depends_on:
      - mpreg-node-1

  mpreg-node-3:
    image: mpreg:latest
    ports:
      - "<host-port>:<container-port>"
    environment:
      - MPREG_PORT=<container-port>
      - MPREG_CONNECT=ws://mpreg-node-1:<container-port>
      - MPREG_CLUSTER_ID=docker-fabric
      - MPREG_TOPOLOGY=dynamic_mesh
    depends_on:
      - mpreg-node-1
```

### Kubernetes ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mpreg-config
data:
  topology: "hierarchical"
  cluster-id: "k8s-fabric"
  gossip-interval: "0.5"
  byzantine-tolerance: "true"
  self-healing: "true"

---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mpreg
spec:
  serviceName: mpreg-service
  replicas: 5
  selector:
    matchLabels:
      app: mpreg
  template:
    metadata:
      labels:
        app: mpreg
    spec:
      containers:
        - name: mpreg
          image: mpreg:latest
          ports:
            - containerPort: <port>
          envFrom:
            - configMapRef:
                name: mpreg-config
          env:
            - name: MPREG_NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: MPREG_CONNECT
              value: "ws://mpreg-0.mpreg-service:<port>"
```

## 🚨 Production Checklist

### Security

- [ ] Enable TLS/SSL for all WebSocket connections
- [ ] Implement authentication and authorization
- [ ] Configure firewall rules
- [ ] Set up VPN for cross-datacenter communication
- [ ] Enable audit logging

### Monitoring

- [ ] Set up health checks for all nodes
- [ ] Monitor connection efficiency metrics
- [ ] Alert on partition detection
- [ ] Track function propagation rates
- [ ] Monitor resource utilization

### High Availability

- [ ] Deploy across multiple availability zones
- [ ] Configure load balancers
- [ ] Set up backup and recovery procedures
- [ ] Test disaster recovery scenarios
- [ ] Document runbooks

### Performance

- [ ] Tune gossip intervals for your network
- [ ] Optimize resource allocation
- [ ] Configure appropriate timeout values
- [ ] Test under expected load
- [ ] Monitor and optimize latency

## 🔗 Next Steps

1. **Start Small**: Begin with Dynamic Mesh for 5-10 nodes
2. **Measure Performance**: Use the benchmarking tools to validate
3. **Scale Up**: Move to Hierarchical as you grow past 15 nodes
4. **Add Resilience**: Implement Self-Healing for critical systems
5. **Go Global**: Use Cross-Datacenter for global deployment

For detailed architecture documentation, see [ADVANCED_FEDERATION_ARCHITECTURE_GUIDE.md](./ADVANCED_FEDERATION_ARCHITECTURE_GUIDE.md).

For testing and validation, see the implementation in `tests/test_advanced_topological_research.py`.
