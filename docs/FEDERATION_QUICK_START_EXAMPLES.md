# MPREG Federation Quick Start Examples

## 🚀 5-Minute Quick Start

Choose your use case and copy-paste the example to get started immediately:

### 1. Edge Computing Network (Dynamic Mesh)

```python
import asyncio
from mpreg.server import MPREGServer
from mpreg.core.config import MPREGSettings

async def create_edge_network():
    """Create a self-organizing edge computing network."""
    nodes = []

    # Create 6 edge nodes that will auto-discover each other
    for i in range(6):
        settings = MPREGSettings(
            host="127.0.0.1",
            port=8000 + i,
            name=f"edge-node-{i}",
            cluster_id="edge-network",
            resources={f"sensor-{i}", f"actuator-{i}"},
            connect=f"ws://127.0.0.1:8000" if i > 0 else None,
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

        node.register_command("process_sensor", process_sensor_data, [f"sensor-{i}"])
        await node.start_async()

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

    for region_idx, region in enumerate(regions):
        region_nodes = []
        base_port = 8000 + region_idx * 10

        for i in range(3):
            settings = MPREGSettings(
                host="127.0.0.1",
                port=base_port + i,
                name=f"{region}-trader-{i}",
                cluster_id="trading-federation",
                resources={f"{region}-market-data", f"{region}-order-book"},
                connect=f"ws://127.0.0.1:{base_port}" if i > 0 else None,
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

            node.register_command("execute_trade", execute_trade, [f"{region}-order-book"])
            await node.start_async()

        # Create inter-regional federation (connect regional leaders)
        if region_idx > 0:
            previous_leader = all_nodes[(region_idx - 1) * 3]
            current_leader = region_nodes[0]
            await current_leader._establish_peer_connection(
                f"ws://127.0.0.1:{previous_leader.settings.port}"
            )

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
        {"name": "local", "count": 8, "base_port": 8000},      # Microservices
        {"name": "regional", "count": 3, "base_port": 8020},   # Regional aggregators
        {"name": "global", "count": 1, "base_port": 8030},     # Global coordinator
    ]

    all_nodes = []
    tier_leaders = []

    for tier in tiers:
        tier_nodes = []

        for i in range(tier["count"]):
            port = tier["base_port"] + i
            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"{tier['name']}-service-{i}",
                cluster_id="enterprise-mesh",
                resources={f"{tier['name']}-compute", f"{tier['name']}-storage"},
                connect=f"ws://127.0.0.1:{tier['base_port']}" if i > 0 else None,
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
                node.register_command("process_request", process_request, [f"{tier['name']}-compute"])

            elif tier["name"] == "regional":
                def aggregate_results(results: list) -> dict:
                    return {
                        "aggregated_by": settings.name,
                        "tier": "regional",
                        "total_results": len(results),
                        "summary": f"Aggregated {len(results)} results",
                    }
                node.register_command("aggregate_results", aggregate_results, [f"{tier['name']}-compute"])

            elif tier["name"] == "global":
                def coordinate_global(operation: str) -> dict:
                    return {
                        "coordinated_by": settings.name,
                        "tier": "global",
                        "operation": operation,
                        "status": "coordinated",
                    }
                node.register_command("coordinate_global", coordinate_global, [f"{tier['name']}-compute"])

            await node.start_async()

        # Track tier leaders for inter-tier connections
        tier_leaders.append(tier_nodes[0])

    # Connect tier leaders hierarchically
    for i in range(len(tier_leaders) - 1):
        await tier_leaders[i]._establish_peer_connection(
            f"ws://127.0.0.1:{tier_leaders[i + 1].settings.port}"
        )

    return all_nodes, tier_leaders

# Usage
enterprise_nodes, leaders = await create_enterprise_mesh()
print(f"✅ Created enterprise service mesh with {len(enterprise_nodes)} nodes across 3 tiers")
```

### 4. Global CDN (Cross-Datacenter)

```python
async def create_global_cdn():
    """Create a global content delivery network."""

    # 3 geographic regions with realistic latencies
    datacenters = [
        {"name": "us-east", "nodes": 4, "latency": 5, "base_port": 8000},
        {"name": "eu-west", "nodes": 4, "latency": 8, "base_port": 8010},
        {"name": "asia-pacific", "nodes": 3, "latency": 12, "base_port": 8020},
    ]

    all_nodes = []
    dc_leaders = []

    for dc in datacenters:
        dc_nodes = []

        for i in range(dc["nodes"]):
            port = dc["base_port"] + i
            settings = MPREGSettings(
                host="127.0.0.1",
                port=port,
                name=f"{dc['name']}-cdn-{i}",
                cluster_id="global-cdn",
                resources={f"{dc['name']}-cache", f"{dc['name']}-origin"},
                connect=f"ws://127.0.0.1:{dc['base_port']}" if i > 0 else None,
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

            node.register_command("serve_content", serve_content, [f"{dc['name']}-cache"])
            node.register_command("replicate_content", replicate_content, [f"{dc['name']}-origin"])
            await node.start_async()

        dc_leaders.append(dc_nodes[0])

    # Create cross-datacenter federation with latency simulation
    cross_dc_latencies = {
        ("us-east", "eu-west"): 80,
        ("us-east", "asia-pacific"): 150,
        ("eu-west", "asia-pacific"): 120,
    }

    for i in range(len(dc_leaders)):
        for j in range(i + 1, len(dc_leaders)):
            dc_i = datacenters[i]["name"]
            dc_j = datacenters[j]["name"]

            # Simulate cross-datacenter latency
            latency_key = (dc_i, dc_j) if (dc_i, dc_j) in cross_dc_latencies else (dc_j, dc_i)
            latency = cross_dc_latencies.get(latency_key, 100)

            print(f"Connecting {dc_i} ↔ {dc_j} (latency: {latency}ms)")
            await asyncio.sleep(latency / 1000.0)  # Simulate latency

            await dc_leaders[i]._establish_peer_connection(
                f"ws://127.0.0.1:{dc_leaders[j].settings.port}"
            )

    return all_nodes, dc_leaders

# Usage
cdn_nodes, dc_leaders = await create_global_cdn()
print(f"✅ Created global CDN with {len(cdn_nodes)} nodes across 3 datacenters")
```

### 5. Mission-Critical System (Self-Healing)

```python
async def create_mission_critical_system():
    """Create a self-healing mission-critical system."""

    # 10-node resilient mesh for maximum fault tolerance
    node_count = 10
    all_nodes = []

    for i in range(node_count):
        settings = MPREGSettings(
            host="127.0.0.1",
            port=8000 + i,
            name=f"critical-node-{i}",
            cluster_id="mission-critical",
            resources={f"critical-service-{i}", "backup-service"},
            connect=f"ws://127.0.0.1:8000" if i > 0 else None,  # Star topology for resilience
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
                "backup_nodes": [f"critical-node-{(i+1)%node_count}", f"critical-node-{(i+2)%node_count}"],
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

        node.register_command("execute_critical", execute_critical_operation, [f"critical-service-{i}"])
        node.register_command("health_check", health_check, ["backup-service"])
        await node.start_async()

    # Test partition recovery scenarios
    partition_scenarios = [
        {"name": "Split Brain", "partitions": [[0,1,2,3,4], [5,6,7,8,9]]},
        {"name": "Minority Failure", "partitions": [[0,1,2,3,4,5,6], [7,8,9]]},
        {"name": "Multiple Islands", "partitions": [[0,1,2,3,4,5], [6,7], [8,9]]},
    ]

    return all_nodes, partition_scenarios

# Usage
critical_nodes, scenarios = await create_mission_critical_system()
print(f"✅ Created mission-critical system with {len(critical_nodes)} self-healing nodes")
```

## 📊 Performance Benchmarking

Run benchmarks to compare topologies for your use case:

```python
from mpreg.tests.performance_research_framework import ScalabilityBenchmark

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
export MPREG_PORT=8000
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
      - "8000:8000"
    environment:
      - MPREG_PORT=8000
      - MPREG_CLUSTER_ID=docker-federation
      - MPREG_TOPOLOGY=dynamic_mesh

  mpreg-node-2:
    image: mpreg:latest
    ports:
      - "8001:8000"
    environment:
      - MPREG_PORT=8000
      - MPREG_CONNECT=ws://mpreg-node-1:8000
      - MPREG_CLUSTER_ID=docker-federation
      - MPREG_TOPOLOGY=dynamic_mesh
    depends_on:
      - mpreg-node-1

  mpreg-node-3:
    image: mpreg:latest
    ports:
      - "8002:8000"
    environment:
      - MPREG_PORT=8000
      - MPREG_CONNECT=ws://mpreg-node-1:8000
      - MPREG_CLUSTER_ID=docker-federation
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
  cluster-id: "k8s-federation"
  gossip-interval: "0.5"
  byzantine-tolerance: "true"
  self-healing: "true"

---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mpreg-federation
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
            - containerPort: 8000
          envFrom:
            - configMapRef:
                name: mpreg-config
          env:
            - name: MPREG_NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: MPREG_CONNECT
              value: "ws://mpreg-federation-0.mpreg-service:8000"
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
