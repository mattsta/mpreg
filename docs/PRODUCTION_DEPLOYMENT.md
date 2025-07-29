# MPREG Planet-Scale Federation Production Deployment Guide

This guide provides comprehensive instructions for deploying the MPREG planet-scale federation system in production environments.

## 🎯 Overview

The MPREG planet-scale federation system provides:

- **Graph-based routing** with geographic optimization
- **Hub-and-spoke architecture** with hierarchical routing
- **Gossip protocol** for epidemic information propagation
- **Distributed state management** with conflict resolution
- **SWIM-based failure detection** and membership management

## 🏗️ Architecture Overview

```
┌─────────────────┐
│   Global Hub    │ ← Single global coordinator
└─────────────────┘
         │
    ┌────┴────┐
    │         │
┌───▼───┐ ┌───▼───┐
│Regional│ │Regional│ ← Regional coordinators
│Hub     │ │Hub     │
└───┬───┘ └───┬───┘
    │         │
┌───▼───┐ ┌───▼───┐
│Local  │ │Local  │ ← Local cluster coordinators
│Hub    │ │Hub    │
└───────┘ └───────┘
```

## 📋 Prerequisites

### System Requirements

**Minimum Requirements per Node:**

- CPU: 4 cores
- RAM: 8GB
- Storage: 100GB SSD
- Network: 1Gbps
- OS: Linux (Ubuntu 20.04+ recommended)

**Recommended Requirements:**

- CPU: 8 cores
- RAM: 16GB
- Storage: 500GB NVMe SSD
- Network: 10Gbps
- OS: Linux (Ubuntu 22.04 LTS)

### Network Requirements

- **Latency**: <50ms between regional hubs
- **Bandwidth**: 100Mbps+ between hubs
- **Ports**:
  - 8080: HTTP API
  - 8081: Gossip protocol
  - 8082: Membership protocol
  - 8083: Consensus protocol

### Python Environment

```bash
# Python 3.9+ required
python3 --version

# Install poetry for dependency management
curl -sSL https://install.python-poetry.org | python3 -
```

## 🚀 Installation

### 1. Clone and Setup

```bash
git clone https://github.com/yourusername/mpreg.git
cd mpreg
poetry install --no-dev
```

### 2. Configuration

Create a configuration file for each node:

```yaml
# config/production.yaml
node:
  id: "global-hub-001"
  region: "us-east"
  hub_tier: "global"
  coordinates:
    latitude: 40.7128
    longitude: -74.0060

network:
  listen_port: 8080
  gossip_port: 8081
  membership_port: 8082
  consensus_port: 8083

gossip:
  interval: 1.0
  fanout: 3
  strategy: "hybrid"

membership:
  probe_interval: 2.0
  probe_timeout: 5.0
  suspicion_timeout: 30.0

consensus:
  threshold: 0.6
  proposal_timeout: 10.0

monitoring:
  metrics_enabled: true
  metrics_port: 9090
  health_check_interval: 30.0
```

### 3. Environment Variables

```bash
# Required environment variables
export MPREG_NODE_ID="global-hub-001"
export MPREG_REGION="us-east"
export MPREG_CONFIG_PATH="/path/to/config/production.yaml"
export MPREG_LOG_LEVEL="INFO"
export MPREG_METRICS_ENABLED="true"
```

## 🌍 Deployment Topology

### Global Hub Deployment

Deploy **one** global hub per federation:

```bash
# Global hub configuration
export MPREG_HUB_TIER="global"
export MPREG_MAX_REGIONAL_HUBS=10

python -m mpreg.production.start_node \
  --config /path/to/global-hub.yaml \
  --hub-tier global
```

### Regional Hub Deployment

Deploy **one** regional hub per major geographic region:

```bash
# Regional hub configuration
export MPREG_HUB_TIER="regional"
export MPREG_MAX_CHILD_HUBS=20

python -m mpreg.production.start_node \
  --config /path/to/regional-hub.yaml \
  --hub-tier regional \
  --parent-hub global-hub-001
```

### Local Hub Deployment

Deploy **multiple** local hubs per region:

```bash
# Local hub configuration
export MPREG_HUB_TIER="local"
export MPREG_MAX_CLUSTERS=100

python -m mpreg.production.start_node \
  --config /path/to/local-hub.yaml \
  --hub-tier local \
  --parent-hub regional-hub-us-east
```

## 🔧 Configuration Management

### Hub Tier Configuration

```python
# examples/production_config.py
from mpreg.production import ProductionConfig

# Global hub configuration
global_config = ProductionConfig(
    node_id="global-hub-001",
    hub_tier="global",
    max_regional_hubs=10,
    gossip_interval=2.0,
    consensus_threshold=0.7
)

# Regional hub configuration
regional_config = ProductionConfig(
    node_id="regional-hub-us-east",
    hub_tier="regional",
    parent_hub="global-hub-001",
    max_child_hubs=20,
    gossip_interval=1.0,
    consensus_threshold=0.6
)

# Local hub configuration
local_config = ProductionConfig(
    node_id="local-hub-nyc-01",
    hub_tier="local",
    parent_hub="regional-hub-us-east",
    max_clusters=100,
    gossip_interval=0.5,
    consensus_threshold=0.5
)
```

### Security Configuration

```yaml
# config/security.yaml
security:
  tls_enabled: true
  cert_file: "/path/to/cert.pem"
  key_file: "/path/to/key.pem"
  ca_file: "/path/to/ca.pem"

  authentication:
    method: "jwt"
    secret_key: "${JWT_SECRET_KEY}"
    token_expiry: 3600

  authorization:
    rbac_enabled: true
    admin_roles: ["federation_admin"]
    operator_roles: ["federation_operator"]
```

## 📊 Monitoring and Observability

### Metrics Collection

```python
# examples/production_monitoring.py
from mpreg.monitoring import MetricsCollector, PrometheusExporter

# Initialize metrics collection
metrics = MetricsCollector()
prometheus_exporter = PrometheusExporter(port=9090)

# Key metrics to monitor
metrics.register_gauge("federation_nodes_total")
metrics.register_gauge("federation_nodes_healthy")
metrics.register_histogram("gossip_message_latency")
metrics.register_histogram("consensus_proposal_time")
metrics.register_counter("membership_failures_total")
```

### Health Checks

```python
# examples/health_checks.py
from mpreg.health import HealthChecker

health_checker = HealthChecker()

# Register health check endpoints
health_checker.register_check("gossip_protocol", gossip_protocol.health_check)
health_checker.register_check("membership_protocol", membership_protocol.health_check)
health_checker.register_check("consensus_manager", consensus_manager.health_check)

# Health check endpoint returns:
# {
#   "status": "healthy",
#   "checks": {
#     "gossip_protocol": {"status": "healthy", "details": {...}},
#     "membership_protocol": {"status": "healthy", "details": {...}},
#     "consensus_manager": {"status": "healthy", "details": {...}}
#   }
# }
```

### Logging Configuration

```python
# examples/production_logging.py
import logging
from mpreg.logging import StructuredLogger

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Use structured logging for better observability
logger = StructuredLogger(__name__)

logger.info("Node started", extra={
    "node_id": "global-hub-001",
    "region": "us-east",
    "hub_tier": "global"
})
```

## 🔄 Operational Procedures

### Starting a Production Node

```bash
#!/bin/bash
# scripts/start_production_node.sh

set -e

NODE_ID=${1:-"local-hub-001"}
CONFIG_PATH=${2:-"/etc/mpreg/production.yaml"}

echo "Starting MPREG federation node: $NODE_ID"

# Validate configuration
python -m mpreg.validation.validate_config --config $CONFIG_PATH

# Start node with proper logging
nohup python -m mpreg.production.start_node \
  --config $CONFIG_PATH \
  --node-id $NODE_ID \
  --log-level INFO \
  --metrics-enabled \
  > /var/log/mpreg/$NODE_ID.log 2>&1 &

echo "Node $NODE_ID started successfully"
echo "PID: $(pgrep -f $NODE_ID)"
```

### Graceful Shutdown

```python
# examples/graceful_shutdown.py
import signal
import asyncio
from mpreg.production import ProductionNode

class GracefulShutdown:
    def __init__(self, node: ProductionNode):
        self.node = node
        self.shutdown_initiated = False

        # Register signal handlers
        signal.signal(signal.SIGTERM, self.shutdown_handler)
        signal.signal(signal.SIGINT, self.shutdown_handler)

    def shutdown_handler(self, signum, frame):
        if not self.shutdown_initiated:
            self.shutdown_initiated = True
            asyncio.create_task(self.graceful_shutdown())

    async def graceful_shutdown(self):
        logger.info("Initiating graceful shutdown...")

        # 1. Stop accepting new requests
        await self.node.stop_accepting_requests()

        # 2. Finish processing current requests
        await self.node.finish_current_requests(timeout=30.0)

        # 3. Announce departure
        await self.node.announce_departure()

        # 4. Stop all protocols
        await self.node.stop()

        logger.info("Graceful shutdown completed")
```

### Rolling Updates

```bash
#!/bin/bash
# scripts/rolling_update.sh

set -e

NODES=("local-hub-001" "local-hub-002" "local-hub-003")
NEW_VERSION="v2.0.0"

echo "Starting rolling update to version $NEW_VERSION"

for NODE in "${NODES[@]}"; do
    echo "Updating node: $NODE"

    # 1. Drain traffic from node
    curl -X POST "http://$NODE:8080/admin/drain"

    # 2. Wait for drain completion
    sleep 30

    # 3. Stop node gracefully
    curl -X POST "http://$NODE:8080/admin/shutdown"

    # 4. Update node binary
    systemctl stop mpreg-$NODE
    cp /tmp/mpreg-$NEW_VERSION /usr/local/bin/mpreg

    # 5. Start node with new version
    systemctl start mpreg-$NODE

    # 6. Wait for node to become healthy
    timeout 60 bash -c "until curl -f http://$NODE:8080/health; do sleep 5; done"

    echo "Node $NODE updated successfully"
done

echo "Rolling update completed"
```

## 🎯 Performance Tuning

### Gossip Protocol Optimization

```python
# examples/performance_tuning.py
from mpreg.federation.federation_gossip import GossipProtocol, GossipStrategy

# High-performance gossip configuration
gossip_config = {
    "gossip_interval": 0.5,  # Faster convergence
    "fanout": 5,             # More connections
    "strategy": GossipStrategy.HYBRID,
    "max_message_size": 64 * 1024,  # 64KB messages
    "compression_enabled": True,
    "batch_size": 10         # Batch messages
}

gossip_protocol = GossipProtocol(
    node_id="high-perf-node",
    **gossip_config
)
```

### Memory Optimization

```python
# examples/memory_optimization.py
from mpreg.optimization import MemoryOptimizer

# Configure memory limits
memory_optimizer = MemoryOptimizer(
    max_graph_cache_size=1000,
    max_gossip_messages=10000,
    max_consensus_proposals=100,
    cleanup_interval=300.0
)

# Apply optimizations
memory_optimizer.apply_to_node(node)
```

### Network Optimization

```python
# examples/network_optimization.py
from mpreg.network import NetworkOptimizer

# Configure network optimizations
network_optimizer = NetworkOptimizer(
    tcp_nodelay=True,
    tcp_keepalive=True,
    connection_pooling=True,
    max_connections_per_node=10,
    connection_timeout=30.0
)
```

## 📋 Troubleshooting

### Common Issues

**1. Node Discovery Problems**

```bash
# Check hub registry status
curl http://localhost:8080/api/registry/status

# Verify network connectivity
telnet regional-hub-us-east 8081

# Check logs for discovery issues
grep "discovery" /var/log/mpreg/node.log
```

**2. Gossip Protocol Issues**

```bash
# Check gossip statistics
curl http://localhost:8080/api/gossip/stats

# Verify message propagation
curl -X POST http://localhost:8080/api/gossip/test-message

# Monitor gossip bandwidth
iftop -i eth0 -P -p 8081
```

**3. Consensus Problems**

```bash
# Check consensus status
curl http://localhost:8080/api/consensus/status

# List active proposals
curl http://localhost:8080/api/consensus/proposals

# Check voting statistics
curl http://localhost:8080/api/consensus/votes
```

### Debugging Commands

```bash
# Check node health
curl http://localhost:8080/health

# Get comprehensive status
curl http://localhost:8080/api/status

# Monitor real-time metrics
curl http://localhost:9090/metrics

# Validate configuration
python -m mpreg.validation.validate_config --config production.yaml

# Test network connectivity
python -m mpreg.tools.network_test --target regional-hub-us-east:8081
```

## 🔒 Security Considerations

### Network Security

```yaml
# Firewall rules
firewall_rules:
  - port: 8080
    protocol: tcp
    source: "internal_network"
    description: "HTTP API"

  - port: 8081
    protocol: tcp
    source: "federation_nodes"
    description: "Gossip protocol"
```

### Authentication and Authorization

```python
# examples/security_config.py
from mpreg.security import SecurityManager, JWTAuth, RBACAuth

# Configure authentication
jwt_auth = JWTAuth(
    secret_key=os.environ["JWT_SECRET_KEY"],
    token_expiry=3600,
    issuer="mpreg-federation"
)

# Configure authorization
rbac_auth = RBACAuth(
    roles={
        "admin": ["read", "write", "admin"],
        "operator": ["read", "write"],
        "viewer": ["read"]
    }
)

security_manager = SecurityManager(
    authentication=jwt_auth,
    authorization=rbac_auth
)
```

## 📈 Scaling Guidelines

### Horizontal Scaling

**Adding New Regions:**

1. Deploy regional hub in new region
2. Configure geographic coordinates
3. Connect to global hub
4. Deploy local hubs in region

**Adding Local Hubs:**

1. Deploy new local hub
2. Connect to regional hub
3. Register clusters with local hub
4. Monitor load distribution

### Vertical Scaling

**CPU Scaling:**

- Monitor CPU utilization
- Scale up when >80% utilization
- Consider CPU-intensive operations (routing, consensus)

**Memory Scaling:**

- Monitor memory usage
- Scale up when >75% utilization
- Consider memory-intensive caches

**Network Scaling:**

- Monitor network throughput
- Scale up when >70% utilization
- Consider gossip traffic patterns

## 🎯 Best Practices

### Deployment Best Practices

1. **Always use configuration management** (Ansible, Terraform)
2. **Deploy in multiple availability zones**
3. **Use load balancers** for high availability
4. **Implement circuit breakers** for resilience
5. **Monitor all key metrics** continuously

### Operational Best Practices

1. **Regular health checks** every 30 seconds
2. **Automated failover** within 60 seconds
3. **Rolling updates** with zero downtime
4. **Backup configurations** regularly
5. **Test disaster recovery** procedures

### Performance Best Practices

1. **Tune gossip parameters** for your network
2. **Monitor consensus latency** and adjust thresholds
3. **Use geographic routing** for optimal paths
4. **Implement caching** for frequently accessed data
5. **Profile memory usage** regularly

## 📚 Additional Resources

- [API Documentation](./API.md)
- [Performance Benchmarks](./BENCHMARKS.md)
- [Security Guide](./SECURITY.md)
- [Monitoring Guide](./MONITORING.md)
- [Troubleshooting Guide](./TROUBLESHOOTING.md)

## 🆘 Support

For production support:

- Create issues at: https://github.com/yourusername/mpreg/issues
- Email: support@mpreg.io
- Slack: #mpreg-support

---

This production deployment guide provides the foundation for deploying MPREG's planet-scale federation system in production environments with high availability, scalability, and operational excellence.
