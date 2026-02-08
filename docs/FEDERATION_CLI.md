# MPREG Fabric Federation CLI Documentation

The MPREG Fabric Federation CLI (`mpreg`) provides command-line tools for managing
fabric-enabled MPREG clusters including discovery, registration, health monitoring,
and deployment automation.

Note: the CLI uses `federation` terminology in command names and config keys, but
the underlying implementation is the unified fabric control plane.
If you omit `--port` when starting a server, the CLI auto-allocates a free port
and prints `MPREG_URL=...` for downstream commands.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Commands Overview](#commands-overview)
- [Real-World Scenarios](#real-world-scenarios)
- [Configuration Management](#configuration-management)
- [Monitoring and Operations](#monitoring-and-operations)
- [Advanced Usage](#advanced-usage)
- [Troubleshooting](#troubleshooting)

## Installation

The CLI is automatically available after installing MPREG:

```bash
# Install MPREG with uv
uv sync

# Verify CLI installation
uv run mpreg --help
```

## Quick Start

### 1. Discover Available Clusters

```bash
# Auto-discover clusters with rich table display
uv run mpreg discover

# Get JSON output for automation
uv run mpreg discover --output json
```

### 2. Generate Configuration Template

```bash
# Generate a fabric federation configuration template
uv run mpreg generate-config federation.json
```

### 3. Validate Configuration

```bash
# Validate your configuration file
uv run mpreg validate-config federation.json
```

### 4. Deploy Federation

```bash
# Test deployment (dry-run)
uv run mpreg deploy federation.json --dry-run

# Deploy fabric federation clusters
uv run mpreg deploy federation.json
```

## Commands Overview

### Core Commands

| Command      | Description                    | Example                                              |
| ------------ | ------------------------------ | ---------------------------------------------------- |
| `discover`   | Find available fabric clusters | `mpreg discover`                                     |
| `register`   | Register a new cluster         | `mpreg register prod-us cluster1 us-west-2 ws://...` |
| `unregister` | Remove a cluster               | `mpreg unregister prod-us`                           |
| `health`     | Check cluster health           | `mpreg health --cluster prod-us`                     |
| `metrics`    | Show performance metrics       | `mpreg metrics`                                      |
| `topology`   | Display fabric topology        | `mpreg topology`                                     |

### Server Commands

| Command        | Description                  | Example                                                       |
| -------------- | ---------------------------- | ------------------------------------------------------------- |
| `server start` | Start an MPREG server        | `mpreg server start --cluster-id dev-cluster`                 |
| `server start` | Override monitoring settings | `mpreg server start --monitoring-port 0 --no-monitoring-cors` |
| `server start` | Disable monitoring           | `mpreg server start --no-monitoring`                          |

### Auto-Discovery Commands

| Command                            | Description                             | Example                                                 |
| ---------------------------------- | --------------------------------------- | ------------------------------------------------------- |
| `auto-discovery discover`          | Run auto-discovery to find clusters     | `mpreg auto-discovery discover`                         |
| `auto-discovery discover --config` | Discover using configuration file       | `mpreg auto-discovery discover --config discovery.json` |
| `auto-discovery generate-config`   | Generate auto-discovery config template | `mpreg auto-discovery generate-config discovery.json`   |

### Configuration Commands

| Command           | Description                   | Example                                        |
| ----------------- | ----------------------------- | ---------------------------------------------- |
| `generate-config` | Create configuration template | `mpreg generate-config config.json`            |
| `validate-config` | Validate configuration        | `mpreg validate-config config.json`            |
| `deploy`          | Deploy from configuration     | `mpreg deploy config.json`                     |
| `config show`     | Display configuration         | `mpreg config show config.json`                |
| `config template` | Generate specific templates   | `mpreg config template production config.json` |

### Monitoring Commands

| Command                       | Description                       | Example                                             |
| ----------------------------- | --------------------------------- | --------------------------------------------------- |
| `monitor health-watch`        | Continuous health monitoring      | `mpreg monitor health-watch --interval 30`          |
| `monitor metrics-watch`       | Real-time metrics monitoring      | `mpreg monitor metrics-watch --interval 60`         |
| `monitor route-trace`         | Route selection trace             | `mpreg monitor route-trace --destination cluster-b` |
| `monitor link-state`          | Link-state status + area counters | `mpreg monitor link-state`                          |
| `monitor transport-endpoints` | Adapter endpoint assignments      | `mpreg monitor transport-endpoints`                 |
| `cleanup`                     | Clean up all resources            | `mpreg cleanup --force`                             |

## Auto-Discovery System

The MPREG Fabric CLI includes a comprehensive auto-discovery system that
automatically finds and registers fabric clusters using multiple discovery
protocols.

### Auto-Discovery Overview

The auto-discovery system supports multiple backend protocols:

- **📁 Static Config**: JSON file-based cluster definitions
- **🔍 Consul**: Service discovery via HashiCorp Consul
- **🌐 DNS SRV**: DNS-based service discovery
- **🔗 HTTP**: REST API endpoints for cluster information
- **📋 Config**: Static clusters from fabric configuration files

### Quick Start with Auto-Discovery

```bash
# Basic auto-discovery (uses default backends)
uv run mpreg auto-discovery run

# Generate auto-discovery configuration template
uv run mpreg auto-discovery generate discovery-config.json

# Use specific discovery configuration
uv run mpreg auto-discovery run --config discovery-config.json

# Get JSON output for automation
uv run mpreg auto-discovery run --output json
```

### Auto-Discovery Configuration

Generate a complete auto-discovery configuration:

```bash
uv run mpreg auto-discovery generate discovery.json
```

This creates a comprehensive configuration with all supported backends:

```json
{
  "auto_discovery": {
    "enabled": true,
    "backends": [
      {
        "protocol": "static_config",
        "config_path": "/etc/mpreg/clusters.json",
        "discovery_interval": 60.0
      },
      {
        "protocol": "consul",
        "host": "localhost",
        "port": 8500,
        "service_name": "mpreg",
        "datacenter": "dc1",
        "discovery_interval": 30.0
      },
      {
        "protocol": "dns_srv",
        "domain": "mpreg.local",
        "service": "_mpreg._tcp",
        "discovery_interval": 120.0
      },
      {
        "protocol": "http_endpoint",
        "discovery_url": "https://discovery.example.com/clusters",
        "discovery_interval": 60.0,
        "registration_ttl": 120.0
      }
    ]
  }
}
```

### Discovery Backend Details

#### Static Config Backend

Discovers clusters from JSON configuration files:

```json
{
  "clusters": [
    {
      "cluster_id": "prod-us-west",
      "cluster_name": "Production US West",
      "region": "us-west-2",
      "server_url": "ws://cluster-usw.company.com:<server-port>",
      "bridge_url": "ws://federation-usw.company.com:<bridge-port>",
      "health_score": 95.0,
      "tags": { "tier": "primary", "environment": "production" }
    }
  ]
}
```

**Configuration:**

```json
{
  "protocol": "static_config",
  "config_path": "/path/to/clusters.json",
  "discovery_interval": 60.0
}
```

#### Consul Backend

Integrates with HashiCorp Consul for service discovery:

**Configuration:**

```json
{
  "protocol": "consul",
  "host": "consul.company.com",
  "port": 8500,
  "service_name": "mpreg",
  "datacenter": "dc1",
  "discovery_interval": 30.0
}
```

#### DNS SRV Backend

Uses DNS SRV records for cluster discovery:

Notes:

- If `resolver_host` is omitted, MPREG will use the system resolver
  (by reading `/etc/resolv.conf` on Unix-like systems).
- `resolver_port`, `use_tcp`, and `timeout_seconds` let you tune DNS lookups.

**Configuration:**

```json
{
  "protocol": "dns_srv",
  "domain": "mpreg.company.com",
  "service": "_mpreg._tcp",
  "resolver_host": "127.0.0.1",
  "resolver_port": 53,
  "use_tcp": false,
  "timeout_seconds": 2.0,
  "discovery_interval": 120.0
}
```

#### HTTP Endpoint Backend

Discovers clusters via HTTP REST API:

**Configuration:**

```json
{
  "protocol": "http_endpoint",
  "discovery_url": "https://discovery.company.com/api/v1/clusters",
  "discovery_interval": 60.0,
  "registration_ttl": 120.0
}
```

### Discovery Output Format

The discovery command displays results in a rich table with discovery source information:

```bash
uv run mpreg auto-discovery run
```

```
🔍 Discovering federation clusters...
                             🌍 Federation Clusters
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Cluster ID    ┃ Name               ┃ Region   ┃  Status  ┃  Health  ┃  Source  ┃ Bridge URL                           ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ prod-us-west  │ Production US West │ us-we... │ 🟢 Active │ ✅ Healthy │ 🔍 Consul │ ws://federation-usw.company.com:<bridge-port> │
│ prod-eu-cent… │ Production EU Cen… │ eu-ce... │ 🟢 Active │ ⚠️ Degraded │ 📁 Static │ ws://federation-euc.company.com:<bridge-port> │
│ staging-glob… │ Staging Global     │ us-ea... │ 🔴 Inactive │ ❓ Unknown │ 🌐 DNS    │ ws://federation-staging.example.com… │
└───────────────┴────────────────────┴──────────┴──────────┴──────────┴──────────┴──────────────────────────────────────┘
```

### Integration with Federation Configuration

Auto-discovery can be integrated with standard federation configuration files:

```json
{
  "version": "1.0",
  "federation": {
    "enabled": true,
    "auto_discovery": true,
    "health_check_interval": 30
  },
  "auto_discovery": {
    "enabled": true,
    "backends": [
      {
        "protocol": "consul",
        "host": "consul.company.com",
        "port": 8500,
        "service_name": "mpreg",
        "discovery_interval": 30.0
      }
    ]
  },
  "clusters": [
    {
      "cluster_id": "manual-cluster",
      "cluster_name": "Manually Configured Cluster",
      "region": "us-east-1",
      "server_url": "ws://manual.company.com:<server-port>",
      "bridge_url": "ws://manual-bridge.company.com:<bridge-port>"
    }
  ]
}
```

This configuration combines:

- **Auto-discovered clusters** from Consul
- **Manually configured clusters** from the `clusters` array

### Health-Aware Discovery

The auto-discovery system includes health-aware filtering:

```bash
# Discover only healthy clusters
uv run mpreg auto-discovery run --config discovery.json
```

Clusters are automatically scored based on:

- **Health status**: healthy, degraded, unhealthy
- **Health score**: 0-100 numeric score
- **Response time**: Discovery backend response latency
- **Availability**: Backend availability and reliability

## Real-World Scenarios

### Scenario 1: Setting Up a Global Production Federation

**Goal**: Deploy a multi-region production federation with US West, EU Central, and Asia Pacific clusters.

```bash
# Step 1: Generate production configuration
uv run mpreg generate-config production-federation.json

# Step 2: Edit configuration for your infrastructure
# (Edit production-federation.json with your cluster details)

# Step 3: Validate the configuration
uv run mpreg validate-config production-federation.json

# Step 4: Test deployment
uv run mpreg deploy production-federation.json --dry-run

# Step 5: Deploy federation
uv run mpreg deploy production-federation.json

# Step 6: Verify deployment
uv run mpreg topology
uv run mpreg health
```

**Example Production Configuration:**

```json
{
  "version": "1.0",
  "federation": {
    "enabled": true,
    "auto_discovery": true,
    "health_check_interval": 30,
    "resilience": {
      "circuit_breaker": {
        "failure_threshold": 5,
        "success_threshold": 3,
        "timeout_seconds": 60
      },
      "retry_policy": {
        "max_attempts": 3,
        "initial_delay_seconds": 1.0,
        "backoff_multiplier": 2.0
      }
    }
  },
  "clusters": [
    {
      "cluster_id": "prod-us-west",
      "cluster_name": "Production US West",
      "region": "us-west-2",
      "server_url": "ws://mpreg-usw.company.com:<server-port>",
      "bridge_url": "ws://federation-usw.company.com:<bridge-port>",
      "priority": 1,
      "resources": ["compute", "storage", "ml-inference"],
      "tags": {
        "environment": "production",
        "tier": "primary",
        "datacenter": "usw2a"
      }
    },
    {
      "cluster_id": "prod-eu-central",
      "cluster_name": "Production EU Central",
      "region": "eu-central-1",
      "server_url": "ws://mpreg-euc.company.com:<server-port>",
      "bridge_url": "ws://federation-euc.company.com:<bridge-port>",
      "priority": 2,
      "resources": ["compute", "storage"],
      "tags": {
        "environment": "production",
        "tier": "secondary",
        "datacenter": "euc1a"
      }
    },
    {
      "cluster_id": "prod-asia-pacific",
      "cluster_name": "Production Asia Pacific",
      "region": "ap-southeast-1",
      "server_url": "ws://mpreg-aps.company.com:<server-port>",
      "bridge_url": "ws://federation-aps.company.com:<bridge-port>",
      "priority": 3,
      "resources": ["compute"],
      "tags": {
        "environment": "production",
        "tier": "secondary",
        "datacenter": "aps1a"
      }
    }
  ]
}
```

### Scenario 2: Development Environment Setup

**Goal**: Quickly set up a development federation for testing.

```bash
# Step 1: Register development clusters manually
uv run mpreg register \
  dev-local "Development Local" local \
  ws://localhost:<server-port> ws://localhost:<bridge-port>

uv run mpreg register \
  dev-staging "Development Staging" us-east-1 \
  ws://staging.dev.company.com:<server-port> ws://federation.dev.company.com:<bridge-port> \
  --no-resilience

# Step 2: Verify setup
uv run mpreg topology

# Step 3: Monitor during development
uv run mpreg monitor health-watch --interval 10 --clusters dev-local dev-staging
```

### Scenario 3: Production Health Monitoring

**Goal**: Set up comprehensive monitoring for a production federation.

```bash
# Continuous health monitoring with alerts
uv run mpreg monitor health-watch --interval 30 > health.log 2>&1 &

# Performance metrics monitoring
uv run mpreg monitor metrics-watch --interval 60 > metrics.log 2>&1 &

# Generate health report for operations team
uv run mpreg health --output json > daily-health-report.json

# Check specific cluster that's having issues
uv run mpreg health --cluster prod-eu-central --output report
```

### Scenario 4: Disaster Recovery and Failover

**Goal**: Handle cluster failures and perform emergency operations.

```bash
# Step 1: Check overall federation health
uv run mpreg health

# Step 2: Identify failed cluster
uv run mpreg health --cluster prod-us-west

# Step 3: Remove failed cluster from federation
uv run mpreg unregister prod-us-west

# Step 4: Add backup cluster
uv run mpreg register \
  backup-us-west "Backup US West" us-west-1 \
  ws://backup-usw.company.com:<server-port> ws://federation-backup-usw.company.com:<bridge-port>

# Step 5: Verify federation is operational
uv run mpreg topology
uv run mpreg health
```

### Scenario 5: Configuration Management and Updates

**Goal**: Update federation configuration across environments.

```bash
# Generate different templates for different environments
uv run mpreg config template production prod-config.json
uv run mpreg config template development dev-config.json

# Validate all configurations
uv run mpreg validate-config prod-config.json
uv run mpreg validate-config dev-config.json

# Show current configuration
uv run mpreg config show prod-config.json --key federation.resilience

# Deploy configuration updates
uv run mpreg deploy prod-config.json --dry-run
uv run mpreg deploy prod-config.json
```

## Configuration Management

### Configuration File Structure

The federation configuration file supports the following structure:

```json
{
  "version": "1.0",
  "federation": {
    "enabled": true,
    "auto_discovery": true,
    "health_check_interval": 30,
    "resilience": {
      "circuit_breaker": {
        "failure_threshold": 5,
        "success_threshold": 3,
        "timeout_seconds": 60
      },
      "retry_policy": {
        "max_attempts": 3,
        "initial_delay_seconds": 1.0,
        "backoff_multiplier": 2.0
      }
    }
  },
  "clusters": [
    {
      "cluster_id": "unique-cluster-id",
      "cluster_name": "Human Readable Name",
      "region": "aws-region-or-datacenter",
      "server_url": "ws://cluster.example.com:<server-port>",
      "bridge_url": "ws://federation.example.com:<bridge-port>",
      "priority": 1,
      "resources": ["compute", "storage"],
      "tags": {
        "environment": "production",
        "tier": "primary"
      }
    }
  ],
  "monitoring": {
    "enabled": true,
    "monitoring_port": 9090,
    "alert_endpoints": ["http://prometheus:9093/api/v1/alerts"]
  }
}
```

### Environment-Specific Configurations

**Production Configuration:**

```bash
# High resilience, comprehensive monitoring
uv run mpreg config template production prod.json
```

**Development Configuration:**

```bash
# Relaxed settings, faster feedback
{
  "federation": {
    "health_check_interval": 10,
    "resilience": {
      "circuit_breaker": {
        "failure_threshold": 10,
        "timeout_seconds": 30
      }
    }
  }
}
```

**Testing Configuration:**

```bash
# Minimal setup for testing
{
  "federation": {
    "enabled": true,
    "auto_discovery": false,
    "health_check_interval": 5
  },
  "clusters": [
    {
      "cluster_id": "test-local",
      "cluster_name": "Test Local",
      "region": "local",
      "server_url": "ws://localhost:<server-port>",
      "bridge_url": "ws://localhost:<bridge-port>"
    }
  ]
}
```

## Monitoring and Operations

### Health Monitoring

```bash
# Basic health check
uv run mpreg health

# Detailed health report
uv run mpreg health --output report

# JSON health data for monitoring systems
uv run mpreg health --output json

# Monitor specific cluster
uv run mpreg health --cluster prod-us-west

# Continuous monitoring
uv run mpreg monitor health-watch --interval 30
```

### Performance Metrics

```bash
# Show all cluster metrics
uv run mpreg metrics

# Monitor specific cluster
uv run mpreg metrics --cluster prod-us-west

# Continuous metrics monitoring
uv run mpreg monitor metrics-watch --interval 60
```

### Topology Visualization

```bash
# Show federation structure
uv run mpreg topology

# Example output:
🌍 Federation Topology
├── 🟢 prod-us-west
│   ├── 📍 Region: us-west-2
│   ├── 🔗 Server: ws://mpreg-usw.company.com:<server-port>
│   ├── 🌉 Bridge: ws://federation-usw.company.com:<bridge-port>
│   └── 🛡️ Resilience
│       ├── Enabled: ✅
│       ├── Circuit Breakers: 1
│       └── 🏥 Health
│           ├── Status: healthy
│           ├── Healthy: 1
│           └── Unhealthy: 0
└── 🟢 prod-eu-central
    ├── 📍 Region: eu-central-1
    ├── 🔗 Server: ws://mpreg-euc.company.com:<server-port>
    └── 🌉 Bridge: ws://federation-euc.company.com:<bridge-port>
```

## Advanced Usage

### Automation and Scripting

**Bash Script for Health Monitoring:**

```bash
#!/bin/bash
# health-monitor.sh

LOG_FILE="/var/log/mpreg-health.log"
ALERT_THRESHOLD=2

while true; do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

    # Get health status
    HEALTH_OUTPUT=$(uv run mpreg health --output json 2>/dev/null)

    if [ $? -eq 0 ]; then
        # Parse unhealthy clusters count
        UNHEALTHY=$(echo "$HEALTH_OUTPUT" | jq '.[] | select(.status == "error" or .status == "unhealthy") | length')

        if [ "$UNHEALTHY" -ge "$ALERT_THRESHOLD" ]; then
            echo "$TIMESTAMP ALERT: $UNHEALTHY unhealthy clusters detected" >> "$LOG_FILE"
            # Send alert (integrate with your alerting system)
            curl -X POST "https://alerts.company.com/webhook" \
                 -d "{\"message\": \"MPREG Federation Alert: $UNHEALTHY unhealthy clusters\"}"
        else
            echo "$TIMESTAMP OK: Federation healthy" >> "$LOG_FILE"
        fi
    else
        echo "$TIMESTAMP ERROR: Health check failed" >> "$LOG_FILE"
    fi

    sleep 300  # Check every 5 minutes
done
```

**Python Integration:**

```python
#!/usr/bin/env python3
# federation_manager.py

import subprocess
import json
import sys

def get_federation_health():
    """Get federation health as Python dict."""
    result = subprocess.run(
        ["uv", "run", "mpreg", "health", "--output", "json"],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        return json.loads(result.stdout)
    else:
        raise Exception(f"Health check failed: {result.stderr}")

def deploy_federation(config_path):
    """Deploy federation from configuration file."""
    # Validate first
    result = subprocess.run(
        ["uv", "run", "mpreg", "validate-config", config_path],
        capture_output=True
    )

    if result.returncode != 0:
        raise Exception(f"Configuration validation failed: {result.stderr}")

    # Deploy
    result = subprocess.run(
        ["uv", "run", "mpreg", "deploy", config_path],
        capture_output=True
    )

    return result.returncode == 0

# Example usage
if __name__ == "__main__":
    try:
        health = get_federation_health()
        print(f"Federation health: {health}")

        # Deploy if needed
        if len(sys.argv) > 1:
            config_file = sys.argv[1]
            success = deploy_federation(config_file)
            print(f"Deployment {'succeeded' if success else 'failed'}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
```

### CI/CD Integration

**GitHub Actions Workflow:**

```yaml
# .github/workflows/federation-deploy.yml
name: Deploy MPREG Federation

on:
  push:
    branches: [main]
    paths: ["federation-config.json"]

jobs:
  deploy-federation:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.12"

      - name: Install uv
        run: pip install uv

      - name: Install Dependencies
        run: uv sync

      - name: Validate Federation Config
        run: uv run mpreg validate-config federation-config.json

      - name: Deploy Federation (Dry Run)
        run: uv run mpreg deploy federation-config.json --dry-run

      - name: Deploy Federation
        if: github.ref == 'refs/heads/main'
        run: uv run mpreg deploy federation-config.json

      - name: Verify Deployment
        run: |
          uv run mpreg health --output json > health-report.json
          uv run mpreg topology
```

**Docker Integration:**

```dockerfile
# Dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync --no-dev

COPY . .

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD uv run mpreg health --output json || exit 1

# Default command
CMD ["uv", "run", "mpreg", "monitor", "health-watch", "--interval", "60"]
```

## Troubleshooting

### Common Issues

**1. Connection Refused Errors**

```bash
# Check if cluster URLs are accessible
curl -I ws://cluster.example.com:<server-port>

# Verify configuration URLs
uv run mpreg config show federation.json --key clusters
```

**2. Health Check Failures**

```bash
# Check specific cluster health
uv run mpreg health --cluster problematic-cluster

# Enable verbose logging
uv run mpreg --verbose health
```

**3. Configuration Validation Errors**

```bash
# Get detailed validation report
uv run mpreg validate-config config.json

# Check configuration syntax
python -m json.tool config.json
```

**4. Deployment Failures**

```bash
# Test deployment first
uv run mpreg deploy config.json --dry-run

# Check cluster connectivity
uv run mpreg discover --config config.json
```

### Debug Commands

```bash
# Enable verbose logging for all commands
uv run mpreg --verbose <command>

# Check CLI version and configuration
uv run mpreg --help

# Verify installation
uv run python -c "from mpreg.cli.main import cli; print('CLI OK')"
```

### Performance Tuning

**Monitoring Configuration:**

```json
{
  "federation": {
    "health_check_interval": 30, // Increase for less load
    "resilience": {
      "circuit_breaker": {
        "failure_threshold": 3, // Lower for faster failover
        "timeout_seconds": 60 // Adjust based on network latency
      }
    }
  }
}
```

**Resource Usage:**

```bash
# Monitor CLI resource usage
uv run mpreg monitor metrics-watch --interval 300  # 5 minutes

# Reduce monitoring frequency for production
uv run mpreg monitor health-watch --interval 60   # 1 minute
```

## Best Practices

1. **Configuration Management**
   - Use version control for configuration files
   - Validate configurations before deployment
   - Use environment-specific configuration files

2. **Monitoring**
   - Set up continuous health monitoring
   - Configure alerting for unhealthy clusters
   - Monitor performance metrics regularly

3. **Deployment**
   - Always test with `--dry-run` first
   - Deploy during maintenance windows
   - Have rollback procedures ready

4. **Security**
   - Use secure WebSocket connections (wss://)
   - Implement proper authentication
   - Regularly update cluster credentials

5. **Operations**
   - Document cluster purposes and dependencies
   - Maintain current cluster inventory
   - Plan for disaster recovery scenarios

## Getting Help

```bash
# General help
uv run mpreg --help

# Command-specific help
uv run mpreg deploy --help
uv run mpreg monitor --help

# Check CLI version
uv run mpreg --version
```

For additional support, refer to the main MPREG documentation or open an issue on the project repository.
