# MPREG Federation CLI Documentation

The MPREG Federation CLI (`mpreg-federation`) provides comprehensive command-line tools for managing federated MPREG clusters including discovery, registration, health monitoring, and deployment automation.

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
# Install MPREG with poetry
poetry install

# Verify CLI installation
poetry run mpreg-federation --help
```

## Quick Start

### 1. Discover Available Clusters

```bash
# Auto-discover clusters with rich table display
poetry run mpreg-federation discover

# Get JSON output for automation
poetry run mpreg-federation discover --output json
```

### 2. Generate Configuration Template

```bash
# Generate a complete federation configuration template
poetry run mpreg-federation generate-config federation.json
```

### 3. Validate Configuration

```bash
# Validate your configuration file
poetry run mpreg-federation validate-config federation.json
```

### 4. Deploy Federation

```bash
# Test deployment (dry-run)
poetry run mpreg-federation deploy federation.json --dry-run

# Deploy federation clusters
poetry run mpreg-federation deploy federation.json
```

## Commands Overview

### Core Commands

| Command      | Description                        | Example                                                         |
| ------------ | ---------------------------------- | --------------------------------------------------------------- |
| `discover`   | Find available federation clusters | `mpreg-federation discover`                                     |
| `register`   | Register a new cluster             | `mpreg-federation register prod-us cluster1 us-west-2 ws://...` |
| `unregister` | Remove a cluster                   | `mpreg-federation unregister prod-us`                           |
| `health`     | Check cluster health               | `mpreg-federation health --cluster prod-us`                     |
| `metrics`    | Show performance metrics           | `mpreg-federation metrics`                                      |
| `topology`   | Display federation structure       | `mpreg-federation topology`                                     |

### Auto-Discovery Commands

| Command                            | Description                             | Example                                                            |
| ---------------------------------- | --------------------------------------- | ------------------------------------------------------------------ |
| `auto-discovery discover`          | Run auto-discovery to find clusters     | `mpreg-federation auto-discovery discover`                         |
| `auto-discovery discover --config` | Discover using configuration file       | `mpreg-federation auto-discovery discover --config discovery.json` |
| `auto-discovery generate-config`   | Generate auto-discovery config template | `mpreg-federation auto-discovery generate-config discovery.json`   |

### Configuration Commands

| Command           | Description                   | Example                                                   |
| ----------------- | ----------------------------- | --------------------------------------------------------- |
| `generate-config` | Create configuration template | `mpreg-federation generate-config config.json`            |
| `validate-config` | Validate configuration        | `mpreg-federation validate-config config.json`            |
| `deploy`          | Deploy from configuration     | `mpreg-federation deploy config.json`                     |
| `config show`     | Display configuration         | `mpreg-federation config show config.json`                |
| `config template` | Generate specific templates   | `mpreg-federation config template production config.json` |

### Monitoring Commands

| Command                 | Description                  | Example                                                |
| ----------------------- | ---------------------------- | ------------------------------------------------------ |
| `monitor health-watch`  | Continuous health monitoring | `mpreg-federation monitor health-watch --interval 30`  |
| `monitor metrics-watch` | Real-time metrics monitoring | `mpreg-federation monitor metrics-watch --interval 60` |
| `cleanup`               | Clean up all resources       | `mpreg-federation cleanup --force`                     |

## Auto-Discovery System

The MPREG Federation CLI includes a comprehensive auto-discovery system that automatically finds and registers federation clusters using multiple discovery protocols.

### Auto-Discovery Overview

The auto-discovery system supports multiple backend protocols:

- **📁 Static Config**: JSON file-based cluster definitions
- **🔍 Consul**: Service discovery via HashiCorp Consul
- **🌐 DNS SRV**: DNS-based service discovery
- **🔗 HTTP**: REST API endpoints for cluster information
- **📋 Config**: Static clusters from federation configuration files

### Quick Start with Auto-Discovery

```bash
# Basic auto-discovery (uses default backends)
poetry run mpreg-federation auto-discovery run

# Generate auto-discovery configuration template
poetry run mpreg-federation auto-discovery generate discovery-config.json

# Use specific discovery configuration
poetry run mpreg-federation auto-discovery run --config discovery-config.json

# Get JSON output for automation
poetry run mpreg-federation auto-discovery run --output json
```

### Auto-Discovery Configuration

Generate a complete auto-discovery configuration:

```bash
poetry run mpreg-federation auto-discovery generate discovery.json
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
        "service_name": "mpreg-federation",
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
      "server_url": "ws://cluster-usw.company.com:8000",
      "bridge_url": "ws://federation-usw.company.com:9000",
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
  "service_name": "mpreg-federation",
  "datacenter": "dc1",
  "discovery_interval": 30.0
}
```

#### DNS SRV Backend

Uses DNS SRV records for cluster discovery:

**Configuration:**

```json
{
  "protocol": "dns_srv",
  "domain": "mpreg.company.com",
  "service": "_mpreg._tcp",
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
poetry run mpreg-federation auto-discovery run
```

```
🔍 Discovering federation clusters...
                             🌍 Federation Clusters
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Cluster ID    ┃ Name               ┃ Region   ┃  Status  ┃  Health  ┃  Source  ┃ Bridge URL                           ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ prod-us-west  │ Production US West │ us-we... │ 🟢 Active │ ✅ Healthy │ 🔍 Consul │ ws://federation-usw.company.com:9000 │
│ prod-eu-cent… │ Production EU Cen… │ eu-ce... │ 🟢 Active │ ⚠️ Degraded │ 📁 Static │ ws://federation-euc.company.com:9000 │
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
        "service_name": "mpreg-federation",
        "discovery_interval": 30.0
      }
    ]
  },
  "clusters": [
    {
      "cluster_id": "manual-cluster",
      "cluster_name": "Manually Configured Cluster",
      "region": "us-east-1",
      "server_url": "ws://manual.company.com:8000",
      "bridge_url": "ws://manual-bridge.company.com:9000"
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
poetry run mpreg-federation auto-discovery run --config discovery.json
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
poetry run mpreg-federation generate-config production-federation.json

# Step 2: Edit configuration for your infrastructure
# (Edit production-federation.json with your cluster details)

# Step 3: Validate the configuration
poetry run mpreg-federation validate-config production-federation.json

# Step 4: Test deployment
poetry run mpreg-federation deploy production-federation.json --dry-run

# Step 5: Deploy federation
poetry run mpreg-federation deploy production-federation.json

# Step 6: Verify deployment
poetry run mpreg-federation topology
poetry run mpreg-federation health
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
      "server_url": "ws://mpreg-usw.company.com:8000",
      "bridge_url": "ws://federation-usw.company.com:9000",
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
      "server_url": "ws://mpreg-euc.company.com:8000",
      "bridge_url": "ws://federation-euc.company.com:9000",
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
      "server_url": "ws://mpreg-aps.company.com:8000",
      "bridge_url": "ws://federation-aps.company.com:9000",
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
poetry run mpreg-federation register \
  dev-local "Development Local" local \
  ws://localhost:8000 ws://localhost:9000

poetry run mpreg-federation register \
  dev-staging "Development Staging" us-east-1 \
  ws://staging.dev.company.com:8000 ws://federation.dev.company.com:9000 \
  --no-resilience

# Step 2: Verify setup
poetry run mpreg-federation topology

# Step 3: Monitor during development
poetry run mpreg-federation monitor health-watch --interval 10 --clusters dev-local dev-staging
```

### Scenario 3: Production Health Monitoring

**Goal**: Set up comprehensive monitoring for a production federation.

```bash
# Continuous health monitoring with alerts
poetry run mpreg-federation monitor health-watch --interval 30 > health.log 2>&1 &

# Performance metrics monitoring
poetry run mpreg-federation monitor metrics-watch --interval 60 > metrics.log 2>&1 &

# Generate health report for operations team
poetry run mpreg-federation health --output json > daily-health-report.json

# Check specific cluster that's having issues
poetry run mpreg-federation health --cluster prod-eu-central --output report
```

### Scenario 4: Disaster Recovery and Failover

**Goal**: Handle cluster failures and perform emergency operations.

```bash
# Step 1: Check overall federation health
poetry run mpreg-federation health

# Step 2: Identify failed cluster
poetry run mpreg-federation health --cluster prod-us-west

# Step 3: Remove failed cluster from federation
poetry run mpreg-federation unregister prod-us-west

# Step 4: Add backup cluster
poetry run mpreg-federation register \
  backup-us-west "Backup US West" us-west-1 \
  ws://backup-usw.company.com:8000 ws://federation-backup-usw.company.com:9000

# Step 5: Verify federation is operational
poetry run mpreg-federation topology
poetry run mpreg-federation health
```

### Scenario 5: Configuration Management and Updates

**Goal**: Update federation configuration across environments.

```bash
# Generate different templates for different environments
poetry run mpreg-federation config template production prod-config.json
poetry run mpreg-federation config template development dev-config.json

# Validate all configurations
poetry run mpreg-federation validate-config prod-config.json
poetry run mpreg-federation validate-config dev-config.json

# Show current configuration
poetry run mpreg-federation config show prod-config.json --key federation.resilience

# Deploy configuration updates
poetry run mpreg-federation deploy prod-config.json --dry-run
poetry run mpreg-federation deploy prod-config.json
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
      "server_url": "ws://cluster.example.com:8000",
      "bridge_url": "ws://federation.example.com:9000",
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
    "metrics_port": 9090,
    "alert_endpoints": ["http://prometheus:9093/api/v1/alerts"]
  }
}
```

### Environment-Specific Configurations

**Production Configuration:**

```bash
# High resilience, comprehensive monitoring
poetry run mpreg-federation config template production prod.json
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
      "server_url": "ws://localhost:8000",
      "bridge_url": "ws://localhost:9000"
    }
  ]
}
```

## Monitoring and Operations

### Health Monitoring

```bash
# Basic health check
poetry run mpreg-federation health

# Detailed health report
poetry run mpreg-federation health --output report

# JSON health data for monitoring systems
poetry run mpreg-federation health --output json

# Monitor specific cluster
poetry run mpreg-federation health --cluster prod-us-west

# Continuous monitoring
poetry run mpreg-federation monitor health-watch --interval 30
```

### Performance Metrics

```bash
# Show all cluster metrics
poetry run mpreg-federation metrics

# Monitor specific cluster
poetry run mpreg-federation metrics --cluster prod-us-west

# Continuous metrics monitoring
poetry run mpreg-federation monitor metrics-watch --interval 60
```

### Topology Visualization

```bash
# Show federation structure
poetry run mpreg-federation topology

# Example output:
🌍 Federation Topology
├── 🟢 prod-us-west
│   ├── 📍 Region: us-west-2
│   ├── 🔗 Server: ws://mpreg-usw.company.com:8000
│   ├── 🌉 Bridge: ws://federation-usw.company.com:9000
│   └── 🛡️ Resilience
│       ├── Enabled: ✅
│       ├── Circuit Breakers: 1
│       └── 🏥 Health
│           ├── Status: healthy
│           ├── Healthy: 1
│           └── Unhealthy: 0
└── 🟢 prod-eu-central
    ├── 📍 Region: eu-central-1
    ├── 🔗 Server: ws://mpreg-euc.company.com:8000
    └── 🌉 Bridge: ws://federation-euc.company.com:9000
```

## Advanced Usage

### Automation and Scripting

**Bash Script for Health Monitoring:**

```bash
#!/bin/bash
# health-monitor.sh

LOG_FILE="/var/log/mpreg-federation-health.log"
ALERT_THRESHOLD=2

while true; do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

    # Get health status
    HEALTH_OUTPUT=$(poetry run mpreg-federation health --output json 2>/dev/null)

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
        ["poetry", "run", "mpreg-federation", "health", "--output", "json"],
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
        ["poetry", "run", "mpreg-federation", "validate-config", config_path],
        capture_output=True
    )

    if result.returncode != 0:
        raise Exception(f"Configuration validation failed: {result.stderr}")

    # Deploy
    result = subprocess.run(
        ["poetry", "run", "mpreg-federation", "deploy", config_path],
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

      - name: Install Poetry
        run: pip install poetry

      - name: Install Dependencies
        run: poetry install

      - name: Validate Federation Config
        run: poetry run mpreg-federation validate-config federation-config.json

      - name: Deploy Federation (Dry Run)
        run: poetry run mpreg-federation deploy federation-config.json --dry-run

      - name: Deploy Federation
        if: github.ref == 'refs/heads/main'
        run: poetry run mpreg-federation deploy federation-config.json

      - name: Verify Deployment
        run: |
          poetry run mpreg-federation health --output json > health-report.json
          poetry run mpreg-federation topology
```

**Docker Integration:**

```dockerfile
# Dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN pip install poetry && poetry install --no-dev

COPY . .

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD poetry run mpreg-federation health --output json || exit 1

# Default command
CMD ["poetry", "run", "mpreg-federation", "monitor", "health-watch", "--interval", "60"]
```

## Troubleshooting

### Common Issues

**1. Connection Refused Errors**

```bash
# Check if cluster URLs are accessible
curl -I ws://cluster.example.com:8000

# Verify configuration URLs
poetry run mpreg-federation config show federation.json --key clusters
```

**2. Health Check Failures**

```bash
# Check specific cluster health
poetry run mpreg-federation health --cluster problematic-cluster

# Enable verbose logging
poetry run mpreg-federation --verbose health
```

**3. Configuration Validation Errors**

```bash
# Get detailed validation report
poetry run mpreg-federation validate-config config.json

# Check configuration syntax
python -m json.tool config.json
```

**4. Deployment Failures**

```bash
# Test deployment first
poetry run mpreg-federation deploy config.json --dry-run

# Check cluster connectivity
poetry run mpreg-federation discover --config config.json
```

### Debug Commands

```bash
# Enable verbose logging for all commands
poetry run mpreg-federation --verbose <command>

# Check CLI version and configuration
poetry run mpreg-federation --help

# Verify installation
poetry run python -c "from mpreg.cli.main import cli; print('CLI OK')"
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
poetry run mpreg-federation monitor metrics-watch --interval 300  # 5 minutes

# Reduce monitoring frequency for production
poetry run mpreg-federation monitor health-watch --interval 60   # 1 minute
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
poetry run mpreg-federation --help

# Command-specific help
poetry run mpreg-federation deploy --help
poetry run mpreg-federation monitor --help

# Check CLI version
poetry run mpreg-federation --version  # (if implemented)
```

For additional support, refer to the main MPREG documentation or open an issue on the project repository.
