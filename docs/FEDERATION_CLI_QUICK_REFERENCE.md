# MPREG Fabric Federation CLI - Quick Reference

Note: CLI command names and config keys still use `federation`, but all behavior
is powered by the unified fabric control plane.
Omit `--port` on `mpreg server start` to auto-allocate a free port; the CLI prints
`MPREG_URL=...` for easy client use.

## Essential Commands

### 🔍 Discovery & Setup

```bash
# Discover clusters
uv run mpreg discover

# Generate config template
uv run mpreg generate-config config.json

# Validate configuration
uv run mpreg validate-config config.json

# Deploy federation
uv run mpreg deploy config.json --dry-run
uv run mpreg deploy config.json
```

### 🖥️ Server Start

```bash
# Start a server with defaults
uv run mpreg server start --port <port> --cluster-id dev-cluster

# Configure monitoring
uv run mpreg server start --monitoring-port <port> --no-monitoring-cors

# Disable monitoring endpoints
uv run mpreg server start --no-monitoring
```

### 🏥 Health & Monitoring

```bash
# Check health
uv run mpreg health
uv run mpreg health --cluster cluster-id

# Show metrics
uv run mpreg metrics

# Continuous monitoring
uv run mpreg monitor health-watch --interval 30
uv run mpreg monitor metrics-watch --interval 60
uv run mpreg monitor route-trace --destination cluster-b
uv run mpreg monitor link-state
uv run mpreg monitor transport-endpoints
uv run mpreg monitor persistence
uv run mpreg monitor persistence-watch --interval 30
uv run mpreg monitor metrics --system unified
uv run mpreg monitor endpoints

# Monitoring endpoint shortcuts (set MPREG_MONITORING_URL)
uv run mpreg monitor health --summary
uv run mpreg monitor metrics-watch --system transport --interval 15
uv run mpreg monitor status
```

### 🌍 Cluster Management

```bash
# Register cluster
uv run mpreg register \
  cluster-id "Cluster Name" region \
  ws://server:<port> ws://bridge:<port>

# Unregister cluster
uv run mpreg unregister cluster-id

# Show topology
uv run mpreg topology

# Cleanup all
uv run mpreg cleanup --force
```

## Quick Configuration Templates

### Production Template

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
      "cluster_id": "prod-primary",
      "cluster_name": "Production Primary",
      "region": "us-west-2",
      "server_url": "ws://cluster.company.com:<port>",
      "bridge_url": "ws://federation.company.com:<port>",
      "priority": 1,
      "resources": ["compute", "storage"]
    }
  ]
}
```

### Development Template

```json
{
  "version": "1.0",
  "federation": {
    "enabled": true,
    "health_check_interval": 10
  },
  "clusters": [
    {
      "cluster_id": "dev-local",
      "cluster_name": "Development Local",
      "region": "local",
      "server_url": "ws://localhost:<port>",
      "bridge_url": "ws://localhost:<port>"
    }
  ]
}
```

## Common Workflows

### 🚀 Initial Setup

```bash
# 1. Generate config
uv run mpreg generate-config federation.json

# 2. Edit federation.json with your cluster details

# 3. Validate
uv run mpreg validate-config federation.json

# 4. Deploy
uv run mpreg deploy federation.json
```

### 📊 Daily Operations

```bash
# Morning health check
uv run mpreg health

# Check topology
uv run mpreg topology

# Review metrics
uv run mpreg metrics
```

### 🚨 Emergency Response

```bash
# Quick health assessment
uv run mpreg health --output json

# Remove failing cluster
uv run mpreg unregister failing-cluster-id

# Add backup cluster
uv run mpreg register backup-cluster "Backup" region ws://backup:<server-port> ws://backup:<bridge-port>
```

### 🔧 Maintenance

```bash
# Validate before changes
uv run mpreg validate-config updated-config.json

# Test deployment
uv run mpreg deploy updated-config.json --dry-run

# Apply changes
uv run mpreg deploy updated-config.json

# Verify
uv run mpreg health
```

## Output Formats

### Health Check Outputs

```bash
# Rich table format (default)
uv run mpreg health

# JSON for automation
uv run mpreg health --output json

# Detailed report
uv run mpreg health --output report
```

### Discovery Outputs

```bash
# Rich table (default)
uv run mpreg discover

# JSON for scripting
uv run mpreg discover --output json
```

## Automation Examples

### Health Monitoring Script

```bash
#!/bin/bash
# Monitor and alert on unhealthy clusters

HEALTH_JSON=$(uv run mpreg health --output json)
UNHEALTHY_COUNT=$(echo "$HEALTH_JSON" | jq '[.[] | select(.status=="error")] | length')

if [ "$UNHEALTHY_COUNT" -gt 0 ]; then
    echo "ALERT: $UNHEALTHY_COUNT unhealthy clusters detected"
    # Send alert to monitoring system
    curl -X POST "$WEBHOOK_URL" -d "{\"message\": \"$UNHEALTHY_COUNT unhealthy clusters\"}"
fi
```

### Deployment Pipeline

```bash
#!/bin/bash
# Safe deployment with validation

CONFIG_FILE="$1"

# Validate configuration
if ! uv run mpreg validate-config "$CONFIG_FILE"; then
    echo "Configuration validation failed"
    exit 1
fi

# Test deployment
if ! uv run mpreg deploy "$CONFIG_FILE" --dry-run; then
    echo "Dry run failed"
    exit 1
fi

# Deploy
uv run mpreg deploy "$CONFIG_FILE"

# Verify
sleep 10
uv run mpreg health
```

## Troubleshooting Quick Fixes

### Connection Issues

```bash
# Test cluster connectivity
curl -I ws://cluster.example.com:<server-port>

# Check configuration
uv run mpreg config show config.json --key clusters
```

### Health Check Failures

```bash
# Verbose health check
uv run mpreg --verbose health --cluster problematic-cluster

# Check specific cluster
uv run mpreg health --cluster cluster-id
```

### Configuration Problems

```bash
# Validate syntax
python -m json.tool config.json

# Check specific sections
uv run mpreg config show config.json --key federation.resilience
```

## Environment Variables

```bash
# Enable verbose logging
export MPREG_CLI_VERBOSE=1

# Set default configuration
export MPREG_FEDERATION_CONFIG=/path/to/config.json
```

## File Locations

- Configuration: `federation.json` (custom location)
- Logs: Check with `--verbose` flag
- Templates: Generated with `generate-config`

## Tips & Tricks

1. **Always validate before deploying**

   ```bash
   uv run mpreg validate-config config.json
   ```

2. **Use dry-run for testing**

   ```bash
   uv run mpreg deploy config.json --dry-run
   ```

3. **Monitor continuously in production**

   ```bash
   uv run mpreg monitor health-watch --interval 30 &
   ```

4. **Use JSON output for automation**

   ```bash
   uv run mpreg health --output json | jq '.cluster_health'
   ```

5. **Check topology regularly**
   ```bash
   uv run mpreg topology
   ```

## Help Commands

```bash
# General help
uv run mpreg --help

# Command-specific help
uv run mpreg deploy --help
uv run mpreg monitor --help
uv run mpreg config --help
```
