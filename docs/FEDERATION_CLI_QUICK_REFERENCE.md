# MPREG Federation CLI - Quick Reference

## Essential Commands

### 🔍 Discovery & Setup

```bash
# Discover clusters
poetry run mpreg-federation discover

# Generate config template
poetry run mpreg-federation generate-config config.json

# Validate configuration
poetry run mpreg-federation validate-config config.json

# Deploy federation
poetry run mpreg-federation deploy config.json --dry-run
poetry run mpreg-federation deploy config.json
```

### 🏥 Health & Monitoring

```bash
# Check health
poetry run mpreg-federation health
poetry run mpreg-federation health --cluster cluster-id

# Show metrics
poetry run mpreg-federation metrics

# Continuous monitoring
poetry run mpreg-federation monitor health-watch --interval 30
poetry run mpreg-federation monitor metrics-watch --interval 60
```

### 🌍 Cluster Management

```bash
# Register cluster
poetry run mpreg-federation register \
  cluster-id "Cluster Name" region \
  ws://server:8000 ws://bridge:9000

# Unregister cluster
poetry run mpreg-federation unregister cluster-id

# Show topology
poetry run mpreg-federation topology

# Cleanup all
poetry run mpreg-federation cleanup --force
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
      "server_url": "ws://cluster.company.com:8000",
      "bridge_url": "ws://federation.company.com:9000",
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
      "server_url": "ws://localhost:8000",
      "bridge_url": "ws://localhost:9000"
    }
  ]
}
```

## Common Workflows

### 🚀 Initial Setup

```bash
# 1. Generate config
poetry run mpreg-federation generate-config federation.json

# 2. Edit federation.json with your cluster details

# 3. Validate
poetry run mpreg-federation validate-config federation.json

# 4. Deploy
poetry run mpreg-federation deploy federation.json
```

### 📊 Daily Operations

```bash
# Morning health check
poetry run mpreg-federation health

# Check topology
poetry run mpreg-federation topology

# Review metrics
poetry run mpreg-federation metrics
```

### 🚨 Emergency Response

```bash
# Quick health assessment
poetry run mpreg-federation health --output json

# Remove failing cluster
poetry run mpreg-federation unregister failing-cluster-id

# Add backup cluster
poetry run mpreg-federation register backup-cluster "Backup" region ws://backup:8000 ws://backup:9000
```

### 🔧 Maintenance

```bash
# Validate before changes
poetry run mpreg-federation validate-config updated-config.json

# Test deployment
poetry run mpreg-federation deploy updated-config.json --dry-run

# Apply changes
poetry run mpreg-federation deploy updated-config.json

# Verify
poetry run mpreg-federation health
```

## Output Formats

### Health Check Outputs

```bash
# Rich table format (default)
poetry run mpreg-federation health

# JSON for automation
poetry run mpreg-federation health --output json

# Detailed report
poetry run mpreg-federation health --output report
```

### Discovery Outputs

```bash
# Rich table (default)
poetry run mpreg-federation discover

# JSON for scripting
poetry run mpreg-federation discover --output json
```

## Automation Examples

### Health Monitoring Script

```bash
#!/bin/bash
# Monitor and alert on unhealthy clusters

HEALTH_JSON=$(poetry run mpreg-federation health --output json)
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
if ! poetry run mpreg-federation validate-config "$CONFIG_FILE"; then
    echo "Configuration validation failed"
    exit 1
fi

# Test deployment
if ! poetry run mpreg-federation deploy "$CONFIG_FILE" --dry-run; then
    echo "Dry run failed"
    exit 1
fi

# Deploy
poetry run mpreg-federation deploy "$CONFIG_FILE"

# Verify
sleep 10
poetry run mpreg-federation health
```

## Troubleshooting Quick Fixes

### Connection Issues

```bash
# Test cluster connectivity
curl -I ws://cluster.example.com:8000

# Check configuration
poetry run mpreg-federation config show config.json --key clusters
```

### Health Check Failures

```bash
# Verbose health check
poetry run mpreg-federation --verbose health --cluster problematic-cluster

# Check specific cluster
poetry run mpreg-federation health --cluster cluster-id
```

### Configuration Problems

```bash
# Validate syntax
python -m json.tool config.json

# Check specific sections
poetry run mpreg-federation config show config.json --key federation.resilience
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
   poetry run mpreg-federation validate-config config.json
   ```

2. **Use dry-run for testing**

   ```bash
   poetry run mpreg-federation deploy config.json --dry-run
   ```

3. **Monitor continuously in production**

   ```bash
   poetry run mpreg-federation monitor health-watch --interval 30 &
   ```

4. **Use JSON output for automation**

   ```bash
   poetry run mpreg-federation health --output json | jq '.cluster_health'
   ```

5. **Check topology regularly**
   ```bash
   poetry run mpreg-federation topology
   ```

## Help Commands

```bash
# General help
poetry run mpreg-federation --help

# Command-specific help
poetry run mpreg-federation deploy --help
poetry run mpreg-federation monitor --help
poetry run mpreg-federation config --help
```
