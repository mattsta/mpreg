# Port Allocation Migration Guide

This guide explains how to migrate existing MPREG tests to use the concurrent-safe port allocation system.

## Overview

The port allocation system enables safe concurrent testing with `pytest-xdist` by ensuring that each test worker gets non-overlapping port ranges. This prevents the common issue where multiple tests try to bind to the same port simultaneously.

## Key Components

### 1. Port Allocator (`mpreg/core/port_allocator.py`)

Port fixtures are now defined in `tests/conftest.py` and backed by the core
allocator (no test-only allocator module).

- **Thread-safe**: Uses file-based locking across processes
- **Worker-aware**: Automatically detects pytest-xdist worker ID
- **Port ranges**: Different categories (servers, clients, federation, testing)
- **Automatic cleanup**: Releases ports and cleans up stale locks

### 2. Test Helpers (`test_helpers.py`)

- **URL generation**: Convert ports to WebSocket URLs
- **Context managers**: Automatic port cleanup
- **TestPortManager**: For complex multi-port scenarios

### 3. Pytest Fixtures (`conftest.py`)

- Pre-configured fixtures for common scenarios
- Automatic integration with existing test infrastructure

## Migration Patterns

### Pattern 1: Simple Server Tests

**Before:**

```python
@pytest.fixture
async def my_server():
    server = MPREGServer(MPREGSettings(port=LEGACY_PORT, ...))
    # ... setup and cleanup
```

**After:**

```python
@pytest.fixture
async def my_server(server_port: int):
    server = MPREGServer(MPREGSettings(port=server_port, ...))
    # ... setup and cleanup
```

### Pattern 2: Multi-Server Clusters

**Before:**

```python
@pytest.fixture
async def cluster():
    server1 = MPREGServer(MPREGSettings(port=LEGACY_PORT_A, ...))
    server2 = MPREGServer(MPREGSettings(port=LEGACY_PORT_B, connect="ws://127.0.0.1:LEGACY_PORT_A", ...))
```

**After:**

```python
@pytest.fixture
async def cluster(port_pair: list[int]):
    port1, port2 = port_pair
    server1 = MPREGServer(MPREGSettings(port=port1, ...))
    server2 = MPREGServer(MPREGSettings(port=port2, connect=f"ws://127.0.0.1:{port1}", ...))
```

### Pattern 3: Complex Multi-Port Scenarios

**Before:**

```python
async def test_complex():
    # Multiple hardcoded ports
    servers = [
        MPREGServer(MPREGSettings(port=LEGACY_PORT_A, ...)),
        MPREGServer(MPREGSettings(port=LEGACY_PORT_B, ...)),
        MPREGServer(MPREGSettings(port=LEGACY_PORT_C, ...)),
    ]
```

**After:**

```python
async def test_complex():
    with TestPortManager() as port_manager:
        ports = port_manager.get_port_range(3, "servers")
        servers = [
            MPREGServer(MPREGSettings(port=port, ...))
            for port in ports
        ]
        # Automatic cleanup when context exits
```

### Pattern 4: Client URL Generation

**Before:**

```python
async def test_client():
    async with MPREGClientAPI("ws://127.0.0.1:LEGACY_PORT") as client:
        # ...
```

**After:**

```python
async def test_client(single_server):
    port = single_server.settings.port
    async with MPREGClientAPI(f"ws://127.0.0.1:{port}") as client:
        # ...
```

Or with helpers:

```python
async def test_client():
    with test_server_url("servers") as server_url:
        async with MPREGClientAPI(server_url) as client:
            # ...
```

## Available Fixtures

### Basic Port Fixtures

- `test_port`: Single port for testing
- `server_port`: Single port for servers
- `client_port`: Single port for clients
- `federation_port`: Single port for federation

### Multi-Port Fixtures

- `port_pair`: List of 2 ports
- `server_cluster_ports`: List of 4 server ports

### Utility Fixtures

- `port_allocator`: Direct access to port allocator instance

## Port Ranges by Worker

Each pytest-xdist worker gets its own port range:

| Category   | Master (gw0) | Worker 1 (gw1) | Worker 2 (gw2) |
| ---------- | ------------ | -------------- | -------------- |
| servers    | 10000-10099  | 10100-10199    | 10200-10299    |
| clients    | 11000-11099  | 11100-11199    | 11200-11299    |
| federation | 12000-12099  | 12100-12199    | 12200-12299    |
| testing    | 13000-13099  | 13100-13199    | 13200-13299    |

## Running Concurrent Tests

```bash
# Run tests in parallel with 4 workers
uv run pytest -n 4

# Run with auto-detection of CPU cores
uv run pytest -n auto

# Run specific test categories in parallel
uv run pytest -n 4 -m "not slow"

# Run with verbose output
uv run pytest -n 4 -v
```

## Migration Checklist

- [ ] Replace hardcoded ports with port allocation fixtures
- [ ] Update client URLs to use dynamic ports
- [ ] Add proper cleanup in test fixtures
- [ ] Test with `pytest -n 2` to verify concurrency safety
- [ ] Mark slow tests with `@pytest.mark.slow`
- [ ] Mark integration tests with `@pytest.mark.integration`

## Common Issues and Solutions

### Issue: Port Already in Use

**Symptom:** `OSError: [Errno 48] Address already in use`
**Solution:** Use port allocation system instead of hardcoded ports

### Issue: Worker Port Conflicts

**Symptom:** Tests pass individually but fail when run concurrently
**Solution:** Ensure all port usage goes through the allocation system

### Issue: Stale Port Locks

**Symptom:** Port allocator claims no ports available
**Solution:** Locks auto-expire after 1 hour, or manually clean `/tmp/mpreg_port_locks/`

### Issue: Test Timeouts in Parallel

**Symptom:** Tests time out when run with `-n auto`
**Solution:** Increase timeout values and ensure proper async cleanup

## Examples

See `test_port_allocation_demo.py` for comprehensive examples of all patterns.
