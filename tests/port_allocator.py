"""
Thread-safe port allocation system for concurrent testing.

This module provides a robust port allocation system that ensures
non-overlapping ports across concurrent test processes. It uses
file-based locking and worker ID detection to provide safe,
deterministic port allocation for pytest-xdist.
"""

import os
import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from mpreg.datastructures.type_aliases import PortNumber


@dataclass
class PortRange:
    """Port range configuration for different test types."""

    start: int
    end: int
    description: str = ""


class PortAllocator:
    """
    Thread-safe port allocator for concurrent testing.

    Features:
    - Worker-aware port allocation for pytest-xdist
    - File-based locking to prevent conflicts across processes
    - Automatic port availability checking
    - Configurable port ranges for different test types
    - Cleanup of stale locks
    """

    # Port ranges for different test categories
    # Optimized ranges to support high-capacity concurrent testing
    RANGES = {
        "servers": PortRange(10000, 16000, "Main server ports (6000 ports)"),
        "clients": PortRange(16000, 20000, "Client connection ports (4000 ports)"),
        "federation": PortRange(20000, 24000, "Federation bridge ports (4000 ports)"),
        "testing": PortRange(24000, 30000, "General testing ports (6000 ports)"),
        "research": PortRange(
            30000, 50000, "Research & scalability testing (20000 ports)"
        ),
    }

    def __init__(self, base_dir: str | None = None):
        """Initialize port allocator with optional base directory for lock files."""
        if base_dir is None:
            base_dir = os.environ.get("PYTEST_CURRENT_TEST_DIR", "/tmp")

        self.base_dir = Path(base_dir) / "mpreg_port_locks"
        self.base_dir.mkdir(exist_ok=True)

        # Worker ID detection for pytest-xdist
        self.worker_id = self._get_worker_id()
        self.worker_offset = self._calculate_worker_offset()

        # Thread safety
        self._lock = threading.Lock()
        self._allocated_ports: set[int] = set()

        # Cleanup old locks on startup
        self._cleanup_stale_locks()

    def _get_worker_id(self) -> str:
        """Get pytest-xdist worker ID or default to 'master'."""
        worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")
        if worker_id == "master":
            # Also check for gw0, gw1, etc. format
            for env_var in os.environ:
                if env_var.startswith("PYTEST_XDIST_WORKER_"):
                    worker_id = os.environ[env_var]
                    break
        return worker_id

    def _calculate_worker_offset(self) -> int:
        """Calculate port offset based on worker ID, wrapping within safe limits."""
        if self.worker_id == "master":
            return 0

        # Extract number from worker ID (e.g., gw0 -> 0, gw1 -> 1)
        try:
            if self.worker_id.startswith("gw"):
                worker_num = int(self.worker_id[2:])
                # CRITICAL FIX: gw0 should not conflict with master
                # Start gw workers at offset 200 (master has 0-199)
                worker_num += 1  # gw0 becomes 1, gw1 becomes 2, etc.
            else:
                # Fallback: hash the worker ID
                worker_num = hash(self.worker_id) % 1000
                worker_num += 1  # Ensure non-zero for non-gw workers
        except (ValueError, IndexError):
            # Fallback: hash the worker ID
            worker_num = hash(self.worker_id) % 1000
            worker_num += 1  # Ensure non-zero

        # OPTIMIZED: Support up to 20 workers + master with guaranteed non-overlapping ranges
        # Each worker gets 200 ports, requiring 4200 total ports minimum
        # Research range is now 20000 ports to accommodate concurrent large-scale tests
        max_workers = 20  # 20 gw workers + 1 master = 21 total (within our capacity)
        ports_per_worker = 200

        # Map worker numbers to safe slots 1-20 (avoiding 0 which is master)
        # worker_num is 1-based for gw workers (gw0->1, gw1->2, etc.)
        safe_worker_num = ((worker_num - 1) % max_workers) + 1  # Map to 1-20
        return safe_worker_num * ports_per_worker

    def _cleanup_stale_locks(self) -> None:
        """Remove lock files older than 1 hour, plus any that belong to dead processes."""
        cutoff_time = time.time() - 3600  # 1 hour ago
        cleanup_count = 0

        for lock_file in self.base_dir.glob("port_*.lock"):
            try:
                # Check file modification time first (fastest check)
                if lock_file.stat().st_mtime < cutoff_time:
                    lock_file.unlink()
                    cleanup_count += 1
                    continue

                # For recent files, check if the process is still alive
                try:
                    with lock_file.open("r") as f:
                        content = f.read().strip()
                        parts = content.split(":")
                        if len(parts) >= 3:
                            worker_id, pid_str, timestamp_str = parts[:3]
                            lock_time = float(timestamp_str)
                            pid = int(pid_str)

                            # If lock is older than 5 minutes, consider it stale
                            if time.time() - lock_time > 300:  # 5 minutes
                                lock_file.unlink()
                                cleanup_count += 1
                                continue

                            # Check if process is still alive (Unix-specific)
                            try:
                                os.kill(
                                    pid, 0
                                )  # Send null signal to check if process exists
                                # Process exists, keep the lock
                            except (OSError, ProcessLookupError):
                                # Process doesn't exist, remove stale lock
                                lock_file.unlink()
                                cleanup_count += 1
                except (ValueError, OSError):
                    # Invalid lock file format or read error, remove it
                    try:
                        lock_file.unlink()
                        cleanup_count += 1
                    except OSError:
                        pass

            except (OSError, FileNotFoundError):
                pass  # File might have been removed by another process

        if cleanup_count > 0:
            print(f"Cleaned up {cleanup_count} stale port lock files")

    def _is_port_available(self, port: int) -> bool:
        """Check if a port is available for binding."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(("127.0.0.1", port))
                return True
        except OSError:
            return False

    def _acquire_port_lock(self, port: int) -> bool:
        """Acquire a file-based lock for a specific port."""
        lock_file = self.base_dir / f"port_{port}.lock"

        try:
            # Try to create lock file exclusively
            with lock_file.open("x") as f:
                f.write(f"{self.worker_id}:{os.getpid()}:{time.time()}\n")
            return True
        except FileExistsError:
            # Check if lock is stale
            try:
                with lock_file.open("r") as f:
                    content = f.read().strip()
                    parts = content.split(":")
                    if len(parts) >= 3:
                        worker_id, pid_str, timestamp_str = parts[:3]
                        lock_time = float(timestamp_str)
                        pid = int(pid_str)

                        # Multiple stale lock detection strategies
                        is_stale = False

                        # 1. Age-based: older than 2 minutes
                        if time.time() - lock_time > 120:
                            is_stale = True

                        # 2. Process-based: check if process is still alive
                        else:
                            try:
                                os.kill(pid, 0)  # Check if process exists
                                # Process exists, not stale
                            except (OSError, ProcessLookupError):
                                # Process doesn't exist, lock is stale
                                is_stale = True

                        if is_stale:
                            lock_file.unlink()
                            return self._acquire_port_lock(port)  # Retry

            except (OSError, ValueError, FileNotFoundError):
                # Invalid lock file, try to remove it
                try:
                    lock_file.unlink()
                    return self._acquire_port_lock(port)  # Retry
                except OSError:
                    pass

            return False

    def _release_port_lock(self, port: int) -> None:
        """Release a file-based lock for a specific port."""
        lock_file = self.base_dir / f"port_{port}.lock"
        try:
            lock_file.unlink()
        except FileNotFoundError:
            pass  # Lock was already released

    def allocate_port(
        self, category: str = "testing", preferred_port: int | None = None
    ) -> int:
        """
        Allocate a safe port for testing.

        Args:
            category: Port category (servers, clients, federation, testing)
            preferred_port: Preferred port number (will be checked for availability)

        Returns:
            Allocated port number

        Raises:
            RuntimeError: If no ports are available in the range
        """
        if category not in self.RANGES:
            raise ValueError(
                f"Unknown port category: {category}. Available: {list(self.RANGES.keys())}"
            )

        port_range = self.RANGES[category]

        with self._lock:
            # Calculate worker-specific range with guaranteed non-overlap
            range_size = port_range.end - port_range.start

            # Special handling for research category - much larger ranges per worker
            if category == "research":
                # Research gets 1000 ports per worker to handle concurrent large-scale tests
                research_ports_per_worker = 1000
                max_research_workers = (
                    range_size // research_ports_per_worker
                )  # 20 workers max
                worker_index = (self.worker_offset // 200) % max_research_workers
                start_port = port_range.start + worker_index * research_ports_per_worker
                end_port = min(
                    start_port + research_ports_per_worker - 1, port_range.end
                )
            elif self.worker_offset >= range_size:
                # Distribute workers evenly across the available range for other categories
                workers_in_range = max(
                    1, range_size // 100
                )  # At least 100 ports per worker
                worker_index = (self.worker_offset // 200) % workers_in_range
                ports_per_worker_in_range = range_size // workers_in_range
                start_port = port_range.start + (
                    worker_index * ports_per_worker_in_range
                )
                end_port = min(
                    port_range.start
                    + ((worker_index + 1) * ports_per_worker_in_range)
                    - 1,
                    port_range.end,
                )
            else:
                # Normal case: apply worker offset directly
                start_port = port_range.start + self.worker_offset
                # Each worker gets 199 ports (200-port spacing with 1-port gap)
                end_port = min(start_port + 199, port_range.end)

            # VALIDATION: Ensure we have a valid range
            if start_port >= port_range.end or start_port > end_port:
                raise RuntimeError(
                    f"Invalid port range calculation for worker {self.worker_id}: "
                    f"start_port={start_port}, end_port={end_port}, "
                    f"range={port_range.start}-{port_range.end}, "
                    f"worker_offset={self.worker_offset}"
                )

            # If preferred port is specified and in range, try it first
            if preferred_port and start_port <= preferred_port <= end_port:
                if (
                    preferred_port not in self._allocated_ports
                    and self._is_port_available(preferred_port)
                    and self._acquire_port_lock(preferred_port)
                ):
                    self._allocated_ports.add(preferred_port)
                    return preferred_port

            # Search for available port in range
            for port in range(start_port, end_port + 1):
                if (
                    port not in self._allocated_ports
                    and self._is_port_available(port)
                    and self._acquire_port_lock(port)
                ):
                    self._allocated_ports.add(port)
                    return port

            # Enhanced error message with debugging info
            raise RuntimeError(
                f"No available ports in {category} range ({start_port}-{end_port}) "
                f"for worker {self.worker_id}. "
                f"Original range: {port_range.start}-{port_range.end}, "
                f"Worker offset: {self.worker_offset}"
            )

    def release_port(self, port: int) -> None:
        """Release an allocated port."""
        with self._lock:
            self._allocated_ports.discard(port)
            self._release_port_lock(port)

    @contextmanager
    def port_context(
        self, category: str = "testing", preferred_port: int | None = None
    ) -> Iterator[int]:
        """
        Context manager for automatic port allocation and cleanup.

        Args:
            category: Port category
            preferred_port: Preferred port number

        Yields:
            Allocated port number
        """
        port = self.allocate_port(category, preferred_port)
        try:
            yield port
        finally:
            self.release_port(port)

    def allocate_port_range(self, count: int, category: str = "testing") -> list[int]:
        """
        Allocate multiple consecutive or near-consecutive ports efficiently.

        Args:
            count: Number of ports to allocate
            category: Port category

        Returns:
            List of allocated port numbers
        """
        if count <= 0:
            return []

        if category not in self.RANGES:
            raise ValueError(
                f"Unknown port category: {category}. Available: {list(self.RANGES.keys())}"
            )

        port_range = self.RANGES[category]

        with self._lock:
            # Calculate worker-specific range (reuse logic from allocate_port)
            range_size = port_range.end - port_range.start

            # Special handling for research category - much larger ranges per worker
            if category == "research":
                # Research gets 1000 ports per worker to handle concurrent large-scale tests
                research_ports_per_worker = 1000
                max_research_workers = (
                    range_size // research_ports_per_worker
                )  # 20 workers max
                worker_index = (self.worker_offset // 200) % max_research_workers
                start_port = port_range.start + worker_index * research_ports_per_worker
                end_port = min(
                    start_port + research_ports_per_worker - 1, port_range.end
                )
            elif self.worker_offset >= range_size:
                workers_in_range = max(1, range_size // 100)
                worker_index = (self.worker_offset // 200) % workers_in_range
                ports_per_worker_in_range = range_size // workers_in_range
                start_port = port_range.start + (
                    worker_index * ports_per_worker_in_range
                )
                end_port = min(
                    port_range.start
                    + ((worker_index + 1) * ports_per_worker_in_range)
                    - 1,
                    port_range.end,
                )
            else:
                start_port = port_range.start + self.worker_offset
                end_port = min(start_port + 199, port_range.end)

            # Validate range
            if start_port >= port_range.end or start_port > end_port:
                raise RuntimeError(
                    f"Invalid port range for bulk allocation: {start_port}-{end_port}"
                )

            # Try to find consecutive available ports
            ports: list[PortNumber] = []
            for port in range(start_port, end_port + 1):
                if len(ports) >= count:
                    break

                if (
                    port not in self._allocated_ports
                    and self._is_port_available(port)
                    and self._acquire_port_lock(port)
                ):
                    self._allocated_ports.add(port)
                    ports.append(port)
                elif ports:  # Break consecutive sequence, but might find more later
                    continue

            if len(ports) >= count:
                return ports[:count]

            # Cleanup partial allocation
            for port in ports:
                self._allocated_ports.discard(port)
                self._release_port_lock(port)

            raise RuntimeError(
                f"Could not allocate {count} ports in {category} range "
                f"({start_port}-{end_port}) for worker {self.worker_id}. "
                f"Only found {len(ports)} available ports."
            )

    @contextmanager
    def port_range_context(
        self, count: int, category: str = "testing"
    ) -> Iterator[list[int]]:
        """Context manager for multiple port allocation."""
        ports = self.allocate_port_range(count, category)
        try:
            yield ports
        finally:
            for port in ports:
                self.release_port(port)

    def _get_worker_range_for_category(
        self, name: str, range_info: PortRange
    ) -> dict[str, Any]:
        """Calculate the actual port range for this worker in a given category."""
        range_size = range_info.end - range_info.start

        # Special handling for research category
        if name == "research":
            # Research gets 1000 ports per worker to handle concurrent large-scale tests
            research_ports_per_worker = 1000
            max_research_workers = (
                range_size // research_ports_per_worker
            )  # 20 workers max
            worker_index = (self.worker_offset // 200) % max_research_workers
            start = range_info.start + worker_index * research_ports_per_worker
            end = min(start + research_ports_per_worker - 1, range_info.end)
        elif self.worker_offset >= range_size:
            # Distribute workers evenly across available range
            workers_in_range = max(1, range_size // 100)
            worker_index = (self.worker_offset // 200) % workers_in_range
            ports_per_worker_in_range = range_size // workers_in_range
            start = range_info.start + (worker_index * ports_per_worker_in_range)
            end = min(
                range_info.start + ((worker_index + 1) * ports_per_worker_in_range) - 1,
                range_info.end,
            )
        else:
            # Normal case: apply worker offset directly
            start = range_info.start + self.worker_offset
            end = min(start + 199, range_info.end)

        return {
            "start": start,
            "end": end,
            "description": range_info.description,
            "available_ports": max(0, end - start + 1),
        }

    def get_worker_info(self) -> dict[str, Any]:
        """Get information about the current worker."""
        return {
            "worker_id": self.worker_id,
            "worker_offset": self.worker_offset,
            "allocated_ports": sorted(self._allocated_ports),
            "port_ranges": {
                name: self._get_worker_range_for_category(name, range_info)
                for name, range_info in self.RANGES.items()
            },
        }


# Global port allocator instance
_port_allocator: PortAllocator | None = None


def get_port_allocator() -> PortAllocator:
    """Get the global port allocator instance."""
    global _port_allocator
    if _port_allocator is None:
        _port_allocator = PortAllocator()
    return _port_allocator


# Convenience functions for common use cases
def allocate_port(category: str = "testing", preferred_port: int | None = None) -> int:
    """Allocate a port using the global allocator."""
    return get_port_allocator().allocate_port(category, preferred_port)


def release_port(port: int) -> None:
    """Release a port using the global allocator."""
    get_port_allocator().release_port(port)


@contextmanager
def port_context(
    category: str = "testing", preferred_port: int | None = None
) -> Iterator[int]:
    """Context manager for port allocation using the global allocator."""
    with get_port_allocator().port_context(category, preferred_port) as port:
        yield port


def allocate_port_range(count: int, category: str = "testing") -> list[int]:
    """Allocate multiple ports using the global allocator."""
    return get_port_allocator().allocate_port_range(count, category)


@contextmanager
def port_range_context(count: int, category: str = "testing") -> Iterator[list[int]]:
    """Context manager for multiple port allocation using the global allocator."""
    with get_port_allocator().port_range_context(count, category) as ports:
        yield ports


# Pytest fixtures
@pytest.fixture
def test_port():
    """Pytest fixture for a single test port."""
    with port_context("testing") as port:
        yield port


@pytest.fixture
def server_port():
    """Pytest fixture for a server port."""
    with port_context("servers") as port:
        yield port


@pytest.fixture
def client_port():
    """Pytest fixture for a client port."""
    with port_context("clients") as port:
        yield port


@pytest.fixture
def federation_port():
    """Pytest fixture for a federation port."""
    with port_context("federation") as port:
        yield port


@pytest.fixture
def port_pair():
    """Pytest fixture for a pair of ports (e.g., server + client)."""
    with port_range_context(2, "testing") as ports:
        yield ports


@pytest.fixture
def server_cluster_ports():
    """Pytest fixture for a cluster of server ports (8 ports for comprehensive testing)."""
    with port_range_context(8, "servers") as ports:
        yield ports


@pytest.fixture
def port_allocator():
    """Pytest fixture for the port allocator instance."""
    return get_port_allocator()
