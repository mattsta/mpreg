"""Thread-safe port allocation for demos and tooling."""

from __future__ import annotations

import os
import socket
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from mpreg.datastructures.type_aliases import PortNumber


@dataclass(frozen=True, slots=True)
class PortRange:
    """Port range configuration for different usage categories."""

    start: int
    end: int
    description: str = ""


class PortAllocator:
    """File-lock-based port allocator with worker-aware ranges."""

    RANGES = {
        "servers": PortRange(10000, 16000, "Main server ports (6000 ports)"),
        "clients": PortRange(16000, 20000, "Client connection ports (4000 ports)"),
        "federation": PortRange(20000, 24000, "Federation ports (4000 ports)"),
        "testing": PortRange(24000, 30000, "General testing ports (6000 ports)"),
        "research": PortRange(30000, 50000, "Research ports (20000 ports)"),
        "monitoring": PortRange(50000, 55000, "Monitoring endpoints (5000 ports)"),
    }

    def __init__(self, base_dir: str | None = None) -> None:
        if base_dir is None:
            base_dir = os.environ.get("PYTEST_CURRENT_TEST_DIR", tempfile.gettempdir())

        self.base_dir = Path(base_dir) / "mpreg_port_locks"
        self.base_dir.mkdir(exist_ok=True, parents=True)

        self.worker_id = self._get_worker_id()
        self.worker_offset = self._calculate_worker_offset()

        self._lock = threading.Lock()
        self._allocated_ports: set[int] = set()

        self._cleanup_stale_locks()

    def _get_worker_id(self) -> str:
        worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")
        if worker_id == "master":
            for env_var in os.environ:
                if env_var.startswith("PYTEST_XDIST_WORKER_"):
                    worker_id = os.environ[env_var]
                    break
        return worker_id

    def _calculate_worker_offset(self) -> int:
        if self.worker_id == "master":
            return 0

        try:
            if self.worker_id.startswith("gw"):
                worker_num = int(self.worker_id[2:]) + 1
            else:
                worker_num = hash(self.worker_id) % 1000
                worker_num += 1
        except ValueError, IndexError:
            worker_num = hash(self.worker_id) % 1000
            worker_num += 1

        max_workers = 20
        ports_per_worker = 200
        safe_worker_num = ((worker_num - 1) % max_workers) + 1
        return safe_worker_num * ports_per_worker

    def _cleanup_stale_locks(self) -> None:
        now = time.time()
        cutoff_time = now - 3600
        grace_period = 5.0
        cleanup_count = 0

        for lock_file in self.base_dir.glob("port_*.lock"):
            try:
                stat = lock_file.stat()
                file_age = now - stat.st_mtime
                if stat.st_mtime < cutoff_time:
                    lock_file.unlink()
                    cleanup_count += 1
                    continue

                try:
                    with lock_file.open("r") as handle:
                        content = handle.read().strip()
                        parts = content.split(":")
                        if len(parts) >= 3:
                            _, pid_str, timestamp_str = parts[:3]
                            lock_time = float(timestamp_str)
                            pid = int(pid_str)

                            try:
                                os.kill(pid, 0)
                                is_stale = False
                            except OSError, ProcessLookupError:
                                is_stale = True

                            if is_stale:
                                lock_file.unlink()
                                cleanup_count += 1
                except ValueError, OSError:
                    if file_age > grace_period:
                        try:
                            lock_file.unlink()
                            cleanup_count += 1
                        except OSError:
                            pass
            except OSError, FileNotFoundError:
                pass

        if cleanup_count:
            port_log.debug("Cleaned up {} stale port lock files", cleanup_count)

    def _is_port_available(self, port: int) -> bool:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.bind(("127.0.0.1", port))
                sock.listen(1)
                return True
        except OSError:
            return False

    def _acquire_port_lock(self, port: int) -> bool:
        lock_file = self.base_dir / f"port_{port}.lock"
        try:
            with lock_file.open("x") as handle:
                handle.write(f"{self.worker_id}:{os.getpid()}:{time.time()}\n")
            return True
        except FileExistsError:
            try:
                with lock_file.open("r") as handle:
                    content = handle.read().strip()
                    parts = content.split(":")
                    if len(parts) >= 3:
                        _, pid_str, timestamp_str = parts[:3]
                        lock_time = float(timestamp_str)
                        pid = int(pid_str)

                        try:
                            os.kill(pid, 0)
                            is_stale = False
                        except OSError, ProcessLookupError:
                            is_stale = True

                        if is_stale:
                            lock_file.unlink()
                            return self._acquire_port_lock(port)
            except OSError, ValueError, FileNotFoundError:
                try:
                    file_age = time.time() - lock_file.stat().st_mtime
                    if file_age > 5.0:
                        lock_file.unlink()
                        return self._acquire_port_lock(port)
                except OSError:
                    pass
            return False

    def _release_port_lock(self, port: int) -> None:
        lock_file = self.base_dir / f"port_{port}.lock"
        with suppress(FileNotFoundError):
            lock_file.unlink()

    def _claim_port(self, port: int) -> bool:
        if not self._acquire_port_lock(port):
            return False
        if not self._is_port_available(port):
            self._release_port_lock(port)
            return False
        self._allocated_ports.add(port)
        return True

    def _calculate_worker_range(self, category: str) -> tuple[int, int]:
        port_range = self.RANGES[category]
        range_size = port_range.end - port_range.start

        if category == "research":
            ports_per_worker = 1000
            max_workers = range_size // ports_per_worker
            worker_index = (self.worker_offset // 200) % max_workers
            start_port = port_range.start + worker_index * ports_per_worker
            end_port = min(start_port + ports_per_worker - 1, port_range.end)
        elif self.worker_offset >= range_size:
            workers_in_range = max(1, range_size // 100)
            worker_index = (self.worker_offset // 200) % workers_in_range
            ports_per_worker = range_size // workers_in_range
            start_port = port_range.start + (worker_index * ports_per_worker)
            end_port = min(
                port_range.start + ((worker_index + 1) * ports_per_worker) - 1,
                port_range.end,
            )
        else:
            start_port = port_range.start + self.worker_offset
            end_port = min(start_port + 199, port_range.end)

        if start_port >= port_range.end or start_port > end_port:
            raise RuntimeError(
                f"Invalid port range for worker {self.worker_id}: "
                f"{start_port}-{end_port}"
            )

        return start_port, end_port

    def _get_worker_range_for_category(self, name: str, range_info: PortRange) -> dict:
        """Return the worker-specific port range details for a category."""
        start_port, end_port = self._calculate_worker_range(name)
        return {
            "start": start_port,
            "end": end_port,
            "description": range_info.description,
            "available_ports": max(0, end_port - start_port + 1),
        }

    def get_worker_info(self) -> dict:
        """Return worker metadata and allocated port ranges."""
        return {
            "worker_id": self.worker_id,
            "worker_offset": self.worker_offset,
            "allocated_ports": sorted(self._allocated_ports),
            "port_ranges": {
                name: self._get_worker_range_for_category(name, range_info)
                for name, range_info in self.RANGES.items()
            },
        }

    def allocate_port(
        self, category: str = "testing", preferred_port: int | None = None
    ) -> int:
        if category not in self.RANGES:
            raise ValueError(
                f"Unknown port category: {category}. Available: {list(self.RANGES.keys())}"
            )

        with self._lock:
            start_port, end_port = self._calculate_worker_range(category)

            if preferred_port and start_port <= preferred_port <= end_port:
                if preferred_port not in self._allocated_ports and self._claim_port(
                    preferred_port
                ):
                    return preferred_port

            for port in range(start_port, end_port + 1):
                if port not in self._allocated_ports and self._claim_port(port):
                    return port

            raise RuntimeError(
                f"No available ports in {category} range ({start_port}-{end_port}) "
                f"for worker {self.worker_id}."
            )

    def release_port(self, port: int) -> None:
        with self._lock:
            self._allocated_ports.discard(port)
            self._release_port_lock(port)

    @contextmanager
    def port_context(
        self, category: str = "testing", preferred_port: int | None = None
    ) -> Iterator[int]:
        port = self.allocate_port(category, preferred_port)
        try:
            yield port
        finally:
            self.release_port(port)

    def allocate_port_range(self, count: int, category: str = "testing") -> list[int]:
        if count <= 0:
            return []
        if category not in self.RANGES:
            raise ValueError(
                f"Unknown port category: {category}. Available: {list(self.RANGES.keys())}"
            )

        with self._lock:
            start_port, end_port = self._calculate_worker_range(category)
            ports: list[PortNumber] = []
            for port in range(start_port, end_port + 1):
                if len(ports) >= count:
                    break
                if port in self._allocated_ports:
                    continue
                if self._claim_port(port):
                    ports.append(port)

            if len(ports) >= count:
                return ports[:count]

            for port in ports:
                self._allocated_ports.discard(port)
                self._release_port_lock(port)

            raise RuntimeError(
                f"Could not allocate {count} ports in {category} range "
                f"({start_port}-{end_port}) for worker {self.worker_id}."
            )

    @contextmanager
    def port_range_context(
        self, count: int, category: str = "testing"
    ) -> Iterator[list[int]]:
        ports = self.allocate_port_range(count, category)
        try:
            yield ports
        finally:
            for port in ports:
                self.release_port(port)


port_log = logger

_port_allocator: PortAllocator | None = None


def get_port_allocator() -> PortAllocator:
    global _port_allocator
    if _port_allocator is None:
        _port_allocator = PortAllocator()
    return _port_allocator


def allocate_port(category: str = "testing", preferred_port: int | None = None) -> int:
    try:
        return get_port_allocator().allocate_port(category, preferred_port)
    except RuntimeError:
        if category == "testing":
            return get_port_allocator().allocate_port("research", preferred_port)
        raise


def release_port(port: int) -> None:
    get_port_allocator().release_port(port)


@contextmanager
def port_context(
    category: str = "testing", preferred_port: int | None = None
) -> Iterator[int]:
    try:
        with get_port_allocator().port_context(category, preferred_port) as port:
            yield port
    except RuntimeError:
        if category == "testing":
            with get_port_allocator().port_context("research", preferred_port) as port:
                yield port
        else:
            raise


def allocate_port_range(count: int, category: str = "testing") -> list[int]:
    try:
        return get_port_allocator().allocate_port_range(count, category)
    except RuntimeError:
        if category == "testing":
            return get_port_allocator().allocate_port_range(count, "research")
        raise


@contextmanager
def port_range_context(count: int, category: str = "testing") -> Iterator[list[int]]:
    try:
        with get_port_allocator().port_range_context(count, category) as ports:
            yield ports
    except RuntimeError:
        if category == "testing":
            with get_port_allocator().port_range_context(count, "research") as ports:
                yield ports
        else:
            raise
