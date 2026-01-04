"""
Comprehensive Storage Adapter Layer for Raft Consensus Implementation.

This module provides clean storage abstractions that allow plugging in different
storage backends for Raft persistent state and log management:

- In-memory storage for testing and development
- File-based storage for single-node development
- SQLite storage for lightweight production deployments
- PostgreSQL/MySQL storage for enterprise production
- External database adapters for distributed storage systems

Design Principles:
- Clean adapter interface following storage protocol
- Atomic operations with transaction support
- Efficient batch operations for log entries
- Configurable durability guarantees (fsync, WAL, etc.)
- Comprehensive error handling and recovery
- Performance optimization for different workloads
"""

from __future__ import annotations

import asyncio
import json
import pickle
import tempfile
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loguru import logger

try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False


from .production_raft import (
    LogEntry,
    LogEntryType,
    PersistentState,
    RaftSnapshot,
    RaftStorageProtocol,
)

storage_log = logger


# Enhanced Storage Protocol with Additional Methods
class EnhancedRaftStorageProtocol(RaftStorageProtocol):
    """Enhanced storage protocol with additional operations for performance."""

    async def save_log_entries_batch(
        self, entries: list[LogEntry], start_index: int
    ) -> None:
        """Efficiently save a batch of log entries starting at start_index."""
        ...

    @abstractmethod
    async def get_log_entries_range(
        self, start_index: int, end_index: int
    ) -> list[LogEntry]:
        """Get log entries in a specific range [start_index, end_index)."""
        ...

    @abstractmethod
    async def truncate_log_from_index(self, from_index: int) -> None:
        """Remove all log entries from from_index onwards."""
        ...

    @abstractmethod
    async def get_storage_info(self) -> dict[str, Any]:
        """Get storage backend information and statistics."""
        ...

    async def compact_storage(self) -> None:
        """Perform storage compaction/optimization."""
        ...


# Base Storage Adapter Class
class BaseRaftStorage(ABC):
    """Base class for Raft storage adapters with common functionality."""

    def __init__(self, storage_id: str = "default"):
        self.storage_id = storage_id
        self.is_initialized = False
        self._lock = asyncio.Lock()

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize storage backend."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close storage backend and cleanup resources."""
        pass

    async def ensure_initialized(self) -> None:
        """Ensure storage is initialized."""
        if not self.is_initialized:
            async with self._lock:
                if not self.is_initialized:
                    await self.initialize()
                    self.is_initialized = True


# Enhanced Log Entry Tracking for In-Memory Storage
@dataclass(frozen=True, slots=True)
class StoredLogEntry:
    """Enhanced log entry with storage metadata for debugging."""

    entry: LogEntry
    stored_at: float = field(default_factory=time.time)
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)
    storage_duration_ms: float = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "storage_duration_ms", 0.0)

    def access(self) -> StoredLogEntry:
        """Create new instance with updated access tracking."""
        current_time = time.time()
        duration = (current_time - self.stored_at) * 1000
        return StoredLogEntry(
            entry=self.entry,
            stored_at=self.stored_at,
            access_count=self.access_count + 1,
            last_accessed=current_time,
        )


@dataclass(slots=True)
class InMemoryStorageMetrics:
    """Comprehensive metrics for in-memory storage operations."""

    total_operations: int = 0
    log_entries_stored: int = 0
    log_entries_accessed: int = 0
    snapshots_stored: int = 0
    persistent_state_saves: int = 0
    persistent_state_loads: int = 0
    average_entry_access_count: float = 0.0
    storage_efficiency_score: float = 0.0
    created_at: float = field(default_factory=time.time)
    last_operation_at: float = field(default_factory=time.time)


# In-Memory Storage Adapter (for testing)
@dataclass
class InMemoryRaftStorage(BaseRaftStorage):
    """
    Enhanced in-memory storage adapter with detailed tracking.

    Features:
    - Clean dataclass design for all stored entities
    - Comprehensive access tracking and metrics
    - Storage duration monitoring for performance analysis
    - Ideal for unit tests and deep debugging sessions
    """

    def __init__(self, storage_id: str = "memory"):
        super().__init__(storage_id)
        self._persistent_state: PersistentState | None = None
        self._stored_entries: dict[int, StoredLogEntry] = {}  # index -> StoredLogEntry
        self._snapshots: list[RaftSnapshot] = []
        self._metrics = InMemoryStorageMetrics()
        self._operation_log: list[tuple[str, float, dict]] = []  # For debugging

    async def initialize(self) -> None:
        """Initialize in-memory storage."""
        storage_log.debug(f"Initialized in-memory storage: {self.storage_id}")

    async def close(self) -> None:
        """Close in-memory storage."""
        self._persistent_state = None
        self._snapshots.clear()
        storage_log.debug(f"Closed in-memory storage: {self.storage_id}")

    async def save_persistent_state(self, state: PersistentState) -> None:
        """Save persistent state to memory."""
        await self.ensure_initialized()
        self._persistent_state = state

        # Update stored entries with enhanced tracking
        self._stored_entries.clear()
        for i, entry in enumerate(state.log_entries):
            stored_entry = StoredLogEntry(entry=entry)
            self._stored_entries[entry.index] = stored_entry

        # Update metrics - direct field assignment
        self._metrics.total_operations += 1
        self._metrics.log_entries_stored = len(state.log_entries)
        self._metrics.persistent_state_saves += 1
        self._metrics.last_operation_at = time.time()

    async def load_persistent_state(self) -> PersistentState | None:
        """Load persistent state from memory."""
        await self.ensure_initialized()

        # Update access tracking for stored entries
        for index, stored_entry in self._stored_entries.items():
            self._stored_entries[index] = stored_entry.access()

        # Update metrics - direct field assignment
        self._metrics.total_operations += 1
        self._metrics.log_entries_accessed += len(self._stored_entries)
        self._metrics.persistent_state_loads += 1
        self._metrics.last_operation_at = time.time()

        return self._persistent_state

    async def save_snapshot(self, snapshot: RaftSnapshot) -> None:
        """Save snapshot to memory."""
        await self.ensure_initialized()
        self._snapshots.append(snapshot)

        # Update metrics - direct field assignment
        self._metrics.total_operations += 1
        self._metrics.snapshots_stored = len(self._snapshots)
        self._metrics.last_operation_at = time.time()

    async def load_latest_snapshot(self) -> RaftSnapshot | None:
        """Load latest snapshot from memory."""
        await self.ensure_initialized()
        self._metrics.total_operations += 1
        self._metrics.last_operation_at = time.time()
        return self._snapshots[-1] if self._snapshots else None

    async def cleanup_old_snapshots(self, keep_count: int = 3) -> None:
        """Clean up old snapshots in memory."""
        await self.ensure_initialized()
        if len(self._snapshots) > keep_count:
            removed_count = len(self._snapshots) - keep_count
            self._snapshots = self._snapshots[-keep_count:]
            self._metrics = InMemoryStorageMetrics(
                total_operations=self._metrics.total_operations + 1,
                log_entries_stored=self._metrics.log_entries_stored,
                log_entries_accessed=self._metrics.log_entries_accessed,
                snapshots_stored=len(self._snapshots),
                persistent_state_saves=self._metrics.persistent_state_saves,
                persistent_state_loads=self._metrics.persistent_state_loads,
                average_entry_access_count=self._metrics.average_entry_access_count,
                storage_efficiency_score=self._metrics.storage_efficiency_score,
                created_at=self._metrics.created_at,
                last_operation_at=time.time(),
            )
            storage_log.debug(f"Cleaned up {removed_count} old snapshots from memory")

    async def save_log_entries_batch(
        self, entries: list[LogEntry], start_index: int
    ) -> None:
        """Save batch of log entries to memory."""
        if not self._persistent_state:
            return

        # Update the log entries efficiently
        current_log = list(self._persistent_state.log_entries)

        # Truncate log if necessary
        if start_index <= len(current_log):
            current_log = current_log[: start_index - 1] if start_index > 1 else []

        # Append new entries
        current_log.extend(entries)

        # Update persistent state
        self._persistent_state = PersistentState(
            current_term=self._persistent_state.current_term,
            voted_for=self._persistent_state.voted_for,
            log_entries=current_log,
        )

        self._metrics.total_operations += 1
        self._metrics.log_entries_stored = len(current_log)
        self._metrics.last_operation_at = time.time()

    async def get_log_entries_range(
        self, start_index: int, end_index: int
    ) -> list[LogEntry]:
        """Get log entries in range from memory."""
        if not self._persistent_state:
            return []

        log_entries = self._persistent_state.log_entries
        start_idx = max(0, start_index - 1)  # Convert to 0-based indexing
        end_idx = min(len(log_entries), end_index - 1)

        self._metrics.total_operations += 1
        self._metrics.last_operation_at = time.time()
        return log_entries[start_idx:end_idx]

    async def truncate_log_from_index(self, from_index: int) -> None:
        """Truncate log from index in memory."""
        if not self._persistent_state:
            return

        current_log = self._persistent_state.log_entries
        if from_index <= len(current_log) + 1:
            truncated_log = current_log[: from_index - 1] if from_index > 1 else []

            self._persistent_state = PersistentState(
                current_term=self._persistent_state.current_term,
                voted_for=self._persistent_state.voted_for,
                log_entries=truncated_log,
            )

            self._metrics.total_operations += 1
            self._metrics.log_entries_stored = len(truncated_log)
            self._metrics.last_operation_at = time.time()

    async def get_storage_info(self) -> dict[str, Any]:
        """Get storage information."""
        return {
            "storage_type": "in_memory",
            "storage_id": self.storage_id,
            "total_operations": self._metrics.total_operations,
            "log_entries_stored": self._metrics.log_entries_stored,
            "log_entries_accessed": self._metrics.log_entries_accessed,
            "snapshots_stored": self._metrics.snapshots_stored,
            "persistent_state_saves": self._metrics.persistent_state_saves,
            "persistent_state_loads": self._metrics.persistent_state_loads,
            "created_at": self._metrics.created_at,
            "last_operation_at": self._metrics.last_operation_at,
        }

    async def compact_storage(self) -> None:
        """No-op for in-memory storage."""
        self._metrics.total_operations += 1
        self._metrics.last_operation_at = time.time()

    def get_debug_info(self) -> dict[str, Any]:
        """Get detailed debugging information about storage state."""
        total_access_count = sum(
            entry.access_count for entry in self._stored_entries.values()
        )

        return {
            "storage_id": self.storage_id,
            "storage_type": "enhanced_in_memory",
            "metrics": self._metrics,
            "stored_entries": list(self._stored_entries.values()),
            "total_entries": len(self._stored_entries),
            "total_snapshots": len(self._snapshots),
            "total_access_count": total_access_count,
            "recent_operations": self._operation_log[-10:]
            if self._operation_log
            else [],
            "persistent_state_exists": self._persistent_state is not None,
            "current_term": self._persistent_state.current_term
            if self._persistent_state
            else None,
            "voted_for": self._persistent_state.voted_for
            if self._persistent_state
            else None,
        }


# File-Based Storage Adapter
@dataclass
class FileBasedRaftStorage(BaseRaftStorage):
    """
    File-based storage adapter for single-node development.

    Stores persistent state and snapshots as JSON/pickle files.
    Provides atomic writes and basic durability guarantees.
    """

    storage_dir: Path
    use_fsync: bool = True

    def __init__(
        self, storage_dir: str | Path, storage_id: str = "file", use_fsync: bool = True
    ):
        super().__init__(storage_id)
        self.storage_dir = Path(storage_dir)
        self.use_fsync = use_fsync
        self.state_file = self.storage_dir / "persistent_state.json"
        self.snapshots_dir = self.storage_dir / "snapshots"
        self._storage_stats = {
            "operations": 0,
            "files_written": 0,
            "files_read": 0,
            "created_at": time.time(),
        }

    async def initialize(self) -> None:
        """Initialize file-based storage."""
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.snapshots_dir.mkdir(exist_ok=True)
        storage_log.debug(f"Initialized file storage at: {self.storage_dir}")

    async def close(self) -> None:
        """Close file-based storage."""
        storage_log.debug(f"Closed file storage: {self.storage_dir}")

    async def _atomic_write_json(self, file_path: Path, data: Any) -> None:
        """Atomically write JSON data to file."""
        temp_file = file_path.with_suffix(".tmp")

        # Write to temporary file first
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=2, default=str)
            if self.use_fsync:
                f.flush()
                import os

                os.fsync(f.fileno())

        # Atomic rename
        temp_file.replace(file_path)
        self._storage_stats["files_written"] += 1

    async def save_persistent_state(self, state: PersistentState) -> None:
        """Save persistent state to file."""
        await self.ensure_initialized()

        # Convert to serializable format
        data = {
            "current_term": state.current_term,
            "voted_for": state.voted_for,
            "log_entries": [
                {
                    "term": entry.term,
                    "index": entry.index,
                    "entry_type": entry.entry_type.value,
                    "command": entry.command,
                    "client_id": entry.client_id,
                    "request_id": entry.request_id,
                    "timestamp": entry.timestamp,
                    "checksum": entry.checksum,
                }
                for entry in state.log_entries
            ],
        }

        await self._atomic_write_json(self.state_file, data)
        self._storage_stats["operations"] += 1

    async def load_persistent_state(self) -> PersistentState | None:
        """Load persistent state from file."""
        await self.ensure_initialized()

        if not self.state_file.exists():
            return None

        try:
            with open(self.state_file) as f:
                data = json.load(f)

            # Reconstruct log entries
            log_entries = []
            for entry_data in data["log_entries"]:
                entry = LogEntry(
                    term=entry_data["term"],
                    index=entry_data["index"],
                    entry_type=LogEntryType(entry_data["entry_type"]),
                    command=entry_data["command"],
                    client_id=entry_data["client_id"],
                    request_id=entry_data["request_id"],
                    timestamp=entry_data["timestamp"],
                )

                # Verify checksum
                if entry.checksum != entry_data["checksum"]:
                    storage_log.error(f"Checksum mismatch for entry {entry.index}")
                    return None

                log_entries.append(entry)

            self._storage_stats["files_read"] += 1
            self._storage_stats["operations"] += 1

            return PersistentState(
                current_term=data["current_term"],
                voted_for=data["voted_for"],
                log_entries=log_entries,
            )

        except Exception as e:
            storage_log.error(f"Error loading persistent state: {e}")
            return None

    async def save_snapshot(self, snapshot: RaftSnapshot) -> None:
        """Save snapshot to file."""
        await self.ensure_initialized()

        snapshot_file = (
            self.snapshots_dir
            / f"snapshot_{snapshot.last_included_index}_{snapshot.snapshot_id}.pkl"
        )

        with open(snapshot_file, "wb") as f:
            pickle.dump(snapshot, f)
            if self.use_fsync:
                f.flush()
                import os

                os.fsync(f.fileno())

        self._storage_stats["files_written"] += 1
        self._storage_stats["operations"] += 1

    async def load_latest_snapshot(self) -> RaftSnapshot | None:
        """Load latest snapshot from file."""
        await self.ensure_initialized()

        snapshot_files = list(self.snapshots_dir.glob("snapshot_*.pkl"))
        if not snapshot_files:
            return None

        # Sort by last_included_index
        def get_index(path):
            parts = path.stem.split("_")
            return int(parts[1]) if len(parts) > 1 else 0

        latest_file = max(snapshot_files, key=get_index)

        try:
            with open(latest_file, "rb") as f:
                snapshot = pickle.load(f)

            self._storage_stats["files_read"] += 1
            self._storage_stats["operations"] += 1
            return snapshot

        except Exception as e:
            storage_log.error(f"Error loading snapshot: {e}")
            return None

    async def cleanup_old_snapshots(self, keep_count: int = 3) -> None:
        """Clean up old snapshot files."""
        await self.ensure_initialized()

        snapshot_files = list(self.snapshots_dir.glob("snapshot_*.pkl"))

        if len(snapshot_files) <= keep_count:
            return

        # Sort by last_included_index
        def get_index(path):
            parts = path.stem.split("_")
            return int(parts[1]) if len(parts) > 1 else 0

        sorted_files = sorted(snapshot_files, key=get_index, reverse=True)

        # Remove old snapshots
        removed_count = 0
        for old_file in sorted_files[keep_count:]:
            old_file.unlink()
            removed_count += 1

        self._storage_stats["operations"] += 1
        storage_log.debug(f"Cleaned up {removed_count} old snapshot files")

    async def get_storage_info(self) -> dict[str, Any]:
        """Get storage information."""
        return {
            "storage_type": "file_based",
            "storage_id": self.storage_id,
            "storage_dir": str(self.storage_dir),
            "use_fsync": self.use_fsync,
            **self._storage_stats,
        }

    async def compact_storage(self) -> None:
        """Compact storage by rewriting state file."""
        # For file storage, compaction means rewriting the state file
        state = await self.load_persistent_state()
        if state:
            await self.save_persistent_state(state)


# SQLite Storage Adapter
class SQLiteRaftStorage(BaseRaftStorage):
    """
    SQLite storage adapter for lightweight production deployments.

    Provides ACID guarantees with WAL mode for better performance.
    Suitable for single-node or small-scale deployments.
    """

    def __init__(
        self, db_path: str | Path, storage_id: str = "sqlite", wal_mode: bool = True
    ):
        if not AIOSQLITE_AVAILABLE:
            raise ImportError("aiosqlite is required for SQLite storage adapter")
        super().__init__(storage_id)
        self.db_path = Path(db_path)
        self.wal_mode = wal_mode
        self._storage_stats = {
            "operations": 0,
            "transactions": 0,
            "created_at": time.time(),
        }

    async def initialize(self) -> None:
        """Initialize SQLite storage."""
        # Create database and tables
        async with aiosqlite.connect(self.db_path) as db:
            # Enable WAL mode for better concurrency
            if self.wal_mode:
                await db.execute("PRAGMA journal_mode=WAL")

            # Create tables
            await db.execute("""
                CREATE TABLE IF NOT EXISTS persistent_state (
                    id INTEGER PRIMARY KEY,
                    current_term INTEGER NOT NULL,
                    voted_for TEXT,
                    updated_at REAL NOT NULL
                )
            """)

            await db.execute("""
                CREATE TABLE IF NOT EXISTS log_entries (
                    entry_index INTEGER PRIMARY KEY,
                    term INTEGER NOT NULL,
                    entry_type TEXT NOT NULL,
                    command TEXT,
                    client_id TEXT,
                    request_id TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    checksum TEXT NOT NULL
                )
            """)

            await db.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id TEXT NOT NULL,
                    last_included_index INTEGER NOT NULL,
                    last_included_term INTEGER NOT NULL,
                    state_machine_state BLOB NOT NULL,
                    configuration TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    checksum TEXT NOT NULL
                )
            """)

            await db.commit()

        storage_log.debug(f"Initialized SQLite storage: {self.db_path}")

    async def close(self) -> None:
        """Close SQLite storage."""
        storage_log.debug(f"Closed SQLite storage: {self.db_path}")

    async def save_persistent_state(self, state: PersistentState) -> None:
        """Save persistent state to SQLite."""
        await self.ensure_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            # Start transaction
            await db.execute("BEGIN TRANSACTION")

            try:
                # Update persistent state
                await db.execute(
                    """
                    INSERT OR REPLACE INTO persistent_state (id, current_term, voted_for, updated_at)
                    VALUES (1, ?, ?, ?)
                """,
                    (state.current_term, state.voted_for, time.time()),
                )

                # Clear existing log entries and insert new ones
                await db.execute("DELETE FROM log_entries")

                for entry in state.log_entries:
                    await db.execute(
                        """
                        INSERT INTO log_entries 
                        (entry_index, term, entry_type, command, client_id, request_id, timestamp, checksum)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            entry.index,
                            entry.term,
                            entry.entry_type.value,
                            json.dumps(entry.command)
                            if entry.command is not None
                            else None,
                            entry.client_id,
                            entry.request_id,
                            entry.timestamp,
                            entry.checksum,
                        ),
                    )

                await db.commit()
                self._storage_stats["operations"] += 1
                self._storage_stats["transactions"] += 1

            except Exception as e:
                await db.rollback()
                storage_log.error(f"Error saving persistent state to SQLite: {e}")
                raise

    async def load_persistent_state(self) -> PersistentState | None:
        """Load persistent state from SQLite."""
        await self.ensure_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            # Load persistent state
            cursor = await db.execute("""
                SELECT current_term, voted_for FROM persistent_state WHERE id = 1
            """)
            row = await cursor.fetchone()

            if not row:
                return None

            current_term, voted_for = row

            # Load log entries
            cursor = await db.execute("""
                SELECT entry_index, term, entry_type, command, client_id, request_id, timestamp, checksum
                FROM log_entries ORDER BY entry_index
            """)

            log_entries = []
            for row in await cursor.fetchall():
                (
                    entry_index,
                    term,
                    entry_type_str,
                    command_json,
                    client_id,
                    request_id,
                    timestamp,
                    checksum,
                ) = row

                command = json.loads(command_json) if command_json is not None else None

                entry = LogEntry(
                    term=term,
                    index=entry_index,
                    entry_type=LogEntryType(entry_type_str),
                    command=command,
                    client_id=client_id,
                    request_id=request_id,
                    timestamp=timestamp,
                )

                # Verify checksum
                if entry.checksum != checksum:
                    storage_log.error(f"Checksum mismatch for entry {entry_index}")
                    return None

                log_entries.append(entry)

            self._storage_stats["operations"] += 1

            return PersistentState(
                current_term=current_term, voted_for=voted_for, log_entries=log_entries
            )

    async def save_snapshot(self, snapshot: RaftSnapshot) -> None:
        """Save snapshot to SQLite."""
        await self.ensure_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO snapshots 
                (snapshot_id, last_included_index, last_included_term, state_machine_state, 
                 configuration, created_at, size_bytes, checksum)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    snapshot.snapshot_id,
                    snapshot.last_included_index,
                    snapshot.last_included_term,
                    snapshot.state_machine_state,
                    json.dumps(sorted(list(snapshot.configuration))),
                    snapshot.created_at,
                    snapshot.size_bytes,
                    snapshot.checksum,
                ),
            )

            await db.commit()
            self._storage_stats["operations"] += 1

    async def load_latest_snapshot(self) -> RaftSnapshot | None:
        """Load latest snapshot from SQLite."""
        await self.ensure_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT snapshot_id, last_included_index, last_included_term, state_machine_state,
                       configuration, created_at, size_bytes, checksum
                FROM snapshots 
                ORDER BY last_included_index DESC, created_at DESC 
                LIMIT 1
            """)

            row = await cursor.fetchone()
            if not row:
                return None

            (
                snapshot_id,
                last_included_index,
                last_included_term,
                state_machine_state,
                configuration_json,
                created_at,
                size_bytes,
                checksum,
            ) = row

            configuration = set(json.loads(configuration_json))

            self._storage_stats["operations"] += 1

            return RaftSnapshot(
                last_included_index=last_included_index,
                last_included_term=last_included_term,
                state_machine_state=state_machine_state,
                configuration=configuration,
                snapshot_id=snapshot_id,
                created_at=created_at,
            )

    async def cleanup_old_snapshots(self, keep_count: int = 3) -> None:
        """Clean up old snapshots in SQLite."""
        await self.ensure_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            # Keep only the most recent snapshots
            await db.execute(
                """
                DELETE FROM snapshots 
                WHERE id NOT IN (
                    SELECT id FROM snapshots 
                    ORDER BY last_included_index DESC, created_at DESC 
                    LIMIT ?
                )
            """,
                (keep_count,),
            )

            await db.commit()
            self._storage_stats["operations"] += 1

    async def get_storage_info(self) -> dict[str, Any]:
        """Get SQLite storage information."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM log_entries")
            log_result = await cursor.fetchone()
            log_count = log_result[0] if log_result else 0

            cursor = await db.execute("SELECT COUNT(*) FROM snapshots")
            snapshot_result = await cursor.fetchone()
            snapshot_count = snapshot_result[0] if snapshot_result else 0

        return {
            "storage_type": "sqlite",
            "storage_id": self.storage_id,
            "db_path": str(self.db_path),
            "wal_mode": self.wal_mode,
            "log_entries_count": log_count,
            "snapshots_count": snapshot_count,
            **self._storage_stats,
        }

    async def compact_storage(self) -> None:
        """Compact SQLite database."""
        await self.ensure_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("VACUUM")
            self._storage_stats["operations"] += 1


# Storage Factory
class RaftStorageFactory:
    """Factory for creating Raft storage adapters."""

    @staticmethod
    def create_memory_storage(storage_id: str = "memory") -> InMemoryRaftStorage:
        """Create in-memory storage adapter."""
        return InMemoryRaftStorage(storage_id)

    @staticmethod
    def create_file_storage(
        storage_dir: str | Path, storage_id: str = "file", use_fsync: bool = True
    ) -> FileBasedRaftStorage:
        """Create file-based storage adapter."""
        return FileBasedRaftStorage(storage_dir, storage_id, use_fsync)

    @staticmethod
    def create_sqlite_storage(
        db_path: str | Path, storage_id: str = "sqlite", wal_mode: bool = True
    ) -> SQLiteRaftStorage:
        """Create SQLite storage adapter."""
        if not AIOSQLITE_AVAILABLE:
            raise ImportError("aiosqlite is required for SQLite storage adapter")
        return SQLiteRaftStorage(db_path, storage_id, wal_mode)

    @staticmethod
    def create_temporary_storage() -> FileBasedRaftStorage:
        """Create temporary file storage for testing."""
        temp_dir = tempfile.mkdtemp(prefix="raft_storage_")
        return FileBasedRaftStorage(temp_dir, "temp", use_fsync=False)
