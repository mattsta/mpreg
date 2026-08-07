from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path


class PersistenceMode(StrEnum):
    """Supported persistence backends."""

    MEMORY = "memory"
    SQLITE = "sqlite"


@dataclass(slots=True)
class PersistenceConfig:
    """Configuration for the unified persistence layer."""

    mode: PersistenceMode = PersistenceMode.MEMORY
    data_dir: Path = field(default_factory=lambda: Path("/tmp/mpreg_data"))
    sqlite_filename: str = "mpreg.sqlite"
    sqlite_wal: bool = True
    sqlite_synchronous: str = "NORMAL"
    sqlite_foreign_keys: bool = True

    def sqlite_path(self) -> Path:
        """Resolve the sqlite database path."""
        return self.data_dir / self.sqlite_filename
