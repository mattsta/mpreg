"""Multi-node shared management audit store (G-Set + watermarks).

Opt-in cluster visibility for admin mutations. Default path remains the
process-local :class:`~mpreg.server_pkg.mgmt_mutations.MgmtAuditLog`.
"""

from __future__ import annotations

from mpreg.server_pkg.shared_audit.models import (
    SharedAuditRecord,
    entry_sort_key,
    merge_records,
    mint_entry_id,
    record_from_mgmt_entry,
    stable_canonical_json,
)
from mpreg.server_pkg.shared_audit.store import SharedAuditStore, Watermark

__all__ = [
    "SharedAuditRecord",
    "SharedAuditStore",
    "Watermark",
    "entry_sort_key",
    "merge_records",
    "mint_entry_id",
    "record_from_mgmt_entry",
    "stable_canonical_json",
]
