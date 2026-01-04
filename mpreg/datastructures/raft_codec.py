"""
Shared serialization helpers for ProductionRaft RPC payloads.

This module centralizes payload encoding/decoding so transports can reuse
the same wire representation regardless of the underlying network.
"""

from __future__ import annotations

import base64
from dataclasses import asdict
from typing import Any

from .production_raft import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    LogEntry,
    LogEntryType,
    RequestVoteRequest,
    RequestVoteResponse,
)


def encode_bytes(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def decode_bytes(data: str) -> bytes:
    return base64.b64decode(data.encode("ascii"))


def serialize_log_entry(entry: LogEntry) -> dict[str, Any]:
    return {
        "term": entry.term,
        "index": entry.index,
        "entry_type": entry.entry_type.value,
        "command": entry.command,
        "client_id": entry.client_id,
        "request_id": entry.request_id,
        "timestamp": entry.timestamp,
    }


def deserialize_log_entry(data: dict[str, Any]) -> LogEntry:
    return LogEntry(
        term=int(data["term"]),
        index=int(data["index"]),
        entry_type=LogEntryType(data["entry_type"]),
        command=data.get("command"),
        client_id=data.get("client_id", ""),
        request_id=data.get("request_id", ""),
        timestamp=float(data.get("timestamp", 0.0)),
    )


def serialize_request_vote(request: RequestVoteRequest) -> dict[str, Any]:
    return asdict(request)


def deserialize_request_vote(data: dict[str, Any]) -> RequestVoteRequest:
    return RequestVoteRequest(
        term=int(data["term"]),
        candidate_id=str(data["candidate_id"]),
        last_log_index=int(data["last_log_index"]),
        last_log_term=int(data["last_log_term"]),
    )


def serialize_request_vote_response(response: RequestVoteResponse) -> dict[str, Any]:
    return asdict(response)


def deserialize_request_vote_response(data: dict[str, Any]) -> RequestVoteResponse:
    return RequestVoteResponse(
        term=int(data["term"]),
        vote_granted=bool(data["vote_granted"]),
        voter_id=str(data["voter_id"]),
    )


def serialize_append_entries(request: AppendEntriesRequest) -> dict[str, Any]:
    return {
        "term": request.term,
        "leader_id": request.leader_id,
        "prev_log_index": request.prev_log_index,
        "prev_log_term": request.prev_log_term,
        "entries": [serialize_log_entry(entry) for entry in request.entries],
        "leader_commit": request.leader_commit,
    }


def deserialize_append_entries(data: dict[str, Any]) -> AppendEntriesRequest:
    return AppendEntriesRequest(
        term=int(data["term"]),
        leader_id=str(data["leader_id"]),
        prev_log_index=int(data["prev_log_index"]),
        prev_log_term=int(data["prev_log_term"]),
        entries=[deserialize_log_entry(entry) for entry in data.get("entries", [])],
        leader_commit=int(data["leader_commit"]),
    )


def serialize_append_entries_response(
    response: AppendEntriesResponse,
) -> dict[str, Any]:
    return asdict(response)


def deserialize_append_entries_response(
    data: dict[str, Any],
) -> AppendEntriesResponse:
    return AppendEntriesResponse(
        term=int(data["term"]),
        success=bool(data["success"]),
        follower_id=str(data["follower_id"]),
        match_index=int(data.get("match_index", 0)),
        conflict_index=int(data.get("conflict_index", -1)),
        conflict_term=int(data.get("conflict_term", -1)),
    )


def serialize_install_snapshot(request: InstallSnapshotRequest) -> dict[str, Any]:
    return {
        "term": request.term,
        "leader_id": request.leader_id,
        "last_included_index": request.last_included_index,
        "last_included_term": request.last_included_term,
        "data": encode_bytes(request.data),
        "done": request.done,
        "offset": request.offset,
    }


def deserialize_install_snapshot(data: dict[str, Any]) -> InstallSnapshotRequest:
    return InstallSnapshotRequest(
        term=int(data["term"]),
        leader_id=str(data["leader_id"]),
        last_included_index=int(data["last_included_index"]),
        last_included_term=int(data["last_included_term"]),
        data=decode_bytes(data.get("data", "")),
        done=bool(data["done"]),
        offset=int(data.get("offset", 0)),
    )


def serialize_install_snapshot_response(
    response: InstallSnapshotResponse,
) -> dict[str, Any]:
    return asdict(response)


def deserialize_install_snapshot_response(
    data: dict[str, Any],
) -> InstallSnapshotResponse:
    return InstallSnapshotResponse(
        term=int(data["term"]),
        follower_id=str(data["follower_id"]),
    )
