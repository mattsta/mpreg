from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Protocol

from mpreg.core.message_queue import (
    DeliveryGuarantee,
    InFlightMessage,
    QueueConfiguration,
    QueuedMessage,
    QueueMessageStatus,
    QueueType,
)
from mpreg.core.serialization import JsonSerializer
from mpreg.datastructures.message_structures import MessageId


@dataclass(frozen=True, slots=True)
class QueuePersistenceState:
    """Persisted queue state snapshot."""

    pending: deque[QueuedMessage] = field(default_factory=deque)
    in_flight: dict[str, InFlightMessage] = field(default_factory=dict)
    dead_letter: deque[QueuedMessage] = field(default_factory=deque)
    fingerprints: list[tuple[float, str]] = field(default_factory=list)
    next_sequence: int = 0
    config: QueueConfiguration | None = None


class QueueStore(Protocol):
    """Persistence interface for queue state."""

    async def load_state(self) -> QueuePersistenceState: ...

    async def save_config(self, config: QueueConfiguration) -> None: ...

    async def enqueue(self, message: QueuedMessage) -> int: ...

    async def mark_in_flight(self, in_flight: InFlightMessage) -> None: ...

    async def ack(self, message_id: str) -> None: ...

    async def move_to_dead_letter(self, message: QueuedMessage) -> int: ...

    async def requeue(self, message: QueuedMessage) -> int: ...

    async def add_fingerprint(self, timestamp: float, fingerprint: str) -> None: ...


def _message_id_to_dict(message_id: MessageId) -> dict[str, Any]:
    return {
        "id": message_id.id,
        "created_at": message_id.created_at,
        "source_node": message_id.source_node,
    }


def _message_id_from_dict(payload: dict[str, Any]) -> MessageId:
    return MessageId(
        id=str(payload.get("id", "")),
        created_at=float(payload.get("created_at", 0.0)),
        source_node=str(payload.get("source_node", "")),
    )


def _queued_message_to_dict(message: QueuedMessage) -> dict[str, Any]:
    return {
        "id": _message_id_to_dict(message.id),
        "topic": message.topic,
        "payload": message.payload,
        "delivery_guarantee": message.delivery_guarantee.value,
        "priority": message.priority,
        "delay_seconds": message.delay_seconds,
        "visibility_timeout_seconds": message.visibility_timeout_seconds,
        "max_retries": message.max_retries,
        "acknowledgment_timeout_seconds": message.acknowledgment_timeout_seconds,
        "required_acknowledgments": message.required_acknowledgments,
        "created_at": message.created_at,
        "headers": dict(message.headers),
        "delivery_attempt": message._delivery_attempt,
        "fingerprint": message._fingerprint,
    }


def _queued_message_from_dict(payload: dict[str, Any]) -> QueuedMessage:
    return QueuedMessage(
        id=_message_id_from_dict(payload.get("id", {})),
        topic=str(payload.get("topic", "")),
        payload=payload.get("payload"),
        delivery_guarantee=DeliveryGuarantee(
            payload.get("delivery_guarantee", DeliveryGuarantee.AT_LEAST_ONCE.value)
        ),
        priority=int(payload.get("priority", 0)),
        delay_seconds=float(payload.get("delay_seconds", 0.0)),
        visibility_timeout_seconds=float(
            payload.get("visibility_timeout_seconds", 30.0)
        ),
        max_retries=int(payload.get("max_retries", 3)),
        acknowledgment_timeout_seconds=float(
            payload.get("acknowledgment_timeout_seconds", 300.0)
        ),
        required_acknowledgments=int(payload.get("required_acknowledgments", 1)),
        created_at=float(payload.get("created_at", time.time())),
        headers=dict(payload.get("headers", {})),
        _delivery_attempt=int(payload.get("delivery_attempt", 1)),
        _fingerprint=str(payload.get("fingerprint", "")),
    )


def _in_flight_to_dict(in_flight: InFlightMessage) -> dict[str, Any]:
    return {
        "message": _queued_message_to_dict(in_flight.message),
        "delivery_attempt": in_flight.delivery_attempt,
        "delivered_at": in_flight.delivered_at,
        "delivered_to": list(in_flight.delivered_to),
        "acknowledged_by": list(in_flight.acknowledged_by),
        "status": in_flight.status.value,
    }


def _in_flight_from_dict(payload: dict[str, Any]) -> InFlightMessage:
    message_payload = payload.get("message", {})
    message = _queued_message_from_dict(message_payload)
    return InFlightMessage(
        message=message,
        delivery_attempt=int(payload.get("delivery_attempt", 1)),
        delivered_at=float(payload.get("delivered_at", time.time())),
        delivered_to=set(payload.get("delivered_to", [])),
        acknowledged_by=set(payload.get("acknowledged_by", [])),
        status=QueueMessageStatus(payload.get("status", "in_flight")),
    )


def _sorted_messages(
    entries: list[tuple[int, QueuedMessage]], config: QueueConfiguration | None
) -> deque[QueuedMessage]:
    if config and config.queue_type == QueueType.PRIORITY:
        ordered = sorted(entries, key=lambda item: (-item[1].priority, item[0]))
    else:
        ordered = sorted(entries, key=lambda item: item[0])
    return deque(message for _, message in ordered)


@dataclass(slots=True)
class MemoryQueueStore:
    """In-memory queue store."""

    namespace: str
    queue_name: str
    _serializer: JsonSerializer = field(default_factory=JsonSerializer)
    _pending: dict[int, QueuedMessage] = field(default_factory=dict)
    _dead_letter: dict[int, QueuedMessage] = field(default_factory=dict)
    _in_flight: dict[str, InFlightMessage] = field(default_factory=dict)
    _fingerprints: list[tuple[float, str]] = field(default_factory=list)
    _next_seq: int = 0
    _config: QueueConfiguration | None = None

    async def load_state(self) -> QueuePersistenceState:
        pending = _sorted_messages(list(self._pending.items()), self._config)
        dead_letter = deque(message for _, message in sorted(self._dead_letter.items()))
        return QueuePersistenceState(
            pending=pending,
            in_flight=dict(self._in_flight),
            dead_letter=dead_letter,
            fingerprints=list(self._fingerprints),
            next_sequence=self._next_seq,
            config=self._config,
        )

    async def save_config(self, config: QueueConfiguration) -> None:
        self._config = config

    async def enqueue(self, message: QueuedMessage) -> int:
        seq = self._next_seq
        self._next_seq += 1
        self._pending[seq] = message
        return seq

    async def mark_in_flight(self, in_flight: InFlightMessage) -> None:
        message_id = str(in_flight.message.id)
        self._in_flight[message_id] = in_flight

    async def ack(self, message_id: str) -> None:
        self._in_flight.pop(message_id, None)
        to_remove = [
            seq for seq, msg in self._pending.items() if str(msg.id) == message_id
        ]
        for seq in to_remove:
            self._pending.pop(seq, None)
        to_remove_dead = [
            seq for seq, msg in self._dead_letter.items() if str(msg.id) == message_id
        ]
        for seq in to_remove_dead:
            self._dead_letter.pop(seq, None)

    async def move_to_dead_letter(self, message: QueuedMessage) -> int:
        seq = self._next_seq
        self._next_seq += 1
        self._dead_letter[seq] = message
        return seq

    async def requeue(self, message: QueuedMessage) -> int:
        return await self.enqueue(message)

    async def add_fingerprint(self, timestamp: float, fingerprint: str) -> None:
        self._fingerprints.append((timestamp, fingerprint))


@dataclass(slots=True)
class SQLiteQueueStore:
    """SQLite-backed queue store."""

    backend: SQLitePersistenceBackend
    namespace: str
    queue_name: str
    _serializer: JsonSerializer = field(default_factory=JsonSerializer)

    async def load_state(self) -> QueuePersistenceState:
        config = await self._load_config()
        meta = await self.backend.fetch_one(
            "SELECT next_seq FROM queue_meta WHERE namespace=? AND queue_name=?",
            (self.namespace, self.queue_name),
        )
        next_seq = int(meta[0]) if meta else 0

        rows = await self.backend.fetch_all(
            "SELECT message_id, status, seq, message, in_flight FROM queue_messages"
            " WHERE namespace=? AND queue_name=?",
            (self.namespace, self.queue_name),
        )
        pending_entries: list[tuple[int, QueuedMessage]] = []
        dead_entries: list[tuple[int, QueuedMessage]] = []
        in_flight: dict[str, InFlightMessage] = {}

        for message_id, status, seq, message_blob, in_flight_blob in rows:
            message = _queued_message_from_dict(
                self._serializer.deserialize(message_blob)
            )
            if status == "pending":
                pending_entries.append((int(seq or 0), message))
            elif status == "dead_letter":
                dead_entries.append((int(seq or 0), message))
            elif status == "in_flight":
                if in_flight_blob:
                    in_flight_state = _in_flight_from_dict(
                        self._serializer.deserialize(in_flight_blob)
                    )
                    in_flight[str(message_id)] = in_flight_state
                else:
                    in_flight[str(message_id)] = InFlightMessage(
                        message=message,
                        delivery_attempt=message._delivery_attempt,
                        delivered_at=time.time(),
                    )

        pending = _sorted_messages(pending_entries, config)
        dead_letter = deque(message for _, message in sorted(dead_entries))

        fingerprint_rows = await self.backend.fetch_all(
            "SELECT timestamp, fingerprint FROM queue_fingerprints WHERE namespace=? AND queue_name=?",
            (self.namespace, self.queue_name),
        )
        fingerprints = [(float(ts), str(fp)) for ts, fp in fingerprint_rows]

        return QueuePersistenceState(
            pending=pending,
            in_flight=in_flight,
            dead_letter=dead_letter,
            fingerprints=fingerprints,
            next_sequence=next_seq,
            config=config,
        )

    async def save_config(self, config: QueueConfiguration) -> None:
        payload = self._serializer.serialize(
            {
                "name": config.name,
                "queue_type": config.queue_type.value,
                "max_size": config.max_size,
                "default_visibility_timeout_seconds": config.default_visibility_timeout_seconds,
                "default_acknowledgment_timeout_seconds": config.default_acknowledgment_timeout_seconds,
                "message_ttl_seconds": config.message_ttl_seconds,
                "enable_dead_letter_queue": config.enable_dead_letter_queue,
                "dead_letter_max_receives": config.dead_letter_max_receives,
                "enable_deduplication": config.enable_deduplication,
                "deduplication_window_seconds": config.deduplication_window_seconds,
                "max_retries": config.max_retries,
            }
        )
        await self.backend.execute(
            "INSERT OR REPLACE INTO queue_meta (namespace, queue_name, queue_config, next_seq)"
            " VALUES (?, ?, ?, COALESCE((SELECT next_seq FROM queue_meta WHERE namespace=? AND queue_name=?), 0))",
            (self.namespace, self.queue_name, payload, self.namespace, self.queue_name),
        )

    async def enqueue(self, message: QueuedMessage) -> int:
        seq = await self._next_sequence()
        payload = self._serializer.serialize(_queued_message_to_dict(message))
        await self.backend.execute(
            "INSERT OR REPLACE INTO queue_messages (namespace, queue_name, message_id, status, seq, message, in_flight, updated_at)"
            " VALUES (?, ?, ?, 'pending', ?, ?, NULL, ?)",
            (
                self.namespace,
                self.queue_name,
                str(message.id),
                seq,
                payload,
                time.time(),
            ),
        )
        return seq

    async def mark_in_flight(self, in_flight: InFlightMessage) -> None:
        payload = self._serializer.serialize(_queued_message_to_dict(in_flight.message))
        in_flight_payload = self._serializer.serialize(_in_flight_to_dict(in_flight))
        await self.backend.execute(
            "INSERT OR REPLACE INTO queue_messages (namespace, queue_name, message_id, status, seq, message, in_flight, updated_at)"
            " VALUES (?, ?, ?, 'in_flight', NULL, ?, ?, ?)",
            (
                self.namespace,
                self.queue_name,
                str(in_flight.message.id),
                payload,
                in_flight_payload,
                time.time(),
            ),
        )

    async def ack(self, message_id: str) -> None:
        await self.backend.execute(
            "DELETE FROM queue_messages WHERE namespace=? AND queue_name=? AND message_id=?",
            (self.namespace, self.queue_name, message_id),
        )

    async def move_to_dead_letter(self, message: QueuedMessage) -> int:
        seq = await self._next_sequence()
        payload = self._serializer.serialize(_queued_message_to_dict(message))
        await self.backend.execute(
            "INSERT OR REPLACE INTO queue_messages (namespace, queue_name, message_id, status, seq, message, in_flight, updated_at)"
            " VALUES (?, ?, ?, 'dead_letter', ?, ?, NULL, ?)",
            (
                self.namespace,
                self.queue_name,
                str(message.id),
                seq,
                payload,
                time.time(),
            ),
        )
        return seq

    async def requeue(self, message: QueuedMessage) -> int:
        return await self.enqueue(message)

    async def add_fingerprint(self, timestamp: float, fingerprint: str) -> None:
        await self.backend.execute(
            "INSERT INTO queue_fingerprints (namespace, queue_name, timestamp, fingerprint)"
            " VALUES (?, ?, ?, ?)",
            (self.namespace, self.queue_name, timestamp, fingerprint),
        )

    async def _next_sequence(self) -> int:
        row = await self.backend.fetch_one(
            "SELECT next_seq FROM queue_meta WHERE namespace=? AND queue_name=?",
            (self.namespace, self.queue_name),
        )
        next_seq = int(row[0]) if row else 0
        await self.backend.execute(
            "INSERT OR REPLACE INTO queue_meta (namespace, queue_name, queue_config, next_seq)"
            " VALUES (?, ?, COALESCE((SELECT queue_config FROM queue_meta WHERE namespace=? AND queue_name=?), NULL), ?)",
            (
                self.namespace,
                self.queue_name,
                self.namespace,
                self.queue_name,
                next_seq + 1,
            ),
        )
        return next_seq

    async def _load_config(self) -> QueueConfiguration | None:
        row = await self.backend.fetch_one(
            "SELECT queue_config FROM queue_meta WHERE namespace=? AND queue_name=?",
            (self.namespace, self.queue_name),
        )
        if row is None or row[0] is None:
            return None
        payload = self._serializer.deserialize(row[0])
        return QueueConfiguration(
            name=str(payload.get("name", self.queue_name)),
            queue_type=QueueType(payload.get("queue_type", QueueType.FIFO.value)),
            max_size=int(payload.get("max_size", 10000)),
            default_visibility_timeout_seconds=float(
                payload.get("default_visibility_timeout_seconds", 30.0)
            ),
            default_acknowledgment_timeout_seconds=float(
                payload.get("default_acknowledgment_timeout_seconds", 300.0)
            ),
            message_ttl_seconds=payload.get("message_ttl_seconds"),
            enable_dead_letter_queue=bool(
                payload.get("enable_dead_letter_queue", True)
            ),
            dead_letter_max_receives=int(payload.get("dead_letter_max_receives", 3)),
            enable_deduplication=bool(payload.get("enable_deduplication", False)),
            deduplication_window_seconds=float(
                payload.get("deduplication_window_seconds", 300.0)
            ),
            max_retries=int(payload.get("max_retries", 3)),
        )
