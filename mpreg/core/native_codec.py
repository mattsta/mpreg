"""Native-backed codec helpers for wire JSON, digests, and size estimates.

Design goals
------------
* **No nested ``str()`` / ``repr()`` on hot paths.** Python's recursive
  ``dict.__repr__`` / ``PyUnicodeWriter_WriteRepr`` is the failure mode that
  hung 50-node gossip (catalog deltas materialised as multi-MB Unicode).
* **Prefer orjson (SIMD / Rust)** for dumps/loads already used by
  :class:`JsonSerializer`. Canonical form uses ``OPT_SORT_KEYS`` so HMAC and
  content digests stay stable without stdlib ``json.dumps``.
* **Bounded walks** for statistics and integrity fingerprints so fan-out
  never pays O(full catalog) in the interpreter on every message create.
* **Pluggable backends** via :class:`CodecBackend` so a future msgpack /
  simdjson / loadable native module can drop in without touching call sites.

This module is the single place mechanical encoding should live for fabric
control-plane, consensus, persistence, and pub/sub data plane code.

Text/file helpers (``dumps_text``, ``loads_text``, ``dumps_pretty``,
``dump_path``, ``load_path``) cover operator JSON and on-disk snapshots so
call sites never import stdlib ``json`` for product paths.
"""

from __future__ import annotations

import hashlib
import sys
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, is_dataclass
from typing import Any, Protocol, runtime_checkable

import orjson

from .errors import OPERATIONAL_EXCEPTIONS

# ---------------------------------------------------------------------------
# Bounds — stats / fingerprints must stay O(1)-ish under catalog fan-out
# ---------------------------------------------------------------------------

_SIZE_MAX_DEPTH = 6
_SIZE_MAX_ITEMS = 256
_SIZE_FALLBACK_BYTES = 256

# Integrity fingerprint: hash a bounded structural sketch, never full repr.
_FP_MAX_DEPTH = 4
_FP_MAX_ITEMS = 64


@runtime_checkable
class CodecBackend(Protocol):
    """Minimal native codec surface (orjson today; msgpack/simdjson later)."""

    name: str

    def dumps(self, data: Any) -> bytes:
        """Encode ``data`` to wire bytes."""
        ...

    def loads(self, data: bytes) -> Any:
        """Decode wire bytes to Python objects."""
        ...

    def canonical_dumps(self, data: Any) -> bytes:
        """Stable key-sorted encoding for HMAC / content digests."""
        ...


def _stable_sequence(items: Any) -> list[Any]:
    """Deterministic list for set/frozenset (canonical digests)."""
    try:
        return sorted(items, key=lambda item: (type(item).__name__, repr(item)))
    except OPERATIONAL_EXCEPTIONS:
        return list(items)


# orjson encodes the full unsigned 64-bit integer range as JSON numbers
# (i64 min .. u64 max). Values outside that raise TypeError and do *not*
# invoke ``default`` — so we only walk the tree on that rare failure path.
# (JS consumers lose precision above 2^53-1 either way; wire peers are Python.)
_ORJSON_INT_MIN = -(1 << 63)
_ORJSON_INT_MAX = (1 << 64) - 1  # u64 max

# Fast-path options: non-str keys handled in Rust; no Python pre-walk.
_ORJSON_BASE_OPTS = orjson.OPT_NON_STR_KEYS


def _json_safe_int(n: int) -> int | str:
    """Pass through ints orjson can emit; decimal-string the rest."""
    if _ORJSON_INT_MIN <= n <= _ORJSON_INT_MAX:
        return n
    return str(n)


def _coerce_orjson_tree(data: Any, *, _depth: int = 0) -> Any:
    """Slow path only: rewrite a tree after orjson TypeError.

    Not used on the hot path. Handles true bigints (outside u64) that orjson
    rejects without calling ``default``. Non-str keys are normally covered by
    ``OPT_NON_STR_KEYS``; this walk still normalizes them for the retry.
    """
    if isinstance(data, bool) or data is None:
        return data
    if isinstance(data, int):
        return _json_safe_int(data)
    if isinstance(data, (str, float, bytes, bytearray, memoryview)):
        return data
    if _depth > 64:
        return data
    if isinstance(data, Mapping):
        out: dict[Any, Any] = {}
        for k, v in data.items():
            key: Any = k if isinstance(k, str) else str(k)
            out[key] = _coerce_orjson_tree(v, _depth=_depth + 1)
        return out
    if isinstance(data, (list, tuple)):
        return [_coerce_orjson_tree(v, _depth=_depth + 1) for v in data]
    if isinstance(data, (set, frozenset)):
        return [
            _coerce_orjson_tree(v, _depth=_depth + 1) for v in _stable_sequence(data)
        ]
    return data


def _orjson_default(obj: Any) -> Any:
    """Adapter for types orjson does not natively encode.

    Called per exotic leaf (set, dataclass, pydantic, …) — not a tree walk.
    Note: oversized integers raise TypeError *before* default is consulted;
    those hit ``_orjson_dumps``'s TypeError retry instead.
    """
    if isinstance(obj, (set, frozenset)):
        return _stable_sequence(obj)
    if isinstance(obj, int) and not isinstance(obj, bool):
        # Reached only for int subclasses or odd paths; plain bigint TypeErrors.
        return _json_safe_int(obj)
    dump = getattr(obj, "model_dump", None)
    if callable(dump):
        return dump()
    if is_dataclass(obj) and not isinstance(obj, type):
        # Lazy import-free dataclass → dict for digest paths.
        return {
            name: getattr(obj, name)
            for name in obj.__dataclass_fields__  # type: ignore[union-attr]
        }
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return bytes(obj)
    # Last resort for signature/digest paths: type name only — never nested repr.
    return f"<{type(obj).__name__}>"


def _orjson_dumps(data: Any, *, option: int = 0) -> bytes:
    """Encode with orjson; no pre-walk on the common path.

    Live suite profiles showed eager ``_coerce_orjson_tree`` dominating every
    envelope send. Catalog/gossip payloads are already JSON-shaped from
    ``to_dict()``, so the hot path is a single Rust encode with:

    * ``default`` — exotic types (set/dataclass/pydantic) at the leaf
    * ``OPT_NON_STR_KEYS`` — int/bool keys without a Python walk
    * TypeError retry + walk — only true bigints outside u64 (rare)
    """
    opts = _ORJSON_BASE_OPTS | option
    try:
        return orjson.dumps(data, option=opts, default=_orjson_default)
    except TypeError:
        # Almost always "Integer exceeds 64-bit range" — walk once, stringify
        # those leaves, retry. Not the gossip hot path.
        return orjson.dumps(
            _coerce_orjson_tree(data), option=opts, default=_orjson_default
        )


@dataclass(slots=True, frozen=True)
class OrjsonBackend:
    """Default backend: orjson (Rust, SIMD where available)."""

    name: str = "orjson"

    def dumps(self, data: Any) -> bytes:
        return _orjson_dumps(data)

    def loads(self, data: bytes) -> Any:
        return orjson.loads(data)

    def canonical_dumps(self, data: Any) -> bytes:
        return _orjson_dumps(data, option=orjson.OPT_SORT_KEYS)


_BACKEND: CodecBackend = OrjsonBackend()
_BACKEND_FACTORIES: dict[str, Callable[[], CodecBackend]] = {
    "orjson": OrjsonBackend,
}


def register_codec_backend(name: str, factory: Callable[[], CodecBackend]) -> None:
    """Register a named codec backend factory (native plugins / tests)."""
    _BACKEND_FACTORIES[name] = factory


def set_codec_backend(backend: CodecBackend | str) -> CodecBackend:
    """Install the process-wide codec backend. Returns the active backend."""
    global _BACKEND
    if isinstance(backend, str):
        factory = _BACKEND_FACTORIES.get(backend)
        if factory is None:
            raise KeyError(f"Unknown codec backend: {backend!r}")
        _BACKEND = factory()
    else:
        _BACKEND = backend
    return _BACKEND


def get_codec_backend() -> CodecBackend:
    return _BACKEND


def dumps(data: Any) -> bytes:
    """Wire encode via the active native backend."""
    return _BACKEND.dumps(data)


def loads(data: bytes) -> Any:
    """Wire decode via the active native backend."""
    return _BACKEND.loads(data)


def canonical_dumps(data: Any) -> bytes:
    """Key-sorted stable encode for HMAC and content digests."""
    return _BACKEND.canonical_dumps(data)


def canonical_hash_hex(
    data: Any,
    *,
    algorithm: str = "sha256",
    truncate: int | None = None,
) -> str:
    """Hash canonical bytes; optional hex truncation for short digests."""
    digest = hashlib.new(algorithm, canonical_dumps(data)).hexdigest()
    if truncate is not None:
        return digest[:truncate]
    return digest


# orjson.JSONDecodeError is a ValueError subclass (not json.JSONDecodeError).
# Export one name so call sites can catch decode failures without stdlib json.
JSONDecodeError = orjson.JSONDecodeError


def dumps_text(data: Any) -> str:
    """UTF-8 text form of :func:`dumps` (compact wire JSON as str)."""
    return dumps(data).decode("utf-8")


def loads_text(data: str | bytes | bytearray | memoryview) -> Any:
    """Decode from ``str`` or bytes via the active backend."""
    if isinstance(data, str):
        return loads(data.encode("utf-8"))
    return loads(bytes(data))


def dumps_pretty(data: Any) -> bytes:
    """Human-indented JSON bytes (operator files, CLI, debug snapshots)."""
    return orjson.dumps(
        data,
        option=orjson.OPT_INDENT_2,
        default=_orjson_default,
    )


def dumps_pretty_text(data: Any) -> str:
    """Human-indented JSON as UTF-8 text."""
    return dumps_pretty(data).decode("utf-8")


def dump_path(path: str | Any, data: Any, *, pretty: bool = True) -> None:
    """Write JSON bytes to a filesystem path (creates parent dirs).

    ``path`` may be ``str`` or ``pathlib.Path``. Uses a single write of
    encoded bytes — no stdlib ``json.dump`` text incremental path.
    """
    from pathlib import Path as _Path

    p = _Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    blob = dumps_pretty(data) if pretty else dumps(data)
    p.write_bytes(blob)


def load_path(path: str | Any) -> Any:
    """Read and decode JSON from a filesystem path."""
    from pathlib import Path as _Path

    return loads(_Path(path).read_bytes())


def estimate_size_bytes(value: Any) -> int:
    """Bounded O(items) memory estimate; never calls nested str/repr.

    Safe for backlog stats, cache sizing, and any fan-out path that used to
    do ``len(str(payload))`` or pympler walks on large nested dicts.
    """
    if value is None:
        return 0
    if isinstance(value, (bytes, bytearray, memoryview)):
        return len(value)
    if isinstance(value, str):
        return len(value)
    if isinstance(value, (bool, int, float)):
        return int(sys.getsizeof(value))

    total = 0
    seen: set[int] = set()
    stack: list[tuple[Any, int]] = [(value, 0)]
    items = 0

    while stack and items < _SIZE_MAX_ITEMS:
        current, depth = stack.pop()
        items += 1
        obj_id = id(current)
        if obj_id in seen:
            continue

        if current is None:
            continue
        if isinstance(current, (bytes, bytearray, memoryview)):
            total += len(current)
            continue
        if isinstance(current, str):
            total += len(current)
            continue
        if isinstance(current, (bool, int, float)):
            total += sys.getsizeof(current)
            continue

        seen.add(obj_id)
        try:
            total += sys.getsizeof(current)
        except TypeError:
            total += _SIZE_FALLBACK_BYTES
            continue

        if depth >= _SIZE_MAX_DEPTH:
            continue

        if isinstance(current, Mapping):
            for key, child in current.items():
                if items + len(stack) >= _SIZE_MAX_ITEMS:
                    break
                if isinstance(key, (str, bytes, bytearray)):
                    total += len(key)
                else:
                    try:
                        total += sys.getsizeof(key)
                    except TypeError:
                        total += 16
                stack.append((child, depth + 1))
            continue

        if isinstance(current, (list, tuple, set, frozenset, deque)):
            for child in current:
                if items + len(stack) >= _SIZE_MAX_ITEMS:
                    break
                stack.append((child, depth + 1))
            continue

        if isinstance(current, Sequence) and not isinstance(
            current, (str, bytes, bytearray, memoryview)
        ):
            try:
                for child in current:
                    if items + len(stack) >= _SIZE_MAX_ITEMS:
                        break
                    stack.append((child, depth + 1))
            except TypeError:
                pass
            continue

        if is_dataclass(current) and not isinstance(current, type):
            try:
                for field_name in current.__dataclass_fields__:  # type: ignore[union-attr]
                    if items + len(stack) >= _SIZE_MAX_ITEMS:
                        break
                    stack.append((getattr(current, field_name), depth + 1))
            except OPERATIONAL_EXCEPTIONS:
                pass
            continue

        obj_dict = getattr(current, "__dict__", None)
        if isinstance(obj_dict, dict):
            for key, child in obj_dict.items():
                if items + len(stack) >= _SIZE_MAX_ITEMS:
                    break
                if isinstance(key, str) and key.startswith("_"):
                    continue
                stack.append((child, depth + 1))

    if not total:
        return _SIZE_FALLBACK_BYTES
    return int(total)


def payload_fingerprint_hex(
    payload: Any,
    *,
    truncate: int = 16,
) -> str:
    """Cheap integrity fingerprint without full nested str/repr or full dumps.

    Strategy (in order):
    1. Prefer stable identity fields on catalog-style dicts (``update_id`` +
       collection lengths) — O(keys), no child walk.
    2. Otherwise stream a bounded structural sketch into SHA-256.
    3. Never materialise a multi-megabyte Unicode tree.
    """
    hasher = hashlib.sha256()
    _feed_fingerprint(hasher, payload)
    return hasher.hexdigest()[:truncate]


def _feed_fingerprint(hasher: Any, payload: Any) -> None:
    if payload is None:
        hasher.update(b"n")
        return
    if isinstance(payload, bool):
        hasher.update(b"t" if payload else b"f")
        return
    if isinstance(payload, int):
        hasher.update(b"i")
        hasher.update(str(payload).encode("ascii", errors="ignore"))
        return
    if isinstance(payload, float):
        hasher.update(b"d")
        hasher.update(repr(payload).encode("ascii", errors="ignore"))
        return
    if isinstance(payload, str):
        raw = payload.encode("utf-8", errors="ignore")
        hasher.update(b"s")
        hasher.update(len(raw).to_bytes(4, "little", signed=False))
        hasher.update(raw[:512])
        return
    if isinstance(payload, (bytes, bytearray, memoryview)):
        raw = bytes(payload)
        hasher.update(b"b")
        hasher.update(len(raw).to_bytes(8, "little", signed=False))
        hasher.update(raw[:512])
        return

    if isinstance(payload, Mapping):
        # Catalog / discovery fast path: identity + shape, not full body.
        update_id = payload.get("update_id") if hasattr(payload, "get") else None
        if isinstance(update_id, str) and update_id:
            hasher.update(b"U")
            hasher.update(update_id.encode("utf-8", errors="ignore"))
            for key in (
                "functions",
                "function_removals",
                "topics",
                "topic_removals",
                "queues",
                "queue_removals",
                "services",
                "service_removals",
                "caches",
                "cache_removals",
                "cache_profiles",
                "cache_profile_removals",
                "nodes",
                "node_removals",
                "cluster_id",
            ):
                if key not in payload:
                    continue
                child = payload[key]
                hasher.update(key.encode("ascii", errors="ignore"))
                if isinstance(child, str):
                    hasher.update(child.encode("utf-8", errors="ignore")[:128])
                elif isinstance(child, (list, tuple)):
                    hasher.update(len(child).to_bytes(4, "little", signed=False))
                elif isinstance(child, (int, float)):
                    hasher.update(str(child).encode("ascii", errors="ignore"))
            return

        hasher.update(b"{")
        items = 0
        try:
            keys = sorted(payload.keys(), key=lambda k: str(k))
        except OPERATIONAL_EXCEPTIONS:
            keys = list(payload.keys())
        for items, key in enumerate(keys, start=1):
            if items > _FP_MAX_ITEMS:
                hasher.update(b"...")
                break
            key_text = key if isinstance(key, str) else str(key)
            hasher.update(key_text.encode("utf-8", errors="ignore")[:64])
            _feed_fingerprint_bounded(hasher, payload[key], depth=1)
        hasher.update(b"}")
        return

    if isinstance(payload, (list, tuple)):
        hasher.update(b"[")
        hasher.update(len(payload).to_bytes(4, "little", signed=False))
        for index, child in enumerate(payload):
            if index >= _FP_MAX_ITEMS:
                hasher.update(b"...")
                break
            _feed_fingerprint_bounded(hasher, child, depth=1)
        hasher.update(b"]")
        return

    if isinstance(payload, (set, frozenset)):
        hasher.update(b"#")
        hasher.update(len(payload).to_bytes(4, "little", signed=False))
        return

    if is_dataclass(payload) and not isinstance(payload, type):
        hasher.update(b"D")
        hasher.update(type(payload).__name__.encode("ascii", errors="ignore"))
        try:
            for field_name in payload.__dataclass_fields__:  # type: ignore[union-attr]
                hasher.update(field_name.encode("ascii", errors="ignore"))
                _feed_fingerprint_bounded(hasher, getattr(payload, field_name), depth=1)
        except OPERATIONAL_EXCEPTIONS:
            pass
        return

    hasher.update(b"o")
    hasher.update(type(payload).__name__.encode("ascii", errors="ignore"))


def _feed_fingerprint_bounded(hasher: Any, value: Any, *, depth: int) -> None:
    if depth >= _FP_MAX_DEPTH:
        hasher.update(b".")
        return
    if value is None or isinstance(value, (bool, int, float, str, bytes, bytearray)):
        _feed_fingerprint(hasher, value)
        return
    if isinstance(value, Mapping):
        hasher.update(b"{")
        hasher.update(len(value).to_bytes(4, "little", signed=False))
        # Only sketch a few keys at nested depth.
        try:
            keys = list(value.keys())[:8]
        except OPERATIONAL_EXCEPTIONS:
            keys = []
        for key in keys:
            key_text = key if isinstance(key, str) else str(key)
            hasher.update(key_text.encode("utf-8", errors="ignore")[:32])
            _feed_fingerprint_bounded(hasher, value[key], depth=depth + 1)
        hasher.update(b"}")
        return
    if isinstance(value, (list, tuple)):
        hasher.update(b"[")
        hasher.update(len(value).to_bytes(4, "little", signed=False))
        for child in value[:8]:
            _feed_fingerprint_bounded(hasher, child, depth=depth + 1)
        hasher.update(b"]")
        return
    hasher.update(type(value).__name__.encode("ascii", errors="ignore")[:32])
