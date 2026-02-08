from __future__ import annotations

import base64
import re
from dataclasses import dataclass


def normalize_zone(zone: str) -> str:
    return zone.strip().strip(".").lower()


def _split_labels(value: str) -> tuple[str, ...]:
    cleaned = value.strip().strip(".")
    if not cleaned:
        return tuple()
    return tuple(label for label in cleaned.split(".") if label)


NODE_LABEL_PREFIX = "b32-"
_DNS_LABEL_RE = re.compile(r"^[a-z0-9-]+$")


def encode_node_id(node_id: str) -> str:
    value = str(node_id)
    if not value:
        return ""
    encoded = base64.b32encode(value.encode("utf-8")).decode("ascii")
    encoded = encoded.rstrip("=").lower()
    return f"{NODE_LABEL_PREFIX}{encoded}"


def decode_node_id(label: str) -> str | None:
    if not label:
        return None
    if label.startswith(NODE_LABEL_PREFIX):
        data = label[len(NODE_LABEL_PREFIX) :].upper()
        padding = "=" * ((8 - len(data) % 8) % 8)
        try:
            decoded = base64.b32decode(data + padding, casefold=True)
        except Exception:
            return None
        try:
            return decoded.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not _DNS_LABEL_RE.match(label):
        return None
    return label


@dataclass(frozen=True, slots=True)
class ZoneMatch:
    zone: str
    relative_labels: tuple[str, ...]


def match_zone(qname: str, zones: tuple[str, ...]) -> ZoneMatch | None:
    labels = _split_labels(qname.lower())
    if not labels or not zones:
        return None
    normalized = [normalize_zone(zone) for zone in zones if zone]
    normalized = [zone for zone in normalized if zone]
    if not normalized:
        return None
    normalized = sorted(normalized, key=lambda zone: len(zone.split(".")), reverse=True)
    for zone in normalized:
        zone_labels = tuple(zone.split("."))
        if len(zone_labels) > len(labels):
            continue
        if labels[-len(zone_labels) :] == zone_labels:
            relative = labels[: -len(zone_labels)]
            return ZoneMatch(zone=zone, relative_labels=relative)
    return None


def parse_service_labels(labels: tuple[str, ...]) -> tuple[str, str, str, str] | None:
    if len(labels) < 3:
        return None
    service = labels[0]
    proto = labels[1]
    if not service.startswith("_") or not proto.startswith("_"):
        return None
    name = labels[2]
    namespace = ".".join(labels[3:]) if len(labels) > 3 else ""
    return service, proto, name, namespace


def parse_node_labels(labels: tuple[str, ...]) -> str | None:
    if len(labels) != 2:
        return None
    if labels[1] != "node":
        return None
    return decode_node_id(labels[0])
