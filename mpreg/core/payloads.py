from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any, TypeVar

from mpreg.datastructures.type_aliases import JsonDict, JsonValue

PAYLOAD_FLOAT = "payload_float"
PAYLOAD_INT = "payload_int"
PAYLOAD_LIST = "payload_list"
PAYLOAD_KEEP_EMPTY = "payload_keep_empty"
PAYLOAD_CONVERTER = "payload_converter"

type PayloadValue = JsonValue
type Payload = JsonDict
type PayloadMapping = Mapping[str, JsonValue]
type PayloadConverter = Callable[[object], JsonValue]


def _is_empty(value: object) -> bool:
    if value is None:
        return True
    if value == "":
        return True
    return bool(isinstance(value, (tuple, list, set, frozenset, dict)) and not value)


def _identity(value: object) -> object:
    return value


def payload_to_dict(value: object) -> JsonValue:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return payload_to_dict(value.value)
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        converted = to_dict()
        if isinstance(converted, Mapping):
            return payload_mapping(converted)
        if isinstance(converted, (list, tuple, set, frozenset)):
            return payload_list(converted)
        if converted is None or isinstance(converted, (str, int, float, bool)):
            return converted
        raise TypeError(
            f"Unsupported payload value from to_dict: {type(converted).__name__}"
        )
    if is_dataclass(value):
        return payload_from_dataclass(value)
    if isinstance(value, Mapping):
        return payload_mapping(value)
    if isinstance(value, (list, tuple, set, frozenset)):
        return payload_list(value)
    raise TypeError(f"Unsupported payload value: {type(value).__name__}")


def payload_list(value: object) -> list[JsonValue]:
    return [payload_to_dict(item) for item in _normalize_list_value(value)]


def payload_mapping(
    value: Mapping[object, object] | None,
    *,
    key_converter: PayloadConverter | None = None,
    value_converter: PayloadConverter | None = None,
    skip_none: bool = True,
) -> JsonDict:
    if not value or not isinstance(value, Mapping):
        return {}
    key_convert = key_converter or str
    value_convert = value_converter or payload_to_dict
    result: JsonDict = {}
    for key, item in value.items():
        if item is None and skip_none:
            continue
        result[key_convert(key)] = value_convert(item)
    return result


def payload_from_dataclass(instance: object) -> Payload:
    if not is_dataclass(instance):
        raise TypeError(f"Expected dataclass instance, got {type(instance).__name__}")
    payload: Payload = {}
    for field in fields(instance):
        value = getattr(instance, field.name)
        metadata = field.metadata
        if value is None:
            if metadata.get(PAYLOAD_KEEP_EMPTY, False):
                if metadata.get(PAYLOAD_LIST, False):
                    payload[field.name] = []
                else:
                    payload[field.name] = None
            continue
        if _is_empty(value) and not metadata.get(PAYLOAD_KEEP_EMPTY, False):
            continue
        converter = metadata.get(PAYLOAD_CONVERTER)
        if converter:
            payload[field.name] = converter(value)
        elif metadata.get(PAYLOAD_LIST, False):
            payload[field.name] = payload_list(value)
        elif metadata.get(PAYLOAD_INT, False):
            payload[field.name] = int(value)
        elif metadata.get(PAYLOAD_FLOAT, False):
            payload[field.name] = float(value)
        else:
            payload[field.name] = payload_to_dict(value)
    return payload


def _normalize_list_value(value: object) -> tuple[object, ...]:
    if value is None:
        return tuple()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(value)
    return (value,)


T = TypeVar("T")


def apply_overrides[T](instance: T, overrides: PayloadMapping) -> T:
    if not is_dataclass(instance):
        raise TypeError(f"Expected dataclass instance, got {type(instance).__name__}")
    if not overrides:
        return instance
    values: list[Any] = []
    override_applied = False
    for field in fields(instance):
        if field.name in overrides:
            value = overrides[field.name]
            if field.metadata.get(PAYLOAD_LIST, False):
                value = _normalize_list_value(value)
            if field.metadata.get(PAYLOAD_INT, False) and value is not None:
                value = int(value)
            if field.metadata.get(PAYLOAD_FLOAT, False) and value is not None:
                value = float(value)
            override_applied = True
        else:
            value = getattr(instance, field.name)
        values.append(value)
    if not override_applied:
        return instance
    return type(instance)(*values)


def parse_request[T](
    request_cls: type[T],
    payload: PayloadMapping | None,
    overrides: PayloadMapping | None = None,
) -> T:
    request = request_cls.from_dict(payload)
    if overrides:
        request = apply_overrides(request, overrides)
    return request
