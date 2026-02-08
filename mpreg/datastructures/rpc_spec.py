from __future__ import annotations

import hashlib
import inspect
import json
import time
import types
import uuid
from collections.abc import Callable, Iterable
from collections.abc import Callable as AbcCallable
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import (
    Annotated,
    Any,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from .function_identity import FunctionIdentity, SemanticVersion
from .type_aliases import (
    EndpointScope,
    FunctionId,
    FunctionVersion,
    JsonDict,
    JsonValue,
    NamespaceName,
    ResourceName,
    RpcDocString,
    RpcDocSummary,
    RpcExampleName,
    RpcName,
    RpcNamespace,
    RpcParamName,
    RpcSpecDigest,
    RpcSpecVersion,
    RpcTag,
    RpcTypeName,
    Timestamp,
)

DEFAULT_RPC_SPEC_VERSION: RpcSpecVersion = "1"
DEFAULT_RPC_SCOPE: EndpointScope = "zone"


class RpcParamKind(Enum):
    POSITIONAL_ONLY = "positional_only"
    POSITIONAL_OR_KEYWORD = "positional_or_keyword"
    VAR_POSITIONAL = "var_positional"
    KEYWORD_ONLY = "keyword_only"
    VAR_KEYWORD = "var_keyword"


class RpcDefaultEncoding(Enum):
    NONE = "none"
    JSON = "json"
    REPR = "repr"


@dataclass(frozen=True, slots=True)
class RpcTypeSpec:
    display: RpcTypeName
    module: str | None = None
    qualname: str | None = None
    origin: RpcTypeName | None = None
    args: tuple[RpcTypeName, ...] = ()
    is_optional: bool = False

    def to_dict(self) -> JsonDict:
        payload: JsonDict = {
            "display": self.display,
            "args": list(self.args),
            "is_optional": self.is_optional,
        }
        if self.module is not None:
            payload["module"] = self.module
        if self.qualname is not None:
            payload["qualname"] = self.qualname
        if self.origin is not None:
            payload["origin"] = self.origin
        return payload

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RpcTypeSpec:
        return cls(
            display=str(payload.get("display", "Any")),
            module=str(payload.get("module")) if payload.get("module") else None,
            qualname=str(payload.get("qualname")) if payload.get("qualname") else None,
            origin=str(payload.get("origin")) if payload.get("origin") else None,
            args=tuple(payload.get("args", []) or []),
            is_optional=bool(payload.get("is_optional", False)),
        )


@dataclass(frozen=True, slots=True)
class RpcDefaultSpec:
    has_default: bool
    encoding: RpcDefaultEncoding = RpcDefaultEncoding.NONE
    json_value: JsonValue | None = None
    repr_value: str | None = None

    def to_dict(self) -> JsonDict:
        payload: JsonDict = {
            "has_default": self.has_default,
            "encoding": self.encoding.value,
        }
        if self.encoding is RpcDefaultEncoding.JSON:
            payload["json_value"] = self.json_value
        if self.encoding is RpcDefaultEncoding.REPR and self.repr_value is not None:
            payload["repr_value"] = self.repr_value
        return payload

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RpcDefaultSpec:
        encoding_raw = str(payload.get("encoding", RpcDefaultEncoding.NONE.value))
        encoding = RpcDefaultEncoding(encoding_raw)
        return cls(
            has_default=bool(payload.get("has_default", False)),
            encoding=encoding,
            json_value=payload.get("json_value"),
            repr_value=str(payload.get("repr_value"))
            if payload.get("repr_value") is not None
            else None,
        )


@dataclass(frozen=True, slots=True)
class RpcParamDoc:
    name: RpcParamName
    description: RpcDocString

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "description": self.description}

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RpcParamDoc:
        return cls(
            name=str(payload.get("name", "")),
            description=str(payload.get("description", "")),
        )


@dataclass(frozen=True, slots=True)
class RpcDocSpec:
    summary: RpcDocSummary
    description: RpcDocString
    param_docs: tuple[RpcParamDoc, ...] = field(default_factory=tuple)
    return_doc: RpcDocString = ""

    def to_dict(self) -> JsonDict:
        return {
            "summary": self.summary,
            "description": self.description,
            "param_docs": [doc.to_dict() for doc in self.param_docs],
            "return_doc": self.return_doc,
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RpcDocSpec:
        return cls(
            summary=str(payload.get("summary", "")),
            description=str(payload.get("description", "")),
            param_docs=tuple(
                RpcParamDoc.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("param_docs", [])
            ),
            return_doc=str(payload.get("return_doc", "")),
        )


@dataclass(frozen=True, slots=True)
class RpcParamSpec:
    name: RpcParamName
    kind: RpcParamKind
    type_spec: RpcTypeSpec
    required: bool
    default: RpcDefaultSpec = field(
        default_factory=lambda: RpcDefaultSpec(has_default=False)
    )
    doc: RpcDocString = ""

    def to_dict(self) -> JsonDict:
        return {
            "name": self.name,
            "kind": self.kind.value,
            "type_spec": self.type_spec.to_dict(),
            "required": self.required,
            "default": self.default.to_dict(),
            "doc": self.doc,
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RpcParamSpec:
        return cls(
            name=str(payload.get("name", "")),
            kind=RpcParamKind(
                str(payload.get("kind", RpcParamKind.POSITIONAL_OR_KEYWORD.value))
            ),
            type_spec=RpcTypeSpec.from_dict(
                payload.get("type_spec", {})  # type: ignore[arg-type]
            ),
            required=bool(payload.get("required", False)),
            default=RpcDefaultSpec.from_dict(
                payload.get("default", {})  # type: ignore[arg-type]
            ),
            doc=str(payload.get("doc", "")),
        )


@dataclass(frozen=True, slots=True)
class RpcReturnSpec:
    type_spec: RpcTypeSpec
    doc: RpcDocString = ""

    def to_dict(self) -> JsonDict:
        return {"type_spec": self.type_spec.to_dict(), "doc": self.doc}

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RpcReturnSpec:
        return cls(
            type_spec=RpcTypeSpec.from_dict(
                payload.get("type_spec", {})  # type: ignore[arg-type]
            ),
            doc=str(payload.get("doc", "")),
        )


@dataclass(frozen=True, slots=True)
class RpcExampleSpec:
    name: RpcExampleName
    description: RpcDocString = ""
    request: JsonValue | None = None
    response: JsonValue | None = None

    def to_dict(self) -> JsonDict:
        payload: JsonDict = {
            "name": self.name,
            "description": self.description,
        }
        if self.request is not None:
            payload["request"] = self.request
        if self.response is not None:
            payload["response"] = self.response
        return payload

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RpcExampleSpec:
        return cls(
            name=str(payload.get("name", "")),
            description=str(payload.get("description", "")),
            request=payload.get("request"),
            response=payload.get("response"),
        )


@dataclass(frozen=True, slots=True)
class RpcSpec:
    identity: FunctionIdentity
    namespace: RpcNamespace
    doc: RpcDocSpec
    parameters: tuple[RpcParamSpec, ...]
    return_spec: RpcReturnSpec
    resources: frozenset[ResourceName] = field(default_factory=frozenset)
    tags: frozenset[RpcTag] = field(default_factory=frozenset)
    scope: EndpointScope = DEFAULT_RPC_SCOPE
    capabilities: frozenset[str] = field(default_factory=frozenset)
    examples: tuple[RpcExampleSpec, ...] = field(default_factory=tuple)
    spec_version: RpcSpecVersion = DEFAULT_RPC_SPEC_VERSION
    spec_digest: RpcSpecDigest = ""

    def to_dict(self, *, include_digest: bool = True) -> JsonDict:
        payload: JsonDict = {
            "identity": self.identity.to_dict(),
            "namespace": self.namespace,
            "doc": self.doc.to_dict(),
            "parameters": [param.to_dict() for param in self.parameters],
            "return_spec": self.return_spec.to_dict(),
            "resources": sorted(self.resources),
            "tags": sorted(self.tags),
            "scope": self.scope,
            "capabilities": sorted(self.capabilities),
            "examples": [example.to_dict() for example in self.examples],
            "spec_version": self.spec_version,
        }
        if include_digest:
            payload["spec_digest"] = self.spec_digest
        return payload

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RpcSpec:
        identity = FunctionIdentity.from_dict(
            payload.get("identity", {})  # type: ignore[arg-type]
        )
        return cls(
            identity=identity,
            namespace=str(payload.get("namespace", "")),
            doc=RpcDocSpec.from_dict(payload.get("doc", {}) or {}),  # type: ignore[arg-type]
            parameters=tuple(
                RpcParamSpec.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("parameters", [])
            ),
            return_spec=RpcReturnSpec.from_dict(
                payload.get("return_spec", {})  # type: ignore[arg-type]
            ),
            resources=frozenset(payload.get("resources", []) or []),
            tags=frozenset(payload.get("tags", []) or []),
            scope=str(payload.get("scope", DEFAULT_RPC_SCOPE)),
            capabilities=frozenset(payload.get("capabilities", []) or []),
            examples=tuple(
                RpcExampleSpec.from_dict(item)  # type: ignore[arg-type]
                for item in payload.get("examples", [])
            ),
            spec_version=str(payload.get("spec_version", DEFAULT_RPC_SPEC_VERSION)),
            spec_digest=str(payload.get("spec_digest", "")),
        )

    def summary(self) -> RpcSpecSummary:
        return RpcSpecSummary.from_spec(self)

    @classmethod
    def from_callable(
        cls,
        handler: Callable[..., Any],
        *,
        name: RpcName | None = None,
        function_id: FunctionId | None = None,
        version: FunctionVersion = "1.0.0",
        namespace: NamespaceName | None = None,
        resources: Iterable[ResourceName] = (),
        tags: Iterable[RpcTag] = (),
        scope: EndpointScope | None = None,
        capabilities: Iterable[str] = (),
        doc: RpcDocSpec | None = None,
        examples: Iterable[RpcExampleSpec] = (),
    ) -> RpcSpec:
        resolved_name = name or handler.__name__
        resolved_namespace = namespace or _namespace_from_name(resolved_name)
        resolved_function_id = function_id or resolved_name
        resolved_scope = scope or DEFAULT_RPC_SCOPE
        identity = FunctionIdentity(
            name=resolved_name,
            function_id=resolved_function_id,
            version=SemanticVersion.parse(version),
        )
        doc_spec = doc or _parse_docstring(inspect.getdoc(handler) or "")
        if not doc_spec.summary:
            summary = resolved_name.replace("_", " ").strip()
            doc_spec = replace(doc_spec, summary=summary)
        param_docs = {
            doc_item.name: doc_item.description for doc_item in doc_spec.param_docs
        }
        parameters = _build_param_specs(handler, param_docs=param_docs)
        return_spec = _build_return_spec(handler, return_doc=doc_spec.return_doc)
        spec = cls(
            identity=identity,
            namespace=resolved_namespace,
            doc=doc_spec,
            parameters=parameters,
            return_spec=return_spec,
            resources=frozenset(resources),
            tags=frozenset(tags),
            scope=resolved_scope,
            capabilities=frozenset(capabilities),
            examples=tuple(examples),
            spec_version=DEFAULT_RPC_SPEC_VERSION,
        )
        digest = _compute_spec_digest(spec)
        return replace(spec, spec_digest=digest)


@dataclass(frozen=True, slots=True)
class RpcSpecSummary:
    identity: FunctionIdentity
    namespace: RpcNamespace
    summary: RpcDocSummary
    parameter_count: int
    required_parameter_count: int
    return_type: RpcTypeSpec
    spec_version: RpcSpecVersion
    spec_digest: RpcSpecDigest

    def to_dict(self) -> JsonDict:
        return {
            "identity": self.identity.to_dict(),
            "namespace": self.namespace,
            "summary": self.summary,
            "parameter_count": self.parameter_count,
            "required_parameter_count": self.required_parameter_count,
            "return_type": self.return_type.to_dict(),
            "spec_version": self.spec_version,
            "spec_digest": self.spec_digest,
        }

    @classmethod
    def from_dict(cls, payload: JsonDict) -> RpcSpecSummary:
        return cls(
            identity=FunctionIdentity.from_dict(
                payload.get("identity", {})  # type: ignore[arg-type]
            ),
            namespace=str(payload.get("namespace", "")),
            summary=str(payload.get("summary", "")),
            parameter_count=int(payload.get("parameter_count", 0)),
            required_parameter_count=int(payload.get("required_parameter_count", 0)),
            return_type=RpcTypeSpec.from_dict(
                payload.get("return_type", {})  # type: ignore[arg-type]
            ),
            spec_version=str(payload.get("spec_version", DEFAULT_RPC_SPEC_VERSION)),
            spec_digest=str(payload.get("spec_digest", "")),
        )

    @classmethod
    def from_spec(cls, spec: RpcSpec) -> RpcSpecSummary:
        required_count = sum(1 for param in spec.parameters if param.required)
        return cls(
            identity=spec.identity,
            namespace=spec.namespace,
            summary=spec.doc.summary,
            parameter_count=len(spec.parameters),
            required_parameter_count=required_count,
            return_type=spec.return_spec.type_spec,
            spec_version=spec.spec_version,
            spec_digest=spec.spec_digest,
        )


@dataclass(frozen=True, slots=True)
class RpcHandlerSpec:
    handler_name: str
    handler_module: str | None
    handler_qualname: str | None
    handler_signature: str
    handler_is_async: bool

    @classmethod
    def from_callable(cls, handler: Callable[..., Any]) -> RpcHandlerSpec:
        return cls(
            handler_name=handler.__name__,
            handler_module=getattr(handler, "__module__", None),
            handler_qualname=getattr(handler, "__qualname__", None),
            handler_signature=str(inspect.signature(handler)),
            handler_is_async=inspect.iscoroutinefunction(handler),
        )


@dataclass(slots=True)
class RpcRegistration:
    spec: RpcSpec
    handler: Callable[..., Any]
    handler_spec: RpcHandlerSpec
    registered_at: Timestamp = field(default_factory=time.time)
    registration_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.handler(*args, **kwargs)

    async def call_async(self, *args: Any, **kwargs: Any) -> Any:
        if inspect.iscoroutinefunction(self.handler):
            return await self.handler(*args, **kwargs)
        return self.handler(*args, **kwargs)

    @classmethod
    def from_callable(
        cls,
        handler: Callable[..., Any],
        *,
        name: RpcName | None = None,
        function_id: FunctionId | None = None,
        version: FunctionVersion = "1.0.0",
        namespace: NamespaceName | None = None,
        resources: Iterable[ResourceName] = (),
        tags: Iterable[RpcTag] = (),
        scope: EndpointScope | None = None,
        capabilities: Iterable[str] = (),
        doc: RpcDocSpec | None = None,
        examples: Iterable[RpcExampleSpec] = (),
        now: Timestamp | None = None,
        registration_id: str | None = None,
    ) -> RpcRegistration:
        spec = RpcSpec.from_callable(
            handler,
            name=name,
            function_id=function_id,
            version=version,
            namespace=namespace,
            resources=resources,
            tags=tags,
            scope=scope,
            capabilities=capabilities,
            doc=doc,
            examples=examples,
        )
        handler_spec = RpcHandlerSpec.from_callable(handler)
        registered_at = now if now is not None else time.time()
        return cls(
            spec=spec,
            handler=handler,
            handler_spec=handler_spec,
            registered_at=registered_at,
            registration_id=registration_id or str(uuid.uuid4()),
        )


def _namespace_from_name(name: RpcName) -> RpcNamespace:
    if "." in name:
        return name.rsplit(".", 1)[0]
    return ""


def _compute_spec_digest(spec: RpcSpec) -> RpcSpecDigest:
    payload = spec.to_dict(include_digest=False)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _parse_docstring(doc: str) -> RpcDocSpec:
    if not doc:
        return RpcDocSpec(summary="", description="", param_docs=tuple(), return_doc="")
    lines = [line.rstrip() for line in doc.strip().splitlines()]
    summary = ""
    description_lines: list[str] = []
    param_docs: list[RpcParamDoc] = []
    return_lines: list[str] = []
    section = "summary"
    current_param_index: int | None = None
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            if section == "summary":
                section = "description"
            continue
        lowered = line.lower()
        if lowered in {"args:", "arguments:", "parameters:"}:
            section = "params"
            current_param_index = None
            continue
        if lowered in {"returns:", "return:"}:
            section = "returns"
            current_param_index = None
            continue
        if section == "summary":
            summary = line
            section = "description"
            continue
        if section == "params":
            if ":" in line:
                name_part, desc_part = line.split(":", 1)
                name = name_part.strip().split(" ", 1)[0]
                desc = desc_part.strip()
                param_docs.append(RpcParamDoc(name=name, description=desc))
                current_param_index = len(param_docs) - 1
            elif current_param_index is not None:
                existing = param_docs[current_param_index]
                desc = f"{existing.description} {line}".strip()
                param_docs[current_param_index] = RpcParamDoc(
                    name=existing.name,
                    description=desc,
                )
            continue
        if section == "returns":
            return_lines.append(line)
            continue
        description_lines.append(line)
    description = " ".join(description_lines).strip()
    return_doc = " ".join(return_lines).strip()
    return RpcDocSpec(
        summary=summary,
        description=description,
        param_docs=tuple(param_docs),
        return_doc=return_doc,
    )


def _build_param_specs(
    handler: Callable[..., Any],
    *,
    param_docs: dict[RpcParamName, RpcDocString],
) -> tuple[RpcParamSpec, ...]:
    signature = inspect.signature(handler)
    hints = _safe_type_hints(handler)
    param_specs: list[RpcParamSpec] = []
    for param in signature.parameters.values():
        if param.kind is inspect.Parameter.POSITIONAL_ONLY:
            kind = RpcParamKind.POSITIONAL_ONLY
        elif param.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD:
            kind = RpcParamKind.POSITIONAL_OR_KEYWORD
        elif param.kind is inspect.Parameter.VAR_POSITIONAL:
            kind = RpcParamKind.VAR_POSITIONAL
        elif param.kind is inspect.Parameter.KEYWORD_ONLY:
            kind = RpcParamKind.KEYWORD_ONLY
        else:
            kind = RpcParamKind.VAR_KEYWORD
        annotation = hints.get(param.name, param.annotation)
        base_annotation, annotation_doc = _split_annotated(annotation)
        type_spec = _type_spec_from_annotation(base_annotation)
        default_spec = _default_spec_from_value(param.default)
        required = not default_spec.has_default and kind not in (
            RpcParamKind.VAR_POSITIONAL,
            RpcParamKind.VAR_KEYWORD,
        )
        doc = param_docs.get(param.name, "") or annotation_doc
        param_specs.append(
            RpcParamSpec(
                name=param.name,
                kind=kind,
                type_spec=type_spec,
                required=required,
                default=default_spec,
                doc=doc,
            )
        )
    return tuple(param_specs)


def _build_return_spec(
    handler: Callable[..., Any],
    *,
    return_doc: RpcDocString,
) -> RpcReturnSpec:
    hints = _safe_type_hints(handler)
    annotation = hints.get("return", inspect.Signature.empty)
    base_annotation, annotation_doc = _split_annotated(annotation)
    return RpcReturnSpec(
        type_spec=_type_spec_from_annotation(base_annotation),
        doc=return_doc or annotation_doc or "",
    )


def _split_annotated(annotation: Any) -> tuple[Any, RpcDocString]:
    origin = get_origin(annotation)
    if origin is Annotated:
        args = get_args(annotation)
        if args:
            base = args[0]
            metadata = args[1:]
            doc = _annotation_doc(metadata)
            return base, doc
    return annotation, ""


def _annotation_doc(metadata: tuple[Any, ...]) -> RpcDocString:
    for item in metadata:
        if isinstance(item, RpcParamDoc):
            return item.description
        if isinstance(item, str):
            return item
    return ""


def _safe_type_hints(handler: Callable[..., Any]) -> dict[str, Any]:
    try:
        return get_type_hints(handler, include_extras=True)
    except TypeError:
        try:
            return get_type_hints(handler)
        except Exception:
            return dict(getattr(handler, "__annotations__", {}) or {})
    except Exception:
        return dict(getattr(handler, "__annotations__", {}) or {})


def _type_spec_from_annotation(annotation: Any) -> RpcTypeSpec:
    if annotation is inspect.Signature.empty or annotation is inspect._empty:
        annotation = Any
    if annotation is None:
        annotation = type(None)
    origin = get_origin(annotation)
    args = get_args(annotation)
    display = _render_annotation(annotation)
    module = _module_name(annotation, origin)
    qualname = _qualname(annotation, origin)
    origin_name = _origin_name(origin)
    arg_display = tuple(_render_annotation(arg) for arg in args)
    is_optional = _is_optional_annotation(annotation)
    return RpcTypeSpec(
        display=display,
        module=module,
        qualname=qualname,
        origin=origin_name,
        args=arg_display,
        is_optional=is_optional,
    )


def _default_spec_from_value(value: Any) -> RpcDefaultSpec:
    if value is inspect.Signature.empty or value is inspect._empty:
        return RpcDefaultSpec(has_default=False, encoding=RpcDefaultEncoding.NONE)
    json_value = _jsonable_default(value)
    if json_value is not None or value is None:
        return RpcDefaultSpec(
            has_default=True,
            encoding=RpcDefaultEncoding.JSON,
            json_value=json_value,
        )
    return RpcDefaultSpec(
        has_default=True,
        encoding=RpcDefaultEncoding.REPR,
        repr_value=repr(value),
    )


def _jsonable_default(value: Any) -> JsonValue | None:
    if value is None:
        return None
    if isinstance(value, Enum):
        return _jsonable_default(value.value)
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, tuple):
        value = list(value)
    if isinstance(value, list):
        return [
            item
            if isinstance(item, (str, int, float, bool, type(None)))
            else repr(item)
            for item in value
        ]
    if isinstance(value, (set, frozenset)):
        items = sorted(value, key=lambda item: str(item))
        return [
            item
            if isinstance(item, (str, int, float, bool, type(None)))
            else repr(item)
            for item in items
        ]
    if isinstance(value, dict):
        out: dict[str, JsonValue] = {}
        for key, item in sorted(value.items(), key=lambda entry: str(entry[0])):
            key_text = str(key)
            if isinstance(item, (str, int, float, bool, type(None))):
                out[key_text] = item
            else:
                out[key_text] = repr(item)
        return out
    return None


def _render_annotation(annotation: Any) -> str:
    if annotation is Any:
        return "Any"
    if annotation is type(None):
        return "None"
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is None:
        name = getattr(annotation, "__name__", None)
        return name or str(annotation)
    if origin in (list, tuple, dict, set, frozenset):
        origin_name = _origin_name(origin) or str(origin)
        if not args:
            return f"{origin_name}[]"
        if origin is dict and len(args) == 2:
            return f"{origin_name}[{_render_annotation(args[0])}, {_render_annotation(args[1])}]"
        return f"{origin_name}[{', '.join(_render_annotation(arg) for arg in args)}]"
    if origin in (Callable, AbcCallable):
        if args:
            return f"Callable[{', '.join(_render_annotation(arg) for arg in args)}]"
        return "Callable"
    if _is_union_origin(origin):
        rendered = " | ".join(_render_annotation(arg) for arg in args)
        return rendered
    return str(annotation)


def _is_union_origin(origin: Any) -> bool:
    union_type = getattr(types, "UnionType", None)
    return origin is Union or (union_type is not None and origin is union_type)


def _is_optional_annotation(annotation: Any) -> bool:
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is None:
        return False
    if not _is_union_origin(origin):
        return False
    return any(arg is type(None) for arg in args)


def _origin_name(origin: Any) -> RpcTypeName | None:
    if origin is None:
        return None
    name = getattr(origin, "__name__", None)
    return name or str(origin)


def _module_name(annotation: Any, origin: Any) -> str | None:
    module = getattr(annotation, "__module__", None)
    if module:
        return module
    return getattr(origin, "__module__", None)


def _qualname(annotation: Any, origin: Any) -> str | None:
    qualname = getattr(annotation, "__qualname__", None)
    if qualname:
        return qualname
    return getattr(origin, "__qualname__", None)
