from __future__ import annotations

import re
from dataclasses import dataclass

from .type_aliases import FunctionId, FunctionName, FunctionVersion

_VERSION_RE = re.compile(r"^v?\d+(\.\d+){0,2}$")


@dataclass(frozen=True, order=True, slots=True)
class SemanticVersion:
    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, raw: FunctionVersion) -> SemanticVersion:
        value = raw.strip()
        if not _VERSION_RE.match(value):
            raise ValueError(f"Invalid semantic version: {raw}")
        value = value.removeprefix("v")
        parts = value.split(".")
        numbers = [int(part) for part in parts]
        while len(numbers) < 3:
            numbers.append(0)
        if len(numbers) != 3:
            raise ValueError(f"Invalid semantic version: {raw}")
        return cls(numbers[0], numbers[1], numbers[2])

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    def to_dict(self) -> dict[str, int]:
        return {"major": self.major, "minor": self.minor, "patch": self.patch}

    @classmethod
    def from_dict(cls, payload: dict[str, int]) -> SemanticVersion:
        return cls(
            int(payload["major"]),
            int(payload["minor"]),
            int(payload["patch"]),
        )


@dataclass(frozen=True, slots=True)
class VersionConstraint:
    min_version: SemanticVersion | None = None
    max_version: SemanticVersion | None = None
    include_min: bool = True
    include_max: bool = True
    exact_version: SemanticVersion | None = None

    @classmethod
    def parse(cls, raw: str) -> VersionConstraint:
        spec = raw.strip()
        if not spec:
            return cls()
        spec = spec.strip("()")
        spec = spec.replace("version", "").strip()
        if not spec:
            return cls()
        parts = [part.strip() for part in spec.split(",") if part.strip()]
        constraint = cls()
        for part in parts:
            match = re.match(r"^(>=|<=|==|=|<|>)(.+)$", part)
            if not match:
                version = SemanticVersion.parse(part)
                constraint = constraint.with_exact(version)
                continue
            op, value = match.groups()
            version = SemanticVersion.parse(value.strip())
            if op in ("=", "=="):
                constraint = constraint.with_exact(version)
            elif op == ">=":
                constraint = constraint.with_min(version, include=True)
            elif op == ">":
                constraint = constraint.with_min(version, include=False)
            elif op == "<=":
                constraint = constraint.with_max(version, include=True)
            elif op == "<":
                constraint = constraint.with_max(version, include=False)
        return constraint

    def with_exact(self, version: SemanticVersion) -> VersionConstraint:
        if (
            self.min_version is not None
            or self.max_version is not None
            or self.exact_version is not None
        ):
            raise ValueError("Exact version cannot be combined with range")
        return VersionConstraint(exact_version=version)

    def with_min(self, version: SemanticVersion, *, include: bool) -> VersionConstraint:
        if self.exact_version is not None:
            raise ValueError("Min version cannot be combined with exact version")
        if self.min_version is not None and version < self.min_version:
            raise ValueError("Min version would be downgraded")
        return VersionConstraint(
            min_version=version,
            max_version=self.max_version,
            include_min=include,
            include_max=self.include_max,
        )

    def with_max(self, version: SemanticVersion, *, include: bool) -> VersionConstraint:
        if self.exact_version is not None:
            raise ValueError("Max version cannot be combined with exact version")
        if self.max_version is not None and version > self.max_version:
            raise ValueError("Max version would be upgraded")
        return VersionConstraint(
            min_version=self.min_version,
            max_version=version,
            include_min=self.include_min,
            include_max=include,
        )

    def matches(self, version: SemanticVersion) -> bool:
        if self.exact_version is not None:
            return version == self.exact_version
        if self.min_version is not None:
            if version < self.min_version:
                return False
            if version == self.min_version and not self.include_min:
                return False
        if self.max_version is not None:
            if version > self.max_version:
                return False
            if version == self.max_version and not self.include_max:
                return False
        return True

    def to_dict(self) -> dict[str, str | bool | None]:
        return {
            "min_version": str(self.min_version) if self.min_version else None,
            "max_version": str(self.max_version) if self.max_version else None,
            "include_min": self.include_min,
            "include_max": self.include_max,
            "exact_version": str(self.exact_version) if self.exact_version else None,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, str | bool | None]) -> VersionConstraint:
        min_raw = payload.get("min_version")
        max_raw = payload.get("max_version")
        exact_raw = payload.get("exact_version")
        if exact_raw:
            return cls(exact_version=SemanticVersion.parse(str(exact_raw)))
        min_version = SemanticVersion.parse(str(min_raw)) if min_raw else None
        max_version = SemanticVersion.parse(str(max_raw)) if max_raw else None
        return cls(
            min_version=min_version,
            max_version=max_version,
            include_min=bool(payload.get("include_min", True)),
            include_max=bool(payload.get("include_max", True)),
        )


@dataclass(frozen=True, slots=True)
class FunctionIdentity:
    name: FunctionName
    function_id: FunctionId
    version: SemanticVersion

    def to_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "function_id": self.function_id,
            "version": str(self.version),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, str]) -> FunctionIdentity:
        return cls(
            name=payload["name"],
            function_id=payload["function_id"],
            version=SemanticVersion.parse(payload["version"]),
        )


@dataclass(frozen=True, slots=True)
class FunctionSelector:
    name: FunctionName | None = None
    function_id: FunctionId | None = None
    version_constraint: VersionConstraint | None = None

    def matches(self, identity: FunctionIdentity) -> bool:
        if self.function_id and self.function_id != identity.function_id:
            return False
        if self.name and self.name != identity.name:
            return False
        return not (
            self.version_constraint
            and not self.version_constraint.matches(identity.version)
        )
