import pytest

from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    FunctionSelector,
    SemanticVersion,
    VersionConstraint,
)


def test_semantic_version_parsing() -> None:
    assert str(SemanticVersion.parse("1")) == "1.0.0"
    assert str(SemanticVersion.parse("1.2")) == "1.2.0"
    assert str(SemanticVersion.parse("v1.2.3")) == "1.2.3"


@pytest.mark.parametrize("raw", ["", "1.2.3.4", "1.x", "v", "1..2"])
def test_semantic_version_invalid(raw: str) -> None:
    with pytest.raises(ValueError):
        SemanticVersion.parse(raw)


def test_version_constraint_exact() -> None:
    constraint = VersionConstraint.parse("==3.2.1")
    assert constraint.matches(SemanticVersion.parse("3.2.1"))
    assert not constraint.matches(SemanticVersion.parse("3.2.2"))


def test_version_constraint_range() -> None:
    constraint = VersionConstraint.parse("version >= 3.0, version < 4")
    assert constraint.matches(SemanticVersion.parse("3.0.0"))
    assert constraint.matches(SemanticVersion.parse("3.9.9"))
    assert not constraint.matches(SemanticVersion.parse("4.0.0"))


def test_version_constraint_default_allows_any() -> None:
    constraint = VersionConstraint.parse("")
    assert constraint.matches(SemanticVersion.parse("1.0.0"))


def test_function_selector_matching() -> None:
    identity = FunctionIdentity(
        name="update",
        function_id="func-123",
        version=SemanticVersion.parse("3.2.1"),
    )
    selector = FunctionSelector(name="update")
    assert selector.matches(identity)
    selector = FunctionSelector(function_id="func-123")
    assert selector.matches(identity)
    selector = FunctionSelector(
        function_id="func-123",
        version_constraint=VersionConstraint.parse(">=3.0,<4"),
    )
    assert selector.matches(identity)
    selector = FunctionSelector(function_id="other-id")
    assert not selector.matches(identity)


def test_roundtrip_serialization() -> None:
    identity = FunctionIdentity(
        name="update",
        function_id="func-xyz",
        version=SemanticVersion.parse("1.2.3"),
    )
    payload = identity.to_dict()
    loaded = FunctionIdentity.from_dict(payload)
    assert loaded == identity


def test_version_constraint_roundtrip() -> None:
    constraint = VersionConstraint.parse(">=1.0,<2.0")
    payload = constraint.to_dict()
    loaded = VersionConstraint.from_dict(payload)
    assert loaded.matches(SemanticVersion.parse("1.5.0"))
    assert not loaded.matches(SemanticVersion.parse("2.0.0"))
