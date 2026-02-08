import pytest

from mpreg.core.rpc_registry import RpcRegistry
from mpreg.datastructures.function_identity import FunctionSelector, VersionConstraint
from mpreg.datastructures.rpc_spec import (
    RpcDocSpec,
    RpcExampleSpec,
    RpcRegistration,
    RpcSpec,
)


def sample_func_1() -> str:
    return "func1_result"


def sample_func_2(arg1: str, arg2: str) -> str:
    return f"func2_result_{arg1}_{arg2}"


async def sample_async() -> str:
    return "async_result"


def test_rpc_implementation_call() -> None:
    impl = RpcRegistration.from_callable(
        sample_func_1, name="test_cmd", function_id="func-1", version="1.0.0"
    )
    assert impl() == "func1_result"


@pytest.mark.asyncio
async def test_rpc_implementation_call_async_handles_sync() -> None:
    impl = RpcRegistration.from_callable(
        sample_func_2, name="test_cmd_args", function_id="func-2", version="1.0.0"
    )
    assert await impl.call_async("a", "b") == "func2_result_a_b"


@pytest.mark.asyncio
async def test_rpc_implementation_call_async_handles_coroutine() -> None:
    impl = RpcRegistration.from_callable(
        sample_async, name="test_cmd_async", function_id="func-3", version="1.0.0"
    )
    assert await impl.call_async() == "async_result"


def test_rpc_registry_register_and_resolve() -> None:
    registry = RpcRegistry()
    impl = RpcRegistration.from_callable(
        sample_func_1, name="cmd1", function_id="func-1", version="1.0.0"
    )
    registry.register(impl)

    selector = FunctionSelector(name="cmd1")
    resolved = registry.resolve(selector)

    assert resolved is impl


def test_rpc_registry_selects_latest_matching_version() -> None:
    registry = RpcRegistry()
    registry.register(
        RpcRegistration.from_callable(
            sample_func_1, name="cmd1", function_id="func-1", version="1.0.0"
        )
    )
    registry.register(
        RpcRegistration.from_callable(
            sample_func_1, name="cmd1", function_id="func-1", version="1.2.0"
        )
    )

    selector = FunctionSelector(
        name="cmd1", version_constraint=VersionConstraint.parse(">=1.1.0")
    )
    resolved = registry.resolve(selector)

    assert resolved is not None
    assert str(resolved.spec.identity.version) == "1.2.0"


def test_rpc_registry_register_duplicate_version_raises() -> None:
    registry = RpcRegistry()
    impl = RpcRegistration.from_callable(
        sample_func_1, name="cmd1", function_id="func-1", version="1.0.0"
    )
    registry.register(impl)

    with pytest.raises(ValueError, match="already registered"):
        registry.register(impl)


def test_rpc_registry_resolve_missing_returns_none() -> None:
    registry = RpcRegistry()
    selector = FunctionSelector(name="missing")
    assert registry.resolve(selector) is None


def test_rpc_registry_register_callable_accepts_metadata() -> None:
    registry = RpcRegistry()

    def meta_func() -> str:
        return "ok"

    doc = RpcDocSpec(
        summary="meta summary", description="meta desc", param_docs=(), return_doc="ok"
    )
    example = RpcExampleSpec(
        name="basic",
        description="basic example",
        request={"payload": "ok"},
        response="ok",
    )

    registration = registry.register_callable(
        meta_func,
        name="svc.meta.func",
        function_id="meta.func",
        version="2.0.0",
        resources=("cpu",),
        tags=("fast",),
        namespace="svc.meta",
        scope="region",
        capabilities=("rpc",),
        doc=doc,
        examples=(example,),
    )

    spec = registration.spec
    assert spec.namespace == "svc.meta"
    assert spec.scope == "region"
    assert spec.resources == frozenset({"cpu"})
    assert spec.tags == frozenset({"fast"})
    assert "rpc" in spec.capabilities
    assert spec.doc.summary == "meta summary"
    assert spec.examples and spec.examples[0].name == "basic"


def test_rpc_spec_digest_stable_for_set_defaults() -> None:
    def with_set(items: set[int] = {2, 1}) -> int:
        return len(items)

    spec_a = RpcSpec.from_callable(
        with_set,
        name="svc.defaults.set",
        function_id="defaults.set",
        version="1.0.0",
    )
    spec_b = RpcSpec.from_callable(
        with_set,
        name="svc.defaults.set",
        function_id="defaults.set",
        version="1.0.0",
    )
    assert spec_a.spec_digest == spec_b.spec_digest
