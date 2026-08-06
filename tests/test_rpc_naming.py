"""Unit tests for FQN RPC naming + namespace deny / bound-namespace policy."""

from __future__ import annotations

import pytest

from mpreg.core.rpc_naming import (
    DEFAULT_USER_NAMESPACE,
    PLATFORM_NAMESPACE_ROOT,
    PlatformRpc,
    assert_call_allowed,
    assert_registration_allowed,
    is_platform_namespace,
    is_under_namespace,
    leaf_name,
    namespace_of,
    qualify_rpc_name,
)

def test_qualify_bare_uses_default_namespace() -> None:
    assert qualify_rpc_name("add") == f"{DEFAULT_USER_NAMESPACE}.add"
    assert qualify_rpc_name("add", "orders") == "orders.add"
    assert qualify_rpc_name("add", "app.orders") == "app.orders.add"

def test_qualify_explicit_fqn_unchanged() -> None:
    assert qualify_rpc_name("orders.create") == "orders.create"
    assert qualify_rpc_name(PlatformRpc.ECHO) == PlatformRpc.ECHO
    assert qualify_rpc_name("mpreg.system.echo", "app") == "mpreg.system.echo"

def test_namespace_deny_blocks_user_mpreg_registration() -> None:
    with pytest.raises(ValueError, match="reserved platform"):
        assert_registration_allowed("mpreg.evil.inject")
    with pytest.raises(ValueError, match="reserved platform"):
        assert_registration_allowed(PLATFORM_NAMESPACE_ROOT)
    with pytest.raises(ValueError, match="reserved platform"):
        assert_registration_allowed("mpreg")

def test_namespace_deny_allows_platform_path() -> None:
    assert_registration_allowed(PlatformRpc.ECHO, allow_platform=True)
    assert_registration_allowed("mpreg.custom.tool", allow_platform=True)

def test_user_namespaces_fully_flexible() -> None:
    for name in (
        "app.add",
        "orders.create",
        "tenant.a.b.c",
        "echo",  # bare leaf is fine once qualified outside mpreg
        "com.example.svc.ping",
    ):
        fqn = qualify_rpc_name(name) if "." not in name else name
        if is_platform_namespace(fqn):
            continue
        assert_registration_allowed(fqn)

def test_bound_namespace_registration() -> None:
    assert_registration_allowed("app.orders.create", bound_namespace="app")
    assert_registration_allowed("app.orders.create", bound_namespace="app.orders")
    with pytest.raises(ValueError, match="outside bound namespace"):
        assert_registration_allowed("app.billing.charge", bound_namespace="app.orders")
    with pytest.raises(ValueError, match="outside bound namespace"):
        assert_registration_allowed("other.add", bound_namespace="app")

def test_bound_namespace_qualifies_bare_under_bound() -> None:
    """Operator lock-in: bare names prepend the bound prefix, not free default."""
    bound = "app.orders"
    assert qualify_rpc_name("create", bound) == "app.orders.create"
    assert_registration_allowed(
        qualify_rpc_name("create", bound), bound_namespace=bound
    )

def test_bound_namespace_call_allows_platform() -> None:
    assert_call_allowed(PlatformRpc.LIST_PEERS, bound_namespace="app.orders")
    assert_call_allowed("app.orders.create", bound_namespace="app.orders")
    with pytest.raises(ValueError, match="outside bound namespace"):
        assert_call_allowed("app.billing.x", bound_namespace="app.orders")
    with pytest.raises(ValueError, match="outside bound namespace"):
        assert_call_allowed(
            PlatformRpc.ECHO,
            allow_platform=False,
            bound_namespace="app.orders",
        )

def test_is_under_namespace_hierarchy() -> None:
    assert is_under_namespace("app.add", "app")
    assert is_under_namespace("app.orders.create", "app.orders")
    assert is_under_namespace("app", "app")
    assert not is_under_namespace("app.add", "app.orders")
    assert not is_under_namespace("mpreg.system.echo", "app")
    assert is_under_namespace("mpreg.system.echo", "mpreg")

def test_leaf_and_namespace_of() -> None:
    assert leaf_name("app.orders.create") == "create"
    assert namespace_of("app.orders.create") == "app.orders"
    assert leaf_name("echo") == "echo"
    assert namespace_of("echo") == ""

def test_platform_rpc_constants_under_mpreg() -> None:
    for attr in dir(PlatformRpc):
        if attr.startswith("_"):
            continue
        value = getattr(PlatformRpc, attr)
        if isinstance(value, str):
            assert is_platform_namespace(value), value
            assert value.startswith(f"{PLATFORM_NAMESPACE_ROOT}.")

def test_inbound_qualify_preserves_opaque_function_id() -> None:
    """function_id is a capability id — never namespace-qualify bare opaque ids."""
    from mpreg.core.config import MPREGSettings
    from mpreg.core.model import RPCCommand
    from mpreg.server import MPREGServer

    server = MPREGServer(
        MPREGSettings(
            host="127.0.0.1",
            port=19998,
            name="qualify-unit",
            cluster_id="c",
            monitoring_enabled=False,
        )
    )
    cmd = RPCCommand(
        name="step",
        fun="mesh_function",
        args=(),
        locs=frozenset(),
        function_id="mesh-function-id",
        version_constraint=">=2.0.0,<3.0.0",
    )
    out = server._qualify_inbound_rpc_command(cmd)
    assert out.fun == "app.mesh_function"
    assert out.function_id == "mesh-function-id"

    # omitted function_id stays None (name-only routing)
    bare = RPCCommand(name="step", fun="mesh_function", args=(), locs=frozenset())
    out_bare = server._qualify_inbound_rpc_command(bare)
    assert out_bare.fun == "app.mesh_function"
    assert out_bare.function_id is None

    # function_id still equal to pre-qualify bare fun → align to FQN
    tied = RPCCommand(
        name="step",
        fun="mesh_function",
        args=(),
        locs=frozenset(),
        function_id="mesh_function",
    )
    out_tied = server._qualify_inbound_rpc_command(tied)
    assert out_tied.function_id == "app.mesh_function"
