from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
from mpreg.datastructures.function_identity import FunctionIdentity, SemanticVersion
from mpreg.fabric.adapters.function_registry import LocalFunctionCatalogAdapter
from mpreg.fabric.function_registry import LocalFunctionRegistry
from mpreg.server import MPREGServer
from tests.test_helpers import TestPortManager


def test_rpc_command_function_metadata_fields() -> None:
    cmd = RPCCommand(
        name="update",
        fun="update",
        args=("payload",),
        locs=frozenset({"db"}),
        function_id="func-update",
        version_constraint=">=1.0,<2.0",
    )
    payload = cmd.model_dump()
    assert payload["function_id"] == "func-update"
    assert payload["version_constraint"] == ">=1.0,<2.0"


def test_function_catalog_adapter_emits_identity() -> None:
    registry = LocalFunctionRegistry(
        node_id="node-1",
        cluster_id="test-cluster",
        ttl_seconds=30.0,
    )
    identity = FunctionIdentity(
        name="update",
        function_id="func-update",
        version=SemanticVersion.parse("1.2.3"),
    )
    registry.register(identity, resources=frozenset({"db"}), now=100.0)
    adapter = LocalFunctionCatalogAdapter(
        registry=registry,
        node_resources=frozenset({"db"}),
    )
    delta = adapter.build_delta(now=100.0, include_node=True)

    assert len(delta.functions) == 1
    endpoint = delta.functions[0]
    assert endpoint.identity.function_id == "func-update"
    assert str(endpoint.identity.version) == "1.2.3"


def test_register_command_conflicting_function_id() -> None:
    with TestPortManager() as port_manager:
        server_port = port_manager.get_server_port()
        settings = MPREGSettings(
            host="127.0.0.1",
            port=server_port,
            name="TestServer",
            cluster_id="test-cluster",
        )
        server = MPREGServer(settings=settings)

        def sample_func() -> str:
            return "ok"

        server.register_command(
            "update", sample_func, ["resource-1"], function_id="func-A", version="1.0.0"
        )

        try:
            server.register_command(
                "update",
                sample_func,
                ["resource-1"],
                function_id="func-B",
                version="1.0.0",
            )
        except ValueError:
            return
        raise AssertionError("Expected conflict on function_id mismatch")
