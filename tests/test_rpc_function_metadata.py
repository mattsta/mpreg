from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
from mpreg.core.rpc_registry import RpcRegistry
from mpreg.datastructures.rpc_spec import RpcRegistration
from mpreg.fabric.adapters.function_registry import LocalFunctionCatalogAdapter
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
    registry = RpcRegistry()

    def update(payload: str) -> str:
        return payload

    implementation = RpcRegistration.from_callable(
        update,
        name="update",
        function_id="func-update",
        version="1.2.3",
        resources=("db",),
    )
    registry.register(implementation)
    adapter = LocalFunctionCatalogAdapter(
        registry=registry,
        node_id="node-1",
        cluster_id="test-cluster",
        node_resources=frozenset({"db"}),
    )
    delta = adapter.build_delta(now=100.0, include_node=True)

    assert len(delta.functions) == 1
    endpoint = delta.functions[0]
    assert endpoint.identity.function_id == "func-update"
    assert str(endpoint.identity.version) == "1.2.3"
    assert endpoint.rpc_summary is not None
    assert endpoint.spec_digest == implementation.spec.spec_digest


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
