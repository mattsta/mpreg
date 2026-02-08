from mpreg.core.rpc_registry import RpcRegistry
from mpreg.datastructures.rpc_spec import RpcRegistration
from mpreg.fabric.adapters.function_registry import LocalFunctionCatalogAdapter
from mpreg.fabric.catalog import TransportEndpoint


def test_function_registry_adapter_builds_delta() -> None:
    registry = RpcRegistry()

    def echo(payload: str) -> str:
        return payload

    implementation = RpcRegistration.from_callable(
        echo,
        name="echo",
        function_id="func-echo",
        version="1.0.0",
        resources=("cpu",),
    )
    registry.register(implementation)

    adapter = LocalFunctionCatalogAdapter(
        registry=registry,
        node_id="node-1",
        cluster_id="cluster-a",
        node_resources=frozenset({"gpu"}),
        node_capabilities=frozenset({"rpc"}),
        function_ttl_seconds=42.0,
        transport_endpoints=(
            TransportEndpoint(
                connection_type="internal",
                protocol="ws",
                host="127.0.0.1",
                port=9001,
            ),
        ),
    )
    delta = adapter.build_delta(now=100.0, include_node=True, update_id="update-1")

    assert delta.cluster_id == "cluster-a"
    assert delta.update_id == "update-1"
    assert len(delta.functions) == 1
    endpoint = delta.functions[0]
    assert endpoint.identity == implementation.spec.identity
    assert endpoint.node_id == "node-1"
    assert endpoint.cluster_id == "cluster-a"
    assert endpoint.resources == frozenset({"cpu"})
    assert endpoint.ttl_seconds == 42.0
    assert delta.nodes[0].resources == frozenset({"gpu"})
    assert delta.nodes[0].capabilities == frozenset({"rpc"})
    assert delta.nodes[0].transport_endpoints == adapter.transport_endpoints
