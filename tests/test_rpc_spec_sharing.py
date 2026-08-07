from mpreg.core.rpc_spec_sharing import RpcSpecSharePolicy
from mpreg.datastructures.rpc_spec import RpcSpec


def test_rpc_spec_share_policy_namespace_boundary() -> None:
    def echo(payload: str) -> str:
        return payload

    svc_spec = RpcSpec.from_callable(
        echo,
        name="svc.alpha.echo",
        function_id="svc.alpha.echo",
        version="1.0.0",
    )
    svc2_spec = RpcSpec.from_callable(
        echo,
        name="svc2.alpha.echo",
        function_id="svc2.alpha.echo",
        version="1.0.0",
    )

    policy = RpcSpecSharePolicy(mode="full", namespaces=("svc",))
    assert policy.include_spec(svc_spec)
    assert not policy.include_spec(svc2_spec)
