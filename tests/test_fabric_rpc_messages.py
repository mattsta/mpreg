from mpreg.fabric.rpc_messages import (
    FABRIC_RPC_REQUEST_KIND,
    FABRIC_RPC_RESPONSE_KIND,
    FabricRPCRequest,
    FabricRPCResponse,
)


def test_fabric_rpc_request_roundtrip() -> None:
    request = FabricRPCRequest(
        request_id="req-1",
        command="echo",
        args=(1, "payload"),
        kwargs={"flag": True},
        resources=("cpu",),
        function_id="func-echo",
        version_constraint=">=1.0.0",
        target_cluster="cluster-b",
        target_node="ws://node-b",
        reply_to="ws://node-a",
        federation_path=("cluster-a",),
        federation_remaining_hops=2,
    )

    payload = request.to_dict()
    assert payload["kind"] == FABRIC_RPC_REQUEST_KIND

    parsed = FabricRPCRequest.from_dict(payload)
    assert parsed == request


def test_fabric_rpc_response_roundtrip() -> None:
    response = FabricRPCResponse(
        request_id="req-2",
        success=True,
        result={"ok": True},
        responder="ws://node-b",
        reply_to="ws://node-a",
        reply_cluster="cluster-a",
        routing_path=("ws://node-a", "ws://node-b"),
    )

    payload = response.to_dict()
    assert payload["kind"] == FABRIC_RPC_RESPONSE_KIND

    parsed = FabricRPCResponse.from_dict(payload)
    assert parsed == response
