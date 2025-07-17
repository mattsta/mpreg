from mpreg.model import (
    CommandNotFoundException,
    GossipMessage,
    PeerInfo,
    RPCCommand,
    RPCError,
    RPCRequest,
)


def test_rpc_command_creation() -> None:
    cmd = RPCCommand(
        name="test_cmd",
        fun="my_func",
        args=("arg1", 123),
        kwargs={"key": "value"},
        locs=frozenset(["loc1"]),
    )
    assert cmd.name == "test_cmd"
    assert cmd.fun == "my_func"
    assert cmd.args == ("arg1", 123)
    assert cmd.kwargs == {"key": "value"}
    assert cmd.locs == frozenset(["loc1"])


def test_rpc_request_creation() -> None:
    cmd = RPCCommand(name="test_cmd", fun="my_func")
    req = RPCRequest(cmds=(cmd,), u="test_uuid")
    assert req.role == "rpc"
    assert req.cmds == (cmd,)
    assert req.u == "test_uuid"


def test_rpc_error_creation() -> None:
    error = RPCError(code=100, message="Test Error", details={"info": "some_detail"})
    assert error.code == 100
    assert error.message == "Test Error"
    assert error.details == {"info": "some_detail"}


def test_command_not_found_error_creation() -> None:
    error = CommandNotFoundException(command_name="non_existent_cmd", details=None)
    assert error.code == 1001
    assert error.message == "Command not found"
    assert error.command_name == "non_existent_cmd"


def test_peer_info_creation() -> None:
    peer = PeerInfo(
        url="ws://localhost:8000",
        funs=("echo",),
        locs=frozenset(["loc1"]),
        last_seen=123.45,
        cluster_id="test_cluster",
    )
    assert peer.url == "ws://localhost:8000"
    assert peer.funs == ("echo",)
    assert peer.locs == frozenset(["loc1"])
    assert peer.last_seen == 123.45
    assert peer.cluster_id == "test_cluster"


def test_gossip_message_creation() -> None:
    peer = PeerInfo(
        url="ws://localhost:8000",
        funs=("echo",),
        locs=frozenset(["loc1"]),
        last_seen=123.45,
        cluster_id="test_cluster",
    )
    gossip = GossipMessage(peers=(peer,), u="gossip_uuid", cluster_id="test_cluster")
    assert gossip.role == "gossip"
    assert gossip.peers == (peer,)
    assert gossip.u == "gossip_uuid"
    assert gossip.cluster_id == "test_cluster"
