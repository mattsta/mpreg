from mpreg.core.errors import MpregErrorCode
from mpreg.server_pkg.rpc_responses import (
    internal_response,
    protocol_response,
    timeout_response,
)


def test_rpc_responses_codes() -> None:
    assert timeout_response("u1", "slow").error.code == int(MpregErrorCode.TIMEOUT)
    assert internal_response("u2", "x").error.code == int(MpregErrorCode.INTERNAL)
    assert protocol_response("u3", "bad").error.code == int(MpregErrorCode.PROTOCOL)
