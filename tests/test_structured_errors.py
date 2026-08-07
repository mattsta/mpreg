from mpreg.core.errors import (
    MpregErrorCode,
    command_not_found,
    hop_budget_exceeded,
    map_exception,
    policy_denied,
    route_loop_detected,
    version_mismatch,
)


def test_error_codes_stable() -> None:
    assert int(MpregErrorCode.COMMAND_NOT_FOUND) == 1001
    assert int(MpregErrorCode.VERSION_MISMATCH) == 1002
    assert int(MpregErrorCode.HOP_BUDGET_EXCEEDED) == 1003
    assert int(MpregErrorCode.ROUTE_LOOP) == 1013


def test_helpers_build_rpc_error() -> None:
    err = command_not_found("foo")
    assert err.rpc_error.code == 1001
    assert "foo" in (err.rpc_error.details or "")

    err = version_mismatch("math.add", ">=2.0.0")
    assert err.code == 1002

    err = hop_budget_exceeded(3)
    assert err.code == 1003
    assert err.retryable is False

    err = policy_denied("namespace blocked")
    assert err.code == 1004

    err = route_loop_detected(node_id="ws://a")
    assert err.code == 1013
    assert err.retryable is False


def test_map_timeout() -> None:
    mapped = map_exception(TimeoutError("deadline"))
    assert mapped is not None
    assert mapped.code == int(MpregErrorCode.TIMEOUT)
    assert mapped.retryable


def test_map_connection_and_text_hints() -> None:
    mapped = map_exception(ConnectionError("peer down"))
    assert mapped is not None
    assert mapped.code == int(MpregErrorCode.UNAVAILABLE)
    assert mapped.retryable

    mapped = map_exception(RuntimeError("No route to cluster-b"))
    assert mapped is not None
    assert mapped.code == int(MpregErrorCode.ROUTE_NOT_FOUND)
