from mpreg.core.errors import MpregError, MpregErrorCode

def test_subscription_failure_is_mpreg_error() -> None:
    err = MpregError.of(MpregErrorCode.UNAVAILABLE, details="Subscription failed: x")
    assert err.code == 1007
    assert err.retryable is True
