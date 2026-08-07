import pytest

from mpreg.client.call_policy import ClientCallPolicy, call_with_policy
from mpreg.core.errors import MpregError, MpregErrorCode


@pytest.mark.asyncio
async def test_retry_on_timeout() -> None:
    attempts = {"n": 0}

    async def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise TimeoutError("slow")
        return "ok"

    policy = ClientCallPolicy(
        max_attempts=5, base_backoff_seconds=0.0, jitter_seconds=0.0
    )
    result = await call_with_policy(flaky, policy)
    assert result == "ok"
    assert attempts["n"] == 3


@pytest.mark.asyncio
async def test_no_retry_on_command_not_found() -> None:
    async def boom() -> None:
        raise MpregError.of(MpregErrorCode.COMMAND_NOT_FOUND, details="x")

    policy = ClientCallPolicy(
        max_attempts=5, base_backoff_seconds=0.0, jitter_seconds=0.0
    )
    with pytest.raises(MpregError) as ei:
        await call_with_policy(boom, policy)
    assert ei.value.code == 1001
