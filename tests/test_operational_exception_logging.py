"""Operator-correct logging for expected vs unexpected caught exceptions."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from mpreg.core.errors import (
    CONDITION_EXCEPTIONS,
    EXPECTED_EXCEPTIONS,
    OPERATIONAL_EXCEPTIONS,
    is_expected_failure,
    log_caught_exception,
    with_operational,
)


@dataclass
class _FakeLog:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    exceptions: list[str] = field(default_factory=list)
    opt_errors: list[tuple[BaseException | None, str]] = field(default_factory=list)

    def error(self, msg: str, *args: object) -> None:
        if args:
            try:
                msg = msg % args
            except TypeError, ValueError:
                msg = f"{msg} {args}"
        self.errors.append(str(msg))

    def warning(self, msg: str, *args: object) -> None:
        self.warnings.append(str(msg))

    def exception(self, msg: str, *args: object) -> None:
        self.exceptions.append(str(msg))

    def opt(self, *, exception: BaseException | bool | None = None):
        parent = self

        class _Opt:
            def error(self, msg: str, *args: object) -> None:
                parent.opt_errors.append(
                    (
                        exception if isinstance(exception, BaseException) else None,
                        str(msg),
                    )
                )
                parent.errors.append(str(msg))

        return _Opt()


def test_categories_partition_operational() -> None:
    assert set(OPERATIONAL_EXCEPTIONS) == set(EXPECTED_EXCEPTIONS) | set(
        CONDITION_EXCEPTIONS
    )
    assert TimeoutError in EXPECTED_EXCEPTIONS
    assert ValueError in CONDITION_EXCEPTIONS
    assert OSError in EXPECTED_EXCEPTIONS


def test_is_expected_failure_classifies() -> None:
    from mpreg.core.transport.interfaces import (
        TransportConnectionError,
        TransportTimeoutError,
    )

    assert is_expected_failure(TimeoutError("t"))
    assert is_expected_failure(ConnectionError("c"))
    assert is_expected_failure(ValueError("v"))
    assert is_expected_failure(KeyError("k"))
    assert is_expected_failure(RuntimeError("r"))
    # Transport errors subclass ConnectionError / TimeoutError (operational).
    assert is_expected_failure(TransportConnectionError("WebSocket connection closed"))
    assert is_expected_failure(TransportTimeoutError("read timed out"))
    assert isinstance(TransportConnectionError("x"), ConnectionError)
    assert isinstance(TransportTimeoutError("x"), TimeoutError)
    # Unknown programmer/fault classes are not "expected conditions"
    assert not is_expected_failure(AttributeError("a"))
    assert not is_expected_failure(ArithmeticError("z"))


def test_log_caught_expected_is_message_only() -> None:
    log = _FakeLog()
    log_caught_exception(log, "cache put failed", ValueError("bad"), level="error")
    assert log.errors
    assert "cache put failed" in log.errors[0]
    assert "bad" in log.errors[0]
    assert log.exceptions == []
    assert log.opt_errors == []


def test_log_caught_expected_warning_level() -> None:
    log = _FakeLog()
    log_caught_exception(
        log, "peer dial soft fail", ConnectionError("reset"), level="warning"
    )
    assert log.warnings
    assert log.errors == []
    assert log.exceptions == []


def test_log_caught_unexpected_emits_stack_path() -> None:
    log = _FakeLog()
    exc = AttributeError("missing")
    log_caught_exception(log, "supervisor boundary", exc, expected=False)
    # loguru-style opt(exception=).error OR exception()
    assert log.opt_errors or log.exceptions
    if log.opt_errors:
        assert log.opt_errors[0][0] is exc
        assert "supervisor boundary" in log.opt_errors[0][1]


def test_log_caught_auto_classifies_unexpected() -> None:
    log = _FakeLog()
    log_caught_exception(log, "mystery", AttributeError("x"))
    assert log.opt_errors or log.exceptions


def test_with_operational_dedupes() -> None:
    class DomainError(Exception):
        pass

    composed = with_operational(DomainError, ValueError)
    assert DomainError in composed
    assert composed.count(ValueError) == 1
    assert TimeoutError in composed


def test_operational_catch_matches_isinstance() -> None:
    from mpreg.core.transport.interfaces import (
        TransportConnectionError,
        TransportTimeoutError,
    )

    for cls in (
        OSError,
        TimeoutError,
        ConnectionError,
        ValueError,
        RuntimeError,
        TransportConnectionError,
        TransportTimeoutError,
    ):
        try:
            raise cls("x")
        except OPERATIONAL_EXCEPTIONS as exc:
            assert isinstance(exc, cls)
        else:
            pytest.fail(f"{cls} not caught by OPERATIONAL_EXCEPTIONS")


def test_dual_catch_log_helper() -> None:
    from mpreg.core.errors import dual_catch_log

    log = _FakeLog()
    dual_catch_log(log, "ops fail", TimeoutError("t"))
    assert log.errors and "ops fail" in log.errors[0]
    assert log.exceptions == []
    log2 = _FakeLog()
    dual_catch_log(log2, "bug", AttributeError("x"))
    assert log2.exceptions or log2.opt_errors


def test_pre_vote_request_does_not_use_expected_stack_path() -> None:
    """Sanity: RequestVote pre_vote field round-trips via codec."""
    from mpreg.datastructures.production_raft import RequestVoteRequest
    from mpreg.datastructures.raft_codec import (
        deserialize_request_vote,
        serialize_request_vote,
    )

    req = RequestVoteRequest(
        term=3,
        candidate_id="c1",
        last_log_index=1,
        last_log_term=2,
        pre_vote=True,
    )
    data = serialize_request_vote(req)
    assert data.get("pre_vote") is True
    back = deserialize_request_vote(data)
    assert back.pre_vote is True
    legacy = deserialize_request_vote(
        {
            "term": 1,
            "candidate_id": "c",
            "last_log_index": 0,
            "last_log_term": 0,
        }
    )
    assert legacy.pre_vote is False
