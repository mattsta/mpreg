"""RPC-plane helpers: deadline stamping and structured response mapping."""

from __future__ import annotations

from typing import Any

from mpreg.core.errors import MpregError, MpregErrorCode
from mpreg.core.rpc_deadline import (
    DeadlineBudget,
    headers_with_deadline_seconds,
)
from mpreg.fabric.message import MessageHeaders
from mpreg.server_pkg.rpc_responses import error_response, timeout_response


class RpcPlane:
    """Helpers shared by server RPC entry points."""

    @staticmethod
    def stamp_deadline(
        headers: MessageHeaders, deadline_seconds: float | None
    ) -> MessageHeaders:
        return headers_with_deadline_seconds(headers, deadline_seconds)

    @staticmethod
    def check_deadline(headers: MessageHeaders) -> None:
        budget = DeadlineBudget.from_headers(headers)
        if budget is not None:
            budget.raise_if_exhausted()

    @staticmethod
    def timeout_if_exhausted(u: str, headers: MessageHeaders) -> Any | None:
        budget = DeadlineBudget.from_headers(headers)
        if budget is not None and budget.exhausted():
            return timeout_response(u, "deadline exhausted before execution")
        return None

    @staticmethod
    def map_error(u: str, exc: BaseException) -> Any:
        from mpreg.server_pkg.rpc_responses import from_exception

        return from_exception(u, exc)

    @staticmethod
    def hop_budget_exceeded(u: str) -> Any:
        return error_response(
            u,
            MpregError.of(
                MpregErrorCode.HOP_BUDGET_EXCEEDED,
                details="fabric hop budget exceeded",
            ),
        )
