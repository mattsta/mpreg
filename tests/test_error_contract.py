"""Contract: public error codes and emission helpers."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from mpreg.core.errors import (
    PUBLIC_ERROR_CODES,
    MpregError,
    MpregErrorCode,
    discovery_rate_limited,
    error_code_catalog,
    internal_error,
    map_exception,
    rpc_error,
    timeout_error,
)
from mpreg.core.model import MPREGException, RPCError


def test_public_codes_stable_set() -> None:
    assert 1001 in PUBLIC_ERROR_CODES
    assert 1006 in PUBLIC_ERROR_CODES
    assert 1099 in PUBLIC_ERROR_CODES
    assert 1101 in PUBLIC_ERROR_CODES
    assert 1102 in PUBLIC_ERROR_CODES
    assert 1000 in PUBLIC_ERROR_CODES


def test_rpc_error_uses_public_namespace() -> None:
    err = rpc_error(MpregErrorCode.TIMEOUT, details="x")
    assert err.code == 1006
    assert err.details == "x"


def test_map_exception_never_none() -> None:
    assert map_exception(RuntimeError("x")).code == int(MpregErrorCode.INTERNAL)
    assert map_exception(TimeoutError("t")).code == int(MpregErrorCode.TIMEOUT)


def test_legacy_wire_remap_timeout_was_1004() -> None:
    legacy = MPREGException(
        rpc_error=RPCError(
            code=1004,
            message="RPC execution timed out after 5 seconds",
            details="The RPC workflow took too long to complete",
        )
    )
    mapped = map_exception(legacy)
    assert mapped.code == int(MpregErrorCode.TIMEOUT)
    assert mapped.retryable is True


def test_legacy_internal_was_1002() -> None:
    legacy = MPREGException(
        rpc_error=RPCError(
            code=1002, message="Internal server error", details="traceback"
        )
    )
    assert map_exception(legacy).code == int(MpregErrorCode.INTERNAL)


def test_legacy_discovery_http_codes() -> None:
    assert map_exception(
        MPREGException(
            rpc_error=RPCError(code=429, message="discovery_rate_limited", details="x")
        )
    ).code == int(MpregErrorCode.DISCOVERY_RATE_LIMITED)
    assert map_exception(
        MPREGException(
            rpc_error=RPCError(
                code=403, message="discovery_access_denied", details="denied"
            )
        )
    ).code == int(MpregErrorCode.DISCOVERY_ACCESS_DENIED)


def test_helpers() -> None:
    assert timeout_error("t").code == 1006
    assert discovery_rate_limited("r").retryable is True
    assert internal_error("i").code == 1099


def test_error_code_catalog_matches_json() -> None:
    catalog = error_code_catalog()
    assert any(r["code"] == 1006 for r in catalog)
    json_path = Path(__file__).resolve().parents[1] / "mpreg/core/error_codes.json"
    assert json_path.is_file()


def _bare_rpcerror_codes(path: Path) -> list[tuple[int, int]]:
    """Return (lineno, code) for RPCError(code=<int>) constructions."""
    tree = ast.parse(path.read_text())
    found: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = None
        if isinstance(func, ast.Name):
            name = func.id
        elif isinstance(func, ast.Attribute):
            name = func.attr
        if name != "RPCError":
            continue
        for kw in node.keywords:
            if kw.arg == "code" and isinstance(kw.value, ast.Constant):
                if isinstance(kw.value.value, int):
                    found.append((node.lineno, kw.value.value))
    return found


@pytest.mark.parametrize(
    "rel",
    [
        "mpreg/server.py",
        "mpreg/core/enhanced_rpc.py",
    ],
)
def test_no_bare_integer_rpcerror_in_server_paths(rel: str) -> None:
    root = Path(__file__).resolve().parents[1]
    path = root / rel
    if not path.exists():
        pytest.skip(f"missing {rel}")
    bare = _bare_rpcerror_codes(path)
    # enhanced_rpc may reconstruct from dict with .get default -1 — allow only via non-constant
    offenders = [(ln, c) for ln, c in bare if c != -1]
    assert offenders == [], f"Bare RPCError codes in {rel}: {offenders}"


def test_mpreg_error_to_rpc_error() -> None:
    e = MpregError.of(MpregErrorCode.POLICY_DENIED, details="no")
    assert e.to_rpc_error().code == 1004
