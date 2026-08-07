"""T42 residual closeout: client/RPC strong_retry_abort surface."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from mpreg.client import CacheOpResult, StrongRetryAbortResult
from mpreg.client.unified_client import MPREGClient
from mpreg.core.cache_models import GlobalCacheKey
from mpreg.core.cache_strong import (
    InProcessStrongTransport,
    StrongLocalBackend,
    StrongPutCoordinator,
    _entry_op_id,
)
from mpreg.core.global_cache import GlobalCacheManager
from mpreg.core.rpc_naming import PlatformRpc
from mpreg.server_pkg.plane_rpc import (
    PLANE_ERR_INVALID_ARGUMENT,
    PLANE_ERR_UNSUPPORTED_CONSISTENCY,
    cache_strong_retry_abort,
    register_cache_rpc_commands,
)

def test_t42_platform_rpc_name() -> None:
    assert PlatformRpc.CACHE_STRONG_RETRY_ABORT == "mpreg.cache.strong_retry_abort"
    assert "strong_retry_abort" in PlatformRpc.CACHE_STRONG_RETRY_ABORT

def test_t42_cache_op_result_promotes_quorum_info() -> None:
    r = CacheOpResult.from_raw(
        {
            "success": False,
            "error_code": 1016,
            "operation_id": "oid-1",
            "quorum_info": {
                "abort_fail_peers": ["n1"],
                "abort_best_effort_residual_candidates": True,
            },
        }
    )
    assert r.success is False
    assert r.operation_id == "oid-1"
    assert r.quorum_info is not None
    assert "n1" in list(r.quorum_info.get("abort_fail_peers") or [])

def test_t42_strong_retry_abort_result_from_raw() -> None:
    r = StrongRetryAbortResult.from_raw(
        {
            "success": True,
            "cleared": True,
            "ok_peers": ["n1"],
            "fail_peers": [],
            "op_id": "x",
            "attempts": 2,
            "ops_driven": True,
            "automatic_heal": False,
        }
    )
    assert r.success and r.cleared
    assert r.ok_peers == ["n1"]
    assert r.automatic_heal is False
    assert r.ops_driven is True

@pytest.mark.asyncio
async def test_t42_plane_handler_clears_residual() -> None:
    tr = InProcessStrongTransport()
    backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(5)}
    for be in backends.values():
        tr.register(be)
    tr.drop_commit |= {"n2", "n3", "n4"}
    tr.drop_abort |= {"n1"}
    coord = StrongPutCoordinator(
        origin_id="n0",
        local=backends["n0"],
        transport=tr,
        replica_factor=5,
        min_replicas=5,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.2,
        abort_attempts=2,
    )
    gcm = GlobalCacheManager.__new__(GlobalCacheManager)
    gcm._strong_coordinator = coord
    gcm._strong_backend = backends["n0"]
    gcm._strong_metrics = __import__("collections").defaultdict(int)
    gcm._strong_latency_ms = []
    gcm._strong_latency_max = 256

    key = GlobalCacheKey(namespace="t42", identifier="k", version="v1.0.0")
    res = await coord.strong_put(key, {"v": 1}, eligible_peers=list(backends))
    assert res.success is False
    oid = res.operation_id or ""
    assert backends["n1"].get_visible(key) is not None
    tr.drop_abort.clear()
    tr.drop_commit.clear()

    class _Srv:
        _cache_manager = gcm
        settings = type("S", (), {"cluster_id": "c1"})()

    out = await cache_strong_retry_abort(
        _Srv(),
        {
            "namespace": "t42",
            "identifier": "k",
            "version": "v1.0.0",
            "op_id": oid,
        },
    )
    assert out.get("success") is True
    assert out.get("cleared") is True
    assert out.get("ops_driven") is True
    assert out.get("automatic_heal") is False
    assert out.get("cft_best_effort") is True
    ent = backends["n1"].get_visible(key)
    assert ent is None or _entry_op_id(ent) != oid

@pytest.mark.asyncio
async def test_t42_plane_handler_validation() -> None:
    class _Srv:
        _cache_manager = object()  # no strong_retry_abort
        settings = type("S", (), {"cluster_id": "c1"})()

    out = await cache_strong_retry_abort(
        _Srv(), {"namespace": "a", "identifier": "b", "op_id": "x"}
    )
    assert out.get("success") is False
    assert out.get("error_code") == PLANE_ERR_UNSUPPORTED_CONSISTENCY

    class _Srv2:
        _cache_manager = type(
            "M",
            (),
            {
                "strong_retry_abort": AsyncMock(
                    return_value={"cleared": True, "ok_peers": [], "fail_peers": []}
                )
            },
        )()
        settings = type("S", (), {"cluster_id": "c1"})()

    missing = await cache_strong_retry_abort(
        _Srv2(), {"namespace": "a", "identifier": "b"}
    )
    assert missing.get("error_code") == PLANE_ERR_INVALID_ARGUMENT
    assert "op_id" in str(missing.get("error_message") or "")

@pytest.mark.asyncio
async def test_t42_client_cache_strong_retry_abort() -> None:
    client = MPREGClient(url="ws://127.0.0.1:9")
    from mpreg.client.client_api import MPREGClientAPI

    captured: dict = {}

    async def fake_call(self, fun, *args, **kwargs):
        captured["fun"] = fun
        body = args[0] if args else kwargs.get("args") or {}
        if isinstance(body, dict):
            captured["body"] = body
        elif args and isinstance(args[0], dict):
            captured["body"] = args[0]
        else:
            # call(fun, body, timeout=...)
            captured["body"] = args[0] if args else {}
        return {
            "success": True,
            "cleared": True,
            "ok_peers": ["n1"],
            "fail_peers": [],
            "op_id": "oid-z",
            "attempts": 2,
            "ops_driven": True,
            "automatic_heal": False,
        }

    orig = MPREGClientAPI.call
    try:
        MPREGClientAPI.call = fake_call  # type: ignore[method-assign]
        object.__setattr__(client.api, "_connected", True)
        r = await client.cache_strong_retry_abort(
            "ns", "id", "oid-z", peers=["n1"], version="v1"
        )
        assert r.success and r.cleared
        assert r.ok_peers == ["n1"]
        assert r.automatic_heal is False
        assert captured.get("fun") == PlatformRpc.CACHE_STRONG_RETRY_ABORT
        body = captured.get("body") or {}
        assert body.get("namespace") == "ns"
        assert body.get("op_id") == "oid-z"
        assert body.get("peers") == ["n1"]
    finally:
        MPREGClientAPI.call = orig  # type: ignore[method-assign]

def test_t42_register_includes_retry_command() -> None:
    registered: list[str] = []

    class _Srv:
        _cache_rpc_registered = False

        def register_command(self, name, handler, resources, allow_platform=False):
            registered.append(str(name))

        async def _rpc_cache_get(self, *a, **k):
            return {}

        async def _rpc_cache_put(self, *a, **k):
            return {}

        async def _rpc_cache_invalidate(self, *a, **k):
            return {}

        async def _rpc_cache_strong_retry_abort(self, *a, **k):
            return {}

    register_cache_rpc_commands(_Srv())
    assert PlatformRpc.CACHE_STRONG_RETRY_ABORT in registered

def test_t42_docs_honesty() -> None:
    root = Path(__file__).resolve().parents[2]
    guide = (root / "docs" / "MPREG_CLIENT_GUIDE.md").read_text(encoding="utf-8")
    assert "cache_strong_retry_abort" in guide or "strong_retry_abort" in guide
    residual = (
        root / "docs" / "SHARED_AUDIT_STRONG_RESIDUAL_HONESTY.md"
    ).read_text(encoding="utf-8")
    assert "Phase 30" in residual or "client" in residual.lower()
    assert "RPC" in residual or "rpc" in residual.lower()
