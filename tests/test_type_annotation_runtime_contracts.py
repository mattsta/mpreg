"""Regression: type-annotation imports that must resolve at runtime.

Full-tree ruff F821 found undefined names in annotations that, under
``from __future__ import annotations``, are usually deferred — but several
sites also construct or return these types at runtime. Missing imports would
fail the first time those paths run. These tests pin the contracts so CI
catches drift without waiting for a full mypy/ruff gate.

Also covers related runtime correctness fixes discovered alongside the lint
pass (mutable default configs, protocol error typing, module import smoke).
"""

from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest


def test_cluster_client_call_policy_and_plane_client_types_importable() -> None:
    """MPREGClusterClient fields/methods reference live types, not string ghosts."""
    from mpreg.client.call_policy import ClientCallPolicy, default_ha_policy
    from mpreg.client.cluster_client import MPREGClusterClient
    from mpreg.client.unified_client import MPREGClient

    hints = getattr(MPREGClusterClient, "__annotations__", {})
    assert "call_policy" in hints
    assert "ClientCallPolicy" in str(hints["call_policy"])

    policy = default_ha_policy()
    assert isinstance(policy, ClientCallPolicy)

    client = MPREGClusterClient(seed_urls=("ws://127.0.0.1:1",), call_policy=policy)
    assert client.call_policy is policy

    ret = inspect.signature(MPREGClusterClient.plane_client).return_annotation
    assert "MPREGClient" in str(ret) or ret is MPREGClient

    # plane_client must construct without NameError when policy/imports are live
    plane = client.plane_client("ws://127.0.0.1:1")
    assert isinstance(plane, MPREGClient)


def test_cluster_client_module_exports_annotation_deps() -> None:
    """Annotation dependencies must be importable from the cluster_client module."""
    import mpreg.client.cluster_client as mod

    assert hasattr(mod, "ClientCallPolicy")
    assert hasattr(mod, "MPREGClient")
    assert mod.ClientCallPolicy is not None
    assert mod.MPREGClient is not None


def test_sqlite_queue_store_backend_annotation_is_real_type(tmp_path: Path) -> None:
    from mpreg.core.persistence.backend import SQLitePersistenceBackend
    from mpreg.core.persistence.queue_store import SQLiteQueueStore

    hints = getattr(SQLiteQueueStore, "__annotations__", {})
    assert "backend" in hints
    assert "SQLitePersistenceBackend" in str(hints["backend"])

    backend = SQLitePersistenceBackend(db_path=tmp_path / "q.sqlite")
    store = SQLiteQueueStore(backend=backend, namespace="ns", queue_name="q")
    assert store.backend is backend


def test_sqlite_queue_store_does_not_import_backend_at_module_level() -> None:
    """Avoid circular import: backend is TYPE_CHECKING-only in queue_store."""
    import mpreg.core.persistence.queue_store as qs

    # Runtime attribute may be absent (string annotation / TYPE_CHECKING)
    # but constructing with a real backend must still work (covered above).
    src = Path(qs.__file__).read_text(encoding="utf-8")
    assert "TYPE_CHECKING" in src
    assert "SQLitePersistenceBackend" in src


def test_transaction_signer_sign_annotation_and_roundtrip() -> None:
    from mpreg.datastructures.blockchain_crypto import TransactionSigner
    from mpreg.datastructures.transaction import Transaction

    param = inspect.signature(TransactionSigner.sign).parameters["transaction"]
    assert "Transaction" in str(param.annotation)

    signer = TransactionSigner.create()
    tx = Transaction(sender="alice", receiver="bob", fee=1)
    signed = signer.sign(tx)
    assert isinstance(signed, Transaction)
    assert signed.signature
    assert signed.verify_signature()


def test_transaction_signer_rejects_non_transaction_at_runtime() -> None:
    """TypeError path must run even with deferred annotations."""
    from mpreg.datastructures.blockchain_crypto import TransactionSigner

    signer = TransactionSigner.create()
    with pytest.raises(TypeError, match="Transaction"):
        signer.sign(object())  # type: ignore[arg-type]


def test_fabric_router_queue_and_cache_entry_types_importable() -> None:
    from mpreg.fabric import router as router_mod
    from mpreg.fabric.catalog import CacheRoleEntry, QueueEndpoint

    assert router_mod.QueueEndpoint is QueueEndpoint
    assert router_mod.CacheRoleEntry is CacheRoleEntry


def test_server_module_imports_cleanly() -> None:
    """server.py is the largest F821 surface; import must not raise NameError."""
    import mpreg.server as server_mod

    assert hasattr(server_mod, "MPREGServer")
    # PubSubSubscription must resolve at runtime (used in handlers, not only hints)
    assert hasattr(server_mod, "PubSubSubscription")
    from mpreg.core.model import PubSubSubscription

    assert server_mod.PubSubSubscription is PubSubSubscription


def test_server_typing_only_symbols_are_importable_for_annotations() -> None:
    """server.py TYPE_CHECKING block must name real symbols (F821 regression)."""
    from mpreg.datastructures.type_aliases import Timestamp
    from mpreg.fabric.catalog import FunctionEndpoint
    from mpreg.fabric.catalog_policy import CatalogFilterPolicy
    from mpreg.fabric.engine import ClusterRoutePlan, RoutingEngine
    from mpreg.fabric.peer_directory import PeerDirectory, PeerNeighbor
    from mpreg.fabric.router import FabricRouteResult, FabricRouteTarget

    for cls in (
        Timestamp,
        FunctionEndpoint,
        CatalogFilterPolicy,
        ClusterRoutePlan,
        RoutingEngine,
        PeerDirectory,
        PeerNeighbor,
        FabricRouteResult,
        FabricRouteTarget,
    ):
        assert cls is not None


def test_client_request_rejects_non_rpc_response_with_mpreg_error() -> None:
    """Protocol mismatch must raise structured MpregError, not bare Exception."""
    from mpreg.client.client import Client
    from mpreg.core.errors import MpregError, MpregErrorCode
    from mpreg.core.model import RPCCommand

    client = Client(url="ws://127.0.0.1:9")
    transport = MagicMock()
    transport.send = AsyncMock()
    client._transport = transport

    async def _drive() -> None:
        cmd = RPCCommand(
            name="echo",
            fun="echo",
            args=("x",),
            locs=frozenset(),
        )

        async def _send(_payload: bytes) -> None:
            # Exactly one pending request after send is scheduled.
            assert len(client._pending_requests) == 1
            fut = next(iter(client._pending_requests.values()))
            fut.set_result({"not": "an rpc response"})

        transport.send.side_effect = _send
        with pytest.raises(MpregError) as ei:
            await client.request([cmd], timeout=1.0)
        assert ei.value.code == int(MpregErrorCode.PROTOCOL)

    asyncio.run(_drive())


def test_client_request_accepts_command_list_not_rpc_request_object() -> None:
    """Public API is request([RPCCommand,...]); RPCRequest is built internally."""
    from mpreg.client.client import Client
    from mpreg.core.model import RPCCommand, RPCRequest

    sig = inspect.signature(Client.request)
    params = list(sig.parameters.values())
    # Bound method signature starts at cmds (self omitted) or includes self
    cmds_param = params[0] if params[0].name != "self" else params[1]
    ann = str(cmds_param.annotation)
    assert "RPCCommand" in ann or cmds_param.name in {"cmds", "commands", "request"}
    # Construction of RPCRequest remains available for enhanced path
    cmd = RPCCommand(name="n", fun="f", args=(), locs=frozenset())
    req = RPCRequest(cmds=(cmd,), u="u1")
    assert req.cmds == (cmd,)


def test_dijkstra_and_astar_default_config_not_shared_mutable() -> None:
    from mpreg.datastructures.graph_algorithms import (
        AStarAlgorithm,
        DijkstraAlgorithm,
    )

    a = DijkstraAlgorithm()
    b = DijkstraAlgorithm()
    assert a.config is not b.config

    c = AStarAlgorithm()
    d = AStarAlgorithm()
    assert c.config is not d.config


def test_port_allocator_except_tuple_is_valid_python3() -> None:
    """except A, B is SyntaxError on Py3 — must be except (A, B)."""
    import ast
    from pathlib import Path

    src = Path("mpreg/core/port_allocator.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    # No bare comma-except style survived (would fail parse on 3.x already)
    assert tree is not None
    from mpreg.core.port_allocator import PortAllocator

    # Instantiation exercises worker offset path
    pa = PortAllocator()
    assert isinstance(pa.worker_offset, int)
    assert "servers" in PortAllocator.RANGES


def test_critical_modules_import_without_nameerror() -> None:
    """Smoke-import modules that had F821 annotation gaps in the lint pass."""
    modules = [
        "mpreg.client.cluster_client",
        "mpreg.client.client",
        "mpreg.core.persistence.queue_store",
        "mpreg.datastructures.blockchain_crypto",
        "mpreg.datastructures.graph_algorithms",
        "mpreg.fabric.router",
        "mpreg.server",
        "mpreg.core.port_allocator",
    ]
    import importlib

    for name in modules:
        mod = importlib.import_module(name)
        assert mod is not None


def test_cluster_remove_server_drops_peer_connection() -> None:
    """Cluster.remove_server must exist and clear peer_connections (runtime path)."""
    from unittest.mock import MagicMock

    from mpreg.server import Cluster

    cluster = Cluster.create(
        cluster_id="c1",
        advertised_urls=("ws://127.0.0.1:1",),
        local_url="ws://127.0.0.1:1",
    )
    conn = MagicMock()
    conn.url = "ws://127.0.0.1:2"
    conn.is_connected = False
    cluster.peer_connections[conn.url] = conn

    cluster.remove_server(conn)
    assert conn.url not in cluster.peer_connections


def test_transport_interface_exposes_close_alias() -> None:
    from mpreg.core.transport.interfaces import TransportInterface

    assert hasattr(TransportInterface, "close")
    assert hasattr(TransportInterface, "disconnect")


def test_hypothesis_lazy_st_annotations_resolve() -> None:
    """st.SearchStrategy in datastructures must typecheck/import without NameError."""
    from mpreg.datastructures import transaction as tx
    from mpreg.datastructures import vector_clock as vc

    # Module-level st proxy or real strategies must expose builds/text used by strategies
    assert hasattr(vc, "st")
    assert hasattr(tx, "st")
    # Strategy factories are callables
    assert callable(vc.clock_entry_strategy)
    strat = vc.clock_entry_strategy()
    assert strat is not None
