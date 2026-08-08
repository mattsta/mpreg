"""Catalog snapshot serialize-once / send-many gates.

Matches the proof style of ``tests/test_native_codec.py`` (latency + invariant
counters). Closes the unfinished half of the Aug 2026 native_codec hang series:
N peers must not force N full catalog ``to_dict`` + wire encodes on ttl=0 flush.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

import pytest

from mpreg.core.model import FabricGossipEnvelope
from mpreg.core.rpc_spec_sharing import RpcSpecSharePolicy
from mpreg.core.serialization import JsonSerializer
from mpreg.datastructures.function_identity import FunctionIdentity, SemanticVersion
from mpreg.datastructures.rpc_spec import RpcSpec
from mpreg.fabric.catalog import FunctionEndpoint, NodeDescriptor, RoutingCatalog
from mpreg.fabric.catalog_delta import RoutingCatalogApplier, RoutingCatalogDelta
from mpreg.fabric.gossip import GossipMessage, GossipMessageType
from mpreg.server_pkg.types import CatalogSnapshotDispatchState


def _make_spec(i: int) -> RpcSpec:
    def _handler(x: int = 0) -> int:
        return x

    return RpcSpec.from_callable(_handler, name=f"fn-{i}", namespace="demo")


def _endpoint(
    i: int, *, with_spec: bool = False, now: float | None = None
) -> FunctionEndpoint:
    advertised = time.time() if now is None else now
    if with_spec:
        spec = _make_spec(i)
        return FunctionEndpoint(
            identity=spec.identity,
            resources=spec.resources or frozenset({"cpu"}),
            node_id="ws://node-a",
            cluster_id="cluster-a",
            rpc_summary=spec.summary(),
            rpc_spec=spec,
            spec_digest=spec.spec_digest,
            advertised_at=advertised,
            ttl_seconds=3600.0,
        )
    identity = FunctionIdentity(
        name=f"fn-{i}",
        function_id=f"id-{i}",
        version=SemanticVersion.parse("1.0.0"),
    )
    return FunctionEndpoint(
        identity=identity,
        resources=frozenset({"cpu"}),
        node_id="ws://node-a",
        cluster_id="cluster-a",
        advertised_at=advertised,
        ttl_seconds=3600.0,
    )


def _fill_catalog(n_functions: int, *, with_spec: bool = False) -> RoutingCatalog:
    catalog = RoutingCatalog()
    now = time.time()
    for i in range(n_functions):
        catalog.functions.register(_endpoint(i, with_spec=with_spec, now=now), now=now)
    catalog.nodes.register(
        NodeDescriptor(
            node_id="ws://node-a",
            cluster_id="cluster-a",
            resources=frozenset({"cpu"}),
            capabilities=frozenset({"rpc"}),
            advertised_at=now,
            ttl_seconds=3600.0,
        ),
        now=now,
    )
    # Direct register does not go through applier; bump once for snapshot id.
    catalog.bump_generation()
    return catalog


@dataclass
class _FakeGossip:
    node_id: str = "ws://node-a"
    vector_clock: Any = field(default_factory=lambda: MagicMock())
    _seq: int = 0

    def __post_init__(self) -> None:
        self.vector_clock.copy.return_value = MagicMock()
        self.vector_clock.to_dict.return_value = {}

    def next_sequence_number(self) -> int:
        self._seq += 1
        return self._seq


@dataclass
class _FakeControlPlane:
    catalog: RoutingCatalog
    gossip: _FakeGossip = field(default_factory=_FakeGossip)


@dataclass
class _RecordingTransport:
    sent: list[bytes] = field(default_factory=list)

    async def send_preencoded(self, peer_id: str, data: bytes) -> bool:
        self.sent.append(data)
        return True

    async def send_bytes(self, peer_id: str, data: bytes) -> bool:
        self.sent.append(data)
        return True


@dataclass
class _SnapshotServer:
    """Minimal stand-in exercising MPREGServer snapshot helpers."""

    settings: Any
    cluster: Any
    serializer: JsonSerializer
    _fabric_control_plane: _FakeControlPlane
    _fabric_gossip_transport: _RecordingTransport
    _catalog_snapshot_dispatch: CatalogSnapshotDispatchState
    _rpc_spec_share_policy: RpcSpecSharePolicy
    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)

    # Bind real implementations from MPREGServer
    _catalog_snapshot_include_rpc_spec = None  # type: ignore[assignment]
    _build_shared_catalog_snapshot_wire = None  # type: ignore[assignment]
    _send_shared_catalog_snapshot_bytes = None  # type: ignore[assignment]
    _flush_catalog_snapshots = None  # type: ignore[assignment]
    _schedule_catalog_snapshot = None  # type: ignore[assignment]


def _bind_server_methods(server: _SnapshotServer) -> None:
    from mpreg.server import MPREGServer

    server._catalog_snapshot_include_rpc_spec = (  # type: ignore[method-assign]
        MPREGServer._catalog_snapshot_include_rpc_spec.__get__(server, _SnapshotServer)
    )
    server._build_shared_catalog_snapshot_wire = (  # type: ignore[method-assign]
        MPREGServer._build_shared_catalog_snapshot_wire.__get__(server, _SnapshotServer)
    )
    server._send_shared_catalog_snapshot_bytes = (  # type: ignore[method-assign]
        MPREGServer._send_shared_catalog_snapshot_bytes.__get__(server, _SnapshotServer)
    )
    server._flush_catalog_snapshots = (  # type: ignore[method-assign]
        MPREGServer._flush_catalog_snapshots.__get__(server, _SnapshotServer)
    )
    server._schedule_catalog_snapshot = (  # type: ignore[method-assign]
        MPREGServer._schedule_catalog_snapshot.__get__(server, _SnapshotServer)
    )


def _make_server(
    catalog: RoutingCatalog,
    *,
    peers: list[str],
    mode: str = "summary",
) -> _SnapshotServer:
    settings = MagicMock()
    settings.cluster_id = "cluster-a"
    settings.name = "test-node"
    cluster = MagicMock()
    cluster.local_url = "ws://node-a"
    server = _SnapshotServer(
        settings=settings,
        cluster=cluster,
        serializer=JsonSerializer(),
        _fabric_control_plane=_FakeControlPlane(catalog=catalog),
        _fabric_gossip_transport=_RecordingTransport(),
        _catalog_snapshot_dispatch=CatalogSnapshotDispatchState(),
        _rpc_spec_share_policy=RpcSpecSharePolicy(mode=mode),
    )
    _bind_server_methods(server)
    for peer in peers:
        server._catalog_snapshot_dispatch.pending_peers.add(peer)
    return server


@pytest.mark.asyncio
async def test_flush_builds_catalog_payload_once_for_many_peers() -> None:
    catalog = _fill_catalog(200)
    peers = [f"ws://peer-{i}" for i in range(20)]
    server = _make_server(catalog, peers=peers)

    await server._flush_catalog_snapshots()  # type: ignore[misc]

    dispatch = server._catalog_snapshot_dispatch
    assert dispatch.flush_batches == 1
    assert dispatch.peers_flushed == 20
    assert dispatch.payload_builds == 1, dispatch.payload_builds
    assert dispatch.to_dict_calls == 1, dispatch.to_dict_calls
    assert dispatch.wire_encodes == 1, dispatch.wire_encodes
    assert dispatch.bytes_sends == 20, dispatch.bytes_sends
    assert dispatch.last_update_id == f"catalog-rev:cluster-a:{catalog.generation}"
    sent = server._fabric_gossip_transport.sent
    assert len(sent) == 20
    assert len(set(sent)) == 1  # identical bytes to every peer


@pytest.mark.asyncio
async def test_flush_latency_much_less_than_n_times_single_encode() -> None:
    """Wall time for N-peer flush must not scale like N full encodes."""
    catalog = _fill_catalog(1500)
    # Baseline: one build cost
    server_one = _make_server(catalog, peers=["ws://peer-0"])
    t0 = time.perf_counter()
    await server_one._flush_catalog_snapshots()  # type: ignore[misc]
    single_dt = time.perf_counter() - t0
    assert single_dt < 0.5, f"single flush too slow: {single_dt:.3f}s"

    peers = [f"ws://peer-{i}" for i in range(20)]
    server_n = _make_server(catalog, peers=peers)
    t1 = time.perf_counter()
    await server_n._flush_catalog_snapshots()  # type: ignore[misc]
    multi_dt = time.perf_counter() - t1

    # Serialize-once: multi-peer should be within a small factor of single,
    # not ~20×. Allow headroom for fan-out loop + yields.
    assert multi_dt < max(0.15, single_dt * 3.0), (
        f"multi flush {multi_dt:.3f}s vs single {single_dt:.3f}s "
        f"(ratio {multi_dt / max(single_dt, 1e-9):.1f}x) — looks like per-peer rebuild"
    )
    dispatch = server_n._catalog_snapshot_dispatch
    assert dispatch.payload_builds == 1
    assert dispatch.wire_encodes == 1


def test_function_endpoint_to_dict_strips_rpc_spec_when_disabled() -> None:
    ep = _endpoint(0, with_spec=True)
    assert ep.rpc_spec is not None
    full = ep.to_dict(include_rpc_spec=True)
    summary = ep.to_dict(include_rpc_spec=False)
    assert "rpc_spec" in full
    assert "rpc_spec" not in summary
    assert "rpc_summary" in summary
    assert "spec_digest" in summary


def test_delta_to_dict_summary_mode_omits_nested_rpc_spec() -> None:
    ep = _endpoint(1, with_spec=True)
    delta = RoutingCatalogDelta(
        update_id="u1",
        cluster_id="cluster-a",
        functions=(ep,),
    )
    payload = delta.to_dict(include_rpc_spec=False)
    assert payload["functions"]
    assert "rpc_spec" not in payload["functions"][0]
    assert "rpc_summary" in payload["functions"][0]


@pytest.mark.asyncio
async def test_shared_snapshot_wire_omits_rpc_spec_under_summary_policy() -> None:
    catalog = _fill_catalog(5, with_spec=True)
    server = _make_server(catalog, peers=["ws://peer-1"], mode="summary")
    built = server._build_shared_catalog_snapshot_wire()  # type: ignore[misc]
    assert built is not None
    wire_bytes, update_id, generation = built
    assert update_id.startswith("catalog-rev:cluster-a:")
    assert generation == catalog.generation
    envelope = JsonSerializer().deserialize(wire_bytes)
    assert envelope["role"] == "fabric-gossip"
    gossip_payload = envelope["payload"]
    functions = gossip_payload["payload"]["functions"]
    assert functions
    assert "rpc_spec" not in functions[0]
    assert "rpc_summary" in functions[0] or "spec_digest" in functions[0]


@pytest.mark.asyncio
async def test_shared_snapshot_wire_includes_rpc_spec_under_full_policy() -> None:
    catalog = _fill_catalog(3, with_spec=True)
    server = _make_server(catalog, peers=["ws://peer-1"], mode="full")
    built = server._build_shared_catalog_snapshot_wire()  # type: ignore[misc]
    assert built is not None
    wire_bytes, _, _ = built
    envelope = JsonSerializer().deserialize(wire_bytes)
    functions = envelope["payload"]["payload"]["functions"]
    assert "rpc_spec" in functions[0]


def test_stable_update_id_dedupes_second_apply() -> None:
    catalog_a = RoutingCatalog()
    applier_a = RoutingCatalogApplier(catalog_a)
    now = time.time()
    ep = _endpoint(0, now=now)
    delta = RoutingCatalogDelta(
        update_id="catalog-rev:cluster-a:1",
        cluster_id="cluster-a",
        sent_at=now,
        functions=(ep,),
    )
    c1 = applier_a.apply(delta, now=now)
    assert c1.get("functions_added", 0) == 1
    c2 = applier_a.apply(delta, now=now + 1.0)
    assert c2.get("skipped_duplicate_update_id") == 1


def test_applier_bumps_catalog_generation() -> None:
    catalog = RoutingCatalog()
    assert catalog.generation == 0
    applier = RoutingCatalogApplier(catalog)
    now = time.time()
    applier.apply(
        RoutingCatalogDelta(
            update_id="u-gen-1",
            cluster_id="cluster-a",
            sent_at=now,
            functions=(_endpoint(0, now=now),),
        ),
        now=now,
    )
    assert catalog.generation == 1


def test_gossip_message_accepts_prebuilt_dict_payload_without_re_to_dict() -> None:
    """Publisher path stores dict payload; outer to_dict must not re-walk typed delta."""
    payload = {"update_id": "x", "functions": [{"name": f"f{i}"} for i in range(500)]}
    t0 = time.perf_counter()
    msg = GossipMessage(
        message_id="m1",
        message_type=GossipMessageType.CATALOG_UPDATE,
        sender_id="ws://a",
        payload=payload,
        sequence_number=1,
        ttl=0,
        max_hops=0,
    )
    outer = msg.to_dict()
    dt = time.perf_counter() - t0
    assert outer["payload"] is payload or outer["payload"] == payload
    assert dt < 0.05, f"to_dict too slow: {dt:.3f}s"


def test_serialize_model_and_send_bytes_roundtrip_shape() -> None:
    ser = JsonSerializer()
    env = FabricGossipEnvelope(payload={"update_id": "u", "k": 1})
    raw = ser.serialize_model(env)
    back = ser.deserialize(raw)
    assert back["role"] == "fabric-gossip"
    assert back["payload"]["update_id"] == "u"
