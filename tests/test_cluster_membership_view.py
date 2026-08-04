"""Cluster.servers is function-catalog; known_node_ids is membership."""

from __future__ import annotations

from mpreg.server import Cluster

def test_known_node_ids_empty_without_engine_or_directory() -> None:
    cluster = Cluster.create(
        cluster_id="c1",
        advertised_urls=("ws://127.0.0.1:9001",),
        local_url="ws://127.0.0.1:9001",
    )
    assert cluster.servers == set()
    assert cluster.known_node_ids == set()

def test_servers_doc_distinguishes_membership() -> None:
    doc = Cluster.servers.__doc__ or ""
    assert "function-catalog" in doc or "advertise" in doc.lower()
    mem_doc = Cluster.known_node_ids.__doc__ or ""
    assert "membership" in mem_doc.lower()
