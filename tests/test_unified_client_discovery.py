"""Phase P: MPREGClient exposes discovery surface (list_peers/cluster_map/catalog)."""

from mpreg.client.unified_client import MPREGClient

def test_unified_client_has_discovery_methods() -> None:
    c = MPREGClient(url="ws://127.0.0.1:1")
    for name in (
        "list_peers",
        "cluster_map",
        "cluster_map_v2",
        "catalog_query",
        "catalog_watch",
        "summary_query",
        "last_trace_context",
    ):
        assert hasattr(c, name), name
        assert callable(getattr(c, name)), name
