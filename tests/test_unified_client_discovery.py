"""Phase P/R: MPREGClient exposes full discovery/DNS/namespace surface."""

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
        "summary_watch",
        "rpc_list",
        "rpc_describe",
        "rpc_report",
        "dns_register",
        "dns_unregister",
        "dns_list",
        "dns_describe",
        "resolver_cache_stats",
        "resolver_resync",
        "discovery_access_audit",
        "namespace_status",
        "namespace_policy_export",
        "namespace_policy_validate",
        "namespace_policy_apply",
        "namespace_policy_audit",
        "last_trace_context",
    ):
        assert hasattr(c, name), name
        assert callable(getattr(c, name)), name

def test_unified_client_api_async_parity() -> None:
    """Every public async API method is on the unified façade (Phase R)."""
    import inspect
    from mpreg.client.client_api import MPREGClientAPI

    skip = {
        "connect",
        "disconnect",
        "__aenter__",
        "__aexit__",
    }
    api_methods = {
        name
        for name, fn in inspect.getmembers(MPREGClientAPI, predicate=inspect.isfunction)
        if not name.startswith("_") and inspect.iscoroutinefunction(fn)
    }
    # also async def on class
    for name, fn in inspect.getmembers(MPREGClientAPI, predicate=inspect.iscoroutinefunction):
        if not name.startswith("_"):
            api_methods.add(name)

    # Collect from source more reliably
    import ast
    from pathlib import Path

    tree = ast.parse(Path(inspect.getfile(MPREGClientAPI)).read_text())
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "MPREGClientAPI":
            for item in node.body:
                if isinstance(item, ast.AsyncFunctionDef) and not item.name.startswith("_"):
                    if item.name not in skip:
                        api_methods.add(item.name)

    uni = MPREGClient(url="ws://127.0.0.1:1")
    missing = sorted(m for m in api_methods if m not in skip and not hasattr(uni, m))
    assert not missing, f"MPREGClient missing API methods: {missing}"
