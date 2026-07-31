from mpreg.server_pkg.openapi_surface import build_monitoring_openapi

def test_openapi_has_core_paths() -> None:
    doc = build_monitoring_openapi()
    assert doc["openapi"].startswith("3.")
    paths = doc["paths"]
    for p in (
        "/health",
        "/metrics/prometheus",
        "/routing/decisions",
        "/mgmt/v1/cluster",
        "/openapi.json",
        "/mgmt/v1/nodes/drain",
    ):
        assert p in paths
