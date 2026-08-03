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
        "/mgmt/v1/peers/detach",
        "/mgmt/v1/policy/apply",
        "/mgmt/v1/audit",
        "/live",
        "/ready",
    ):
        assert p in paths

def test_openapi_mgmt_mutations_have_request_bodies() -> None:
    paths = build_monitoring_openapi()["paths"]
    drain = paths["/mgmt/v1/nodes/drain"]["post"]
    assert "requestBody" in drain
    assert "draining" in drain["requestBody"]["content"]["application/json"]["schema"][
        "properties"
    ]
    detach = paths["/mgmt/v1/peers/detach"]["post"]
    assert "peer_url" in detach["requestBody"]["content"]["application/json"]["schema"][
        "required"
    ]
    apply_ = paths["/mgmt/v1/policy/apply"]["post"]
    assert "rules" in apply_["requestBody"]["content"]["application/json"]["schema"][
        "properties"
    ]

def test_openapi_mgmt_mutations_declare_bearer_security() -> None:
    paths = build_monitoring_openapi()["paths"]
    for path in (
        "/mgmt/v1/nodes/drain",
        "/mgmt/v1/peers/detach",
        "/mgmt/v1/policy/apply",
        "/mgmt/v1/audit",
        "/metrics/prometheus",
    ):
        op = next(iter(paths[path].values()))
        assert op.get("security") == [{"bearerAuth": []}], path
