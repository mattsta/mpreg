"""Packaged settings profiles."""

from __future__ import annotations

from pathlib import Path

from mpreg.core.config import MPREGSettings

def test_packaged_profiles_load() -> None:
    root = Path(__file__).resolve().parents[1] / "mpreg" / "profiles"
    names = ["dev", "single-node", "cluster", "federated", "discovery-resolver"]
    for name in names:
        path = root / f"{name}.toml"
        assert path.is_file(), path
        settings = MPREGSettings.from_path(path)
        assert settings.cluster_id
        assert settings.name

def test_dev_profile_enables_four_plane_facade() -> None:
    root = Path(__file__).resolve().parents[1] / "mpreg" / "profiles"
    dev = MPREGSettings.from_path(root / "dev.toml")
    assert dev.enable_default_cache is True
    assert dev.enable_default_queue is True

def test_federated_profile_snapshot_fail_closed() -> None:
    root = Path(__file__).resolve().parents[1] / "mpreg" / "profiles"
    fed = MPREGSettings.from_path(root / "federated.toml")
    assert fed.fabric_snapshot_fail_on_restore_error is True
    assert fed.fabric_route_security_config is not None
    assert fed.fabric_route_security_config.allow_unsigned is False

def test_function_index_alias() -> None:
    from mpreg.server import Cluster

    cluster = Cluster.create(
        cluster_id="c",
        advertised_urls=("ws://127.0.0.1:1",),
        local_url="ws://127.0.0.1:1",
    )
    assert cluster.function_index == {}
    assert cluster.funtimes == cluster.function_index
