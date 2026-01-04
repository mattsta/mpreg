import json
from pathlib import Path

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.persistence.config import PersistenceMode


def test_settings_from_dict_normalizes_collections(tmp_path: Path) -> None:
    settings = MPREGSettings.from_dict(
        {
            "host": "0.0.0.0",
            "resources": ["gpu", "ml"],
            "peers": ("ws://peer-1",),
            "advertised_urls": ["ws://advertised-1"],
            "log_debug_scopes": ["fabric.router"],
            "monitoring_enabled": False,
            "persistence_config": {
                "mode": "sqlite",
                "data_dir": str(tmp_path),
            },
        }
    )

    assert settings.resources == {"gpu", "ml"}
    assert settings.peers == ["ws://peer-1"]
    assert settings.advertised_urls == ("ws://advertised-1",)
    assert settings.log_debug_scopes == ("fabric.router",)
    assert settings.persistence_config is not None
    assert settings.persistence_config.mode is PersistenceMode.SQLITE
    assert settings.persistence_config.data_dir == tmp_path


def test_settings_from_dict_handles_scalar_values() -> None:
    settings = MPREGSettings.from_dict(
        {
            "resources": "gpu",
            "peers": "ws://peer-1",
            "advertised_urls": "ws://advertised-1",
            "log_debug_scopes": "fabric.router",
        }
    )
    assert settings.resources == {"gpu"}
    assert settings.peers == ["ws://peer-1"]
    assert settings.advertised_urls == ("ws://advertised-1",)
    assert settings.log_debug_scopes == ("fabric.router",)


def test_settings_from_toml(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.toml"
    config_path.write_text(
        """
        [mpreg]
        host = "127.0.0.1"
        monitoring_enabled = false

        [mpreg.persistence_config]
        mode = "sqlite"
        data_dir = "/tmp/mpreg_test"
        """
    )

    settings = MPREGSettings.from_toml(config_path)
    assert settings.host == "127.0.0.1"
    assert settings.monitoring_enabled is False
    assert settings.persistence_config is not None
    assert settings.persistence_config.mode is PersistenceMode.SQLITE


def test_settings_from_json(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.json"
    payload = {
        "mpreg": {
            "host": "127.0.0.1",
            "monitoring_enabled": False,
            "persistence_config": {"mode": "memory"},
        }
    }
    config_path.write_text(json.dumps(payload))

    settings = MPREGSettings.from_json(config_path)
    assert settings.host == "127.0.0.1"
    assert settings.monitoring_enabled is False
    assert settings.persistence_config is not None
    assert settings.persistence_config.mode is PersistenceMode.MEMORY


def test_settings_from_path(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.toml"
    config_path.write_text(
        """
        [mpreg]
        host = "127.0.0.1"
        monitoring_enabled = false
        """
    )
    settings = MPREGSettings.from_path(config_path)
    assert settings.host == "127.0.0.1"

    with pytest.raises(ValueError):
        MPREGSettings.from_path(tmp_path / "settings.txt")
