"""INV-E4: Packaged profiles load and encode safe fabric/modality defaults."""

from __future__ import annotations

from pathlib import Path

from mpreg.client.call_policy import ClientCallPolicy, RpcExecutionMode
from mpreg.core.config import MPREGSettings
from mpreg.fabric.link_state import LinkStateMode

_PROFILES = Path(__file__).resolve().parents[2] / "mpreg" / "profiles"

def test_soft_rt_profile_loads() -> None:
    settings = MPREGSettings.from_path(str(_PROFILES / "soft-rt.toml"))
    assert settings.fabric_routing_enabled is True
    assert settings.fabric_routing_max_hops <= 5
    assert settings.fabric_link_state_mode is LinkStateMode.PREFER
    # Soft-RT client policy pairs with this profile
    policy = ClientCallPolicy.for_mode(
        RpcExecutionMode.M2_SOFT_RT, deadline_seconds=0.5
    )
    assert policy.share_deadline_across_attempts is True
    assert policy.deadline_seconds == 0.5

def test_federated_profile_loads() -> None:
    settings = MPREGSettings.from_path(str(_PROFILES / "federated.toml"))
    assert settings.fabric_routing_enabled is True
    assert settings.fabric_link_state_mode is LinkStateMode.DISABLED
    # Federated default client: HA async (not soft-RT fail-closed by default)
    ha = ClientCallPolicy.for_mode(RpcExecutionMode.M1_ASYNC)
    assert ha.share_deadline_across_attempts is False

def test_dev_profile_loads() -> None:
    settings = MPREGSettings.from_path(str(_PROFILES / "dev.toml"))
    assert settings.fabric_routing_enabled is True
