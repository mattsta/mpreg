"""L1 profile_settings_tour — load MPREGSettings from packaged profiles."""

from __future__ import annotations

import asyncio
from pathlib import Path

from mpreg.core.config import MPREGSettings
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

def _profiles_dir() -> Path:
    here = Path(__file__).resolve()
    # …/mpreg/examples/apps/01_simple/profile_settings_tour/run.py → mpreg/profiles
    candidate = here.parents[4] / "profiles"
    if candidate.is_dir():
        return candidate
    # cwd-relative fallback
    cwd = Path("mpreg/profiles")
    if cwd.is_dir():
        return cwd
    raise FileNotFoundError(f"profiles dir not found from {here}")

PROFILES = _profiles_dir()

async def main() -> None:
    with app_run(
        "profile_settings_tour",
        "Profile Settings Tour — from_path on packaged TOML",
        level="L1",
    ):
        with scenario("dev.toml four-plane defaults", "boot.settings", "boot.profile"):
            path = PROFILES / "dev.toml"
            ensure(path.is_file(), f"missing {path}")
            s = MPREGSettings.from_path(str(path))
            ensure(s.name == "dev-node", f"name {s.name}")
            ensure(s.cluster_id == "dev-cluster", f"cluster {s.cluster_id}")
            ensure(s.enable_default_cache is True, "cache off in dev")
            ensure(s.enable_default_queue is True, "queue off in dev")
            ensure(s.host == "127.0.0.1", f"host {s.host}")
            ok(
                f"dev name={s.name} cache={s.enable_default_cache} "
                f"queue={s.enable_default_queue} fabric={s.fabric_routing_enabled}"
            )

        with scenario("single-node.toml loads", "boot.settings", "boot.profile"):
            path = PROFILES / "single-node.toml"
            ensure(path.is_file(), f"missing {path}")
            s = MPREGSettings.from_path(str(path))
            ensure(bool(s.name), "empty name")
            ok(f"single-node name={s.name} cluster={s.cluster_id}")

        with scenario(
            "federated.toml stricter defaults", "boot.profile", "fabric.cluster_id"
        ):
            path = PROFILES / "federated.toml"
            ensure(path.is_file(), f"missing {path}")
            s = MPREGSettings.from_path(str(path))
            ensure(bool(s.cluster_id), "federated needs cluster_id")
            ok(
                f"federated name={s.name} cluster={s.cluster_id} "
                f"fabric_routing={getattr(s, 'fabric_routing_enabled', None)}"
            )

        with scenario(
            "soft-rt.toml present for deadline demos", "boot.profile", "rpc.deadline"
        ):
            path = PROFILES / "soft-rt.toml"
            ensure(path.is_file(), f"missing {path}")
            s = MPREGSettings.from_path(str(path))
            ok(f"soft-rt name={s.name} log={s.log_level}")

        with scenario("invalid path fails clearly", "boot.settings"):
            failed = False
            try:
                MPREGSettings.from_path(str(PROFILES / "does-not-exist.toml"))
            except Exception as exc:
                failed = True
                step(f"expected error: {type(exc).__name__}: {exc}")
            ensure(failed, "missing profile should raise")
            ok("missing profile fails closed")
            step("production: mpreg config-check <profile> before start")

        await asyncio.sleep(0)

if __name__ == "__main__":
    asyncio.run(main())
