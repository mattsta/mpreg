"""Management mutation plane: drain, detach, policy apply, audit, readiness."""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.monitoring.unified_monitoring import MonitoringConfig, UnifiedSystemMonitor
from mpreg.core.namespace_policy import NamespacePolicyRule
from mpreg.fabric.connection_manager import FederationConnectionManager
from mpreg.fabric.federation_config import FederationConfig, FederationMode
from mpreg.fabric.monitoring_endpoints import create_federation_monitoring_system
from mpreg.server_pkg.mgmt_mutations import (
    MgmtAuditLog,
    apply_namespace_policy,
    apply_node_drain,
    apply_peer_detach,
    policy_dry_run,
)
from mpreg.server_pkg.openapi_surface import build_monitoring_openapi

class _FakeServer:
    def __init__(self) -> None:
        self.settings = MPREGSettings(
            name="n1",
            cluster_id="c1",
            discovery_policy_enabled=True,
            discovery_policy_default_allow=True,
            discovery_policy_rules=(),
        )
        self.cluster = type("C", (), {"local_url": "ws://127.0.0.1:1"})()
        self._mgmt_draining = False
        self._mgmt_audit_log = MgmtAuditLog()
        self._namespace_policy_engine = None
        self._closed: list[str] = []
        self._peer_directory = type(
            "PD",
            (),
            {
                "remove_node_by_id": lambda self, u: True,
            },
        )()
        self._peer_dial_state: dict[str, Any] = {"ws://peer:9": object()}
        self._bound = False

    async def _close_peer_connection(self, peer_url: str) -> None:
        self._closed.append(peer_url)

    def _namespace_policy_apply(self, payload: dict[str, Any]) -> dict[str, Any]:
        rules = payload.get("rules") or ()
        self.settings.discovery_policy_rules = tuple(
            r if isinstance(r, NamespacePolicyRule) else NamespacePolicyRule.from_dict(r)
            for r in rules
        )
        self.settings.discovery_policy_enabled = bool(
            payload.get("enabled", self.settings.discovery_policy_enabled)
        )
        return {
            "applied": True,
            "valid": True,
            "rule_count": len(self.settings.discovery_policy_rules),
        }

    def _bind_namespace_policy_to_data_planes(self) -> None:
        self._bound = True

def test_apply_node_drain_and_audit() -> None:
    server = _FakeServer()
    result = apply_node_drain(server, draining=True, actor="ops", reason="deploy")
    assert result["applied"] is True
    assert server._mgmt_draining is True
    entries = server._mgmt_audit_log.snapshot()
    assert len(entries) == 1
    assert entries[0]["event"] == "node_drain"
    assert entries[0]["actor"] == "ops"
    assert entries[0]["detail"]["draining"] is True

    clear = apply_node_drain(server, draining=False, actor="ops")
    assert clear["draining"] is False
    assert server._mgmt_draining is False

@pytest.mark.asyncio
async def test_apply_peer_detach() -> None:
    server = _FakeServer()
    bad = await apply_peer_detach(server, peer_url="", actor="a")
    assert bad["applied"] is False
    assert bad["error"] == "peer_url_required"

    self_detach = await apply_peer_detach(
        server, peer_url="ws://127.0.0.1:1", actor="a"
    )
    assert self_detach["applied"] is False
    assert self_detach["error"] == "cannot_detach_self"

    ok = await apply_peer_detach(server, peer_url="ws://peer:9", actor="a")
    assert ok["applied"] is True
    assert "ws://peer:9" in server._closed
    assert "ws://peer:9" not in server._peer_dial_state

def test_apply_policy_and_dry_run() -> None:
    server = _FakeServer()
    result = apply_namespace_policy(
        server,
        {
            "enabled": True,
            "default_allow": False,
            "rules": [
                {
                    "namespace": "ns",
                    "owners": ["c1"],
                    "visibility": ["c1"],
                    "allow_summaries": True,
                }
            ],
            "actor": "policy-bot",
        },
        actor="policy-bot",
    )
    assert result["applied"] is True
    assert server._bound is True

    dry = policy_dry_run(
        server,
        {
            "namespace": "ns.jobs",
            "action": "query",
            "viewer_cluster_id": "c1",
        },
    )
    assert dry["dry_run"] is True
    # Engine may not be set on fake until apply rebuilds it; dry-run builds ephemeral
    assert "allowed" in dry

@pytest.mark.asyncio
async def test_mgmt_http_drain_detach_audit_ready(
    server_cluster_ports: list[int],
) -> None:
    port, monitoring_port = server_cluster_ports[:2]
    settings = MPREGSettings(
        host="127.0.0.1", port=port, name="mgmt-mut", cluster_id="c-mut"
    )
    federation_config = FederationConfig(
        federation_mode=FederationMode.STRICT_ISOLATION,
        local_cluster_id=settings.cluster_id,
    )
    fm = FederationConnectionManager(federation_config=federation_config)
    um = UnifiedSystemMonitor(config=MonitoringConfig())
    task = asyncio.create_task(um.start())

    state = {"draining": False}
    audit = MgmtAuditLog()
    closed: list[str] = []

    def drain_provider(body: dict) -> dict:
        draining = bool(body.get("draining", True))
        state["draining"] = draining
        from mpreg.server_pkg.mgmt_mutations import MgmtAuditEntry
        import time

        audit.record(
            MgmtAuditEntry(
                event="node_drain",
                timestamp=time.time(),
                actor=body.get("actor"),
                success=True,
                detail={"draining": draining},
            )
        )
        return {"applied": True, "draining": draining}

    async def detach_provider(body: dict) -> dict:
        peer = str(body.get("peer_url") or "")
        if not peer:
            return {"applied": False, "error": "peer_url_required"}
        closed.append(peer)
        return {"applied": True, "detail": {"peer_url": peer}}

    def policy_provider(body: dict) -> dict:
        return {"applied": True, "valid": True, "rule_count": len(body.get("rules") or [])}

    def audit_provider() -> list:
        return audit.snapshot()

    try:
        mon = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=fm,
            unified_monitor=um,
            monitoring_port=monitoring_port,
            mgmt_drain_provider=drain_provider,
            mgmt_detach_provider=detach_provider,
            mgmt_policy_apply_provider=policy_provider,
            mgmt_audit_provider=audit_provider,
            draining_provider=lambda: state["draining"],
        )
        await mon.start()
        try:
            base = f"http://127.0.0.1:{monitoring_port}"
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{base}/live") as resp:
                    assert resp.status == 200

                async with session.get(f"{base}/ready") as resp:
                    ready_before = await resp.json()
                    assert ready_before.get("draining") is False

                async with session.post(
                    f"{base}/mgmt/v1/nodes/drain",
                    json={"draining": True, "actor": "tester"},
                ) as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data["applied"] is True
                    assert data["draining"] is True

                async with session.get(f"{base}/ready") as resp:
                    assert resp.status == 503
                    ready_body = await resp.json()
                    assert ready_body["ready"] is False
                    assert ready_body["draining"] is True

                async with session.post(
                    f"{base}/mgmt/v1/peers/detach",
                    json={"peer_url": "ws://peer:1"},
                ) as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data["applied"] is True
                assert closed == ["ws://peer:1"]

                async with session.post(
                    f"{base}/mgmt/v1/policy/apply",
                    json={"rules": [], "enabled": True},
                ) as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data["applied"] is True

                async with session.get(f"{base}/mgmt/v1/audit?limit=10") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data["audit_kind"] == "mgmt_mutations"
                    assert data["mutation_count"] >= 1
                    assert any(m["event"] == "node_drain" for m in data["mutations"])

                # clear drain — draining flag clears even if health still not ready
                async with session.post(
                    f"{base}/mgmt/v1/nodes/drain",
                    json={"draining": False},
                ) as resp:
                    assert resp.status == 200
                async with session.get(f"{base}/ready") as resp:
                    ready_after = await resp.json()
                    assert ready_after.get("draining") is False
        finally:
            await mon.stop()
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await um.stop()

def test_openapi_mutations_not_501() -> None:
    doc = build_monitoring_openapi()
    for path in (
        "/mgmt/v1/nodes/drain",
        "/mgmt/v1/peers/detach",
        "/mgmt/v1/policy/apply",
    ):
        post = doc["paths"][path]["post"]
        responses = post.get("responses") or {}
        assert "501" not in responses
        assert "200" in responses
