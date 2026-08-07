"""E2: /mgmt/v1/raft handler shape (INV-E2)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from aiohttp.test_utils import TestClient, TestServer

from mpreg.core.config import MPREGSettings
from mpreg.fabric.federation_config import FederationConfig
from mpreg.fabric.monitoring_endpoints import FederationMonitoringSystem


@pytest.mark.asyncio
async def test_mgmt_raft_unconfigured() -> None:
    settings = MPREGSettings(host="127.0.0.1", port=9001, name="t", cluster_id="c")
    fed = FederationConfig()
    mon = FederationMonitoringSystem(
        settings=settings,
        federation_config=fed,
        federation_manager=MagicMock(),
        unified_monitor=MagicMock(),
        raft_status_provider=None,
    )
    server = TestServer(mon.app)
    async with TestClient(server) as client:
        resp = await client.get("/mgmt/v1/raft")
        assert resp.status == 200
        body = await resp.json()
        assert body["status"] == "ok"
        assert body["raft"]["configured"] is False


@pytest.mark.asyncio
async def test_mgmt_raft_with_provider() -> None:
    settings = MPREGSettings(host="127.0.0.1", port=9002, name="t", cluster_id="c")
    fed = FederationConfig()

    def provider() -> dict:
        return {
            "configured": True,
            "registered": 1,
            "nodes": [
                {
                    "node_id": "n1",
                    "role": "leader",
                    "term": 2,
                    "commit_index": 5,
                    "membership_change_supported": False,
                }
            ],
            "membership_change_supported": False,
        }

    mon = FederationMonitoringSystem(
        settings=settings,
        federation_config=fed,
        federation_manager=MagicMock(),
        unified_monitor=MagicMock(),
        raft_status_provider=provider,
    )
    server = TestServer(mon.app)
    async with TestClient(server) as client:
        resp = await client.get("/mgmt/v1/raft")
        assert resp.status == 200
        body = await resp.json()
        assert body["raft"]["nodes"][0]["term"] == 2
