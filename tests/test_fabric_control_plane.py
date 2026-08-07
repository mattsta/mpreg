import pytest

from mpreg.fabric.control_plane import FabricControlPlane
from mpreg.fabric.gossip import GossipProtocol
from mpreg.fabric.gossip_transport import InProcessGossipTransport


@pytest.mark.asyncio
async def test_fabric_control_plane_create_wires_gossip() -> None:
    transport = InProcessGossipTransport()
    gossip = GossipProtocol(node_id="node-a", transport=transport)
    control_plane = FabricControlPlane.create(local_cluster="cluster-a", gossip=gossip)

    assert control_plane.catalog is control_plane.applier.catalog
    assert gossip.catalog_applier is control_plane.applier
    assert control_plane.index.catalog is control_plane.catalog
    assert control_plane.route_table.local_cluster == "cluster-a"
    assert gossip.route_applier is None
    assert control_plane.route_announcer is None
