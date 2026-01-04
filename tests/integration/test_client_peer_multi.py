import pytest

from mpreg.server import MPREGServer
from tests.test_helpers import wait_for_condition


def _peer_node_ids(server: MPREGServer) -> set[str]:
    directory = server._peer_directory
    if not directory:
        return set()
    return {node.node_id for node in directory.nodes()}


@pytest.mark.asyncio
async def test_peer_directory_converges(cluster_3_servers):
    """Ensure the fabric peer directory converges across three servers."""
    server1, server2, server3 = cluster_3_servers
    expected = {
        server1.cluster.local_url,
        server2.cluster.local_url,
        server3.cluster.local_url,
    }

    await wait_for_condition(
        lambda: expected.issubset(_peer_node_ids(server1))
        and expected.issubset(_peer_node_ids(server2))
        and expected.issubset(_peer_node_ids(server3)),
        timeout=6.0,
        error_message="Peer directory did not converge across 3 servers",
    )
