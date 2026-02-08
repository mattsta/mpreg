import pytest

from mpreg.client.dns_client import MPREGDnsClient


@pytest.mark.asyncio
async def test_dns_client_rejects_unknown_qtype() -> None:
    client = MPREGDnsClient("127.0.0.1", 53)
    with pytest.raises(ValueError):
        await client.resolve("example.com", qtype="NOPE")
