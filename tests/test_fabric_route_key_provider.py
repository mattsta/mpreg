import pytest

from mpreg.fabric.route_keys import RouteKeyRegistry, refresh_route_keys
from mpreg.fabric.route_security import RouteAnnouncementSigner


class SyncKeyProvider:
    def __init__(self, signer: RouteAnnouncementSigner) -> None:
        self.signer = signer
        self.calls = 0

    def refresh(self, registry: RouteKeyRegistry) -> None:
        self.calls += 1
        registry.register_key(
            cluster_id="cluster-sync",
            public_key=self.signer.public_key,
            now=100.0,
        )


class AsyncKeyProvider:
    def __init__(self, signer: RouteAnnouncementSigner) -> None:
        self.signer = signer
        self.calls = 0

    async def refresh(self, registry: RouteKeyRegistry) -> None:
        self.calls += 1
        registry.register_key(
            cluster_id="cluster-async",
            public_key=self.signer.public_key,
            now=200.0,
        )


@pytest.mark.asyncio
async def test_refresh_route_keys_supports_sync_and_async() -> None:
    registry = RouteKeyRegistry()
    sync_provider = SyncKeyProvider(RouteAnnouncementSigner.create())
    async_provider = AsyncKeyProvider(RouteAnnouncementSigner.create())

    await refresh_route_keys(sync_provider, registry)
    assert sync_provider.calls == 1
    assert registry.resolve_public_keys("cluster-sync")

    await refresh_route_keys(async_provider, registry)
    assert async_provider.calls == 1
    assert registry.resolve_public_keys("cluster-async")
