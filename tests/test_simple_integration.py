"""Simple integration test to verify the test framework works properly."""

from collections.abc import Callable
from typing import Any

import pytest

from mpreg.server import MPREGServer


class TestSimpleIntegration:
    """Basic integration tests to verify framework functionality."""

    @pytest.mark.asyncio
    async def test_basic_echo_integration(
        self, single_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Test basic echo functionality end-to-end."""
        client = await client_factory(single_server.settings.port)

        # Test simple echo
        result = await client.call("echo", "Hello World!")
        assert result == "Hello World!"

        # Test echo with different data types
        test_data = {"message": "test", "number": 42, "list": [1, 2, 3]}
        result = await client.call("echo", test_data)
        assert result == test_data

    @pytest.mark.asyncio
    async def test_multi_args_integration(
        self, single_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Test multi-argument functions."""
        client = await client_factory(single_server.settings.port)

        result = await client.call("echos", "arg1", "arg2", "arg3")
        # JSON serialization converts tuples to lists, so we need to handle that
        assert result == ["arg1", "arg2", "arg3"] or result == ("arg1", "arg2", "arg3")

    @pytest.mark.asyncio
    async def test_multiple_concurrent_clients(
        self, single_server: MPREGServer, client_factory: Callable[[int], Any]
    ) -> None:
        """Test multiple clients can connect and work concurrently."""
        # Create 3 clients
        clients = [
            await client_factory(single_server.settings.port),
            await client_factory(single_server.settings.port),
            await client_factory(single_server.settings.port),
        ]

        # Each client makes a call with unique data
        import asyncio

        tasks = [
            clients[0].call("echo", "client_0_message"),
            clients[1].call("echo", "client_1_message"),
            clients[2].call("echo", "client_2_message"),
        ]

        results = await asyncio.gather(*tasks)

        assert results[0] == "client_0_message"
        assert results[1] == "client_1_message"
        assert results[2] == "client_2_message"
