"""Simple test client for an empty MPREG Cluster.

Every MPREG server instance in a cluster has "echo" and "echos" commands by default,
where "echo" accepts 1 argument and "echos" accepts a list of arguments, so we can always
test a cluster with echo commands for the processing/resolution/connection logic."""

import asyncio
import dataclasses
import pprint as pp
import sys

from dataclasses import dataclass, field
from typing import Any

import orjson
import ulid
import websockets
import websockets.client
from loguru import logger

from .model import RPCCommand
from .timer import Timer

if sys.version_info >= (3, 12):
    asyncio.get_event_loop().set_task_factory(asyncio.eager_task_factory)


@dataclass(slots=True)
class Request:
    cmds: tuple[RPCCommand, ...]

    # uniqe id for this request...
    u: str | None = field(default_factory=lambda: str(ulid.new()))

    def dump(self):
        return dict(role="rpc", rpc=dataclasses.asdict(self))


@dataclass
class Client:
    # websocket URL like: ws://127.0.0.1:7773
    url: str

    # optionally disable big log printing to get more accurate timing measurements
    full_log: bool = True

    async def request(self, cmds: list[RPCCommand]):
        req = Request(cmds=cmds)

        send = orjson.dumps(req.dump())

        if self.full_log:
            logger.info("====================== NEW REQUEST ======================")
            logger.info("[{}] Sending:\n{}", req.u, pp.pformat(req))

        await self.websocket.send(send)

        got = orjson.loads(await self.websocket.recv())

        if self.full_log:
            logger.info("[{}] Result:\n{}", got["u"], pp.pformat(got))

        assert req.u == got["u"]
        return got

    async def connect(self):
        async for websocket in websockets.connect(self.url, user_agent_header=None):
            self.websocket = websocket

            # Test one simple echo command
            with Timer("Single Echo"):
                await self.request([RPCCommand("first", "echo", ("hi there!",), frozenset())])

            # Test echo command chaining its result to ANOTHER echo command
            with Timer("Double Echo"):
                await self.request(
                    [
                        RPCCommand("first", "echo", ("hi there!",), frozenset()),
                        RPCCommand("second", "echo", ("first",), frozenset()),
                    ]
                )

            # Test echo command chaining its result to ANOTHER echo command and THIRD unrelated command
            # Note: if a command has NO follow-ons, it runs at the first level and doesn't get returned.
            #       Values returned to the client only happen if they are in the last processing level.
            #       This also means if all commands have NO shared arguments, ALL commands get returned.
            with Timer("Triple Echo"):
                await self.request(
                    [
                        RPCCommand("|first", "echo", ("hi there!",), frozenset()),
                        RPCCommand("|second", "echo", ("|first",), frozenset()),
                        RPCCommand("|third", "echos", ("|first", "AND ME TOO"), frozenset()),
                    ]
                )

            # test final result combining first/second
            with Timer("Triple Echo 2"):
                await self.request(
                    [
                        RPCCommand("|first", "echo", ("hi there!",), frozenset()),
                        RPCCommand("|second", "echo", ("|first",), frozenset()),
                        RPCCommand(
                            "|third",
                            "echos",
                            ("|first", "|second", "AND ME TOO"),
                            frozenset(),
                        ),
                    ]
                )

            # test re-assembly of previously assembled results
            with Timer("Quad Echo"):
                await self.request(
                    [
                        RPCCommand("|first", "echo", ("hi there!",), frozenset()),
                        RPCCommand("|second", "echo", ("|first",), frozenset()),
                        RPCCommand(
                            "|third",
                            "echos",
                            ("|first", "|second", "AND ME TOO"),
                            frozenset(),
                        ),
                        RPCCommand("|4th", "echo", ("|third",), frozenset()),
                    ]
                )

            # return required because this is inside an infinte websocket re-connect generator
            # (though, for extra load testing, comment out the 'return' and it's just an infinte loop of requests)
            return

    def run(self):
        try:
            with Timer("Total Run"):
                asyncio.run(self.connect())
        except KeyboardInterrupt:
            logger.warning("EXIT REQUEST CONFIRMED")


@logger.catch
def cmd():
    import jsonargparse

    jsonargparse.CLI(Client)
