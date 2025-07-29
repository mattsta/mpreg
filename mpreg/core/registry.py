import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from loguru import logger


@dataclass(slots=True)
class Command:
    name: str
    fun: Callable[..., Any]

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fun(*args, **kwargs)

    async def call_async(self, *args: Any, **kwargs: Any) -> Any:
        """Call the command function, handling both sync and async functions properly."""
        if asyncio.iscoroutinefunction(self.fun):
            return await self.fun(*args, **kwargs)
        else:
            return self.fun(*args, **kwargs)


@dataclass(slots=True)
class CommandRegistry:
    """A registry for storing and looking up commands."""

    _commands: dict[str, Command] = field(default_factory=dict)

    def register(self, command: Command) -> None:
        """Register a command."""
        if command.name in self._commands:
            # Allow overriding commands (useful for tests and reregistration)
            logger.debug(f"Overriding existing command: {command.name}")
        self._commands[command.name] = command

    def get(self, name: str) -> Command:
        """Get a command by name."""
        try:
            return self._commands[name]
        except KeyError:
            raise ValueError(f"Command {name} not found.")

    def __contains__(self, name: str) -> bool:
        return name in self._commands
