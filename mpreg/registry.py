from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(slots=True)
class Command:
    name: str
    fun: Callable[..., Any]

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fun(*args, **kwargs)


@dataclass
class CommandRegistry:
    """A registry for storing and looking up commands."""

    _commands: dict[str, Command] = field(default_factory=dict)

    def register(self, command: Command) -> None:
        """Register a command."""
        if command.name in self._commands:
            raise ValueError(f"Command {command.name} already registered.")
        self._commands[command.name] = command

    def get(self, name: str) -> Command:
        """Get a command by name."""
        try:
            return self._commands[name]
        except KeyError:
            raise ValueError(f"Command {name} not found.")

    def __contains__(self, name: str) -> bool:
        return name in self._commands
