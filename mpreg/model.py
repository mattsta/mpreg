from dataclasses import dataclass
from typing import Any, Tuple, FrozenSet


@dataclass(slots=True, frozen=True)
class RPCCommand:
    """
    Represents a single, immutable command within an RPC request.

    This is a standardized data transfer object used for communication
    between the client and server.
    """
    name: str
    fun: str
    args: Tuple[Any, ...]
    locs: FrozenSet[str]
