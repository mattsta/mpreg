"""Named scenario registry — pluggable ecosystem entry point."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from mpreg.testing.distlab.models import ScenarioResult
from mpreg.testing.distlab.scenario import Scenario

ScenarioFactory = Callable[[], Scenario | Awaitable[Scenario]]

@dataclass(slots=True)
class ScenarioRegistry:
    """Map scenario name → factory. Self-describing catalog for CLI and suites."""

    name: str = "default"
    _factories: dict[str, ScenarioFactory] = field(default_factory=dict)
    _meta: dict[str, dict[str, Any]] = field(default_factory=dict)

    def register(
        self,
        name: str,
        factory: ScenarioFactory,
        *,
        track: str = "",
        description: str = "",
        tags: tuple[str, ...] = (),
    ) -> None:
        if not name:
            raise ValueError("scenario name required")
        self._factories[name] = factory
        self._meta[name] = {
            "track": track,
            "description": description,
            "tags": list(tags),
        }

    def list(self) -> list[str]:
        return sorted(self._factories)

    def meta(self, name: str) -> dict[str, Any]:
        return dict(self._meta.get(name) or {})

    def get(self, name: str) -> ScenarioFactory:
        if name not in self._factories:
            raise KeyError(f"unknown scenario {name!r}; known={self.list()}")
        return self._factories[name]

    async def build(self, name: str) -> Scenario:
        import asyncio

        factory = self.get(name)
        sc = factory()
        if asyncio.iscoroutine(sc):
            sc = await sc
        return sc  # type: ignore[return-value]

    async def run(self, name: str) -> ScenarioResult:
        sc = await self.build(name)
        return await sc.run()

    def catalog(self) -> list[dict[str, Any]]:
        out = []
        for n in self.list():
            m = self.meta(n)
            out.append({"name": n, **m})
        return out

# Process-global default registry (builtins register on import of builtins module).
DEFAULT_REGISTRY = ScenarioRegistry(name="mpreg-distlab")

def get_registry() -> ScenarioRegistry:
    return DEFAULT_REGISTRY
