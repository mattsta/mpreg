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

    def select(
        self,
        *,
        track: str = "",
        prefix: str = "",
        tag: str = "",
        names: list[str] | None = None,
        preset: str = "",
        exclude_tags: tuple[str, ...] = ("not_bft",),
        limit: int = 0,
        skip_unknown: bool = True,
    ) -> list[str]:
        """Select scenario names for suite runs.

        By default excludes ``not_bft`` demos (they may leave intentional dirty state).
        ``preset`` expands via :data:`SUITE_PRESETS` (e.g. ``smoke``).
        """
        if preset:
            preset_names = resolve_preset(preset)
            if not preset_names:
                raise KeyError(
                    f"unknown suite preset {preset!r}; known={sorted(SUITE_PRESETS)}"
                )
            names = list(preset_names) + list(names or [])
        if names:
            chosen = []
            known = set(self._factories)
            for n in names:
                if n in known:
                    chosen.append(n)
                elif not skip_unknown:
                    raise KeyError(f"unknown scenario {n!r}; known={self.list()}")
        else:
            chosen = []
            for n in self.list():
                m = self.meta(n)
                if track and m.get("track") != track:
                    continue
                if prefix and not n.startswith(prefix):
                    continue
                tags = set(m.get("tags") or [])
                if tag and tag not in tags:
                    continue
                if exclude_tags and tags.intersection(exclude_tags):
                    continue
                chosen.append(n)
        if limit and limit > 0:
            chosen = chosen[:limit]
        return chosen

    async def run_suite(
        self,
        *,
        track: str = "",
        prefix: str = "",
        tag: str = "",
        names: list[str] | None = None,
        preset: str = "",
        exclude_tags: tuple[str, ...] = ("not_bft",),
        limit: int = 0,
        fail_fast: bool = False,
    ) -> dict[str, Any]:
        """Run multiple scenarios; return aggregate report."""
        selected = self.select(
            track=track,
            prefix=prefix,
            tag=tag,
            names=names,
            preset=preset,
            exclude_tags=exclude_tags,
            limit=limit,
        )
        results: list[ScenarioResult] = []
        failed: list[str] = []
        for name in selected:
            try:
                res = await self.run(name)
            except Exception as exc:  # noqa: BLE001
                from mpreg.testing.distlab.models import CheckResult, CheckViolation

                res = ScenarioResult(
                    name=name,
                    ok=False,
                    duration_s=0.0,
                    history_len=0,
                    check=CheckResult(
                        name="suite",
                        ok=False,
                        violations=[
                            CheckViolation(
                                checker="suite",
                                message=f"{type(exc).__name__}: {exc}",
                            )
                        ],
                    ),
                    meta={"suite_error": True},
                )
            results.append(res)
            if not res.ok:
                failed.append(name)
                if fail_fast:
                    break
        return {
            "ok": not failed,
            "selected": selected,
            "preset": preset or None,
            "ran": len(results),
            "passed": sum(1 for r in results if r.ok),
            "failed": failed,
            "results": [r.to_dict() for r in results],
            "total_duration_s": sum(r.duration_s for r in results),
        }


# Process-global default registry (builtins register on import of builtins module).
DEFAULT_REGISTRY = ScenarioRegistry(name="mpreg-distlab")

# Named suite presets — fast in-process subsets for CI / operator smoke.
# Names that are not registered are skipped at select time.
# ``ci-core`` is composed at resolve time (smoke ∪ strong-core ∪ audit-core).
SUITE_PRESETS: dict[str, tuple[str, ...]] = {
    "smoke": (
        "strong.happy_3",
        "strong.drop_prepare",
        "strong.drop_abort",
        "strong.refuse_get_delete",
        "audit.multi_origin",
    ),
    "strong-core": (
        "strong.happy_3",
        "strong.happy_5",
        "strong.partition_majority",
        "strong.drop_prepare",
        "strong.drop_commit",
        "strong.drop_abort",
        "strong.refuse_get_delete",
        # T28/T29: CFT honesty demos (not residual-free; structural checker only)
        "strong.cft_partial_commit_lost_abort",
        "strong.cft_residual_healed_by_lww",
        "strong.cft_residual_survives_pending_purge",
        "strong.cft_orphan_backup_gc",
        "strong.cft_retry_abort_clears_residual",
        # T49: RPC fan-in self-target (peers=[self] local.abort)
        "strong.cft_retry_abort_self_target",
        # T52: product library surface GlobalCacheManager.strong_retry_abort
        "strong.cft_gcm_retry_abort_clears_residual",
        # T62: residual_ops_hint enrichment (guidance only; does not clear)
        "strong.cft_residual_ops_hint_enriched",
    ),
    "audit-core": (
        "audit.multi_origin",
        "audit.partition_heal",
        "audit.digest_repair",
        "audit.duplicate_idempotent",
        "audit.ineligible_local",
    ),
    # Placeholder so list_presets / unknown-check know the name; expanded below.
    "ci-core": (),
}


def resolve_preset(name: str) -> list[str]:
    """Return scenario names for a suite preset (empty if unknown).

    ``ci-core`` is the ordered union of smoke + strong-core + audit-core
    (deduplicated, first-seen wins) for a single CI/operator gate.
    """
    key = (name or "").strip().lower()
    if key == "ci-core":
        seen: set[str] = set()
        out: list[str] = []
        for part in ("smoke", "strong-core", "audit-core"):
            for n in SUITE_PRESETS.get(part, ()):
                if n not in seen:
                    seen.add(n)
                    out.append(n)
        return out
    if key not in SUITE_PRESETS:
        return []
    return list(SUITE_PRESETS[key])


def get_registry() -> ScenarioRegistry:
    return DEFAULT_REGISTRY
