#!/usr/bin/env python3
from __future__ import annotations

from mpreg.core.native_codec import dumps_pretty_text, load_path, loads_text

"""
Main CLI Entry Point for MPREG Fabric Federation Management.

Provides command-line interface for comprehensive fabric federation operations:
- Cluster discovery, registration, and management
- Health monitoring and alerting
- Performance metrics and analytics
- Configuration validation and deployment
- Backup and recovery operations
"""

import asyncio
import os
import sys
import time
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import aiohttp
import click
from rich.console import Console
from rich.table import Table

from mpreg import __version__
from mpreg.cli.async_utils import run_coro
from mpreg.client.client_api import MPREGClientAPI
from mpreg.client.dns_client import MPREGDnsClient
from mpreg.client.pubsub_client import MPREGPubSubClient
from mpreg.core.cluster_map import (
    CatalogQueryRequest,
    CatalogWatchRequest,
    ListPeersRequest,
)
from mpreg.core.config import MPREGSettings
from mpreg.core.discovery_monitoring import DiscoveryAccessAuditRequest
from mpreg.core.discovery_summary import SummaryQueryRequest, SummaryWatchRequest
from mpreg.core.logging import configure_logging
from mpreg.core.namespace_policy import (
    NamespacePolicyApplyRequest,
    NamespacePolicyAuditRequest,
    NamespacePolicyRule,
)
from mpreg.core.persistence.config import PersistenceConfig, PersistenceMode
from mpreg.core.port_allocator import allocate_port
from mpreg.datastructures.type_aliases import JsonDict, MetadataValue, TenantId
from mpreg.dns import decode_node_id, encode_node_id
from mpreg.server import MPREGServer

from .federation_cli import FederationCLI
from .output import add_format_option, emit

console = Console()

def _strong_abort_fail_peers(body: dict[str, Any]) -> list[str]:
    """Extract last_abort_fail_peers from metrics body or nested coordinator."""
    peers = body.get("last_abort_fail_peers")
    if peers is None:
        peers = (body.get("coordinator") or {}).get("last_abort_fail_peers")
    return list(peers or [])

def _strong_abort_fail_op_id(body: dict[str, Any]) -> str:
    """Extract last_abort_fail_op_id from metrics body or nested coordinator."""
    oid = body.get("last_abort_fail_op_id")
    if oid is None or oid == "":
        oid = (body.get("coordinator") or {}).get("last_abort_fail_op_id")
    return str(oid or "")

def strong_residual_ops_hint(body: dict[str, Any]) -> str:
    """Ops remediation hint when CFT residual candidates are present.

    Prefers server-provided ``residual_ops_hint`` when non-empty and already
    enriched (T53/T58/T59); rebuilds via ``format_residual_ops_hint`` with
    ``recent_abort_fails`` when placeholders remain. Still CFT best-effort —
    not automatic heal, not residual-free proof, not BFT. Empty when no
    abort_fail peers (no residual candidates known).
    """
    from mpreg.core.cache_strong import format_residual_ops_hint

    existing = body.get("residual_ops_hint")
    recent = list(body.get("recent_abort_fails") or [])
    if not recent:
        coord = body.get("coordinator") or {}
        if isinstance(coord, dict):
            recent = list(coord.get("recent_abort_fails") or [])
    peers = _strong_abort_fail_peers(body)
    oid = _strong_abort_fail_op_id(body)
    built = format_residual_ops_hint(peers, oid, recent_abort_fails=recent)
    if isinstance(existing, str) and existing.strip():
        # Prefer server string unless it still has placeholders and we enriched
        if ("<ns>" in existing or "<id>" in existing) and built and (
            "<ns>" not in built
        ):
            return built
        return existing.strip()
    return built

def evaluate_strong_doctor_payload(
    payload: dict[str, Any],
) -> tuple[bool, str]:
    """Semantic doctor check for /metrics/strong or /mgmt/v1/strong JSON.

    Returns ``(ok, detail)``. Fails closed if capabilities claim quorum get/delete
    (v1 put-only MVP honesty). Does not claim WAN SLA.
    """
    body = (
        payload.get("strong")
        if isinstance(payload.get("strong"), dict)
        else payload
    )
    if not isinstance(body, dict):
        return False, "strong body missing"
    health = str(body.get("health", "")).lower()
    caps = body.get("capabilities") or {}
    counters = body.get("counters") or {}
    if caps.get("get_quorum") or caps.get("delete_quorum"):
        return (
            False,
            "strong dishonest capabilities (get_quorum/delete_quorum claimed)",
        )
    # T27: when caps present, cft_only / abort_best_effort must not be false
    # (v1 is CFT + best-effort ABORT only — never claim residual-free BFT abort).
    if caps and caps.get("cft_only") is False:
        return (
            False,
            "strong dishonest capabilities (cft_only=false; v1 is CFT-only)",
        )
    if caps and caps.get("abort_best_effort") is False:
        return (
            False,
            "strong dishonest capabilities "
            "(abort_best_effort=false; lost ABORT is a CFT limit)",
        )
    # T29: pending TTL must never be advertised as residual L1 GC
    if caps and caps.get("pending_ttl_clears_residual_l1") is True:
        return (
            False,
            "strong dishonest capabilities "
            "(pending_ttl_clears_residual_l1=true; purge is not residual GC)",
        )
    # T41: retry_abort must stay ops-driven (never claim automatic background heal)
    if caps and caps.get("retry_abort_ops_driven") is False:
        return (
            False,
            "strong dishonest capabilities "
            "(retry_abort_ops_driven=false; retry is ops-driven CFT, not auto-heal)",
        )
    if health in {"misconfigured", "critical"}:
        return False, f"strong health={health}"
    if health == "disabled":
        return (
            True,
            (
                f"strong disabled (ok) "
                f"puts_ok={counters.get('puts_ok', 0)} "
                f"gets_refused={counters.get('gets_refused', 0)} "
                f"deletes_refused={counters.get('deletes_refused', 0)}"
            ),
        )
    fail_peers = _strong_abort_fail_peers(body)
    fail_oid = _strong_abort_fail_op_id(body)
    detail = (
        f"strong health={health or 'n/a'} "
        f"bound={body.get('coordinator_bound')} "
        f"put_q={caps.get('put_majority_commit')} "
        f"get_q={caps.get('get_quorum', False)} "
        f"del_q={caps.get('delete_quorum', False)} "
        f"ryw={caps.get('local_ryw_after_put')} "
        f"cft={caps.get('cft_only', True)} "
        f"abort_be={caps.get('abort_best_effort', True)} "
        f"ttl_gc={caps.get('pending_ttl_clears_residual_l1', False)} "
        f"retry_ops={caps.get('retry_abort_ops_driven', True)} "
        f"puts_ok={counters.get('puts_ok', 0)} "
        f"gets_ref={counters.get('gets_refused', 0)} "
        f"dels_ref={counters.get('deletes_refused', 0)} "
        f"abort_fail={counters.get('aborts_peer_fail', 0)} "
        f"abort_fail_peers={fail_peers} "
        f"abort_fail_op_id={fail_oid or '-'} "
        f"retry_abort={counters.get('retry_abort_calls', body.get('retry_abort_calls', 0))} "
        f"retry_cleared={counters.get('retry_abort_cleared', body.get('retry_abort_cleared', 0))} "
        f"visible={body.get('visible_count', 0)} "
        f"backups={body.get('backups_count', 0)} "
        f"pruned={body.get('backups_pruned_total', 0)}"
    )
    # T51: ops remediation when residual candidates present (not auto-heal)
    hint = strong_residual_ops_hint(body)
    if hint:
        detail = f"{detail} | {hint}"
    return True, detail

# Capability keys that must never be true in v1 shared-audit metrics.
_AUDIT_DISHONEST_CAPS = (
    "siem",
    "bft",
    "infinite_retention",
    "linearizable_cluster_ops",
    "multi_tenant_beyond_cluster_id",
)

def evaluate_shared_audit_doctor_payload(
    payload: dict[str, Any],
) -> tuple[bool, str]:
    """Semantic doctor check for /metrics/shared-audit JSON.

    Returns ``(ok, detail)``. Fails closed if capabilities claim SIEM/BFT/
    infinite retention/linearizable ops/multi-tenant beyond cluster_id.
    Does not claim WAN SLA.
    """
    body = (
        payload.get("shared_audit")
        if isinstance(payload.get("shared_audit"), dict)
        else payload
    )
    if not isinstance(body, dict):
        return False, "shared_audit body missing"
    status = str(body.get("status", "")).lower()
    caps = body.get("capabilities") or {}
    counters = body.get("counters") or {}
    dishonest = [k for k in _AUDIT_DISHONEST_CAPS if caps.get(k)]
    if dishonest:
        return (
            False,
            f"shared_audit dishonest capabilities ({','.join(dishonest)} claimed)",
        )
    if status in {"misconfigured", "critical"}:
        return False, f"shared_audit status={status}"
    if status == "disabled":
        return (
            True,
            (
                f"shared_audit disabled (ok) "
                f"gset={caps.get('gset_epidemic', False)} "
                f"siem={caps.get('siem', False)} "
                f"bft={caps.get('bft', False)} "
                f"store_size={body.get('store_size', 0)}"
            ),
        )
    return (
        True,
        (
            f"shared_audit status={status or 'n/a'} "
            f"gset={caps.get('gset_epidemic')} "
            f"siem={caps.get('siem', False)} "
            f"bft={caps.get('bft', False)} "
            f"inf_ret={caps.get('infinite_retention', False)} "
            f"lin_ops={caps.get('linearizable_cluster_ops', False)} "
            f"store_size={body.get('store_size', 0)} "
            f"deltas_recv={counters.get('deltas_recv', 0)} "
            f"drops={counters.get('publish_dropped', 0)}"
        ),
    )

def setup_logging(verbose: bool = False, *, json_logs: bool = False) -> None:
    """Setup logging configuration."""
    level = "DEBUG" if verbose else "INFO"
    configure_logging(level, colorize=not json_logs, json_logs=json_logs)

@click.group()
@click.version_option(__version__, prog_name="mpreg")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option(
    "--json-logs",
    is_flag=True,
    help="Emit structured JSON logs (one object per line on stderr)",
)
@click.pass_context
def cli(ctx, verbose: bool, json_logs: bool):
    """
    MPREG Fabric Management CLI.

    Operate fabric-enabled MPREG clusters: discovery, RPC, monitoring,
    routing diagnostics, and deployment automation. (Historical "federation"
    subcommands remain as aliases where noted.)

    \b
    Common entrypoints:
      mpreg call FUN [ARGS]     — RPC (alias of mpreg client call)
      mpreg dns …               — DNS plane (alias of mpreg client dns-*)
      mpreg doctor --url HTTP   — monitoring health (not WS)
      mpreg examples …          — curriculum apps
      mpreg distlab …           — DistLab scenarios (platform self-test)
      mpreg test concurrent …   — high-concurrency pytest runner
    """
    setup_logging(verbose, json_logs=json_logs)
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["json_logs"] = json_logs

@cli.group()
def client():
    """Client commands for interacting with MPREG servers."""

@client.command("call")
@click.argument("fun")
@click.argument("args", nargs=-1)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option(
    "--locs",
    multiple=True,
    help="Resource locations to target (can be repeated)",
)
@click.option("--timeout", type=float, default=None, help="RPC timeout in seconds")
def call(
    fun: str,
    args: tuple[str, ...],
    url: str | None,
    locs: tuple[str, ...],
    timeout: float | None,
):
    """Call a remote RPC function."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    def _parse_arg(value: str) -> Any:
        try:
            return loads_text(value)
        except Exception:
            return value

    async def _call():
        from mpreg.client.unified_client import MPREGClient

        async with MPREGClient(url) as client:
            parsed_args = tuple(_parse_arg(arg) for arg in args)
            result = await client.call(
                fun,
                *parsed_args,
                locs=frozenset(locs) if locs else None,
                timeout=timeout,
            )
            console.print(result)

    run_coro(_call())

@client.command("queue-send")
@click.option("--url", default=None, envvar="MPREG_URL", help="MPREG server URL")
@click.option("--queue", "queue_name", required=True, help="Queue name")
@click.option("--payload", required=True, help="JSON or raw string payload")
@click.option("--topic", default=None, help="Optional topic")
def client_queue_send(
    url: str | None, queue_name: str, payload: str, topic: str | None
) -> None:
    """Smoke: send a message via MPREGClient.queue_send (ERG-05)."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    def _parse(value: str) -> Any:
        try:
            return loads_text(value)
        except Exception:
            return value

    async def _run() -> None:
        from mpreg.client.unified_client import MPREGClient

        async with MPREGClient(url) as c:
            result = await c.queue_send(
                queue_name, _parse(payload), topic=topic or queue_name
            )
            console.print(result)

    run_coro(_run())

@client.command("cache-get")
@click.option("--url", default=None, envvar="MPREG_URL", help="MPREG server URL")
@click.option("--namespace", required=True)
@click.option("--key", "identifier", required=True)
def client_cache_get(url: str | None, namespace: str, identifier: str) -> None:
    """Smoke: cache_get via MPREGClient (ERG-05)."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _run() -> None:
        from mpreg.client.unified_client import MPREGClient

        async with MPREGClient(url) as c:
            result = await c.cache_get(namespace, identifier)
            console.print(result)

    run_coro(_run())

@client.command("cache-put")
@click.option("--url", default=None, envvar="MPREG_URL", help="MPREG server URL")
@click.option("--namespace", required=True)
@click.option("--key", "identifier", required=True)
@click.option("--value", required=True, help="JSON or raw string value")
def client_cache_put(
    url: str | None, namespace: str, identifier: str, value: str
) -> None:
    """Smoke: cache_put via MPREGClient (ERG-05)."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    def _parse(v: str) -> Any:
        try:
            return loads_text(v)
        except Exception:
            return v

    async def _run() -> None:
        from mpreg.client.unified_client import MPREGClient

        async with MPREGClient(url) as c:
            result = await c.cache_put(namespace, identifier, _parse(value))
            console.print(result)

    run_coro(_run())

@client.command("queue-receive")
@click.option("--url", default=None, envvar="MPREG_URL", help="MPREG server URL")
@click.option("--queue", "queue_name", required=True, help="Queue name")
@click.option("--subscriber-id", default="cli-subscriber", help="Subscriber id")
@click.option("--timeout", type=float, default=5.0, help="Receive timeout seconds")
def client_queue_receive(
    url: str | None, queue_name: str, subscriber_id: str, timeout: float
) -> None:
    """Smoke: queue_receive via MPREGClient (ERG-T10-10)."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _run() -> None:
        from mpreg.client.unified_client import MPREGClient

        async with MPREGClient(url) as c:
            result = await c.queue_receive(
                queue_name,
                subscriber_id=subscriber_id,
                timeout_seconds=timeout,
            )
            console.print(result)

    run_coro(_run())

@client.command("queue-ack")
@click.option("--url", default=None, envvar="MPREG_URL", help="MPREG server URL")
@click.option("--queue", "queue_name", required=True, help="Queue name")
@click.option("--message-id", required=True, help="Message id to acknowledge")
@click.option("--subscriber-id", default="cli-subscriber", help="Subscriber id")
def client_queue_ack(
    url: str | None, queue_name: str, message_id: str, subscriber_id: str
) -> None:
    """Smoke: queue_ack via MPREGClient (ERG-T10-10)."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _run() -> None:
        from mpreg.client.unified_client import MPREGClient

        async with MPREGClient(url) as c:
            result = await c.queue_ack(queue_name, message_id, subscriber_id)
            console.print(result)

    run_coro(_run())

@client.command("cache-invalidate")
@click.option("--url", default=None, envvar="MPREG_URL", help="MPREG server URL")
@click.option("--pattern", required=True, help="Invalidation pattern")
def client_cache_invalidate(url: str | None, pattern: str) -> None:
    """Smoke: cache_invalidate via MPREGClient (ERG-T10-10)."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _run() -> None:
        from mpreg.client.unified_client import MPREGClient

        async with MPREGClient(url) as c:
            result = await c.cache_invalidate(pattern)
            console.print(result)

    run_coro(_run())

@client.command("cache-strong-retry-abort")
@click.option("--url", default=None, envvar="MPREG_URL", help="MPREG server URL")
@click.option("--namespace", required=True, help="Cache namespace")
@click.option("--key", "identifier", required=True, help="Cache key identifier")
@click.option(
    "--op-id",
    "op_id",
    required=True,
    help="STRONG put operation_id / op_id to re-ABORT",
)
@click.option(
    "--version",
    default="v1.0.0",
    show_default=True,
    help="Cache key version (must match put)",
)
@click.option(
    "--peer",
    "peers",
    multiple=True,
    help="Optional residual peer id (repeatable); default = last_abort_fail_peers",
)
@click.option(
    "--timeout",
    type=float,
    default=None,
    help="Optional RPC timeout seconds",
)
@click.option(
    "--loc",
    "locs",
    multiple=True,
    help="Optional resource location pin (repeatable; e.g. cache)",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Print raw result dict as JSON",
)
def client_cache_strong_retry_abort(
    url: str | None,
    namespace: str,
    identifier: str,
    op_id: str,
    version: str,
    peers: tuple[str, ...],
    timeout: float | None,
    locs: tuple[str, ...],
    as_json: bool,
) -> None:
    """Ops-driven CFT re-ABORT after recovery (not automatic heal).

    Calls ``MPREGClient.cache_strong_retry_abort`` → platform RPC
    ``mpreg.cache.strong_retry_abort``. Still CFT best-effort — not residual-free
    while ABORT is lost, not BFT, not background heal.
    """
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _run() -> None:
        from mpreg.client.unified_client import MPREGClient

        peer_list = list(peers) if peers else None
        loc_set = frozenset(locs) if locs else None
        async with MPREGClient(url) as c:
            result = await c.cache_strong_retry_abort(
                namespace,
                identifier,
                op_id,
                version=version,
                peers=peer_list,
                locs=loc_set,
                timeout=timeout,
            )
            if as_json:
                payload = {
                    "success": result.success,
                    "cleared": result.cleared,
                    "ok_peers": list(result.ok_peers or []),
                    "fail_peers": list(result.fail_peers or []),
                    "op_id": result.op_id,
                    "attempts": result.attempts,
                    "ops_driven": result.ops_driven,
                    "automatic_heal": result.automatic_heal,
                    "error_message": result.error_message,
                    "error_code": result.error_code,
                }
                console.print_json(data=payload)
            else:
                status = "cleared" if result.cleared else "still_fail"
                console.print(
                    f"retry_abort {status} op_id={result.op_id} "
                    f"ok={list(result.ok_peers or [])} "
                    f"fail={list(result.fail_peers or [])} "
                    f"attempts={result.attempts} "
                    f"ops_driven={result.ops_driven} "
                    f"automatic_heal={result.automatic_heal}"
                )
                if result.error_message:
                    console.print(f"[yellow]error={result.error_message}[/yellow]")
                # Honesty banner — never market as auto-heal
                console.print(
                    "[dim]CFT best-effort ops path only "
                    "(not automatic residual heal, not BFT)[/dim]"
                )
            if not result.success:
                raise SystemExit(1)

    run_coro(_run())

@client.command("publish")
@click.option("--url", default=None, envvar="MPREG_URL", help="MPREG server URL")
@click.option("--topic", required=True)
@click.option("--payload", required=True, help="JSON or raw string payload")
def client_publish(url: str | None, topic: str, payload: str) -> None:
    """Smoke: fail-closed publish via MPREGClient (ERG-05)."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    def _parse(v: str) -> Any:
        try:
            return loads_text(v)
        except Exception:
            return v

    async def _run() -> None:
        from mpreg.client.unified_client import MPREGClient

        async with MPREGClient(url) as c:
            result = await c.publish(topic, _parse(payload))
            console.print(result)

    run_coro(_run())

@client.command("list-peers")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option(
    "--scope", default=None, help="Discovery scope (local, zone, region, global)"
)
@click.option("--cluster-id", default=None, help="Filter peers by cluster id")
@click.option(
    "--target-cluster", default=None, help="Target cluster for federated routing"
)
def list_peers(
    url: str | None,
    scope: str | None,
    cluster_id: str | None,
    target_cluster: str | None,
):
    """List known peers in the cluster."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _list():
        async with MPREGClientAPI(url) as client:
            request = ListPeersRequest(scope=scope, cluster_id=cluster_id)
            peers = await client.list_peers(request, target_cluster=target_cluster)
            console.print([peer.to_dict() for peer in peers])

    run_coro(_list())

@client.command("resolver-cache-stats")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
def resolver_cache_stats(url: str | None) -> None:
    """Fetch resolver cache stats."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _stats() -> None:
        async with MPREGClientAPI(url) as client:
            result = await client.resolver_cache_stats()
            console.print(result.to_dict())

    run_coro(_stats())

@client.command("resolver-resync")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
def resolver_resync(url: str | None) -> None:
    """Trigger a resolver cache resync."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _resync() -> None:
        async with MPREGClientAPI(url) as client:
            result = await client.resolver_resync()
            console.print(result.to_dict())

    run_coro(_resync())

def _parse_metadata_items(items: tuple[str, ...]) -> dict[str, MetadataValue]:
    metadata: dict[str, MetadataValue] = {}
    for item in items:
        if "=" in item:
            key, value = item.split("=", 1)
            key = key.strip()
            if not key:
                continue
            try:
                parsed = loads_text(value)
                if isinstance(parsed, (str, int, float, bool)):
                    metadata[key] = parsed
                else:
                    metadata[key] = str(parsed)
            except Exception:
                metadata[key] = value
        else:
            metadata[item] = True
    return metadata

@client.command("dns-register")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option("--name", required=True, help="Service name")
@click.option("--namespace", required=True, help="Service namespace")
@click.option("--protocol", default="tcp", help="Service protocol")
@click.option("--port", type=int, required=True, help="Service port")
@click.option(
    "--target",
    "--targets",
    multiple=True,
    help="Service target host (repeatable; --targets alias)",
)
@click.option("--tag", multiple=True, help="Service tag (repeatable)")
@click.option("--capability", multiple=True, help="Service capability (repeatable)")
@click.option("--metadata", multiple=True, help="Metadata key=value (repeatable)")
@click.option("--priority", type=int, default=0, help="SRV priority")
@click.option("--weight", type=int, default=0, help="SRV weight")
@click.option("--scope", default=None, help="Discovery scope")
@click.option("--ttl", type=float, default=None, help="TTL override in seconds")
def dns_register(
    url: str | None,
    name: str,
    namespace: str,
    protocol: str,
    port: int,
    target: tuple[str, ...],
    tag: tuple[str, ...],
    capability: tuple[str, ...],
    metadata: tuple[str, ...],
    priority: int,
    weight: int,
    scope: str | None,
    ttl: float | None,
) -> None:
    """Register a DNS service endpoint."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _register() -> None:
        async with MPREGClientAPI(url) as client:
            payload = {
                "name": name,
                "namespace": namespace,
                "protocol": protocol,
                "port": port,
                "targets": list(target),
                "tags": list(tag),
                "capabilities": list(capability),
                "metadata": _parse_metadata_items(metadata),
                "priority": priority,
                "weight": weight,
                "scope": scope,
                "ttl_seconds": ttl,
            }
            response = await client.dns_register(payload)
            console.print(response.to_dict())

    run_coro(_register())

@client.command("dns-unregister")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option("--name", required=True, help="Service name")
@click.option("--namespace", required=True, help="Service namespace")
@click.option("--protocol", default="tcp", help="Service protocol")
@click.option("--port", type=int, required=True, help="Service port")
def dns_unregister(
    url: str | None,
    name: str,
    namespace: str,
    protocol: str,
    port: int,
) -> None:
    """Unregister a DNS service endpoint."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _unregister() -> None:
        async with MPREGClientAPI(url) as client:
            response = await client.dns_unregister(
                {
                    "name": name,
                    "namespace": namespace,
                    "protocol": protocol,
                    "port": port,
                }
            )
            console.print(response.to_dict())

    run_coro(_unregister())

@client.command("dns-list")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option("--namespace", default=None, help="Filter by namespace")
@click.option("--name", default=None, help="Filter by service name")
@click.option("--protocol", default=None, help="Filter by protocol")
@click.option("--port", type=int, default=None, help="Filter by port")
@click.option("--scope", default=None, help="Discovery scope")
@click.option("--tag", multiple=True, help="Filter by tag (repeatable)")
@click.option("--capability", multiple=True, help="Filter by capability (repeatable)")
@click.option("--limit", type=int, default=None, help="Page size limit")
@click.option("--page-token", default=None, help="Pagination token")
def dns_list(
    url: str | None,
    namespace: str | None,
    name: str | None,
    protocol: str | None,
    port: int | None,
    scope: str | None,
    tag: tuple[str, ...],
    capability: tuple[str, ...],
    limit: int | None,
    page_token: str | None,
) -> None:
    """List DNS service endpoints."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _list() -> None:
        async with MPREGClientAPI(url) as client:
            response = await client.dns_list(
                {
                    "namespace": namespace,
                    "name": name,
                    "protocol": protocol,
                    "port": port,
                    "scope": scope,
                    "tags": list(tag),
                    "capabilities": list(capability),
                    "limit": limit,
                    "page_token": page_token,
                }
            )
            console.print(response.to_dict())

    run_coro(_list())

@client.command("dns-describe")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option("--namespace", default=None, help="Filter by namespace")
@click.option("--name", default=None, help="Filter by service name")
@click.option("--protocol", default=None, help="Filter by protocol")
@click.option("--port", type=int, default=None, help="Filter by port")
@click.option("--scope", default=None, help="Discovery scope")
@click.option("--tag", multiple=True, help="Filter by tag (repeatable)")
@click.option("--capability", multiple=True, help="Filter by capability (repeatable)")
@click.option("--limit", type=int, default=None, help="Page size limit")
@click.option("--page-token", default=None, help="Pagination token")
def dns_describe(
    url: str | None,
    namespace: str | None,
    name: str | None,
    protocol: str | None,
    port: int | None,
    scope: str | None,
    tag: tuple[str, ...],
    capability: tuple[str, ...],
    limit: int | None,
    page_token: str | None,
) -> None:
    """Describe DNS service endpoints with full metadata."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _describe() -> None:
        async with MPREGClientAPI(url) as client:
            response = await client.dns_describe(
                {
                    "namespace": namespace,
                    "name": name,
                    "protocol": protocol,
                    "port": port,
                    "scope": scope,
                    "tags": list(tag),
                    "capabilities": list(capability),
                    "limit": limit,
                    "page_token": page_token,
                }
            )
            console.print(response.to_dict())

    run_coro(_describe())

@client.command("dns-node-encode")
@click.argument("node_id")
def dns_node_encode(node_id: str) -> None:
    """Encode a node_id into a DNS-safe label."""
    label = encode_node_id(node_id)
    if not label:
        raise click.ClickException("Failed to encode node_id")
    console.print(label)

@client.command("dns-node-decode")
@click.argument("label")
def dns_node_decode(label: str) -> None:
    """Decode a DNS node label back into a node_id."""
    decoded = decode_node_id(label)
    if decoded is None:
        raise click.ClickException("Invalid DNS node label")
    console.print(decoded)

@client.command("dns-resolve")
@click.option(
    "--host",
    default="127.0.0.1",
    help="DNS gateway host (default: 127.0.0.1)",
)
@click.option("--port", type=int, required=True, help="DNS gateway port")
@click.option("--qname", required=True, help="Query name")
@click.option("--qtype", default="A", help="Query type (A, AAAA, SRV, TXT, ANY)")
@click.option("--tcp/--udp", default=False, help="Use TCP instead of UDP")
@click.option("--timeout", type=float, default=2.0, help="Query timeout in seconds")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON output")
def dns_resolve(
    host: str,
    port: int,
    qname: str,
    qtype: str,
    tcp: bool,
    timeout: float,
    as_json: bool,
) -> None:
    """Resolve a DNS name against the MPREG DNS gateway."""

    async def _resolve() -> None:
        try:
            client = MPREGDnsClient(
                host=host,
                port=port,
                use_tcp=tcp,
                timeout=timeout,
            )
            result = await client.resolve(qname, qtype=qtype)
        except TimeoutError as exc:
            raise click.ClickException("DNS query timed out") from exc
        except Exception as exc:
            raise click.ClickException(str(exc)) from exc
        if as_json:
            console.print(result.to_dict())
            return
        if not result.answers:
            console.print(f"No answers (rcode={result.rcode})")
            return
        table = Table(title=f"DNS Answers ({result.rcode})")
        table.add_column("Name")
        table.add_column("Type")
        table.add_column("TTL")
        table.add_column("Data")
        for answer in result.answers:
            table.add_row(answer.name, answer.rtype, str(answer.ttl), answer.rdata)
        console.print(table)

    run_coro(_resolve())

@client.group("namespace-policy")
def namespace_policy():
    """Namespace policy management commands."""

@namespace_policy.command("validate")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option(
    "--rules-file",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
    help="Path to a JSON file containing namespace policy rules",
)
@click.option("--actor", default=None, help="Audit actor label")
def namespace_policy_validate(url: str | None, rules_file: str, actor: str | None):
    """Validate namespace policy rules."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    with open(rules_file, encoding="utf-8") as handle:
        rules = loads_text(handle.read())

    async def _validate():
        async with MPREGClientAPI(url) as client:
            raw_rules = rules
            if isinstance(raw_rules, dict):
                raw_rules = [raw_rules]
            parsed_rules = tuple(
                NamespacePolicyRule.from_dict(rule)
                for rule in raw_rules
                if isinstance(rule, dict)
            )
            request = NamespacePolicyApplyRequest(rules=parsed_rules, actor=actor)
            result = await client.namespace_policy_validate(request)
            console.print(result.to_dict())

    run_coro(_validate())

@namespace_policy.command("apply")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option(
    "--rules-file",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
    help="Path to a JSON file containing namespace policy rules",
)
@click.option(
    "--enabled/--no-enabled",
    default=None,
    help="Enable or disable namespace policy enforcement",
)
@click.option(
    "--default-allow/--default-deny",
    default=None,
    help="Default allow behavior when no policy matches",
)
@click.option("--actor", default=None, help="Audit actor label")
def namespace_policy_apply(
    url: str | None,
    rules_file: str,
    enabled: bool | None,
    default_allow: bool | None,
    actor: str | None,
):
    """Apply namespace policy rules."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    with open(rules_file, encoding="utf-8") as handle:
        rules = loads_text(handle.read())

    async def _apply():
        async with MPREGClientAPI(url) as client:
            raw_rules = rules
            if isinstance(raw_rules, dict):
                raw_rules = [raw_rules]
            parsed_rules = tuple(
                NamespacePolicyRule.from_dict(rule)
                for rule in raw_rules
                if isinstance(rule, dict)
            )
            request = NamespacePolicyApplyRequest(
                rules=parsed_rules,
                enabled=enabled,
                default_allow=default_allow,
                actor=actor,
            )
            result = await client.namespace_policy_apply(request)
            console.print(result.to_dict())

    run_coro(_apply())

@namespace_policy.command("export")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
def namespace_policy_export(url: str | None):
    """Export current namespace policy configuration."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _export():
        async with MPREGClientAPI(url) as client:
            result = await client.namespace_policy_export()
            console.print(result.to_dict())

    run_coro(_export())

@namespace_policy.command("audit")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option("--limit", type=int, default=None, help="Limit audit entries")
def namespace_policy_audit(url: str | None, limit: int | None):
    """Fetch namespace policy audit entries."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _audit():
        async with MPREGClientAPI(url) as client:
            request = NamespacePolicyAuditRequest(limit=limit)
            result = await client.namespace_policy_audit(request)
            console.print(result.to_dict())

    run_coro(_audit())

@cli.group()
def discovery():
    """Discovery plane commands."""

@cli.group()
def report():
    """Reporting commands."""

@discovery.command("query")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option(
    "--entry-type",
    default="functions",
    help="Catalog entry type (functions, nodes, queues, topics, caches, cache_profiles)",
)
@click.option("--namespace", default=None, help="Namespace prefix to filter")
@click.option(
    "--scope", default=None, help="Discovery scope (local, zone, region, global)"
)
@click.option(
    "--viewer-cluster-id",
    default=None,
    help="Viewer cluster for policy checks (ignored when identity is derived from connection)",
)
@click.option(
    "--viewer-tenant-id",
    default=None,
    help="Viewer tenant for policy checks (when tenant mode is enabled)",
)
@click.option("--capability", "capabilities", multiple=True, help="Capability filter")
@click.option("--resource", "resources", multiple=True, help="Resource filter")
@click.option("--tag", "tags", multiple=True, help="Tag filter (repeatable)")
@click.option("--cluster-id", default=None, help="Filter to cluster ID")
@click.option("--node-id", default=None, help="Filter to node ID")
@click.option("--function-name", default=None, help="Function name filter")
@click.option("--function-id", default=None, help="Function ID filter")
@click.option("--version-constraint", default=None, help="Semantic version constraint")
@click.option("--queue-name", default=None, help="Queue name filter")
@click.option("--topic", default=None, help="Topic filter")
@click.option("--limit", type=int, default=None, help="Page size limit")
@click.option("--page-token", default=None, help="Pagination token")
def discovery_query(
    url: str | None,
    entry_type: str,
    namespace: str | None,
    scope: str | None,
    viewer_cluster_id: str | None,
    viewer_tenant_id: TenantId | None,
    capabilities: tuple[str, ...],
    resources: tuple[str, ...],
    tags: tuple[str, ...],
    cluster_id: str | None,
    node_id: str | None,
    function_name: str | None,
    function_id: str | None,
    version_constraint: str | None,
    queue_name: str | None,
    topic: str | None,
    limit: int | None,
    page_token: str | None,
):
    """Run a catalog_query for discovery entries."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _query() -> None:
        request = CatalogQueryRequest(
            entry_type=entry_type,
            namespace=namespace,
            scope=scope,
            viewer_cluster_id=viewer_cluster_id,
            viewer_tenant_id=viewer_tenant_id,
            capabilities=capabilities,
            resources=resources,
            tags=tags,
            cluster_id=cluster_id,
            node_id=node_id,
            function_name=function_name,
            function_id=function_id,
            version_constraint=version_constraint,
            queue_name=queue_name,
            topic=topic,
            limit=limit,
            page_token=page_token,
        )
        async with MPREGClientAPI(url) as client:
            result = await client.catalog_query(request)
            console.print(result.to_dict())

    run_coro(_query())

@discovery.command("summary")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option("--namespace", default=None, help="Namespace prefix to filter")
@click.option("--service-id", default=None, help="Service ID filter")
@click.option(
    "--viewer-cluster-id",
    default=None,
    help="Viewer cluster for policy checks (ignored when identity is derived from connection)",
)
@click.option(
    "--viewer-tenant-id",
    default=None,
    help="Viewer tenant for policy checks (when tenant mode is enabled)",
)
@click.option(
    "--scope", default=None, help="Discovery scope (local, zone, region, global)"
)
@click.option(
    "--include-ingress/--no-include-ingress",
    default=False,
    help="Include ingress hints for source clusters",
)
@click.option("--ingress-limit", type=int, default=None, help="Ingress URL limit")
@click.option(
    "--ingress-scope",
    default=None,
    help="Ingress scope filter (local, zone, region, global)",
)
@click.option(
    "--ingress-capability",
    "ingress_capabilities",
    multiple=True,
    help="Ingress capability filter (repeatable)",
)
@click.option(
    "--ingress-tag",
    "ingress_tags",
    multiple=True,
    help="Ingress tag filter (repeatable)",
)
@click.option("--limit", type=int, default=None, help="Page size limit")
@click.option("--page-token", default=None, help="Pagination token")
def discovery_summary(
    url: str | None,
    namespace: str | None,
    service_id: str | None,
    viewer_cluster_id: str | None,
    viewer_tenant_id: TenantId | None,
    scope: str | None,
    include_ingress: bool,
    ingress_limit: int | None,
    ingress_scope: str | None,
    ingress_capabilities: tuple[str, ...],
    ingress_tags: tuple[str, ...],
    limit: int | None,
    page_token: str | None,
):
    """Run a summary_query for discovery summaries."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _summary() -> None:
        effective_include_ingress = include_ingress or bool(
            ingress_limit is not None
            or ingress_scope
            or ingress_capabilities
            or ingress_tags
        )
        async with MPREGClientAPI(url) as client:
            request = SummaryQueryRequest(
                scope=scope,
                namespace=namespace,
                service_id=service_id,
                viewer_cluster_id=viewer_cluster_id,
                viewer_tenant_id=viewer_tenant_id,
                include_ingress=effective_include_ingress,
                ingress_limit=ingress_limit,
                ingress_scope=ingress_scope,
                ingress_capabilities=ingress_capabilities,
                ingress_tags=ingress_tags,
                limit=limit,
                page_token=page_token,
            )
            result = await client.summary_query(request)
            console.print(result.to_dict())

    run_coro(_summary())

@discovery.command("watch")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option(
    "--summary/--delta",
    default=False,
    help="Watch summary exports instead of catalog deltas",
)
@click.option(
    "--scope", default=None, help="Discovery scope (local, zone, region, global)"
)
@click.option("--namespace", default=None, help="Namespace filter")
@click.option("--cluster-id", default=None, help="Cluster ID filter")
@click.option("--viewer-tenant-id", default=None, help="Viewer tenant id")
@click.option("--duration", type=float, default=None, help="Stop after N seconds")
@click.option("--count", type=int, default=None, help="Stop after N messages")
def discovery_watch(
    url: str | None,
    summary: bool,
    scope: str | None,
    namespace: str | None,
    cluster_id: str | None,
    viewer_tenant_id: TenantId | None,
    duration: float | None,
    count: int | None,
):
    """Watch discovery delta or summary topics."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _watch() -> None:
        async with MPREGClientAPI(url) as client:
            pubsub = MPREGPubSubClient(base_client=client)
            await pubsub.start()
            queue: asyncio.Queue = asyncio.Queue()

            def on_message(message):
                queue.put_nowait(message)

            watch_request = (
                SummaryWatchRequest(
                    scope=scope,
                    namespace=namespace,
                    cluster_id=cluster_id,
                    viewer_tenant_id=viewer_tenant_id,
                )
                if summary
                else CatalogWatchRequest(
                    scope=scope,
                    namespace=namespace,
                    cluster_id=cluster_id,
                    viewer_tenant_id=viewer_tenant_id,
                )
            )
            watch_info = (
                await client.summary_watch(watch_request)
                if summary
                else await client.catalog_watch(watch_request)
            )
            await pubsub.subscribe(
                patterns=[watch_info.topic],
                callback=on_message,
                get_backlog=False,
            )

            deadline = time.time() + duration if duration else None
            received = 0
            while True:
                timeout = None
                if deadline is not None:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        break
                    timeout = max(0.1, remaining)
                try:
                    if timeout is None:
                        message = await queue.get()
                    else:
                        message = await asyncio.wait_for(queue.get(), timeout=timeout)
                except TimeoutError:
                    break
                console.print(
                    {
                        "topic": message.topic,
                        "timestamp": message.timestamp,
                        "payload": message.payload,
                    }
                )
                received += 1
                if count is not None and received >= count:
                    break

            await pubsub.stop()

    run_coro(_watch())

@discovery.command("status")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring endpoint URL (or set MPREG_MONITORING_URL)",
)
def discovery_status(url: str | None) -> None:
    """Fetch discovery status from monitoring endpoints."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_MONITORING_URL.")

    async def _status() -> None:
        base_url = url.rstrip("/")
        endpoints = {
            "summary": "/discovery/summary",
            "cache": "/discovery/cache",
            "policy": "/discovery/policy",
            "lag": "/discovery/lag",
        }
        results: JsonDict = {}
        async with aiohttp.ClientSession() as session:
            for key, path in endpoints.items():
                async with session.get(f"{base_url}{path}") as response:
                    results[key] = await response.json()
        console.print(results)

    run_coro(_status())

@discovery.command("access-audit")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_URL",
    help="MPREG server URL (or set MPREG_URL)",
)
@click.option("--limit", type=int, default=None, help="Limit audit entries")
def discovery_access_audit(url: str | None, limit: int | None) -> None:
    """Fetch discovery access audit entries."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_URL.")

    async def _audit() -> None:
        async with MPREGClientAPI(url) as client:
            request = DiscoveryAccessAuditRequest(limit=limit)
            result = await client.discovery_access_audit(request)
            console.print(result.to_dict())

    run_coro(_audit())

@report.command("namespace-health")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring endpoint URL (or set MPREG_MONITORING_URL)",
)
@click.option(
    "--output",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
@click.option("--namespace", default=None, help="Namespace prefix filter")
@click.option("--limit", type=int, default=None, help="Limit rows displayed")
def report_namespace_health(
    url: str | None,
    output: str,
    namespace: str | None,
    limit: int | None,
) -> None:
    """Report namespace export health from discovery summary metrics."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_MONITORING_URL.")

    async def _report() -> None:
        base_url = url.rstrip("/")
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base_url}/discovery/summary") as response:
                payload = await response.json()
        summary_export = payload.get("summary_export", {})
        entries = summary_export.get("per_namespace", [])
        if not isinstance(entries, list):
            entries = []
        if namespace:
            entries = [
                entry
                for entry in entries
                if isinstance(entry, dict)
                and str(entry.get("namespace", "")).startswith(namespace)
            ]
        entries = [
            entry
            for entry in entries
            if isinstance(entry, dict) and entry.get("namespace")
        ]
        entries.sort(
            key=lambda item: (
                -int(item.get("summaries_exported", 0)),
                item.get("namespace"),
            )
        )
        if limit is not None and limit >= 0:
            entries = entries[:limit]

        if output == "json":
            console.print(
                {
                    "summary_export": summary_export,
                    "namespaces": entries,
                }
            )
            return

        table = Table(title="Namespace Export Health")
        table.add_column("Namespace")
        table.add_column("Exports", justify="right")
        table.add_column("Summaries", justify="right")
        table.add_column("Last Export Count", justify="right")
        table.add_column("Last Export At")

        for entry in entries:
            last_export_at = entry.get("last_export_at")
            if isinstance(last_export_at, (int, float)):
                last_export = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(float(last_export_at))
                )
            else:
                last_export = "-"
            table.add_row(
                str(entry.get("namespace", "")),
                str(entry.get("exports_total", 0)),
                str(entry.get("summaries_exported", 0)),
                str(entry.get("last_export_count", 0)),
                last_export,
            )

        console.print(table)

    run_coro(_report())

@report.command("export-lag")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring endpoint URL (or set MPREG_MONITORING_URL)",
)
@click.option(
    "--output",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
def report_export_lag(url: str | None, output: str) -> None:
    """Report summary export lag from discovery metrics."""
    if not url:
        raise click.UsageError("Provide --url or set MPREG_MONITORING_URL.")

    async def _report() -> None:
        base_url = url.rstrip("/")
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base_url}/discovery/lag") as response:
                payload = await response.json()
        lag = payload.get("lag", {})
        if output == "json":
            console.print(lag)
            return

        table = Table(title="Discovery Export Lag")
        table.add_column("Field")
        table.add_column("Value")
        table.add_row("Resolver Enabled", str(lag.get("resolver_enabled")))
        table.add_row("Summary Export Enabled", str(lag.get("summary_export_enabled")))
        table.add_row(
            "Delta Lag (s)",
            str(lag.get("delta_lag_seconds", "n/a")),
        )
        table.add_row(
            "Summary Export Lag (s)",
            str(lag.get("summary_export_lag_seconds", "n/a")),
        )
        last_delta_at = lag.get("last_delta_at")
        if isinstance(last_delta_at, (int, float)):
            last_delta = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(float(last_delta_at))
            )
        else:
            last_delta = "-"
        last_summary_at = lag.get("last_summary_export_at")
        if isinstance(last_summary_at, (int, float)):
            last_summary = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(float(last_summary_at))
            )
        else:
            last_summary = "-"
        table.add_row("Last Delta At", last_delta)
        table.add_row("Last Summary Export At", last_summary)
        console.print(table)

    run_coro(_report())

@cli.group()
def server():
    """Server management commands."""

@server.command("start")
@click.option("--host", default="127.0.0.1", help="Host to bind")
@click.option(
    "--port",
    default=None,
    type=int,
    help="Port to bind (auto-allocate when omitted)",
)
@click.option("--name", default="MPREG Server", help="Server name")
@click.option("--resource", "resources", multiple=True, help="Server resource tag")
@click.option("--peer", "peers", multiple=True, help="Static peer URL")
@click.option("--connect", default=None, help="Peer URL to connect on startup")
@click.option("--cluster-id", default="default-cluster", help="Cluster ID")
@click.option(
    "--advertised-url",
    "advertised_urls",
    multiple=True,
    help="Advertised URL for inbound connections",
)
@click.option("--enable-cache", is_flag=True, help="Enable default cache stack")
@click.option("--enable-queue", is_flag=True, help="Enable default queue manager")
@click.option(
    "--enable-cache-federation",
    is_flag=True,
    help="Enable L4 fabric cache federation replication",
)
@click.option("--cache-region", default="local", help="Cache region name")
@click.option("--cache-latitude", type=float, default=0.0, help="Cache latitude")
@click.option("--cache-longitude", type=float, default=0.0, help="Cache longitude")
@click.option("--cache-capacity-mb", type=int, default=512, help="Cache capacity in MB")
@click.option(
    "--monitoring-port",
    type=int,
    default=None,
    help="Monitoring HTTP port (auto-allocate when omitted)",
)
@click.option(
    "--monitoring-host",
    default=None,
    help="Monitoring HTTP host (defaults to server host)",
)
@click.option(
    "--monitoring/--no-monitoring",
    default=True,
    help="Enable or disable monitoring endpoints",
)
@click.option(
    "--monitoring-cors/--no-monitoring-cors",
    default=False,
    help="Enable CORS for monitoring endpoints (off by default; enable only for browser UIs)",
)
@click.option(
    "--monitoring-token",
    default=None,
    envvar="MPREG_MONITORING_TOKEN",
    help="Bearer token required for monitoring HTTP endpoints (or set MPREG_MONITORING_TOKEN)",
)
@click.option(
    "--persistence-mode",
    type=click.Choice(["off", "memory", "sqlite"]),
    default="off",
    help="Enable unified persistence (memory or sqlite)",
)
@click.option(
    "--persistence-dir",
    type=click.Path(),
    default=None,
    help="Persistence data directory (sqlite)",
)
@click.option(
    "--persistence-sqlite-filename",
    type=str,
    default="mpreg.sqlite",
    help="SQLite filename for persistence backend",
)
@click.option(
    "--json-logs",
    is_flag=True,
    help="Emit structured JSON logs from the server process",
)
@click.pass_context
def start_server(
    ctx: click.Context,
    host: str,
    port: int | None,
    name: str,
    resources: tuple[str, ...],
    peers: tuple[str, ...],
    connect: str | None,
    cluster_id: str,
    advertised_urls: tuple[str, ...],
    enable_cache: bool,
    enable_queue: bool,
    enable_cache_federation: bool,
    cache_region: str,
    cache_latitude: float,
    cache_longitude: float,
    cache_capacity_mb: int,
    monitoring_port: int | None,
    monitoring_host: str | None,
    monitoring: bool,
    monitoring_cors: bool,
    monitoring_token: str | None,
    persistence_mode: str,
    persistence_dir: str | None,
    persistence_sqlite_filename: str,
    json_logs: bool,
):
    """Start an MPREG server."""

    async def _start():
        selected_port = port if port not in (None, 0) else allocate_port("servers")
        selected_monitoring_port = monitoring_port
        if monitoring and selected_monitoring_port in (None, 0):
            selected_monitoring_port = allocate_port("monitoring")

        server_url = f"ws://{host}:{selected_port}"
        console.print(f"Server endpoint: {server_url}")
        console.print(f"MPREG_URL={server_url}")
        if monitoring:
            monitor_host = monitoring_host or host
            monitor_url = f"http://{monitor_host}:{selected_monitoring_port}"
            console.print(f"Monitoring endpoint: {monitor_url}")
            console.print(f"MPREG_MONITORING_URL={monitor_url}")

        persistence_config = None
        if persistence_mode != "off":
            data_dir = Path(persistence_dir) if persistence_dir else None
            persistence_config = PersistenceConfig(
                mode=PersistenceMode(persistence_mode),
                data_dir=data_dir or PersistenceConfig().data_dir,
                sqlite_filename=persistence_sqlite_filename,
            )

        settings = MPREGSettings(
            host=host,
            port=selected_port,
            name=name,
            resources=set(resources) if resources else None,
            peers=list(peers) if peers else None,
            connect=connect,
            cluster_id=cluster_id,
            advertised_urls=tuple(advertised_urls) if advertised_urls else None,
            gossip_interval=5.0,
            log_level="INFO",
            enable_default_cache=enable_cache,
            enable_default_queue=enable_queue,
            enable_cache_federation=enable_cache_federation,
            cache_region=cache_region,
            cache_latitude=cache_latitude,
            cache_longitude=cache_longitude,
            cache_capacity_mb=cache_capacity_mb,
            monitoring_port=selected_monitoring_port,
            monitoring_host=monitoring_host,
            monitoring_enabled=monitoring,
            monitoring_enable_cors=monitoring_cors,
            monitoring_auth_token=monitoring_token,
            persistence_config=persistence_config,
            json_logs=bool(json_logs or (ctx.obj or {}).get("json_logs")),
        )
        server_instance = MPREGServer(settings=settings)
        await server_instance.server()

    run_coro(_start())

@server.command("start-config")
@click.argument("settings_path", type=click.Path(exists=True))
def start_config(settings_path: str) -> None:
    """Start an MPREG server from a JSON or TOML settings file."""

    async def _start() -> None:
        settings = MPREGSettings.from_path(settings_path)
        # Print effective endpoints before bind (ports may still auto-allocate).
        host = settings.host or "127.0.0.1"
        port = settings.port
        if port in (None, 0):
            console.print(
                "[yellow]Server port will auto-allocate; "
                "watch logs for final MPREG_URL[/yellow]"
            )
        else:
            server_url = f"ws://{host}:{port}"
            console.print(f"Server endpoint: {server_url}")
            console.print(f"MPREG_URL={server_url}")
        if settings.monitoring_enabled:
            mon_host = settings.monitoring_host or host
            mon_port = settings.monitoring_port
            if mon_port in (None, 0):
                console.print(
                    "[yellow]Monitoring port will auto-allocate; "
                    "watch logs for final MPREG_MONITORING_URL[/yellow]"
                )
            else:
                mon_url = f"http://{mon_host}:{mon_port}"
                console.print(f"Monitoring endpoint: {mon_url}")
                console.print(f"MPREG_MONITORING_URL={mon_url}")
        server_instance = MPREGServer(settings=settings)
        await server_instance.server()

    run_coro(_start())

@cli.command("doctor")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@click.option(
    "--token",
    default=None,
    envvar="MPREG_MONITORING_TOKEN",
    help="Monitoring bearer token if auth is enabled",
)
@click.option(
    "--timeout",
    type=float,
    default=5.0,
    help="HTTP timeout seconds",
)
@click.option(
    "--deep",
    is_flag=True,
    default=False,
    help="Also probe Raft status and link-state (DS validation deep checks)",
)
@click.option(
    "--data-plane",
    "data_plane",
    is_flag=True,
    default=False,
    help="Also smoke RPC echo via MPREG_URL / --rpc-url (USE-T10-02)",
)
@click.option(
    "--strong",
    "check_strong",
    is_flag=True,
    default=False,
    help="Also probe /metrics/strong and fail on critical health",
)
@click.option(
    "--audit",
    "check_audit",
    is_flag=True,
    default=False,
    help="Also probe /metrics/shared-audit and fail on critical status",
)
@click.option(
    "--rpc-url",
    default=None,
    envvar="MPREG_URL",
    help="WebSocket RPC URL for --data-plane smoke (or MPREG_URL)",
)
@add_format_option
def doctor(
    url: str | None,
    token: str | None,
    timeout: float,
    output_format: str,
    deep: bool,
    data_plane: bool,
    check_strong: bool,
    check_audit: bool,
    rpc_url: str | None,
) -> None:
    """Probe monitoring health, discovery, and persistence endpoints."""

    monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
    if not monitoring_url:
        raise click.UsageError(
            "Provide --url or set MPREG_MONITORING_URL to the monitoring HTTP base "
            "(e.g. http://127.0.0.1:9090). WebSocket RPC URLs belong in --rpc-url / MPREG_URL."
        )
    # Phase H F3: fail closed with a clear hint when operators pass a WS RPC URL.
    _mu = monitoring_url.strip().lower()
    if _mu.startswith(("ws://", "wss://")):
        raise click.UsageError(
            f"--url looks like a WebSocket RPC endpoint ({monitoring_url!r}). "
            "mpreg doctor probes the *monitoring HTTP* base "
            "(e.g. http://127.0.0.1:9090), not the WS data plane. "
            "Pass the monitoring URL as --url, and use --rpc-url / MPREG_URL "
            "with --data-plane for RPC smoke."
        )

    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    async def _doctor() -> int:
        base = monitoring_url.rstrip("/")
        checks = [
            ("live", f"{base}/live"),
            ("ready", f"{base}/ready"),
            ("health", f"{base}/health"),
            ("health_summary", f"{base}/health/summary"),
            ("metrics_unified", f"{base}/metrics/unified"),
            ("persistence", f"{base}/metrics/persistence"),
            ("discovery_cache", f"{base}/discovery/cache"),
            ("discovery_policy", f"{base}/discovery/policy"),
            ("prometheus", f"{base}/metrics/prometheus"),
            ("route_decisions", f"{base}/routing/decisions?limit=5"),
            ("mgmt_cluster", f"{base}/mgmt/v1/cluster"),
            ("mgmt_catalog", f"{base}/mgmt/v1/catalog"),
            ("endpoints", f"{base}/endpoints"),
            ("openapi", f"{base}/openapi.json"),
            ("mgmt_audit", f"{base}/mgmt/v1/audit"),
        ]
        if deep:
            checks.extend(
                [
                    ("mgmt_raft", f"{base}/mgmt/v1/raft"),
                    ("link_state", f"{base}/routing/link-state"),
                ]
            )
        if check_strong:
            checks.extend(
                [
                    ("metrics_strong", f"{base}/metrics/strong"),
                    ("mgmt_strong", f"{base}/mgmt/v1/strong"),
                ]
            )
        if check_audit:
            checks.append(("metrics_shared_audit", f"{base}/metrics/shared-audit"))
        failures = 0
        rows: list[dict[str, str]] = []
        timeout_cfg = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(
            timeout=timeout_cfg, headers=headers
        ) as session:
            for name, endpoint in checks:
                try:
                    async with session.get(endpoint) as response:
                        body_preview = ""
                        payload: object | None = None
                        ctype = response.headers.get("Content-Type", "")
                        if "json" in ctype:
                            payload = await response.json(content_type=None)
                            body_preview = str(payload)[:120]
                        else:
                            text_body = await response.text()
                            body_preview = (
                                text_body.splitlines()[0][:120] if text_body else ""
                            )
                            payload = text_body
                        ok = 200 <= response.status < 300
                        # Deep optional planes: 503 means feature not wired — warn only.
                        if (
                            deep
                            and name in ("mgmt_raft", "link_state")
                            and response.status == 503
                        ):
                            ok = True
                            body_preview = f"optional unavailable: {body_preview}"
                        # Semantic checks (not just HTTP 2xx)
                        if ok and name == "ready" and isinstance(payload, dict):
                            if payload.get("ready") is False:
                                ok = False
                                body_preview = f"not ready: {body_preview}"
                        if ok and name == "health" and isinstance(payload, dict):
                            # Nested critical still passes liveness HTTP 200; surface it.
                            fh = payload.get("federation_health") or {}
                            if isinstance(fh, dict):
                                ost = str(fh.get("overall_status", "")).lower()
                                if ost in {"critical", "unavailable", "unhealthy"}:
                                    body_preview = (
                                        f"liveness ok but federation {ost}: "
                                        f"{body_preview}"
                                    )
                                    # Do not fail plain doctor on nested status —
                                    # /ready is the admission gate. Mark WARN via detail.
                        if ok and name == "prometheus" and isinstance(payload, str):
                            if "mpreg_info" not in payload:
                                ok = False
                                body_preview = "missing mpreg_info metric"
                        residual_hint = ""
                        if (
                            ok
                            and name in ("metrics_strong", "mgmt_strong")
                            and isinstance(payload, dict)
                        ):
                            sok, sdetail = evaluate_strong_doctor_payload(payload)
                            if not sok:
                                ok = False
                            body_preview = sdetail
                            # T71: machine-readable residual_ops_hint on doctor JSON rows
                            sbody = (
                                payload.get("strong")
                                if isinstance(payload.get("strong"), dict)
                                else payload
                            )
                            if isinstance(sbody, dict):
                                residual_hint = strong_residual_ops_hint(sbody)
                        if (
                            ok
                            and name == "metrics_shared_audit"
                            and isinstance(payload, dict)
                        ):
                            aok, adetail = evaluate_shared_audit_doctor_payload(
                                payload
                            )
                            if not aok:
                                ok = False
                            body_preview = adetail
                        if (
                            deep
                            and ok
                            and name == "mgmt_raft"
                            and isinstance(payload, dict)
                            and response.status == 200
                        ):
                            # When raft is intentionally unbound, configured=false is OK.
                            # When profile claims HA raft, operators should use --deep
                            # and inspect configured; we only fail if body is empty.
                            if "configured" not in payload and "nodes" not in payload:
                                ok = False
                                body_preview = f"unexpected raft body: {body_preview}"
                        if not ok:
                            failures += 1
                        row: dict[str, str] = {
                            "check": name,
                            "status": "OK" if ok else str(response.status),
                            "detail": body_preview,
                        }
                        # T71: always key on strong checks (empty when no residual)
                        if name in ("metrics_strong", "mgmt_strong"):
                            row["residual_ops_hint"] = residual_hint
                        rows.append(row)
                except Exception as exc:  # noqa: BLE001 - doctor must report all failures
                    failures += 1
                    rows.append(
                        {
                            "check": name,
                            "status": "ERROR",
                            "detail": str(exc)[:120],
                        }
                    )
        if data_plane:
            target = (rpc_url or "").strip()
            if not target:
                failures += 1
                rows.append(
                    {
                        "check": "data_plane",
                        "status": "ERROR",
                        "detail": "set --rpc-url or MPREG_URL for --data-plane",
                    }
                )
            else:
                try:
                    from mpreg.client.unified_client import MPREGClient

                    async with MPREGClient(target) as client:
                        # Lightweight connectivity: empty DAG / status-style call may
                        # 1001; success is establishing session + round-trip.
                        try:
                            await client.call(
                                "mpreg.system.echo", "doctor", timeout=timeout
                            )
                            detail = "echo ok"
                            ok_dp = True
                        except Exception as exc:  # noqa: BLE001
                            msg = str(exc).lower()
                            # Command-not-found still proves data-plane RPC path works.
                            if "not found" in msg or "1001" in msg:
                                detail = f"rpc reachable ({exc})"
                                ok_dp = True
                            else:
                                detail = str(exc)[:120]
                                ok_dp = False
                    if ok_dp:
                        rows.append(
                            {
                                "check": "data_plane",
                                "status": "OK",
                                "detail": detail,
                            }
                        )
                    else:
                        failures += 1
                        rows.append(
                            {
                                "check": "data_plane",
                                "status": "ERROR",
                                "detail": detail,
                            }
                        )
                except Exception as exc:  # noqa: BLE001
                    failures += 1
                    rows.append(
                        {
                            "check": "data_plane",
                            "status": "ERROR",
                            "detail": str(exc)[:120],
                        }
                    )

        report = {
            "base": base,
            "failures": failures,
            "passed": failures == 0,
            "checks": rows,
        }
        if output_format.lower() == "table":
            table = Table(title=f"MPREG doctor — {base}")
            table.add_column("Check")
            table.add_column("Status")
            table.add_column("Detail")
            for row in rows:
                status = row["status"]
                status_cell = (
                    f"[green]{status}[/green]"
                    if status == "OK"
                    else f"[red]{status}[/red]"
                )
                table.add_row(row["check"], status_cell, row["detail"])
            console.print(table)
            if failures:
                console.print(
                    f"[red]doctor failed: {failures} check(s) unsuccessful[/red]"
                )
            else:
                console.print("[green]doctor passed[/green]")
        else:
            emit(report, output_format=output_format, table_title="Doctor")
        return 1 if failures else 0

    raise SystemExit(run_coro(_doctor()))

@cli.command("config-check")
@click.argument("settings_path", type=click.Path(exists=True))
@click.option(
    "--strict",
    is_flag=True,
    default=False,
    help="ERG-T13-01: exit 2 on any warning (CI production gate). "
    "Default exits 0 with warnings listed (lab_ok).",
)
@click.option(
    "--explain",
    is_flag=True,
    default=False,
    help="ERG Phase Q: print a human field guide for each settings group "
    "(what the knobs mean and how they interact) after the structured report.",
)
@add_format_option
def config_check(
    settings_path: str, output_format: str, strict: bool, explain: bool
) -> None:
    """Validate a settings file and report grouped configuration summary.

    Exit codes (ERG-T13-01 / USE-T13-02):
      0 — ok or lab_ok (warnings present, non-strict)
      1 — load/parse failure (raised by Click / from_path)
      2 — strict mode with warnings, or fatal config errors

    Use ``--explain`` for operator-oriented field discoverability (Phase Q / ERG).
    """
    settings = MPREGSettings.from_path(settings_path)
    groups = {
        "identity": {
            "name": settings.name,
            "cluster_id": settings.cluster_id,
            "host": settings.host,
            "port": settings.port,
        },
        "monitoring": {
            "enabled": settings.monitoring_enabled,
            "port": settings.monitoring_port,
            "cors": settings.monitoring_enable_cors,
            "auth_configured": bool(settings.monitoring_auth_token),
        },
        "fabric": {
            "routing_enabled": settings.fabric_routing_enabled,
            "catalog_ttl": settings.fabric_catalog_ttl_seconds,
            "route_ttl": settings.fabric_route_ttl_seconds,
            "link_state_mode": str(settings.fabric_link_state_mode),
            "route_require_signatures": bool(
                settings.fabric_route_security_config
                and settings.fabric_route_security_config.require_signatures
            ),
            "route_allow_unsigned": (
                True
                if settings.fabric_route_security_config is None
                else bool(settings.fabric_route_security_config.allow_unsigned)
            ),
        },
        "discovery": {
            "resolver_mode": settings.discovery_resolver_mode,
            "summary_export": settings.discovery_summary_export_enabled,
            "summary_signing": bool(settings.discovery_summary_signing_secret),
            "policy_enabled": settings.discovery_policy_enabled,
            "tenant_mode": settings.discovery_tenant_mode,
        },
        "systems": {
            "cache": settings.enable_default_cache,
            "queue": settings.enable_default_queue,
            "cache_federation": settings.enable_cache_federation,
        },
        "strong_cache": {
            "enabled": bool(getattr(settings, "cache_strong_enabled", False)),
            "replica_factor": getattr(settings, "cache_strong_replica_factor", None),
            "min_replicas": getattr(settings, "cache_strong_min_replicas", None),
            "lab_single_node": bool(
                getattr(settings, "cache_strong_lab_single_node", False)
            ),
            "prepare_timeout_s": getattr(
                settings, "cache_strong_prepare_timeout_s", None
            ),
            "commit_timeout_s": getattr(
                settings, "cache_strong_commit_timeout_s", None
            ),
            "pending_ttl_s": getattr(settings, "cache_strong_pending_ttl_s", None),
            # Honesty: v1 put-only MVP (not advertised as product get/delete quorum)
            "capabilities": {
                "put_majority_commit": bool(
                    getattr(settings, "cache_strong_enabled", False)
                ),
                "get_quorum": False,
                "delete_quorum": False,
                "local_ryw_after_put": True,
                "cft_only": True,
                "abort_best_effort": True,
                "pending_ttl_clears_residual_l1": False,
                "retry_abort_ops_driven": True,
            },
        },
        "shared_audit": {
            "enabled": bool(getattr(settings, "mgmt_audit_shared_enabled", False)),
            "audit_path": getattr(settings, "mgmt_audit_path", None),
            "max_entries": getattr(settings, "mgmt_audit_shared_max_entries", None),
            "gossip_targets": getattr(
                settings, "mgmt_audit_shared_gossip_targets", None
            ),
            "reconcile_interval_s": getattr(
                settings, "mgmt_audit_shared_reconcile_interval_s", None
            ),
            # Honesty: v1 G-Set epidemic (not SIEM/BFT/infinite retention)
            "capabilities": {
                "gset_epidemic": bool(
                    getattr(settings, "mgmt_audit_shared_enabled", False)
                ),
                "siem": False,
                "bft": False,
                "infinite_retention": False,
                "linearizable_cluster_ops": False,
                "multi_tenant_beyond_cluster_id": False,
            },
        },
        "persistence": (
            {
                "mode": settings.persistence_config.mode.value,
                "data_dir": str(settings.persistence_config.data_dir),
            }
            if settings.persistence_config
            else {"enabled": False}
        ),
    }
    warnings: list[str] = []
    if settings.monitoring_enabled and settings.monitoring_enable_cors:
        warnings.append(
            "monitoring CORS is enabled — disable in production unless needed"
        )
    if settings.monitoring_enabled and not settings.monitoring_auth_token:
        warnings.append(
            "monitoring has no auth token — set monitoring_auth_token for production"
        )
    mon_host = str(getattr(settings, "monitoring_host", None) or settings.host or "")
    if (
        settings.monitoring_enabled
        and mon_host not in ("127.0.0.1", "localhost", "::1")
        and not settings.monitoring_auth_token
    ):
        warnings.append(
            "monitoring bound on non-loopback host without monitoring_auth_token "
            f"(host={mon_host!r}) — set token or monitoring_host=127.0.0.1 (ERG-T15-01)"
        )
    if not settings.enable_default_queue or not settings.enable_default_cache:
        missing = []
        if not settings.enable_default_queue:
            missing.append("queue")
        if not settings.enable_default_cache:
            missing.append("cache")
        warnings.append(
            "four-plane incomplete: enable_default_"
            + "/enable_default_".join(missing)
            + " false — MPREGClient plane RPCs need managers "
            "(--enable-queue / --enable-cache or a profile such as dev.toml)"
        )
    if (
        settings.discovery_summary_export_enabled
        and not settings.discovery_summary_signing_secret
    ):
        warnings.append("summary export enabled without signing secret")
    sec = settings.fabric_route_security_config
    if settings.enable_cache_federation or (settings.peers and len(settings.peers) > 0):
        if sec is None or not sec.require_signatures:
            warnings.append(
                "federated/multi-peer fabric without fabric_route_require_signatures=true"
            )
        if not getattr(settings, "fabric_gossip_require_hmac", False):
            warnings.append(
                "gossip envelopes are unsigned by default "
                "(fabric_gossip_require_hmac=false) — route signing ≠ gossip authenticity"
            )
    if settings.persistence_config is not None and not getattr(
        settings, "fabric_snapshot_fail_on_restore_error", False
    ):
        warnings.append(
            "fabric snapshot restore is best-effort "
            "(fabric_snapshot_fail_on_restore_error=false)"
        )
    if not getattr(settings, "mgmt_audit_path", None):
        warnings.append(
            "mgmt audit is process-local only — set mgmt_audit_path for JSONL durability"
        )
    if sec is not None and sec.allow_unsigned:
        warnings.append("fabric_route_allow_unsigned=true weakens route authenticity")
    if (
        settings.discovery_summary_signing_secret
        and settings.discovery_summary_signing_secret.startswith("change-me")
    ):
        warnings.append(
            "discovery_summary_signing_secret is still the profile placeholder"
        )
    gossip_secret = getattr(settings, "fabric_gossip_hmac_secret", None)
    if (
        gossip_secret
        and isinstance(gossip_secret, str)
        and gossip_secret.startswith("change-me")
    ):
        warnings.append("fabric_gossip_hmac_secret is still the profile placeholder")
    if getattr(settings, "fabric_gossip_require_hmac", False) and not gossip_secret:
        warnings.append(
            "fabric_gossip_require_hmac=true without fabric_gossip_hmac_secret"
        )
    # COR-09 / multi-tenant: federated peers without discovery policy leave
    # namespace gates off (lab default). Warn so operators do not ship open CP.
    if (
        (settings.peers and len(settings.peers) > 0)
        or settings.enable_cache_federation
        or getattr(settings, "fabric_routing_enabled", False)
    ) and not getattr(settings, "discovery_policy_enabled", False):
        warnings.append(
            "discovery_policy_enabled=false on a federated/multi-peer node — "
            "namespace/tenant gates are off (lab default; enable for multi-tenant). "
            "Use federated.toml only as a lab baseline or set discovery_policy_enabled=true"
        )
    # ERG-T10-12: federated profile name with placeholders is a prod footgun.
    name = str(getattr(settings, "name", "") or "")
    if name.startswith("federated") and (
        (
            settings.discovery_summary_signing_secret
            and str(settings.discovery_summary_signing_secret).startswith("change-me")
        )
        or (
            getattr(settings, "fabric_gossip_hmac_secret", None)
            and str(getattr(settings, "fabric_gossip_hmac_secret", "")).startswith(
                "change-me"
            )
        )
    ):
        warnings.append(
            "federated profile still uses change-me placeholder secrets — "
            "rotate before any shared deployment (ERG-T10-12)"
        )
    # T20: STRONG + shared audit config honesty
    strong_on = bool(getattr(settings, "cache_strong_enabled", False))
    if strong_on and not getattr(settings, "enable_default_cache", False):
        warnings.append(
            "cache_strong_enabled=true but enable_default_cache=false — "
            "STRONG coordinator will not bind without a cache manager"
        )
    if strong_on:
        rf = int(getattr(settings, "cache_strong_replica_factor", 3) or 3)
        mr = int(getattr(settings, "cache_strong_min_replicas", 3) or 3)
        if mr > rf:
            warnings.append(
                f"cache_strong_min_replicas ({mr}) > cache_strong_replica_factor ({rf})"
            )
        if bool(getattr(settings, "cache_strong_lab_single_node", False)) and mr > 1:
            warnings.append(
                "cache_strong_lab_single_node=true with min_replicas>1 — "
                "lab mode is single-origin only (not multi-replica SLA)"
            )
        if not settings.monitoring_enabled:
            warnings.append(
                "cache_strong_enabled without monitoring_enabled — "
                "operators cannot scrape /metrics/strong (lab ok; enable for ops)"
            )
        warnings.append(
            "STRONG is put-only MVP: get/delete quorum are not implemented "
            "(always 1012); local RYW uses EVENTUAL/WEAK get after put "
            "(not WAN SLA, not BFT, not fsync)"
        )
    audit_shared = bool(getattr(settings, "mgmt_audit_shared_enabled", False))
    if audit_shared and not getattr(settings, "mgmt_audit_path", None):
        warnings.append(
            "mgmt_audit_shared_enabled=true without mgmt_audit_path — "
            "shared epidemic has no local JSONL durability anchor"
        )
    if audit_shared and not settings.monitoring_enabled:
        warnings.append(
            "mgmt_audit_shared_enabled without monitoring_enabled — "
            "operators cannot scrape /metrics/shared-audit"
        )
    if audit_shared:
        warnings.append(
            "shared audit is a bounded G-Set epidemic (not SIEM, not BFT, "
            "not infinite retention)"
        )
    # ERG-T13-01: severity tiers — stock profiles are lab_ok by default.
    status = "ok" if not warnings else "lab_ok"
    explain_guide = {
        "identity": (
            "Node name + cluster_id identify this process in gossip/catalog. "
            "host/port are the WebSocket RPC listen address (not monitoring)."
        ),
        "monitoring": (
            "HTTP ops plane (health/metrics/mgmt). Prefer loopback + "
            "monitoring_auth_token outside lab. CORS off in production."
        ),
        "fabric": (
            "Cross-cluster routing/catalog TTLs and route signature policy. "
            "require_signatures + gossip HMAC for multi-peer production."
        ),
        "discovery": (
            "Resolver mode, summary export/signing, and tenant/namespace policy. "
            "Enable discovery_policy_enabled for multi-tenant gates."
        ),
        "systems": (
            "Default cache/queue managers for four-plane MPREGClient RPCs. "
            "Profiles (dev.toml) turn both on; bare defaults leave them off."
        ),
        "strong_cache": (
            "Flag-gated ConsistencyLevel.STRONG put majority-commit "
            "(cache_strong_enabled). Default off → 1012. Put-only MVP: get/delete "
            "always refuse 1012; local RYW via EVENTUAL/WEAK get. CFT only: "
            "ABORT is best-effort (aborts_peer_fail may leave peer L1 until "
            "delivered ABORT or later LWW success put — not pending TTL). "
            "Ops loop after recovery: scrape /metrics/strong for "
            "last_abort_fail_peers / last_abort_fail_op_id / residual_ops_hint "
            "(may fill --namespace/--key from recent_abort_fails), then "
            "`mpreg client cache-strong-retry-abort` (ops-driven CFT; not "
            "auto-heal). Not WAN SLA, not BFT, not fsync. "
            "See docs/CACHING_SYSTEM.md and residual honesty."
        ),
        "shared_audit": (
            "Shared mgmt audit G-Set epidemic (mgmt_audit_shared_enabled). "
            "Requires mgmt_audit_path for durable local JSONL. Bounded watermark "
            "window — not SIEM, not BFT, not infinite retention, not linearizable "
            "cluster ops. capabilities.siem/bft/… always false. "
            "Scrape /metrics/shared-audit when mon on."
        ),
        "persistence": (
            "Unified persistence (memory|sqlite today). remote SQL/other stores backends "
            "are not shipped — see PERSISTENCE_FRAMEWORK_PLAN. data_dir holds sqlite files."
        ),
        "warnings": (
            "lab_ok means safe for curriculum/local; use --strict (exit 2) as a "
            "CI production gate. Set mgmt_audit_path for durable JSONL audit. "
            "STRONG/shared-audit honesty warnings are expected when those flags are on."
        ),
    }
    report = {
        "groups": groups,
        "warnings": warnings,
        "ok": len(warnings) == 0,
        "status": status,
        "strict": bool(strict),
        "explain": bool(explain),
    }
    if explain:
        report["guide"] = explain_guide
    emit(report, output_format=output_format, table_title="Config check")
    if explain:
        # Human field guide always on stderr-safe stdout after structured emit.
        print()
        print("# config-check --explain (field guide)")
        for key, blurb in explain_guide.items():
            print(f"## {key}")
            print(blurb)
            print()
    if warnings and strict:
        raise SystemExit(2)

@cli.group("admin")
def admin_group():
    """Management mutations: drain, detach, policy, audit (monitoring HTTP)."""

def _admin_base_url(url: str | None) -> str:
    if not url:
        raise click.UsageError("Provide --url or set MPREG_MONITORING_URL.")
    return url.rstrip("/")

def _admin_headers(token: str | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers

@admin_group.command("drain")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL",
)
@click.option(
    "--token",
    default=None,
    envvar="MPREG_MONITORING_TOKEN",
    help="Bearer token for monitoring auth",
)
@click.option("--clear", is_flag=True, help="Clear drain (admit traffic again)")
@click.option("--actor", default=None, help="Actor name for audit")
@click.option("--reason", default=None, help="Reason for audit")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON")
def admin_drain(
    url: str | None,
    token: str | None,
    clear: bool,
    actor: str | None,
    reason: str | None,
    as_json: bool,
) -> None:
    """Enter or clear node drain (affects /ready)."""

    async def _run() -> None:
        base = _admin_base_url(url)
        body = {
            "draining": not clear,
            "actor": actor,
            "reason": reason,
        }
        async with (
            aiohttp.ClientSession() as session,
            session.post(
                f"{base}/mgmt/v1/nodes/drain",
                json=body,
                headers=_admin_headers(token),
            ) as resp,
        ):
            data = await resp.json(content_type=None)
            if as_json:
                console.print(data)
            else:
                console.print(
                    f"[{'green' if resp.status == 200 else 'red'}]"
                    f"HTTP {resp.status} drain applied={data.get('applied')} "
                    f"draining={data.get('draining', body['draining'])}[/]"
                )
            if resp.status >= 400:
                raise SystemExit(1)

    run_coro(_run())

@admin_group.command("detach")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL",
)
@click.option(
    "--token",
    default=None,
    envvar="MPREG_MONITORING_TOKEN",
    help="Bearer token for monitoring auth",
)
@click.argument("peer_url")
@click.option("--actor", default=None, help="Actor name for audit")
@click.option("--reason", default=None, help="Reason for audit")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON")
def admin_detach(
    url: str | None,
    token: str | None,
    peer_url: str,
    actor: str | None,
    reason: str | None,
    as_json: bool,
) -> None:
    """Detach a peer connection from this node."""

    async def _run() -> None:
        base = _admin_base_url(url)
        body = {"peer_url": peer_url, "actor": actor, "reason": reason}
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{base}/mgmt/v1/peers/detach",
                json=body,
                headers=_admin_headers(token),
            ) as resp:
                data = await resp.json(content_type=None)
                if as_json:
                    console.print(data)
                else:
                    console.print(
                        f"[{'green' if resp.status == 200 and data.get('applied') else 'red'}]"
                        f"HTTP {resp.status} detach applied={data.get('applied')} "
                        f"peer={peer_url}[/]"
                    )
                if resp.status >= 400 or data.get("applied") is False:
                    raise SystemExit(1)

    run_coro(_run())

@admin_group.command("audit")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL",
)
@click.option(
    "--token",
    default=None,
    envvar="MPREG_MONITORING_TOKEN",
    help="Bearer token for monitoring auth",
)
@click.option("--limit", default=50, show_default=True, type=int)
@click.option("--json", "as_json", is_flag=True, help="Emit JSON")
def admin_audit(url: str | None, token: str | None, limit: int, as_json: bool) -> None:
    """Show recent management mutation audit entries."""

    async def _run() -> None:
        base = _admin_base_url(url)
        async with (
            aiohttp.ClientSession() as session,
            session.get(
                f"{base}/mgmt/v1/audit",
                params={"limit": str(limit)},
                headers=_admin_headers(token),
            ) as resp,
        ):
            data = await resp.json(content_type=None)
            if as_json:
                console.print(data)
                return
            mutations = data.get("mutations") or []
            table = Table(title="Mgmt mutation audit")
            table.add_column("event")
            table.add_column("actor")
            table.add_column("success")
            table.add_column("detail")
            for m in mutations:
                table.add_row(
                    str(m.get("event")),
                    str(m.get("actor")),
                    str(m.get("success")),
                    str(m.get("detail"))[:80],
                )
            console.print(table)
            if resp.status >= 400:
                raise SystemExit(1)

    run_coro(_run())

@admin_group.command("policy")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL",
)
@click.option(
    "--token",
    default=None,
    envvar="MPREG_MONITORING_TOKEN",
    help="Bearer token for monitoring auth",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Validate only via /mgmt/v1/policy/dry-run (no apply)",
)
@click.option(
    "--file",
    "policy_file",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="JSON file body for policy apply/dry-run",
)
@click.option("--json-body", default=None, help="Inline JSON body (overrides --file)")
@click.option("--actor", default=None, help="Actor name for audit")
@click.option("--reason", default=None, help="Reason for audit")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON")
def admin_policy(
    url: str | None,
    token: str | None,
    dry_run: bool,
    policy_file: str | None,
    json_body: str | None,
    actor: str | None,
    reason: str | None,
    as_json: bool,
) -> None:
    """Apply or dry-run discovery/namespace policy via monitoring HTTP (ERG-02)."""

    async def _run() -> None:
        base = _admin_base_url(url)
        if json_body:
            body = loads_text(json_body)
        elif policy_file:
            body = load_path(policy_file)
        else:
            body = {}
        if not isinstance(body, dict):
            raise click.UsageError("Policy body must be a JSON object")
        if actor is not None:
            body.setdefault("actor", actor)
        if reason is not None:
            body.setdefault("reason", reason)
        path = "/mgmt/v1/policy/dry-run" if dry_run else "/mgmt/v1/policy/apply"
        async with (
            aiohttp.ClientSession() as session,
            session.post(
                f"{base}{path}",
                json=body,
                headers=_admin_headers(token),
            ) as resp,
        ):
            data = await resp.json(content_type=None)
            if as_json:
                console.print(data)
            else:
                console.print(
                    f"[{'green' if resp.status == 200 else 'red'}]"
                    f"HTTP {resp.status} policy {'dry-run' if dry_run else 'apply'} "
                    f"applied={data.get('applied', data.get('ok'))}[/]"
                )
                if isinstance(data, dict) and data.get("detail"):
                    console.print(str(data.get("detail"))[:200])
            if resp.status >= 400:
                raise SystemExit(1)

    run_coro(_run())

@cli.group("profile")
def profile_group():
    """List and show built-in settings profiles."""

def _profiles_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "profiles"

# ERG-T14-02 / ERG-T13-09: operator risk tags for packaged profiles
_PROFILE_RISK_TAGS: dict[str, str] = {
    "dev": "lab",
    "single-node": "lab",
    "cluster": "prod-baseline (same-trust; mon loopback)",
    "soft-rt": "soft-rt (latency; not multi-tenant)",
    "federated": "federated baseline (rotate secrets)",
    "federated-lab": "lab federated (open CP intentional)",
    "discovery-resolver": "lab/ops (discovery CP; mon loopback)",
}

@profile_group.command("list")
def profile_list() -> None:
    """List packaged TOML settings profiles with risk tags."""
    root = _profiles_dir()
    if not root.is_dir():
        console.print("[red]No profiles directory found.[/red]")
        return
    table = Table(title="MPREG settings profiles")
    table.add_column("Name")
    table.add_column("Risk")
    table.add_column("Path")
    for path in sorted(root.glob("*.toml")):
        tag = _PROFILE_RISK_TAGS.get(path.stem, "unspecified — read profile header")
        table.add_row(path.stem, tag, str(path))
    console.print(table)
    console.print(f"Start with: [bold]mpreg server start-config {root}/dev.toml[/bold]")

@profile_group.command("show")
@click.argument("name")
def profile_show(name: str) -> None:
    """Print a packaged profile TOML file."""
    root = _profiles_dir()
    path = root / f"{name}.toml"
    if not path.exists():
        # allow name with .toml
        alt = root / name
        path = alt if alt.exists() else path
    if not path.exists():
        raise click.UsageError(f"Unknown profile {name!r}. Try: mpreg profile list")
    console.print(path.read_text())

@profile_group.command("path")
@click.argument("name")
def profile_path(name: str) -> None:
    """Print the filesystem path of a packaged profile."""
    root = _profiles_dir()
    path = root / f"{name}.toml"
    if not path.exists():
        raise click.UsageError(f"Unknown profile {name!r}")
    console.print(str(path))

@cli.group()
def demo():
    """Run capability demos via the unified mpreg-example runner.

    Preferred::

        uv run mpreg-example demo tier1
        uv run mpreg-example run plane_rpc
        uv run mpreg-example smoke
    """

def _demo_via_example(argv: list[str]) -> None:
    """Delegate demo CLI to the unified curriculum runner (entrypoints only)."""
    from mpreg.examples.apps._shared.runner import main as examples_main

    examples_main(argv)

@demo.command("tier1")
@click.argument(
    "system",
    type=click.Choice(
        ["rpc", "pubsub", "queue", "cache", "federation", "fabric", "monitoring", "all"]
    ),
    required=False,
    default="all",
)
def demo_tier1(system: str) -> None:
    """Run tier-1 plane tour(s) via mpreg-example."""
    if system == "all":
        _demo_via_example(["demo", "tier1"])
        return
    # federation is historical alias for fabric
    plane = "fabric" if system in ("federation", "fabric") else system
    _demo_via_example(["run", f"plane_{plane}"])

@demo.command("tier2")
def demo_tier2() -> None:
    """Run tier-2 integration tours via mpreg-example."""
    _demo_via_example(["demo", "tier2"])

@demo.command("tier3")
def demo_tier3() -> None:
    """Run tier-3 expansion via mpreg-example."""
    _demo_via_example(["demo", "tier3"])

@demo.command("all")
def demo_all() -> None:
    """Run tier1 + tier2 + tier3 bundles via mpreg-example."""
    from mpreg.examples.apps._shared.runner import main as examples_main

    examples_main(["demo", "tier1"])
    examples_main(["demo", "tier2"])
    examples_main(["demo", "tier3"])

@demo.command("quick")
def demo_quick() -> None:
    """Fast demo bundle (hello_rpc + plane_rpc)."""
    _demo_via_example(["demo", "quick"])

@demo.command("list")
def demo_list() -> None:
    """List demo bundles."""
    _demo_via_example(["bundles"])

@cli.group("examples")
def examples_group() -> None:
    """Curriculum example apps (product-shaped learning path).

    See docs/examples-curriculum/ and ``mpreg examples list``.
    """

@examples_group.command("list")
@click.option(
    "--level", type=click.Choice(["L0", "L1", "L2", "L3", "L4"]), default=None
)
@click.option("--smoke", is_flag=True, help="Only smoke-bundle apps")
@click.option("--suite", is_flag=True, help="Only suite-bundle apps")
@click.option(
    "--kind",
    type=click.Choice(["product", "plane", "integration", "legacy"]),
    default=None,
    help="Filter by app kind",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
def examples_list(
    level: str | None, smoke: bool, suite: bool, kind: str | None, fmt: str
) -> None:
    """List curriculum apps."""
    from mpreg.examples.apps._shared.runner import main as examples_main

    argv = ["list"]
    if level:
        argv.extend(["--level", level])
    if smoke:
        argv.append("--smoke")
    if suite:
        argv.append("--suite")
    if kind:
        argv.extend(["--kind", kind])
    argv.extend(["--format", fmt])
    examples_main(argv)

@examples_group.command("describe")
@click.argument("app_id")
def examples_describe(app_id: str) -> None:
    """Describe one curriculum app."""
    from mpreg.examples.apps._shared.runner import main as examples_main

    examples_main(["describe", app_id])

@examples_group.command("path")
@click.argument("app_id")
def examples_path(app_id: str) -> None:
    """Print on-disk path for an app."""
    from mpreg.examples.apps._shared.runner import main as examples_main

    examples_main(["path", app_id])

@examples_group.command("run")
@click.argument("app_id")
@click.option("--timeout", type=float, default=120.0, show_default=True)
def examples_run(app_id: str, timeout: float) -> None:
    """Run one curriculum app (assertable)."""
    from mpreg.examples.apps._shared.runner import main as examples_main

    examples_main(["run", app_id, "--timeout", str(timeout)])

@examples_group.command("smoke")
@click.option("--timeout", type=float, default=120.0, show_default=True)
@click.option("--no-fail-fast", is_flag=True)
def examples_smoke(timeout: float, no_fail_fast: bool) -> None:
    """Run smoke bundle (L0 + selected L1)."""
    from mpreg.examples.apps._shared.runner import main as examples_main

    argv = ["smoke", "--timeout", str(timeout)]
    if no_fail_fast:
        argv.append("--no-fail-fast")
    examples_main(argv)

@examples_group.command("suite")
@click.option("--timeout", type=float, default=180.0, show_default=True)
@click.option("--no-fail-fast", is_flag=True)
def examples_suite(timeout: float, no_fail_fast: bool) -> None:
    """Run full shipped curriculum suite."""
    from mpreg.examples.apps._shared.runner import main as examples_main

    argv = ["suite", "--timeout", str(timeout)]
    if no_fail_fast:
        argv.append("--no-fail-fast")
    examples_main(argv)

@cli.group("distlab")
def distlab_group() -> None:
    """First-party DistLab (history/checker/nemesis scenarios).

    Jepsen-inspired platform self-test lab — not Elle, not WAN, not BFT.

    \b
      uv run mpreg distlab list
      uv run mpreg distlab catalog --json
      uv run mpreg distlab run strong.happy_3
      uv run mpreg distlab suite --preset smoke
      uv run mpreg distlab suite --track T2 --limit 5
      uv run mpreg distlab presets
    """

@distlab_group.command("list")
@click.option("--track", default="", help="Filter by track id (T1..T7)")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON catalog rows")
def distlab_list(track: str, as_json: bool) -> None:
    """List registered DistLab scenarios."""
    from mpreg.testing.distlab.cli import list_scenarios

    code = list_scenarios(track=track or "", as_json=as_json)
    if code:
        raise SystemExit(code)

@distlab_group.command("catalog")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON")
def distlab_catalog(as_json: bool) -> None:
    """Show scenario catalog with track/tags/description."""
    from mpreg.testing.distlab.cli import catalog

    code = catalog(as_json=as_json)
    if code:
        raise SystemExit(code)

@distlab_group.command("run")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True, help="Emit ScenarioResult JSON")
def distlab_run(name: str, as_json: bool) -> None:
    """Run one in-process DistLab scenario by name."""
    from mpreg.testing.distlab.cli import run_scenario

    code = run_scenario(name, as_json=as_json)
    if code:
        raise SystemExit(code)

@distlab_group.command("suite")
@click.option("--track", default="", help="Filter by track id (T2, T4, T13, …)")
@click.option("--prefix", default="", help="Name prefix (e.g. strong.)")
@click.option("--tag", default="", help="Require registry tag")
@click.option(
    "--preset",
    default="",
    help="Named suite preset (smoke, strong-core, audit-core)",
)
@click.option(
    "--name",
    "names",
    multiple=True,
    help="Explicit scenario name (repeatable)",
)
@click.option("--limit", type=int, default=0, help="Max scenarios (0 = all matched)")
@click.option("--fail-fast", is_flag=True, help="Stop on first failure")
@click.option(
    "--include-not-bft",
    is_flag=True,
    help="Include not_bft demos (may leave intentional dirty state)",
)
@click.option("--json", "as_json", is_flag=True, help="Emit suite report JSON")
def distlab_suite(
    track: str,
    prefix: str,
    tag: str,
    preset: str,
    names: tuple[str, ...],
    limit: int,
    fail_fast: bool,
    include_not_bft: bool,
    as_json: bool,
) -> None:
    """Run a filtered DistLab suite (excludes not_bft by default)."""
    from mpreg.testing.distlab.cli import run_suite

    code = run_suite(
        track=track or "",
        prefix=prefix or "",
        tag=tag or "",
        names=list(names) if names else None,
        preset=preset or "",
        include_not_bft=include_not_bft,
        limit=limit,
        fail_fast=fail_fast,
        as_json=as_json,
    )
    if code:
        raise SystemExit(code)

@distlab_group.command("presets")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON")
def distlab_presets(as_json: bool) -> None:
    """List named DistLab suite presets (smoke, strong-core, …)."""
    from mpreg.testing.distlab.cli import list_presets

    code = list_presets(as_json=as_json)
    if code:
        raise SystemExit(code)

@cli.group("test")
def test_group() -> None:
    """Developer test runners (entry points only — never python -m)."""

@test_group.command("concurrent")
@click.option("-n", "--workers", type=int, default=16, show_default=True)
@click.option("--stall-seconds", type=float, default=90.0, show_default=True)
@click.option("--open-files", type=int, default=1_048_576, show_default=True)
@click.option("--log", type=click.Path(), default=None)
@click.option("--junit", type=click.Path(), default=None)
@click.option("--profile-dir", type=click.Path(), default=None)
@click.option("--no-sudo-pyspy", is_flag=True)
@click.argument("pytest_args", nargs=-1, type=click.UNPROCESSED)
def test_concurrent(
    workers: int,
    stall_seconds: float,
    open_files: int,
    log: str | None,
    junit: str | None,
    profile_dir: str | None,
    no_sudo_pyspy: bool,
    pytest_args: tuple[str, ...],
) -> None:
    """Run pytest under high concurrency with hang profiling."""
    from pathlib import Path

    from mpreg.testing.concurrent_runner import (
        DEFAULT_PROFILE_DIR,
        ConcurrentSuiteRunner,
    )

    runner = ConcurrentSuiteRunner(
        workers=workers,
        log_path=Path(log) if log else None,
        junit_path=Path(junit) if junit else None,
        profile_dir=Path(profile_dir) if profile_dir else DEFAULT_PROFILE_DIR,
        stall_seconds=stall_seconds,
        open_file_target=open_files,
        extra_pytest_args=list(pytest_args),
        use_sudo_for_pyspy=not no_sudo_pyspy,
    )
    result = runner.run()
    console.print(
        f"done exit={result.exit_code} duration={result.duration_seconds:.1f}s "
        f"nofile={result.open_files.soft}/{result.open_files.hard} "
        f"stalls={len(result.stall_dumps)}"
    )
    if result.summary_line:
        console.print(result.summary_line)
    for dump in result.stall_dumps:
        console.print(f"stall dump: {dump}")
    raise SystemExit(result.exit_code)

@cli.command()
@click.option(
    "--config", "-c", type=click.Path(exists=True), help="Configuration file path"
)
@click.option(
    "--output",
    "-o",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
def discover(config: str | None, output: str):
    """Discover available fabric clusters."""

    async def _discover():
        federation_cli = FederationCLI()
        clusters = await federation_cli.discover_clusters(config)

        if output == "table":
            federation_cli.display_cluster_list(clusters)
        elif output == "json":
            # Convert dataclasses to dict for JSON serialization
            clusters_dict = [
                {
                    "cluster_id": cluster.cluster_id,
                    "cluster_name": cluster.cluster_name,
                    "region": cluster.region,
                    "bridge_url": cluster.bridge_url,
                    "server_url": cluster.server_url,
                    "status": cluster.status,
                    "health": cluster.health,
                    "discovery_source": cluster.discovery_source,
                    "health_score": cluster.health_score,
                }
                for cluster in clusters
            ]
            console.print(dumps_pretty_text(clusters_dict))

    run_coro(_discover())

@cli.command()
@click.argument("cluster_id")
@click.argument("cluster_name")
@click.argument("region")
@click.argument("server_url")
@click.argument("bridge_url")
@click.option("--no-resilience", is_flag=True, help="Disable resilience monitoring")
def register(
    cluster_id: str,
    cluster_name: str,
    region: str,
    server_url: str,
    bridge_url: str,
    no_resilience: bool,
):
    """Register a new federation cluster."""

    async def _register():
        federation_cli = FederationCLI()
        success = await federation_cli.register_cluster(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            region=region,
            server_url=server_url,
            bridge_url=bridge_url,
            enable_resilience=not no_resilience,
        )

        if not success:
            sys.exit(1)

    run_coro(_register())

@cli.command()
@click.argument("cluster_id")
def unregister(cluster_id: str):
    """Unregister a federation cluster."""

    async def _unregister():
        federation_cli = FederationCLI()
        success = await federation_cli.unregister_cluster(cluster_id)

        if not success:
            sys.exit(1)

    run_coro(_unregister())

@cli.command()
@click.option("--cluster", "-c", help="Specific cluster ID to check")
@click.option(
    "--output",
    "-o",
    type=click.Choice(["report", "json"]),
    default="report",
    help="Output format",
)
def health(cluster: str | None, output: str):
    """Check cluster health status."""

    async def _health():
        federation_cli = FederationCLI()
        health_results = await federation_cli.check_cluster_health(cluster)

        if output == "report":
            federation_cli.display_health_report(health_results)
        elif output == "json":
            # Convert dataclasses to dict for JSON serialization
            serializable_results: dict[str, JsonDict] = {}
            for cluster_id, result in health_results.items():
                serializable_results[cluster_id] = {}
                for key, value in result.items():
                    if is_dataclass(value) and not isinstance(value, type):
                        # Convert dataclass instance to dict
                        serializable_results[cluster_id][key] = asdict(value)
                    else:
                        serializable_results[cluster_id][key] = value
            console.print(dumps_pretty_text(serializable_results))

    run_coro(_health())

@cli.command("federation-metrics")
@click.option("--cluster", "-c", help="Specific cluster ID to show metrics for")
def federation_metrics(cluster: str | None):
    """Display federation performance metrics for clusters."""

    async def _metrics():
        federation_cli = FederationCLI()
        await federation_cli.show_metrics(cluster)

    run_coro(_metrics())

@cli.command()
@click.argument("output_path", type=click.Path())
def generate_config(output_path: str):
    """Generate a federation configuration template."""
    federation_cli = FederationCLI()
    federation_cli.generate_config_template(output_path)

@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
def validate_config(config_path: str):
    """Validate federation configuration file."""

    async def _validate():
        federation_cli = FederationCLI()
        is_valid = await federation_cli.validate_config(config_path)

        if not is_valid:
            sys.exit(1)

    run_coro(_validate())

@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option(
    "--dry-run", is_flag=True, help="Validate configuration without deploying"
)
def deploy(config_path: str, dry_run: bool):
    """Deploy federation clusters from configuration file."""

    async def _deploy():
        federation_cli = FederationCLI()

        if dry_run:
            console.print(
                "[bold blue]🔍 Running deployment validation (dry-run mode)[/bold blue]"
            )
            is_valid = await federation_cli.validate_config(config_path)
            if is_valid:
                console.print(
                    "[green]✅ Configuration is valid and ready for deployment[/green]"
                )
            else:
                console.print("[red]❌ Configuration validation failed[/red]")
                sys.exit(1)
        else:
            success = await federation_cli.deploy_from_config(config_path)
            if not success:
                sys.exit(1)

    run_coro(_deploy())

@cli.command()
def topology():
    """Display federation topology."""
    federation_cli = FederationCLI()
    federation_cli.display_topology()

@cli.command()
@click.option("--force", is_flag=True, help="Force cleanup without confirmation")
def cleanup(force: bool):
    """Clean up all federation resources."""

    async def _cleanup():
        federation_cli = FederationCLI()

        if not force:
            if not click.confirm(
                "Are you sure you want to clean up all federation resources?"
            ):
                console.print("[yellow]⚠️ Cleanup cancelled[/yellow]")
                return

        await federation_cli.cleanup_all()

    run_coro(_cleanup())

@cli.group()
def monitor():
    """Fabric federation monitoring commands."""

@monitor.command()
@click.option(
    "--interval", "-i", type=int, default=30, help="Health check interval in seconds"
)
@click.option("--clusters", "-c", multiple=True, help="Specific clusters to monitor")
@click.option(
    "--summary/--detail",
    default=True,
    help="Use summary or detailed endpoint (monitoring URL mode only)",
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@add_format_option
def health_watch(
    interval: int,
    clusters: tuple[str, ...],
    summary: bool,
    url: str | None,
    output_format: str,
):
    """Continuously monitor cluster health."""

    async def _health_watch():
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if monitoring_url:
            base_url = monitoring_url.rstrip("/")
            console.print(
                f"[bold green]🔍 Starting monitoring endpoint health watch (interval: {interval}s)[/bold green]"
            )
            try:
                while True:
                    console.clear()
                    console.print(
                        f"[bold blue]🏥 Fabric Health Monitor - {time.time():.0f}[/bold blue]"
                    )
                    async with aiohttp.ClientSession() as session:
                        if clusters:
                            for cluster_id in clusters:
                                endpoint = f"{base_url}/health/clusters/{cluster_id}"
                                async with session.get(endpoint) as response:
                                    payload = await response.json()
                                    emit(
                                        payload,
                                        output_format=output_format,
                                        table_title="Health",
                                    )
                        else:
                            endpoint = (
                                f"{base_url}/health/summary"
                                if summary
                                else f"{base_url}/health"
                            )
                            async with session.get(endpoint) as response:
                                payload = await response.json()
                                emit(
                                    payload,
                                    output_format=output_format,
                                    table_title="Health",
                                )
                    await asyncio.sleep(interval)
            except KeyboardInterrupt:
                console.print("\n[yellow]⚠️ Health monitoring stopped[/yellow]")
            return

        federation_cli = FederationCLI()
        console.print(
            f"[bold green]🔍 Starting health monitoring (interval: {interval}s)[/bold green]"
        )

        try:
            while True:
                console.clear()
                console.print(
                    f"[bold blue]🏥 Fabric Health Monitor - {time.time():.0f}[/bold blue]"
                )

                if clusters:
                    for cluster_id in clusters:
                        health_results = await federation_cli.check_cluster_health(
                            cluster_id
                        )
                        federation_cli.display_health_report(health_results)
                else:
                    health_results = await federation_cli.check_cluster_health(None)
                    federation_cli.display_health_report(health_results)

                await asyncio.sleep(interval)

        except KeyboardInterrupt:
            console.print("\n[yellow]⚠️ Health monitoring stopped[/yellow]")

    run_coro(_health_watch())

@monitor.command("health")
@click.option(
    "--cluster",
    "-c",
    default=None,
    help="Specific cluster ID to query",
)
@click.option(
    "--summary/--detail",
    default=True,
    help="Use summary or detailed endpoint (monitoring URL mode only)",
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@add_format_option
def health_endpoint(
    cluster: str | None, summary: bool, url: str | None, output_format: str
) -> None:
    """Fetch health status from the monitoring endpoint."""

    async def _health_endpoint() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        base_url = monitoring_url.rstrip("/")
        if cluster:
            endpoint = f"{base_url}/health/clusters/{cluster}"
        else:
            endpoint = f"{base_url}/health/summary" if summary else f"{base_url}/health"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                if response.status != 200:
                    emit(payload, output_format=output_format, table_title="Health")
                    return
                emit(payload, output_format=output_format, table_title="Health")

    run_coro(_health_endpoint())

@monitor.command()
@click.option(
    "--interval", "-i", type=int, default=60, help="Metrics update interval in seconds"
)
@click.option("--clusters", "-c", multiple=True, help="Specific clusters to monitor")
@click.option(
    "--system",
    type=click.Choice(
        ["unified", "rpc", "pubsub", "queue", "cache", "transport", "federation"],
        case_sensitive=False,
    ),
    default="unified",
    help="Metrics system to query (monitoring URL mode only)",
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@add_format_option
def metrics_watch(
    interval: int,
    clusters: tuple[str, ...],
    system: str,
    url: str | None,
    output_format: str,
):
    """Continuously monitor cluster metrics."""

    async def _metrics_watch():
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if monitoring_url:
            endpoint_map = {
                "unified": "/metrics/unified",
                "rpc": "/metrics/rpc",
                "pubsub": "/metrics/pubsub",
                "queue": "/metrics/queue",
                "cache": "/metrics/cache",
                "transport": "/metrics/transport",
                "federation": "/metrics",
            }
            endpoint = f"{monitoring_url.rstrip('/')}{endpoint_map[system]}"
            console.print(
                f"[bold green]📊 Starting monitoring endpoint metrics watch (interval: {interval}s)[/bold green]"
            )
            try:
                while True:
                    console.clear()
                    console.print(
                        f"[bold blue]📊 Fabric Metrics Monitor - {time.time():.0f}[/bold blue]"
                    )
                    async with aiohttp.ClientSession() as session:
                        async with session.get(endpoint) as response:
                            payload = await response.json()
                            emit(
                                payload,
                                output_format=output_format,
                                table_title="Metrics",
                            )
                    await asyncio.sleep(interval)
            except KeyboardInterrupt:
                console.print("\n[yellow]⚠️ Metrics monitoring stopped[/yellow]")
            return

        federation_cli = FederationCLI()
        console.print(
            f"[bold green]📊 Starting metrics monitoring (interval: {interval}s)[/bold green]"
        )

        try:
            while True:
                console.clear()
                console.print(
                    f"[bold blue]📊 Fabric Metrics Monitor - {time.time():.0f}[/bold blue]"
                )

                if clusters:
                    for cluster_id in clusters:
                        await federation_cli.show_metrics(cluster_id)
                else:
                    await federation_cli.show_metrics(None)

                await asyncio.sleep(interval)

        except KeyboardInterrupt:
            console.print("\n[yellow]⚠️ Metrics monitoring stopped[/yellow]")

    run_coro(_metrics_watch())

@monitor.command("status")
@click.option(
    "--cluster",
    "-c",
    default=None,
    help="Specific cluster ID for health details",
)
@click.option(
    "--output",
    "-o",
    type=click.Choice(["table", "json"]),
    default=None,
    help="Deprecated: use --format",
)
@add_format_option
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def status(
    cluster: str | None, output: str | None, output_format: str, url: str | None
) -> None:
    """Show a compact admin status summary from monitoring endpoints."""
    if output is not None:
        output_format = "json" if output == "json" else "table"

    def _format_timestamp(raw: Any) -> str:
        if not raw:
            return "n/a"
        try:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(raw)))
        except Exception:
            return str(raw)

    def _summarize_transport(snapshot_payload: Mapping[str, object]) -> dict[str, int]:
        snapshots = snapshot_payload.get("transport_health_snapshots", {}) or {}
        if not isinstance(snapshots, dict):
            return {"total": 0, "healthy": 0, "degraded": 0}
        healthy = 0
        degraded = 0
        for entry in snapshots.values():
            score = entry.get("overall_health_score", 0.0)
            try:
                score_val = float(score)
            except Exception:
                score_val = 0.0
            if score_val >= 0.9:
                healthy += 1
            elif score_val >= 0.7:
                degraded += 1
        total = len(snapshots)
        return {"total": total, "healthy": healthy, "degraded": degraded}

    async def _status() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        base_url = monitoring_url.rstrip("/")
        health_endpoint = (
            f"{base_url}/health/clusters/{cluster}"
            if cluster
            else f"{base_url}/health/summary"
        )
        endpoints = {
            "health": health_endpoint,
            "persistence": f"{base_url}/metrics/persistence",
            "transport": f"{base_url}/metrics/transport",
            "unified": f"{base_url}/metrics/unified",
        }
        results: JsonDict = {}
        async with aiohttp.ClientSession() as session:
            for key, endpoint in endpoints.items():
                async with session.get(endpoint) as response:
                    payload = await response.json()
                    results[key] = {
                        "status_code": response.status,
                        "payload": payload,
                    }

        if output == "json":
            emit(results, output_format=output_format, table_title="Status")
            return

        table = Table(title="MPREG Monitoring Status")
        table.add_column("Section")
        table.add_column("Value")

        health_payload = results.get("health", {}).get("payload", {})
        if cluster:
            cluster_info = health_payload.get("cluster", {})
            table.add_row(
                "Health",
                f"{cluster_info.get('status', 'unknown')} (cluster={cluster})",
            )
        else:
            summary = health_payload.get("health_summary", {})
            overall = summary.get("overall", {})
            table.add_row(
                "Health",
                f"{overall.get('status', 'unknown')} "
                f"score={overall.get('health_score', 'n/a')}",
            )
            table.add_row(
                "Clusters",
                f"{summary.get('cluster_counts', {}).get('total', 'n/a')} total",
            )

        persistence_payload = results.get("persistence", {}).get("payload", {})
        persistence = persistence_payload.get("persistence_snapshots", {})
        if persistence.get("enabled"):
            table.add_row(
                "Persistence",
                f"{persistence.get('mode', 'n/a')} dir={persistence.get('data_dir', 'n/a')}",
            )
            fabric = persistence.get("fabric", {})
            table.add_row(
                "Snapshots",
                f"saved={_format_timestamp(fabric.get('snapshot_last_saved_at'))} "
                f"restored={_format_timestamp(fabric.get('snapshot_last_restored_at'))}",
            )
        else:
            table.add_row("Persistence", "disabled")

        transport_payload = results.get("transport", {}).get("payload", {})
        transport_summary = _summarize_transport(transport_payload)
        table.add_row(
            "Transport",
            f"{transport_summary['total']} endpoints "
            f"(healthy={transport_summary['healthy']}, degraded={transport_summary['degraded']})",
        )

        unified_payload = results.get("unified", {}).get("payload", {})
        unified_metrics = unified_payload.get("unified_metrics", {})
        table.add_row(
            "Unified",
            f"health={unified_metrics.get('overall_health_status', 'n/a')} "
            f"score={unified_metrics.get('overall_health_score', 'n/a')}",
        )

        console.print(table)

    run_coro(_status())

@monitor.command("decisions")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL",
)
@click.option("--limit", type=int, default=20, help="Max decisions to return")
@click.option("--message-id", default=None, help="Filter by message id")
@click.option("--correlation-id", default=None, help="Filter by correlation id")
@click.option("--token", default=None, envvar="MPREG_MONITORING_TOKEN")
@add_format_option
def monitor_decisions(
    url: str | None,
    limit: int,
    message_id: str | None,
    correlation_id: str | None,
    token: str | None,
    output_format: str,
) -> None:
    """Show recent fabric route decisions from the audit ring buffer."""

    async def _run() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print("[red]Set --url or MPREG_MONITORING_URL[/red]")
            return
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        params = [f"limit={limit}"]
        if message_id:
            params.append(f"message_id={message_id}")
        if correlation_id:
            params.append(f"correlation_id={correlation_id}")
        endpoint = f"{monitoring_url.rstrip('/')}/routing/decisions?" + "&".join(params)
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(endpoint) as response:
                payload = await response.json(content_type=None)
                emit(
                    payload, output_format=output_format, table_title="Route decisions"
                )

    run_coro(_run())

@monitor.command("route-trace")
@click.option(
    "--destination",
    "-d",
    required=True,
    help="Destination cluster ID to trace",
)
@click.option(
    "--avoid",
    "-a",
    multiple=True,
    help="Clusters to avoid (can be repeated)",
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@add_format_option
def route_trace(
    destination: str, avoid: tuple[str, ...], url: str | None, output_format: str
) -> None:
    """Fetch a route selection trace from the monitoring endpoint."""

    async def _route_trace() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        params = {"destination": destination}
        if avoid:
            params["avoid"] = ",".join(avoid)
        endpoint = f"{monitoring_url.rstrip('/')}/routing/trace"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint, params=params) as response:
                payload = await response.json()
                if response.status != 200:
                    emit(
                        payload, output_format=output_format, table_title="Route trace"
                    )
                    return
                emit(payload, output_format=output_format, table_title="Route trace")

    run_coro(_route_trace())

@monitor.command("link-state")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@add_format_option
def link_state(url: str | None, output_format: str) -> None:
    """Fetch link-state routing status from the monitoring endpoint."""

    async def _link_state() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint = f"{monitoring_url.rstrip('/')}/routing/link-state"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                if response.status != 200:
                    emit(payload, output_format=output_format, table_title="Link state")
                    return
                emit(payload, output_format=output_format, table_title="Link state")

    run_coro(_link_state())

@monitor.command("transport-endpoints")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@add_format_option
def transport_endpoints(url: str | None, output_format: str) -> None:
    """Fetch transport endpoint assignments from monitoring."""

    async def _transport_endpoints() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint = f"{monitoring_url.rstrip('/')}/transport/endpoints"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                if response.status != 200:
                    emit(
                        payload,
                        output_format=output_format,
                        table_title="Transport endpoints",
                    )
                    return
                emit(
                    payload,
                    output_format=output_format,
                    table_title="Transport endpoints",
                )

    run_coro(_transport_endpoints())

@monitor.command("metrics")
@click.option(
    "--system",
    type=click.Choice(
        ["unified", "rpc", "pubsub", "queue", "cache", "transport", "federation"],
        case_sensitive=False,
    ),
    default="unified",
    help="Metrics system to query",
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@add_format_option
def monitor_metrics(system: str, url: str | None, output_format: str) -> None:
    """Fetch one-shot metrics from the monitoring endpoint."""

    async def _metrics() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint_map = {
            "unified": "/metrics/unified",
            "rpc": "/metrics/rpc",
            "pubsub": "/metrics/pubsub",
            "queue": "/metrics/queue",
            "cache": "/metrics/cache",
            "transport": "/metrics/transport",
            "federation": "/metrics",
        }
        endpoint = f"{monitoring_url.rstrip('/')}{endpoint_map[system]}"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                if response.status != 200:
                    emit(payload, output_format=output_format, table_title="Metrics")
                    return
                emit(payload, output_format=output_format, table_title="Metrics")

    run_coro(_metrics())

@monitor.command("prometheus")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@click.option(
    "--token",
    default=None,
    envvar="MPREG_MONITORING_TOKEN",
    help="Monitoring bearer token if auth is enabled",
)
def monitor_prometheus(url: str | None, token: str | None) -> None:
    """Fetch Prometheus text metrics from the monitoring endpoint."""

    async def _prom() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        endpoint = f"{monitoring_url.rstrip('/')}/metrics/prometheus"
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(endpoint) as response:
                text_body = await response.text()
                if response.status != 200:
                    console.print(f"[red]HTTP {response.status}[/red]")
                console.print(text_body)

    run_coro(_prom())

@monitor.command("persistence")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@add_format_option
def persistence(url: str | None, output_format: str) -> None:
    """Fetch persistence snapshot status from monitoring."""

    async def _persistence() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint = f"{monitoring_url.rstrip('/')}/metrics/persistence"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                if response.status != 200:
                    emit(
                        payload, output_format=output_format, table_title="Persistence"
                    )
                    return
                emit(payload, output_format=output_format, table_title="Persistence")

    run_coro(_persistence())

@monitor.command("strong")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@click.option(
    "--mgmt/--metrics",
    "use_mgmt",
    default=False,
    help="Use /mgmt/v1/strong instead of /metrics/strong",
)
@add_format_option
def monitor_strong(url: str | None, use_mgmt: bool, output_format: str) -> None:
    """Fetch STRONG majority-commit put metrics (process-local; not WAN SLA)."""

    async def _strong() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        path = "/mgmt/v1/strong" if use_mgmt else "/metrics/strong"
        endpoint = f"{monitoring_url.rstrip('/')}{path}"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                # Human summary for table/plain: capabilities honesty + refuse counters
                fmt = (output_format or "json").lower()
                if fmt in {"table", "plain"} and isinstance(payload, dict):
                    body = (
                        payload.get("strong")
                        if isinstance(payload.get("strong"), dict)
                        else payload
                    )
                    if isinstance(body, dict):
                        caps = body.get("capabilities") or {}
                        counters = body.get("counters") or {}
                        fail_peers = _strong_abort_fail_peers(body)
                        fail_oid = _strong_abort_fail_op_id(body)
                        console.print(
                            "[bold]STRONG[/bold] "
                            f"health={body.get('health')} "
                            f"bound={body.get('coordinator_bound')} "
                            f"pending={body.get('pending_count', 0)} "
                            f"visible={body.get('visible_count', 0)} "
                            f"backups={body.get('backups_count', 0)} "
                            f"pruned={body.get('backups_pruned_total', 0)} | "
                            f"caps put={caps.get('put_majority_commit')} "
                            f"get_quorum={caps.get('get_quorum', False)} "
                            f"delete_quorum={caps.get('delete_quorum', False)} "
                            f"ryw={caps.get('local_ryw_after_put')} "
                            f"cft={caps.get('cft_only', True)} "
                            f"abort_be={caps.get('abort_best_effort', True)} "
                            f"ttl_gc={caps.get('pending_ttl_clears_residual_l1', False)} "
                            f"retry_ops={caps.get('retry_abort_ops_driven', True)} | "
                            f"puts_ok={counters.get('puts_ok', 0)} "
                            f"puts_fail={counters.get('puts_fail', 0)} "
                            f"gets_refused={counters.get('gets_refused', 0)} "
                            f"deletes_refused={counters.get('deletes_refused', 0)} "
                            f"abort_fail={counters.get('aborts_peer_fail', 0)} "
                            f"abort_fail_peers={fail_peers} "
                            f"abort_fail_op_id={fail_oid or '-'} "
                            f"retry_abort={counters.get('retry_abort_calls', body.get('retry_abort_calls', 0))} "
                            f"retry_cleared={counters.get('retry_abort_cleared', body.get('retry_abort_cleared', 0))} "
                            "[dim](not WAN SLA; get/delete quorum is v1.1; "
                            "ABORT best-effort CFT; pending TTL ≠ residual GC; "
                            "abort_fail_peers = CFT residual candidates; "
                            "retry_abort = ops-driven not auto-heal)[/dim]"
                        )
                        # T51: remediation hint when residual candidates present
                        hint = strong_residual_ops_hint(body)
                        if hint:
                            console.print(f"[yellow]{hint}[/yellow]")
                emit(payload, output_format=output_format, table_title="STRONG")

    run_coro(_strong())

@monitor.command("audit")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@add_format_option
def monitor_audit(url: str | None, output_format: str) -> None:
    """Fetch shared-audit G-Set epidemic metrics from monitoring."""

    async def _audit() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint = f"{monitoring_url.rstrip('/')}/metrics/shared-audit"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                # Human summary for table/plain: capabilities honesty
                fmt = (output_format or "json").lower()
                if fmt in {"table", "plain"} and isinstance(payload, dict):
                    body = (
                        payload.get("shared_audit")
                        if isinstance(payload.get("shared_audit"), dict)
                        else payload
                    )
                    if isinstance(body, dict):
                        caps = body.get("capabilities") or {}
                        counters = body.get("counters") or {}
                        console.print(
                            "[bold]Shared audit[/bold] "
                            f"status={body.get('status')} "
                            f"store={body.get('store_size', 0)} "
                            f"flag={body.get('enabled_flag')} | "
                            f"caps gset={caps.get('gset_epidemic')} "
                            f"siem={caps.get('siem', False)} "
                            f"bft={caps.get('bft', False)} "
                            f"inf_ret={caps.get('infinite_retention', False)} "
                            f"lin_ops={caps.get('linearizable_cluster_ops', False)} | "
                            f"deltas_recv={counters.get('deltas_recv', 0)} "
                            f"drops={counters.get('publish_dropped', 0)} "
                            "[dim](not SIEM; not BFT; bounded watermark)[/dim]"
                        )
                emit(payload, output_format=output_format, table_title="Shared audit")

    run_coro(_audit())

@monitor.command("dns")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@add_format_option
def dns_metrics(url: str | None, output_format: str) -> None:
    """Fetch DNS gateway metrics from monitoring."""

    async def _dns_metrics() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint = f"{monitoring_url.rstrip('/')}/dns/metrics"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                if response.status != 200:
                    emit(
                        payload, output_format=output_format, table_title="DNS metrics"
                    )
                    return
                emit(payload, output_format=output_format, table_title="DNS metrics")

    run_coro(_dns_metrics())

@monitor.command("dns-watch")
@click.option("--interval", default=5.0, help="Polling interval in seconds")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@add_format_option
def dns_watch(interval: float, url: str | None, output_format: str) -> None:
    """Continuously monitor DNS gateway metrics."""

    async def _dns_watch() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint = f"{monitoring_url.rstrip('/')}/dns/metrics"
        console.print(
            f"[bold green]📡 Starting DNS metrics watch (interval: {interval}s)[/bold green]"
        )
        async with aiohttp.ClientSession() as session:
            try:
                while True:
                    async with session.get(endpoint) as response:
                        payload = await response.json()
                        emit(
                            payload,
                            output_format=output_format,
                            table_title="DNS metrics",
                        )
                    await asyncio.sleep(interval)
            except KeyboardInterrupt:
                console.print("\n[yellow]⚠️ DNS metrics watch stopped[/yellow]")

    run_coro(_dns_watch())

@monitor.command("persistence-watch")
@click.option(
    "--interval", "-i", type=int, default=30, help="Polling interval in seconds"
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@add_format_option
def persistence_watch(interval: int, url: str | None, output_format: str) -> None:
    """Continuously monitor persistence snapshot status."""

    async def _persistence_watch() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint = f"{monitoring_url.rstrip('/')}/metrics/persistence"
        try:
            while True:
                console.clear()
                console.print(
                    f"[bold blue]💾 Persistence Snapshots - {time.time():.0f}[/bold blue]"
                )
                async with aiohttp.ClientSession() as session:
                    async with session.get(endpoint) as response:
                        payload = await response.json()
                        emit(
                            payload,
                            output_format=output_format,
                            table_title="Persistence",
                        )
                await asyncio.sleep(interval)
        except KeyboardInterrupt:
            console.print("\n[yellow]⚠️ Persistence monitoring stopped[/yellow]")

    run_coro(_persistence_watch())

@monitor.command("endpoints")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
@add_format_option
def endpoints(url: str | None, output_format: str) -> None:
    """List available monitoring endpoints."""

    async def _endpoints() -> None:
        monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
        if not monitoring_url:
            console.print(
                "[red]Monitoring URL required. Use --url or set MPREG_MONITORING_URL.[/red]"
            )
            return
        endpoint = f"{monitoring_url.rstrip('/')}/endpoints"
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                payload = await response.json()
                if response.status != 200:
                    emit(payload, output_format=output_format, table_title="Endpoints")
                    return
                emit(payload, output_format=output_format, table_title="Endpoints")

    run_coro(_endpoints())

@cli.group()
def auto_discovery():
    """Auto-discovery management commands."""

@auto_discovery.command()
@click.option(
    "--config", "-c", type=click.Path(exists=True), help="Discovery configuration file"
)
@click.option(
    "--output",
    "-o",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
def run(config: str | None, output: str):
    """Run auto-discovery to find fabric clusters."""

    async def _auto_discover():
        federation_cli = FederationCLI()
        clusters = await federation_cli.discover_clusters(config)

        if output == "table":
            federation_cli.display_cluster_list(clusters)
        elif output == "json":
            clusters_dict = [
                {
                    "cluster_id": cluster.cluster_id,
                    "cluster_name": cluster.cluster_name,
                    "region": cluster.region,
                    "bridge_url": cluster.bridge_url,
                    "server_url": cluster.server_url,
                    "status": cluster.status,
                    "health": cluster.health,
                    "discovery_source": cluster.discovery_source,
                    "health_score": cluster.health_score,
                }
                for cluster in clusters
            ]
            console.print(dumps_pretty_text(clusters_dict))

    run_coro(_auto_discover())

@auto_discovery.command()
@click.argument("output_path", type=click.Path())
def generate(output_path: str):
    """Generate auto-discovery configuration template."""
    discovery_config = {
        "auto_discovery": {
            "enabled": True,
            "backends": [
                {
                    "protocol": "static_config",
                    "config_path": "/etc/mpreg/clusters.json",
                    "discovery_interval": 60.0,
                },
                {
                    "protocol": "consul",
                    "host": "localhost",
                    "port": 8500,
                    "service_name": "mpreg-federation",
                    "datacenter": "dc1",
                    "discovery_interval": 30.0,
                },
                {
                    "protocol": "dns_srv",
                    "domain": "mpreg.local",
                    "service": "_mpreg._tcp",
                    "resolver_host": "127.0.0.1",
                    "resolver_port": 53,
                    "use_tcp": False,
                    "timeout_seconds": 2.0,
                    "discovery_interval": 120.0,
                },
                {
                    "protocol": "http_endpoint",
                    "discovery_url": "https://discovery.example.com/clusters",
                    "discovery_interval": 60.0,
                    "registration_ttl": 120.0,
                },
            ],
        }
    }

    from pathlib import Path

    output_file = Path(output_path)
    with open(output_file, "w") as f:
        f.write(dumps_pretty_text(discovery_config))

    console.print(
        f"[green]✅ Auto-discovery configuration generated: {output_path}[/green]"
    )

@cli.group()
def config():
    """Configuration management commands."""

@config.command()
@click.argument(
    "template_name", type=click.Choice(["basic", "production", "development"])
)
@click.argument("output_path", type=click.Path())
def template(template_name: str, output_path: str):
    """Generate specific configuration templates."""
    federation_cli = FederationCLI()

    # For now, use the same template - could be extended for different templates
    federation_cli.generate_config_template(output_path)
    console.print(
        f"[green]✅ Generated {template_name} template: {output_path}[/green]"
    )

@config.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--key", help="Specific configuration key to show")
def show(config_path: str, key: str | None):
    """Show configuration file contents."""

    from rich.panel import Panel
    from rich.syntax import Syntax

    try:
        with open(config_path) as f:
            config_data = loads_text(f.read())

        if key:
            # Show specific key
            if key in config_data:
                console.print(dumps_pretty_text(config_data[key]))
            else:
                console.print(f"[red]❌ Key '{key}' not found in configuration[/red]")
        else:
            # Show entire configuration
            config_syntax = Syntax(
                dumps_pretty_text(config_data),
                "json",
                theme="monokai",
                line_numbers=True,
            )

            panel = Panel(
                config_syntax,
                title=f"🔧 Configuration: {config_path}",
                border_style="blue",
            )
            console.print(panel)

    except Exception as e:
        console.print(f"[red]❌ Error reading configuration: {e}[/red]")

# ---------------------------------------------------------------------------
# Phase H F2: top-level aliases operators guess first (call / dns).
# ---------------------------------------------------------------------------
cli.add_command(call, "call")

@cli.group("dns")
def dns_group() -> None:
    """DNS plane commands (aliases for ``mpreg client dns-*``)."""

for _src, _dest in (
    ("dns-register", "register"),
    ("dns-unregister", "unregister"),
    ("dns-list", "list"),
    ("dns-describe", "describe"),
    ("dns-resolve", "resolve"),
    ("dns-node-encode", "node-encode"),
    ("dns-node-decode", "node-decode"),
):
    _cmd = client.commands.get(_src)
    if _cmd is not None:
        dns_group.add_command(_cmd, _dest)

def main():
    """Main CLI entry point."""
    try:
        cli()
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ Operation cancelled by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]❌ Unexpected error: {e}[/red]")
        if "--verbose" in sys.argv or "-v" in sys.argv:
            import traceback

            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
