#!/usr/bin/env python3
from __future__ import annotations
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
import json
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
    """
    setup_logging(verbose, json_logs=json_logs)
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["json_logs"] = json_logs

@cli.group()
def client():
    """Client commands for interacting with MPREG servers."""
    pass

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
            return json.loads(value)
        except Exception:
            return value

    async def _call():
        async with MPREGClientAPI(url) as client:
            parsed_args = tuple(_parse_arg(arg) for arg in args)
            result = await client.call(
                fun,
                *parsed_args,
                locs=frozenset(locs) if locs else None,
                timeout=timeout,
            )
            console.print(result)

    asyncio.run(_call())

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
            return json.loads(value)
        except Exception:
            return value

    async def _run() -> None:
        from mpreg.client.unified_client import MPREGClient

        async with MPREGClient(url) as c:
            result = await c.queue_send(
                queue_name, _parse(payload), topic=topic or queue_name
            )
            console.print(result)

    asyncio.run(_run())

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

    asyncio.run(_run())

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
            return json.loads(v)
        except Exception:
            return v

    async def _run() -> None:
        from mpreg.client.unified_client import MPREGClient

        async with MPREGClient(url) as c:
            result = await c.cache_put(namespace, identifier, _parse(value))
            console.print(result)

    asyncio.run(_run())

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
            return json.loads(v)
        except Exception:
            return v

    async def _run() -> None:
        from mpreg.client.unified_client import MPREGClient

        async with MPREGClient(url) as c:
            result = await c.publish(topic, _parse(payload))
            console.print(result)

    asyncio.run(_run())

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

    asyncio.run(_list())

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

    asyncio.run(_stats())

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

    asyncio.run(_resync())

def _parse_metadata_items(items: tuple[str, ...]) -> dict[str, MetadataValue]:
    metadata: dict[str, MetadataValue] = {}
    for item in items:
        if "=" in item:
            key, value = item.split("=", 1)
            key = key.strip()
            if not key:
                continue
            try:
                parsed = json.loads(value)
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
@click.option("--target", multiple=True, help="Service target host (repeatable)")
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

    asyncio.run(_register())

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

    asyncio.run(_unregister())

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

    asyncio.run(_list())

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

    asyncio.run(_describe())

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

    asyncio.run(_resolve())

@client.group("namespace-policy")
def namespace_policy():
    """Namespace policy management commands."""
    pass

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
        rules = json.load(handle)

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

    asyncio.run(_validate())

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
        rules = json.load(handle)

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

    asyncio.run(_apply())

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

    asyncio.run(_export())

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

    asyncio.run(_audit())

@cli.group()
def discovery():
    """Discovery plane commands."""
    pass

@cli.group()
def report():
    """Reporting commands."""
    pass

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

    asyncio.run(_query())

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

    asyncio.run(_summary())

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

    asyncio.run(_watch())

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

    asyncio.run(_status())

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

    asyncio.run(_audit())

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

    asyncio.run(_report())

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

    asyncio.run(_report())

@cli.group()
def server():
    """Server management commands."""
    pass

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

    asyncio.run(_start())

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

    asyncio.run(_start())

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
@add_format_option
def doctor(
    url: str | None,
    token: str | None,
    timeout: float,
    output_format: str,
    deep: bool,
) -> None:
    """Probe monitoring health, discovery, and persistence endpoints."""

    monitoring_url = url or os.environ.get("MPREG_MONITORING_URL")
    if not monitoring_url:
        raise click.UsageError(
            "Provide --url or set MPREG_MONITORING_URL to the monitoring HTTP base."
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
        failures = 0
        rows: list[dict[str, str]] = []
        timeout_cfg = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=timeout_cfg, headers=headers) as session:
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
                        rows.append(
                            {
                                "check": name,
                                "status": "OK" if ok else str(response.status),
                                "detail": body_preview,
                            }
                        )
                except Exception as exc:  # noqa: BLE001 - doctor must report all failures
                    failures += 1
                    rows.append(
                        {
                            "check": name,
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

    raise SystemExit(asyncio.run(_doctor()))

@cli.command("config-check")
@click.argument("settings_path", type=click.Path(exists=True))
@add_format_option
def config_check(settings_path: str, output_format: str) -> None:
    """Validate a settings file and report grouped configuration summary."""
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
        warnings.append("monitoring CORS is enabled — disable in production unless needed")
    if settings.monitoring_enabled and not settings.monitoring_auth_token:
        warnings.append("monitoring has no auth token — set monitoring_auth_token for production")
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
    if settings.discovery_summary_export_enabled and not settings.discovery_summary_signing_secret:
        warnings.append("summary export enabled without signing secret")
    sec = settings.fabric_route_security_config
    if settings.enable_cache_federation or (
        settings.peers and len(settings.peers) > 0
    ):
        if sec is None or not sec.require_signatures:
            warnings.append(
                "federated/multi-peer fabric without fabric_route_require_signatures=true"
            )
        if not getattr(settings, "fabric_gossip_require_hmac", False):
            warnings.append(
                "gossip envelopes are unsigned by default "
                "(fabric_gossip_require_hmac=false) — route signing ≠ gossip authenticity"
            )
    if (
        settings.persistence_config is not None
        and not getattr(settings, "fabric_snapshot_fail_on_restore_error", False)
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
        warnings.append(
            "fabric_route_allow_unsigned=true weakens route authenticity"
        )
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
        warnings.append(
            "fabric_gossip_hmac_secret is still the profile placeholder"
        )
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
            "namespace/tenant gates are off (lab default; enable for multi-tenant)"
        )
    report = {"groups": groups, "warnings": warnings, "ok": len(warnings) == 0}
    emit(report, output_format=output_format, table_title="Config check")
    if warnings:
        raise SystemExit(2)

@cli.group("admin")
def admin_group():
    """Management mutations: drain, detach, policy, audit (monitoring HTTP)."""
    pass

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
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{base}/mgmt/v1/nodes/drain",
                json=body,
                headers=_admin_headers(token),
            ) as resp:
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

    asyncio.run(_run())

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

    asyncio.run(_run())

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
def admin_audit(
    url: str | None, token: str | None, limit: int, as_json: bool
) -> None:
    """Show recent management mutation audit entries."""

    async def _run() -> None:
        base = _admin_base_url(url)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{base}/mgmt/v1/audit",
                params={"limit": str(limit)},
                headers=_admin_headers(token),
            ) as resp:
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

    asyncio.run(_run())

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
    import json as _json
    from pathlib import Path

    async def _run() -> None:
        base = _admin_base_url(url)
        if json_body:
            body = _json.loads(json_body)
        elif policy_file:
            body = _json.loads(Path(policy_file).read_text(encoding="utf-8"))
        else:
            body = {}
        if not isinstance(body, dict):
            raise click.UsageError("Policy body must be a JSON object")
        if actor is not None:
            body.setdefault("actor", actor)
        if reason is not None:
            body.setdefault("reason", reason)
        path = "/mgmt/v1/policy/dry-run" if dry_run else "/mgmt/v1/policy/apply"
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{base}{path}",
                json=body,
                headers=_admin_headers(token),
            ) as resp:
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

    asyncio.run(_run())

@cli.group("profile")
def profile_group():
    """List and show built-in settings profiles."""
    pass

def _profiles_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "profiles"

@profile_group.command("list")
def profile_list() -> None:
    """List packaged TOML settings profiles."""
    root = _profiles_dir()
    if not root.is_dir():
        console.print("[red]No profiles directory found.[/red]")
        return
    table = Table(title="MPREG settings profiles")
    table.add_column("Name")
    table.add_column("Path")
    for path in sorted(root.glob("*.toml")):
        table.add_row(path.stem, str(path))
    console.print(table)
    console.print(
        "Start with: [bold]mpreg server start-config "
        f"{root}/dev.toml[/bold]"
    )

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
    """Run tiered MPREG demos."""
    pass

@demo.command("tier1")
@click.argument(
    "system",
    type=click.Choice(["rpc", "pubsub", "queue", "cache", "federation", "monitoring"]),
)
def demo_tier1(system: str) -> None:
    """Run a tier 1 single-system demo."""
    from mpreg.examples.tier1_single_system_full import SYSTEMS

    asyncio.run(SYSTEMS[system]())

@demo.command("tier2")
def demo_tier2() -> None:
    """Run the tier 2 integration demos."""
    from mpreg.examples.tier2_integrations import main as tier2_main

    asyncio.run(tier2_main())

@demo.command("tier3")
def demo_tier3() -> None:
    """Run the tier 3 full-system demo."""
    from mpreg.examples.tier3_full_system_expansion import main as tier3_main

    asyncio.run(tier3_main())

@demo.command("all")
def demo_all() -> None:
    """Run tier 1 (RPC), tier 2, and tier 3 demos."""
    from mpreg.examples.tier1_single_system_full import demo_rpc
    from mpreg.examples.tier2_integrations import main as tier2_main
    from mpreg.examples.tier3_full_system_expansion import main as tier3_main

    async def _run():
        await demo_rpc()
        await tier2_main()
        await tier3_main()

    asyncio.run(_run())

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
            import json

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
            console.print(json.dumps(clusters_dict, indent=2))

    asyncio.run(_discover())

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

    asyncio.run(_register())

@cli.command()
@click.argument("cluster_id")
def unregister(cluster_id: str):
    """Unregister a federation cluster."""

    async def _unregister():
        federation_cli = FederationCLI()
        success = await federation_cli.unregister_cluster(cluster_id)

        if not success:
            sys.exit(1)

    asyncio.run(_unregister())

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
            import json

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
            console.print(json.dumps(serializable_results, indent=2, default=str))

    asyncio.run(_health())

@cli.command("federation-metrics")
@click.option("--cluster", "-c", help="Specific cluster ID to show metrics for")
def federation_metrics(cluster: str | None):
    """Display federation performance metrics for clusters."""

    async def _metrics():
        federation_cli = FederationCLI()
        await federation_cli.show_metrics(cluster)

    asyncio.run(_metrics())

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

    asyncio.run(_validate())

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

    asyncio.run(_deploy())

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

    asyncio.run(_cleanup())

@cli.group()
def monitor():
    """Fabric federation monitoring commands."""
    pass

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
def health_watch(interval: int, clusters: tuple[str, ...], summary: bool, url: str | None, output_format: str):
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
                                    emit(payload, output_format=output_format, table_title="Health")
                        else:
                            endpoint = (
                                f"{base_url}/health/summary"
                                if summary
                                else f"{base_url}/health"
                            )
                            async with session.get(endpoint) as response:
                                payload = await response.json()
                                emit(payload, output_format=output_format, table_title="Health")
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

    asyncio.run(_health_watch())

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
def health_endpoint(cluster: str | None, summary: bool, url: str | None, output_format: str) -> None:
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

    asyncio.run(_health_endpoint())

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
def metrics_watch(interval: int, clusters: tuple[str, ...], system: str, url: str | None, output_format: str):
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
                            emit(payload, output_format=output_format, table_title="Metrics")
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

    asyncio.run(_metrics_watch())

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
def status(cluster: str | None, output: str | None, output_format: str, url: str | None) -> None:
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

    asyncio.run(_status())

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
                emit(payload, output_format=output_format, table_title="Route decisions")

    asyncio.run(_run())

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
def route_trace(destination: str, avoid: tuple[str, ...], url: str | None, output_format: str) -> None:
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
                    emit(payload, output_format=output_format, table_title="Route trace")
                    return
                emit(payload, output_format=output_format, table_title="Route trace")

    asyncio.run(_route_trace())

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

    asyncio.run(_link_state())

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
                    emit(payload, output_format=output_format, table_title="Transport endpoints")
                    return
                emit(payload, output_format=output_format, table_title="Transport endpoints")

    asyncio.run(_transport_endpoints())

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

    asyncio.run(_metrics())

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

    asyncio.run(_prom())

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
                    emit(payload, output_format=output_format, table_title="Persistence")
                    return
                emit(payload, output_format=output_format, table_title="Persistence")

    asyncio.run(_persistence())

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
                    emit(payload, output_format=output_format, table_title="DNS metrics")
                    return
                emit(payload, output_format=output_format, table_title="DNS metrics")

    asyncio.run(_dns_metrics())

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
                        emit(payload, output_format=output_format, table_title="DNS metrics")
                    await asyncio.sleep(interval)
            except KeyboardInterrupt:
                console.print("\n[yellow]⚠️ DNS metrics watch stopped[/yellow]")

    asyncio.run(_dns_watch())

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
                        emit(payload, output_format=output_format, table_title="Persistence")
                await asyncio.sleep(interval)
        except KeyboardInterrupt:
            console.print("\n[yellow]⚠️ Persistence monitoring stopped[/yellow]")

    asyncio.run(_persistence_watch())

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

    asyncio.run(_endpoints())

@cli.group()
def auto_discovery():
    """Auto-discovery management commands."""
    pass

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
            import json

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
            console.print(json.dumps(clusters_dict, indent=2))

    asyncio.run(_auto_discover())

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

    import json
    from pathlib import Path

    output_file = Path(output_path)
    with open(output_file, "w") as f:
        json.dump(discovery_config, f, indent=2)

    console.print(
        f"[green]✅ Auto-discovery configuration generated: {output_path}[/green]"
    )

@cli.group()
def config():
    """Configuration management commands."""
    pass

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
    import json

    from rich.panel import Panel
    from rich.syntax import Syntax

    try:
        with open(config_path) as f:
            config_data = json.load(f)

        if key:
            # Show specific key
            if key in config_data:
                console.print(json.dumps(config_data[key], indent=2))
            else:
                console.print(f"[red]❌ Key '{key}' not found in configuration[/red]")
        else:
            # Show entire configuration
            config_syntax = Syntax(
                json.dumps(config_data, indent=2),
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
