from __future__ import annotations

#!/usr/bin/env python3
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

console = Console()


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = "DEBUG" if verbose else "INFO"
    configure_logging(level, colorize=True)


@click.group()
@click.version_option(__version__, prog_name="mpreg")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.pass_context
def cli(ctx, verbose: bool):
    """
    MPREG Fabric Federation Management CLI.

    Comprehensive tools for managing fabric-enabled MPREG clusters including
    discovery, registration, health monitoring, and deployment automation.
    """
    setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


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
        include_ingress = include_ingress or bool(
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
                include_ingress=include_ingress,
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
    default=True,
    help="Enable or disable CORS for monitoring endpoints",
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
def start_server(
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
    persistence_mode: str,
    persistence_dir: str | None,
    persistence_sqlite_filename: str,
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
            persistence_config=persistence_config,
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
        server_instance = MPREGServer(settings=settings)
        await server_instance.server()

    asyncio.run(_start())


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


@cli.command()
@click.option("--cluster", "-c", help="Specific cluster ID to show metrics for")
def metrics(cluster: str | None):
    """Display performance metrics for clusters."""

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
def health_watch(
    interval: int, clusters: tuple[str, ...], summary: bool, url: str | None
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
                                    console.print_json(data=payload)
                        else:
                            endpoint = (
                                f"{base_url}/health/summary"
                                if summary
                                else f"{base_url}/health"
                            )
                            async with session.get(endpoint) as response:
                                payload = await response.json()
                                console.print_json(data=payload)
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
def health_endpoint(cluster: str | None, summary: bool, url: str | None) -> None:
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
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

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
def metrics_watch(
    interval: int, clusters: tuple[str, ...], system: str, url: str | None
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
                            console.print_json(data=payload)
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
    default="table",
    help="Output format",
)
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def status(cluster: str | None, output: str, url: str | None) -> None:
    """Show a compact admin status summary from monitoring endpoints."""

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
            console.print_json(data=results)
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
def route_trace(destination: str, avoid: tuple[str, ...], url: str | None) -> None:
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
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

    asyncio.run(_route_trace())


@monitor.command("link-state")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def link_state(url: str | None) -> None:
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
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

    asyncio.run(_link_state())


@monitor.command("transport-endpoints")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def transport_endpoints(url: str | None) -> None:
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
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

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
def metrics(system: str, url: str | None) -> None:
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
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

    asyncio.run(_metrics())


@monitor.command("persistence")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def persistence(url: str | None) -> None:
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
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

    asyncio.run(_persistence())


@monitor.command("dns")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def dns_metrics(url: str | None) -> None:
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
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

    asyncio.run(_dns_metrics())


@monitor.command("dns-watch")
@click.option("--interval", default=5.0, help="Polling interval in seconds")
@click.option(
    "--url",
    default=None,
    envvar="MPREG_MONITORING_URL",
    help="Monitoring base URL (or set MPREG_MONITORING_URL)",
)
def dns_watch(interval: float, url: str | None) -> None:
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
                        console.print_json(data=payload)
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
def persistence_watch(interval: int, url: str | None) -> None:
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
                        console.print_json(data=payload)
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
def endpoints(url: str | None) -> None:
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
                    console.print_json(data=payload)
                    return
                console.print_json(data=payload)

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
