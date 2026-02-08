from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, TypeVar

from loguru import logger

from ..core.cluster_map import (
    CatalogQueryRequest,
    CatalogQueryResponse,
    CatalogWatchRequest,
    CatalogWatchResponse,
    ClusterMapRequest,
    ClusterMapResponse,
    ClusterMapSnapshot,
    ListPeersRequest,
    PeerSnapshot,
)
from ..core.discovery_monitoring import (
    DiscoveryAccessAuditRequest,
    DiscoveryAccessAuditResponse,
)
from ..core.discovery_resolver import (
    DiscoveryResolverCacheStatsResponse,
    DiscoveryResolverResyncResponse,
)
from ..core.discovery_summary import (
    SummaryQueryRequest,
    SummaryQueryResponse,
    SummaryWatchRequest,
    SummaryWatchResponse,
)
from ..core.dns_registry import (
    DnsDescribeRequest,
    DnsDescribeResponse,
    DnsListRequest,
    DnsListResponse,
    DnsRegisterRequest,
    DnsRegisterResponse,
    DnsUnregisterRequest,
    DnsUnregisterResponse,
)
from ..core.model import CommandNotFoundException, MPREGException, RPCCommand
from ..core.namespace_policy import (
    NamespacePolicyApplyRequest,
    NamespacePolicyApplyResponse,
    NamespacePolicyAuditRequest,
    NamespacePolicyAuditResponse,
    NamespacePolicyExportResponse,
    NamespacePolicyValidationResponse,
    NamespaceStatusRequest,
    NamespaceStatusResponse,
)
from ..core.payloads import Payload, PayloadMapping, apply_overrides, parse_request
from ..core.rpc_discovery import (
    RpcDescribeRequest,
    RpcDescribeResponse,
    RpcListRequest,
    RpcListResponse,
    RpcReportRequest,
    RpcReportResponse,
)
from ..core.transport.interfaces import SecurityConfig, TransportConfig
from .client import Client

client_api_log = logger

RequestT = TypeVar("RequestT")


@dataclass(slots=True)
class MPREGClientAPI:
    """A high-level client API for interacting with the MPREG cluster."""

    url: str
    full_log: bool = True
    auth_token: str | None = None
    api_key: str | None = None
    transport_config: TransportConfig | None = None

    # Fields assigned in __post_init__
    _client: Client = field(init=False)  # Needs special initialization in __post_init__
    _connected: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        """Initialize client and connection state."""
        transport_config = self.transport_config or TransportConfig()
        if self.auth_token or self.api_key:
            security = transport_config.security
            transport_config = replace(
                transport_config,
                security=SecurityConfig(
                    ssl_context=security.ssl_context,
                    verify_cert=security.verify_cert,
                    cert_file=security.cert_file,
                    key_file=security.key_file,
                    ca_file=security.ca_file,
                    auth_token=self.auth_token or security.auth_token,
                    api_key=self.api_key or security.api_key,
                ),
            )
        self._client = Client(
            url=self.url,
            full_log=self.full_log,
            transport_config=transport_config,
        )

    def _prepare_request(
        self,
        request_cls: type[RequestT],
        request: PayloadMapping | RequestT | None,
        overrides: PayloadMapping | None,
    ) -> RequestT:
        if isinstance(request, request_cls):
            if overrides:
                return apply_overrides(request, overrides)
            return request
        return parse_request(request_cls, request, overrides)

    def _request_payload(
        self,
        request_cls: type[RequestT],
        request: PayloadMapping | RequestT | None,
        overrides: PayloadMapping | None,
    ) -> Payload | None:
        if request is None and not overrides:
            return None
        request_obj = self._prepare_request(request_cls, request, overrides)
        payload = request_obj.to_dict()
        return payload or None

    async def _call_payload(
        self,
        command: str,
        payload: Payload | None,
        *,
        target_cluster: str | None = None,
    ) -> object:
        if payload is None:
            return await self.call(command, target_cluster=target_cluster)
        return await self.call(command, payload, target_cluster=target_cluster)

    async def connect(self) -> None:
        """Establishes a connection to the MPREG server."""
        if not self._connected:
            await self._client.connect()
            self._connected = True
            client_api_log.info("Connected to MPREG server at {}", self._client.url)

    async def disconnect(self) -> None:
        """Closes the connection to the MPREG server."""
        if self._connected:
            await self._client.disconnect()
            self._connected = False

    async def call(
        self,
        fun: str,
        *args: Any,
        locs: frozenset[str] | None = None,
        function_id: str | None = None,
        version_constraint: str | None = None,
        target_cluster: str | None = None,
        routing_topic: str | None = None,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """Calls an RPC function on the MPREG cluster.

        Args:
            fun: The name of the RPC function to call.
            *args: Positional arguments for the RPC function.
            locs: Optional set of resource locations where the command can be executed.
            target_cluster: Optional target cluster for federated routing.
            routing_topic: Optional unified routing topic for policy-based routing.
            timeout: Optional timeout in seconds for the RPC call.
            **kwargs: Keyword arguments for the RPC function.

        Returns:
            The result of the RPC function call.

        Raises:
            CommandNotFoundException: If the specified function is not found on any available server.
            asyncio.TimeoutError: If the RPC call times out.
            Exception: For other RPC errors returned by the server.
        """
        if not self._connected:
            await self.connect()

        command = RPCCommand(
            name=fun,  # Using fun as name for simplicity in this API
            fun=fun,
            args=tuple(args),
            locs=locs or frozenset(),
            function_id=function_id,
            version_constraint=version_constraint,
            target_cluster=target_cluster,
            routing_topic=routing_topic,
            kwargs=kwargs,
        )
        try:
            result = await self._client.request(cmds=[command], timeout=timeout)
            # For single command calls, extract the result directly
            if isinstance(result, dict) and len(result) == 1:
                return list(result.values())[0]
            return result
        except CommandNotFoundException as e:
            raise e
        except MPREGException as e:
            client_api_log.error(
                "RPC Call Failed: {}: {}", e.rpc_error.code, e.rpc_error.message
            )
            raise e
        except Exception as e:
            client_api_log.error("RPC Call Failed: {}", e)
            raise

    async def list_peers(
        self,
        request: PayloadMapping | ListPeersRequest | None = None,
        *,
        target_cluster: str | None = None,
        **kwargs: object,
    ) -> tuple[PeerSnapshot, ...]:
        """Return the cluster's current peer list (optionally scoped)."""
        payload = self._request_payload(ListPeersRequest, request, kwargs)
        result = await self._call_payload(
            "list_peers", payload, target_cluster=target_cluster
        )
        if not isinstance(result, list):
            raise TypeError(f"Expected peer list response, got {type(result).__name__}")
        return tuple(
            PeerSnapshot.from_dict(item) for item in result if isinstance(item, dict)
        )

    async def cluster_map(self) -> ClusterMapSnapshot:
        """Return a snapshot of the cluster map for discovery-aware clients."""
        result = await self.call("cluster_map")
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected cluster map response, got {type(result).__name__}"
            )
        return ClusterMapSnapshot.from_dict(result)

    async def cluster_map_v2(
        self,
        request: PayloadMapping | ClusterMapRequest | None = None,
        **kwargs: object,
    ) -> ClusterMapResponse:
        """Return a scoped cluster map snapshot for discovery-aware clients."""
        payload = self._request_payload(ClusterMapRequest, request, kwargs)
        result = await self._call_payload("cluster_map_v2", payload)
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected cluster map v2 response, got {type(result).__name__}"
            )
        return ClusterMapResponse.from_dict(result)

    async def catalog_query(
        self,
        request: PayloadMapping | CatalogQueryRequest | None = None,
        **kwargs: object,
    ) -> CatalogQueryResponse:
        """Run a scoped catalog query for discovery-aware clients."""
        payload = self._request_payload(CatalogQueryRequest, request, kwargs)
        result = await self._call_payload("catalog_query", payload)
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected catalog query response, got {type(result).__name__}"
            )
        return CatalogQueryResponse.from_dict(result)

    async def catalog_watch(
        self,
        request: PayloadMapping | CatalogWatchRequest | None = None,
        **kwargs: object,
    ) -> CatalogWatchResponse:
        """Return catalog watch topic metadata for discovery delta streams."""
        payload = self._request_payload(CatalogWatchRequest, request, kwargs)
        result = await self._call_payload("catalog_watch", payload)
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected catalog watch response, got {type(result).__name__}"
            )
        return CatalogWatchResponse.from_dict(result)

    async def dns_register(
        self,
        request: PayloadMapping | DnsRegisterRequest | None = None,
        **kwargs: object,
    ) -> DnsRegisterResponse:
        """Register a service endpoint for DNS interoperability."""
        payload = self._request_payload(DnsRegisterRequest, request, kwargs)
        result = await self._call_payload("dns_register", payload)
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected dns register response, got {type(result).__name__}"
            )
        return DnsRegisterResponse.from_dict(result)

    async def dns_unregister(
        self,
        request: PayloadMapping | DnsUnregisterRequest | None = None,
        **kwargs: object,
    ) -> DnsUnregisterResponse:
        """Unregister a service endpoint from DNS interoperability."""
        payload = self._request_payload(DnsUnregisterRequest, request, kwargs)
        result = await self._call_payload("dns_unregister", payload)
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected dns unregister response, got {type(result).__name__}"
            )
        return DnsUnregisterResponse.from_dict(result)

    async def dns_list(
        self,
        request: PayloadMapping | DnsListRequest | None = None,
        **kwargs: object,
    ) -> DnsListResponse:
        """List DNS-exposed service endpoints from the discovery catalog."""
        payload = self._request_payload(DnsListRequest, request, kwargs)
        result = await self._call_payload("dns_list", payload)
        if not isinstance(result, dict):
            raise TypeError(f"Expected dns list response, got {type(result).__name__}")
        return DnsListResponse.from_dict(result)

    async def dns_describe(
        self,
        request: PayloadMapping | DnsDescribeRequest | None = None,
        **kwargs: object,
    ) -> DnsDescribeResponse:
        """Return detailed DNS service endpoint records."""
        payload = self._request_payload(DnsDescribeRequest, request, kwargs)
        result = await self._call_payload("dns_describe", payload)
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected dns describe response, got {type(result).__name__}"
            )
        return DnsDescribeResponse.from_dict(result)

    async def summary_query(
        self,
        request: PayloadMapping | SummaryQueryRequest | None = None,
        **kwargs: object,
    ) -> SummaryQueryResponse:
        """Return summarized discovery records for local or global scopes."""
        payload = self._request_payload(SummaryQueryRequest, request, kwargs)
        result = await self._call_payload("summary_query", payload)
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected summary query response, got {type(result).__name__}"
            )
        return SummaryQueryResponse.from_dict(result)

    async def summary_watch(
        self,
        request: PayloadMapping | SummaryWatchRequest | None = None,
        **kwargs: object,
    ) -> SummaryWatchResponse:
        """Return summary export topic metadata for discovery summaries."""
        payload = self._request_payload(SummaryWatchRequest, request, kwargs)
        result = await self._call_payload("summary_watch", payload)
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected summary watch response, got {type(result).__name__}"
            )
        return SummaryWatchResponse.from_dict(result)

    async def rpc_list(
        self,
        request: PayloadMapping | RpcListRequest | None = None,
        *,
        target_cluster: str | None = None,
        **kwargs: object,
    ) -> RpcListResponse:
        """Return a filtered list of RPC capabilities with summaries."""
        payload = self._request_payload(RpcListRequest, request, kwargs)
        result = await self._call_payload(
            "rpc_list", payload, target_cluster=target_cluster
        )
        if not isinstance(result, dict):
            raise TypeError(f"Expected rpc_list response, got {type(result).__name__}")
        return RpcListResponse.from_dict(result)

    async def rpc_describe(
        self,
        request: PayloadMapping | RpcDescribeRequest | None = None,
        *,
        target_cluster: str | None = None,
        **kwargs: object,
    ) -> RpcDescribeResponse:
        """Return detailed RPC specifications from local, catalog, or scatter modes."""
        payload = self._request_payload(RpcDescribeRequest, request, kwargs)
        result = await self._call_payload(
            "rpc_describe", payload, target_cluster=target_cluster
        )
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected rpc_describe response, got {type(result).__name__}"
            )
        return RpcDescribeResponse.from_dict(result)

    async def rpc_report(
        self,
        request: PayloadMapping | RpcReportRequest | None = None,
        *,
        target_cluster: str | None = None,
        **kwargs: object,
    ) -> RpcReportResponse:
        """Return aggregated RPC inventory metrics for reporting."""
        payload = self._request_payload(RpcReportRequest, request, kwargs)
        result = await self._call_payload(
            "rpc_report", payload, target_cluster=target_cluster
        )
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected rpc_report response, got {type(result).__name__}"
            )
        return RpcReportResponse.from_dict(result)

    async def discovery_access_audit(
        self,
        request: PayloadMapping | DiscoveryAccessAuditRequest | None = None,
        **kwargs: object,
    ) -> DiscoveryAccessAuditResponse:
        """Fetch discovery access audit entries."""
        payload = self._request_payload(DiscoveryAccessAuditRequest, request, kwargs)
        result = await self._call_payload("discovery_access_audit", payload)
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected discovery access audit response, got {type(result).__name__}"
            )
        return DiscoveryAccessAuditResponse.from_dict(result)

    async def resolver_cache_stats(self) -> DiscoveryResolverCacheStatsResponse:
        """Return resolver cache statistics for discovery resolver nodes."""
        result = await self.call("resolver_cache_stats")
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected resolver cache stats response, got {type(result).__name__}"
            )
        return DiscoveryResolverCacheStatsResponse.from_dict(result)

    async def resolver_resync(self) -> DiscoveryResolverResyncResponse:
        """Trigger a resolver cache resync from the fabric catalog."""
        result = await self.call("resolver_resync")
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected resolver resync response, got {type(result).__name__}"
            )
        return DiscoveryResolverResyncResponse.from_dict(result)

    async def namespace_status(
        self,
        request: PayloadMapping | NamespaceStatusRequest | None = None,
        **kwargs: object,
    ) -> NamespaceStatusResponse:
        """Return namespace policy status for the given namespace."""
        payload = self._request_payload(NamespaceStatusRequest, request, kwargs)
        result = await self._call_payload("namespace_status", payload)
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected namespace status response, got {type(result).__name__}"
            )
        return NamespaceStatusResponse.from_dict(result)

    async def namespace_policy_export(self) -> NamespacePolicyExportResponse:
        """Export current namespace policy configuration."""
        result = await self.call("namespace_policy_export")
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected namespace policy export response, got {type(result).__name__}"
            )
        return NamespacePolicyExportResponse.from_dict(result)

    async def namespace_policy_validate(
        self,
        request: PayloadMapping | NamespacePolicyApplyRequest | None = None,
        **kwargs: object,
    ) -> NamespacePolicyValidationResponse:
        """Validate namespace policy rules."""
        payload = self._request_payload(NamespacePolicyApplyRequest, request, kwargs)
        result = await self._call_payload("namespace_policy_validate", payload)
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected namespace policy validate response, got {type(result).__name__}"
            )
        return NamespacePolicyValidationResponse.from_dict(result)

    async def namespace_policy_apply(
        self,
        request: PayloadMapping | NamespacePolicyApplyRequest | None = None,
        **kwargs: object,
    ) -> NamespacePolicyApplyResponse:
        """Apply namespace policy rules."""
        payload = self._request_payload(NamespacePolicyApplyRequest, request, kwargs)
        result = await self._call_payload("namespace_policy_apply", payload)
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected namespace policy apply response, got {type(result).__name__}"
            )
        return NamespacePolicyApplyResponse.from_dict(result)

    async def namespace_policy_audit(
        self,
        request: PayloadMapping | NamespacePolicyAuditRequest | None = None,
        **kwargs: object,
    ) -> NamespacePolicyAuditResponse:
        """Fetch namespace policy audit entries."""
        payload = self._request_payload(NamespacePolicyAuditRequest, request, kwargs)
        result = await self._call_payload("namespace_policy_audit", payload)
        if not isinstance(result, dict):
            raise TypeError(
                f"Expected namespace policy audit response, got {type(result).__name__}"
            )
        return NamespacePolicyAuditResponse.from_dict(result)

    async def __aenter__(self) -> MPREGClientAPI:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.disconnect()
