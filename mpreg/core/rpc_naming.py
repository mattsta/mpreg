"""Fully-qualified RPC naming (FQN) helpers.

Design (Phase H / sole-consumer north star)
------------------------------------------
* Every RPC name on the wire is a dotted FQN (``namespace...leaf``).
* Bare names (no ``.``) are the **only** exception: they auto-qualify by
  prepending the active/custom namespace (default ``app``).
* The platform root ``mpreg`` / ``mpreg.*`` is a **namespace deny** for
  user registration — not a denylist of individual command short-names.
  Users may register anything outside that root.
* Hierarchical namespaces double as optional operator↔client permission
  / conformance bounds (``bound_namespace``): callers and registrars may
  be locked to a prefix or a deeper position under it.

Platform builtins live under well-known ``mpreg.*`` leaves (see
:class:`PlatformRpc`). Client convenience methods and internal plane
registration use those constants; application code uses bare names or
its own FQNs under non-platform roots.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

# ── Roots & defaults ──────────────────────────────────────────────────

PLATFORM_NAMESPACE_ROOT: Final = "mpreg"
DEFAULT_USER_NAMESPACE: Final = "app"

# Well-known platform sub-namespaces (hierarchical permission boundaries).
NS_SYSTEM: Final = f"{PLATFORM_NAMESPACE_ROOT}.system"
NS_DISCO: Final = f"{PLATFORM_NAMESPACE_ROOT}.disco"
NS_DNS: Final = f"{PLATFORM_NAMESPACE_ROOT}.dns"
NS_RPC_META: Final = f"{PLATFORM_NAMESPACE_ROOT}.rpc"
NS_POLICY: Final = f"{PLATFORM_NAMESPACE_ROOT}.policy"
NS_QUEUE: Final = f"{PLATFORM_NAMESPACE_ROOT}.queue"
NS_CACHE: Final = f"{PLATFORM_NAMESPACE_ROOT}.cache"

class PlatformRpc:
    """Canonical FQNs for platform-owned RPC commands.

    Registration of these names is allowed only via
    ``allow_platform=True`` (server internals). Users cannot inject into
    ``mpreg.*``.
    """

    # system / diagnostics
    ECHO: Final = f"{NS_SYSTEM}.echo"
    ECHOS: Final = f"{NS_SYSTEM}.echos"
    LIST_PEERS: Final = f"{NS_SYSTEM}.list_peers"
    CLUSTER_MAP: Final = f"{NS_SYSTEM}.cluster_map"
    CLUSTER_MAP_V2: Final = f"{NS_SYSTEM}.cluster_map_v2"

    # discovery / catalog
    CATALOG_QUERY: Final = f"{NS_DISCO}.catalog_query"
    CATALOG_WATCH: Final = f"{NS_DISCO}.catalog_watch"
    SUMMARY_QUERY: Final = f"{NS_DISCO}.summary_query"
    SUMMARY_WATCH: Final = f"{NS_DISCO}.summary_watch"
    RESOLVER_CACHE_STATS: Final = f"{NS_DISCO}.resolver_cache_stats"
    RESOLVER_RESYNC: Final = f"{NS_DISCO}.resolver_resync"
    DISCOVERY_ACCESS_AUDIT: Final = f"{NS_DISCO}.access_audit"

    # DNS interoperability
    DNS_REGISTER: Final = f"{NS_DNS}.register"
    DNS_UNREGISTER: Final = f"{NS_DNS}.unregister"
    DNS_LIST: Final = f"{NS_DNS}.list"
    DNS_DESCRIBE: Final = f"{NS_DNS}.describe"

    # RPC meta
    RPC_LIST: Final = f"{NS_RPC_META}.list"
    RPC_DESCRIBE_LOCAL: Final = f"{NS_RPC_META}.describe_local"
    RPC_DESCRIBE: Final = f"{NS_RPC_META}.describe"
    RPC_REPORT: Final = f"{NS_RPC_META}.report"

    # namespace policy
    NAMESPACE_STATUS: Final = f"{NS_POLICY}.namespace_status"
    NAMESPACE_POLICY_VALIDATE: Final = f"{NS_POLICY}.namespace_policy_validate"
    NAMESPACE_POLICY_APPLY: Final = f"{NS_POLICY}.namespace_policy_apply"
    NAMESPACE_POLICY_EXPORT: Final = f"{NS_POLICY}.namespace_policy_export"
    NAMESPACE_POLICY_AUDIT: Final = f"{NS_POLICY}.namespace_policy_audit"

    # queue plane
    QUEUE_CREATE: Final = f"{NS_QUEUE}.create"
    QUEUE_SEND: Final = f"{NS_QUEUE}.send"
    QUEUE_ACK: Final = f"{NS_QUEUE}.ack"
    QUEUE_RECEIVE: Final = f"{NS_QUEUE}.receive"

    # cache plane
    CACHE_GET: Final = f"{NS_CACHE}.get"
    CACHE_PUT: Final = f"{NS_CACHE}.put"
    CACHE_INVALIDATE: Final = f"{NS_CACHE}.invalidate"
    # T42: ops-driven CFT residual re-ABORT (not automatic heal)
    CACHE_STRONG_RETRY_ABORT: Final = f"{NS_CACHE}.strong_retry_abort"

@dataclass(frozen=True, slots=True)
class RpcNameContext:
    """Active namespace + optional hierarchical bound for qualify/assert."""

    default_namespace: str = DEFAULT_USER_NAMESPACE
    bound_namespace: str | None = None
    allow_platform: bool = False

    def qualify(self, name: str) -> str:
        return qualify_rpc_name(name, self.default_namespace)

    def assert_registration(self, fqn: str) -> None:
        assert_registration_allowed(
            fqn,
            allow_platform=self.allow_platform,
            bound_namespace=self.bound_namespace,
        )

    def assert_call(self, fqn: str) -> None:
        assert_call_allowed(
            fqn,
            allow_platform=True,  # calling platform is fine; injecting is not
            bound_namespace=self.bound_namespace,
        )

def normalize_namespace(namespace: str | None) -> str:
    """Return a non-empty namespace string (default user root)."""
    if namespace is None:
        return DEFAULT_USER_NAMESPACE
    value = namespace.strip()
    if not value:
        return DEFAULT_USER_NAMESPACE
    if value.endswith("."):
        value = value.rstrip(".")
    if not value:
        return DEFAULT_USER_NAMESPACE
    return value

def is_fqn(name: str) -> bool:
    """True when *name* already contains a namespace separator."""
    return "." in name

def leaf_name(fqn: str) -> str:
    """Last segment of an FQN (or the whole string if bare)."""
    if not fqn:
        return fqn
    if "." not in fqn:
        return fqn
    return fqn.rsplit(".", 1)[-1]

def namespace_of(fqn: str) -> str:
    """Parent namespace of an FQN (empty string for bare names)."""
    if "." not in fqn:
        return ""
    return fqn.rsplit(".", 1)[0]

def is_under_namespace(name: str, bound: str) -> bool:
    """True if *name* equals *bound* or is a hierarchical child of it.

    Examples::

        is_under_namespace("app.add", "app") → True
        is_under_namespace("app.orders.create", "app.orders") → True
        is_under_namespace("app.add", "app.orders") → False
        is_under_namespace("mpreg.system.echo", "mpreg") → True
    """
    if not bound:
        return True
    bound_n = bound.rstrip(".")
    if not bound_n:
        return True
    if name == bound_n:
        return True
    return name.startswith(f"{bound_n}.")

def is_platform_namespace(name: str) -> bool:
    """True if *name* is the platform root or any ``mpreg.*`` FQN."""
    if not name:
        return False
    if name == PLATFORM_NAMESPACE_ROOT:
        return True
    return name.startswith(f"{PLATFORM_NAMESPACE_ROOT}.")

def qualify_rpc_name(
    name: str,
    default_namespace: str | None = None,
) -> str:
    """Resolve a call/register name to a wire FQN.

    * Already-dotted names pass through unchanged (explicit wins).
    * Bare names become ``{default_namespace}.{name}``.
    """
    raw = (name or "").strip()
    if not raw:
        raise ValueError("RPC name must be a non-empty string")
    if is_fqn(raw):
        return raw
    ns = normalize_namespace(default_namespace)
    return f"{ns}.{raw}"

def assert_registration_allowed(
    fqn: str,
    *,
    allow_platform: bool = False,
    bound_namespace: str | None = None,
) -> None:
    """Raise ``ValueError`` when registration violates namespace policy.

    * Users cannot inject into ``mpreg`` / ``mpreg.*`` unless
      ``allow_platform=True`` (server builtin path only).
    * When ``bound_namespace`` is set, the FQN must sit at or under that
      hierarchical prefix (operator↔client conformance binding).
    """
    if not fqn or not str(fqn).strip():
        raise ValueError("RPC name must be a non-empty string")
    name = str(fqn).strip()
    if is_platform_namespace(name):
        if not allow_platform:
            raise ValueError(
                f"RPC registration denied: '{name}' is under the reserved platform "
                f"namespace '{PLATFORM_NAMESPACE_ROOT}.*'. Register under your own "
                f"namespace (e.g. '{DEFAULT_USER_NAMESPACE}.{leaf_name(name)}' or an "
                f"explicit FQN outside mpreg). Platform builtins are installed only "
                f"by the server with allow_platform=True."
            )
        # Platform path bypasses bound_namespace — builtins always install.
        return
    if bound_namespace:
        bound = bound_namespace.strip().rstrip(".")
        if bound and not is_under_namespace(name, bound):
            raise ValueError(
                f"RPC registration denied: '{name}' is outside bound namespace "
                f"'{bound}'. Operator/client conformance requires names at or "
                f"under that hierarchical prefix."
            )

def assert_call_allowed(
    fqn: str,
    *,
    allow_platform: bool = True,
    bound_namespace: str | None = None,
) -> None:
    """Raise ``ValueError`` when a call target violates bound-namespace policy.

    Calling into ``mpreg.*`` is allowed by default (clients need platform
    surfaces). When a bound namespace is configured, calls must target
    that bound **or** the platform root (so list_peers / rpc.list still
    work for locked clients). Pass ``allow_platform=False`` to lock
    purely inside the bound tree.
    """
    if not fqn or not str(fqn).strip():
        raise ValueError("RPC name must be a non-empty string")
    name = str(fqn).strip()
    if not bound_namespace:
        return
    bound = bound_namespace.strip().rstrip(".")
    if not bound:
        return
    if is_under_namespace(name, bound):
        return
    if allow_platform and is_platform_namespace(name):
        return
    raise ValueError(
        f"RPC call denied: '{name}' is outside bound namespace '{bound}'"
        + (
            f" (platform '{PLATFORM_NAMESPACE_ROOT}.*' calls are also blocked)"
            if not allow_platform
            else ""
        )
        + "."
    )

__all__ = [
    "DEFAULT_USER_NAMESPACE",
    "NS_CACHE",
    "NS_DISCO",
    "NS_DNS",
    "NS_POLICY",
    "NS_QUEUE",
    "NS_RPC_META",
    "NS_SYSTEM",
    "PLATFORM_NAMESPACE_ROOT",
    "PlatformRpc",
    "RpcNameContext",
    "assert_call_allowed",
    "assert_registration_allowed",
    "is_fqn",
    "is_platform_namespace",
    "is_under_namespace",
    "leaf_name",
    "namespace_of",
    "normalize_namespace",
    "qualify_rpc_name",
]
