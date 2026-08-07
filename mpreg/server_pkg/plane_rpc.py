"""Queue and cache RPC command handlers peeled from MPREGServer.

Keeps the composition root thin: registration + attach stay on the server;
request handling lives here so policy, delivery honesty, and actor binding
are testable without the full server module.
"""

from __future__ import annotations

from typing import Any

# ERG-T13-04: stable plane façade error codes (see mpreg/core/error_codes.json).
PLANE_ERR_UNAVAILABLE = 1007  # UNAVAILABLE
PLANE_ERR_INVALID_ARGUMENT = 1008  # INVALID_ARGUMENT
PLANE_ERR_UNSUPPORTED_DELIVERY = 1011  # UNSUPPORTED_DELIVERY (EO)
PLANE_ERR_UNSUPPORTED_CONSISTENCY = 1012  # UNSUPPORTED_CONSISTENCY (STRONG)

def _plane_err(
    message: str,
    *,
    code: int,
    **extra: Any,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "success": False,
        "error_message": message,
        "error_code": int(code),
    }
    out.update(extra)
    return out

def rpc_payload_dict(payload: object) -> dict[str, Any]:
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return dict(payload)
    return {}

def rpc_actor_ids(server: Any, body: dict[str, Any]) -> tuple[str, str | None]:
    """Resolve actor cluster/tenant for queue/cache RPC policy binding.

    COR-05: when namespace policy is enabled, trust **connection/session**
    identity only — never client-supplied ``cluster_id`` / ``tenant_id`` in
    the RPC body (those are spoofable). Body fields remain a convenience
    only while policy is disabled (lab/dev).

    Session identity is bound by ``MPREGServer.run_rpc`` into
    ``_rpc_actor_context`` from the accepting connection's viewer ids.
    Local same-cluster clients receive ``settings.cluster_id`` as the
    session viewer (node-local owner). Cross-cluster peer RPC uses the
    peer's advertised cluster. Body spoof fields are ignored when policy is on.
    """
    settings = getattr(server, "settings", None)
    policy_on = bool(getattr(settings, "discovery_policy_enabled", False))
    # Connection/session-bound identity (set by run_rpc / accepting session).
    conn_cluster = getattr(server, "_rpc_session_cluster_id", None)
    conn_tenant = getattr(server, "_rpc_session_tenant_id", None)
    # COR-T10-04: prefer task-local ContextVar over instance field.
    ctx = None
    try:
        from mpreg.server import _current_rpc_actor_context

        ctx = _current_rpc_actor_context.get()
    except Exception:
        ctx = None
    if not isinstance(ctx, dict):
        ctx = getattr(server, "_rpc_actor_context", None)
    if isinstance(ctx, dict):
        if conn_cluster is None:
            conn_cluster = ctx.get("cluster_id")
        if conn_tenant is None:
            conn_tenant = ctx.get("tenant_id")

    if policy_on:
        # Never read body identity under policy — spoofable.
        cluster_raw = conn_cluster or getattr(settings, "cluster_id", None) or ""
        tenant_raw = conn_tenant
    else:
        cluster_raw = (
            body.get("cluster_id")
            or body.get("actor_cluster")
            or body.get("source_cluster")
            or conn_cluster
            or getattr(settings, "cluster_id", None)
            or ""
        )
        tenant_raw = (
            body.get("tenant_id")
            or body.get("actor_tenant_id")
            or body.get("viewer_tenant_id")
            or conn_tenant
        )

    cluster_id = str(cluster_raw).strip() if cluster_raw is not None else ""
    tenant_id = str(tenant_raw).strip() if tenant_raw is not None else None
    if tenant_id == "":
        tenant_id = None
    return cluster_id, tenant_id

def register_queue_rpc_commands(server: Any) -> None:
    """Expose queue manager operations on the RPC command surface under mpreg.queue.*."""
    if getattr(server, "_queue_rpc_registered", False):
        return
    from mpreg.core.rpc_naming import PlatformRpc

    # Prefer bound server methods (thin wrappers) so registration matches other cmds.
    # allow_platform=True: plane surface is platform-owned (namespace deny root).
    server.register_command(
        PlatformRpc.QUEUE_CREATE,
        server._rpc_queue_create,
        ["queue"],
        allow_platform=True,
    )
    server.register_command(
        PlatformRpc.QUEUE_SEND, server._rpc_queue_send, ["queue"], allow_platform=True
    )
    server.register_command(
        PlatformRpc.QUEUE_ACK, server._rpc_queue_ack, ["queue"], allow_platform=True
    )
    server.register_command(
        PlatformRpc.QUEUE_RECEIVE,
        server._rpc_queue_receive,
        ["queue"],
        allow_platform=True,
    )
    server._queue_rpc_registered = True

def register_cache_rpc_commands(server: Any) -> None:
    """Expose cache manager operations on the RPC command surface under mpreg.cache.*."""
    if getattr(server, "_cache_rpc_registered", False):
        return
    from mpreg.core.rpc_naming import PlatformRpc

    server.register_command(
        PlatformRpc.CACHE_GET, server._rpc_cache_get, ["cache"], allow_platform=True
    )
    server.register_command(
        PlatformRpc.CACHE_PUT, server._rpc_cache_put, ["cache"], allow_platform=True
    )
    server.register_command(
        PlatformRpc.CACHE_INVALIDATE,
        server._rpc_cache_invalidate,
        ["cache"],
        allow_platform=True,
    )
    server.register_command(
        PlatformRpc.CACHE_STRONG_RETRY_ABORT,
        server._rpc_cache_strong_retry_abort,
        ["cache"],
        allow_platform=True,
    )
    server._cache_rpc_registered = True

async def queue_create(
    server: Any, payload: object = None, **kwargs: object
) -> dict[str, Any]:
    from mpreg.core.namespace_policy import actor_context

    body = rpc_payload_dict(payload)
    if kwargs:
        body.update({k: v for k, v in kwargs.items() if v is not None})
    manager = getattr(server, "_queue_manager", None)
    if manager is None:
        return _plane_err("queue_manager_unavailable", code=PLANE_ERR_UNAVAILABLE)
    name = str(body.get("queue_name") or body.get("name") or "")
    if not name:
        return _plane_err("queue_name_required", code=PLANE_ERR_INVALID_ARGUMENT)
    cluster_id, tenant_id = rpc_actor_ids(server, body)
    with actor_context(tenant_id=tenant_id, cluster_id=cluster_id):
        ok = await manager.create_queue(name)
    return {"success": bool(ok), "queue_name": name}

async def queue_send(
    server: Any, payload: object = None, **kwargs: object
) -> dict[str, Any]:
    from mpreg.core.message_queue import DeliveryGuarantee
    from mpreg.core.namespace_policy import actor_context

    body = rpc_payload_dict(payload)
    if kwargs:
        body.update({k: v for k, v in kwargs.items() if v is not None})
    manager = getattr(server, "_queue_manager", None)
    if manager is None:
        return _plane_err("queue_manager_unavailable", code=PLANE_ERR_UNAVAILABLE)
    queue_name = str(body.get("queue_name") or body.get("name") or "")
    if not queue_name:
        return _plane_err("queue_name_required", code=PLANE_ERR_INVALID_ARGUMENT)
    topic = str(body.get("topic") or f"mpreg.queue.{queue_name}")
    payload_data = body.get("payload", body.get("message"))
    dg_raw = str(body.get("delivery_guarantee") or "at_least_once").strip().lower()

    if dg_raw in {"exactly_once", "exact_once", "eo"}:
        return {
            "success": False,
            "error_message": "unsupported_delivery_guarantee:exactly_once",
            "error_code": PLANE_ERR_UNSUPPORTED_DELIVERY,
            "queue_name": queue_name,
            "topic": topic,
        }
    try:
        dg = DeliveryGuarantee(dg_raw)
    except ValueError:
        return _plane_err(
            f"unsupported_delivery_guarantee:{dg_raw}",
            code=PLANE_ERR_UNSUPPORTED_DELIVERY,
            queue_name=queue_name,
            topic=topic,
        )
    cluster_id, tenant_id = rpc_actor_ids(server, body)
    with actor_context(tenant_id=tenant_id, cluster_id=cluster_id):
        result = await manager.send_message(
            queue_name, topic, payload_data, delivery_guarantee=dg
        )
    mid = getattr(result, "message_id", None)
    return {
        "success": bool(getattr(result, "success", False)),
        "message_id": str(mid) if mid is not None else None,
        "error_message": getattr(result, "error_message", None),
        "queue_name": queue_name,
        "topic": topic,
    }

async def queue_ack(
    server: Any, payload: object = None, **kwargs: object
) -> dict[str, Any]:
    from mpreg.core.namespace_policy import actor_context

    body = rpc_payload_dict(payload)
    if kwargs:
        body.update({k: v for k, v in kwargs.items() if v is not None})
    manager = getattr(server, "_queue_manager", None)
    if manager is None:
        return _plane_err("queue_manager_unavailable", code=PLANE_ERR_UNAVAILABLE)
    queue_name = str(body.get("queue_name") or body.get("name") or "")
    message_id = str(body.get("message_id") or body.get("id") or "")
    subscriber_id = str(body.get("subscriber_id") or body.get("subscriber") or "")
    if not queue_name or not message_id or not subscriber_id:
        return _plane_err(
            "queue_name_message_id_subscriber_id_required",
            code=PLANE_ERR_INVALID_ARGUMENT,
        )
    cluster_id, tenant_id = rpc_actor_ids(server, body)
    with actor_context(tenant_id=tenant_id, cluster_id=cluster_id):
        ok = await manager.acknowledge_message(queue_name, message_id, subscriber_id)
    return {
        "success": bool(ok),
        "queue_name": queue_name,
        "message_id": message_id,
        "subscriber_id": subscriber_id,
    }

async def queue_receive(
    server: Any, payload: object = None, **kwargs: object
) -> dict[str, Any]:
    """Poll one message from a queue (short-lived subscription)."""
    from mpreg.core.namespace_policy import actor_context

    body = rpc_payload_dict(payload)
    if kwargs:
        body.update({k: v for k, v in kwargs.items() if v is not None})
    manager = getattr(server, "_queue_manager", None)
    if manager is None:
        return _plane_err("queue_manager_unavailable", code=PLANE_ERR_UNAVAILABLE)
    queue_name = str(body.get("queue_name") or body.get("name") or "")
    if not queue_name:
        return _plane_err("queue_name_required", code=PLANE_ERR_INVALID_ARGUMENT)
    subscriber_id = (
        str(body.get("subscriber_id") or body.get("subscriber") or "").strip() or None
    )
    topic_pattern = str(body.get("topic_pattern") or body.get("topic") or "#")
    try:
        timeout_seconds = float(
            body.get("timeout_seconds") or body.get("timeout") or 5.0
        )
    except TypeError, ValueError:
        timeout_seconds = 5.0
    timeout_seconds = max(0.05, min(timeout_seconds, 60.0))
    auto_ack = bool(body.get("auto_acknowledge", False))
    cluster_id, tenant_id = rpc_actor_ids(server, body)
    with actor_context(tenant_id=tenant_id, cluster_id=cluster_id):
        message = await manager.receive_message(
            queue_name,
            subscriber_id=subscriber_id,
            topic_pattern=topic_pattern,
            timeout_seconds=timeout_seconds,
            auto_acknowledge=auto_ack,
        )
    if message is None:
        return {
            "success": True,
            "empty": True,
            "message": None,
            "queue_name": queue_name,
        }
    mid = getattr(message, "id", None) or getattr(message, "message_id", None)
    payload_data = getattr(message, "payload", None)
    topic = getattr(message, "topic", None)
    return {
        "success": True,
        "empty": False,
        "message": {
            "message_id": str(mid) if mid is not None else None,
            "payload": payload_data,
            "topic": str(topic) if topic is not None else None,
        },
        "queue_name": queue_name,
        "subscriber_id": subscriber_id,
    }

async def cache_get(
    server: Any, payload: object = None, **kwargs: object
) -> dict[str, Any]:
    from mpreg.core.cache_models import GlobalCacheKey
    from mpreg.core.namespace_policy import actor_context

    body = rpc_payload_dict(payload)
    if kwargs:
        body.update({k: v for k, v in kwargs.items() if v is not None})
    manager = getattr(server, "_cache_manager", None)
    if manager is None:
        return _plane_err("cache_manager_unavailable", code=PLANE_ERR_UNAVAILABLE)
    namespace = str(body.get("namespace") or "")
    identifier = str(body.get("identifier") or body.get("key") or "")
    if not namespace or not identifier:
        return {
            "success": False,
            "error_message": "namespace_and_identifier_required",
        }
    key = GlobalCacheKey(
        namespace=namespace,
        identifier=identifier,
        version=str(body.get("version") or "v1.0.0"),
    )
    cluster_id, tenant_id = rpc_actor_ids(server, body)
    with actor_context(tenant_id=tenant_id, cluster_id=cluster_id):
        result = await manager.get(key)
    entry = getattr(result, "entry", None)
    value = getattr(entry, "value", None) if entry is not None else None
    return {
        "success": bool(getattr(result, "success", False)),
        "value": value,
        "error_message": getattr(result, "error_message", None),
        "namespace": namespace,
        "identifier": identifier,
    }

async def cache_put(
    server: Any, payload: object = None, **kwargs: object
) -> dict[str, Any]:
    from mpreg.core.cache_models import GlobalCacheKey
    from mpreg.core.namespace_policy import actor_context

    body = rpc_payload_dict(payload)
    if kwargs:
        body.update({k: v for k, v in kwargs.items() if v is not None})
    manager = getattr(server, "_cache_manager", None)
    if manager is None:
        return _plane_err("cache_manager_unavailable", code=PLANE_ERR_UNAVAILABLE)
    namespace = str(body.get("namespace") or "")
    identifier = str(body.get("identifier") or body.get("key") or "")
    if not namespace or not identifier:
        return {
            "success": False,
            "error_message": "namespace_and_identifier_required",
        }
    if "value" not in body:
        return _plane_err("value_required", code=PLANE_ERR_INVALID_ARGUMENT)
    key = GlobalCacheKey(
        namespace=namespace,
        identifier=identifier,
        version=str(body.get("version") or "v1.0.0"),
    )
    # ERG-T11-06: honor consistency_level; refuse STRONG with error_code 1012
    from mpreg.core.cache_models import CacheOptions, ConsistencyLevel

    raw_cl = body.get("consistency_level") or body.get("consistency")
    opts = CacheOptions()
    if raw_cl is not None:
        try:
            if isinstance(raw_cl, ConsistencyLevel):
                opts = CacheOptions(consistency_level=raw_cl)
            else:
                opts = CacheOptions(
                    consistency_level=ConsistencyLevel(str(raw_cl).lower())
                )
        except Exception:
            return {
                "success": False,
                "error_message": f"invalid_consistency_level:{raw_cl}",
                "error_code": PLANE_ERR_UNSUPPORTED_CONSISTENCY,
                "namespace": namespace,
                "identifier": identifier,
            }
    cluster_id, tenant_id = rpc_actor_ids(server, body)
    with actor_context(tenant_id=tenant_id, cluster_id=cluster_id):
        result = await manager.put(key, body.get("value"), options=opts)
    success = bool(getattr(result, "success", False))
    err = getattr(result, "error_message", None)
    out: dict[str, Any] = {
        "success": success,
        "error_message": err,
        "namespace": namespace,
        "identifier": identifier,
    }
    code = getattr(result, "error_code", None)
    if code is not None:
        out["error_code"] = int(code)
    elif (
        not success
        and err
        and (
            "STRONG" in str(err)
            or "strong" in str(err).lower()
            or "not implemented" in str(err).lower()
            or "disabled" in str(err).lower()
        )
    ):
        out["error_code"] = PLANE_ERR_UNSUPPORTED_CONSISTENCY
    qi = getattr(result, "quorum_info", None)
    if qi is not None:
        out["quorum_info"] = qi
    oid = getattr(result, "operation_id", None)
    if oid is not None:
        out["operation_id"] = str(oid)
    return out

async def cache_invalidate(
    server: Any, payload: object = None, **kwargs: object
) -> dict[str, Any]:
    from mpreg.core.namespace_policy import actor_context

    body = rpc_payload_dict(payload)
    if kwargs:
        body.update({k: v for k, v in kwargs.items() if v is not None})
    manager = getattr(server, "_cache_manager", None)
    if manager is None:
        return _plane_err("cache_manager_unavailable", code=PLANE_ERR_UNAVAILABLE)
    pattern = str(body.get("pattern") or body.get("namespace") or "")
    if not pattern:
        return _plane_err("pattern_required", code=PLANE_ERR_INVALID_ARGUMENT)
    cluster_id, tenant_id = rpc_actor_ids(server, body)
    with actor_context(tenant_id=tenant_id, cluster_id=cluster_id):
        result = await manager.invalidate(pattern)
    return {
        "success": bool(getattr(result, "success", False)),
        "error_message": getattr(result, "error_message", None),
        "pattern": pattern,
    }

async def cache_strong_retry_abort(
    server: Any, payload: object = None, **kwargs: object
) -> dict[str, Any]:
    """Ops-driven CFT re-ABORT for residual candidates (not automatic heal).

    Body: namespace, identifier, op_id, optional version, optional peers list.
    Wraps ``GlobalCacheManager.strong_retry_abort``. Still CFT best-effort —
    not residual-free while ABORT is lost, not BFT, not background heal.
    """
    from mpreg.core.namespace_policy import actor_context

    body = rpc_payload_dict(payload)
    if kwargs:
        body.update({k: v for k, v in kwargs.items() if v is not None})
    manager = getattr(server, "_cache_manager", None)
    if manager is None:
        return _plane_err("cache_manager_unavailable", code=PLANE_ERR_UNAVAILABLE)
    if not hasattr(manager, "strong_retry_abort"):
        return _plane_err(
            "strong_retry_abort_unavailable",
            code=PLANE_ERR_UNSUPPORTED_CONSISTENCY,
        )
    namespace = str(body.get("namespace") or "")
    identifier = str(body.get("identifier") or body.get("key") or "")
    op_id = str(body.get("op_id") or body.get("operation_id") or "")
    if not namespace or not identifier:
        return _plane_err(
            "namespace_and_identifier_required", code=PLANE_ERR_INVALID_ARGUMENT
        )
    if not op_id:
        return _plane_err("op_id_required", code=PLANE_ERR_INVALID_ARGUMENT)
    from mpreg.core.cache_models import GlobalCacheKey

    key = GlobalCacheKey(
        namespace=namespace,
        identifier=identifier,
        version=str(body.get("version") or "v1.0.0"),
    )
    peers_raw = body.get("peers")
    peers: list[str] | None
    if peers_raw is None:
        peers = None
    elif isinstance(peers_raw, (list, tuple)):
        peers = [str(p) for p in peers_raw if p]
    else:
        return _plane_err("peers_must_be_list", code=PLANE_ERR_INVALID_ARGUMENT)

    cluster_id, tenant_id = rpc_actor_ids(server, body)
    with actor_context(tenant_id=tenant_id, cluster_id=cluster_id):
        out = await manager.strong_retry_abort(key, op_id, peers=peers)
    if not isinstance(out, dict):
        out = {"raw": out}
    # Normalize success for client façades: cleared or empty fail_peers
    cleared = bool(out.get("cleared"))
    err = out.get("error")
    result: dict[str, Any] = {
        "success": cleared and not err,
        "cleared": cleared,
        "ok_peers": list(out.get("ok_peers") or []),
        "fail_peers": list(out.get("fail_peers") or []),
        "op_id": str(out.get("op_id") or op_id),
        "attempts": int(out.get("attempts") or 0),
        "namespace": namespace,
        "identifier": identifier,
        # Honesty: ops-driven CFT — not automatic residual heal
        "ops_driven": True,
        "cft_best_effort": True,
        "automatic_heal": False,
    }
    if err:
        result["error_message"] = str(err)
        code = out.get("error_code")
        if code is not None:
            result["error_code"] = int(code)
        else:
            result["error_code"] = PLANE_ERR_UNSUPPORTED_CONSISTENCY
    elif not cleared and list(out.get("fail_peers") or []):
        result["success"] = False
        result["error_message"] = (
            "retry_abort_still_fail: CFT residual candidates remain "
            f"(fail_peers={list(out.get('fail_peers') or [])})"
        )
    return result
