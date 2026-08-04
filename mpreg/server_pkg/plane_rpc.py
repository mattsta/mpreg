"""Queue and cache RPC command handlers peeled from MPREGServer.

Keeps the composition root thin: registration + attach stay on the server;
request handling lives here so policy, delivery honesty, and actor binding
are testable without the full server module.
"""

from __future__ import annotations

from typing import Any

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
    """Expose queue manager operations on the RPC command surface."""
    if getattr(server, "_queue_rpc_registered", False):
        return
    # Prefer bound server methods (thin wrappers) so registration matches other cmds.
    server.register_command("queue_create", server._rpc_queue_create, ["queue"])
    server.register_command("queue_send", server._rpc_queue_send, ["queue"])
    server.register_command("queue_ack", server._rpc_queue_ack, ["queue"])
    server.register_command("queue_receive", server._rpc_queue_receive, ["queue"])
    server._queue_rpc_registered = True

def register_cache_rpc_commands(server: Any) -> None:
    """Expose cache manager operations on the RPC command surface."""
    if getattr(server, "_cache_rpc_registered", False):
        return
    server.register_command("cache_get", server._rpc_cache_get, ["cache"])
    server.register_command("cache_put", server._rpc_cache_put, ["cache"])
    server.register_command("cache_invalidate", server._rpc_cache_invalidate, ["cache"])
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
        return {"success": False, "error_message": "queue_manager_unavailable"}
    name = str(body.get("queue_name") or body.get("name") or "")
    if not name:
        return {"success": False, "error_message": "queue_name_required"}
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
        return {"success": False, "error_message": "queue_manager_unavailable"}
    queue_name = str(body.get("queue_name") or body.get("name") or "")
    if not queue_name:
        return {"success": False, "error_message": "queue_name_required"}
    topic = str(body.get("topic") or f"mpreg.queue.{queue_name}")
    payload_data = body.get("payload", body.get("message"))
    dg_raw = str(body.get("delivery_guarantee") or "at_least_once").strip().lower()

    if dg_raw in {"exactly_once", "exact_once", "eo"}:
        return {
            "success": False,
            "error_message": "unsupported_delivery_guarantee:exactly_once",
            "queue_name": queue_name,
            "topic": topic,
        }
    try:
        dg = DeliveryGuarantee(dg_raw)
    except ValueError:
        return {
            "success": False,
            "error_message": f"unsupported_delivery_guarantee:{dg_raw}",
            "queue_name": queue_name,
            "topic": topic,
        }
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
        return {"success": False, "error_message": "queue_manager_unavailable"}
    queue_name = str(body.get("queue_name") or body.get("name") or "")
    message_id = str(body.get("message_id") or body.get("id") or "")
    subscriber_id = str(body.get("subscriber_id") or body.get("subscriber") or "")
    if not queue_name or not message_id or not subscriber_id:
        return {
            "success": False,
            "error_message": "queue_name_message_id_subscriber_id_required",
        }
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
        return {"success": False, "error_message": "queue_manager_unavailable"}
    queue_name = str(body.get("queue_name") or body.get("name") or "")
    if not queue_name:
        return {"success": False, "error_message": "queue_name_required"}
    subscriber_id = str(
        body.get("subscriber_id") or body.get("subscriber") or ""
    ).strip() or None
    topic_pattern = str(body.get("topic_pattern") or body.get("topic") or "#")
    try:
        timeout_seconds = float(body.get("timeout_seconds") or body.get("timeout") or 5.0)
    except (TypeError, ValueError):
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
        return {"success": False, "error_message": "cache_manager_unavailable"}
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
        return {"success": False, "error_message": "cache_manager_unavailable"}
    namespace = str(body.get("namespace") or "")
    identifier = str(body.get("identifier") or body.get("key") or "")
    if not namespace or not identifier:
        return {
            "success": False,
            "error_message": "namespace_and_identifier_required",
        }
    if "value" not in body:
        return {"success": False, "error_message": "value_required"}
    key = GlobalCacheKey(
        namespace=namespace,
        identifier=identifier,
        version=str(body.get("version") or "v1.0.0"),
    )
    cluster_id, tenant_id = rpc_actor_ids(server, body)
    with actor_context(tenant_id=tenant_id, cluster_id=cluster_id):
        result = await manager.put(key, body.get("value"))
    return {
        "success": bool(getattr(result, "success", False)),
        "error_message": getattr(result, "error_message", None),
        "namespace": namespace,
        "identifier": identifier,
    }

async def cache_invalidate(
    server: Any, payload: object = None, **kwargs: object
) -> dict[str, Any]:
    from mpreg.core.namespace_policy import actor_context

    body = rpc_payload_dict(payload)
    if kwargs:
        body.update({k: v for k, v in kwargs.items() if v is not None})
    manager = getattr(server, "_cache_manager", None)
    if manager is None:
        return {"success": False, "error_message": "cache_manager_unavailable"}
    pattern = str(body.get("pattern") or body.get("namespace") or "")
    if not pattern:
        return {"success": False, "error_message": "pattern_required"}
    cluster_id, tenant_id = rpc_actor_ids(server, body)
    with actor_context(tenant_id=tenant_id, cluster_id=cluster_id):
        result = await manager.invalidate(pattern)
    return {
        "success": bool(getattr(result, "success", False)),
        "error_message": getattr(result, "error_message", None),
        "pattern": pattern,
    }
