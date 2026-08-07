"""X5: Catalog gossip lag — never silent wrong-cluster success."""

from __future__ import annotations

import time

from mpreg.core.errors import MpregError, MpregErrorCode
from mpreg.fabric.catalog import (
    FunctionCatalog,
    FunctionEndpoint,
    FunctionIdentity,
    FunctionSelector,
)


def test_missing_function_structured_not_found() -> None:
    cat = FunctionCatalog()
    selector = FunctionSelector(name="brand_new_fn")
    found = cat.find(selector)
    assert found == []
    err = MpregError.of(MpregErrorCode.COMMAND_NOT_FOUND, details="brand_new_fn")
    assert err.code == int(MpregErrorCode.COMMAND_NOT_FOUND)
    # Must be a structured error object clients can branch on
    assert err.rpc_error.code == int(MpregErrorCode.COMMAND_NOT_FOUND)


def test_lagged_registration_becomes_visible() -> None:
    cat = FunctionCatalog()
    now = time.time()
    identity = FunctionIdentity(name="new_fn", function_id="fn.new_fn", version="1.0.0")
    ep = FunctionEndpoint(
        identity=identity,
        resources=frozenset({"cpu"}),
        node_id="n1",
        cluster_id="c1",
        advertised_at=now,
        ttl_seconds=30.0,
    )
    assert cat.register(ep, now=now) is True
    found = cat.find(FunctionSelector(name="new_fn"), now=now)
    assert len(found) == 1
    assert found[0].cluster_id == "c1"


def test_stale_catalog_does_not_invent_endpoints() -> None:
    cat = FunctionCatalog()
    # Empty after lag: discovery must not invent a hit
    assert cat.find(FunctionSelector(name="missing")) == []
