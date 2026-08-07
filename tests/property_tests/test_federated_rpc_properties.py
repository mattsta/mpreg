"""
Property-based tests for MPREG's fabric function catalog and routing invariants.

This module uses Hypothesis to verify critical invariants and properties
of function identity matching, catalog selection, and expiration behavior.
"""

from __future__ import annotations

import string

import hypothesis.strategies as st
from hypothesis import given, settings

from mpreg.datastructures.function_identity import (
    FunctionIdentity,
    FunctionSelector,
    SemanticVersion,
    VersionConstraint,
)
from mpreg.fabric.catalog import FunctionCatalog, FunctionEndpoint

ALPHABET = string.ascii_lowercase + string.digits + "_-"


@st.composite
def semantic_versions(draw: st.DrawFn) -> SemanticVersion:
    return SemanticVersion(
        draw(st.integers(min_value=0, max_value=5)),
        draw(st.integers(min_value=0, max_value=5)),
        draw(st.integers(min_value=0, max_value=10)),
    )


@st.composite
def function_identities(draw: st.DrawFn) -> FunctionIdentity:
    name = draw(st.text(min_size=1, max_size=20, alphabet=ALPHABET))
    function_id = draw(st.text(min_size=1, max_size=20, alphabet=ALPHABET))
    version = draw(semantic_versions())
    return FunctionIdentity(name=name, function_id=function_id, version=version)


@st.composite
def function_resources(draw: st.DrawFn) -> frozenset[str]:
    items = draw(
        st.lists(
            st.text(min_size=1, max_size=12, alphabet=ALPHABET),
            min_size=0,
            max_size=6,
            unique=True,
        )
    )
    return frozenset(items)


@st.composite
def function_endpoint(draw: st.DrawFn) -> FunctionEndpoint:
    identity = draw(function_identities())
    resources = draw(function_resources())
    node_id = draw(st.text(min_size=1, max_size=16, alphabet=ALPHABET))
    cluster_id = draw(st.text(min_size=1, max_size=16, alphabet=ALPHABET))
    advertised_at = draw(
        st.floats(
            min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False
        )
    )
    ttl_seconds = draw(
        st.floats(min_value=0.1, max_value=300.0, allow_nan=False, allow_infinity=False)
    )
    return FunctionEndpoint(
        identity=identity,
        resources=resources,
        node_id=node_id,
        cluster_id=cluster_id,
        advertised_at=advertised_at,
        ttl_seconds=ttl_seconds,
    )


@st.composite
def ordered_versions(
    draw: st.DrawFn,
) -> tuple[SemanticVersion, SemanticVersion]:
    v1 = draw(semantic_versions())
    v2 = draw(semantic_versions())
    return (v1, v2) if v1 <= v2 else (v2, v1)


class TestFunctionCatalogProperties:
    """Property-based tests for catalog identity and selection behavior."""

    @given(
        bounds=ordered_versions(),
        include_min=st.booleans(),
        include_max=st.booleans(),
        candidate=semantic_versions(),
    )
    @settings(max_examples=50, deadline=5000)
    def test_version_constraint_range_matches(
        self,
        bounds: tuple[SemanticVersion, SemanticVersion],
        include_min: bool,
        include_max: bool,
        candidate: SemanticVersion,
    ) -> None:
        min_version, max_version = bounds
        constraint = VersionConstraint(
            min_version=min_version,
            max_version=max_version,
            include_min=include_min,
            include_max=include_max,
        )
        expected = True
        if candidate < min_version:
            expected = False
        if candidate > max_version:
            expected = False
        if candidate == min_version and not include_min:
            expected = False
        if candidate == max_version and not include_max:
            expected = False
        assert constraint.matches(candidate) == expected

    @given(identity=function_identities())
    @settings(max_examples=50, deadline=4000)
    def test_function_selector_exact_match(self, identity: FunctionIdentity) -> None:
        selector = FunctionSelector(
            name=identity.name,
            function_id=identity.function_id,
            version_constraint=VersionConstraint.parse(f"=={identity.version}"),
        )
        assert selector.matches(identity)
        different = SemanticVersion(
            identity.version.major,
            identity.version.minor,
            identity.version.patch + 1,
        )
        assert not selector.matches(
            FunctionIdentity(
                name=identity.name,
                function_id=identity.function_id,
                version=different,
            )
        )

    @given(endpoint=function_endpoint())
    @settings(max_examples=40, deadline=4000)
    def test_function_endpoint_round_trip(self, endpoint: FunctionEndpoint) -> None:
        payload = endpoint.to_dict()
        loaded = FunctionEndpoint.from_dict(payload)
        assert loaded == endpoint

    @given(endpoint=function_endpoint(), now=st.floats(min_value=0.0, max_value=1200.0))
    @settings(max_examples=40, deadline=4000)
    def test_catalog_expiration_behavior(
        self, endpoint: FunctionEndpoint, now: float
    ) -> None:
        catalog = FunctionCatalog()
        expired = now > (endpoint.advertised_at + endpoint.ttl_seconds)
        registered = catalog.register(endpoint, now=now)
        assert registered is (not expired)
        selector = FunctionSelector(
            name=endpoint.identity.name,
            function_id=endpoint.identity.function_id,
            version_constraint=VersionConstraint.parse(
                f"=={endpoint.identity.version}"
            ),
        )
        matches = catalog.find(selector, resources=endpoint.resources, now=now)
        assert (len(matches) == 1) is (not expired)

    @given(endpoint=function_endpoint())
    @settings(max_examples=40, deadline=4000)
    def test_catalog_deduplicates_keys(self, endpoint: FunctionEndpoint) -> None:
        catalog = FunctionCatalog()
        now = endpoint.advertised_at
        if catalog.register(endpoint, now=now):
            catalog.register(endpoint, now=now)
            assert catalog.entry_count() == 1
        else:
            assert catalog.entry_count() == 0
