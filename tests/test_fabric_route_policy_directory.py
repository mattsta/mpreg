from mpreg.fabric.route_control import RoutePolicy
from mpreg.fabric.route_policy_directory import (
    RouteNeighborPolicy,
    RoutePolicyDirectory,
)


def test_route_policy_directory_default_and_override() -> None:
    default = RoutePolicy(max_hops=3)
    override = RoutePolicy(allowed_tags={"gold"})

    directory = RoutePolicyDirectory(default_policy=default)
    directory.register(RouteNeighborPolicy(cluster_id="cluster-b", policy=override))

    assert directory.policy_for("cluster-b") is override
    assert directory.policy_for("cluster-c") is default

    resolver = directory.resolver(use_default=True)
    assert resolver("cluster-c") is default

    resolver_no_default = directory.resolver(use_default=False)
    assert resolver_no_default("cluster-c") is None
