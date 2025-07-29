#!/usr/bin/env python3

from mpreg.core.federated_delivery_guarantees import ClusterWeight, GlobalConsensusRound


def test_quorum_debug():
    """Debug the quorum calculation."""
    consensus_round = GlobalConsensusRound(
        cluster_weights={
            "cluster-1": ClusterWeight(cluster_id="cluster-1", weight=1.0),
            "cluster-2": ClusterWeight(cluster_id="cluster-2", weight=1.0),
            "cluster-3": ClusterWeight(cluster_id="cluster-3", weight=1.0),
        },
        required_weight_threshold=0.67,  # Need 67% of weight
    )

    print(f"Total clusters: {len(consensus_round.cluster_weights)}")
    print(f"Total weight: {consensus_round.total_weight()}")
    print(f"Required threshold: {consensus_round.required_weight_threshold}")
    print(
        f"Required weight: {consensus_round.total_weight() * consensus_round.required_weight_threshold}"
    )

    # Print effective weights
    for cluster_id, weight in consensus_round.cluster_weights.items():
        print(
            f"Cluster {cluster_id}: weight={weight.weight}, reliability={weight.reliability_score}, trusted={weight.is_trusted}, effective={weight.effective_weight()}"
        )

    print("\nNo votes yet:")
    print(f"Current weight: {consensus_round.current_weight()}")
    print(f"Has quorum: {consensus_round.has_quorum()}")

    # One vote (33% weight)
    consensus_round.votes_received["cluster-1"] = True
    print("\nOne vote (cluster-1):")
    print(f"Votes received: {consensus_round.votes_received}")
    print(f"Current weight: {consensus_round.current_weight()}")
    print(f"Has quorum: {consensus_round.has_quorum()}")

    # Two votes (67% weight)
    consensus_round.votes_received["cluster-2"] = True
    current = consensus_round.current_weight()
    required = (
        consensus_round.total_weight() * consensus_round.required_weight_threshold
    )
    diff = required - current
    print("\nTwo votes (cluster-1, cluster-2):")
    print(f"Votes received: {consensus_round.votes_received}")
    print(f"Current weight: {current}")
    print(f"Required weight: {required}")
    print(f"Difference: {diff}")
    current_pct = current / consensus_round.total_weight()
    required_pct = consensus_round.required_weight_threshold
    pct_diff = required_pct - current_pct
    print(f"Current percentage: {current_pct}")
    print(f"Required percentage: {required_pct}")
    print(f"Percentage difference: {pct_diff}")
    print(f"Has quorum: {consensus_round.has_quorum()}")


if __name__ == "__main__":
    test_quorum_debug()
