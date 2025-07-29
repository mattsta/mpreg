# DAO Integration Examples - Real-World Scenarios

## Quick Reference

### Basic DAO Creation

```python
from mpreg.datastructures import (
    DecentralizedAutonomousOrganization, DaoConfig, DaoType,
    MembershipType, DaoMember, DaoProposal, ProposalType, VoteType
)

# Create DAO
dao = DecentralizedAutonomousOrganization(
    name="My DAO",
    description="Community governance",
    dao_type=DaoType.COMMUNITY_GOVERNANCE
)

# Add member
member = DaoMember(
    member_id="alice",
    voting_power=1000,
    token_balance=10000
)
dao = dao.add_member(member)

# Create proposal
proposal = DaoProposal(
    proposer_id="alice",
    title="Increase rewards",
    description="Proposal to increase staking rewards by 5%"
)
dao = dao.create_proposal("alice", proposal)

# Vote
proposal_id = list(dao.proposals.keys())[0]
dao = dao.cast_vote("alice", proposal_id, VoteType.FOR)
```

## Federation Integration Scenarios

### 1. Global Federation Governance

```python
class GlobalFederationGovernance:
    """Manages global federation decisions through hierarchical DAOs."""

    def __init__(self):
        self.global_dao = self._create_global_dao()
        self.regional_daos = {}
        self.local_daos = {}

    def _create_global_dao(self):
        """Create top-level global governance DAO."""
        config = DaoConfig(
            dao_type=DaoType.FEDERATION_GOVERNANCE,
            membership_type=MembershipType.DELEGATION_BASED,
            voting_period_seconds=21 * 86400,  # 3 weeks for global decisions
            quorum_threshold=0.6,              # 60% quorum
            approval_threshold=0.75,           # 75% supermajority
            proposal_deposit=10000             # High stakes for global proposals
        )

        return DecentralizedAutonomousOrganization(
            name="MPREG Global Federation",
            description="Highest level governance for global protocol decisions",
            dao_type=DaoType.FEDERATION_GOVERNANCE,
            config=config,
            treasury_balance=100000000  # 100M global treasury
        )

    def add_regional_representative(self, region: str, representative_id: str,
                                  voting_power: int):
        """Add regional representative to global DAO."""
        rep = DaoMember(
            member_id=representative_id,
            voting_power=voting_power,
            token_balance=voting_power * 2,
            metadata={
                "role": "regional_representative",
                "region": region,
                "represents": f"{region}_federation"
            }
        )
        self.global_dao = self.global_dao.add_member(rep)

    def create_protocol_upgrade_proposal(self, proposer: str, version: str,
                                       changes: dict) -> str:
        """Create global protocol upgrade proposal."""
        proposal = DaoProposal(
            proposer_id=proposer,
            proposal_type=ProposalType.PROTOCOL_UPGRADE,
            title=f"Global Protocol Upgrade to {version}",
            description=f"Upgrade MPREG protocol with major changes: {changes}",
            execution_data=json.dumps({
                "version": version,
                "changes": changes,
                "rollout_strategy": "phased",
                "testing_period": 30,  # days
                "compatibility_mode": True
            }).encode()
        )

        self.global_dao = self.global_dao.create_proposal(proposer, proposal)
        return list(self.global_dao.proposals.keys())[-1]

    def coordinate_cross_regional_vote(self, proposal_id: str):
        """Coordinate voting across all regional representatives."""
        # This would integrate with regional DAOs to collect consensus
        regional_consensus = {}

        for region, dao in self.regional_daos.items():
            # Regional DAO votes internally first
            regional_result = self._get_regional_consensus(dao, proposal_id)
            regional_consensus[region] = regional_result

            # Regional representative votes based on regional consensus
            rep_id = f"{region}_representative"
            if regional_result["passed"]:
                self.global_dao = self.global_dao.cast_vote(
                    rep_id, proposal_id, VoteType.FOR,
                    f"Regional consensus: {regional_result['approval_rate']:.1%} approval"
                )
            else:
                self.global_dao = self.global_dao.cast_vote(
                    rep_id, proposal_id, VoteType.AGAINST,
                    f"Regional consensus: insufficient approval"
                )

        return regional_consensus

# Usage example
federation = GlobalFederationGovernance()

# Add regional representatives
regions = [
    ("north_america", "na_rep", 25000),
    ("europe", "eu_rep", 20000),
    ("asia_pacific", "ap_rep", 30000),
    ("latin_america", "la_rep", 15000),
    ("africa", "af_rep", 10000)
]

for region, rep_id, power in regions:
    federation.add_regional_representative(region, rep_id, power)

# Create major protocol upgrade
upgrade_proposal_id = federation.create_protocol_upgrade_proposal(
    proposer="na_rep",
    version="3.0.0",
    changes={
        "consensus_algorithm": "proof_of_stake_2.0",
        "transaction_throughput": "10x_increase",
        "smart_contracts": "full_support",
        "cross_chain_bridges": "native_support"
    }
)

print(f"Global upgrade proposal created: {upgrade_proposal_id}")
```

### 2. Regional Hub Management

```python
class RegionalHubManager:
    """Manages regional federation hubs through specialized DAOs."""

    def __init__(self, region: str):
        self.region = region
        self.hub_dao = self._create_hub_dao()
        self.infrastructure_dao = self._create_infrastructure_dao()
        self.economic_dao = self._create_economic_dao()

    def _create_hub_dao(self):
        """Create main hub governance DAO."""
        config = DaoConfig(
            dao_type=DaoType.FEDERATION_GOVERNANCE,
            membership_type=MembershipType.STAKE_BASED,
            voting_period_seconds=7 * 86400,   # 1 week
            quorum_threshold=0.4,              # 40% quorum
            approval_threshold=0.6,            # 60% approval
            proposal_deposit=2000
        )

        return DecentralizedAutonomousOrganization(
            name=f"{self.region} Hub Governance",
            description=f"Main governance for {self.region} federation hub",
            dao_type=DaoType.FEDERATION_GOVERNANCE,
            config=config,
            treasury_balance=10000000  # 10M regional treasury
        )

    def _create_infrastructure_dao(self):
        """Create infrastructure-specific DAO."""
        config = DaoConfig(
            dao_type=DaoType.RESOURCE_ALLOCATION,
            membership_type=MembershipType.STAKE_BASED,
            voting_period_seconds=5 * 86400,   # 5 days
            quorum_threshold=0.3,              # 30% quorum
            approval_threshold=0.5,            # Simple majority
            proposal_deposit=1000
        )

        return DecentralizedAutonomousOrganization(
            name=f"{self.region} Infrastructure DAO",
            description="Infrastructure and resource allocation decisions",
            dao_type=DaoType.RESOURCE_ALLOCATION,
            config=config,
            treasury_balance=5000000  # 5M infrastructure fund
        )

    def _create_economic_dao(self):
        """Create economic policy DAO."""
        config = DaoConfig(
            dao_type=DaoType.PROJECT_FUNDING,
            membership_type=MembershipType.TOKEN_HOLDER,
            voting_period_seconds=10 * 86400,  # 10 days
            quorum_threshold=0.25,             # 25% quorum
            approval_threshold=0.55,           # 55% approval
            proposal_deposit=500
        )

        return DecentralizedAutonomousOrganization(
            name=f"{self.region} Economic DAO",
            description="Economic policy and funding decisions",
            dao_type=DaoType.PROJECT_FUNDING,
            config=config,
            treasury_balance=20000000  # 20M economic fund
        )

    def add_hub_operator(self, operator_id: str, stake: int,
                        infrastructure_stake: int, economic_stake: int):
        """Add hub operator to all relevant DAOs."""
        # Main hub governance
        hub_member = DaoMember(
            member_id=operator_id,
            voting_power=stake,
            token_balance=stake * 3,
            metadata={"role": "hub_operator", "region": self.region}
        )
        self.hub_dao = self.hub_dao.add_member(hub_member)

        # Infrastructure governance
        infra_member = DaoMember(
            member_id=operator_id,
            voting_power=infrastructure_stake,
            token_balance=infrastructure_stake * 2,
            metadata={"role": "infrastructure_operator"}
        )
        self.infrastructure_dao = self.infrastructure_dao.add_member(infra_member)

        # Economic governance
        econ_member = DaoMember(
            member_id=operator_id,
            voting_power=economic_stake,
            token_balance=economic_stake * 2,
            metadata={"role": "economic_participant"}
        )
        self.economic_dao = self.economic_dao.add_member(econ_member)

    def propose_infrastructure_upgrade(self, proposer: str, upgrade_spec: dict) -> str:
        """Propose infrastructure upgrade."""
        proposal = DaoProposal(
            proposer_id=proposer,
            proposal_type=ProposalType.BUDGET_ALLOCATION,
            title=f"Infrastructure Upgrade: {upgrade_spec['name']}",
            description=f"Upgrade regional infrastructure: {upgrade_spec['description']}",
            execution_data=json.dumps(upgrade_spec).encode()
        )

        self.infrastructure_dao = self.infrastructure_dao.create_proposal(proposer, proposal)
        return list(self.infrastructure_dao.proposals.keys())[-1]

    def propose_economic_policy(self, proposer: str, policy: dict) -> str:
        """Propose economic policy change."""
        proposal = DaoProposal(
            proposer_id=proposer,
            proposal_type=ProposalType.PARAMETER_CHANGE,
            title=f"Economic Policy: {policy['name']}",
            description=f"Update economic parameters: {policy['description']}",
            execution_data=json.dumps(policy).encode()
        )

        self.economic_dao = self.economic_dao.create_proposal(proposer, proposal)
        return list(self.economic_dao.proposals.keys())[-1]

# Usage example
asia_hub = RegionalHubManager("asia_pacific")

# Add major hub operators
operators = [
    ("tokyo_datacenter", 15000, 12000, 18000),
    ("singapore_hub", 18000, 15000, 20000),
    ("mumbai_node", 12000, 10000, 15000),
    ("sydney_gateway", 10000, 8000, 12000)
]

for op_id, stake, infra_stake, econ_stake in operators:
    asia_hub.add_hub_operator(op_id, stake, infra_stake, econ_stake)

# Propose infrastructure upgrade
upgrade_id = asia_hub.propose_infrastructure_upgrade(
    proposer="tokyo_datacenter",
    upgrade_spec={
        "name": "5G Edge Computing Rollout",
        "description": "Deploy edge computing nodes across Asia-Pacific",
        "budget": 2000000,
        "timeline": "12_months",
        "coverage": ["tokyo", "singapore", "mumbai", "sydney"],
        "expected_latency_improvement": "60%"
    }
)

print(f"Infrastructure upgrade proposed: {upgrade_id}")
```

### 3. Cross-Chain DAO Coordination

```python
class CrossChainDAOCoordinator:
    """Coordinates DAO decisions across multiple blockchain networks."""

    def __init__(self):
        self.chain_daos = {}
        self.bridge_connections = {}
        self.pending_cross_chain_proposals = {}

    def register_chain_dao(self, chain_id: str, dao: DecentralizedAutonomousOrganization):
        """Register DAO on specific blockchain."""
        self.chain_daos[chain_id] = dao

    def register_bridge(self, from_chain: str, to_chain: str, bridge_contract: str):
        """Register cross-chain bridge."""
        bridge_key = f"{from_chain}_{to_chain}"
        self.bridge_connections[bridge_key] = {
            "from_chain": from_chain,
            "to_chain": to_chain,
            "bridge_contract": bridge_contract,
            "active": True
        }

    def create_cross_chain_proposal(self, origin_chain: str, target_chains: list,
                                  proposal_template: DaoProposal) -> dict:
        """Create synchronized proposal across multiple chains."""
        cross_chain_id = f"cross_chain_{uuid.uuid4()}"

        # Create proposal on origin chain
        origin_dao = self.chain_daos[origin_chain]
        origin_proposal = DaoProposal(
            proposer_id=proposal_template.proposer_id,
            proposal_type=ProposalType.CONTRACT_EXECUTION,
            title=f"[CROSS-CHAIN] {proposal_template.title}",
            description=f"Multi-chain proposal: {proposal_template.description}",
            execution_data=json.dumps({
                "type": "cross_chain_coordination",
                "cross_chain_id": cross_chain_id,
                "target_chains": target_chains,
                "original_proposal": {
                    "title": proposal_template.title,
                    "description": proposal_template.description,
                    "execution_data": proposal_template.execution_data.hex()
                }
            }).encode(),
            metadata={"cross_chain_id": cross_chain_id, "origin_chain": origin_chain}
        )

        origin_dao = origin_dao.create_proposal(proposal_template.proposer_id, origin_proposal)
        origin_proposal_id = list(origin_dao.proposals.keys())[-1]

        # Create corresponding proposals on target chains
        target_proposal_ids = {}
        for target_chain in target_chains:
            if target_chain in self.chain_daos:
                target_dao = self.chain_daos[target_chain]
                target_proposal = DaoProposal(
                    proposer_id="cross_chain_coordinator",
                    proposal_type=ProposalType.CONTRACT_EXECUTION,
                    title=f"[FROM {origin_chain.upper()}] {proposal_template.title}",
                    description=f"Cross-chain proposal from {origin_chain}: {proposal_template.description}",
                    execution_data=proposal_template.execution_data,
                    metadata={
                        "cross_chain_id": cross_chain_id,
                        "origin_chain": origin_chain,
                        "origin_proposal_id": origin_proposal_id
                    }
                )

                target_dao = target_dao.create_proposal("cross_chain_coordinator", target_proposal)
                target_proposal_ids[target_chain] = list(target_dao.proposals.keys())[-1]

        # Track cross-chain proposal
        self.pending_cross_chain_proposals[cross_chain_id] = {
            "origin_chain": origin_chain,
            "origin_proposal_id": origin_proposal_id,
            "target_chains": target_chains,
            "target_proposal_ids": target_proposal_ids,
            "status": "voting",
            "created_at": time.time()
        }

        return {
            "cross_chain_id": cross_chain_id,
            "origin_proposal_id": origin_proposal_id,
            "target_proposal_ids": target_proposal_ids
        }

    def check_cross_chain_consensus(self, cross_chain_id: str) -> dict:
        """Check if cross-chain proposal has achieved consensus."""
        if cross_chain_id not in self.pending_cross_chain_proposals:
            raise ValueError("Cross-chain proposal not found")

        proposal_data = self.pending_cross_chain_proposals[cross_chain_id]
        origin_chain = proposal_data["origin_chain"]
        origin_proposal_id = proposal_data["origin_proposal_id"]

        # Check origin chain result
        origin_dao = self.chain_daos[origin_chain]
        origin_result = origin_dao.calculate_voting_result(origin_proposal_id)

        # Check target chain results
        target_results = {}
        for target_chain, target_proposal_id in proposal_data["target_proposal_ids"].items():
            target_dao = self.chain_daos[target_chain]
            target_result = target_dao.calculate_voting_result(target_proposal_id)
            target_results[target_chain] = target_result

        # Determine overall consensus
        all_passed = origin_result.proposal_passed and all(
            result.proposal_passed for result in target_results.values()
        )

        consensus_data = {
            "cross_chain_id": cross_chain_id,
            "overall_consensus": all_passed,
            "origin_result": {
                "chain": origin_chain,
                "passed": origin_result.proposal_passed,
                "approval_rate": origin_result.get_approval_rate(),
                "participation_rate": origin_result.get_participation_rate()
            },
            "target_results": {
                chain: {
                    "passed": result.proposal_passed,
                    "approval_rate": result.get_approval_rate(),
                    "participation_rate": result.get_participation_rate()
                }
                for chain, result in target_results.items()
            }
        }

        return consensus_data

    def execute_cross_chain_consensus(self, cross_chain_id: str) -> dict:
        """Execute cross-chain proposal if consensus achieved."""
        consensus = self.check_cross_chain_consensus(cross_chain_id)

        if not consensus["overall_consensus"]:
            raise ValueError("Cross-chain consensus not achieved")

        proposal_data = self.pending_cross_chain_proposals[cross_chain_id]
        execution_results = {}

        # Execute on origin chain
        origin_chain = proposal_data["origin_chain"]
        origin_dao = self.chain_daos[origin_chain]
        origin_proposal_id = proposal_data["origin_proposal_id"]

        # Finalize and execute origin proposal
        origin_dao = origin_dao.finalize_proposal(origin_proposal_id)
        origin_dao = origin_dao.execute_proposal(origin_proposal_id, "cross_chain_coordinator")
        execution_results[origin_chain] = "executed"

        # Execute on target chains
        for target_chain, target_proposal_id in proposal_data["target_proposal_ids"].items():
            target_dao = self.chain_daos[target_chain]
            target_dao = target_dao.finalize_proposal(target_proposal_id)
            target_dao = target_dao.execute_proposal(target_proposal_id, "cross_chain_coordinator")
            execution_results[target_chain] = "executed"

        # Update proposal status
        proposal_data["status"] = "executed"
        proposal_data["executed_at"] = time.time()
        proposal_data["execution_results"] = execution_results

        return {
            "cross_chain_id": cross_chain_id,
            "execution_status": "success",
            "chains_executed": list(execution_results.keys()),
            "execution_results": execution_results
        }

# Usage example
coordinator = CrossChainDAOCoordinator()

# Register DAOs on different chains
ethereum_dao = DecentralizedAutonomousOrganization(
    name="MPREG Ethereum DAO",
    description="MPREG governance on Ethereum",
    dao_type=DaoType.FEDERATION_GOVERNANCE
)

polygon_dao = DecentralizedAutonomousOrganization(
    name="MPREG Polygon DAO",
    description="MPREG governance on Polygon",
    dao_type=DaoType.FEDERATION_GOVERNANCE
)

coordinator.register_chain_dao("ethereum", ethereum_dao)
coordinator.register_chain_dao("polygon", polygon_dao)

# Register cross-chain bridges
coordinator.register_bridge("ethereum", "polygon", "0x1234...bridge_contract")
coordinator.register_bridge("polygon", "ethereum", "0x5678...bridge_contract")

# Create cross-chain liquidity proposal
liquidity_proposal = DaoProposal(
    proposer_id="multi_chain_operator",
    proposal_type=ProposalType.CONTRACT_EXECUTION,
    title="Deploy Cross-Chain Liquidity Pool",
    description="Create unified liquidity pool across Ethereum and Polygon",
    execution_data=json.dumps({
        "pool_tokens": ["MPREG", "ETH", "MATIC", "USDC"],
        "initial_liquidity_eth": 1000,
        "initial_liquidity_polygon": 100000,
        "cross_chain_rate": "1:100"
    }).encode()
)

# Create cross-chain proposal
cross_chain_result = coordinator.create_cross_chain_proposal(
    origin_chain="ethereum",
    target_chains=["polygon"],
    proposal_template=liquidity_proposal
)

print(f"Cross-chain proposal created: {cross_chain_result}")
```

## Production Deployment Scripts

### 1. Automated DAO Factory

```python
#!/usr/bin/env python3
"""
Production DAO Deployment Script
Automates creation and configuration of DAOs for different use cases.
"""

import os
import json
import argparse
from pathlib import Path
from typing import Dict, List, Any

from mpreg.datastructures import (
    DecentralizedAutonomousOrganization, DaoConfig, DaoType,
    MembershipType, DaoMember
)

class ProductionDAOFactory:
    """Factory for deploying production-ready DAOs."""

    def __init__(self, config_file: str = "dao_config.json"):
        self.config = self._load_config(config_file)
        self.deployment_log = []

    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Load deployment configuration."""
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                return json.load(f)

        # Default configuration
        return {
            "environment": "production",
            "network": "mainnet",
            "treasury_initial_balance": 1000000,
            "governance_token": "MPREG",
            "default_voting_period_days": 7,
            "default_quorum_threshold": 0.25,
            "default_approval_threshold": 0.6,
            "backup_enabled": True,
            "monitoring_enabled": True
        }

    def deploy_federation_governance(self, region: str,
                                   hub_operators: List[Dict[str, Any]]) -> DecentralizedAutonomousOrganization:
        """Deploy federation governance DAO for specific region."""
        print(f"Deploying Federation Governance DAO for {region}...")

        config = DaoConfig(
            dao_type=DaoType.FEDERATION_GOVERNANCE,
            membership_type=MembershipType.STAKE_BASED,
            voting_period_seconds=self.config["default_voting_period_days"] * 86400,
            quorum_threshold=self.config["default_quorum_threshold"],
            approval_threshold=self.config["default_approval_threshold"],
            proposal_deposit=2000,
            emergency_threshold=0.8
        )

        dao = DecentralizedAutonomousOrganization(
            name=f"{region} Federation Governance",
            description=f"Decentralized governance for {region} federation hub",
            dao_type=DaoType.FEDERATION_GOVERNANCE,
            config=config,
            treasury_balance=self.config["treasury_initial_balance"]
        )

        # Add hub operators
        for operator in hub_operators:
            member = DaoMember(
                member_id=operator["id"],
                voting_power=operator["stake"],
                token_balance=operator["stake"] * 2,
                metadata={
                    "role": "hub_operator",
                    "region": region,
                    "location": operator.get("location", "unknown")
                }
            )
            dao = dao.add_member(member)
            print(f"  Added hub operator: {operator['id']} (stake: {operator['stake']})")

        self._log_deployment("federation_governance", region, dao)
        print(f"✓ Federation Governance DAO deployed for {region}")
        return dao

    def deploy_community_dao(self, name: str, description: str,
                           founding_members: List[Dict[str, Any]]) -> DecentralizedAutonomousOrganization:
        """Deploy community governance DAO."""
        print(f"Deploying Community DAO: {name}...")

        config = DaoConfig(
            dao_type=DaoType.COMMUNITY_GOVERNANCE,
            membership_type=MembershipType.TOKEN_HOLDER,
            voting_period_seconds=self.config["default_voting_period_days"] * 86400,
            quorum_threshold=0.15,  # Lower quorum for community
            approval_threshold=0.55, # Lower threshold for community
            proposal_deposit=500,
            max_proposals_per_member=5
        )

        dao = DecentralizedAutonomousOrganization(
            name=name,
            description=description,
            dao_type=DaoType.COMMUNITY_GOVERNANCE,
            config=config,
            treasury_balance=self.config["treasury_initial_balance"] // 2
        )

        # Add founding members
        for member_data in founding_members:
            member = DaoMember(
                member_id=member_data["id"],
                voting_power=member_data["voting_power"],
                token_balance=member_data["token_balance"],
                reputation_score=member_data.get("reputation", 50),
                metadata={
                    "role": member_data.get("role", "community_member"),
                    "founding_member": True
                }
            )
            dao = dao.add_member(member)
            print(f"  Added founding member: {member_data['id']}")

        self._log_deployment("community_governance", name, dao)
        print(f"✓ Community DAO deployed: {name}")
        return dao

    def deploy_technical_committee(self, experts: List[Dict[str, Any]]) -> DecentralizedAutonomousOrganization:
        """Deploy technical committee DAO."""
        print("Deploying Technical Committee DAO...")

        config = DaoConfig(
            dao_type=DaoType.TECHNICAL_COMMITTEE,
            membership_type=MembershipType.REPUTATION_BASED,
            voting_period_seconds=5 * 86400,  # 5 days for tech decisions
            quorum_threshold=0.6,              # Higher quorum for tech
            approval_threshold=0.75,           # Higher approval for tech
            proposal_deposit=100,              # Lower deposit for experts
            emergency_threshold=0.85
        )

        dao = DecentralizedAutonomousOrganization(
            name="MPREG Technical Committee",
            description="Expert governance for technical protocol decisions",
            dao_type=DaoType.TECHNICAL_COMMITTEE,
            config=config,
            treasury_balance=self.config["treasury_initial_balance"] // 10
        )

        # Add technical experts
        for expert in experts:
            member = DaoMember(
                member_id=expert["id"],
                voting_power=expert["voting_power"],
                token_balance=expert.get("token_balance", 1000),
                reputation_score=expert["reputation"],
                metadata={
                    "role": "technical_expert",
                    "specialization": expert.get("specialization", "general"),
                    "credentials": expert.get("credentials", [])
                }
            )
            dao = dao.add_member(member)
            print(f"  Added expert: {expert['id']} (reputation: {expert['reputation']})")

        self._log_deployment("technical_committee", "main", dao)
        print("✓ Technical Committee DAO deployed")
        return dao

    def _log_deployment(self, dao_type: str, identifier: str,
                       dao: DecentralizedAutonomousOrganization):
        """Log deployment details."""
        deployment_record = {
            "dao_type": dao_type,
            "identifier": identifier,
            "dao_id": dao.dao_id,
            "name": dao.name,
            "member_count": dao.get_member_count(),
            "total_voting_power": dao.get_total_voting_power(),
            "treasury_balance": dao.treasury_balance,
            "deployed_at": time.time(),
            "config": self.config
        }

        self.deployment_log.append(deployment_record)

        # Save deployment log
        if self.config.get("backup_enabled", True):
            self._save_deployment_log()

    def _save_deployment_log(self):
        """Save deployment log to file."""
        log_file = f"dao_deployments_{int(time.time())}.json"
        with open(log_file, 'w') as f:
            json.dump(self.deployment_log, f, indent=2, default=str)
        print(f"Deployment log saved: {log_file}")

    def deploy_complete_federation(self, federation_spec: Dict[str, Any]):
        """Deploy complete federation DAO hierarchy."""
        print("🚀 Starting complete federation deployment...")

        deployed_daos = {}

        # Deploy global governance
        if "global_governance" in federation_spec:
            global_spec = federation_spec["global_governance"]
            global_dao = self.deploy_community_dao(
                name=global_spec["name"],
                description=global_spec["description"],
                founding_members=global_spec["founding_members"]
            )
            deployed_daos["global"] = global_dao

        # Deploy regional governance
        if "regional_governance" in federation_spec:
            deployed_daos["regional"] = {}
            for region, region_spec in federation_spec["regional_governance"].items():
                regional_dao = self.deploy_federation_governance(
                    region=region,
                    hub_operators=region_spec["hub_operators"]
                )
                deployed_daos["regional"][region] = regional_dao

        # Deploy technical committee
        if "technical_committee" in federation_spec:
            tech_dao = self.deploy_technical_committee(
                experts=federation_spec["technical_committee"]["experts"]
            )
            deployed_daos["technical"] = tech_dao

        print("🎉 Complete federation deployment successful!")
        return deployed_daos

def main():
    """Main deployment script."""
    parser = argparse.ArgumentParser(description="Deploy MPREG DAOs")
    parser.add_argument("--config", default="federation_spec.json",
                       help="Federation specification file")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show deployment plan without executing")

    args = parser.parse_args()

    # Load federation specification
    if not os.path.exists(args.config):
        print(f"❌ Configuration file not found: {args.config}")
        return 1

    with open(args.config, 'r') as f:
        federation_spec = json.load(f)

    if args.dry_run:
        print("🔍 Dry run - showing deployment plan:")
        print(json.dumps(federation_spec, indent=2))
        return 0

    # Deploy federation
    factory = ProductionDAOFactory()
    deployed_daos = factory.deploy_complete_federation(federation_spec)

    print(f"\n📊 Deployment Summary:")
    print(f"  DAOs deployed: {len(factory.deployment_log)}")
    print(f"  Total members: {sum(log['member_count'] for log in factory.deployment_log)}")
    print(f"  Total voting power: {sum(log['total_voting_power'] for log in factory.deployment_log)}")

    return 0

if __name__ == "__main__":
    import sys
    import time
    sys.exit(main())
```

### 2. Federation Configuration Template

```json
{
  "global_governance": {
    "name": "MPREG Global Federation",
    "description": "Global governance for MPREG federated network",
    "founding_members": [
      {
        "id": "founding_council_1",
        "voting_power": 15000,
        "token_balance": 50000,
        "role": "founding_council",
        "reputation": 100
      },
      {
        "id": "founding_council_2",
        "voting_power": 12000,
        "token_balance": 40000,
        "role": "founding_council",
        "reputation": 95
      },
      {
        "id": "founding_council_3",
        "voting_power": 13000,
        "token_balance": 45000,
        "role": "founding_council",
        "reputation": 98
      }
    ]
  },
  "regional_governance": {
    "north_america": {
      "hub_operators": [
        {
          "id": "na_hub_nyc",
          "stake": 20000,
          "location": "New York"
        },
        {
          "id": "na_hub_sf",
          "stake": 18000,
          "location": "San Francisco"
        },
        {
          "id": "na_hub_toronto",
          "stake": 15000,
          "location": "Toronto"
        }
      ]
    },
    "europe": {
      "hub_operators": [
        {
          "id": "eu_hub_london",
          "stake": 22000,
          "location": "London"
        },
        {
          "id": "eu_hub_frankfurt",
          "stake": 19000,
          "location": "Frankfurt"
        },
        {
          "id": "eu_hub_amsterdam",
          "stake": 17000,
          "location": "Amsterdam"
        }
      ]
    },
    "asia_pacific": {
      "hub_operators": [
        {
          "id": "ap_hub_tokyo",
          "stake": 25000,
          "location": "Tokyo"
        },
        {
          "id": "ap_hub_singapore",
          "stake": 23000,
          "location": "Singapore"
        },
        {
          "id": "ap_hub_sydney",
          "stake": 16000,
          "location": "Sydney"
        }
      ]
    }
  },
  "technical_committee": {
    "experts": [
      {
        "id": "lead_architect",
        "voting_power": 4,
        "reputation": 100,
        "specialization": "system_architecture",
        "credentials": ["PhD_CS", "10_years_blockchain"]
      },
      {
        "id": "security_expert",
        "voting_power": 4,
        "reputation": 98,
        "specialization": "cryptography",
        "credentials": ["Security_Audit_Lead", "CVE_Researcher"]
      },
      {
        "id": "consensus_expert",
        "voting_power": 3,
        "reputation": 95,
        "specialization": "consensus_algorithms",
        "credentials": ["Research_Scientist", "Consensus_Papers"]
      },
      {
        "id": "performance_expert",
        "voting_power": 3,
        "reputation": 92,
        "specialization": "performance_optimization",
        "credentials": ["Systems_Engineer", "HFT_Background"]
      },
      {
        "id": "protocol_expert",
        "voting_power": 2,
        "reputation": 90,
        "specialization": "protocol_design",
        "credentials": ["Protocol_Designer", "Standards_Committee"]
      }
    ]
  }
}
```

This comprehensive integration guide demonstrates how the MPREG DAO system can be seamlessly integrated into real-world federated network scenarios. The examples show:

1. **Hierarchical Governance**: Global → Regional → Local DAO structures
2. **Cross-Chain Coordination**: Multi-blockchain governance synchronization
3. **Production Deployment**: Automated deployment and configuration tools
4. **Real-World Use Cases**: Infrastructure management, economic policy, technical decisions

The modular design allows for flexible deployment while maintaining strong governance guarantees and blockchain-backed transparency across the entire federation network.
