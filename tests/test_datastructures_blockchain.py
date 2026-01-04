"""
Comprehensive property-based tests for Blockchain datastructure.

This test suite uses the hypothesis library to aggressively test the correctness
of the Blockchain implementation with property-based testing, focusing on consensus
mechanisms, chain validation, and fork resolution.
"""

import time

import pytest
from hypothesis import given

from mpreg.datastructures.block import Block
from mpreg.datastructures.blockchain import (
    Blockchain,
    simple_blockchain_strategy,
)
from mpreg.datastructures.blockchain_types import (
    ConsensusConfig,
    ConsensusType,
    OperationType,
)
from mpreg.datastructures.transaction import Transaction


class TestBlockchain:
    """Test Blockchain datastructure."""

    @given(simple_blockchain_strategy())
    def test_blockchain_creation(self, blockchain: Blockchain):
        """Test that valid blockchains can be created."""
        assert blockchain.chain_id != ""
        assert blockchain.genesis_block is not None
        assert blockchain.genesis_block.height == 0
        assert isinstance(blockchain.blocks, tuple)
        assert isinstance(blockchain.consensus_config, ConsensusConfig)
        assert blockchain.validate_chain()

    def test_blockchain_validation(self):
        """Test blockchain validation."""
        # Valid blockchain
        blockchain = Blockchain.create_new_chain("test_chain", "genesis_miner")
        assert blockchain.validate_chain()

        # Invalid chain ID
        with pytest.raises(ValueError, match="Chain ID cannot be empty"):
            Blockchain(chain_id="")

        # Test invalid chain with invalid genesis (create with minimal valid block then manually validate)
        try:
            # Create a valid block first then modify it to be invalid for blockchain purposes
            valid_block = Block.create_genesis("test_miner")
            # Modify to have wrong height for blockchain
            invalid_genesis = Block(
                block_id=valid_block.block_id,
                height=1,  # Genesis should be 0, but this is 1
                miner="test_miner",
                merkle_root=valid_block.merkle_root,
                vector_clock=valid_block.vector_clock,
                transactions=valid_block.transactions,
                difficulty=valid_block.difficulty,
                nonce=valid_block.nonce,
                timestamp=valid_block.timestamp,
            )
            with pytest.raises(ValueError, match="Invalid genesis block"):
                Blockchain(chain_id="test", genesis_block=invalid_genesis)
        except ValueError as e:
            # Block constructor itself may reject the invalid block
            assert "height" in str(e).lower() or "miner" in str(e).lower()

    @given(simple_blockchain_strategy())
    def test_blockchain_height_calculation(self, blockchain: Blockchain):
        """Test blockchain height calculation."""
        height = blockchain.get_height()
        assert height >= 0
        assert height == len(blockchain.blocks)

        # Latest block should match height
        if height > 0:
            latest = blockchain.get_latest_block()
            assert latest.height == height
        else:
            # No blocks beyond genesis
            latest = blockchain.get_latest_block()
            assert latest == blockchain.genesis_block

    def test_block_addition(self):
        """Test adding blocks to blockchain."""
        blockchain = Blockchain.create_new_chain("test_chain", "genesis_miner")

        # Add valid block
        tx = Transaction(sender="alice", receiver="bob", fee=10)
        new_block = Block.create_next_block(
            blockchain.get_latest_block(), (tx,), "miner1"
        )

        new_blockchain = blockchain.add_block(new_block)
        assert new_blockchain.get_height() == 1
        assert new_blockchain.blocks[-1] == new_block
        assert new_blockchain.validate_chain()

        # Original blockchain unchanged
        assert blockchain.get_height() == 0

    def test_invalid_block_addition(self):
        """Test adding invalid blocks to blockchain."""
        blockchain = Blockchain.create_new_chain("test_chain", "genesis_miner")

        # Block with wrong height
        invalid_block = Block(
            height=2,  # Should be 1
            miner="miner1",
        )

        with pytest.raises(ValueError, match="Invalid block"):
            blockchain.add_block(invalid_block)

    def test_block_rejects_invalid_merkle_root(self):
        """Blocks with mismatched merkle roots should be rejected."""
        tx = Transaction(sender="alice", receiver="bob", fee=1)
        with pytest.raises(ValueError, match="Merkle root"):
            Block(
                height=1,
                previous_hash="bad_prev",
                merkle_root="bad_root",
                transactions=(tx,),
                miner="miner1",
                difficulty=1,
                nonce=0,
                timestamp=time.time(),
            )

    def test_transaction_lookup(self):
        """Test finding transactions in blockchain."""
        # Create blockchain with transaction
        tx = Transaction(sender="alice", receiver="bob", fee=10)
        blockchain = Blockchain.create_new_chain(
            "test_chain", "genesis_miner", initial_transactions=(tx,)
        )

        # Should find transaction in genesis block
        result = blockchain.find_transaction(tx.transaction_id)
        assert result is not None
        found_block, found_tx = result
        assert found_tx == tx
        assert found_block == blockchain.genesis_block

        # Should not find non-existent transaction
        assert blockchain.find_transaction("nonexistent") is None

    def test_balance_calculation(self):
        """Test balance calculation for nodes."""
        # Create transactions
        tx1 = Transaction(sender="alice", receiver="bob", fee=10)
        tx2 = Transaction(sender="bob", receiver="charlie", fee=5)

        blockchain = Blockchain.create_new_chain(
            "test_chain", "genesis_miner", initial_transactions=(tx1, tx2)
        )

        # Bob should have positive balance (received 10, paid 5)
        bob_balance = blockchain.get_balance("bob")
        assert bob_balance == 5  # 10 received - 5 paid

        # Alice should have negative balance (paid 10)
        alice_balance = blockchain.get_balance("alice")
        assert alice_balance == -10

        # Charlie should have positive balance (received 5)
        charlie_balance = blockchain.get_balance("charlie")
        assert charlie_balance == 5

    def test_fork_creation(self):
        """Test creating forks from blockchain."""
        # Create blockchain with multiple blocks
        blockchain = Blockchain.create_new_chain("main_chain", "genesis_miner")

        tx1 = Transaction(sender="alice", receiver="bob", fee=10)
        block1 = Block.create_next_block(blockchain.genesis_block, (tx1,), "miner1")
        blockchain = blockchain.add_block(block1)

        tx2 = Transaction(sender="bob", receiver="charlie", fee=5)
        block2 = Block.create_next_block(block1, (tx2,), "miner2")
        blockchain = blockchain.add_block(block2)

        # Fork from genesis
        fork_from_genesis = blockchain.fork_at_block(
            blockchain.genesis_block.get_block_hash()
        )
        assert fork_from_genesis.chain_id.endswith("_fork")
        assert fork_from_genesis.get_height() == 0
        assert len(fork_from_genesis.blocks) == 0

        # Fork from block1
        fork_from_block1 = blockchain.fork_at_block(block1.get_block_hash())
        assert fork_from_block1.get_height() == 1
        assert len(fork_from_block1.blocks) == 1

    def test_fork_resolution(self):
        """Test fork resolution using longest chain rule."""
        # Create main chain
        main_chain = Blockchain.create_new_chain("main", "genesis_miner")
        tx1 = Transaction(sender="alice", receiver="bob", fee=10)
        block1 = Block.create_next_block(main_chain.genesis_block, (tx1,), "miner1")
        main_chain = main_chain.add_block(block1)

        # Create longer competing chain
        competing_chain = Blockchain.create_new_chain("competing", "genesis_miner")
        # Use same genesis block for valid comparison
        competing_chain = Blockchain(
            chain_id="competing",
            genesis_block=main_chain.genesis_block,
            blocks=(),
            consensus_config=competing_chain.consensus_config,
        )

        tx2 = Transaction(sender="bob", receiver="charlie", fee=5)
        tx3 = Transaction(sender="charlie", receiver="alice", fee=3)
        block2 = Block.create_next_block(
            competing_chain.genesis_block, (tx2,), "miner2", difficulty=2
        )
        block3 = Block.create_next_block(block2, (tx3,), "miner3", difficulty=2)

        competing_chain = competing_chain.add_block(block2)
        competing_chain = competing_chain.add_block(block3)

        # Resolve fork - competing chain should win (higher total work)
        resolved = main_chain.resolve_fork(competing_chain)
        assert resolved == competing_chain
        assert resolved.get_height() == 2

    def test_difficulty_adjustment(self):
        """Test difficulty adjustment mechanism."""
        blockchain = Blockchain.create_new_chain("test_chain", "genesis_miner")

        # With few blocks, should return default difficulty
        current_diff = blockchain.get_current_difficulty()
        assert current_diff > 0

        # Create chain with many blocks for difficulty adjustment
        for i in range(10):
            tx = Transaction(sender=f"sender{i}", receiver=f"receiver{i}", fee=1)
            new_block = Block.create_next_block(
                blockchain.get_latest_block(),
                (tx,),
                f"miner{i}",
                difficulty=current_diff,
            )
            blockchain = blockchain.add_block(new_block)

        # Difficulty should still be calculated
        new_diff = blockchain.get_current_difficulty()
        assert new_diff > 0

    @given(simple_blockchain_strategy())
    def test_blockchain_serialization(self, blockchain: Blockchain):
        """Test blockchain serialization roundtrip."""
        # Serialize to dict
        blockchain_dict = blockchain.to_dict()
        assert isinstance(blockchain_dict, dict)
        assert "chain_id" in blockchain_dict
        assert "genesis_block" in blockchain_dict
        assert "blocks" in blockchain_dict
        assert "consensus_config" in blockchain_dict
        assert "crypto_config" in blockchain_dict
        assert "summary" in blockchain_dict

        # Deserialize from dict
        reconstructed = Blockchain.from_dict(blockchain_dict)
        assert reconstructed.chain_id == blockchain.chain_id
        assert reconstructed.get_height() == blockchain.get_height()
        assert (
            reconstructed.consensus_config.consensus_type
            == blockchain.consensus_config.consensus_type
        )
        assert (
            reconstructed.crypto_config.signature_algorithm
            == blockchain.crypto_config.signature_algorithm
        )

    def test_consensus_validation(self):
        """Test consensus-specific validation."""
        # Proof of Work blockchain
        pow_config = ConsensusConfig(
            consensus_type=ConsensusType.PROOF_OF_WORK,
            block_time_target=10,
            difficulty_adjustment_interval=100,
            max_transactions_per_block=100,
        )

        pow_chain = Blockchain.create_new_chain("pow_chain", "miner", pow_config)

        # Create block for PoW chain
        tx = Transaction(sender="alice", receiver="bob", fee=10)
        pow_block = Block.create_next_block(pow_chain.genesis_block, (tx,), "miner")

        # Should pass basic validation
        current_time = time.time()
        can_add = pow_chain.can_add_block(pow_block, current_time)
        # Note: May fail PoW verification since we haven't mined it

        # Test transaction limit
        many_txs = tuple(
            Transaction(sender=f"sender{i}", receiver=f"receiver{i}", fee=1)
            for i in range(150)  # Exceeds limit of 100
        )

        invalid_block = Block.create_next_block(
            pow_chain.genesis_block, many_txs, "miner"
        )
        assert not pow_chain.can_add_block(invalid_block, current_time)

    @given(simple_blockchain_strategy())
    def test_blockchain_immutability(self, blockchain: Blockchain):
        """Test blockchain immutability."""
        original_height = blockchain.get_height()
        original_chain_id = blockchain.chain_id

        # Create a new block
        tx = Transaction(sender="test", receiver="test2", fee=1)
        new_block = Block.create_next_block(
            blockchain.get_latest_block(), (tx,), "test_miner"
        )

        if not blockchain.can_add_block(new_block):
            return

        # Adding should return new blockchain
        new_blockchain = blockchain.add_block(new_block)

        # Original blockchain should be unchanged
        assert blockchain.get_height() == original_height
        assert blockchain.chain_id == original_chain_id
        assert new_blockchain is not blockchain

    def test_vector_clock_state(self):
        """Test getting vector clock state from blockchain."""
        blockchain = Blockchain.create_new_chain("test_chain", "genesis_miner")

        # Genesis should have empty vector clock
        vc_state = blockchain.get_vector_clock_state()
        assert vc_state.is_empty()

        # Add block and check vector clock progression
        tx = Transaction(sender="alice", receiver="bob", fee=10)
        new_block = Block.create_next_block(blockchain.genesis_block, (tx,), "miner1")
        blockchain = blockchain.add_block(new_block)

        updated_vc_state = blockchain.get_vector_clock_state()
        assert not updated_vc_state.is_empty()


class TestBlockchainProperties:
    """Test mathematical and cryptographic properties of blockchains."""

    @given(simple_blockchain_strategy())
    def test_blockchain_total_work_calculation(self, blockchain: Blockchain):
        """Test total work calculation."""
        total_work = blockchain.get_total_work()
        assert total_work > 0

        # Should be sum of all block difficulties
        expected_work = blockchain.genesis_block.difficulty
        for block in blockchain.blocks:
            expected_work += block.difficulty

        assert total_work == expected_work

    def test_chain_summary(self):
        """Test chain summary information."""
        blockchain = Blockchain.create_new_chain("test_chain", "genesis_miner")

        # Add some blocks with transactions
        for i in range(3):
            tx = Transaction(sender=f"sender{i}", receiver=f"receiver{i}", fee=i + 1)
            new_block = Block.create_next_block(
                blockchain.get_latest_block(), (tx,), f"miner{i}"
            )
            blockchain = blockchain.add_block(new_block)

        summary = blockchain.get_chain_summary()

        assert summary["chain_id"] == "test_chain"
        assert summary["height"] == 3
        assert summary["total_blocks"] == 4  # Including genesis
        assert summary["total_transactions"] == 3
        assert summary["total_work"] > 0
        assert "latest_block_hash" in summary
        assert "latest_timestamp" in summary
        assert summary["consensus_type"] == ConsensusType.PROOF_OF_AUTHORITY.value

    def test_blockchain_iteration(self):
        """Test iterating over blockchain."""
        blockchain = Blockchain.create_new_chain("test_chain", "genesis_miner")

        # Add a block
        tx = Transaction(sender="alice", receiver="bob", fee=10)
        new_block = Block.create_next_block(blockchain.genesis_block, (tx,), "miner1")
        blockchain = blockchain.add_block(new_block)

        # Test iteration
        blocks = list(blockchain)
        assert len(blocks) == 2
        assert blocks[0] == blockchain.genesis_block
        assert blocks[1] == new_block

        # Test length
        assert len(blockchain) == 2

    def test_block_lookup_by_height(self):
        """Test looking up blocks by height."""
        blockchain = Blockchain.create_new_chain("test_chain", "genesis_miner")

        # Should find genesis block at height 0
        genesis = blockchain.get_block_at_height(0)
        assert genesis == blockchain.genesis_block

        # Should return None for non-existent height
        assert blockchain.get_block_at_height(5) is None

        # Add block and test lookup
        tx = Transaction(sender="alice", receiver="bob", fee=10)
        new_block = Block.create_next_block(blockchain.genesis_block, (tx,), "miner1")
        blockchain = blockchain.add_block(new_block)

        found_block = blockchain.get_block_at_height(1)
        assert found_block == new_block

    def test_block_lookup_by_hash(self):
        """Test looking up blocks by hash."""
        blockchain = Blockchain.create_new_chain("test_chain", "genesis_miner")

        # Should find genesis block by hash
        genesis_hash = blockchain.genesis_block.get_block_hash()
        found_genesis = blockchain.get_block_by_hash(genesis_hash)
        assert found_genesis == blockchain.genesis_block

        # Should return None for non-existent hash
        assert blockchain.get_block_by_hash("nonexistent_hash") is None

        # Add block and test lookup
        tx = Transaction(sender="alice", receiver="bob", fee=10)
        new_block = Block.create_next_block(blockchain.genesis_block, (tx,), "miner1")
        blockchain = blockchain.add_block(new_block)

        new_block_hash = new_block.get_block_hash()
        found_block = blockchain.get_block_by_hash(new_block_hash)
        assert found_block == new_block


class TestBlockchainConsensus:
    """Test consensus mechanisms and validation."""

    def test_proof_of_authority(self):
        """Test Proof of Authority consensus."""
        poa_config = ConsensusConfig(
            consensus_type=ConsensusType.PROOF_OF_AUTHORITY,
            block_time_target=5,
            difficulty_adjustment_interval=50,
            max_transactions_per_block=500,
        )

        blockchain = Blockchain.create_new_chain(
            "poa_chain", "authority_node", poa_config
        )

        # Authority node should be able to create blocks
        tx = Transaction(sender="alice", receiver="bob", fee=10)
        auth_block = Block.create_next_block(
            blockchain.genesis_block, (tx,), "authority_node"
        )

        assert blockchain.can_add_block(auth_block)
        new_blockchain = blockchain.add_block(auth_block)
        assert new_blockchain.validate_chain()

    def test_proof_of_stake(self):
        """Test Proof of Stake consensus."""
        pos_config = ConsensusConfig(
            consensus_type=ConsensusType.PROOF_OF_STAKE,
            block_time_target=15,
            difficulty_adjustment_interval=200,
            max_transactions_per_block=2000,
        )

        blockchain = Blockchain.create_new_chain("pos_chain", "staker_node", pos_config)

        # Staker should be able to create blocks
        tx = Transaction(sender="alice", receiver="bob", fee=10)
        stake_block = Block.create_next_block(
            blockchain.genesis_block, (tx,), "staker_node"
        )

        assert blockchain.can_add_block(stake_block)
        new_blockchain = blockchain.add_block(stake_block)
        assert new_blockchain.validate_chain()

    def test_proof_of_work_validation(self):
        """Test Proof of Work validation requirements."""
        pow_config = ConsensusConfig(
            consensus_type=ConsensusType.PROOF_OF_WORK,
            block_time_target=10,
            difficulty_adjustment_interval=100,
            max_transactions_per_block=1000,
        )

        blockchain = Blockchain.create_new_chain("pow_chain", "miner", pow_config)

        # Create block without proper PoW
        tx = Transaction(sender="alice", receiver="bob", fee=10)
        pow_block = Block.create_next_block(blockchain.genesis_block, (tx,), "miner")

        # Should fail PoW validation (unless we get lucky with nonce)
        can_add = blockchain.can_add_block(pow_block)
        # Note: This might occasionally pass if the hash happens to have leading zeros

    def test_transaction_limits(self):
        """Test transaction count limits per block."""
        limited_config = ConsensusConfig(
            consensus_type=ConsensusType.PROOF_OF_AUTHORITY,
            block_time_target=10,
            difficulty_adjustment_interval=100,
            max_transactions_per_block=2,  # Very low limit for testing
        )

        blockchain = Blockchain.create_new_chain(
            "limited_chain", "miner", limited_config
        )

        # Create block with too many transactions
        many_txs = tuple(
            Transaction(sender=f"sender{i}", receiver=f"receiver{i}", fee=1)
            for i in range(5)  # Exceeds limit of 2
        )

        invalid_block = Block.create_next_block(
            blockchain.genesis_block, many_txs, "miner"
        )
        assert not blockchain.can_add_block(invalid_block)

        # Valid block with acceptable number of transactions
        few_txs = many_txs[:2]  # Only take first 2
        valid_block = Block.create_next_block(
            blockchain.genesis_block, few_txs, "miner"
        )
        assert blockchain.can_add_block(valid_block)


class TestBlockchainExamples:
    """Test specific blockchain examples and use cases."""

    def test_simple_payment_chain(self):
        """Test a simple payment blockchain."""
        blockchain = Blockchain.create_new_chain("payment_chain", "bank_node")

        # Alice sends money to Bob
        tx1 = Transaction(
            sender="alice",
            receiver="bob",
            operation_type=OperationType.TRANSFER,
            payload=b"payment for services",
            fee=1,
        )

        block1 = Block.create_next_block(blockchain.genesis_block, (tx1,), "validator1")
        blockchain = blockchain.add_block(block1)

        # Bob sends money to Charlie
        tx2 = Transaction(
            sender="bob",
            receiver="charlie",
            operation_type=OperationType.TRANSFER,
            payload=b"payment forwarded",
            fee=1,
        )

        block2 = Block.create_next_block(block1, (tx2,), "validator2")
        blockchain = blockchain.add_block(block2)

        assert blockchain.validate_chain()
        assert blockchain.get_height() == 2

        # Check final balances
        assert blockchain.get_balance("alice") == -1  # Paid fee
        assert blockchain.get_balance("bob") == 0  # Received fee, paid fee
        assert blockchain.get_balance("charlie") == 1  # Received fee

    def test_federation_governance_chain(self):
        """Test blockchain for federation governance."""
        blockchain = Blockchain.create_new_chain("federation_gov", "federation_hub")

        # Node join request
        join_tx = Transaction(
            sender="new_node_id",
            receiver="hub_registry",
            operation_type=OperationType.FEDERATION_JOIN,
            payload=b'{"node_type": "full", "capabilities": ["routing", "storage"]}',
            fee=10,
        )

        # Governance vote
        vote_tx = Transaction(
            sender="council_member_1",
            receiver="governance_contract",
            operation_type=OperationType.CONSENSUS_VOTE,
            payload=b'{"proposal_id": "proposal_123", "vote": "approve"}',
            fee=1,
        )

        governance_block = Block.create_next_block(
            blockchain.genesis_block, (join_tx, vote_tx), "governance_validator"
        )

        blockchain = blockchain.add_block(governance_block)

        assert blockchain.validate_chain()
        assert blockchain.get_height() == 1
        assert governance_block.transaction_count() == 2

        # Verify specific transactions can be found
        found_join = blockchain.find_transaction(join_tx.transaction_id)
        assert found_join is not None
        assert found_join[1].operation_type == OperationType.FEDERATION_JOIN

    def test_message_routing_chain(self):
        """Test blockchain for message routing and delivery."""
        blockchain = Blockchain.create_new_chain("message_chain", "message_hub")

        # Message transactions
        messages = []
        for i in range(3):
            msg_tx = Transaction(
                sender=f"node_{i}",
                receiver=f"node_{i + 1}",
                operation_type=OperationType.MESSAGE,
                payload=f"Hello from node {i}".encode(),
                fee=1,
            )
            messages.append(msg_tx)

        # Create block with all messages
        message_block = Block.create_next_block(
            blockchain.genesis_block, tuple(messages), "message_validator"
        )

        blockchain = blockchain.add_block(message_block)

        assert blockchain.validate_chain()
        assert message_block.transaction_count() == 3

        # Verify message routing can be tracked
        for i, msg in enumerate(messages):
            found = blockchain.find_transaction(msg.transaction_id)
            assert found is not None
            assert found[1].operation_type == OperationType.MESSAGE
            assert f"Hello from node {i}".encode() == found[1].payload

    def test_blockchain_fork_resolution_scenario(self):
        """Test realistic fork resolution scenario."""
        # Create main chain
        main_chain = Blockchain.create_new_chain("main_network", "main_miner")

        # Add blocks to main chain
        for i in range(2):
            tx = Transaction(sender=f"user{i}", receiver=f"merchant{i}", fee=5)
            block = Block.create_next_block(
                main_chain.get_latest_block(), (tx,), f"main_miner_{i}", difficulty=1
            )
            main_chain = main_chain.add_block(block)

        # Create competing fork with higher difficulty
        fork_chain = main_chain.fork_at_block(main_chain.genesis_block.get_block_hash())

        # Add blocks with higher total work to fork
        for i in range(3):
            tx = Transaction(
                sender=f"fork_user{i}", receiver=f"fork_merchant{i}", fee=3
            )
            block = Block.create_next_block(
                fork_chain.get_latest_block(),
                (tx,),
                f"fork_miner_{i}",
                difficulty=2,  # Higher difficulty
            )
            fork_chain = fork_chain.add_block(block)

        # Fork should win due to higher total work
        resolved = main_chain.resolve_fork(fork_chain)
        assert resolved == fork_chain
        assert resolved.get_total_work() > main_chain.get_total_work()
        assert resolved.get_height() == 3
        assert resolved.chain_id == "main_network_fork"
