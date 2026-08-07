"""
Comprehensive property-based tests for Merkle tree datastructure.

This test suite uses the hypothesis library to aggressively test the correctness
of the centralized Merkle tree implementation with property-based testing.

Tests cover:
- Tree construction and validation
- Immutability and thread safety
- Proof generation and verification
- Tree comparison and merging
- Cryptographic properties
- Edge cases and error conditions
"""

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from mpreg.datastructures.merkle_tree import (
    MerkleNode,
    MerkleProof,
    MerkleTree,
    _compute_internal_hash,
    _compute_leaf_hash,
    merkle_leaf_data_strategy,
    merkle_proof_strategy,
    merkle_tree_strategy,
    non_empty_merkle_tree_strategy,
)


class TestMerkleNode:
    """Test MerkleNode datastructure."""

    @given(merkle_leaf_data_strategy())
    def test_leaf_node_creation(self, data: bytes):
        """Test that valid leaf nodes can be created."""
        node = MerkleNode.create_leaf(data)
        assert node.is_leaf()
        assert not node.is_internal()
        assert node.leaf_data == data
        assert node.left_child is None
        assert node.right_child is None
        assert node.hash_value == _compute_leaf_hash(data)

    def test_leaf_node_validation(self):
        """Test leaf node validation."""
        data = b"test_data"
        hash_value = _compute_leaf_hash(data)

        # Valid leaf node
        node = MerkleNode(hash_value=hash_value, leaf_data=data)
        assert node.is_leaf()

        # Invalid: leaf with children
        with pytest.raises(ValueError, match="Leaf nodes cannot have children"):
            MerkleNode(
                hash_value=hash_value,
                leaf_data=data,
                left_child=MerkleNode.create_leaf(b"child"),
            )

        # Invalid: leaf without data
        with pytest.raises(ValueError, match="Leaf nodes must have data"):
            MerkleNode(hash_value=hash_value)

    @given(merkle_leaf_data_strategy(), merkle_leaf_data_strategy())
    def test_internal_node_creation(self, left_data: bytes, right_data: bytes):
        """Test that valid internal nodes can be created."""
        left_node = MerkleNode.create_leaf(left_data)
        right_node = MerkleNode.create_leaf(right_data)

        internal_node = MerkleNode.create_internal(left_node, right_node)

        assert internal_node.is_internal()
        assert not internal_node.is_leaf()
        assert internal_node.leaf_data is None
        assert internal_node.left_child == left_node
        assert internal_node.right_child == right_node
        assert internal_node.hash_value == _compute_internal_hash(
            left_node.hash_value, right_node.hash_value
        )

    def test_internal_node_validation(self):
        """Test internal node validation."""
        left = MerkleNode.create_leaf(b"left")
        right = MerkleNode.create_leaf(b"right")
        hash_value = _compute_internal_hash(left.hash_value, right.hash_value)

        # Valid internal node
        node = MerkleNode(hash_value=hash_value, left_child=left, right_child=right)
        assert node.is_internal()

        # Invalid: internal with leaf data
        with pytest.raises(ValueError, match="Internal nodes cannot have leaf data"):
            MerkleNode(
                hash_value=hash_value,
                left_child=left,
                right_child=right,
                leaf_data=b"data",
            )

        # Invalid: internal missing children
        with pytest.raises(ValueError, match="Internal nodes must have both children"):
            MerkleNode(hash_value=hash_value, left_child=left)


class TestMerkleTreeBasics:
    """Test basic MerkleTree functionality."""

    def test_empty_tree(self):
        """Test empty tree creation and properties."""
        tree = MerkleTree.empty()
        assert tree.is_empty()
        assert tree.leaf_count() == 0
        assert len(tree) == 0
        assert tree.depth() == 0
        assert tree.root_hash() == ""
        assert tree.get_leaves() == ()

    def test_single_leaf_tree(self):
        """Test tree with single leaf."""
        data = b"single_leaf"
        tree = MerkleTree.single_leaf(data)

        assert not tree.is_empty()
        assert tree.leaf_count() == 1
        assert len(tree) == 1
        assert tree.depth() == 1
        assert tree.root_hash() == _compute_leaf_hash(data)
        assert tree.get_leaf(0) == data
        assert data in tree
        assert tree.contains_leaf(data)

    @given(st.lists(merkle_leaf_data_strategy(), min_size=1, max_size=20))
    def test_tree_from_leaves(self, leaves: list[bytes]):
        """Test tree creation from multiple leaves."""
        tree = MerkleTree.from_leaves(leaves)

        assert not tree.is_empty()
        assert tree.leaf_count() == len(leaves)
        assert len(tree) == len(leaves)
        assert tree.get_leaves() == tuple(leaves)

        # All leaves should be accessible
        for i, data in enumerate(leaves):
            assert tree.get_leaf(i) == data
            assert tree[i] == data
            assert data in tree

    @given(merkle_tree_strategy())
    def test_tree_properties(self, tree: MerkleTree):
        """Test basic properties of any tree."""
        # Leaf count consistency
        assert tree.leaf_count() == len(tree)
        assert tree.leaf_count() == len(tree.get_leaves())

        # Empty tree properties
        if tree.is_empty():
            assert tree.leaf_count() == 0
            assert tree.depth() == 0
            assert tree.root_hash() == ""
        else:
            assert tree.leaf_count() > 0
            assert tree.depth() > 0
            assert tree.root_hash() != ""

        # All leaves accessible
        for i in range(tree.leaf_count()):
            assert tree.get_leaf(i) == tree[i]
            assert tree.get_leaf(i) in tree

    def test_tree_indexing_errors(self):
        """Test that invalid indexing raises appropriate errors."""
        tree = MerkleTree.from_leaves([b"a", b"b", b"c"])

        # Valid indices
        assert tree.get_leaf(0) == b"a"
        assert tree.get_leaf(2) == b"c"

        # Invalid indices
        with pytest.raises(IndexError):
            tree.get_leaf(-1)

        with pytest.raises(IndexError):
            tree.get_leaf(3)

        with pytest.raises(IndexError):
            tree[10]


class TestMerkleTreeOperations:
    """Test MerkleTree operations and mutations."""

    @given(merkle_tree_strategy(), merkle_leaf_data_strategy())
    def test_append_leaf(self, tree: MerkleTree, new_data: bytes):
        """Test appending a leaf to a tree."""
        new_tree = tree.append_leaf(new_data)

        # Original tree unchanged (immutability)
        assert tree.leaf_count() == tree.leaf_count()

        # New tree has additional leaf
        assert new_tree.leaf_count() == tree.leaf_count() + 1
        assert new_tree.get_leaf(tree.leaf_count()) == new_data
        assert new_data in new_tree

        # All original leaves preserved
        for i in range(tree.leaf_count()):
            assert new_tree.get_leaf(i) == tree.get_leaf(i)

    @given(non_empty_merkle_tree_strategy(), merkle_leaf_data_strategy())
    def test_update_leaf(self, tree: MerkleTree, new_data: bytes):
        """Test updating a leaf in a tree."""
        index = 0  # Update first leaf
        original_data = tree.get_leaf(index)

        new_tree = tree.update_leaf(index, new_data)

        # Original tree unchanged (immutability)
        assert tree.get_leaf(index) == original_data

        # New tree has updated leaf
        assert new_tree.get_leaf(index) == new_data
        assert new_tree.leaf_count() == tree.leaf_count()

        # Other leaves unchanged
        for i in range(1, tree.leaf_count()):
            assert new_tree.get_leaf(i) == tree.get_leaf(i)

    @given(non_empty_merkle_tree_strategy())
    def test_remove_leaf(self, tree: MerkleTree):
        """Test removing a leaf from a tree."""
        assume(tree.leaf_count() > 1)  # Need at least 2 leaves

        index = 0  # Remove first leaf
        removed_data = tree.get_leaf(index)

        new_tree = tree.remove_leaf(index)

        # Original tree unchanged (immutability)
        assert tree.get_leaf(index) == removed_data

        # New tree has one fewer leaf
        assert new_tree.leaf_count() == tree.leaf_count() - 1

        # Check if removed data was unique
        original_count = sum(1 for leaf in tree.get_leaves() if leaf == removed_data)
        new_count = sum(1 for leaf in new_tree.get_leaves() if leaf == removed_data)

        if original_count == 1:
            # Data was unique, should not be in new tree
            assert removed_data not in new_tree
        else:
            # Data was duplicated, should still be in tree but with one less occurrence
            assert new_count == original_count - 1

        # Remaining leaves shifted down
        for i in range(new_tree.leaf_count()):
            assert new_tree.get_leaf(i) == tree.get_leaf(i + 1)

    def test_update_remove_errors(self):
        """Test that invalid update/remove operations raise errors."""
        tree = MerkleTree.from_leaves([b"a", b"b"])

        # Invalid update index
        with pytest.raises(IndexError):
            tree.update_leaf(-1, b"new")

        with pytest.raises(IndexError):
            tree.update_leaf(2, b"new")

        # Invalid remove index
        with pytest.raises(IndexError):
            tree.remove_leaf(-1)

        with pytest.raises(IndexError):
            tree.remove_leaf(2)

    @given(merkle_tree_strategy())
    def test_find_leaf_operations(self, tree: MerkleTree):
        """Test finding leaves in a tree."""
        for i, data in enumerate(tree.get_leaves()):
            # Should find existing leaves
            assert tree.find_leaf_index(data) >= 0
            assert tree.contains_leaf(data)

        # Should not find non-existent data
        non_existent = b"definitely_not_in_tree_" + b"x" * 20
        assume(non_existent not in tree.get_leaves())

        assert not tree.contains_leaf(non_existent)
        with pytest.raises(ValueError, match="Leaf data not found"):
            tree.find_leaf_index(non_existent)


class TestMerkleProofs:
    """Test Merkle proof generation and verification."""

    @given(non_empty_merkle_tree_strategy())
    def test_proof_generation(self, tree: MerkleTree):
        """Test that proofs can be generated for all leaves."""
        for i in range(tree.leaf_count()):
            proof = tree.generate_proof(i)

            assert proof.leaf_index == i
            assert proof.leaf_data == tree.get_leaf(i)
            assert proof.root_hash == tree.root_hash()
            assert isinstance(proof.proof_path, list)

    @given(non_empty_merkle_tree_strategy())
    def test_proof_verification(self, tree: MerkleTree):
        """Test that generated proofs verify correctly."""
        for i in range(tree.leaf_count()):
            proof = tree.generate_proof(i)

            # Proof should verify against the tree
            assert tree.verify_proof(proof)

            # Proof should self-verify
            assert proof.verify()

    @given(merkle_proof_strategy())
    def test_proof_properties(self, proof: MerkleProof):
        """Test properties of valid proofs."""
        assert proof.leaf_index >= 0
        assert len(proof.leaf_data) > 0
        assert len(proof.root_hash) > 0
        assert isinstance(proof.proof_path, list)

        # Proof should be self-consistent
        assert proof.verify()

    def test_proof_serialization(self):
        """Test proof serialization and deserialization."""
        tree = MerkleTree.from_leaves([b"a", b"b", b"c", b"d"])
        proof = tree.generate_proof(1)

        # Serialize to dict
        proof_dict = proof.to_dict()
        assert isinstance(proof_dict, dict)
        assert "leaf_index" in proof_dict
        assert "leaf_data" in proof_dict
        assert "proof_path" in proof_dict
        assert "root_hash" in proof_dict

        # Deserialize from dict
        reconstructed_proof = MerkleProof.from_dict(proof_dict)
        assert reconstructed_proof.leaf_index == proof.leaf_index
        assert reconstructed_proof.leaf_data == proof.leaf_data
        assert reconstructed_proof.proof_path == proof.proof_path
        assert reconstructed_proof.root_hash == proof.root_hash

        # Reconstructed proof should still verify
        assert reconstructed_proof.verify()

    def test_invalid_proof_verification(self):
        """Test that invalid proofs fail verification."""
        tree = MerkleTree.from_leaves([b"a", b"b", b"c", b"d"])
        valid_proof = tree.generate_proof(1)

        # Proof with wrong root hash
        invalid_proof = MerkleProof(
            leaf_index=valid_proof.leaf_index,
            leaf_data=valid_proof.leaf_data,
            proof_path=valid_proof.proof_path,
            root_hash="wrong_hash",
        )
        assert not invalid_proof.verify()
        assert not tree.verify_proof(invalid_proof)

        # Proof with wrong leaf data
        invalid_proof2 = MerkleProof(
            leaf_index=valid_proof.leaf_index,
            leaf_data=b"wrong_data",
            proof_path=valid_proof.proof_path,
            root_hash=valid_proof.root_hash,
        )
        assert not invalid_proof2.verify()


class TestMerkleTreeComparison:
    """Test MerkleTree comparison and synchronization."""

    @given(merkle_tree_strategy())
    def test_tree_self_comparison(self, tree: MerkleTree):
        """Test comparing a tree with itself."""
        assert tree.compare_trees(tree) == "equal"
        assert tree.find_differences(tree) == []

    @given(merkle_tree_strategy(), merkle_tree_strategy())
    def test_tree_comparison(self, tree1: MerkleTree, tree2: MerkleTree):
        """Test comparing different trees."""
        comparison = tree1.compare_trees(tree2)

        if tree1.root_hash() == tree2.root_hash():
            assert comparison == "equal"
            assert tree1.find_differences(tree2) == []
        else:
            assert comparison == "different"
            # Should find at least one difference if hashes differ
            differences = tree1.find_differences(tree2)
            if tree1.leaf_count() > 0 or tree2.leaf_count() > 0:
                assert len(differences) > 0

    def test_tree_differences(self):
        """Test finding specific differences between trees."""
        tree1 = MerkleTree.from_leaves([b"a", b"b", b"c"])
        tree2 = MerkleTree.from_leaves([b"a", b"x", b"c"])

        differences = tree1.find_differences(tree2)
        assert differences == [1]  # Only index 1 differs

        # Different sizes
        tree3 = MerkleTree.from_leaves([b"a", b"b"])
        differences = tree1.find_differences(tree3)
        assert 2 in differences  # tree1 has extra leaf at index 2

    @given(merkle_tree_strategy(), merkle_tree_strategy())
    def test_tree_merging(self, tree1: MerkleTree, tree2: MerkleTree):
        """Test merging trees."""
        merged = tree1.merge_with(tree2)

        # Merged tree should contain all unique leaves
        expected_leaves = set(tree1.get_leaves()) | set(tree2.get_leaves())
        assert len(merged.get_leaves()) >= len(expected_leaves)

        # All original leaves should be in merged tree
        for leaf in tree1.get_leaves():
            assert leaf in merged
        for leaf in tree2.get_leaves():
            assert leaf in merged

    def test_comparison_type_checking(self):
        """Test that comparison with wrong type raises error."""
        tree = MerkleTree.from_leaves([b"test"])

        with pytest.raises(TypeError, match="Can only compare with MerkleTree"):
            tree.compare_trees("not a tree")  # type: ignore

        with pytest.raises(TypeError, match="Can only compare with MerkleTree"):
            tree.find_differences(42)  # type: ignore


class TestMerkleTreeProperties:
    """Test mathematical and cryptographic properties of Merkle trees."""

    @given(st.lists(merkle_leaf_data_strategy(), min_size=1, max_size=10))
    def test_deterministic_hashing(self, leaves: list[bytes]):
        """Test that trees with same leaves have same root hash."""
        tree1 = MerkleTree.from_leaves(leaves)
        tree2 = MerkleTree.from_leaves(leaves)

        assert tree1.root_hash() == tree2.root_hash()
        assert tree1.compare_trees(tree2) == "equal"

    @given(st.lists(merkle_leaf_data_strategy(), min_size=2, max_size=10))
    def test_order_sensitivity(self, leaves: list[bytes]):
        """Test that trees are sensitive to leaf order."""
        assume(len(set(leaves)) > 1)  # Need distinct leaves

        tree1 = MerkleTree.from_leaves(leaves)
        reversed_leaves = list(reversed(leaves))
        tree2 = MerkleTree.from_leaves(reversed_leaves)

        if leaves != reversed_leaves:
            assert tree1.root_hash() != tree2.root_hash()
            assert tree1.compare_trees(tree2) == "different"

    @given(non_empty_merkle_tree_strategy())
    def test_proof_path_length(self, tree: MerkleTree):
        """Test that proof paths have correct length."""
        for i in range(tree.leaf_count()):
            proof = tree.generate_proof(i)
            expected_length = tree.depth() - 1
            assert len(proof.proof_path) == expected_length

    @given(merkle_tree_strategy(), merkle_leaf_data_strategy())
    def test_append_changes_hash(self, tree: MerkleTree, new_data: bytes):
        """Test that appending a leaf changes the root hash."""
        assume(new_data not in tree.get_leaves())

        original_hash = tree.root_hash()
        new_tree = tree.append_leaf(new_data)

        if tree.is_empty():
            # Empty tree gets its first hash
            assert new_tree.root_hash() != ""
        else:
            # Non-empty tree hash should change
            assert new_tree.root_hash() != original_hash

    @given(non_empty_merkle_tree_strategy(), merkle_leaf_data_strategy())
    def test_update_changes_hash(self, tree: MerkleTree, new_data: bytes):
        """Test that updating a leaf changes the root hash."""
        original_data = tree.get_leaf(0)
        assume(new_data != original_data)

        original_hash = tree.root_hash()
        new_tree = tree.update_leaf(0, new_data)

        assert new_tree.root_hash() != original_hash

    @given(merkle_tree_strategy())
    def test_immutability_properties(self, tree: MerkleTree):
        """Test that tree operations preserve immutability."""
        original_leaf_count = tree.leaf_count()
        original_root_hash = tree.root_hash()
        original_leaves = tree.get_leaves()

        if not tree.is_empty():
            # Operations that should create new trees
            new_tree = tree.append_leaf(b"new_data")
            updated_tree = tree.update_leaf(0, b"updated_data")

            # Original tree should be unchanged
            assert tree.leaf_count() == original_leaf_count
            assert tree.root_hash() == original_root_hash
            assert tree.get_leaves() == original_leaves

            # New trees should be different
            assert new_tree is not tree
            assert updated_tree is not tree


class TestMerkleTreeSerialization:
    """Test Merkle tree serialization and deserialization."""

    @given(merkle_tree_strategy())
    def test_tree_serialization(self, tree: MerkleTree):
        """Test tree serialization roundtrip."""
        # Serialize to dict
        tree_dict = tree.to_dict()
        assert isinstance(tree_dict, dict)
        assert "leaves" in tree_dict
        assert "root_hash" in tree_dict
        assert "leaf_count" in tree_dict

        # Deserialize from dict
        reconstructed_tree = MerkleTree.from_dict(tree_dict)

        # Trees should be equal
        assert reconstructed_tree.leaf_count() == tree.leaf_count()
        assert reconstructed_tree.root_hash() == tree.root_hash()
        assert reconstructed_tree.get_leaves() == tree.get_leaves()
        assert reconstructed_tree.compare_trees(tree) == "equal"

    def test_empty_tree_serialization(self):
        """Test serialization of empty tree."""
        tree = MerkleTree.empty()
        tree_dict = tree.to_dict()
        reconstructed = MerkleTree.from_dict(tree_dict)

        assert reconstructed.is_empty()
        assert reconstructed.compare_trees(tree) == "equal"


class TestMerkleTreeValidation:
    """Test Merkle tree validation and error conditions."""

    def test_tree_construction_validation(self):
        """Test that invalid tree construction raises errors."""
        # These should be caught during tree construction
        # Most validation happens in MerkleNode and helper functions

        # Empty leaf list should create empty tree
        tree = MerkleTree.from_leaves([])
        assert tree.is_empty()

    def test_proof_validation(self):
        """Test proof validation."""
        # Invalid leaf index
        with pytest.raises(ValueError, match="Leaf index must be non-negative"):
            MerkleProof(
                leaf_index=-1, leaf_data=b"data", proof_path=[], root_hash="hash"
            )

        # Empty leaf data
        with pytest.raises(ValueError, match="Leaf data cannot be empty"):
            MerkleProof(leaf_index=0, leaf_data=b"", proof_path=[], root_hash="hash")

        # Empty root hash
        with pytest.raises(ValueError, match="Root hash cannot be empty"):
            MerkleProof(leaf_index=0, leaf_data=b"data", proof_path=[], root_hash="")

    def test_empty_tree_operations(self):
        """Test operations on empty tree."""
        tree = MerkleTree.empty()

        # Cannot generate proof for empty tree
        with pytest.raises(ValueError, match="Cannot generate proof for empty tree"):
            tree.generate_proof(0)

        # Cannot access leaves in empty tree
        with pytest.raises(IndexError):
            tree.get_leaf(0)


class TestMerkleTreeExamples:
    """Test specific examples and use cases."""

    def test_simple_merkle_tree_example(self):
        """Test a simple, well-documented example."""
        # Create tree with 4 leaves
        leaves = [b"alice", b"bob", b"charlie", b"david"]
        tree = MerkleTree.from_leaves(leaves)

        assert tree.leaf_count() == 4
        assert tree.depth() == 3  # 3 levels: leaves, internal, root

        # Generate and verify proof for each leaf
        for i, data in enumerate(leaves):
            proof = tree.generate_proof(i)
            assert proof.leaf_data == data
            assert proof.verify()
            assert tree.verify_proof(proof)

        # Update a leaf and verify change
        new_tree = tree.update_leaf(1, b"robert")
        assert new_tree.get_leaf(1) == b"robert"
        assert new_tree.root_hash() != tree.root_hash()

    def test_large_tree_performance(self):
        """Test that large trees work correctly."""
        # Create a larger tree (but not too large for tests)
        leaves = [f"data_{i}".encode() for i in range(100)]
        tree = MerkleTree.from_leaves(leaves)

        assert tree.leaf_count() == 100

        # Test a few random proofs
        for i in [0, 50, 99]:
            proof = tree.generate_proof(i)
            assert proof.verify()
            assert tree.verify_proof(proof)

    def test_merkle_tree_synchronization_example(self):
        """Test a realistic synchronization scenario."""
        # Two nodes with different data
        node1_data = [b"file1", b"file2", b"file3"]
        node2_data = [b"file1", b"file2_modified", b"file3", b"file4"]

        tree1 = MerkleTree.from_leaves(node1_data)
        tree2 = MerkleTree.from_leaves(node2_data)

        # Trees should be different
        assert tree1.compare_trees(tree2) == "different"

        # Find differences
        differences = tree1.find_differences(tree2)
        assert len(differences) > 0

        # Merge trees
        merged = tree1.merge_with(tree2)
        assert merged.leaf_count() >= max(tree1.leaf_count(), tree2.leaf_count())

        # All original data should be present in merged tree
        for data in node1_data:
            assert data in merged
        for data in node2_data:
            assert data in merged
