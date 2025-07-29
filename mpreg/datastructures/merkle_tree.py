"""
Centralized Merkle Tree implementation for MPREG.

This module provides a unified, immutable Merkle tree implementation for cryptographic
data integrity verification and efficient synchronization in distributed systems.

Key Features:
- Immutable design for thread safety and correctness
- Efficient binary tree structure with cryptographic hashes
- Merkle proof generation and verification
- Support for dynamic leaf insertion and updates
- Comprehensive comparison and synchronization methods
- Well-encapsulated with semantic type aliases
- Property-based testing support

Merkle trees are commonly used with vector clocks for:
- Efficient data synchronization between distributed nodes
- Cryptographic verification of data integrity
- Bandwidth-efficient diff computation
- Tamper-evident data structures
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterator
from dataclasses import dataclass, field

from hypothesis import strategies as st

from .type_aliases import (
    JsonDict,
    MerkleHash,
    MerkleLeafData,
    MerkleLeafIndex,
    MerkleProofPath,
    MerkleTreeDepth,
)


@dataclass(frozen=True, slots=True)
class MerkleNode:
    """Single node in a Merkle tree."""

    hash_value: MerkleHash
    left_child: MerkleNode | None = None
    right_child: MerkleNode | None = None
    leaf_data: MerkleLeafData | None = None

    def __post_init__(self) -> None:
        # Validate node consistency
        has_leaf_data = self.leaf_data is not None
        has_left = self.left_child is not None
        has_right = self.right_child is not None
        has_both_children = has_left and has_right
        has_any_children = has_left or has_right

        if has_leaf_data and has_any_children:
            # Node has leaf data but also children - this is ambiguous
            # If it has both children, treat as "internal with leaf data" error
            # If it has only one child, treat as "leaf with children" error
            if has_both_children:
                raise ValueError("Internal nodes cannot have leaf data")
            else:
                raise ValueError("Leaf nodes cannot have children")
        elif has_leaf_data and not has_any_children:
            # Valid leaf node
            pass
        elif not has_leaf_data and has_any_children:
            # Should be internal node - validate both children exist
            if not has_both_children:
                raise ValueError("Internal nodes must have both children")
        else:
            # No data and no children - invalid
            raise ValueError("Leaf nodes must have data")

    def is_leaf(self) -> bool:
        """Check if this node is a leaf."""
        return self.leaf_data is not None

    def is_internal(self) -> bool:
        """Check if this node is an internal node."""
        return not self.is_leaf()

    @classmethod
    def create_leaf(cls, data: MerkleLeafData) -> MerkleNode:
        """Create a leaf node with given data."""
        hash_value = _compute_leaf_hash(data)
        return cls(hash_value=hash_value, leaf_data=data)

    @classmethod
    def create_internal(cls, left: MerkleNode, right: MerkleNode) -> MerkleNode:
        """Create an internal node from two children."""
        hash_value = _compute_internal_hash(left.hash_value, right.hash_value)
        return cls(hash_value=hash_value, left_child=left, right_child=right)


@dataclass(frozen=True, slots=True)
class MerkleProof:
    """
    Merkle proof for verifying a leaf's inclusion in a tree.

    A Merkle proof allows efficient verification that a specific leaf
    is part of a Merkle tree without requiring the entire tree.
    """

    leaf_index: MerkleLeafIndex
    leaf_data: MerkleLeafData
    proof_path: MerkleProofPath
    root_hash: MerkleHash

    def __post_init__(self) -> None:
        if self.leaf_index < 0:
            raise ValueError("Leaf index must be non-negative")
        if not self.leaf_data:
            raise ValueError("Leaf data cannot be empty")
        if not self.root_hash:
            raise ValueError("Root hash cannot be empty")

    def verify(self) -> bool:
        """Verify that this proof is valid."""
        current_hash = _compute_leaf_hash(self.leaf_data)
        current_index = self.leaf_index

        for sibling_hash, is_right_sibling in self.proof_path:
            if is_right_sibling:
                # Sibling is on the right, we are on the left
                current_hash = _compute_internal_hash(current_hash, sibling_hash)
            else:
                # Sibling is on the left, we are on the right
                current_hash = _compute_internal_hash(sibling_hash, current_hash)

            current_index //= 2

        return current_hash == self.root_hash

    def to_dict(self) -> JsonDict:
        """Convert proof to dictionary for serialization."""
        return {
            "leaf_index": self.leaf_index,
            "leaf_data": self.leaf_data.hex(),
            "proof_path": [
                {"hash": hash_val, "is_right_sibling": is_right}
                for hash_val, is_right in self.proof_path
            ],
            "root_hash": self.root_hash,
        }

    @classmethod
    def from_dict(cls, data: JsonDict) -> MerkleProof:
        """Create proof from dictionary."""
        return cls(
            leaf_index=data["leaf_index"],
            leaf_data=bytes.fromhex(data["leaf_data"]),
            proof_path=[
                (item["hash"], item["is_right_sibling"]) for item in data["proof_path"]
            ],
            root_hash=data["root_hash"],
        )


@dataclass(frozen=True, slots=True)
class MerkleTree:
    """
    Immutable Merkle tree for cryptographic data integrity verification.

    A Merkle tree is a binary tree where:
    - Each leaf node contains a hash of some data
    - Each internal node contains a hash of its children's hashes
    - The root hash represents the entire dataset

    This implementation provides:
    - Immutable design for thread safety
    - Efficient proof generation and verification
    - Support for trees of any size (automatically pads to power of 2)
    - Comprehensive comparison and synchronization methods
    """

    _root: MerkleNode | None = None
    _leaves: tuple[MerkleLeafData, ...] = field(default_factory=tuple)
    _leaf_count: int = 0

    def __post_init__(self) -> None:
        if self._leaf_count < 0:
            raise ValueError("Leaf count cannot be negative")
        if self._leaf_count != len(self._leaves):
            raise ValueError("Leaf count must match number of leaves")
        if self._leaf_count > 0 and self._root is None:
            raise ValueError("Non-empty tree must have a root")
        if self._leaf_count == 0 and self._root is not None:
            raise ValueError("Empty tree cannot have a root")

    @classmethod
    def empty(cls) -> MerkleTree:
        """Create an empty Merkle tree."""
        return cls()

    @classmethod
    def from_leaves(cls, leaves: list[MerkleLeafData]) -> MerkleTree:
        """Create a Merkle tree from a list of leaf data."""
        if not leaves:
            return cls.empty()

        leaf_count = len(leaves)
        leaves_tuple = tuple(leaves)

        # Build the tree bottom-up
        root = _build_merkle_tree(leaves)

        return cls(_root=root, _leaves=leaves_tuple, _leaf_count=leaf_count)

    @classmethod
    def single_leaf(cls, data: MerkleLeafData) -> MerkleTree:
        """Create a Merkle tree with a single leaf."""
        return cls.from_leaves([data])

    def is_empty(self) -> bool:
        """Check if the tree is empty."""
        return self._leaf_count == 0

    def leaf_count(self) -> int:
        """Get the number of leaves in the tree."""
        return self._leaf_count

    def depth(self) -> MerkleTreeDepth:
        """Get the depth of the tree."""
        if self.is_empty():
            return 0
        return math.ceil(math.log2(self._leaf_count)) + 1

    def root_hash(self) -> MerkleHash:
        """Get the root hash of the tree."""
        if self._root is None:
            return ""
        return self._root.hash_value

    def get_leaf(self, index: MerkleLeafIndex) -> MerkleLeafData:
        """Get leaf data at the specified index."""
        if index < 0 or index >= self._leaf_count:
            raise IndexError(f"Leaf index {index} out of range [0, {self._leaf_count})")
        return self._leaves[index]

    def get_leaves(self) -> tuple[MerkleLeafData, ...]:
        """Get all leaf data."""
        return self._leaves

    def contains_leaf(self, data: MerkleLeafData) -> bool:
        """Check if the tree contains the specified leaf data."""
        return data in self._leaves

    def find_leaf_index(self, data: MerkleLeafData) -> MerkleLeafIndex:
        """Find the index of the specified leaf data."""
        try:
            return self._leaves.index(data)
        except ValueError:
            raise ValueError("Leaf data not found in tree")

    def append_leaf(self, data: MerkleLeafData) -> MerkleTree:
        """Create a new tree with an additional leaf."""
        new_leaves = list(self._leaves) + [data]
        return MerkleTree.from_leaves(new_leaves)

    def update_leaf(
        self, index: MerkleLeafIndex, new_data: MerkleLeafData
    ) -> MerkleTree:
        """Create a new tree with updated leaf at the specified index."""
        if index < 0 or index >= self._leaf_count:
            raise IndexError(f"Leaf index {index} out of range [0, {self._leaf_count})")

        new_leaves = list(self._leaves)
        new_leaves[index] = new_data
        return MerkleTree.from_leaves(new_leaves)

    def remove_leaf(self, index: MerkleLeafIndex) -> MerkleTree:
        """Create a new tree with the leaf at the specified index removed."""
        if index < 0 or index >= self._leaf_count:
            raise IndexError(f"Leaf index {index} out of range [0, {self._leaf_count})")

        new_leaves = list(self._leaves)
        del new_leaves[index]
        return MerkleTree.from_leaves(new_leaves)

    def generate_proof(self, index: MerkleLeafIndex) -> MerkleProof:
        """Generate a Merkle proof for the leaf at the specified index."""
        if self._root is None:
            raise ValueError("Cannot generate proof for empty tree")

        if index < 0 or index >= self._leaf_count:
            raise IndexError(f"Leaf index {index} out of range [0, {self._leaf_count})")

        leaf_data = self._leaves[index]
        # Use the padded tree size for proof generation
        padded_tree_size = 2 ** math.ceil(math.log2(self._leaf_count))
        proof_path = _generate_proof_path(self._root, index, padded_tree_size)

        return MerkleProof(
            leaf_index=index,
            leaf_data=leaf_data,
            proof_path=proof_path,
            root_hash=self.root_hash(),
        )

    def verify_proof(self, proof: MerkleProof) -> bool:
        """Verify a Merkle proof against this tree."""
        if proof.root_hash != self.root_hash():
            return False
        return proof.verify()

    def compare_trees(self, other: MerkleTree) -> str:
        """
        Compare this tree with another tree.

        Returns:
            'equal': Trees are identical
            'different': Trees have different content
        """
        if not isinstance(other, MerkleTree):
            raise TypeError(f"Can only compare with MerkleTree, got {type(other)}")

        if self.root_hash() == other.root_hash():
            return "equal"
        else:
            return "different"

    def find_differences(self, other: MerkleTree) -> list[MerkleLeafIndex]:
        """Find indices where this tree differs from another tree."""
        if not isinstance(other, MerkleTree):
            raise TypeError(f"Can only compare with MerkleTree, got {type(other)}")

        differences = []
        max_leaves = max(self._leaf_count, other._leaf_count)

        for i in range(max_leaves):
            self_data = self._leaves[i] if i < self._leaf_count else None
            other_data = other._leaves[i] if i < other._leaf_count else None

            if self_data != other_data:
                differences.append(i)

        return differences

    def merge_with(self, other: MerkleTree) -> MerkleTree:
        """Merge this tree with another tree, taking the union of all leaves."""
        if not isinstance(other, MerkleTree):
            raise TypeError(f"Can only merge with MerkleTree, got {type(other)}")

        # Simple merge: concatenate all unique leaves
        all_leaves = list(self._leaves)
        for leaf in other._leaves:
            if leaf not in all_leaves:
                all_leaves.append(leaf)

        return MerkleTree.from_leaves(all_leaves)

    def to_dict(self) -> JsonDict:
        """Convert tree to dictionary for serialization."""
        return {
            "leaves": [leaf.hex() for leaf in self._leaves],
            "root_hash": self.root_hash(),
            "leaf_count": self._leaf_count,
        }

    @classmethod
    def from_dict(cls, data: JsonDict) -> MerkleTree:
        """Create tree from dictionary."""
        leaves = [bytes.fromhex(leaf_hex) for leaf_hex in data["leaves"]]
        return cls.from_leaves(leaves)

    def __iter__(self) -> Iterator[MerkleLeafData]:
        """Iterate over leaf data."""
        return iter(self._leaves)

    def __len__(self) -> int:
        """Get number of leaves."""
        return self._leaf_count

    def __getitem__(self, index: MerkleLeafIndex) -> MerkleLeafData:
        """Get leaf data at index."""
        return self.get_leaf(index)

    def __contains__(self, data: MerkleLeafData) -> bool:
        """Check if leaf data is in tree."""
        return self.contains_leaf(data)

    def __repr__(self) -> str:
        """String representation of the tree."""
        if self.is_empty():
            return "MerkleTree(empty)"

        return f"MerkleTree(leaves={self._leaf_count}, root={self.root_hash()[:16]}...)"


# Helper functions for Merkle tree operations


def _compute_leaf_hash(data: MerkleLeafData) -> MerkleHash:
    """Compute hash for leaf node data."""
    return hashlib.sha256(b"leaf:" + data).hexdigest()


def _compute_internal_hash(left_hash: MerkleHash, right_hash: MerkleHash) -> MerkleHash:
    """Compute hash for internal node from children hashes."""
    combined = f"internal:{left_hash}:{right_hash}".encode()
    return hashlib.sha256(combined).hexdigest()


def _build_merkle_tree(leaves: list[MerkleLeafData]) -> MerkleNode:
    """Build a Merkle tree from leaf data."""
    if not leaves:
        raise ValueError("Cannot build tree from empty leaves")

    # Create leaf nodes
    nodes = [MerkleNode.create_leaf(data) for data in leaves]

    # Pad to next power of 2 if necessary
    target_size = 2 ** math.ceil(math.log2(len(nodes)))
    while len(nodes) < target_size:
        # Use a deterministic padding value that includes the padding index
        # This ensures trees with different leaf counts have different structures
        padding_data = f"__PADDING_{len(nodes)}__".encode()
        padding_node = MerkleNode.create_leaf(padding_data)
        nodes.append(padding_node)

    # Build tree bottom-up
    while len(nodes) > 1:
        next_level = []
        for i in range(0, len(nodes), 2):
            left = nodes[i]
            right = nodes[i + 1] if i + 1 < len(nodes) else nodes[i]
            parent = MerkleNode.create_internal(left, right)
            next_level.append(parent)
        nodes = next_level

    return nodes[0]


def _generate_proof_path(
    root: MerkleNode, target_index: MerkleLeafIndex, tree_size: int
) -> MerkleProofPath:
    """Generate proof path for a leaf at the given index."""
    proof_path: MerkleProofPath = []

    # tree_size is already the padded size (power of 2)
    current_index = target_index
    current_node = root

    # Traverse down the tree
    while current_node.is_internal():
        left_child = current_node.left_child
        right_child = current_node.right_child

        if left_child is None or right_child is None:
            raise ValueError("Internal node missing children")

        # Determine which subtree contains our target
        subtree_size = tree_size // 2

        if current_index < subtree_size:
            # Target is in left subtree, right child is sibling
            proof_path.append((right_child.hash_value, True))
            current_node = left_child
        else:
            # Target is in right subtree, left child is sibling
            proof_path.append((left_child.hash_value, False))
            current_node = right_child
            current_index -= subtree_size

        tree_size = subtree_size

    # Reverse the proof path since we built it top-down but verification works bottom-up
    return list(reversed(proof_path))


# Hypothesis strategies for property-based testing


def merkle_leaf_data_strategy() -> st.SearchStrategy[MerkleLeafData]:
    """Generate valid leaf data for testing."""
    return st.binary(min_size=1, max_size=100)


def merkle_tree_strategy(
    max_leaves: int = 10, min_leaves: int = 0
) -> st.SearchStrategy[MerkleTree]:
    """Generate valid MerkleTree instances for testing."""
    return st.lists(
        merkle_leaf_data_strategy(), min_size=min_leaves, max_size=max_leaves
    ).map(MerkleTree.from_leaves)


def non_empty_merkle_tree_strategy(
    max_leaves: int = 10,
) -> st.SearchStrategy[MerkleTree]:
    """Generate non-empty MerkleTree instances for testing."""
    return merkle_tree_strategy(max_leaves=max_leaves, min_leaves=1)


def merkle_proof_strategy(
    tree: MerkleTree | None = None,
) -> st.SearchStrategy[MerkleProof]:
    """Generate valid MerkleProof instances for testing."""
    if tree is None:
        # Generate a tree and proof together
        @st.composite
        def generate_tree_and_proof(draw):
            test_tree = draw(non_empty_merkle_tree_strategy())
            index = draw(st.integers(min_value=0, max_value=test_tree.leaf_count() - 1))
            return test_tree.generate_proof(index)

        return generate_tree_and_proof()
    else:
        # Generate proof for existing tree
        if tree.is_empty():
            raise ValueError("Cannot generate proof for empty tree")

        return st.integers(min_value=0, max_value=tree.leaf_count() - 1).map(
            tree.generate_proof
        )
