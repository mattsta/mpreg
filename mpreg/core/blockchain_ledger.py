"""Shared ledger wrapper for blockchain-backed services."""

from __future__ import annotations

from dataclasses import dataclass

from mpreg.datastructures.block import Block
from mpreg.datastructures.blockchain import Blockchain
from mpreg.datastructures.blockchain_store import BlockchainStore
from mpreg.datastructures.blockchain_types import Timestamp


@dataclass(slots=True)
class BlockchainLedger:
    """Mutable holder for the current blockchain state."""

    blockchain: Blockchain
    store: BlockchainStore | None = None

    def __post_init__(self) -> None:
        if self.store is not None:
            if self.store.has_chain(self.blockchain.chain_id):
                self.blockchain = self.store.load_chain(self.blockchain.chain_id)
            else:
                self.store.save_chain(self.blockchain)

    @classmethod
    def from_store(cls, store: BlockchainStore, chain_id: str) -> BlockchainLedger:
        return cls(blockchain=store.load_chain(chain_id), store=store)

    def add_block(
        self, block: Block, *, current_time: Timestamp | None = None
    ) -> Blockchain:
        self.blockchain = self.blockchain.add_block(block, current_time=current_time)
        if self.store is not None:
            self.store.append_block(self.blockchain.chain_id, block)
        return self.blockchain

    def replace(self, blockchain: Blockchain) -> None:
        if not blockchain.validate_chain():
            raise ValueError("Invalid blockchain")
        self.blockchain = blockchain
        if self.store is not None:
            self.store.save_chain(blockchain)
