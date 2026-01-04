"""Tests for sqlite-backed blockchain persistence."""

from __future__ import annotations

import pytest

from mpreg.core.blockchain_ledger import BlockchainLedger
from mpreg.datastructures.block import Block
from mpreg.datastructures.blockchain import Blockchain
from mpreg.datastructures.blockchain_store import BlockchainStore
from mpreg.datastructures.transaction import Transaction


def test_blockchain_store_roundtrip(tmp_path):
    db_path = tmp_path / "chain.db"
    store = BlockchainStore(str(db_path))

    chain = Blockchain.create_new_chain("persist_chain", "miner1")
    tx = Transaction(sender="alice", receiver="bob", fee=1)
    block = Block.create_next_block(chain.genesis_block, (tx,), "miner1")
    chain = chain.add_block(block)

    store.save_chain(chain)
    loaded = store.load_chain("persist_chain")

    assert loaded.validate_chain()
    assert loaded.get_chain_summary() == chain.get_chain_summary()


def test_blockchain_ledger_persists_append(tmp_path):
    db_path = tmp_path / "ledger.db"
    store = BlockchainStore(str(db_path))

    chain = Blockchain.create_new_chain("ledger_chain", "miner1")
    ledger = BlockchainLedger(chain, store=store)

    tx = Transaction(sender="alice", receiver="bob", fee=1)
    block = Block.create_next_block(
        ledger.blockchain.get_latest_block(), (tx,), "miner1"
    )
    ledger.add_block(block)

    reloaded = store.load_chain("ledger_chain")
    assert reloaded.get_height() == 1


def test_blockchain_store_rejects_bad_height(tmp_path):
    db_path = tmp_path / "bad_height.db"
    store = BlockchainStore(str(db_path))

    chain = Blockchain.create_new_chain("height_chain", "miner1")
    store.save_chain(chain)

    bad_block = Block.create_next_block(chain.genesis_block, (), "miner1")
    bad_block = Block(
        height=2,
        previous_hash=bad_block.previous_hash,
        merkle_root=bad_block.merkle_root,
        vector_clock=bad_block.vector_clock,
        transactions=bad_block.transactions,
        miner=bad_block.miner,
        difficulty=bad_block.difficulty,
        nonce=bad_block.nonce,
        timestamp=bad_block.timestamp,
    )

    with pytest.raises(ValueError, match="Block height mismatch"):
        store.append_block("height_chain", bad_block)
