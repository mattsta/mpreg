"""SQLite-backed persistence for blockchain data."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from .block import Block
from .blockchain import Blockchain
from .blockchain_types import ConsensusConfig, ConsensusType, CryptoConfig


@dataclass(slots=True)
class BlockchainStore:
    """Persist and load blockchains from sqlite."""

    db_path: str

    def __post_init__(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chains (
                    chain_id TEXT PRIMARY KEY,
                    consensus_config TEXT NOT NULL,
                    crypto_config TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS blocks (
                    chain_id TEXT NOT NULL,
                    height INTEGER NOT NULL,
                    block_hash TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    block_json TEXT NOT NULL,
                    PRIMARY KEY (chain_id, height),
                    FOREIGN KEY (chain_id) REFERENCES chains(chain_id) ON DELETE CASCADE
                )
                """
            )

    def has_chain(self, chain_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM chains WHERE chain_id = ? LIMIT 1", (chain_id,)
            ).fetchone()
            return row is not None

    def save_chain(self, blockchain: Blockchain) -> None:
        payload = blockchain.to_dict()
        consensus_config = json.dumps(payload["consensus_config"])
        crypto_config = json.dumps(payload.get("crypto_config", {}))
        created_at = blockchain.genesis_block.timestamp

        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO chains (chain_id, consensus_config, crypto_config, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (blockchain.chain_id, consensus_config, crypto_config, created_at),
            )
            conn.execute(
                "DELETE FROM blocks WHERE chain_id = ?", (blockchain.chain_id,)
            )

            all_blocks = [blockchain.genesis_block] + list(blockchain.blocks)
            for block in all_blocks:
                block_json = json.dumps(block.to_dict())
                conn.execute(
                    """
                    INSERT INTO blocks (chain_id, height, block_hash, timestamp, block_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        blockchain.chain_id,
                        block.height,
                        block.get_block_hash(),
                        block.timestamp,
                        block_json,
                    ),
                )

    def append_block(self, chain_id: str, block: Block) -> None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT MAX(height) FROM blocks WHERE chain_id = ?",
                (chain_id,),
            ).fetchone()
            expected_height = 0 if row[0] is None else row[0] + 1
            if block.height != expected_height:
                raise ValueError(
                    f"Block height mismatch: expected {expected_height}, got {block.height}"
                )
            if not self.has_chain(chain_id):
                consensus_config = json.dumps(
                    {
                        "consensus_type": ConsensusType.PROOF_OF_AUTHORITY.value,
                        "block_time_target": 10,
                        "difficulty_adjustment_interval": 100,
                        "max_transactions_per_block": 1000,
                        "minimum_stake": 0,
                        "authority_threshold": 0.67,
                    }
                )
                crypto_config = json.dumps(
                    {
                        "hash_algorithm": "sha256",
                        "signature_algorithm": "ed25519",
                        "key_length": 256,
                        "require_signatures": False,
                    }
                )
                conn.execute(
                    """
                    INSERT INTO chains (chain_id, consensus_config, crypto_config, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (chain_id, consensus_config, crypto_config, time.time()),
                )
            block_json = json.dumps(block.to_dict())
            conn.execute(
                """
                INSERT INTO blocks (chain_id, height, block_hash, timestamp, block_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    chain_id,
                    block.height,
                    block.get_block_hash(),
                    block.timestamp,
                    block_json,
                ),
            )

    def load_chain(self, chain_id: str) -> Blockchain:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT consensus_config, crypto_config FROM chains WHERE chain_id = ?",
                (chain_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Chain {chain_id} not found")
            consensus_payload = json.loads(row[0])
            crypto_payload = json.loads(row[1])

            consensus_config = ConsensusConfig(
                consensus_type=ConsensusType(consensus_payload["consensus_type"]),
                block_time_target=consensus_payload["block_time_target"],
                difficulty_adjustment_interval=consensus_payload[
                    "difficulty_adjustment_interval"
                ],
                max_transactions_per_block=consensus_payload[
                    "max_transactions_per_block"
                ],
                minimum_stake=consensus_payload.get("minimum_stake", 0),
                authority_threshold=consensus_payload.get("authority_threshold", 0.67),
            )
            crypto_config = CryptoConfig(
                hash_algorithm=crypto_payload.get("hash_algorithm", "sha256"),
                signature_algorithm=crypto_payload.get(
                    "signature_algorithm", "ed25519"
                ),
                key_length=crypto_payload.get("key_length", 256),
                require_signatures=crypto_payload.get("require_signatures", False),
            )

            block_rows = conn.execute(
                """
                SELECT block_json FROM blocks
                WHERE chain_id = ?
                ORDER BY height ASC
                """,
                (chain_id,),
            ).fetchall()
            if not block_rows:
                raise ValueError(f"Chain {chain_id} has no blocks")
            blocks = [Block.from_dict(json.loads(row[0])) for row in block_rows]
            genesis_block = blocks[0]
            return Blockchain(
                chain_id=chain_id,
                genesis_block=genesis_block,
                blocks=tuple(blocks[1:]),
                consensus_config=consensus_config,
                crypto_config=crypto_config,
            )

    def validate_chain(self, chain_id: str) -> bool:
        chain = self.load_chain(chain_id)
        return chain.validate_chain()
