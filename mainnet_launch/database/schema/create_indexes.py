"""
Create performance indexes for the schema.

Usage:
  python create_indexes.py

Notes:
- Uses CREATE INDEX CONCURRENTLY for Postgres to avoid long blocking.
- CONCURRENTLY cannot run inside a transaction; we use AUTOCOMMIT.
- Index creation is idempotent: we check pg_indexes first.
"""

# not verified to work

from __future__ import annotations

import os
from urllib.parse import urlparse

from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect

load_dotenv()

# tmp = urlparse(os.getenv("LOCAL_MAIN_FORK_DATABASE_URL"))

ENGINE = create_engine(
    f"postgresql+psycopg2://{tmp.username}:{tmp.password}@{tmp.hostname}{tmp.path}?sslmode=require",
    echo=False,
    pool_pre_ping=True,
    pool_timeout=30,
    pool_size=5,
    max_overflow=0,
)


# ---- helpers ----


def _existing_index_names(schema: str = "public") -> set[str]:
    """
    Return a set of existing index names in the given schema.
    """
    with ENGINE.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = :schema
                """
            ),
            {"schema": schema},
        ).fetchall()
    return {r[0] for r in rows}


def _create_index(
    *,
    name: str,
    ddl: str,
    schema: str = "public",
) -> None:
    """
    Create an index if it does not already exist.
    Uses AUTOCOMMIT so CREATE INDEX CONCURRENTLY is allowed.
    """
    existing = _existing_index_names(schema=schema)
    if name in existing:
        print(f"OK (exists): {name}")
        return

    # Ensure we are not in a transaction block
    with ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        print(f"Creating: {name}")
        conn.execute(text(ddl))
        print(f"OK (created): {name}")


def create_indexes(schema: str = "public", concurrently: bool = True) -> None:
    """
    Create the recommended indexes.

    concurrently=True uses CREATE INDEX CONCURRENTLY (Postgres only).
    """
    # If you ever point this at a non-Postgres DB, fail loudly.
    dialect = ENGINE.dialect.name
    if dialect != "postgresql":
        raise RuntimeError(f"This script is intended for PostgreSQL, got dialect={dialect}")

    c = "CONCURRENTLY " if concurrently else ""

    # ---- Core join/filter indexes ----
    #
    # blocks is PK (block, chain_id) already, so it is indexed.
    # Most speed wins are on the *child* tables used in joins + time filters.

    index_ddls: list[tuple[str, str]] = [
        # Generic: accelerate joins/time-range filters by (chain_id, block)
        (
            "ix_transactions_chain_block",
            f"CREATE INDEX {c}ix_transactions_chain_block ON {schema}.transactions (chain_id, block)",
        ),
        (
            "ix_autopool_states_chain_block",
            f"CREATE INDEX {c}ix_autopool_states_chain_block ON {schema}.autopool_states (chain_id, block)",
        ),
        (
            "ix_destination_states_chain_block",
            f"CREATE INDEX {c}ix_destination_states_chain_block ON {schema}.destination_states (chain_id, block)",
        ),
        (
            "ix_autopool_destination_states_chain_block",
            f"CREATE INDEX {c}ix_autopool_destination_states_chain_block ON {schema}.autopool_destination_states (chain_id, block)",
        ),
        (
            "ix_token_values_chain_block",
            f"CREATE INDEX {c}ix_token_values_chain_block ON {schema}.token_values (chain_id, block)",
        ),
        (
            "ix_destination_token_values_chain_block",
            f"CREATE INDEX {c}ix_destination_token_values_chain_block ON {schema}.destination_token_values (chain_id, block)",
        ),
        # Hot-table composites for common access patterns
        (
            "ix_autopool_states_pool_chain_block",
            f"CREATE INDEX {c}ix_autopool_states_pool_chain_block ON {schema}.autopool_states (autopool_vault_address, chain_id, block)",
        ),
        (
            "ix_destination_states_dest_chain_block",
            f"CREATE INDEX {c}ix_destination_states_dest_chain_block ON {schema}.destination_states (destination_vault_address, chain_id, block)",
        ),
        (
            "ix_autopool_destination_states_pool_chain_block",
            f"CREATE INDEX {c}ix_autopool_destination_states_pool_chain_block ON {schema}.autopool_destination_states (autopool_vault_address, chain_id, block)",
        ),
        (
            "ix_autopool_destination_states_dest_chain_block",
            f"CREATE INDEX {c}ix_autopool_destination_states_dest_chain_block ON {schema}.autopool_destination_states (destination_vault_address, chain_id, block)",
        ),
        # Many-to-many lookups
        (
            "ix_autopool_destinations_pool_chain",
            f"CREATE INDEX {c}ix_autopool_destinations_pool_chain ON {schema}.autopool_destinations (autopool_vault_address, chain_id)",
        ),
        (
            "ix_autopool_destinations_dest_chain",
            f"CREATE INDEX {c}ix_autopool_destinations_dest_chain ON {schema}.autopool_destinations (destination_vault_address, chain_id)",
        ),
        (
            "ix_destination_tokens_dest_chain",
            f"CREATE INDEX {c}ix_destination_tokens_dest_chain ON {schema}.destination_tokens (destination_vault_address, chain_id)",
        ),
        (
            "ix_destination_tokens_token_chain",
            f"CREATE INDEX {c}ix_destination_tokens_token_chain ON {schema}.destination_tokens (token_address, chain_id)",
        ),
        # Event-style tables: common join is by tx_hash (+ chain_id)
        # Note: transactions.tx_hash is PK already, so that's indexed.
        (
            "ix_rebalance_events_chain_tx",
            f"CREATE INDEX {c}ix_rebalance_events_chain_tx ON {schema}.rebalance_events (chain_id, tx_hash)",
        ),
        (
            "ix_incentive_token_swapped_chain_tx",
            f"CREATE INDEX {c}ix_incentive_token_swapped_chain_tx ON {schema}.incentive_token_swapped (chain_id, tx_hash)",
        ),
        (
            "ix_incentive_token_balance_updated_chain_tx",
            f"CREATE INDEX {c}ix_incentive_token_balance_updated_chain_tx ON {schema}.incentive_token_balance_updated (chain_id, tx_hash)",
        ),
        (
            "ix_autopool_fees_chain_tx",
            f"CREATE INDEX {c}ix_autopool_fees_chain_tx ON {schema}.autopool_fees (chain_id, tx_hash)",
        ),
        (
            "ix_autopool_transfers_chain_tx",
            f"CREATE INDEX {c}ix_autopool_transfers_chain_tx ON {schema}.autopool_transfers (chain_id, tx_hash)",
        ),
        (
            "ix_autopool_deposits_chain_tx",
            f"CREATE INDEX {c}ix_autopool_deposits_chain_tx ON {schema}.autopool_deposits (chain_id, tx_hash)",
        ),
        (
            "ix_autopool_withdrawals_chain_tx",
            f"CREATE INDEX {c}ix_autopool_withdrawals_chain_tx ON {schema}.autopool_withdrawals (chain_id, tx_hash)",
        ),
        # Underlying deposit/withdraw: often joined by destination and filtered by chain
        (
            "ix_destination_underlying_deposited_dest_chain",
            f"CREATE INDEX {c}ix_destination_underlying_deposited_dest_chain ON {schema}.destination_underlying_deposited (destination_vault_address, chain_id)",
        ),
        (
            "ix_destination_underlying_withdraw_dest_chain",
            f"CREATE INDEX {c}ix_destination_underlying_withdraw_dest_chain ON {schema}.destination_underlying_withdraw (destination_vault_address, chain_id)",
        ),
    ]

    for name, ddl in index_ddls:
        _create_index(name=name, ddl=ddl, schema=schema)


if __name__ == "__main__":
    create_indexes(schema="public", concurrently=True)
