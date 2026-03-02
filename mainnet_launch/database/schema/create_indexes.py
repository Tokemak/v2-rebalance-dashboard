"""Create and drop performance indexes.

Usage:
    poetry run create-indexes
    poetry run drop-indexes
    python create_indexes.py
    python create_indexes.py --drop

Uses CREATE INDEX CONCURRENTLY (no table locks, requires AUTOCOMMIT).
Idempotent — checks pg_indexes before creating.
"""

import sys

from sqlalchemy import text

from mainnet_launch.database.schema.full import ENGINE


def _existing_index_names(schema="public"):
    """Return the set of index names in the given schema."""
    with ENGINE.connect() as conn:
        rows = conn.execute(
            text("SELECT indexname FROM pg_indexes WHERE schemaname = :schema"),
            {"schema": schema},
        ).fetchall()
    return {r[0] for r in rows}


def _create_index(name, ddl, schema="public"):
    """Create an index if it doesn't already exist (AUTOCOMMIT for CONCURRENTLY)."""
    if name in _existing_index_names(schema):
        print(f"OK (exists): {name}")
        return
    with ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        print(f"Creating: {name}")
        conn.execute(text(ddl))
        print(f"OK (created): {name}")


def _drop_index(name, schema="public", concurrently=True):
    """Drop an index if it exists (AUTOCOMMIT for CONCURRENTLY)."""
    if name not in _existing_index_names(schema):
        print(f"OK (not present): {name}")
        return
    c = "CONCURRENTLY " if concurrently else ""
    with ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        print(f"Dropping: {name}")
        conn.execute(text(f"DROP INDEX {c}{schema}.{name}"))
        print(f"OK (dropped): {name}")


def _index_definitions(schema, concurrently):
    """Return (name, CREATE INDEX ddl) pairs for all indexes."""
    c = "CONCURRENTLY " if concurrently else ""

    return [
        # (chain_id, block) on child tables — accelerates joins + time-range filters
        # blocks PK is (block, chain_id) already
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
        # composite indexes for common access patterns (address, chain_id, block)
        # NOTE: ix_autopool_states_pool_chain_block omitted — identical to PK (autopool_vault_address, chain_id, block)
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
        # many-to-many lookups
        # NOTE: ix_autopool_destinations_dest_chain omitted — leftmost prefix of PK (dest, chain_id, autopool)
        # NOTE: ix_destination_tokens_dest_chain omitted — leftmost prefix of PK (dest, chain_id, token)
        (
            "ix_autopool_destinations_pool_chain",
            f"CREATE INDEX {c}ix_autopool_destinations_pool_chain ON {schema}.autopool_destinations (autopool_vault_address, chain_id)",
        ),
        (
            "ix_destination_tokens_token_chain",
            f"CREATE INDEX {c}ix_destination_tokens_token_chain ON {schema}.destination_tokens (token_address, chain_id)",
        ),
        # event tables — join on tx_hash (+ chain_id where it exists)
        # transactions.tx_hash is PK already
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
        # NOTE: ix_autopool_fees_tx omitted — leftmost prefix of PK (tx_hash, ...)
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
        # underlying deposit/withdraw — joined by destination
        (
            "ix_destination_underlying_deposited_dest",
            f"CREATE INDEX {c}ix_destination_underlying_deposited_dest ON {schema}.destination_underlying_deposited (destination_vault_address)",
        ),
        (
            "ix_destination_underlying_withdraw_dest",
            f"CREATE INDEX {c}ix_destination_underlying_withdraw_dest ON {schema}.destination_underlying_withdraw (destination_vault_address)",
        ),
        # blocks — every query filters on datetime
        (
            "ix_blocks_chain_datetime",
            f"CREATE INDEX {c}ix_blocks_chain_datetime ON {schema}.blocks (chain_id, datetime)",
        ),
        # destination_token_values — largest table, queries filter by destination
        (
            "ix_destination_token_values_dest_chain_block",
            f"CREATE INDEX {c}ix_destination_token_values_dest_chain_block ON {schema}.destination_token_values (destination_vault_address, chain_id, block)",
        ),
        # token_values — joined by token but only had (chain_id, block)
        (
            "ix_token_values_token_chain_block",
            f"CREATE INDEX {c}ix_token_values_token_chain_block ON {schema}.token_values (token_address, chain_id, block)",
        ),
    ]


def create_indexes(schema="public", concurrently=True):
    """Create all recommended indexes. Idempotent."""
    assert ENGINE.dialect.name == "postgresql", "Only works with PostgreSQL"
    for name, ddl in _index_definitions(schema, concurrently):
        _create_index(name, ddl, schema)


def drop_indexes(schema="public", concurrently=True):
    """Drop all indexes created by create_indexes()."""
    assert ENGINE.dialect.name == "postgresql", "Only works with PostgreSQL"
    for name, _ddl in _index_definitions(schema, concurrently):
        _drop_index(name, schema=schema, concurrently=concurrently)


if __name__ == "__main__":
    if "--drop" in sys.argv:
        drop_indexes(schema="public", concurrently=True)
    else:
        create_indexes(schema="public", concurrently=True)
