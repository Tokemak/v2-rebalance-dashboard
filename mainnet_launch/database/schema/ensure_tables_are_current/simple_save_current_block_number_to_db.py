"""Minimal example to verify that we can connect alchemy, reads and writes to the database

poetry run python mainnet_launch/database/schema/ensure_tables_are_current/simple_save_current_block_number_to_db.py
"""

from mainnet_launch.constants import *
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_blocks import (
    ensure_all_blocks_are_in_table,
)

from mainnet_launch.database.postgres_operations import get_highest_value_in_field_where
from mainnet_launch.database.schema.full import Blocks


def save_and_verify_top_blocks():
    """Helpful small script to verify that the database, multicall and chains are working together properly."""
    for chain in ALL_CHAINS:
        top_block = chain.get_block_near_top()
        ensure_all_blocks_are_in_table([top_block], chain)
        found_top_block = get_highest_value_in_field_where(Blocks, Blocks.block, Blocks.chain_id == chain.chain_id)
        print(f"Chain {chain.name} top block {top_block}, found in DB {found_top_block}")
        assert top_block == found_top_block, f"Top block {top_block} not found in DB, found {found_top_block} instead"


if __name__ == "__main__":
    save_and_verify_top_blocks()
