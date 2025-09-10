"""Minimal example to verify that we can connect alchemy, reads and writes to the database."""

from mainnet_launch.constants import ALL_CHAINS, profile_function
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_blocks import (
    ensure_all_blocks_are_in_table,
)

from mainnet_launch.database.postgres_operations import get_highest_value_in_field_where
from mainnet_launch.database.schema.full import Blocks


def save_and_verify_top_blocks():
    for chain in ALL_CHAINS:
        top_block = chain.get_block_near_top()
        ensure_all_blocks_are_in_table([top_block], chain)
        found_top_block = get_highest_value_in_field_where(Blocks, Blocks.block, Blocks.chain_id == chain.chain_id)
        print(f"Chain {chain.chain_id} top block {top_block}, found in DB {found_top_block}")
        assert top_block == found_top_block, f"Top block {top_block} not found in DB, found {found_top_block} instead"


if __name__ == "__main__":
    save_and_verify_top_blocks()
