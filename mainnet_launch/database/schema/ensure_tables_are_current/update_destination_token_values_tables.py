import pandas as pd
from multicall import Call
import numpy as np
from web3 import Web3


from mainnet_launch.database.schema.full import (
    DestinationStates,
    DestinationTokenValues,
    AutopoolDestinationStates,
    Autopools,
    DestinationTokens,
    Destinations,
    Tokens,
    TokenValues,
)


from mainnet_launch.abis import STATS_CALCULATOR_REGISTRY_ABI
from mainnet_launch.data_fetching.get_events import fetch_events


from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
    identity_with_bool_success,
    get_state_by_one_block,
)
from mainnet_launch.constants import ALL_CHAINS, ROOT_PRICE_ORACLE, ChainData, STATS_CALCULATOR_REGISTRY, WETH


def ensure_destination_token_values_are_current():
    for chain in ALL_CHAINS:
        possible_blocks = build_blocks_to_use(chain)

        missing_blocks = get_subset_not_already_in_column(
            DestinationTokenValues,
            DestinationTokenValues.block,
            possible_blocks,
            where_clause=DestinationTokenValues.chain_id == chain.chain_id,
        )

        all_destination_tokens_orm: list[DestinationTokens] = get_full_table_as_orm(
            DestinationTokens, where_clause=DestinationTokens.chain_id == chain.chain_id
        )

        # getSpotPriceInETH(token_address, pool)
        ROOT_PRICE_ORACLE

        print(all_destination_tokens_orm[0])

        return


if __name__ == "__main__":
    ensure_destination_token_values_are_current()
