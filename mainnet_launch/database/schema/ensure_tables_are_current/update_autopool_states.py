# import pandas as pd
# from multicall import Call
# import numpy as np
# from web3 import Web3


# from mainnet_launch.database.schema.full import (
#     DestinationTokenValues,
#     TokenValues,
#     Autopools,
#     DestinationStates,
#     DestinationTokens,
#     Destinations,
#     AutopoolDestinationStates,
#     Tokens,
#     AutopoolStates
# )
# import plotly.express as px


# from mainnet_launch.abis import STATS_CALCULATOR_REGISTRY_ABI
# from mainnet_launch.data_fetching.get_events import fetch_events


# from mainnet_launch.database.schema.postgres_operations import (
#     get_full_table_as_orm,
#     get_full_table_as_df,
#     insert_avoid_conflicts,
#     get_subset_not_already_in_column,
#     natural_left_right_using_where,
# )
# from mainnet_launch.data_fetching.get_state_by_block import (
#     get_raw_state_by_blocks,
#     safe_normalize_with_bool_success,
#     build_blocks_to_use,
#     identity_with_bool_success,
#     get_state_by_one_block,
# )
# from mainnet_launch.constants import (
#     ALL_CHAINS,
#     ROOT_PRICE_ORACLE,
#     ChainData,
# )

# from mainnet_launch.pages.autopool_diagnostics.lens_contract import (
#     fetch_pools_and_destinations_df,
# )


# def build_autopool_state_calls(autopools:list[Autopools]) -> list[Call]:

#     # autopool total Shaes
#     # total_shares: Mapped[float] = mapped_column(nullable=False)  # vault.totalSupply
#     # total_nav: Mapped[float] = m_column(nullable=False)  # nav
#     # nav_per_share: Mapped[float] = mappedapped_column(nullable=False)  # nav per share
#     symbol_calls = [
#         Call(
#             t,
#             "symbol()(string)",
#             [(t + "_symbol", identity_with_bool_success)],
#         )
#         for t in autopools
#     ]

#     name_calls = [
#         Call(
#             t,
#             "name()(string)",
#             [(t + "_name", identity_with_bool_success)],
#         )
#         for t in token_addresses
#     ]


# def ensure_autopool_states_is_current():
#     for chain in ALL_CHAINS:
#         possible_blocks = build_blocks_to_use(chain)

#         missing_blocks = get_subset_not_already_in_column(
#             AutopoolDestinationStates,
#             AutopoolDestinationStates.block,
#             possible_blocks,
#             where_clause=AutopoolDestinationStates.chain_id == chain.chain_id,
#         )

#         if len(missing_blocks) == 0:
#             continue
