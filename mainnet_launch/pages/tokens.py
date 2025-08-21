"""Update the Tokens table"""

import pandas as pd

import streamlit as st
import plotly.express as px
from mainnet_launch.constants.constants import (
    AutopoolConstants,
    STATS_CALCULATOR_REGISTRY,
    ALL_CHAINS,
    ROOT_PRICE_ORACLE,
    ChainData,
    ETH_CHAIN,
)
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
from mainnet_launch.abis import AUTOPOOL_VAULT_ABI


from mainnet_launch.database.database_operations import (
    write_dataframe_to_table,
    get_earliest_block_from_table_with_chain,
    get_all_rows_in_table_by_chain,
)
from mainnet_launch.database.should_update_database import should_update_table

from mainnet_launch.abis import STATS_CALCULATOR_REGISTRY_ABI


from multicall import Call
from mainnet_launch.data_fetching.get_state_by_block import (
    identity_with_bool_success,
    get_raw_state_by_blocks,
    get_state_by_one_block,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)

from mainnet_launch.database.schema.full import Tokens, Blocks


def _get_incentive_tokens(chain: ChainData) -> list[Tokens]:
    pass


def _get_possible_asset_tokens(chain: ChainData) -> list[Tokens]:
    pass


def update_tokens_table():
    for chain in ALL_CHAINS:
        incentive_tokens = _get_incentive_tokens(chain)
        asset_tokens = _get_possible_asset_tokens(chain)
        tokens = [*incentive_tokens, *asset_tokens]
        # update table with these tokens
