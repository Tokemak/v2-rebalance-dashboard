"""Fetches the hypothetical nav and shares for an Autopool if certain costs are excluded"""

import pandas as pd
import streamlit as st
from datetime import timedelta, datetime, timezone
from multicall import Call
import plotly.express as px
import plotly.graph_objects as go
import json
import numpy as np

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    get_state_by_one_block,
    identity_with_bool_success,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
    eth_client,
)
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.constants import AUTO_LRT, AutopoolConstants, CACHE_TIME
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI
from mainnet_launch.solver_diagnostics.fetch_rebalance_events import fetch_and_clean_rebalance_between_destination_events
from mainnet_launch.autopool_diagnostics.nav_per_share_if_no_discount import fetch_destination_totalEthValueHeldIfNoDiscount


def fetch_autopool_nav_and_shares_with_conditions(autopool:AutopoolConstants, blocks: list[int]) -> pd.DataFrame:

    
    
    
    autopool_df = pd.DataFrame()
    autopool_df['block'] = blocks
    autopool_df = add_timestamp_to_df_with_block_column(autopool_df)
    
    # mock values
    autopool_df['actual_nav'] = 1
    autopool_df['actual_shares'] = 1
    autopool_df['actual_nav_per_share'] = 1
    
    autopool_df['cumulative_shares_minted_from_fees'] = .01
    autopool_df['cumulative_nav_lost_to_rebalance_costs'] = -.05
    autopool_df['nav_diff_if_restored_to_peg'] = .02
    
    # add in all the permuations of nav per share backing out these costs
    

    
    return autopool_df
    