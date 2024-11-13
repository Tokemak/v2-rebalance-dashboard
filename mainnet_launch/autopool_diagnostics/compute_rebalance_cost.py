"""
Returns the NAV lost to rebalances.

Note we can't use the the swapCost event because it prints the min amount of LP tokens instead of the actual events



"""

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

from mainnet_launch.lens_contract import get_pools_and_destinations_call
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
from mainnet_launch.top_level.key_metrics import fetch_key_metrics_data
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.constants import AUTO_ETH, AUTO_LRT, BAL_ETH, AutopoolConstants, CACHE_TIME, ALL_AUTOPOOLS
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI, AUTOPOOL_ETH_STRATEGY_ABI, ERC_20_ABI
from mainnet_launch.solver_diagnostics.fetch_rebalance_events import (
    fetch_and_clean_rebalance_between_destination_events
)
from mainnet_launch.destinations import get_destination_details, ALL_AUTOPOOLS


def _fetch_all_lp_token_transfers() -> pd.DataFrame:
    
    destinations = get_destination_details()

    dfs = []
    for d in destinations:
        addr = d.lpTokenAddress if d.lpTokenAddress is not None else d.autopool.autopool_eth_addr
        lp_token_contract = eth_client.eth.contract(eth_client.toChecksumAddress(addr), abi=ERC_20_ABI)
        transfers = fetch_events(lp_token_contract.events.Transfer, start_block=20538409)
        transfers['token_address'] = lp_token_contract.address
        dfs.append(transfers)

    transfer_df = pd.concat(dfs, axis=0)
    return transfer_df



def _fetch_lp_token_value_df(blocks:list[int], autopool:AutopoolConstants) -> pd.DataFrame:
    full_data_df = get_raw_state_by_blocks([get_pools_and_destinations_call()], blocks, include_block_number=True)
    
    def _extract_safe_lp_token_value(row: dict):
        """Returns the safe lp token value for each destination in autopool as sum(reservesInEth) / actualLPTotalSupply"""
        for a, destination_list in zip(row['getPoolsAndDestinations']["autopools"], row['getPoolsAndDestinations']["destinations"]):
            if a["poolAddress"].lower() == autopool.autopool_eth_addr.lower():
                
                destination_safe_lp_token_value = {}
                for destination in destination_list:
                    tvl_in_pool = sum(destination['reservesInEth'])  / 1e18
                    num_lp_tokens = destination['actualLPTotalSupply'] / 1e18
                    safe_lp_token_value = tvl_in_pool / num_lp_tokens
                    destination_safe_lp_token_value[destination['lpTokenName']] = safe_lp_token_value  
                            
        
        return destination_safe_lp_token_value
    
    safe_lp_token_value_records = full_data_df.apply(_extract_safe_lp_token_value, axis=1)
    safe_lp_token_value_df = pd.DataFrame.from_records(safe_lp_token_value_records)
    safe_lp_token_value_df.index = full_data_df.index
    safe_lp_token_value_df['block'] = full_data_df['block']
    return safe_lp_token_value_df



def _compute_rebalance_cost_from_rebalance_event_df(rebalance_events_df:pd.DataFrame, autopool: AutopoolConstants):
    
    blocks_with_rebalances = rebalance_events_df['block'].values
    blocks_before_rebalances = [b - 1 for b in rebalance_events_df['block'].values]

def compute_daily_rebalance_costs(autopool:AutopoolConstants) -> pd.DataFrame:
    
    strategy_contract = eth_client.eth.contract(autopool.autopool_eth_strategy_addr, abi=AUTOPOOL_ETH_STRATEGY_ABI)
    df = fetch_events(strategy_contract.events.RebalanceBetweenDestinations)
    pass
    
    
    
    
    # rebalance_blocks = [ ['block'],
           
    #        fetch_events(strategy_contract.events.SuccessfulRebalanceBetweenDestinations)['block'],        
    # ]
    
    # rebalance_between_destinations_df = fetch_events(strategy_contract.events.RebalanceBetweenDestinations)
    
    # # mock
    # churn_rebalances = rebalance_between_destinations_df.head()
    # between_destination_rebalances = rebalance_between_destinations_df.tail()
    # rebalance_to_idle_df = fetch_events(strategy_contract.events.RebalanceToIdle)
    # between_destination_rebalances = between_destination_rebalances.append(rebalance_to_idle_df)
    
    # we only care about the blocks and the prices
    
    
    
compute_daily_rebalance_costs(AUTO_ETH)
    
