"""
Returns the NAV lost to rebalances.

Note we can't use the the swapCost event because it uses the min amount of LP tokens instead of the actual events



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
from mainnet_launch.constants import AUTO_ETH, AUTO_LRT, BAL_ETH, AutopoolConstants, CACHE_TIME
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI, AUTOPOOL_ETH_STRATEGY_ABI, ERC_20_ABI
from mainnet_launch.solver_diagnostics.fetch_rebalance_events import (
    fetch_and_clean_rebalance_between_destination_events,
)
from mainnet_launch.destinations import get_destination_details, ALL_AUTOPOOLS


def _fetch_all_lp_token_transfers(autopool: AutopoolConstants) -> pd.DataFrame:

    destinations = [d for d in get_destination_details() if d.autopool == autopool]

    lp_tokens = list(set([d.lpTokenAddress for d in destinations]))
    vaultAddresses = list(set([d.vaultAddress for d in destinations]))
    dfs = []

    for lp_token_address in lp_tokens:  # might not need this
        token_contract = eth_client.eth.contract(eth_client.toChecksumAddress(lp_token_address), abi=ERC_20_ABI)
        transfers = fetch_events(token_contract.events.Transfer, start_block=20538409)
        transfers["token_address"] = token_contract.address
        dfs.append(transfers)

    for vault_address in vaultAddresses:
        token_contract = eth_client.eth.contract(eth_client.toChecksumAddress(vault_address), abi=ERC_20_ABI)
        transfers = fetch_events(token_contract.events.Transfer, start_block=20538409)
        transfers["token_address"] = token_contract.address
        dfs.append(transfers)

    transfer_df = pd.concat(dfs, axis=0)
    return transfer_df


def _fetch_safe_lp_token_value_df(blocks: list[int], autopool: AutopoolConstants) -> pd.DataFrame:
    full_data_df = get_raw_state_by_blocks([get_pools_and_destinations_call()], blocks, include_block_number=True)

    def _extract_safe_lp_token_value(row: dict):
        """Returns the safe lp token value for each destination in autopool as sum(reservesInEth) / actualLPTotalSupply"""
        for a, destination_list in zip(
            row["getPoolsAndDestinations"]["autopools"], row["getPoolsAndDestinations"]["destinations"]
        ):
            if a["poolAddress"].lower() == autopool.autopool_eth_addr.lower():

                destination_safe_lp_token_value = {}
                for destination in destination_list:
                    tvl_in_pool = sum(destination["reservesInEth"]) / 1e18
                    num_lp_tokens = destination["actualLPTotalSupply"] / 1e18
                    safe_lp_token_value = tvl_in_pool / num_lp_tokens
                    lp_token_addr = eth_client.toChecksumAddress(destination["lpTokenAddress"])
                    destination_safe_lp_token_value[lp_token_addr] = safe_lp_token_value

        return destination_safe_lp_token_value

    safe_lp_token_value_records = full_data_df.apply(_extract_safe_lp_token_value, axis=1)
    safe_lp_token_value_df = pd.DataFrame.from_records(safe_lp_token_value_records)
    safe_lp_token_value_df.index = full_data_df.index
    safe_lp_token_value_df["block"] = full_data_df["block"]
    return safe_lp_token_value_df


def _fetch_lp_token_validated_spot_price(blocks: list[int], autopool: AutopoolConstants) -> pd.DataFrame:
    
    destinations = [d for d in get_destination_details() if d.autopool == autopool]
    
    get_validated_spot_price_calls = []
    for dest in destinations:
        call = Call(
        dest.vaultAddress,
        ["getValidatedSpotPrice()(uint256)"],
        [(dest.vaultAddress, safe_normalize_with_bool_success)],
    )
        get_validated_spot_price_calls.append(call)
    
    validated_spot_price_df = get_raw_state_by_blocks(get_validated_spot_price_calls, blocks, include_block_number=True)
    return validated_spot_price_df


def _compute_rebalance_cost_from_rebalance_event_df(rebalance_events_df: pd.DataFrame, autopool: AutopoolConstants):

    lp_token_transfer_df = _fetch_all_lp_token_transfers(autopool)

    blocks_with_rebalances = rebalance_events_df["block"].values
    blocks_before_rebalances = [b - 1 for b in rebalance_events_df["block"].values]

    # safe_lp_token_value_at_block_before_rebalance = _fetch_safe_lp_token_value_df(blocks_before_rebalances, autopool)
    validated_spot_price_df = _fetch_lp_token_validated_spot_price(blocks_before_rebalances, autopool)

    return rebalance_events_df, lp_token_transfer_df, validated_spot_price_df


def compute_daily_rebalance_costs(autopool: AutopoolConstants) -> pd.DataFrame:

    strategy_contract = eth_client.eth.contract(autopool.autopool_eth_strategy_addr, abi=AUTOPOOL_ETH_STRATEGY_ABI)
    rebalance_between_destinations_df = fetch_events(strategy_contract.events.RebalanceBetweenDestinations)
    destination_details = get_destination_details()

    destination_vault_address_to_lp_token_address = {d.vaultAddress: d.lpTokenAddress for d in destination_details}

    rebalance_between_destinations_df["outDestinationLpToken"] = rebalance_between_destinations_df[
        "outSummaryStats"
    ].apply(lambda x: destination_vault_address_to_lp_token_address[eth_client.toChecksumAddress(x[0])])

    rebalance_between_destinations_df["outDestinationVault"] = rebalance_between_destinations_df[
        "outSummaryStats"
    ].apply(lambda x: eth_client.toChecksumAddress(x[0]))

    rebalance_between_destinations_df["inDestinationLpToken"] = rebalance_between_destinations_df[
        "inSummaryStats"
    ].apply(lambda x: destination_vault_address_to_lp_token_address[eth_client.toChecksumAddress(x[0])])

    rebalance_between_destinations_df["inDestinationVault"] = rebalance_between_destinations_df["inSummaryStats"].apply(
        lambda x: eth_client.toChecksumAddress(x[0])
    )

    rebalance_between_destinations_df["params_amountIn"] = rebalance_between_destinations_df["params"].apply(
        lambda x: int(x[2]) / 1e18
    )

    rebalance_between_destinations_df["params_amountOut"] = rebalance_between_destinations_df["params"].apply(
        lambda x: int(x[5]) / 1e18
    )

    # non elective rebalances, taking inital capital
    rebalance_from_idle = rebalance_between_destinations_df[
        rebalance_between_destinations_df["outDestinationLpToken"] == autopool.autopool_eth_addr
    ].copy()

    # elective chrun rebalances, ether dest -> dest or dest -> idle, to fill back up idle
    rebalance_between_destinations = rebalance_between_destinations_df = rebalance_between_destinations_df[
        rebalance_between_destinations_df["outDestinationLpToken"] != autopool.autopool_eth_addr
    ].copy()

    a = _compute_rebalance_cost_from_rebalance_event_df(rebalance_between_destinations, autopool)
    b = _compute_rebalance_cost_from_rebalance_event_df(rebalance_from_idle, autopool)
    # rebalance_events_df, lp_token_transfer_df, safe_lp_token_value_at_block_before_rebalance

    return a, b


if __name__ == "__main__":
    compute_daily_rebalance_costs(AUTO_ETH)
