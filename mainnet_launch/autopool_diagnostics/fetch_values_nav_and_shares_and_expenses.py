"""Returns of the autopool before and after expenses and fees"""

import pandas as pd
import streamlit as st
from multicall import Call


from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)

from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
from mainnet_launch.solver_diagnostics.fetch_rebalance_events import (
    fetch_rebalance_events_df,
)
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.constants import AutopoolConstants, CACHE_TIME
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI


@st.cache_data(ttl=CACHE_TIME)
def fetch_nav_and_shares_and_factors_that_impact_nav_per_share(autopool: AutopoolConstants) -> pd.DataFrame:
    daily_nav_shares_df = _fetch_actual_nav_per_share_by_day(autopool)
    cumulative_new_shares_df = _fetch_cumulative_fee_shares_minted_by_day(autopool)
    cumulative_rebalance_from_idle_swap_cost, cumulative_rebalance_not_from_idle_swap_cost = (
        _fetch_cumulative_nav_lost_to_rebalances(autopool)
    )
    df = pd.concat(
        [
            daily_nav_shares_df,
            cumulative_new_shares_df,
            cumulative_rebalance_from_idle_swap_cost,
            cumulative_rebalance_not_from_idle_swap_cost,
        ],
        axis=1,
    )
    df.iloc[0] = df.iloc[0].fillna(0)
    df = df.ffill()
    return df


def _fetch_actual_nav_per_share_by_day(autopool: AutopoolConstants) -> pd.DataFrame:

    def handle_getAssetBreakdown(success, AssetBreakdown):
        if success:
            totalIdle, totalDebt, totalDebtMin, totalDebtMax = AssetBreakdown
            return int(totalIdle + totalDebt) / 1e18
        return None

    calls = [
        Call(
            autopool.autopool_eth_addr,
            ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
            [("actual_nav", handle_getAssetBreakdown)],
        ),
        Call(
            autopool.autopool_eth_addr,
            ["totalSupply()(uint256)"],
            [("actual_shares", safe_normalize_with_bool_success)],
        ),
    ]

    blocks = build_blocks_to_use(autopool.chain)
    df = get_raw_state_by_blocks(calls, blocks, autopool.chain)
    df["actual_nav_per_share"] = df["actual_nav"] / df["actual_shares"]
    daily_nav_shares_df = df.resample("1D").last()
    return daily_nav_shares_df


def _fetch_cumulative_nav_lost_to_rebalances(autopool: AutopoolConstants) -> pd.DataFrame:
    rebalance_df = fetch_rebalance_events_df(autopool)

    rebalance_from_idle_df = rebalance_df[
        rebalance_df["outDestinationVault"].str.lower() == autopool.autopool_eth_addr.lower()
    ].copy()
    rebalance_not_from_idle_df = rebalance_df[
        ~(rebalance_df["outDestinationVault"].str.lower() == autopool.autopool_eth_addr.lower())
    ].copy()

    cumulative_rebalance_from_idle_swap_cost = rebalance_from_idle_df["swap_cost"].resample("1D").sum().cumsum()
    cumulative_rebalance_from_idle_swap_cost.name = "rebalance_from_idle_swap_cost"

    cumulative_rebalance_not_from_idle_swap_cost = rebalance_not_from_idle_df["swap_cost"].resample("1D").sum().cumsum()
    cumulative_rebalance_not_from_idle_swap_cost.name = "rebalance_not_idle_swap_cost"
    return cumulative_rebalance_from_idle_swap_cost, cumulative_rebalance_not_from_idle_swap_cost


def _fetch_cumulative_fee_shares_minted_by_day(autopool: AutopoolConstants) -> pd.DataFrame:
    vault_contract = autopool.chain.client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
    FeeCollected_df = add_timestamp_to_df_with_block_column(
        fetch_events(vault_contract.events.FeeCollected), autopool.chain
    )
    PeriodicFeeCollected_df = add_timestamp_to_df_with_block_column(
        fetch_events(vault_contract.events.PeriodicFeeCollected), autopool.chain
    )
    PeriodicFeeCollected_df["new_shares_from_periodic_fees"] = PeriodicFeeCollected_df["mintedShares"] / 1e18
    FeeCollected_df["new_shares_from_streaming_fees"] = FeeCollected_df["mintedShares"] / 1e18
    fee_df = pd.concat(
        [
            PeriodicFeeCollected_df[["new_shares_from_periodic_fees"]],
            FeeCollected_df[["new_shares_from_streaming_fees"]],
        ]
    )
    daily_fee_share_df = fee_df.resample("1D").sum()
    cumulative_new_shares_df = daily_fee_share_df.cumsum()
    return cumulative_new_shares_df
