import pandas as pd
import streamlit as st
from multicall import Call
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
)

from mainnet_launch.constants import CACHE_TIME, ALL_AUTOPOOLS, AutopoolConstants



def nav_per_share_call(name: str, autopool_vault_address: str) -> Call:
    return Call(
        autopool_vault_address,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [(name, safe_normalize_with_bool_success)],
    )




def fetch_nav_per_share(blocks: list[int], autopool: AutopoolConstants) -> pd.DataFrame:

    nav_per_share_df = get_raw_state_by_blocks(
        calls=[nav_per_share_call(autopool.name, autopool.autopool_eth_addr)], blocks=blocks, chain=autopool.chain
    )

    # nav_per_share_df = _fetch_all_all_pool_nav_per_share(blocks)[[autopool.name]]
    nav_per_share_df = nav_per_share_df.resample("1D").last()

    # Calculate the 30-day difference and annualized return
    nav_per_share_df["30_day_difference"] = nav_per_share_df[autopool.name].diff(periods=30)
    # Normalized to starting NAV per share for 30-day return
    nav_per_share_df["30_day_annualized_return"] = (
        (nav_per_share_df["30_day_difference"] / nav_per_share_df[autopool.name].shift(30)) * (365 / 30) * 100
    )

    # Calculate the 7-day difference and annualized return
    nav_per_share_df["7_day_difference"] = nav_per_share_df[autopool.name].diff(periods=7)
    # Normalized to starting NAV per share for 7-day return
    nav_per_share_df["7_day_annualized_return"] = (
        (nav_per_share_df["7_day_difference"] / nav_per_share_df[autopool.name].shift(7)) * (365 / 7) * 100
    )

    # Calculate daily returns
    nav_per_share_df["daily_return"] = nav_per_share_df[autopool.name].pct_change()

    # Calculate 7-day moving average of daily returns
    nav_per_share_df["7_day_MA_return"] = nav_per_share_df["daily_return"].rolling(window=7).mean()

    # Annualize the 7-day moving average return
    nav_per_share_df["7_day_MA_annualized_return"] = nav_per_share_df["7_day_MA_return"] * 365 * 100

    # Calculate 30-day moving average of daily returns
    nav_per_share_df["30_day_MA_return"] = nav_per_share_df["daily_return"].rolling(window=30).mean()

    # Annualize the 30-day moving average return
    nav_per_share_df["30_day_MA_annualized_return"] = nav_per_share_df["30_day_MA_return"] * 365 * 100

    return nav_per_share_df
