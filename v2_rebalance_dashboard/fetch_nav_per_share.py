import pandas as pd
import streamlit as st

from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    build_blocks_to_use,
    safe_normalize_with_bool_success,
)
from v2_rebalance_dashboard.constants import balETH_AUTOPOOL_ETH_ADDRESS
import plotly.express as px
import numpy as np


def nav_per_share_call(name: str, autopool_vault_address: str) -> Call:
    return Call(
        autopool_vault_address,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [(name, safe_normalize_with_bool_success)],
    )


@st.cache_data(ttl=12 * 3600)
def fetch_daily_nav_per_share_to_plot():
    blocks = build_blocks_to_use()
    calls = [
        nav_per_share_call("balETH", balETH_AUTOPOOL_ETH_ADDRESS),
    ]
    nav_per_share_df = sync_safe_get_raw_state_by_block(calls, blocks)

    # Calculate the 30-day difference and annualized return
    nav_per_share_df["30_day_difference"] = nav_per_share_df["balETH"].diff(periods=30)
    # Normalized to starting NAV per share for 30-day return
    nav_per_share_df["30_day_annualized_return"] = (
        (nav_per_share_df["30_day_difference"] / nav_per_share_df["balETH"].shift(30)) * (365 / 30) * 100
    )

    # Calculate the 7-day difference and annualized return
    nav_per_share_df["7_day_difference"] = nav_per_share_df["balETH"].diff(periods=7)
    # Normalized to starting NAV per share for 7-day return
    nav_per_share_df["7_day_annualized_return"] = (
        (nav_per_share_df["7_day_difference"] / nav_per_share_df["balETH"].shift(7)) * (365 / 7) * 100
    )

    return nav_per_share_df
