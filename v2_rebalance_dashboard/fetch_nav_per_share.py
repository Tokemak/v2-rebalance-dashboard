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

@st.cache_data(ttl=12*3600)
def fetch_daily_nav_per_share_to_plot():
    blocks = build_blocks_to_use()
    calls = [
        nav_per_share_call("balETH", balETH_AUTOPOOL_ETH_ADDRESS),
    ]
    nav_per_share_df = sync_safe_get_raw_state_by_block(calls, blocks)

   # Calculate the 30-day difference and annualized return
    nav_per_share_df['30_day_difference'] = nav_per_share_df['balETH'].diff(periods=30)
    # Normalized to starting NAV per share for 30-day return
    nav_per_share_df['30_day_annualized_return'] = (nav_per_share_df['30_day_difference'] / nav_per_share_df['balETH'].shift(30)) * (365 / 30) * 100
    
    # Calculate the 7-day difference and annualized return
    nav_per_share_df['7_day_difference'] = nav_per_share_df['balETH'].diff(periods=7)
    # Normalized to starting NAV per share for 7-day return
    nav_per_share_df['7_day_annualized_return'] = (nav_per_share_df['7_day_difference'] / nav_per_share_df['balETH'].shift(7)) * (365 / 7) * 100

    # Plot NAV Per Share
    nav_fig = px.line(nav_per_share_df, y='balETH', title=' ')
    nav_fig.update_traces(line=dict(width=3))
    nav_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=400,
        width=800,
        font=dict(size=16),
        yaxis_title='NAV Per Share',
        xaxis_title='',
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray')
    )

    # Plot 30-day Annualized Return
    annualized_return_fig = px.line(nav_per_share_df, y='30_day_annualized_return', title=' ')
    annualized_return_fig.update_traces(line=dict(width=3))
    annualized_return_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=400,
        width=800,
        font=dict(size=16),
        yaxis_title='30-day Annualized Return (%)',
        xaxis_title='',
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray')
    )

    # Plot 7-day Annualized Return
    annualized_7dreturn_fig = px.line(nav_per_share_df, y='7_day_annualized_return', title=' ')
    annualized_7dreturn_fig.update_traces(line=dict(width=3))
    annualized_7dreturn_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=400,
        width=800,
        font=dict(size=16),
        yaxis_title='7-day Annualized Return (%)',
        xaxis_title='',
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray')
    )

    return nav_fig, annualized_return_fig, annualized_7dreturn_fig


