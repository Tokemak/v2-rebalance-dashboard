import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime, timedelta

from mainnet_launch.constants import CACHE_TIME, eth_client, AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.get_state_by_block import add_timestamp_to_df_with_block_column
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI


start_block = 20759126  # Sep 15, 2024


@st.cache_data(ttl=CACHE_TIME)
def fetch_autopool_fee_data(autopool: AutopoolConstants):
    vault_contract = eth_client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
    streaming_fee_df = fetch_events(vault_contract.events.FeeCollected, start_block=start_block)
    periodic_fee_df = fetch_events(vault_contract.events.PeriodicFeeCollected, start_block=start_block)

    streaming_fee_df = add_timestamp_to_df_with_block_column(streaming_fee_df)
    periodic_fee_df = add_timestamp_to_df_with_block_column(periodic_fee_df)

    periodic_fee_df["normalized_fees"] = periodic_fee_df["fees"].apply(lambda x: int(x) / 1e18)

    if len(streaming_fee_df) > 0:
        raise ValueError("There are streaming fees now, need to double check _fetch_autopool_fee_df function")

    fee_df = periodic_fee_df[["normalized_fees"]].copy()
    return fee_df


def fetch_and_render_autopool_fee_data(autopool: AutopoolConstants):
    fee_df = fetch_autopool_fee_data(autopool)
    st.header(f"{autopool.name} Autopool Fees")

    _display_fee_metrics(fee_df)

    # Generate fee figures
    daily_fee_fig, cumulative_fee_fig, weekly_fee_fig = _build_fee_figures(autopool, fee_df)

    st.plotly_chart(daily_fee_fig, use_container_width=True)
    st.plotly_chart(weekly_fee_fig, use_container_width=True)
    st.plotly_chart(cumulative_fee_fig, use_container_width=True)


def _display_fee_metrics(fee_df: pd.DataFrame):
    """Calculate and display fee metrics at the top of the dashboard."""
    # I don't really like this pattern, redo it
    today = datetime.now()
    seven_days_ago = today - timedelta(days=7)
    thirty_days_ago = today - timedelta(days=30)
    year_start = datetime(today.year, 1, 1)

    fees_last_7_days = fee_df[fee_df.index >= seven_days_ago]["normalized_fees"].sum()

    if len(fee_df[fee_df.index >= thirty_days_ago]) > 0:
        fees_last_30_days = fee_df[fee_df.index >= thirty_days_ago]["normalized_fees"].sum()
    else:
        fees_last_30_days = "None"

    fees_year_to_date = fee_df[fee_df.index >= year_start]["normalized_fees"].sum()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(label="Fees Earned Over Last 7 Days (ETH)", value=f"{fees_last_7_days:.2f}")

    with col2:
        st.metric(
            label="Fees Earned Over Last 30 Days (ETH)",
            value=f"{fees_last_30_days:.2f}" if isinstance(fees_last_30_days, (int, float)) else fees_last_30_days,
        )

    with col3:
        st.metric(label="Fees Earned Year to Date (ETH)", value=f"{fees_year_to_date:.2f}")


def _build_fee_figures(autopool: AutopoolConstants, fee_df: pd.DataFrame):
    # Ensure the 'fee_df' is indexed by datetime
    fee_df.index = pd.to_datetime(fee_df.index)

    # 1. Daily Fees
    daily_fees_df = fee_df.resample("1D").sum()
    daily_fee_fig = px.bar(daily_fees_df)
    daily_fee_fig.update_layout(
        title=f"{autopool.name} Total Daily Fees",
        xaxis_tickformat="%Y-%m-%d",
        xaxis_title="Date",
        yaxis_title="ETH",
        xaxis_tickangle=-45,
        width=900,
        height=500,
    )

    # 2. Cumulative Lifetime Fees
    cumulative_fees_df = daily_fees_df.cumsum()
    cumulative_fee_fig = px.line(cumulative_fees_df)
    cumulative_fee_fig.update_layout(
        title=f"{autopool.name} Cumulative Lifetime Fees",
        xaxis_tickformat="%Y-%m-%d",
        xaxis_title="Date",
        yaxis_title="Cumulative ETH",
        xaxis_tickangle=-45,
        width=900,
        height=500,
    )

    # 3. Weekly Fees
    weekly_fees_df = fee_df.resample("1W").sum()
    weekly_fee_fig = px.bar(weekly_fees_df)
    weekly_fee_fig.update_layout(
        title=f"{autopool.name} Total Weekly Fees",
        xaxis_tickformat="%Y-%m-%d",
        xaxis_title="Date",
        yaxis_title="ETH",
        xaxis_tickangle=-45,
        width=900,
        height=500,
    )

    return daily_fee_fig, cumulative_fee_fig, weekly_fee_fig
