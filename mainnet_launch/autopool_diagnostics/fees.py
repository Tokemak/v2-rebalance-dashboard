import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime, timedelta

from mainnet_launch.constants import eth_client, AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.get_state_by_block import add_timestamp_to_df_with_block_column
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI


start_block = 20759126  # Sep 15, 2024


def display_autopool_fees(autopool: AutopoolConstants):
    st.header("Autopool Fees")

    # Fetch fee data
    fee_df = _fetch_autopool_fee_df(autopool)

    # Add metrics at the top
    _display_fee_metrics(fee_df)

    # Generate fee figures
    daily_fee_fig, cumulative_fee_fig, weekly_fee_fig = _build_fee_figures(autopool, fee_df)

    st.plotly_chart(daily_fee_fig, use_container_width=True)
    st.plotly_chart(weekly_fee_fig, use_container_width=True)
    st.plotly_chart(cumulative_fee_fig, use_container_width=True)


def _display_fee_metrics(fee_df: pd.DataFrame):
    """Calculate and display fee metrics at the top of the dashboard."""

    # Today's date and filter windows
    today = datetime.now()
    seven_days_ago = today - timedelta(days=7)
    thirty_days_ago = today - timedelta(days=30)
    year_start = datetime(today.year, 1, 1)

    # Fees over the last 7 days
    fees_last_7_days = fee_df[fee_df.index >= seven_days_ago]["normalized_fees"].sum()

    # Fees over the last 30 days (if enough data, otherwise show None)
    if len(fee_df[fee_df.index >= thirty_days_ago]) > 0:
        fees_last_30_days = fee_df[fee_df.index >= thirty_days_ago]["normalized_fees"].sum()
    else:
        fees_last_30_days = "None"

    # Fees year-to-date
    fees_year_to_date = fee_df[fee_df.index >= year_start]["normalized_fees"].sum()

    col1, col2, col3 = st.columns(3)

    # Display the metrics in the respective columns
    with col1:
        st.metric(label="Fees Earned Over Last 7 Days (ETH)", value=f"{fees_last_7_days:.2f}")

    with col2:
        st.metric(
            label="Fees Earned Over Last 30 Days (ETH)",
            value=f"{fees_last_30_days:.2f}" if isinstance(fees_last_30_days, (int, float)) else fees_last_30_days,
        )

    with col3:
        st.metric(label="Fees Earned Year to Date (ETH)", value=f"{fees_year_to_date:.2f}")


@st.cache_data(ttl=3600)  # Cache data for 1 hour
def _fetch_autopool_fee_df(autopool: AutopoolConstants) -> pd.DataFrame:
    contract = eth_client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
    streaming_fee_df = fetch_events(contract.events.FeeCollected, start_block=start_block)
    periodic_fee_df = fetch_events(contract.events.PeriodicFeeCollected, start_block=start_block)

    # Add timestamps to the dataframes
    streaming_fee_df = add_timestamp_to_df_with_block_column(streaming_fee_df)
    periodic_fee_df = add_timestamp_to_df_with_block_column(periodic_fee_df)

    # Normalize the fee amounts to ETH
    periodic_fee_df["normalized_fees"] = periodic_fee_df["fees"].apply(lambda x: int(x) / 1e18)

    # Raise an error if streaming fees are detected (as per your original note)
    if len(streaming_fee_df) > 0:
        raise ValueError("There are streaming fees now, need to double check _fetch_autopool_fee_df function")

    # Return the periodic fee dataframe with normalized fees
    fee_df = periodic_fee_df[["normalized_fees"]].copy()
    return fee_df


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

    # Return all three figures
    return daily_fee_fig, cumulative_fee_fig, weekly_fee_fig


if __name__ == "__main__":
    _fetch_autopool_fee_df(ALL_AUTOPOOLS[0])
