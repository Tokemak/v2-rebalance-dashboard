# deposit_withdraw.py

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import plotly.express as px
from mainnet_launch.constants import CACHE_TIME, eth_client, AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI

from mainnet_launch.destination_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats

start_block = 20759126  # Sep 15, 2024


@st.cache_data(ttl=CACHE_TIME)
def fetch_autopool_destination_counts_data(autopool: AutopoolConstants):
    blocks = build_blocks_to_use(autopool.chain)
    uwcr_df, allocation_df, compositeReturn_out_df, total_nav_series, summary_stats_df, pR_df = (
        fetch_destination_summary_stats(blocks, autopool)
    )
    destination_count_figure = _make_destination_count_figure(autopool, summary_stats_df)
    return destination_count_figure


def fetch_and_render_autopool_destination_counts_data(autopool: AutopoolConstants):
    st.header(f"{autopool.name} Destination Counts")
    destination_count_figure = fetch_autopool_destination_counts_data(autopool)
    st.plotly_chart(destination_count_figure, use_container_width=True)


def _make_destination_count_figure(autopool: AutopoolConstants, summary_stats_df: pd.DataFrame) -> go.Figure:
    ownedShares_df = summary_stats_df.map(lambda row: row["ownedShares"] if isinstance(row, dict) else None).astype(
        float
    )
    daily_owned_shares_df = ownedShares_df.resample("1D").last()
    destination_count_df = pd.DataFrame(index=daily_owned_shares_df.index)
    destination_count_df["Count of Destinations Allocated"] = daily_owned_shares_df.apply(
        lambda row: (row > 0).sum(), axis=1
    )
    destination_count_df["Count of Destinations Available"] = daily_owned_shares_df.apply(
        lambda row: row.notna().sum(), axis=1
    )

    destination_count_figure = go.Figure()

    # Add bars for the count of allocated destinations
    destination_count_figure.add_trace(
        go.Bar(
            x=destination_count_df.index,
            y=destination_count_df["Count of Destinations Allocated"],
            name="Allocated Destinations",
            marker_color="blue",
        )
    )

    # Add bars for the count of available destinations
    destination_count_figure.add_trace(
        go.Bar(
            x=destination_count_df.index,
            y=destination_count_df["Count of Destinations Available"],
            name="Available Destinations",
            marker_color="orange",
        )
    )

    # Update layout for grouped bars
    destination_count_figure.update_layout(
        title=f"{autopool.name} Destinations Count",
        xaxis_title="Date",
        yaxis_title="Count",
        barmode="group",  # Group the bars side by side
        xaxis_tickformat="%Y-%m-%d",
        xaxis_tickangle=-45,
        width=900,
        height=500,
    )

    return destination_count_figure
