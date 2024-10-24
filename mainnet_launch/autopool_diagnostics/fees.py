import pandas as pd
import streamlit as st
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
import colorsys

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
    streaming_fee_df["normalized_fees"] = streaming_fee_df["fees"].apply(lambda x: int(x) / 1e18)

    if len(streaming_fee_df) > 0:
        print("Warning: There are streaming fees now. Incorporating them into the total fees.")

    pfee_df = periodic_fee_df[["normalized_fees"]].copy()
    sfee_df = streaming_fee_df[["normalized_fees"]].copy()

    return pfee_df, sfee_df


@st.cache_data(ttl=CACHE_TIME)
def fetch_autopool_rewardliq_plot(autopool: AutopoolConstants):
    vault_contract = eth_client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
    rewardsliq = vault_contract.events.DestinationDebtReporting.createFilter(fromBlock=start_block)
    rewardsliq_events = rewardsliq.get_all_entries()

    # Process event data
    event_data = []
    for event in rewardsliq_events:
        block = eth_client.eth.get_block(event["blockNumber"])
        event_data.append(
            {
                "timestamp": datetime.fromtimestamp(block["timestamp"]),
                "destination": event["args"]["destination"],
                "claimed": event["args"]["claimed"] / 1e18,  # Convert Wei to ETH
            }
        )

    # Create DataFrame
    df = pd.DataFrame(event_data)

    # Generate distinct colors for each destination
    unique_destinations = df["destination"].unique()
    n = len(unique_destinations)
    colors = [
        f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"
        for r, g, b in [colorsys.hsv_to_rgb(i / n, 0.8, 0.8) for i in range(n)]
    ]
    color_map = dict(zip(unique_destinations, colors))

    # Create subplots
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=("Destination Debt Reporting: Claimed ETH over Time", "Total Claimed ETH for Autopool"),
    )

    # Plot for individual destinations
    for destination in unique_destinations:
        df_dest = df[df["destination"] == destination]
        fig.add_trace(
            go.Scatter(
                x=df_dest["timestamp"],
                y=df_dest["claimed"],
                mode="markers",
                name=destination,
                marker=dict(
                    size=df_dest["claimed"] * 2,
                    color=color_map[destination],
                    sizemode="area",
                    sizemin=4,
                    sizeref=2.0 * max(df["claimed"]) / (40.0**2),
                    line=dict(width=0),
                ),
                text=df_dest["claimed"].apply(lambda x: f"{x:.4f} ETH"),
                hoverinfo="text+name",
            ),
            row=1,
            col=1,
        )

    # Calculate and plot total claimed over time
    total_claimed_data = df.groupby("timestamp").agg(total_claimed=("claimed", "sum")).reset_index()
    total_claimed_data["cumulative_claimed"] = total_claimed_data["total_claimed"].cumsum()

    fig.add_trace(
        go.Scatter(
            x=total_claimed_data["timestamp"],
            y=total_claimed_data["cumulative_claimed"],
            mode="lines+markers",
            name="Total Claimed",
            line=dict(color="blue", width=2),
            marker=dict(color="blue", size=8),
        ),
        row=2,
        col=1,
    )

    # Customize the layout
    fig.update_layout(height=800, hovermode="closest", showlegend=True)

    fig.update_xaxes(title_text="Time", row=2, col=1)
    fig.update_yaxes(title_text="Claimed (ETH)", row=1, col=1)
    fig.update_yaxes(title_text="Total Claimed (ETH)", row=2, col=1)
    st.plotly_chart(fig, use_container_width=True)


def fetch_and_render_autopool_fee_data(autopool: AutopoolConstants):
    fee_df, sfee_df = fetch_autopool_fee_data(autopool)
    st.header(f"{autopool.name} Autopool Fees")

    _display_fee_metrics(fee_df, True)
    _display_fee_metrics(sfee_df, False)

    # Generate fee figures
    daily_fee_fig, cumulative_fee_fig, weekly_fee_fig = _build_fee_figures(autopool, fee_df)
    daily_sfee_fig, cumulative_sfee_fig, weekly_sfee_fig = _build_fee_figures(autopool, sfee_df)

    st.subheader(f"{autopool.name} Autopool Periodic Fees")

    st.plotly_chart(daily_fee_fig, use_container_width=True)
    st.plotly_chart(weekly_fee_fig, use_container_width=True)
    st.plotly_chart(cumulative_fee_fig, use_container_width=True)

    st.subheader(f"{autopool.name} Autopool Streaming Fees")
    st.plotly_chart(daily_sfee_fig, use_container_width=True)
    st.plotly_chart(weekly_sfee_fig, use_container_width=True)
    st.plotly_chart(cumulative_sfee_fig, use_container_width=True)


def _display_fee_metrics(fee_df: pd.DataFrame, isPeriodic: bool):
    """Calculate and display fee metrics at the top of the dashboard."""
    # I don't really like this pattern, redo it
    today = datetime.now(timezone.utc)

    seven_days_ago = today - timedelta(days=7)
    thirty_days_ago = today - timedelta(days=30)
    year_ago = today - timedelta(days=465)

    fees_last_7_days = fee_df[fee_df.index >= seven_days_ago]["normalized_fees"].sum()

    if len(fee_df[fee_df.index >= thirty_days_ago]) > 0:
        fees_last_30_days = fee_df[fee_df.index >= thirty_days_ago]["normalized_fees"].sum()
    else:
        fees_last_30_days = "None"

    fees_year_to_date = fee_df[fee_df.index >= year_ago]["normalized_fees"].sum()

    col1, col2, col3 = st.columns(3)

    with col1:
        if isPeriodic:
            st.metric(label="Periodic Fees Earned Over Last 7 Days (ETH)", value=f"{fees_last_7_days:.2f}")
        else:
            st.metric(label="Streaming Fees Earned Over Last 7 Days (ETH)", value=f"{fees_last_7_days:.2f}")

    with col2:
        if isPeriodic:
            st.metric(
                label="Periodic Fees Earned Over Last 30 Days (ETH)",
                value=f"{fees_last_30_days:.2f}" if isinstance(fees_last_30_days, (int, float)) else fees_last_30_days,
            )
        else:
            st.metric(
                label="Streaming Fees Earned Over Last 30 Days (ETH)",
                value=f"{fees_last_30_days:.2f}" if isinstance(fees_last_30_days, (int, float)) else fees_last_30_days,
            )

    with col3:
        if isPeriodic:
            st.metric(label="Periodic Fees Earned Year to Date (ETH)", value=f"{fees_year_to_date:.2f}")
        else:
            st.metric(label="Streaming Fees Earned Year to Date (ETH)", value=f"{fees_year_to_date:.2f}")


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
