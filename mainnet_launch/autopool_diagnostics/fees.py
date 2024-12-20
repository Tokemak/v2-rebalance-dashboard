import pandas as pd
import streamlit as st
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
import colorsys


from mainnet_launch.destinations import get_destination_details
from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use
from mainnet_launch.constants import CACHE_TIME, eth_client, AutopoolConstants, ALL_AUTOPOOLS, BASE_ETH, AUTO_LRT
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI


@st.cache_data(ttl=CACHE_TIME)
def fetch_autopool_fee_data(autopool: AutopoolConstants):
    vault_contract = autopool.chain.client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
    streaming_fee_df = add_timestamp_to_df_with_block_column(
        fetch_events(vault_contract.events.FeeCollected), autopool.chain
    )
    periodic_fee_df = add_timestamp_to_df_with_block_column(
        fetch_events(vault_contract.events.PeriodicFeeCollected), autopool.chain
    )

    periodic_fee_df["normalized_fees"] = periodic_fee_df["fees"].apply(lambda x: int(x) / 1e18)
    streaming_fee_df["normalized_fees"] = streaming_fee_df["fees"].apply(lambda x: int(x) / 1e18)
    periodic_fee_df = periodic_fee_df[["normalized_fees", "block"]].copy()
    streaming_fee_df = streaming_fee_df[["normalized_fees"]].copy()

    periodic_fee_df.columns = [f"{autopool.name}_periodic"]
    streaming_fee_df.columns = [f"{autopool.name}_streaming"]

    return periodic_fee_df, streaming_fee_df


@st.cache_data(ttl=CACHE_TIME)
def fetch_autopool_destination_debt_reporting_events(autopool: AutopoolConstants) -> pd.DataFrame:
    vault_contract = autopool.chain.client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
    # event DestinationDebtReporting(
    #     address destination, AutopoolDebt.IdleDebtUpdates debtInfo, uint256 claimed, uint256 claimGasUsed
    # );

    #  AutopoolDebt.IdleDebtUpdates
    #     struct IdleDebtUpdates {
    #     bool pricesWereSafe;
    #     uint256 totalIdleDecrease;
    #     uint256 totalIdleIncrease;
    #     uint256 totalDebtIncrease;
    #     uint256 totalDebtDecrease;
    #     uint256 totalMinDebtIncrease;
    #     uint256 totalMinDebtDecrease;
    #     uint256 totalMaxDebtIncrease;
    #     uint256 totalMaxDebtDecrease;
    # }
    debt_reporting_events_df = fetch_events(
        vault_contract.events.DestinationDebtReporting, start_block=autopool.chain.block_autopool_first_deployed
    )
    debt_reporting_events_df = add_timestamp_to_df_with_block_column(debt_reporting_events_df, autopool.chain)
    debt_reporting_events_df["eth_claimed"] = debt_reporting_events_df["claimed"] / 1e18  # claimed is in ETH
    blocks = build_blocks_to_use(autopool.chain)
    vault_to_name = {d.vaultAddress: d.vault_name for d in get_destination_details(autopool, blocks)}
    debt_reporting_events_df["destinationName"] = debt_reporting_events_df["destination"].apply(
        lambda x: vault_to_name[x]
    )
    return debt_reporting_events_df


def fetch_and_render_autopool_rewardliq_plot(autopool: AutopoolConstants):
    debt_reporting_events_df = fetch_autopool_destination_debt_reporting_events(autopool)
    destination_cumulative_sum = debt_reporting_events_df.pivot_table(
        values="eth_claimed", columns="destinationName", index="timestamp", fill_value=0
    ).cumsum()
    cumulative_eth_claimed_area_plot = px.area(
        destination_cumulative_sum, title="Cumulative ETH value of rewards claimed by destination"
    )
    individual_reward_claim_events_fig = px.scatter(
        debt_reporting_events_df,
        x=debt_reporting_events_df.index,
        y="eth_claimed",
        color="destinationName",
        size="eth_claimed",
        size_max=40,
        title="Individual reward claiming and liquidation events",
    )
    st.plotly_chart(cumulative_eth_claimed_area_plot, use_container_width=True)
    st.plotly_chart(individual_reward_claim_events_fig, use_container_width=True)


def fetch_and_render_autopool_fee_data(autopool: AutopoolConstants):

    fee_df, sfee_df = fetch_autopool_fee_data(autopool)
    if (len(fee_df) == 0) and (len(sfee_df) == 0):
        # the fee plots don't stop loading if for baseETH I suspect it is because there are no fees
        # if there are no fees then we don't need to plot anything
        return
    st.header(f"{autopool.name} Autopool Fees")

    _display_fee_metrics(autopool, fee_df, True)
    _display_fee_metrics(autopool, sfee_df, False)

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


def _display_fee_metrics(autopool: AutopoolConstants, fee_df: pd.DataFrame, isPeriodic: bool):
    """Calculate and display fee metrics at the top of the dashboard."""
    # I don't really like this pattern, redo it
    today = datetime.now(timezone.utc)

    seven_days_ago = today - timedelta(days=7)
    thirty_days_ago = today - timedelta(days=30)
    year_ago = today - timedelta(days=365)

    if isPeriodic:
        fee_column_name = f"{autopool.name}_periodic"
    else:
        fee_column_name = f"{autopool.name}_streaming"

    fees_last_7_days = fee_df[fee_df.index >= seven_days_ago][fee_column_name].sum()

    if len(fee_df[fee_df.index >= thirty_days_ago]) > 0:
        fees_last_30_days = fee_df[fee_df.index >= thirty_days_ago][fee_column_name].sum()
    else:
        fees_last_30_days = "None"

    fees_year_to_date = fee_df[fee_df.index >= year_ago][fee_column_name].sum()

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
    # Resample from Wednesday 4:00 PM to Wednesday 4:00 PM UTC (hour 16)
    shifted_fee_df = fee_df.shift(-16, freq="h")
    weekly_fees_df = shifted_fee_df.resample("W-WED").sum()
    weekly_fees_df.index = weekly_fees_df.index + pd.Timedelta(hours=16)

    weekly_fee_fig = px.bar(weekly_fees_df)
    weekly_fee_fig.update_layout(
        title=f"{autopool.name} Total Weekly Fees",
        xaxis_tickformat="%Y-%m-%d %H:%M",
        xaxis_title="Date",
        yaxis_title="ETH",
        xaxis_tickangle=-45,
        width=900,
        height=500,
    )

    return daily_fee_fig, cumulative_fee_fig, weekly_fee_fig


if __name__ == "__main__":
    fetch_and_render_autopool_rewardliq_plot(AUTO_LRT)
