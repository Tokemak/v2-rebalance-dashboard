# deposit_withdraw.py

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import plotly.express as px
from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
from mainnet_launch.abis import AUTOPOOL_VAULT_ABI


from mainnet_launch.database.database_operations import (
    write_dataframe_to_table,
    get_earliest_block_from_table_with_autopool,
    get_all_rows_in_table_by_autopool,
)
from mainnet_launch.database.should_update_database import should_update_table


DEPOSIT_AND_WITHDRAW_FROM_AUTOPOOL_TABLE = "DEPOSIT_AND_WITHDRAW_FROM_AUTOPOOL_TABLE"


def fetch_autopool_deposit_and_withdraw_stats_data(autopool: AutopoolConstants):
    df = fetch_autopool_deposit_and_withdraw_events(autopool)
    deposit_df = df[df["event"] == "Deposit"].copy()
    withdraw_df = df[df["event"] == "Withdraw"].copy()
    daily_change_fig = _make_deposit_and_withdraw_figure(autopool, deposit_df, withdraw_df)
    scatter_plot_fig = _make_scatter_plot_figure(autopool, deposit_df, withdraw_df)
    return daily_change_fig, scatter_plot_fig


def fetch_and_render_autopool_deposit_and_withdraw_stats_data(autopool: AutopoolConstants):
    daily_change_fig, scatter_plot_fig = fetch_autopool_deposit_and_withdraw_stats_data(autopool)
    st.header("Autopool Deposit and Withdrawal Stats")
    st.plotly_chart(daily_change_fig, use_container_width=True)
    st.plotly_chart(scatter_plot_fig, use_container_width=True)


def fetch_autopool_deposit_and_withdraw_events(autopool: AutopoolConstants) -> pd.DataFrame:
    if should_update_table(DEPOSIT_AND_WITHDRAW_FROM_AUTOPOOL_TABLE):
        add_new_autopool_deposit_and_withdraw_events_to_table()

    rebalance_events_df = get_all_rows_in_table_by_autopool(DEPOSIT_AND_WITHDRAW_FROM_AUTOPOOL_TABLE, autopool)
    return rebalance_events_df


def add_new_autopool_deposit_and_withdraw_events_to_table():
    if should_update_table(DEPOSIT_AND_WITHDRAW_FROM_AUTOPOOL_TABLE):
        for autopool in ALL_AUTOPOOLS:
            highest_block_already_fetched = get_earliest_block_from_table_with_autopool(
                DEPOSIT_AND_WITHDRAW_FROM_AUTOPOOL_TABLE, autopool
            )
            deposit_and_withdraw_events_df = _fetch_autopool_deposit_and_withdraw_events_from_external_source(
                autopool, highest_block_already_fetched
            )
            write_dataframe_to_table(deposit_and_withdraw_events_df, DEPOSIT_AND_WITHDRAW_FROM_AUTOPOOL_TABLE)


def _fetch_autopool_deposit_and_withdraw_events_from_external_source(
    autopool: AutopoolConstants, start_block: int
) -> pd.DataFrame:
    contract = autopool.chain.client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
    deposit_df = fetch_events(contract.events.Deposit, start_block=start_block)
    withdraw_df = fetch_events(contract.events.Withdraw, start_block=start_block)

    deposit_df["normalized_assets"] = deposit_df["assets"].apply(lambda x: int(x) / 1e18)
    withdraw_df["normalized_assets"] = withdraw_df["assets"].apply(lambda x: int(x) / 1e18)

    cols = ["hash", "log_index", "block", "event", "normalized_assets"]

    df = pd.concat([deposit_df[cols], withdraw_df[cols]], axis=0)
    df = add_timestamp_to_df_with_block_column(df, autopool.chain).reset_index()
    df["autopool"] = autopool.name
    return df


def _make_deposit_and_withdraw_figure(
    autopool: AutopoolConstants, deposit_df: pd.DataFrame, withdraw_df: pd.DataFrame
) -> go.Figure:
    total_withdrawals_per_day = -withdraw_df["normalized_assets"].resample("1D").sum()
    total_deposits_per_day = deposit_df["normalized_assets"].resample("1D").sum()

    fig = go.Figure()

    fig.add_trace(
        go.Bar(x=total_withdrawals_per_day.index, y=total_withdrawals_per_day, name="Withdrawals", marker_color="red")
    )

    fig.add_trace(
        go.Bar(x=total_deposits_per_day.index, y=total_deposits_per_day, name="Deposits", marker_color="blue")
    )

    fig.update_layout(
        title=f"{autopool.name} Total Withdrawals and Deposits per Day",
        xaxis_tickformat="%Y-%m-%d",
        xaxis_title="Date",
        yaxis_title="ETH",
        barmode="group",
        xaxis_tickangle=-45,
        width=900,
        height=500,
    )

    return fig


def _make_scatter_plot_figure(
    autopool: AutopoolConstants, deposit_df: pd.DataFrame, withdraw_df: pd.DataFrame
) -> go.Figure:

    deposits = deposit_df[["normalized_assets"]]
    withdraws = -withdraw_df[["normalized_assets"]]

    # Concatenate withdraws and deposits
    change_df = pd.concat([withdraws, deposits])

    # Create a column to indicate whether the value is positive (above zero) or negative (below zero)
    change_df["color"] = change_df["normalized_assets"].apply(lambda x: "Deposit" if x >= 0 else "Withdrawal")

    # Plot with different colors based on the value of 'normalized_assets'
    fig = px.scatter(
        change_df,
        y="normalized_assets",
        size=change_df["normalized_assets"].abs(),
        color=change_df["color"],  # Use the new 'color' column to map colors
        color_discrete_map={"Deposit": "blue", "Withdrawal": "red"},
    )  # Specify color map

    fig.update_layout(
        title=f"{autopool.name} Individual Deposits and Withdrawals per Day",
        xaxis_tickformat="%Y-%m-%d",
        xaxis_title="Date",
        yaxis_title="ETH",
        barmode="group",
        xaxis_tickangle=-45,
        width=900,
        height=500,
    )

    return fig
