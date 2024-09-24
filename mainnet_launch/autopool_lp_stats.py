import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from mainnet_launch.constants import eth_client, AutopoolConstants
from mainnet_launch.get_events import fetch_events
from mainnet_launch.get_state_by_block import get_raw_state_by_blocks
from mainnet_launch.abis import AUTOPOOL_VAULT_ABI


start_block = 20759126  # Sep 15, 2024



def display_autopool_lp_stats(autopool: AutopoolConstants):
    st.header("Autopool Allocation Over Time By Destination")
    deposit_df, withdraw_df = _fetch_raw_deposit_and_withdrawal_dfs(autopool)
    daily_change_fig = _make_deposit_and_withdraw_figure(autopool, deposit_df, withdraw_df)
    lp_deposit_and_withdraw_df = _make_scatter_plot_figure(autopool, deposit_df, withdraw_df)

    st.header(f"{autopool.name} Our LP Stats")
    st.plotly_chart(daily_change_fig, use_container_width=True)
    st.plotly_chart(lp_deposit_and_withdraw_df, use_container_width=True)

    with st.expander(f"See explanation for Deposits and Withdrawals"):
        st.write(
            """
            - Total Deposits and Withdrawals per Day: Daily total Deposit and Withdrawals in ETH per day
            - Individual Deposits and Withdrawals per Day: Each point is scaled by the size of the deposit or withdrawal
            """
        )





def _make_unique_wallets_figure(deposit_df, withdraw_df) -> go.Figure:
    # cumulative count of wallets that have touched any autopool
    
    # group by day week etc
    pass

def _make_current_unique_wallets():
    # for each autopool get the cumulative current holders of each token. 
    pass

@st.cache_data(ttl=3600)  # 1 hours
def _fetch_raw_deposit_and_withdrawal_dfs(autopool: AutopoolConstants) -> tuple[pd.DataFrame, pd.DataFrame]:
    contract = eth_client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)

    deposit_df = fetch_events(contract.events.Deposit, start_block=start_block)
    withdraw_df = fetch_events(contract.events.Withdraw, start_block=start_block)

    blocks = list(set([*deposit_df["block"], *withdraw_df["block"]]))
    # calling with empty calls gets the block:timestamp
    block_and_timestamp_df = get_raw_state_by_blocks([], blocks, include_block_number=True).reset_index()

    deposit_df = pd.merge(deposit_df, block_and_timestamp_df, on="block", how="left")
    deposit_df.set_index("timestamp", inplace=True)
    withdraw_df = pd.merge(withdraw_df, block_and_timestamp_df, on="block", how="left")
    withdraw_df.set_index("timestamp", inplace=True)

    deposit_df["normalized_assets"] = deposit_df["assets"].apply(lambda x: int(x) / 1e18)
    withdraw_df["normalized_assets"] = withdraw_df["assets"].apply(lambda x: int(x) / 1e18)

    return deposit_df, withdraw_df


def _make_deposit_and_withdraw_figure(
    autopool: AutopoolConstants, deposit_df: pd.DataFrame, withdraw_df: pd.DataFrame
) -> go.Figure:
    total_withdrawals_per_day = -withdraw_df["normalized_assets"].resample("1D").sum()
    total_deposits_per_day = deposit_df["normalized_assets"].resample("1D").sum()

    fig = go.Figure()

    # Add total withdrawals
    fig.add_trace(
        go.Bar(x=total_withdrawals_per_day.index, y=total_withdrawals_per_day, name="Withdrawals", marker_color="red")
    )

    # Add total deposits
    fig.add_trace(
        go.Bar(x=total_deposits_per_day.index, y=total_deposits_per_day, name="Deposits", marker_color="blue")
    )

    # Update layout for better visualization
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
