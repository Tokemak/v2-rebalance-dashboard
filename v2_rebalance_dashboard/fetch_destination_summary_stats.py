import pandas as pd
import streamlit as st
from datetime import timedelta
from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    build_blocks_to_use,
    sync_get_raw_state_by_block_one_block,
)

from v2_rebalance_dashboard.constants import eth_client, balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS, ROOT_DIR
import plotly.express as px
import json

import numpy as np

import warnings

warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

with open(ROOT_DIR / "vault_abi.json", "r") as fin:
    vault_abi = json.load(fin)

with open(ROOT_DIR / "strategy_abi.json", "r") as fin:
    strategy_abi = json.load(fin)

balETH_autopool_vault = "0x72cf6d7C85FfD73F18a83989E7BA8C1c30211b73"
vault_contract = eth_client.eth.contract(balETH_autopool_vault, abi=vault_abi)

autoPool = eth_client.eth.contract(balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS, abi=strategy_abi)


def _clean_summary_stats_info(success, summary_stats):
    if success is True:
        summary = {
            "destination": summary_stats[0],
            "baseApr": summary_stats[1] / 1e18,
            "feeApr": summary_stats[2] / 1e18,
            "incentiveApr": summary_stats[3] / 1e18,
            "safeTotalSupply": summary_stats[4] / 1e18,
            "priceReturn": summary_stats[5] / 1e18,
            "maxDiscount": summary_stats[6] / 1e18,
            "maxPremium": summary_stats[7] / 1e18,
            "ownedShares": summary_stats[8] / 1e18,
            "compositeReturn": summary_stats[9] / 1e18,
            "pricePerShare": summary_stats[10] / 1e18,
            # ignoring slashings costs, no longer part of model
        }
        return summary
    else:
        return None


def build_summary_stats_call(
    name: str,
    autopool_eth_strategy_address: str,
    destination_vault_address: str,
    direction: str = "out",
    amount: int = 0,
) -> Call:

    # hasn't been an error so far
    # /// @notice Gets the safe price of the underlying LP token
    # /// @dev Price validated to be inside our tolerance against spot price. Will revert if outside.
    # /// @return price Value of 1 unit of the underlying LP token in terms of the base asset
    # function getValidatedSafePrice() external returns (uint256 price);

    # getDestinationSummaryStats uses getValidatedSafePrice, it can revert sometimes
    # None, commuicates, uncertaintity, the solver cannot re

    if direction == "in":
        direction_enum = 0
    elif direction == "out":
        direction_enum = 1
    # lose slashing info, intentionally
    return_types = "(address,uint256,uint256,uint256,uint256,int256,int256,int256,uint256,int256,uint256)"

    return Call(
        autopool_eth_strategy_address,
        [
            f"getDestinationSummaryStats(address,uint8,uint256)({return_types})",
            destination_vault_address,
            direction_enum,
            amount,
        ],
        [(name, _clean_summary_stats_info)],
    )

def _summary_stats_df_to_figures(summary_stats_df: pd.DataFrame):
    # Extract and process data
    pricePerShare_df = summary_stats_df.map(lambda row: row["pricePerShare"] if isinstance(row, dict) else None).astype(float)
    ownedShares_df = summary_stats_df.map(lambda row: row["ownedShares"] if isinstance(row, dict) else None).astype(float)
    compositeReturn_df = summary_stats_df.map(lambda row: row["compositeReturn"] if isinstance(row, dict) else None).astype(float)
    compositeReturn_df = 100 * (compositeReturn_df.clip(upper=1).replace(1, np.nan).astype(float))
    base = summary_stats_df.map(lambda row: row["baseApr"] if isinstance(row, dict) else None).astype(float)
    fee = summary_stats_df.map(lambda row: row["feeApr"] if isinstance(row, dict) else None).astype(float)
    incentive = summary_stats_df.map(lambda row: row["incentiveApr"] if isinstance(row, dict) else None).astype(float)
    pR = summary_stats_df.map(lambda row: row["priceReturn"] if isinstance(row, dict) else None).astype(float)
    uwcr_df = 100 * (base + fee + incentive + pR)
    allocation_df = pricePerShare_df * ownedShares_df

    # Limit to the last 90 days
    end_date = allocation_df.index[-1]
    start_date = end_date - timedelta(days=90)
    filtered_allocation_df = allocation_df[(allocation_df.index >= start_date) & (allocation_df.index <= end_date)]

    # Calculate portions for the area chart
    portion_filtered_df = filtered_allocation_df.div(filtered_allocation_df.sum(axis=1), axis=0).fillna(0)

    # Filter out columns with all zero allocations
    portion_filtered_df = portion_filtered_df.loc[:, (portion_filtered_df != 0).any(axis=0)]

    # Create a stacked area chart for allocation over time
    allocation_area_fig = px.area(
        portion_filtered_df,
        labels={"index": "", "value": "Allocation Proportion"},
        color_discrete_sequence=px.colors.qualitative.Set1
    )
    allocation_area_fig.update_layout(
        title = ' ',
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=600,
        width=800,
        xaxis_title="",
        font=dict(size=16),
        legend=dict(font=dict(size=18), orientation='h', x=0.5, xanchor='center', y=-0.2),
        legend_title_text='',
        plot_bgcolor='white',  # Set plot background to white
        paper_bgcolor='white',  # Set paper background to white
        xaxis=dict(showgrid=True, gridcolor='lightgray'),  # Set x-axis grid lines to gray
        yaxis=dict(showgrid=True, gridcolor='lightgray')   # Set y-axis grid lines to gray
    )

    # Calculate weighted return
    summary_stats_df["balETH_weighted_return"] = (compositeReturn_df * portion_filtered_df).sum(axis=1)
    compositeReturn_df["balETH_weighted_return"] = (compositeReturn_df * portion_filtered_df).sum(axis=1)
    uwcr_df["Expected_Return"] = (uwcr_df * portion_filtered_df).sum(axis=1)
    # Create a line chart for weighted return
    weighted_return_fig = px.line(
        summary_stats_df,
        x=summary_stats_df.index,
        y="balETH_weighted_return",
        line_shape="linear",
        markers=True
    )

    weighted_return_fig.update_traces(
        line=dict(width=8),
        line_color="blue",
        line_width=4,
        line_dash = "dash",
        marker=dict(size=10, symbol='circle', color='blue') 
    )

    weighted_return_fig.update_layout(
        title = ' ',
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=600,
        width=800,
        font=dict(size=16),
        yaxis_title="Weighted Return (%)",
        xaxis_title="",
        legend=dict(font=dict(size=18), orientation='h', x=0.5, xanchor='center', y=-0.2),
        legend_title_text='',
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray')
    )

    # Create a combined line chart for weighted return and composite return
    columns_to_plot = compositeReturn_df.columns[:]

    combined_return_fig = px.line(
        compositeReturn_df,
        x=compositeReturn_df.index,
        y=columns_to_plot,
        markers=False
    )
    combined_return_fig.update_traces(
        line=dict(width=8),
        selector=dict(name="balETH_weighted_return"),
        line_color="blue",
        line_dash="dash",
        line_width=4,
        marker=dict(size=10, symbol='circle', color='blue') 
    )
    combined_return_fig.update_layout(
        title = ' ',
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=600,
        width=800,
        font=dict(size=16),
        yaxis_title="Return (%)",
        xaxis_title="",
        legend=dict(font=dict(size=18), orientation='h', x=0.5, xanchor='auto', y=-0.2),
        legend_title_text='',
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray')
    )

    # Prepare data for pie chart
    pie_df = allocation_df.copy()
    pie_df["date"] = allocation_df.index
    pie_data = pie_df.groupby("date").max().tail(1).T.reset_index()
    pie_data.columns = ["Destination", "ETH Value"]
    pie_data = pie_data[pie_data["ETH Value"] > 0]

    # Create the pie chart
    lp_allocation_pie_fig = px.pie(
        pie_data,
        names="Destination",
        values="ETH Value",
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    lp_allocation_pie_fig.update_layout(
        title = ' ',
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=400,
        width=800,
        font=dict(size=16),
        legend=dict(font=dict(size=18), orientation="h", x=0.5, xanchor="center"),
        legend_title_text='',
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    lp_allocation_pie_fig.update_traces(textinfo='percent+label', hoverinfo='label+value+percent')

    # Plot unweighted CR
    uwcr_return_fig = px.line(uwcr_df, y='Expected_Return', title=' ')
    uwcr_return_fig.update_traces(line=dict(width=3))
    uwcr_return_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=400,
        width=800,
        font=dict(size=16),
        yaxis_title='Expected Annualized Return (%)',
        xaxis_title='',
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray')
    )

    return allocation_area_fig, weighted_return_fig, combined_return_fig, lp_allocation_pie_fig, uwcr_return_fig

@st.cache_data(ttl=12*3600)
def fetch_summary_stats_figures():
    vaults_df = pd.read_csv(ROOT_DIR / "vaults.csv")
    calls = [
        build_summary_stats_call(
            "idle",
            balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS,
            balETH_autopool_vault,
            direction="out",
            amount=0,
        )
    ]
    for i, (destination_vault_address, vault_name) in enumerate(zip(vaults_df["vaultAddress"], vaults_df["name"])):
        call = build_summary_stats_call(
            name=f"{vault_name}_ {i}",  # some duplicate names here
            autopool_eth_strategy_address=balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS,
            destination_vault_address=destination_vault_address,
            direction="out",
            amount=0,
        )
        calls.append(call)
    blocks = build_blocks_to_use()
    summary_stats_df = sync_safe_get_raw_state_by_block(calls, blocks)

    lp_allocation_bar_fig, cr_out_fig1, cr_out_fig2, lp_allocation_pie_fig, uwcr_return_fig = _summary_stats_df_to_figures(summary_stats_df)
    return lp_allocation_bar_fig, cr_out_fig1, cr_out_fig2, lp_allocation_pie_fig, uwcr_return_fig
