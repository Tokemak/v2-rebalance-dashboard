import pandas as pd
import streamlit as st
from datetime import timedelta
from multicall import Call
from mainnet_launch.get_state_by_block import (
    get_raw_state_by_blocks,
    get_state_by_one_block,
    build_blocks_to_use,
    identity_with_bool_success,
    build_blocks_to_use,
)

from mainnet_launch.constants import AutopoolConstants, eth_client, ALL_AUTOPOOLS
import plotly.express as px
import numpy as np


def build_summary_stats_cleaning_function(autopool: AutopoolConstants):
    def _clean_summary_stats_info(success, summary_stats):
        if success is True:
            summary = {
                f"{autopool.name}_destination": summary_stats[0],
                f"{autopool.name}_baseApr": summary_stats[1] / 1e18,
                f"{autopool.name}_feeApr": summary_stats[2] / 1e18,
                f"{autopool.name}_incentiveApr": summary_stats[3] / 1e18,
                f"{autopool.name}_safeTotalSupply": summary_stats[4] / 1e18,
                f"{autopool.name}_priceReturn": summary_stats[5] / 1e18,
                f"{autopool.name}_maxDiscount": summary_stats[6] / 1e18,
                f"{autopool.name}_maxPremium": summary_stats[7] / 1e18,
                f"{autopool.name}_ownedShares": summary_stats[8] / 1e18,
                f"{autopool.name}_compositeReturn": summary_stats[9] / 1e18,
                f"{autopool.name}_pricePerShare": summary_stats[10] / 1e18,
            }
            return summary
        else:
            return None

    return _clean_summary_stats_info


def _build_summary_stats_call(
    autopool: AutopoolConstants,
    destination_vault_address: str,
    direction: str = "out",
    amount: int = 0,
) -> Call:
    # /// @notice Gets the safe price of the underlying LP token
    # /// @dev Price validated to be inside our tolerance against spot price. Will revert if outside.
    # /// @return price Value of 1 unit of the underlying LP token in terms of the base asset
    # function getValidatedSafePrice() external returns (uint256 price);
    # getDestinationSummaryStats uses getValidatedSafePrice. So when prices are outside tolerance this function reverts

    # TODO find a version of this function that won't revert,
    if direction == "in":
        direction_enum = 0
    elif direction == "out":
        direction_enum = 1
    return_types = "(address,uint256,uint256,uint256,uint256,int256,int256,int256,uint256,int256,uint256)"

    cleaning_function = build_summary_stats_cleaning_function(autopool)
    return Call(
        autopool.autopool_eth_strategy_addr,
        [
            f"getDestinationSummaryStats(address,uint8,uint256)({return_types})",
            destination_vault_address,
            direction_enum,
            amount,
        ],
        [(f"{autopool.name}_{destination_vault_address}", cleaning_function)],
    )


def _build_all_summary_stats_calls() -> list[Call]:

    # first we need the current destinations
    # not only works with the current destinations, can edit later to include remove destinations.
    # TODO add another tab, if needed of the removed destinations

    get_destinations_calls = [
        Call(a.autopool_eth_addr, "getDestinations()(address[])", [(a.name, identity_with_bool_success)])
        for a in ALL_AUTOPOOLS
    ]
    block = eth_client.eth.get_block("latest").number
    destinations = get_state_by_one_block(get_destinations_calls, block)

    summary_stats_calls = []
    for autopool in ALL_AUTOPOOLS:
        for destination in destinations[autopool.name]:
            call = _build_summary_stats_call(autopool, destination)
            summary_stats_calls.append(call)
    return summary_stats_calls


@st.cache_data(ttl=12 * 3600)  # 12 hours
def _fetch_summary_stats_data(blocks: list[int]) -> pd.DataFrame:
    summary_stats_calls = _build_all_summary_stats_calls()
    summary_stats_df = get_raw_state_by_blocks(summary_stats_calls, blocks)
    return summary_stats_df


def fetch_destination_summary_stats(blocks: list[int], autopool: AutopoolConstants) -> pd.DataFrame:
    summary_stats_df = _fetch_summary_stats_data(blocks)
    cols = [c for c in summary_stats_df if autopool.name in c[:10]]
    summary_stats_for_only_this_autopool = summary_stats_df[cols].copy()
    summary_stats_for_only_this_autopool.columns = [c.split("_")[1] for c in summary_stats_for_only_this_autopool]
    return summary_stats_for_only_this_autopool


if __name__ == "__main__":
    blocks = build_blocks_to_use()
    df = fetch_destination_summary_stats(blocks, ALL_AUTOPOOLS[0])


# @st.cache_data(ttl=12 * 3600)  # 12 hours
# def _fetch_summary_stats_df(blocks: list[int]) -> pd.DataFrame:
#     """Only fetch the summary stats once"""
#     calls = [nav_per_share_call(autopool.name, autopool.autopool_eth_addr) for autopool in ALL_AUTOPOOLS]
#     nav_per_share_df = get_raw_state_by_block(calls, blocks)
#     return nav_per_share_df


# def _summary_stats_df_to_figures(summary_stats_df: pd.DataFrame):
#     # Extract and process data
#     pricePerShare_df = summary_stats_df.map(lambda row: row["pricePerShare"] if isinstance(row, dict) else None).astype(
#         float
#     )
#     ownedShares_df = summary_stats_df.map(lambda row: row["ownedShares"] if isinstance(row, dict) else None).astype(
#         float
#     )
#     compositeReturn_df = summary_stats_df.map(
#         lambda row: row["compositeReturn"] if isinstance(row, dict) else None
#     ).astype(float)
#     compositeReturn_df = 100 * (compositeReturn_df.clip(upper=1).replace(1, np.nan).astype(float))
#     base = summary_stats_df.map(lambda row: row["baseApr"] if isinstance(row, dict) else None).astype(float)
#     fee = summary_stats_df.map(lambda row: row["feeApr"] if isinstance(row, dict) else None).astype(float)
#     incentive = summary_stats_df.map(lambda row: row["incentiveApr"] if isinstance(row, dict) else None).astype(float)
#     pR = summary_stats_df.map(lambda row: row["priceReturn"] if isinstance(row, dict) else None).astype(float)
#     uwcr_df = 100 * (base + fee + incentive + pR)
#     allocation_df = pricePerShare_df * ownedShares_df

#     # Limit to the last 90 days
#     end_date = allocation_df.index[-1]
#     start_date = end_date - timedelta(days=90)
#     filtered_allocation_df = allocation_df[(allocation_df.index >= start_date) & (allocation_df.index <= end_date)]

#     # Calculate portions for the area chart
#     portion_filtered_df = filtered_allocation_df.div(filtered_allocation_df.sum(axis=1), axis=0).fillna(0)

#     # Filter out columns with all zero allocations
#     portion_filtered_df = portion_filtered_df.loc[:, (portion_filtered_df != 0).any(axis=0)]

#     # Create a stacked area chart for allocation over time
#     allocation_area_fig = px.area(
#         portion_filtered_df * 100,
#         labels={"index": "", "value": "Percent Allocation"},
#         color_discrete_sequence=px.colors.qualitative.Set1,
#     )
#     allocation_area_fig.update_layout(
#         title=" ",
#         title_x=0.5,
#         margin=dict(l=40, r=40, t=40, b=80),
#         height=600,
#         width=800,
#         xaxis_title="",
#         font=dict(size=16),
#         legend=dict(font=dict(size=18), orientation="h", x=0.5, xanchor="center", y=-0.2),
#         legend_title_text="",
#         plot_bgcolor="white",  # Set plot background to white
#         paper_bgcolor="white",  # Set paper background to white
#         xaxis=dict(showgrid=True, gridcolor="lightgray"),  # Set x-axis grid lines to gray
#         yaxis=dict(showgrid=True, gridcolor="lightgray"),  # Set y-axis grid lines to gray
#     )

#     # Calculate weighted return
#     summary_stats_df["balETH_weighted_return"] = (compositeReturn_df * portion_filtered_df).sum(axis=1)
#     compositeReturn_df["balETH_weighted_return"] = (compositeReturn_df * portion_filtered_df).sum(axis=1)
#     uwcr_df["Expected_Return"] = (uwcr_df * portion_filtered_df).sum(axis=1)
#     # Create a line chart for weighted return
#     weighted_return_fig = px.line(
#         summary_stats_df, x=summary_stats_df.index, y="balETH_weighted_return", line_shape="linear", markers=True
#     )

#     weighted_return_fig.update_traces(
#         line=dict(width=8),
#         line_color="blue",
#         line_width=3,
#         line_dash="dash",
#         marker=dict(size=10, symbol="circle", color="blue"),
#     )

#     weighted_return_fig.update_layout(
#         title=" ",
#         title_x=0.5,
#         margin=dict(l=40, r=40, t=40, b=80),
#         height=600,
#         width=800,
#         font=dict(size=16),
#         yaxis_title="Weighted Return (%)",
#         xaxis_title="",
#         legend=dict(font=dict(size=18), orientation="h", x=0.5, xanchor="center", y=-0.2),
#         legend_title_text="",
#         plot_bgcolor="white",
#         paper_bgcolor="white",
#         xaxis=dict(showgrid=True, gridcolor="lightgray"),
#         yaxis=dict(showgrid=True, gridcolor="lightgray"),
#     )

#     # Create a combined line chart for weighted return and composite return
#     columns_to_plot = compositeReturn_df.columns[:]

#     combined_return_fig = px.line(compositeReturn_df, x=compositeReturn_df.index, y=columns_to_plot, markers=False)
#     combined_return_fig.update_traces(
#         line=dict(width=8),
#         selector=dict(name="balETH_weighted_return"),
#         line_color="blue",
#         line_dash="dash",
#         line_width=3,
#         marker=dict(size=10, symbol="circle", color="blue"),
#     )
#     combined_return_fig.update_layout(
#         title=" ",
#         title_x=0.5,
#         margin=dict(l=40, r=40, t=40, b=80),
#         height=600,
#         width=800,
#         font=dict(size=16),
#         yaxis_title="Return (%)",
#         xaxis_title="",
#         legend=dict(font=dict(size=18), orientation="h", x=0.5, xanchor="auto", y=-0.2),
#         legend_title_text="",
#         plot_bgcolor="white",
#         paper_bgcolor="white",
#         xaxis=dict(showgrid=True, gridcolor="lightgray"),
#         yaxis=dict(showgrid=True, gridcolor="lightgray"),
#     )

#     # Prepare data for pie chart
#     pie_df = allocation_df.copy()
#     pie_df["date"] = allocation_df.index
#     pie_data = pie_df.groupby("date").max().tail(1).T.reset_index()
#     pie_data.columns = ["Destination", "ETH Value"]
#     pie_data = pie_data[pie_data["ETH Value"] > 0]

#     # Create the pie chart
#     lp_allocation_pie_fig = px.pie(
#         pie_data, names="Destination", values="ETH Value", color_discrete_sequence=px.colors.qualitative.Pastel
#     )
#     lp_allocation_pie_fig.update_layout(
#         title=" ",
#         title_x=0.5,
#         margin=dict(l=40, r=40, t=40, b=40),
#         height=400,
#         width=800,
#         font=dict(size=16),
#         legend=dict(font=dict(size=18), orientation="h", x=0.5, xanchor="center"),
#         legend_title_text="",
#         plot_bgcolor="white",
#         paper_bgcolor="white",
#     )
#     lp_allocation_pie_fig.update_traces(textinfo="percent+label", hoverinfo="label+value+percent")

#     return allocation_area_fig, weighted_return_fig, combined_return_fig, lp_allocation_pie_fig, uwcr_df


# @st.cache_data(ttl=3 * 3600)
# def fetch_summary_stats_figures():
#     vaults_df = pd.read_csv(ROOT_DIR / "vaults.csv")
#     calls = [
#         build_summary_stats_call(
#             "idle",
#             balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS,
#             balETH_autopool_vault,
#             direction="out",
#             amount=0,
#         )
#     ]
#     for i, (destination_vault_address, vault_name) in enumerate(zip(vaults_df["vaultAddress"], vaults_df["name"])):
#         call = build_summary_stats_call(
#             name=f"{vault_name}_ {i}",  # some duplicate names here
#             autopool_eth_strategy_address=balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS,
#             destination_vault_address=destination_vault_address,
#             direction="out",
#             amount=0,
#         )
#         calls.append(call)
#     blocks = build_blocks_to_use()
#     summary_stats_df = sync_safe_get_raw_state_by_block(calls, blocks)

#     lp_allocation_bar_fig, cr_out_fig1, cr_out_fig2, lp_allocation_pie_fig, uwcr_df = _summary_stats_df_to_figures(
#         summary_stats_df
#     )
#     return lp_allocation_bar_fig, cr_out_fig1, cr_out_fig2, lp_allocation_pie_fig, uwcr_df
