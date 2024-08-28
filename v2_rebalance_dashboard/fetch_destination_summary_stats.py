import pandas as pd

from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    build_blocks_to_use,
    sync_get_raw_state_by_block_one_block,
)

from v2_rebalance_dashboard.constants import eth_client, balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS
import plotly.express as px
import json

import numpy as np

import warnings

warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

with open("/home/parker/Documents/Tokemak/v2-rebalance-dashboard/v2_rebalance_dashboard/vault_abi.json", "r") as fin:
    vault_abi = json.load(fin)

with open("/home/parker/Documents/Tokemak/v2-rebalance-dashboard/v2_rebalance_dashboard/strategy_abi.json", "r") as fin:
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
    pricePerShare_df = summary_stats_df.map(lambda row: row["pricePerShare"] if isinstance(row, dict) else None).astype(
        float
    )
    ownedShares_df = summary_stats_df.map(lambda row: row["ownedShares"] if isinstance(row, dict) else None).astype(
        float
    )
    compositeReturn_df = summary_stats_df.map(
        lambda row: row["compositeReturn"] if isinstance(row, dict) else None
    ).astype(float)
    # clean up spikes up
    compositeReturn_df = 100 * (compositeReturn_df.clip(upper=1).replace(1, np.nan).astype(float))
    allocation_df = pricePerShare_df * ownedShares_df
    # LIMIT BY Destintion where value >0
    pie_df = allocation_df.copy()
    pie_df["date"] = allocation_df.index
    pie_data = pie_df.groupby("date").max().tail(1).T.reset_index()
    pie_data.columns = ["Destination", "ETH Value"]

    pie_data = pie_data[pie_data["ETH Value"] > 0]
    lp_allocation_pie_fig = px.pie(
        pie_data, names="Destination", values="ETH Value", title="Current ETH Value by Destination"
    )

    lp_allocation_pie_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=600,
        width=600 * 3,
    )

    lp_allocation_bar_fig = px.bar(allocation_df)

    lp_allocation_bar_fig.update_layout(
        # not attached to these settings
        title="ETH In Each Destination",
        xaxis_title="Date",
        yaxis_title="ETH Per Destination",
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=600,
        width=600 * 3,
    )

    portion_df = allocation_df.copy()
    eth_nav = allocation_df.sum(axis=1)
    for col in allocation_df.columns:
        portion_df[col] = allocation_df[col] / eth_nav

    portion_df["balETH_weighted_return"] = 0.0  # to make the shapes match
    compositeReturn_df["balETH_weighted_return"] = (compositeReturn_df * portion_df).sum(axis=1)

    cr_out_fig = px.line(compositeReturn_df)
    cr_out_fig.update_layout(
        # not attached to these settings
        title="balETH Weighted Composite Return Out vs Other Destinations",
        xaxis_title="Date",
        yaxis_title="Composite Return Percent",
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=600,
        width=600 * 3,
    )

    cr_out_fig.update_traces(
        line=dict(width=2),
        selector=dict(name="balETH_weighted_return"),
        line_color="red",
        line_dash="dash",
        line_width=4,
    )

    return lp_allocation_bar_fig, cr_out_fig, lp_allocation_pie_fig


def fetch_summary_stats_figures():
    vaults_df = pd.read_csv("/home/parker/Documents/Tokemak/v2-rebalance-dashboard/v2_rebalance_dashboard/vaults.csv")
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

    lp_allocation_bar_fig, cr_out_fig, lp_allocation_pie_fig = _summary_stats_df_to_figures(summary_stats_df)
    return lp_allocation_bar_fig, cr_out_fig, lp_allocation_pie_fig
