import pandas as pd
import streamlit as st
from datetime import timedelta
from multicall import Call
import plotly.express as px
import numpy as np


from mainnet_launch.get_state_by_block import (
    get_raw_state_by_blocks,
    get_state_by_one_block,
    build_blocks_to_use,
    identity_with_bool_success,
    build_blocks_to_use,
)

from mainnet_launch.constants import AutopoolConstants, eth_client, ALL_AUTOPOOLS

from mainnet_launch.destinations import attempt_destination_address_to_symbol


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

    # cleaning_function = build_summary_stats_cleaning_function(autopool)
    return Call(
        autopool.autopool_eth_strategy_addr,
        [
            f"getDestinationSummaryStats(address,uint8,uint256)({return_types})",
            destination_vault_address,
            direction_enum,
            amount,
        ],
        [(f"{autopool.name}_{destination_vault_address}", _clean_summary_stats_info)],
    )


def _build_all_summary_stats_calls(blocks: list[int]) -> list[Call]:
    get_destinations_calls = [
        Call(a.autopool_eth_addr, "getDestinations()(address[])", [(a.name, identity_with_bool_success)])
        for a in ALL_AUTOPOOLS
    ]
    block = max(blocks)
    destinations = get_state_by_one_block(get_destinations_calls, block)
    # dict of [autopool.name: [list of current destination vaults]]

    summary_stats_calls = []
    for autopool in ALL_AUTOPOOLS:
        for destination in destinations[autopool.name]:
            call = _build_summary_stats_call(autopool, eth_client.toChecksumAddress(destination))
            summary_stats_calls.append(call)

    for autopool in ALL_AUTOPOOLS:
        # summary stats on idle ETH
        call = _build_summary_stats_call(autopool, eth_client.toChecksumAddress(autopool.autopool_eth_addr))
        summary_stats_calls.append(call)

    return summary_stats_calls


def _fetch_summary_stats_data(blocks: list[int]) -> pd.DataFrame:
    summary_stats_calls = _build_all_summary_stats_calls(blocks)
    summary_stats_df = get_raw_state_by_blocks(summary_stats_calls, blocks)
    return summary_stats_df


def fetch_destination_summary_stats(
    blocks: list[int], autopool: AutopoolConstants
) -> list[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    all_autopool_summary_stats_df = _fetch_summary_stats_data(blocks)
    # check if the autopool name prefix is in the columns
    cols = [c for c in all_autopool_summary_stats_df if autopool.name in c[:10]]
    summary_stats_df = all_autopool_summary_stats_df[cols].copy()
    # columns look like "balETH_0x148Ca723BefeA7b021C399413b8b7426A4701500"
    # extract out only the destination address
    summary_stats_df.columns = [c.split("_")[1] for c in summary_stats_df]

    uwcr_df, allocation_df, compositeReturn_out_df = clean_summary_stats_df(summary_stats_df)
    total_nav_df = allocation_df.sum(axis=1)
    uwcr_df.columns = [attempt_destination_address_to_symbol(c) for c in uwcr_df.columns]
    allocation_df.columns = [attempt_destination_address_to_symbol(c) for c in allocation_df.columns]
    compositeReturn_out_df.columns = [attempt_destination_address_to_symbol(c) for c in compositeReturn_out_df.columns]
    return uwcr_df, allocation_df, compositeReturn_out_df, total_nav_df, summary_stats_df


def clean_summary_stats_df(summary_stats_df: pd.DataFrame):
    uwcr_df = _extract_unweighted_composite_return_df(summary_stats_df)
    allocation_df = _extract_allocation_df(summary_stats_df)
    total_nav_df = allocation_df.sum(axis=1)
    portion_df = allocation_df.div(total_nav_df, axis=0)
    uwcr_df["Expected_Return"] = (uwcr_df.fillna(0) * portion_df.fillna(0)).sum(axis=1)

    compositeReturn_out_df = summary_stats_df.map(
        lambda row: row["compositeReturn"] if isinstance(row, dict) else None
    ).astype(float)
    # fix issue where composite return out can be massive
    compositeReturn_out_df = 100 * (compositeReturn_out_df.clip(upper=1).replace(1, np.nan).astype(float))
    return uwcr_df, allocation_df, compositeReturn_out_df


def _extract_unweighted_composite_return_df(summary_stats_df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of base + fee + incentive + price return for each destination in summary_stats_df"""
    base = summary_stats_df.map(lambda row: row["baseApr"] if isinstance(row, dict) else None).astype(float)
    fee = summary_stats_df.map(lambda row: row["feeApr"] if isinstance(row, dict) else None).astype(float)
    incentive = summary_stats_df.map(lambda row: row["incentiveApr"] if isinstance(row, dict) else None).astype(float)
    pR = summary_stats_df.map(lambda row: row["priceReturn"] if isinstance(row, dict) else None).astype(float)
    uwcr_df = 100 * (base + fee + incentive + pR)
    return uwcr_df


def _extract_allocation_df(summary_stats_df: pd.DataFrame) -> pd.DataFrame:
    """Returns eth present ETH value in each destiantion"""
    pricePerShare_df = summary_stats_df.map(lambda row: row["pricePerShare"] if isinstance(row, dict) else 0).astype(
        float
    )
    ownedShares_df = summary_stats_df.map(lambda row: row["ownedShares"] if isinstance(row, dict) else 0).astype(float)
    allocation_df = pricePerShare_df * ownedShares_df
    return allocation_df
