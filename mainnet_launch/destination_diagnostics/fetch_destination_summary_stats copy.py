import pandas as pd
import streamlit as st
from datetime import timedelta
from multicall import Call
import plotly.express as px
import numpy as np


from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    get_state_by_one_block,
    identity_with_bool_success,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)
from mainnet_launch.lens_contract import fetch_pools_and_destinations_df
from mainnet_launch.constants import CACHE_TIME, AutopoolConstants, eth_client, ALL_AUTOPOOLS, AUTO_LRT
from mainnet_launch.destinations import (
    attempt_destination_address_to_vault_name,
    get_destination_details,
    DestinationDetails,
)

POINTS_HOOK = "0xA386067eB5F7Dc9b731fe1130745b0FB00c615C3"


@st.cache_data(ttl=CACHE_TIME)
def fetch_destination_summary_stats(
    blocks: list[int], autopool: AutopoolConstants
) -> list[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary_stats_df = build_autopool_summary_stats_df(autopool)

    uwcr_df, allocation_df, compositeReturn_out_df = clean_summary_stats_df(summary_stats_df)
    total_nav_df = allocation_df.sum(axis=1)
    destination_points_calls = _build_destination_points_calls()
    points_df = get_raw_state_by_blocks(destination_points_calls, blocks)

    return uwcr_df, allocation_df, compositeReturn_out_df, total_nav_df, summary_stats_df, points_df


def _fetch_autopool_destination_data(autopool: AutopoolConstants) -> pd.DataFrame:
    destination_details = get_destination_details()
    blocks = build_blocks_to_use()

    calls = [
        _build_summary_stats_call(dest.autopool, dest.vaultAddress)
        for dest in destination_details
        if dest.autopool == autopool
    ]

    autopool_all_destinations_summary_stats_df = get_raw_state_by_blocks(calls, blocks)

    return autopool_all_destinations_summary_stats_df, destination_details


def _filter_and_format_summary_stats(
    autopool: AutopoolConstants,
    autopool_all_destinations_summary_stats_df: pd.DataFrame,
    destination_details: list[DestinationDetails],
) -> pd.DataFrame:
    """Filters the summary stats DataFrame to only have data for the current destinations and formats it by destination vault name."""

    destination_addresses = [c for c in autopool_all_destinations_summary_stats_df.columns]
    destination_vault_address_to_destination = {d.vaultAddress: d for d in destination_details}

    def get_current_destinations_by_block(
        autopool: AutopoolConstants, getPoolsAndDestinations: pd.DataFrame
    ) -> list[str]:
        for a, list_of_destinations in zip(
            getPoolsAndDestinations["autopools"], getPoolsAndDestinations["destinations"]
        ):
            if a["poolAddress"].lower() == autopool.autopool_eth_addr.lower():
                return [dest["vaultAddress"] for dest in list_of_destinations]

    pools_and_destinations_df = fetch_pools_and_destinations_df()
    autopool_all_destinations_summary_stats_df["current_destinations"] = pools_and_destinations_df.apply(
        lambda row: get_current_destinations_by_block(autopool, row["getPoolsAndDestinations"]), axis=1
    )

    def _limit_destination_summary_stats_to_current_destinations(row: dict):
        active_destinations = {}
        for addr in destination_addresses:
            destination_details = destination_vault_address_to_destination[addr]
            active_destinations[destination_details.vault_name] = row[addr]
        return active_destinations

    destination_name_to_destination_summary_stats_df = pd.DataFrame.from_records(
        autopool_all_destinations_summary_stats_df.apply(
            _limit_destination_summary_stats_to_current_destinations, axis=1
        )
    )
    destination_name_to_destination_summary_stats_df.index = autopool_all_destinations_summary_stats_df.index

    return destination_name_to_destination_summary_stats_df


def build_autopool_summary_stats_df(autopool: AutopoolConstants) -> pd.DataFrame:
    """
    Returns a DataFrame where the columns are the destination vault name, and the values are the dict from getDestinationSummaryStats()

    example response:
    {
        'destination': '0xf9779aef9f77e78c857cb4a068c65ccbee25baac',
        'baseApr': 0.0196821087299046,
        'feeApr': 0.002059510749280563,
        'incentiveApr': 0.06320866588379175,
        'safeTotalSupply': 3107.5585939492885,
        'priceReturn': 0.000423177979394055,
        'maxDiscount': 0.000912399533874503,
        'maxPremium': 0.0,
        'ownedShares': 844.9280278590563,
        'compositeReturn': 0.07905259675399179,
        'pricePerShare': 1.016326271270958
    }

    """
    autopool_all_destinations_summary_stats_df, destination_details = _fetch_autopool_destination_data(autopool)

    destination_name_to_destination_summary_stats_df = _filter_and_format_summary_stats(
        autopool, autopool_all_destinations_summary_stats_df, destination_details
    )

    return destination_name_to_destination_summary_stats_df


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
        [(destination_vault_address, _clean_summary_stats_info)],
    )


def _build_destination_points_calls() -> list[Call]:
    destination_details = get_destination_details()

    destination_points_calls = [
        Call(
            POINTS_HOOK,
            ["destinationBoosts(address)(uint256)", dest.vaultAddress],
            [(dest.vaultAddress, safe_normalize_with_bool_success)],
        )
        for dest in destination_details
    ]
    return destination_points_calls


def clean_summary_stats_df(summary_stats_df: pd.DataFrame):
    uwcr_df = _extract_unweighted_composite_return_df(summary_stats_df)
    # allocation_df = _extract_allocation_df(summary_stats_df) # wrong, need another method other than using the names as keys
    # I think we keep the destinations as columns
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
    # this can have problems with timing, where we have funds in a destination but it is no longer active
    ownedShares_df = summary_stats_df.map(lambda row: row["ownedShares"] if isinstance(row, dict) else 0).astype(float)
    allocation_df = pricePerShare_df * ownedShares_df
    return allocation_df


if __name__ == "__main__":
    blocks = build_blocks_to_use()
    fetch_destination_summary_stats(blocks, AUTO_LRT)
