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
from mainnet_launch.constants import CACHE_TIME, AutopoolConstants, eth_client, ALL_AUTOPOOLS, AUTO_LRT, time_decorator
from mainnet_launch.destinations import (
    attempt_destination_address_to_readable_name,
    get_destination_details,
    DestinationDetails,
)

POINTS_HOOK = "0xA386067eB5F7Dc9b731fe1130745b0FB00c615C3"


@st.cache_data(ttl=CACHE_TIME)
def fetch_destination_summary_stats(blocks, autopool: AutopoolConstants) -> pd.DataFrame:
    """
    Returns a DataFrame where the columns are the destination vaultAddress, and the values are the destination summary stats
    and price return when amount=0 and direction=out

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
        'pricePerShare': 1.016326271270958,
        'pointsApr':None | .001
    }

    """
    destination_details = get_destination_details()
    summary_stats_df = _fetch_autopool_destination_df(blocks, destination_details, autopool)

    destination_points_calls = _build_destination_points_calls(autopool)
    points_df = get_raw_state_by_blocks(destination_points_calls, blocks)

    def _add_points_value_to_summary_stats(summary_stats_cell, points_cell: float | None):

        if summary_stats_cell is None:
            return None
        else:
            if points_cell is None:
                summary_stats_cell["points"] = None
            else:
                summary_stats_cell["points"] = float(points_cell)

            return summary_stats_cell

    destinations = [dest for dest in destination_details if dest.autopool == autopool]

    for dest in destinations:
        summary_stats_df[dest.vaultAddress] = summary_stats_df[dest.vaultAddress].combine(
            points_df[dest.vaultAddress], _add_points_value_to_summary_stats
        )

    summary_stats_df = _combine_migrated_destinations(summary_stats_df, destination_details)
    uwcr_df, allocation_df, compositeReturn_out_df, total_nav_df = _build_summary_stats_dfs(summary_stats_df)

    return uwcr_df, allocation_df, compositeReturn_out_df, total_nav_df, summary_stats_df, points_df


def _combine_migrated_destinations(
    summary_stats_df: pd.DataFrame, destination_details: set[DestinationDetails]
) -> pd.DataFrame:
    # Occasionally the same underlying destinations are added or removed from an autopool's currenct destinations
    # when this happens, the autopool can still own some shares of that destination, eg `ownedShares` > 0
    # so in that case, we want to use only the stats of the active destination and combine all the owned shares.

    dest_vault_to_destination = {d.vaultAddress: d for d in destination_details}

    def _combine_to_destination_name(row: pd.Series) -> pd.DataFrame:
        target_name = 'Tokemak-Wrapped Ether-Balancer weETH/rETH StablePool'
        # just add in the current destination, but combine the shares together

        merged_migration_data = {}
        for destination_vault_address in row.keys():
            if destination_vault_address != "current_destinations":
                
                destination_name = dest_vault_to_destination[destination_vault_address].vault_name
                if destination_name != target_name:
                    continue
                summary_stats_data: dict | None = row[destination_vault_address]

                if destination_name not in merged_migration_data:
                    merged_migration_data[destination_name] = summary_stats_data
                                    
                if summary_stats_data is None:
                    continue
                
                if merged_migration_data[destination_name] is None:
                    merged_migration_data[destination_name] = summary_stats_data
                
                elif (merged_migration_data[destination_name] is not None) and (summary_stats_data is not None):
                    if 'ownedShares' not in merged_migration_data[destination_name]:
                        raise ValueError(f'expected to find ownedShares but not in {merged_migration_data[destination_name]=}') 
                    merged_migration_data[destination_name]['ownedShares'] += summary_stats_data['ownedShares']


        return merged_migration_data

    merged_destination_df = pd.DataFrame.from_records(summary_stats_df.apply(_combine_to_destination_name, axis=1))
    return merged_destination_df


def _fetch_autopool_destination_df(
    blocks, destination_details: list[DestinationDetails], autopool: AutopoolConstants
) -> pd.DataFrame:
    calls = [
        _build_summary_stats_call(dest.autopool, dest) for dest in destination_details if dest.autopool == autopool
    ]
    destination_summary_stats_df = get_raw_state_by_blocks(calls, blocks)

    def get_current_destinations_by_block(
        autopool: AutopoolConstants, getPoolsAndDestinations: pd.DataFrame
    ) -> list[str]:
        for a, list_of_destinations in zip(
            getPoolsAndDestinations["autopools"], getPoolsAndDestinations["destinations"]
        ):
            if a["poolAddress"].lower() == autopool.autopool_eth_addr.lower():
                return [eth_client.toChecksumAddress(dest["vaultAddress"]) for dest in list_of_destinations]

    pools_and_destinations_df = fetch_pools_and_destinations_df()
    destination_summary_stats_df["current_destinations"] = pools_and_destinations_df.apply(
        lambda row: get_current_destinations_by_block(autopool, row["getPoolsAndDestinations"]), axis=1
    )

    return destination_summary_stats_df


def _clean_summary_stats_info(success, summary_stats):
    if success is True:
        summary = {
            "destination": summary_stats[0],  # address
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
            "pointsApr": None,  # added later
        }
        return summary
    else:
        return None


def _build_summary_stats_call(
    autopool: AutopoolConstants,
    dest: DestinationDetails,
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
            dest.vaultAddress,
            direction_enum,
            amount,
        ],
        [(dest.vaultAddress, _clean_summary_stats_info)],
    )


def _build_destination_points_calls(autopool: AutopoolConstants) -> list[Call]:
    destination_details = get_destination_details()

    destination_points_calls = [
        Call(
            POINTS_HOOK,
            ["destinationBoosts(address)(uint256)", dest.vaultAddress],
            [(dest.vaultAddress, safe_normalize_with_bool_success)],
        )
        for dest in destination_details
        if dest.autopool == autopool
    ]

    return destination_points_calls


def _build_summary_stats_dfs(summary_stats_df: pd.DataFrame):
    uwcr_df = _extract_unweighted_composite_return_df(summary_stats_df)
    allocation_df = _extract_allocation_df(summary_stats_df)
    compositeReturn_out_df = _extract_composite_return_out_df(summary_stats_df)

    total_nav_df = allocation_df.sum(axis=1)
    portion_df = allocation_df.div(total_nav_df, axis=0)
    uwcr_df["Expected_Return"] = (uwcr_df.fillna(0) * portion_df.fillna(0)).sum(axis=1)
    return uwcr_df, allocation_df, compositeReturn_out_df, total_nav_df


def _extract_composite_return_out_df(summary_stats_df: pd.DataFrame) -> pd.DataFrame:
    compositeReturn_out_df = summary_stats_df.map(
        lambda row: row["compositeReturn"] if isinstance(row, dict) else None
    ).astype(float)
    # If we dont have an estimate for safeTotalSupply the Composite Return Out can be very large
    # clip all CR >100% to None.
    compositeReturn_out_df = 100 * (compositeReturn_out_df.clip(upper=1).replace(1, np.nan).astype(float))
    return compositeReturn_out_df


def _extract_unweighted_composite_return_df(summary_stats_df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataframe of base + fee + incentive + price return + pointsApr for each destination in summary_stats_df"""
    base = summary_stats_df.map(lambda row: row["baseApr"] if isinstance(row, dict) else None).astype(float)
    fee = summary_stats_df.map(lambda row: row["feeApr"] if isinstance(row, dict) else None).astype(float)
    incentive = summary_stats_df.map(lambda row: row["incentiveApr"] if isinstance(row, dict) else None).astype(float)
    priceReturn = summary_stats_df.map(lambda row: row["priceReturn"] if isinstance(row, dict) else None).astype(float)
    points = summary_stats_df.map(lambda row: row["pointsApr"] if isinstance(row, dict) else None).astype(float)
    uwcr_df = 100 * (base + fee + incentive + priceReturn + points)
    return uwcr_df


def _extract_allocation_df(summary_stats_df: pd.DataFrame) -> pd.DataFrame:
    """Returns eth present ETH value in each destination"""
    pricePerShare_df = summary_stats_df.map(lambda row: row["pricePerShare"] if isinstance(row, dict) else 0).astype(
        float
    )
    ownedShares_df = summary_stats_df.map(lambda row: row["ownedShares"] if isinstance(row, dict) else 0).astype(float)
    allocation_df = pricePerShare_df * ownedShares_df
    return allocation_df


@time_decorator
def tester():
    blocks = build_blocks_to_use()
    uwcr_df, allocation_df, compositeReturn_out_df, total_nav_df, summary_stats_df, points_df = (
        fetch_destination_summary_stats(blocks, AUTO_LRT)
    )

    pass


if __name__ == "__main__":
    tester()
