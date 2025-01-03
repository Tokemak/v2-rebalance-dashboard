import pandas as pd
from multicall import Call
import numpy as np
from web3 import Web3


from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)
from mainnet_launch.lens_contract import fetch_pools_and_destinations_df
from mainnet_launch.constants import (
    AutopoolConstants,
    ALL_AUTOPOOLS,
    AUTO_LRT,
    POINTS_HOOK,
    ChainData,
)
from mainnet_launch.destinations import (
    get_destination_details,
    DestinationDetails,
)
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column

from mainnet_launch.data_fetching.new_databases import write_dataframe_to_table, does_table_exist, run_read_only_query
from mainnet_launch.data_fetching.should_update_database import should_update_table


DESTINATION_SUMMARY_STATS_TABLE = "DESTINATION_SUMMARY_STATS_TABLE"
SUMMARY_STATS_FIELDS = [
    "baseApr",
    "feeApr",
    "incentiveApr",
    "safeTotalSupply",
    "priceReturn",
    "maxDiscount",
    "maxPremium",
    "ownedShares",
    "compositeReturn",
    "pricePerShare",
    "pointsApr",
]


def _add_new_destination_summary_stats_to_table() -> None:
    for autopool in ALL_AUTOPOOLS:
        highest_block_already_fetched = _get_highest_block_to_fetch_for_destination_summary_stats(autopool)
        blocks = [b for b in build_blocks_to_use(autopool.chain) if b >= highest_block_already_fetched]

        flat_summary_stats_df = _fetch_destination_summary_stats_from_external_source(autopool, blocks)
        write_dataframe_to_table(flat_summary_stats_df, DESTINATION_SUMMARY_STATS_TABLE)


def _fetch_destination_summary_stats_from_external_source(
    autopool: AutopoolConstants, blocks: list[int]
) -> pd.DataFrame:
    destination_details = get_destination_details(autopool)
    raw_summary_stats_df = _get_earliest_raw_summary_stats_that_does_not_revert(blocks, destination_details, autopool)
    summary_stats_df = _combine_migrated_destinations(autopool, raw_summary_stats_df, destination_details)
    # note the blocks here are not exactly accurate, but are approximatly accurate
    # if the getDestinationsummaryStats() call reverts we get the price upto 30 minutes in the past, in 10 minute chunks
    # so the block and timestamp can be up to 30 minute old.
    # note this is only a very small amount of data
    summary_stats_df["block"] = blocks
    flat_summary_stats_df = _flatten_summary_stats_df(summary_stats_df, autopool)
    return flat_summary_stats_df


def _flatten_summary_stats_df(summary_stats_df: pd.DataFrame, autopool: AutopoolConstants) -> pd.DataFrame:
    merged_df = None
    for col in SUMMARY_STATS_FIELDS:
        df = summary_stats_df.map(lambda row: row[col] if isinstance(row, dict) else 0).astype(float)
        df["block"] = summary_stats_df["block"]

        long_form_df = pd.melt(df, id_vars=["block"], var_name="destination", value_name=col)
        if merged_df is None:
            merged_df = long_form_df.copy()
        else:
            merged_df = pd.merge(merged_df, long_form_df, how="inner", on=["block", "destination"])

    merged_df["autopool"] = autopool.name
    return merged_df


def _get_highest_block_to_fetch_for_destination_summary_stats(autopool: AutopoolConstants) -> int:
    if does_table_exist(DESTINATION_SUMMARY_STATS_TABLE):
        query = f"""
        SELECT max(block) as highest_found_block from {DESTINATION_SUMMARY_STATS_TABLE}
        where autopool = ?
        """
        params = (autopool.name,)
        df = run_read_only_query(query, params)

        possible_highest_block = df["highest_found_block"].values[0]
        if possible_highest_block is None:
            return autopool.chain.block_autopool_first_deployed
        else:
            return int(possible_highest_block)
    else:
        return autopool.chain.block_autopool_first_deployed


def _get_earliest_raw_summary_stats_that_does_not_revert(
    blocks: list[int], destination_details: list[DestinationDetails], autopool: AutopoolConstants
) -> pd.DataFrame:
    """
    Returns a DataFrame where the columns are the destination vaultName,
    and the values are the destination summary stats
    and price return when amount=0 and direction=out

    combines ownedShares across all shares

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
        'pointsApr': .001
    }

    if a call reverts try that same call [X minute in the past], now only 30 but could be more

    There is not a garentuee this works, but it should prevent most missing values

    there is some small risk of conflict if a rebalance occurs with the destination we are looking at during between the minutes

    that could cause errors, but it is unlikely

    """

    current_df = _fetch_autopool_destination_df(blocks, destination_details, autopool)

    # in 10 minute chunks look back and use the soonest value that does not revert
    # since getValidatedSpotPrice(lpToken) ocassionally reverts for small windows

    blocks_per_minute = round(60 / autopool.chain.approx_seconds_per_block)
    for num_minutes in [30]:
        # approx
        blocks_in_the_past = [b - (blocks_per_minute * num_minutes) for b in blocks]
        previous_df = _fetch_autopool_destination_df(blocks_in_the_past, destination_details, autopool)

        # if current is nan and previous is not nan, use previous, else use current
        replaced_values = np.where(
            pd.isna(current_df.values) & ~pd.isna(previous_df.values), previous_df.values, current_df.values
        )
        current_df = pd.DataFrame(replaced_values, columns=current_df.columns, index=current_df.index)

    return current_df


def _combine_migrated_destinations(
    autopool: AutopoolConstants, summary_stats_df: pd.DataFrame, destination_details: set[DestinationDetails]
) -> pd.DataFrame:
    # Occasionally the same underlying destinations are added or removed from an autopool's currenct destinations
    # when this happens, the autopool can still own some shares of that destination, eg `ownedShares` > 0
    # so in that case, we want to use only the stats of the active destination and combine all the owned shares.
    def _combine_to_destination_name(row: pd.Series) -> pd.DataFrame:
        # just add in the current destination, but combine the ownedShares together by destination.vault_name

        ownedShares_record = {d.vault_name: 0 for d in destination_details}
        for dest in destination_details:
            summary_stats_data = row[dest.vaultAddress]
            if summary_stats_data is not None:
                ownedShares_record[dest.vault_name] += summary_stats_data["ownedShares"]

        name_to_summary_records = {}
        for dest in destination_details:
            if dest.vaultAddress in [*row["current_destinations"], autopool.autopool_eth_addr]:
                summary_stats_data = row[dest.vaultAddress]
                if summary_stats_data is not None:
                    # overwrite the ownShares so that it counts the shares in deprecated destinations
                    summary_stats_data["ownedShares"] = ownedShares_record[dest.vault_name]
                name_to_summary_records[dest.vault_name] = summary_stats_data
        return name_to_summary_records

    merged_destination_df = pd.DataFrame.from_records(summary_stats_df.apply(_combine_to_destination_name, axis=1))
    merged_destination_df.index = summary_stats_df.index
    return merged_destination_df


def _fetch_autopool_destination_df(
    blocks, destination_details: list[DestinationDetails], autopool: AutopoolConstants
) -> pd.DataFrame:
    calls = [_build_summary_stats_call(dest.autopool, dest) for dest in destination_details]
    summary_stats_df = get_raw_state_by_blocks(calls, blocks, autopool.chain, include_block_number=True)

    def get_current_destinations_by_block(
        autopool: AutopoolConstants, getPoolsAndDestinations: pd.DataFrame
    ) -> list[str]:
        for a, list_of_destinations in zip(
            getPoolsAndDestinations["autopools"], getPoolsAndDestinations["destinations"]
        ):
            if a["poolAddress"].lower() == autopool.autopool_eth_addr.lower():
                return [Web3.toChecksumAddress(dest["vaultAddress"]) for dest in list_of_destinations]

    pools_and_destinations_df = fetch_pools_and_destinations_df(autopool.chain, blocks)

    summary_stats_df["current_destinations"] = pools_and_destinations_df.apply(
        lambda row: get_current_destinations_by_block(autopool, row["getPoolsAndDestinations"]), axis=1
    )

    destination_points_calls = _build_destination_points_calls(destination_details, autopool.chain)
    points_df = get_raw_state_by_blocks(destination_points_calls, blocks, autopool.chain)

    def _add_points_value_to_summary_stats(summary_stats_cell, points_cell: float | None):

        if summary_stats_cell is None:
            return None
        else:
            if points_cell is None:
                summary_stats_cell["pointsApr"] = 0
            else:
                summary_stats_cell["pointsApr"] = float(points_cell)

            return summary_stats_cell

    for dest in destination_details:
        summary_stats_df[dest.vaultAddress] = summary_stats_df[dest.vaultAddress].combine(
            points_df[dest.vaultAddress], _add_points_value_to_summary_stats
        )

    return summary_stats_df


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
            "pointsApr": None,  # set later
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


def _build_destination_points_calls(destination_details: set[DestinationDetails], chain: ChainData) -> list[Call]:

    destination_points_calls = [
        Call(
            POINTS_HOOK(chain),
            ["destinationBoosts(address)(uint256)", dest.vaultAddress],
            [(dest.vaultAddress, safe_normalize_with_bool_success)],
        )
        for dest in destination_details
    ]

    return destination_points_calls


def fetch_destination_summary_stats(autopool: AutopoolConstants, summary_stats_field: str):
    if summary_stats_field not in SUMMARY_STATS_FIELDS:
        raise ValueError(f"Can only fetch {SUMMARY_STATS_FIELDS=} you tried to fetch {summary_stats_field=}")

    if should_update_table(DESTINATION_SUMMARY_STATS_TABLE):
        _add_new_destination_summary_stats_to_table()

    query = f"""
        SELECT destination, block, {summary_stats_field} from {DESTINATION_SUMMARY_STATS_TABLE}
        WHERE autopool = ?
        """
    params = (autopool.name,)
    long_summary_stats_df = run_read_only_query(query, params)
    summary_stats_df = pd.pivot(long_summary_stats_df, columns=['destination'], values=summary_stats_field, index='block')
    summary_stats_df = summary_stats_df.reset_index()
    summary_stats_df = add_timestamp_to_df_with_block_column(summary_stats_df, autopool.chain)
    

    return summary_stats_df


if __name__ == "__main__":
    summary_stats_df = fetch_destination_summary_stats(AUTO_LRT)

    pass
