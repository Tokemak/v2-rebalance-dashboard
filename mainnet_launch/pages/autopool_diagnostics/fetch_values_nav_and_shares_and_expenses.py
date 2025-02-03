"""Returns of the autopool before and after expenses and fees"""

import pandas as pd
from multicall import Call

from mainnet_launch.constants import AutopoolConstants, AUTO_ETH

from mainnet_launch.pages.rebalance_events.rebalance_events import (
    fetch_rebalance_events_df,
)

from mainnet_launch.pages.protocol_level_profit_and_loss.fees import (
    fetch_all_autopool_fee_events,
)
from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats
from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS


from mainnet_launch.data_fetching.get_state_by_block import (
    build_blocks_to_use,
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
)
from mainnet_launch.database.database_operations import (
    write_dataframe_to_table,
    run_read_only_query,
    get_earliest_block_from_table_with_autopool,
    get_all_rows_in_table_by_autopool,
)

from mainnet_launch.database.should_update_database import (
    should_update_table,
)


ACUTAL_NAV_AND_SHARES_TABLE = "ACUTAL_NAV_AND_SHARES_TABLE"


def add_new_acutal_nav_and_acutal_shares_to_table():
    if should_update_table(ACUTAL_NAV_AND_SHARES_TABLE):
        for autopool in ALL_AUTOPOOLS:
            highest_block_already_fetched = get_earliest_block_from_table_with_autopool(
                ACUTAL_NAV_AND_SHARES_TABLE, autopool
            )
            # I don't think this is the right pattern, maybe make a better one,
            # eg handle the empty dataframe in write_dataframe_to_table and get_raw_state_by_blocks
            blocks = [b for b in build_blocks_to_use(autopool.chain) if b > highest_block_already_fetched]
            if len(blocks) > 0:
                nav_and_shares_df = _fetch_actual_nav_per_share_by_day(autopool, blocks)
                write_dataframe_to_table(nav_and_shares_df, ACUTAL_NAV_AND_SHARES_TABLE)


def _fetch_actual_nav_per_share_by_day(autopool: AutopoolConstants, blocks: list[int]) -> pd.DataFrame:
    def handle_getAssetBreakdown(success, AssetBreakdown):
        if success:
            totalIdle, totalDebt, totalDebtMin, totalDebtMax = AssetBreakdown
            return int(totalIdle + totalDebt) / 1e18
        return None

    calls = [
        Call(
            autopool.autopool_eth_addr,
            ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
            [("actual_nav", handle_getAssetBreakdown)],
        ),
        Call(
            autopool.autopool_eth_addr,
            ["totalSupply()(uint256)"],
            [("actual_shares", safe_normalize_with_bool_success)],
        ),
    ]

    df = get_raw_state_by_blocks(calls, blocks, autopool.chain, include_block_number=True).reset_index()
    df["autopool"] = autopool.name
    return df


def fetch_actual_nav_and_actual_shares(autopool: AutopoolConstants) -> pd.DataFrame:
    add_new_acutal_nav_and_acutal_shares_to_table()
    df = get_all_rows_in_table_by_autopool(ACUTAL_NAV_AND_SHARES_TABLE, autopool)
    daily_nav_shares_df = df.resample("1D").last()
    return daily_nav_shares_df


def fetch_nav_and_shares_and_factors_that_impact_nav_per_share(autopool: AutopoolConstants) -> pd.DataFrame:
    daily_nav_shares_df = fetch_actual_nav_and_actual_shares(autopool)
    cumulative_new_shares_df = _fetch_daily_shares_minted_to_fees(autopool)
    cumulative_rebalance_from_idle_swap_cost, cumulative_rebalance_not_from_idle_swap_cost = (
        _fetch_daily_nav_lost_to_rebalances(autopool)
    )
    implied_extra_nav_if_price_return_is_zero = _fetch_implied_extra_nav_if_price_return_is_zero(autopool)

    df = pd.concat(
        [
            daily_nav_shares_df,
            cumulative_new_shares_df,
            cumulative_rebalance_from_idle_swap_cost,
            cumulative_rebalance_not_from_idle_swap_cost,
            implied_extra_nav_if_price_return_is_zero,
        ],
        axis=1,
    )
    df = df.fillna(0)
    return df


def _fetch_daily_nav_lost_to_rebalances(autopool: AutopoolConstants) -> pd.DataFrame:
    rebalance_df = fetch_rebalance_events_df(autopool)

    rebalance_from_idle_df = rebalance_df[
        rebalance_df["outDestinationVault"].str.lower() == autopool.autopool_eth_addr.lower()
    ].copy()
    rebalance_not_from_idle_df = rebalance_df[
        ~(rebalance_df["outDestinationVault"].str.lower() == autopool.autopool_eth_addr.lower())
    ].copy()

    # clip swap cost to 0
    daily_rebalance_from_idle_swap_cost = rebalance_from_idle_df["swapCost"].resample("1D").sum()
    daily_rebalance_from_idle_swap_cost.name = "rebalance_from_idle_swap_cost"

    daily_rebalance_not_from_idle_swap_cost = rebalance_not_from_idle_df["swapCost"].resample("1D").sum()
    daily_rebalance_not_from_idle_swap_cost.name = "rebalance_not_idle_swap_cost"

    # daily_rebalance_from_idle_swap_cost = (
    #     (rebalance_from_idle_df["outEthValue"] - rebalance_from_idle_df["inEthValue"]).resample("1D").sum()
    # )
    # daily_rebalance_from_idle_swap_cost.name = "rebalance_from_idle_swap_cost"

    # daily_rebalance_not_from_idle_swap_cost = (
    #     (rebalance_not_from_idle_df["outEthValue"] - rebalance_not_from_idle_df["inEthValue"]).resample("1D").sum()
    # )
    # daily_rebalance_not_from_idle_swap_cost.name = "rebalance_not_idle_swap_cost"

    return daily_rebalance_from_idle_swap_cost, daily_rebalance_not_from_idle_swap_cost


def _fetch_daily_shares_minted_to_fees(autopool: AutopoolConstants) -> pd.DataFrame:
    fee_df = fetch_all_autopool_fee_events(autopool)[
        ["new_shares_from_periodic_fees", "new_shares_from_streaming_fees"]
    ]
    daily_fee_share_df = fee_df.resample("1D").sum()
    return daily_fee_share_df


def _fetch_implied_extra_nav_if_price_return_is_zero(autopool: AutopoolConstants) -> pd.DataFrame:
    pricePerShare_df = fetch_destination_summary_stats(autopool, "pricePerShare")
    ownedShares_df = fetch_destination_summary_stats(autopool, "ownedShares")
    allocation_df = pricePerShare_df * ownedShares_df

    priceReturn_df = fetch_destination_summary_stats(autopool, "priceReturn")

    implied_extra_nav_if_price_return_is_zero = (allocation_df * priceReturn_df).sum(axis=1).resample("1D").last()
    implied_extra_nav_if_price_return_is_zero.name = "additional_nav_if_price_return_was_0"
    return implied_extra_nav_if_price_return_is_zero


if __name__ == "__main__":
    df = fetch_nav_and_shares_and_factors_that_impact_nav_per_share(AUTO_ETH)
    print(df.columns)
    print(df.shape)
    print(df.head())

    print(df.tail())
