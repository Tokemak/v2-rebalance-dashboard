"""Returns of the autopool before and after expenses and fees"""

import pandas as pd

from mainnet_launch.constants import AutopoolConstants, AUTO_ETH

from mainnet_launch.pages.rebalance_events.rebalance_events import (
    fetch_rebalance_events_df,
)

from mainnet_launch.pages.protocol_level_profit_and_loss.fees import (
    fetch_all_autopool_fee_events,
)

from mainnet_launch.pages.key_metrics.fetch_nav_per_share import fetch_autopool_nav_per_share


def fetch_nav_and_shares_and_factors_that_impact_nav_per_share(autopool: AutopoolConstants) -> pd.DataFrame:
    # this is only using cached data so it does not need it's own table
    daily_nav_shares_df = _fetch_actual_nav_per_share_by_day(autopool)
    cumulative_new_shares_df = _fetch_cumulative_fee_shares_minted_by_day(autopool)
    cumulative_rebalance_from_idle_swap_cost, cumulative_rebalance_not_from_idle_swap_cost = (
        _fetch_cumulative_nav_lost_to_rebalances(autopool)
    )
    df = pd.concat(
        [
            daily_nav_shares_df,
            cumulative_new_shares_df,
            cumulative_rebalance_from_idle_swap_cost,
            cumulative_rebalance_not_from_idle_swap_cost,
        ],
        axis=1,
    )

    df.iloc[0] = df.iloc[0].fillna(0)  # new shares, nav los to fees, nav lost to swap costs all start out at 0
    df = df.ffill()

    return df


def _fetch_actual_nav_per_share_by_day(autopool: AutopoolConstants) -> pd.DataFrame:
    df = fetch_autopool_nav_per_share(autopool)
    df["actual_nav_per_share"] = df[autopool.name]
    daily_nav_shares_df = df[["actual_nav_per_share"]].resample("1D").last()
    return daily_nav_shares_df


def _fetch_cumulative_nav_lost_to_rebalances(autopool: AutopoolConstants) -> pd.DataFrame:
    rebalance_df = fetch_rebalance_events_df(autopool)

    rebalance_from_idle_df = rebalance_df[
        rebalance_df["outDestinationVault"].str.lower() == autopool.autopool_eth_addr.lower()
    ].copy()
    rebalance_not_from_idle_df = rebalance_df[
        ~(rebalance_df["outDestinationVault"].str.lower() == autopool.autopool_eth_addr.lower())
    ].copy()

    cumulative_rebalance_from_idle_swap_cost = rebalance_from_idle_df["swapCost"].resample("1D").sum().cumsum()
    cumulative_rebalance_from_idle_swap_cost.name = "rebalance_from_idle_swap_cost"

    cumulative_rebalance_not_from_idle_swap_cost = rebalance_not_from_idle_df["swapCost"].resample("1D").sum().cumsum()
    cumulative_rebalance_not_from_idle_swap_cost.name = "rebalance_not_idle_swap_cost"
    return cumulative_rebalance_from_idle_swap_cost, cumulative_rebalance_not_from_idle_swap_cost


def _fetch_cumulative_fee_shares_minted_by_day(autopool: AutopoolConstants) -> pd.DataFrame:
    fee_df = fetch_all_autopool_fee_events(autopool)[
        ["new_shares_from_periodic_fees", "new_shares_from_streaming_fees"]
    ]
    daily_fee_share_df = fee_df.resample("1D").sum()
    cumulative_new_shares_df = daily_fee_share_df.cumsum()
    return cumulative_new_shares_df


if __name__ == "__main__":
    df = fetch_nav_and_shares_and_factors_that_impact_nav_per_share(AUTO_ETH)
    print(df.columns)
    print(df.shape)
    print(df.head())

    print(df.tail())
