from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from mainnet_launch.constants import STREAMLIT_IN_MEMORY_CACHE_TIME, AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.pages.rebalance_events.rebalance_events import (
    fetch_rebalance_events_df,
)
from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats


def fetch_and_render_turnover_data(autopool: AutopoolConstants):
    turnover_summary = fetch_turnover_data(autopool)
    st.header(f"{autopool.name} Turnover")
    st.table(turnover_summary)


def fetch_turnover_data(autopool: AutopoolConstants) -> pd.DataFrame:
    clean_rebalance_df = fetch_rebalance_events_df(autopool)

    def _is_a_rebalance_between_the_same_destination(row) -> bool:
        # moveName = f"{out_destination_symbol} -> {in_destination_symbol}"
        destination_symbols = row["moveName"].split(" -> ")
        if len(destination_symbols) == 2:
            out_destination_symbol, in_destination_symbol = destination_symbols
            if out_destination_symbol == in_destination_symbol:
                return True
        return False

    clean_rebalance_df["is_rebalance_between_the_same_destination"] = clean_rebalance_df.apply(
        _is_a_rebalance_between_the_same_destination, axis=1
    )

    pricePerShare_df = fetch_destination_summary_stats(autopool, "pricePerShare")
    ownedShares_df = fetch_destination_summary_stats(autopool, "ownedShares")
    allocation_df = pricePerShare_df * ownedShares_df
    total_nav_series = allocation_df.sum(axis=1)

    today = datetime.now(timezone.utc)

    seven_days_ago = today - timedelta(days=7)
    thirty_days_ago = today - timedelta(days=30)
    one_year_ago = today - timedelta(days=365)

    records = []
    for window_name, window in zip(
        ["seven_days_ago", "thirty_days_ago", "one_year_ago"], [seven_days_ago, thirty_days_ago, one_year_ago]
    ):

        recent_df = clean_rebalance_df[clean_rebalance_df.index >= window]
        rebalance_count = len(recent_df)

        avg_tvl = float(total_nav_series[total_nav_series.index >= window].mean())

        total_volume_with_rebalances_to_same_destination = recent_df["outEthValue"].sum()
        total_volume_without_rebalances_to_same_destination = recent_df[
            ~recent_df["is_rebalance_between_the_same_destination"]
        ]["outEthValue"].sum()

        record = {
            "window": window_name,
            "rebalances": rebalance_count,
            "avg_tvl": round(avg_tvl, 2),
            "volume_with_rebalances_to_self": round(total_volume_with_rebalances_to_same_destination, 2),
            "volume_without_rebalances_to_self": round(total_volume_without_rebalances_to_same_destination, 2),
            "turnover_with_rebalances_to_self": round(total_volume_with_rebalances_to_same_destination / avg_tvl, 3),
            "turnover_without_rebalances_to_self": round(
                total_volume_without_rebalances_to_same_destination / avg_tvl, 3
            ),
        }
        records.append(record)

    turnover_summary = pd.DataFrame.from_records(records)
    return turnover_summary


if __name__ == "__main__":

    for a in ALL_AUTOPOOLS:
        fetch_and_render_turnover_data(a)
