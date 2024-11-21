import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta, timezone

import streamlit as st

from mainnet_launch.constants import CACHE_TIME, AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.solver_diagnostics.fetch_rebalance_events import (
    fetch_and_clean_rebalance_between_destination_events,
)
from mainnet_launch.constants import CACHE_TIME, AutopoolConstants
from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use
from mainnet_launch.destination_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column


def fetch_and_render_turnover_data(autopool: AutopoolConstants):
    turnover_summary = fetch_turnover_data(autopool)
    st.header(f"{autopool.name} Turnover")
    st.table(turnover_summary)


@st.cache_data(ttl=CACHE_TIME)
def fetch_turnover_data(autopool: AutopoolConstants) -> pd.DataFrame:
    blocks = build_blocks_to_use(autopool.chain)
    clean_rebalance_df = fetch_and_clean_rebalance_between_destination_events(autopool)
    uwcr_df, allocation_df, compositeReturn_out_df, total_nav_series, summary_stats_df, pR_df = (
        fetch_destination_summary_stats(blocks, autopool)
    )
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

        eth_value_solver_took_from_to_autopool = recent_df["outEthValue"].sum()
        eth_value_solver_sent_to_autopool = recent_df["inEthValue"].sum()
        eth_value_lost_to_solver = eth_value_solver_took_from_to_autopool - eth_value_solver_sent_to_autopool

        turnover = eth_value_solver_took_from_to_autopool / avg_tvl
        record = {
            "autopool": autopool.name,
            "duration": window_name,
            "rebalance_count": rebalance_count,
            "autopool_avg_eth_tvl": avg_tvl,
            "eth_value_solver_took_from_autopool": eth_value_solver_took_from_to_autopool,
            "eth_value_solver_sent_to_autopool": eth_value_solver_sent_to_autopool,
            "eth_value_lost_to_solver": eth_value_lost_to_solver,
            "turnover": turnover,
        }
        records.append(record)

    turnover_summary = pd.DataFrame.from_records(records).round(2)
    return turnover_summary


if __name__ == "__main__":

    fetch_turnover_data(ALL_AUTOPOOLS[2])
    fetch_turnover_data(ALL_AUTOPOOLS[-1])
