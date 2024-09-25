import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta

import streamlit as st

from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.solver_diagnostics.fetch_rebalance_events import fetch_rebalance_events_df
from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use

from mainnet_launch.destination_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats


def display_autopool_turnover(autopool: AutopoolConstants):
    rebalance_summary = _fetch_high_level_rebalance_summary(autopool)
    st.header("Autopool Turnover")
    st.table(rebalance_summary)


@st.cache_data(ttl=3600)
def _fetch_high_level_rebalance_summary(autopool: AutopoolConstants) -> pd.DataFrame:
    blocks = build_blocks_to_use()
    uwcr_df, allocation_df, compositeReturn_df, total_nav_df, summary_stats_df, points_df = (
        fetch_destination_summary_stats(blocks, autopool)
    )

    clean_rebalance_df = fetch_rebalance_events_df(autopool)
    today = datetime.now()
    seven_days_ago = today - timedelta(days=7)
    thirty_days_ago = today - timedelta(days=30)
    one_year_ago = today - timedelta(days=365)

    records = []
    for window_name, window in zip(
        ["seven_days_ago", "thirty_days_ago", "one_year_ago"], [seven_days_ago, thirty_days_ago, one_year_ago]
    ):

        recent_df = clean_rebalance_df[clean_rebalance_df["date"] >= window]
        rebalance_count = len(recent_df)

        avg_tvl = float(total_nav_df[total_nav_df.index >= window].mean())

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

    rebalance_summary = pd.DataFrame.from_records(records).round(2)
    return rebalance_summary
