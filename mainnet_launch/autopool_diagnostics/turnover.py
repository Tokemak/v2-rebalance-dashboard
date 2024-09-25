import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta

import streamlit as st

from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.solver_diagnostics.fetch_rebalance_events import fetch_rebalance_events_df


def display_autopool_turnover(autopool: AutopoolConstants):
    rebalance_summary = _fetch_high_level_rebalance_summary(autopool)
    st.header("Autopool Turnover")
    st.table(rebalance_summary)


def _fetch_high_level_rebalance_summary(autopool: AutopoolConstants) -> pd.DataFrame:
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

        eth_value_solver_took_from_to_autopool = recent_df["outEthValue"].sum()
        eth_value_solver_sent_to_autopool = recent_df["inEthValue"].sum()
        eth_value_lost_to_solver = eth_value_solver_took_from_to_autopool - eth_value_solver_sent_to_autopool

        record = {
            "autopool": autopool.name,
            "duration": window_name,
            "rebalance_count": rebalance_count,
            "eth_value_solver_took_from_autopool": eth_value_solver_took_from_to_autopool,
            "eth_value_solver_sent_to_autopool": eth_value_solver_sent_to_autopool,
            "eth_value_lost_to_solver": eth_value_lost_to_solver,
        }
        records.append(record)

    rebalance_summary = pd.DataFrame.from_records(records).round(2)
    return rebalance_summary
