import json
import os
import xml.etree.ElementTree as ET
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import streamlit as st

from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS, eth_client, SOLVER_REBALANCE_PLANS_DIR, AUTO_ETH
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI, AUTOPOOL_ETH_STRATEGY_ABI
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.solver_diagnostics.fetch_rebalance_events import (
    fetch_and_clean_rebalance_between_destination_events,
)
from mainnet_launch.destinations import attempt_destination_address_to_symbol
from mainnet_launch.data_fetching.get_state_by_block import (
    add_timestamp_to_df_with_block_column,
)
import boto3
from botocore import UNSIGNED
from botocore.client import Config

from mainnet_launch.constants import SOLVER_REBALANCE_PLANS_DIR, ALL_AUTOPOOLS


def fetch_and_render_solver_diagnositics_data(autopool: AutopoolConstants):
    proposed_vs_actual_rebalance_scatter_plot_fig, bar_chart_count_proposed_vs_actual_rebalances_fig = (
        fetch_solver_diagnostics_data(autopool)
    )
    _render_solver_diagnostics(
        autopool, proposed_vs_actual_rebalance_scatter_plot_fig, bar_chart_count_proposed_vs_actual_rebalances_fig
    )


@st.cache_data(ttl=3600)
def fetch_solver_diagnostics_data(autopool: AutopoolConstants):
    ensure_all_rebalance_plans_are_loaded()
    solver_df = _load_solver_df(autopool)
    proposed_rebalances_df = solver_df[solver_df["sodOnly"] == False].copy()
    proposed_rebalances_df.set_index("date", inplace=True)

    rebalance_event_df = fetch_and_clean_rebalance_between_destination_events(autopool)
    proposed_vs_actual_rebalance_scatter_plot_fig = _make_proposed_vs_actual_rebalance_scatter_plot(
        proposed_rebalances_df, rebalance_event_df
    )
    bar_chart_count_proposed_vs_actual_rebalances_fig = _make_proposed_vs_actual_rebalances_bar_plot(
        proposed_rebalances_df, rebalance_event_df
    )

    return proposed_vs_actual_rebalance_scatter_plot_fig, bar_chart_count_proposed_vs_actual_rebalances_fig


def _render_solver_diagnostics(
    autopool: AutopoolConstants,
    proposed_rebalances_fig: go.Figure,
    bar_chart_count_proposed_vs_actual_rebalances_fig: go.Figure,
):
    st.header(f"{autopool.name} Solver Diagnostics")
    st.plotly_chart(proposed_rebalances_fig, use_container_width=True)
    st.plotly_chart(bar_chart_count_proposed_vs_actual_rebalances_fig, use_container_width=True)


def ensure_all_rebalance_plans_are_loaded():
    for autopool in ALL_AUTOPOOLS:
        s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        response = s3_client.list_objects_v2(Bucket=autopool.solver_rebalance_plans_bucket)
        all_rebalance_plans = [o["Key"] for o in response["Contents"]]
        local_rebalance_plans = [str(path).split("/")[-1] for path in SOLVER_REBALANCE_PLANS_DIR.glob("*.json")]
        rebalance_plans_to_fetch = [
            json_path for json_path in all_rebalance_plans if json_path not in local_rebalance_plans
        ]
        for json_key in rebalance_plans_to_fetch:
            s3_client.download_file(
                autopool.solver_rebalance_plans_bucket, json_key, SOLVER_REBALANCE_PLANS_DIR / json_key
            )


def _load_solver_df(autopool: AutopoolConstants) -> pd.DataFrame:
    autopool_plans = [p for p in SOLVER_REBALANCE_PLANS_DIR.glob("*.json") if autopool.autopool_eth_addr in str(p)]

    all_data = []
    for plan_json in autopool_plans:
        with open(plan_json, "r") as fin:
            data = json.load(fin)
            data["date"] = pd.to_datetime(data["timestamp"], unit="s")
            data["destinationIn"] = attempt_destination_address_to_symbol(data["destinationIn"])
            data["destinationOut"] = attempt_destination_address_to_symbol(data["destinationOut"])
            data["moveName"] = f"{data['destinationOut']} -> {data['destinationIn']}"
            all_data.append(data)
    solver_df = pd.DataFrame.from_records(all_data)
    solver_df.sort_values("date", ascending=True, inplace=True)
    return solver_df


def _make_proposed_vs_actual_rebalance_scatter_plot(
    proposed_rebalances_df: pd.DataFrame, rebalance_event_df: pd.DataFrame
) -> go.Figure:
    moves_df = pd.concat([proposed_rebalances_df["moveName"], rebalance_event_df["moveName"]], axis=1)
    moves_df.columns = ["proposed_rebalances", "actual_rebalances"]

    sizes_df = pd.concat(
        [proposed_rebalances_df["amountOutETH"].apply(lambda x: int(x) / 1e18), rebalance_event_df["outEthValue"]],
        axis=1,
    )
    sizes_df.columns = ["proposed_amount", "actual_amount"]

    proposed_rebalances_fig = go.Scatter(
        x=moves_df.index,
        y=moves_df["proposed_rebalances"],
        mode="markers",
        name="Proposed Rebalances",
        marker=dict(color="blue", size=10),
        text=sizes_df["proposed_amount"],
        hovertemplate="Proposed ETH Amount Out: %{text}<extra></extra>",
    )

    actual_rebalances_fig = go.Scatter(
        x=moves_df.index,
        y=moves_df["actual_rebalances"],
        mode="markers",
        name="Actual Rebalances",
        marker=dict(symbol="x", color="red", size=12),
        text=sizes_df["actual_amount"],
        hovertemplate="Actual ETH Amount Out: %{text}<extra></extra>",
    )

    proposed_vs_actual_rebalance_scatter_plot_fig = go.Figure(data=[proposed_rebalances_fig, actual_rebalances_fig])

    proposed_vs_actual_rebalance_scatter_plot_fig.update_layout(
        yaxis_title="Rebalances",
        xaxis_title="Date",
        title="Proposed vs Actual Rebalances",
        height=600,
        width=600 * 3,
    )
    return proposed_vs_actual_rebalance_scatter_plot_fig


def _make_proposed_vs_actual_rebalances_bar_plot(
    proposed_rebalance_df: pd.DataFrame, rebalance_event_df: pd.DataFrame
) -> go.Figure:
    today = datetime.now()
    seven_days_ago = today - timedelta(days=7)
    thirty_days_ago = today - timedelta(days=30)
    one_year_ago = today - timedelta(days=30)

    records = []
    for time_period, window in zip(
        ["seven_days_ago", "thirty_days_ago", "one_year_ago"], [seven_days_ago, thirty_days_ago, one_year_ago]
    ):
        num_proposed_rebalances = sum(proposed_rebalance_df.index >= window)
        num_actual_rebalances = sum(rebalance_event_df.index >= window)
        records.append(
            {
                "time_period": time_period,
                "num_actual_rebalances": num_actual_rebalances,
                "num_proposed_rebalances": num_proposed_rebalances,
            }
        )

    proposed_and_actual_rebalance_counts_df = pd.DataFrame.from_records(records)
    bar_chart_count_proposed_vs_actual_rebalances_fig = go.Figure()

    bar_chart_count_proposed_vs_actual_rebalances_fig.add_trace(
        go.Bar(
            x=proposed_and_actual_rebalance_counts_df["time_period"],
            y=proposed_and_actual_rebalance_counts_df["num_proposed_rebalances"],
            name="Proposed Rebalances",
            marker_color="blue",
        )
    )

    bar_chart_count_proposed_vs_actual_rebalances_fig.add_trace(
        go.Bar(
            x=proposed_and_actual_rebalance_counts_df["time_period"],
            y=proposed_and_actual_rebalance_counts_df["num_actual_rebalances"],
            name="Actual Rebalances",
            marker_color="green",
        )
    )

    bar_chart_count_proposed_vs_actual_rebalances_fig.update_layout(
        title="Proposed vs Actual Rebalances Over Time",
        xaxis_title="Time Period",
        yaxis_title="Count",
        barmode="group",
        bargap=0.15,
        bargroupgap=0.1,
        template="plotly",
    )
    return bar_chart_count_proposed_vs_actual_rebalances_fig
