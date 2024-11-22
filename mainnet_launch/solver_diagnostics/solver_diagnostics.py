import json
import os
import xml.etree.ElementTree as ET
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta, timezone
import streamlit as st

from mainnet_launch.constants import (
    CACHE_TIME,
    AutopoolConstants,
    ALL_AUTOPOOLS,
    eth_client,
    SOLVER_REBALANCE_PLANS_DIR,
    AUTO_ETH,
)
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI, AUTOPOOL_ETH_STRATEGY_ABI
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.destinations import get_destination_details
from mainnet_launch.solver_diagnostics.fetch_rebalance_events import (
    fetch_and_clean_rebalance_between_destination_events,
)
from mainnet_launch.solver_diagnostics.rebalance_events import fetch_and_render_solver_profit_data
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use

import boto3
from botocore import UNSIGNED
from botocore.client import Config

from mainnet_launch.constants import CACHE_TIME, SOLVER_REBALANCE_PLANS_DIR, ALL_AUTOPOOLS, BAL_ETH, AUTO_LRT


def fetch_and_render_solver_diagnositics_data(autopool: AutopoolConstants):
    figs = fetch_solver_diagnostics_data(autopool)
    _render_solver_diagnostics(autopool, figs)
    fetch_and_render_solver_profit_data(autopool)


@st.cache_data(ttl=CACHE_TIME)
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

    hours_between_plans_fig = _make_hours_between_fig(solver_df)

    plan_count_fig = _make_count_of_solver_plans_each_day_plot(solver_df)

    dex_win_fig = _dex_win_metrics(solver_df)

    size_of_candidate_set = _add_add_rank_count(solver_df)

    return [
        proposed_vs_actual_rebalance_scatter_plot_fig,
        bar_chart_count_proposed_vs_actual_rebalances_fig,
        plan_count_fig,
        hours_between_plans_fig,
        dex_win_fig,
        size_of_candidate_set,
    ]


def _render_solver_diagnostics(autopool: AutopoolConstants, figs):
    st.header(f"{autopool.name} Solver Diagnostics")
    for fig in figs:
        st.plotly_chart(fig, use_container_width=True)


def ensure_all_rebalance_plans_are_loaded():
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    for autopool in ALL_AUTOPOOLS:

        # # the base ETH bucket does not work, unsure why
        # if autopool.name != "baseETH":
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
    blocks = build_blocks_to_use(autopool.chain)
    destination_details = get_destination_details(autopool, blocks)
    destination_vault_address_to_symbol = {dest.vaultAddress: dest.vault_name for dest in destination_details}
    all_data = []
    for plan_json in autopool_plans:
        with open(plan_json, "r") as fin:
            data = json.load(fin)
            data["date"] = pd.to_datetime(data["timestamp"], unit="s", utc=True)
            if data["destinationIn"] in destination_vault_address_to_symbol:
                data["destinationIn"] = destination_vault_address_to_symbol[data["destinationIn"]]

            if data["destinationOut"] in destination_vault_address_to_symbol:
                data["destinationOut"] = destination_vault_address_to_symbol[data["destinationOut"]]

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
    today = datetime.now(timezone.utc)
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


def _make_count_of_solver_plans_each_day_plot(solver_df):
    solver_count_per_day = solver_df.groupby(solver_df["date"].dt.date).size()
    solver_count_per_day_df = pd.DataFrame(solver_count_per_day, columns=["Num Generated"])
    return px.line(solver_count_per_day_df, title="Count of solver plans per day")


def _dex_win_metrics(solver_df):
    all_steps = solver_df["steps"].values
    dex_steps = []

    for steps in all_steps:
        for step in steps:
            if "dex" in step:
                dex_steps.append(step)

    absolute_counts = pd.DataFrame.from_records(dex_steps)["dex"].value_counts()
    normalized_counts = (100 * pd.DataFrame.from_records(dex_steps)["dex"].value_counts(normalize=True)).round(2)

    # Combine the two DataFrames into one for display
    combined_df = pd.DataFrame(
        {
            "DEX": absolute_counts.index,  # The index (dex names)
            "Count of step": absolute_counts.values,  # The absolute counts
            "Percent of steps": normalized_counts.values,  # The normalized counts
        }
    )

    # Create the table figure
    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=list(combined_df.columns), align="left"  # Use column names from the combined DataFrame
                ),  # Align header to the left
                cells=dict(
                    values=[combined_df[col] for col in combined_df.columns],  # Table data from the DataFrame
                    align="left",
                ),
            )  # Align cells to the left
        ]
    )
    fig.update_layout(title="Dex Aggregator Win Counts")

    return fig


def _make_hours_between_fig(solver_df):
    solver_df["hoursBetween"] = (solver_df["date"].diff().dt.total_seconds()) / 3600
    hours_between_plans_fig = px.scatter(
        y=solver_df["hoursBetween"],
        x=solver_df["date"],
        title="Hours Between Rebalance Plans Generated",
        labels={"x": "Rebalances", "y": "Hours"},
    )
    return hours_between_plans_fig


def _add_add_rank_count(solver_df):
    solver_df["len_addRank"] = solver_df["addRank"].apply(lambda x: len(x))
    fig = px.bar(solver_df, x="date", y="len_addRank", title="Candidate Destinations Size")
    return fig


if __name__ == "__main__":
    # fetch_and_render_solver_diagnositics_data(ALL_AUTOPOOLS[0])
    ensure_all_rebalance_plans_are_loaded()
