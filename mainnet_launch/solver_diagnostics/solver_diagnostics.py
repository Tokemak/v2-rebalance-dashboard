import json
import os
import xml.etree.ElementTree as ET
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from mainnet_launch.solver_diagnostics.ensure_solver_plans_are_loaded import (
    ensure_all_rebalance_plans_are_loaded,
    SOLVER_PLAN_DATA_PATH,
)
from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS, eth_client
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.solver_diagnostics.rebalance_events import fetch_rebalance_events_df


def load_solver_df(autopool: AutopoolConstants) -> pd.DataFrame:
    # load all the solver plans for this autopool
    pass


def _fetch_rebalance_between_destination(autopool: AutopoolConstants) -> pd.DataFrame:
    rebalance_df = fetch_rebalance_events_df(autopool)


def load_balETH_solver_df():
    destination_df = pd.read_parquet(ROOT_DIR / "vaults.parquet")
    rebalance_event_df = fetch_clean_rebalance_events()

    destination_vault_to_name = {
        str(vault_address).lower(): name[22:]
        for vault_address, name in zip(destination_df["vaultAddress"], destination_df["name"])
    }
    destination_vault_to_name["0x72cf6d7c85ffd73f18a83989e7ba8c1c30211b73"] = "balETH idle"
    solver_df = load_solver_df()
    balETH_solver_df = solver_df[solver_df["poolAddress"] == balETH].copy()
    balETH_solver_df.set_index("date", inplace=True)

    balETH_solver_df["destinationInName"] = balETH_solver_df.apply(
        lambda row: (
            destination_vault_to_name[row["destinationIn"].lower()]
            if row["destinationIn"].lower() in destination_vault_to_name
            else None
        ),
        axis=1,
    )
    balETH_solver_df["destinationOutName"] = balETH_solver_df.apply(
        lambda row: (
            destination_vault_to_name[row["destinationOut"].lower()]
            if row["destinationOut"].lower() in destination_vault_to_name
            else None
        ),
        axis=1,
    )

    balETH_solver_df["moveName"] = balETH_solver_df.apply(
        lambda row: f"Exit {row['destinationOutName']} enter {row['destinationInName']}", axis=1
    )
    rebalance_event_df["moveName"] = rebalance_event_df.apply(
        lambda row: f"Exit {row['out_destination']} enter {row['in_destination']}", axis=1
    )
    balETH_proposed_rebalances_df = balETH_solver_df[balETH_solver_df["sodOnly"] == False].copy()
    return balETH_solver_df, balETH_proposed_rebalances_df, destination_df, rebalance_event_df


def make_proposed_vs_actual_rebalance_scatter_plot(
    balETH_solver_df: pd.DataFrame, rebalance_event_df: pd.DataFrame
) -> go.Figure:
    moves_df = pd.concat([balETH_solver_df["moveName"], rebalance_event_df["moveName"]], axis=1)
    moves_df.columns = ["proposed_rebalances", "actual_rebalances"]
    proposed_rebalances_fig = go.Scatter(
        x=moves_df.index,
        y=moves_df["proposed_rebalances"],
        mode="markers",
        name="Proposed Rebalances",
        marker=dict(color="blue", size=10),
    )

    # Create the plot with actual rebalances with red 'x' markers
    actual_rebalances_fig = go.Scatter(
        x=moves_df.index,
        y=moves_df["actual_rebalances"],
        mode="markers",
        name="Actual Rebalances",
        marker=dict(symbol="x", color="red", size=12),
    )

    # Combine both plots into one figure
    proposed_vs_actual_rebalance_scatter_plot_fig = go.Figure(data=[proposed_rebalances_fig, actual_rebalances_fig])

    # Update layout
    proposed_vs_actual_rebalance_scatter_plot_fig.update_layout(
        yaxis_title="Rebalances",
        xaxis_title="Date",
        title="balETH Proposed vs Actual Rebalances",
        height=600,
        width=600 * 2,
    )
    return proposed_vs_actual_rebalance_scatter_plot_fig


def get_proposed_vs_actual_rebalances(
    balETH_solver_df: pd.DataFrame, rebalance_event_df: pd.DataFrame, start_date: str = "8-01-2024"
):
    recent_solves = balETH_solver_df[balETH_solver_df.index > start_date]
    num_proposed_rebalances = (~recent_solves["sodOnly"].astype(bool)).sum()
    num_actual_rebalances = rebalance_event_df[rebalance_event_df.index > start_date].shape[0]
    rebalance_counts = {"Acutal": num_actual_rebalances, "Proposed": int(num_proposed_rebalances)}

    actual_vs_proposed_rebalance_bar_fig = go.Figure(
        data=[go.Bar(name="Rebalances", x=list(rebalance_counts.keys()), y=list(rebalance_counts.values()))]
    )

    # Update layout for better visualization
    actual_vs_proposed_rebalance_bar_fig.update_layout(
        title=f"Proposed vs Actual Rebalances after {start_date}",
        xaxis_title="Acutal Vs Proposed",
        yaxis_title="Count",
        bargap=0.2,
        bargroupgap=0.1,
        height=600,
        width=600,
    )
    return actual_vs_proposed_rebalance_bar_fig


def block_to_date(block: int):
    return pd.to_datetime(eth_client.eth.getBlock(block).timestamp, unit="s")


def make_hours_since_last_nav_event_plot(nav_df: pd.DataFrame):
    nav_df["date"] = nav_df["block"].apply(block_to_date)
    time_diff_hours = nav_df["date"].diff().dt.total_seconds()[2:] / 3600
    time_diff_hours.index = nav_df["date"][2:]
    hours_since_last_nav_event_fig = px.scatter(
        time_diff_hours,
        labels={"value": "Hours", "index": "Date"},
        title="Hours Since Last Nav Event",
        height=600,
        width=600 * 3,
    )
    hours_since_last_nav_event_fig.add_hline(
        y=24, line_dash="dash", line_color="red", annotation_text="24-hour threshold", annotation_position="top right"
    )
    hours_since_last_nav_event_fig.update_yaxes(range=[20, 25])
    return hours_since_last_nav_event_fig


def fetch_solver_diagnostics_charts(autopool_name: str = "balETH") -> dict:
    if autopool_name != "balETH":
        raise ValueError("only works for balETH")
    _ensure_all_rebalance_plans_are_loaded()

    balETH_solver_df, balETH_proposed_rebalances_df, destination_df, rebalance_event_df = load_balETH_solver_df()
    proposed_vs_actual_rebalance_scatter_plot_fig = make_proposed_vs_actual_rebalance_scatter_plot(
        balETH_solver_df, rebalance_event_df
    )
    actual_vs_proposed_rebalance_bar_fig = get_proposed_vs_actual_rebalances(
        balETH_solver_df, rebalance_event_df, start_date="8-01-2024"
    )

    vault_events, strategy_events = _get_all_events_df()
    hours_since_last_nav_event_fig = make_hours_since_last_nav_event_plot(vault_events["Nav"])

    return {
        "proposed_vs_actual_rebalance_scatter_plot_fig": proposed_vs_actual_rebalance_scatter_plot_fig,
        "actual_vs_proposed_rebalance_bar_fig": actual_vs_proposed_rebalance_bar_fig,
        "hours_since_last_nav_event_fig": hours_since_last_nav_event_fig,  # might want to move
    }


if __name__ == "__main__":
    a = fetch_solver_diagnostics_charts()
    pass
