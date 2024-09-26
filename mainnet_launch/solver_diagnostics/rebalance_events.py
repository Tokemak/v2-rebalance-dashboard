import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.solver_diagnostics.fetch_rebalance_events import fetch_rebalance_events_df


def display_rebalance_events(autopool: AutopoolConstants) -> go.Figure:
    clean_rebalance_df = fetch_rebalance_events_df(autopool)
    fig = _make_plots(clean_rebalance_df)
    st.header(f"{autopool.name} Rebalance Events")
    st.plotly_chart(fig, use_container_width=True)


def _make_plots(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    fig = make_subplots(
        rows=6,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=(
            "Composite Returns",
            "in/out ETH Values",
            "Swap Cost, Predicted Gain",
            "Swap Cost as Percentage of Out ETH Value",
            "Break Even Days and Offset Period",
            "Solver Profit and Gas",
        ),
    )

    _add_composite_return_figures(clean_rebalance_df, fig)
    _add_in_out_eth_value(clean_rebalance_df, fig)
    _add_predicted_gain_and_swap_cost(clean_rebalance_df, fig)
    _add_swap_cost_percent(clean_rebalance_df, fig)
    _add_break_even_days_and_offset_period(clean_rebalance_df, fig)
    _add_solver_profit(clean_rebalance_df, fig)

    # Update layout
    fig.update_layout(
        height=6 * 400,
        width=1000,
        title_text="",
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color="black"),
    )

    # Update x-axes
    fig.update_xaxes(
        title_text=" ",
        row=6,
        col=1,
        showgrid=True,
        gridwidth=1,
        gridcolor="lightgray",
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor="black",
    )

    # Update y-axes
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="lightgray",
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor="black",
    )
    return fig


def add_solver_cumulative_gas_price(clean_rebalance_df: pd.DataFrame, fig: go.Figure):
    pass


def _add_composite_return_figures(clean_rebalance_df: pd.DataFrame, fig: go.Figure):
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["out_compositeReturn"], name="Out Composite Return"),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["in_compositeReturn"], name="In Composite Return"),
        row=1,
        col=1,
    )
    fig.update_yaxes(title_text="Return (%)", row=1, col=1)


def _add_in_out_eth_value(clean_rebalance_df: pd.DataFrame, fig: go.Figure):
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["outEthValue"], name="Out ETH Value"), row=2, col=1
    )
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["inEthValue"], name="In ETH Value"), row=2, col=1
    )

    fig.update_yaxes(title_text="ETH", row=2, col=1)


def _add_predicted_gain_and_swap_cost(clean_rebalance_df: pd.DataFrame, fig: go.Figure):
    fig.add_trace(
        go.Bar(
            x=clean_rebalance_df["date"],
            y=clean_rebalance_df["predicted_gain_during_swap_cost_off_set_period"],
            name="Predicted Gain",
        ),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["swapCost"], name="Swap Cost"), row=3, col=1
    )

    fig.update_yaxes(title_text="ETH", row=3, col=1)


def _add_swap_cost_percent(clean_rebalance_df: pd.DataFrame, fig: go.Figure):
    swap_cost_percentage = (clean_rebalance_df["slippage"]) * 100
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=swap_cost_percentage, name="Swap Cost Percentage"), row=4, col=1
    )
    fig.update_yaxes(title_text="Swap Cost (%)", row=4, col=1)


def _add_break_even_days_and_offset_period(clean_rebalance_df: pd.DataFrame, fig: go.Figure):
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["break_even_days"], name="Break Even Days"),
        row=5,
        col=1,
    )
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["offset_period"], name="Offset Period"), row=5, col=1
    )
    fig.update_yaxes(title_text="Days", row=5, col=1)


def _add_solver_profit(clean_rebalance_df: pd.DataFrame, fig: go.Figure):
    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["solver_profit"], name="Solver Profit Before Gas"),
        row=6,
        col=1,
    )

    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=clean_rebalance_df["gasCostInETH"], name="Solver Gas Cost in ETH"),
        row=6,
        col=1,
    )

    solver_profit_after_gas_costs = clean_rebalance_df["solver_profit"].astype(float) - clean_rebalance_df[
        "gasCostInETH"
    ].astype(float)

    fig.add_trace(
        go.Bar(x=clean_rebalance_df["date"], y=solver_profit_after_gas_costs, name="Solver Profit After Gas"),
        row=6,
        col=1,
    )
    fig.update_yaxes(title_text="ETH", row=6, col=1)
