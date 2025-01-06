import plotly.graph_objects as go
import plotly.express as px
import plotly.subplots as sp
import pandas as pd
import streamlit as st


from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.solver_diagnostics.fetch_rebalance_events import fetch_rebalance_events_df


def fetch_rebalance_events_data(autopool: AutopoolConstants):
    clean_rebalance_df = fetch_rebalance_events_df(autopool)
    rebalance_figures = _make_plots(clean_rebalance_df, False)
    return rebalance_figures


def fetch_and_render_rebalance_events_data(autopool: AutopoolConstants):
    rebalance_figures = fetch_rebalance_events_data(autopool)
    st.header(f"{autopool.name} Rebalance Events")

    # Render each figure individually
    for figure in rebalance_figures:
        st.plotly_chart(figure, use_container_width=True)


def fetch_and_render_solver_profit_data(autopool: AutopoolConstants):
    clean_rebalance_df = fetch_rebalance_events_df(autopool)
    solver_figures = _make_plots(clean_rebalance_df, True)

    for figure in solver_figures:
        st.plotly_chart(figure, use_container_width=True)


def _make_plots(clean_rebalance_df: pd.DataFrame, solverOnly: bool = False):
    figures = []

    if solverOnly:
        figures.append(_add_solver_profit(clean_rebalance_df))
        figures.append(_add_solver_cumulative_profit(clean_rebalance_df))
        figures.append(_make_solver_histograms(clean_rebalance_df))
        figures.extend(_make_solver_box_plot_figures(clean_rebalance_df))
    else:
        figures.append(_add_composite_return_figures(clean_rebalance_df))
        figures.append(_add_in_out_eth_value(clean_rebalance_df))
        figures.append(_add_predicted_gain_and_swap_cost(clean_rebalance_df))
        figures.append(_add_swap_cost_percent(clean_rebalance_df))
        figures.append(_add_break_even_days_and_offset_period(clean_rebalance_df))

    return figures


def _add_composite_return_figures(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["out_compositeReturn"], name="Out Composite Return")
    )
    fig.add_trace(
        go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["in_compositeReturn"], name="In Composite Return")
    )
    fig.update_yaxes(title_text="Return (%)")
    fig.update_layout(
        title="Composite Returns",
        bargap=0.0,
        bargroupgap=0.01,
    )
    return fig


def _add_in_out_eth_value(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["outEthValue"], name="Out ETH Value"))
    fig.add_trace(go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["inEthValue"], name="In ETH Value"))
    fig.update_yaxes(title_text="ETH")
    fig.update_layout(
        title="In/Out ETH Values",
        bargap=0.0,
        bargroupgap=0.01,
    )
    return fig


def _add_predicted_gain_and_swap_cost(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=clean_rebalance_df.index,
            y=clean_rebalance_df["predicted_gain_during_swap_cost_off_set_period"],
            name="Predicted Gain",
        )
    )
    fig.add_trace(go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["swapCost"], name="Swap Cost"))
    fig.update_yaxes(title_text="ETH")
    fig.update_layout(title="Swap Cost and Predicted Gain", bargap=0.0, bargroupgap=0.01)
    return fig


def _add_swap_cost_percent(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    swap_cost_percentage = clean_rebalance_df["slippage"] * 100
    fig = go.Figure()
    fig.add_trace(go.Bar(x=clean_rebalance_df.index, y=swap_cost_percentage, name="Swap Cost Percentage"))
    fig.update_yaxes(title_text="Swap Cost (%)")
    fig.update_layout(
        title="Swap Cost as Percentage of Out ETH Value",
        bargap=0.0,
        bargroupgap=0.01,
    )
    return fig


def _add_break_even_days_and_offset_period(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["break_even_days"], name="Break Even Days"))
    fig.add_trace(go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["offset_period"], name="Offset Period"))
    fig.update_yaxes(title_text="Days")
    fig.update_layout(
        title="Break Even Days and Offset Period",
        bargap=0.0,
        bargroupgap=0.01,
    )
    return fig


def _add_solver_profit(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["solver_profit"], name="Solver Profit Before Gas")
    )
    fig.add_trace(
        go.Bar(x=clean_rebalance_df.index, y=clean_rebalance_df["gasCostInETH"], name="Solver Gas Cost in ETH")
    )

    solver_profit_after_gas_costs = clean_rebalance_df["solver_profit"].astype(float) - clean_rebalance_df[
        "gasCostInETH"
    ].astype(float)

    fig.add_trace(go.Bar(x=clean_rebalance_df.index, y=solver_profit_after_gas_costs, name="Solver Profit After Gas"))
    fig.update_yaxes(title_text="ETH")
    fig.update_layout(
        title="Solver Profit and Gas Costs",
        barmode="group",
        bargap=0.0,
        bargroupgap=0.01,
    )
    return fig


def make_expoded_box_plot(df: pd.DataFrame, col: str, resolution: str = "1W"):
    # assumes df is timestmap index
    list_df = df.resample(resolution)[col].apply(list).reset_index()
    exploded_df = list_df.explode(col)

    return px.box(exploded_df, x="timestamp", y=col, title=f"Distribution of {col}")


def _make_solver_histograms(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    cols = [
        "gasCostInETH",
        "solver_profit",
        "outEthValue",
        "swapCost",
        "break_even_days",
        "slippage",
        "predicted_gain_during_swap_cost_off_set_period",
        "predicted_increase_after_swap_cost",
    ]

    num_columns = int(len(cols) / 3) + 1
    num_rows = int(len(cols) / 3) + 1
    fig = sp.make_subplots(
        rows=num_rows,
        cols=num_columns,
        subplot_titles=cols,
        x_title="Value",
        y_title="Frequency (%)",
    )

    for i, col in enumerate(cols):
        row = (i // num_columns) + 1
        col_pos = (i % num_columns) + 1

        # Set x-axis label based on column
        x_label = "ETH"
        if col == "break_even_days":
            x_label = "Days"
        elif col == "slippage":
            x_label = "Percent"

        # Create histogram
        hist = go.Histogram(
            histnorm="percent",
            x=clean_rebalance_df[col],
            name=col,
        )

        fig.add_trace(hist, row=row, col=col_pos)
        fig.update_xaxes(title_text=x_label, row=row, col=col_pos)

    fig.update_layout(height=800, width=1200, showlegend=False, title="Solver Distribution Histograms")
    return fig


def _make_solver_box_plot_figures(clean_rebalance_df: pd.DataFrame) -> go.Figure:
    cols = [
        "gasCostInETH",
        "solver_profit",
        "outEthValue",
        "swapCost",
        "break_even_days",
        "slippage",
        "predicted_gain_during_swap_cost_off_set_period",
        "predicted_increase_after_swap_cost",
    ]

    box_plots_over_time = []

    for col in cols:
        x_label = "ETH"
        if col == "break_even_days":
            x_label = "Days"
        if col == "slippage":
            x_label = "Percent"
        fig = make_expoded_box_plot(clean_rebalance_df, col)
        fig.update_xaxes(title_text=x_label)
        box_plots_over_time.append(fig)

    return box_plots_over_time


def _add_solver_cumulative_profit(clean_rebalance_df: pd.DataFrame) -> go.Figure:

    daily_profit_df = clean_rebalance_df[["solver_profit", "gasCostInETH"]].resample("1D").sum()
    fig = go.Figure()
    fig.add_trace(
        go.Line(x=daily_profit_df.index, y=daily_profit_df["solver_profit"].cumsum(), name="Solver Profit Before Gas")
    )
    fig.add_trace(
        go.Line(x=daily_profit_df.index, y=daily_profit_df["gasCostInETH"].cumsum(), name="Solver Gas Cost in ETH")
    )

    solver_profit_after_gas_costs = daily_profit_df["solver_profit"].astype(float) - daily_profit_df[
        "gasCostInETH"
    ].astype(float)

    fig.add_trace(
        go.Line(x=daily_profit_df.index, y=solver_profit_after_gas_costs.cumsum(), name="Solver Profit After Gas")
    )
    fig.update_yaxes(title_text="ETH")
    fig.update_layout(title="Cumulative Profit and Gas Costs")
    return fig


if __name__ == "__main__":
    from mainnet_launch.constants import AUTO_LRT

    fetch_and_render_rebalance_events_data(AUTO_LRT)
