import plotly.graph_objects as go
import plotly.express as px
import pandas as pd



def make_rebalance_events_plots(clean_rebalance_df):
    figures = []
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


def make_expoded_box_plot(df: pd.DataFrame, col: str, resolution: str = "1W"):
    # assumes df is timestmap index
    list_df = df.resample(resolution)[col].apply(list).reset_index()
    exploded_df = list_df.explode(col)

    return px.box(exploded_df, x="timestamp", y=col, title=f"Distribution of {col}")

