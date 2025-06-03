import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from mainnet_launch.constants import AutopoolConstants

from mainnet_launch.database.schema.postgres_operations import merge_tables_as_df, TableSelector, get_full_table_as_df
from mainnet_launch.database.schema.full import (
    RebalanceEvents,
    RebalancePlans,
    Blocks,
    Destinations,
    Transactions,
    DestinationTokens,
    Tokens,
)


# @st.cache_data(ttl=60 * 60)
def _load_full_rebalance_event_df(autopool: AutopoolConstants) -> pd.DataFrame:

    rebalance_df = merge_tables_as_df(
        selectors=[
            TableSelector(
                RebalanceEvents,
            ),
            TableSelector(
                Transactions, [Transactions.block], join_on=(Transactions.tx_hash == RebalanceEvents.tx_hash)
            ),
            TableSelector(
                Blocks,
                [Blocks.datetime],
                (Transactions.block == Blocks.block) & (Transactions.chain_id == Blocks.chain_id),
            ),
        ],
        where_clause=(RebalanceEvents.autopool_vault_address == autopool.autopool_eth_addr),
        order_by=Blocks.datetime,
    )

    tokens_df = merge_tables_as_df(
        [
            TableSelector(
                DestinationTokens,
            ),
            TableSelector(
                table=Tokens,
                select_fields=[Tokens.symbol, Tokens.decimals],
                join_on=(DestinationTokens.token_address == Tokens.token_address),
            ),
        ],
    )
    destinations_df = get_full_table_as_df(Destinations, where_clause=Destinations.chain_id == 1)
    plan_df = get_full_table_as_df(
        RebalancePlans, where_clause=RebalancePlans.autopool_vault_address == autopool.autopool_eth_addr
    )

    destination_to_underlying = destinations_df.set_index("destination_vault_address")["underlying_symbol"].to_dict()

    rebalance_df["destination_in_symbol"] = rebalance_df["destination_in"].map(destination_to_underlying)
    rebalance_df["destination_out_symbol"] = rebalance_df["destination_out"].map(destination_to_underlying)
    # rebalance_df["move_name"] = rebalance_df["destination_out_symbol"] + " -> " + rebalance_df["destination_in_symbol"]

    destination_token_address_to_symbols = (
        tokens_df.groupby("destination_vault_address")["symbol"].apply(tuple).apply(str).to_dict()
    )
    rebalance_df["destination_in_tokens"] = rebalance_df["destination_in"].map(destination_token_address_to_symbols)
    rebalance_df["destination_out_tokens"] = rebalance_df["destination_out"].map(destination_token_address_to_symbols)
    rebalance_df["tokens_move_name"] = (
        rebalance_df["destination_out_tokens"] + " -> " + rebalance_df["destination_in_tokens"]
    )

    rebalance_df["spot_swap_cost"] = rebalance_df["spot_value_out"] - rebalance_df["spot_value_in"]
    rebalance_df["spot_slippage_bps"] = 10_000 * rebalance_df["spot_swap_cost"] / rebalance_df["spot_value_out"]

    rebalance_df["safe_swap_cost"] = rebalance_df["safe_value_out"] - rebalance_df["safe_value_in"]
    rebalance_df["safe_slippage_bps"] = 10_000 * rebalance_df["safe_swap_cost"] / rebalance_df["safe_value_out"]

    # hacky exclude duplicate columns
    common = rebalance_df.columns.intersection(plan_df.columns)
    plan_df = plan_df.drop(columns=common)
    rebalance_df = rebalance_df.merge(plan_df, left_on="rebalance_file_path", right_on="file_name", how="left")
    return rebalance_df


def fetch_and_render_rebalance_events_data(autopool: AutopoolConstants):
    rebalance_df = _load_full_rebalance_event_df(autopool)

    rebalance_figures = _make_rebalance_events_plots(rebalance_df)
    st.header(f"{autopool.symbol} Rebalance Events")

    for figure in rebalance_figures:

        figure.show()
        # st.plotly_chart(figure, use_container_width=True)


def make_expoded_box_plot(df: pd.DataFrame, col: str, resolution: str = "1W"):
    # assumes df is timestmap index
    list_df = df.resample(resolution)[col].apply(list).reset_index()
    exploded_df = list_df.explode(col)
    return px.box(exploded_df, x="timestamp", y=col, title=f"Distribution of {col}")


def _make_rebalance_events_plots(clean_rebalance_df):
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


if __name__ == "__main__":
    from mainnet_launch.constants import AUTO_LRT, AUTO_USD

    fetch_and_render_rebalance_events_data(AUTO_LRT)
