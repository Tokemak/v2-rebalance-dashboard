import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from mainnet_launch.constants import AutopoolConstants


from mainnet_launch.database.schema.postgres_operations import (
    merge_tables_as_df,
    TableSelector,
    get_full_table_as_df,
    insert_avoid_conflicts,
)
from mainnet_launch.database.schema.full import (
    RebalanceEvents,
    RebalancePlans,
    Blocks,
    Destinations,
    Transactions,
    DestinationTokens,
    Tokens,
    Autopools,
    AutopoolStates,
)


def fetch_rebalance_events_df(autopool: AutopoolConstants) -> pd.DataFrame:
    return _load_full_rebalance_event_df(autopool)


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
    rebalance_df = rebalance_df.set_index("datetime")

    autopool_state_df = get_full_table_as_df(
        AutopoolStates, where_clause=AutopoolStates.autopool_vault_address == autopool.autopool_eth_addr
    )[["block", "total_nav"]]
    df = pd.merge(rebalance_df, autopool_state_df, on="block", how="left")
    df.index = rebalance_df.index
    return df


def fetch_and_render_rebalance_events_data(autopool: AutopoolConstants):
    rebalance_df = _load_full_rebalance_event_df(autopool)

    rebalance_figures = _make_rebalance_events_plots(rebalance_df)
    st.header(f"{autopool.symbol} Rebalance Events")

    for figure in rebalance_figures:

        # figure.show()
        st.plotly_chart(figure, use_container_width=True)


def make_expoded_box_plot(df: pd.DataFrame, col: str, resolution: str = "1W"):
    # assumes df is timestmap index
    list_df = df.resample(resolution)[col].apply(list).reset_index()
    exploded_df = list_df.explode(col)
    return px.box(exploded_df, x="timestamp", y=col, title=f"Distribution of {col}")


def _make_rebalance_events_plots(rebalance_df: pd.DataFrame):
    rebalance_df["swap_cost_in_bps_of_value_out"] = rebalance_df["spot_slippage_bps"]
    rebalance_df["swap_cost_in_bps_of_aum"] = 10_000 * rebalance_df["spot_swap_cost"] / rebalance_df["total_nav"]

    total_daily_swap_cost_over_average_nav = rebalance_df.groupby(rebalance_df.index.date).apply(
        lambda g: 10_000 * g["spot_swap_cost"].sum() / g["total_nav"].mean()
    )
    px.line(total_daily_swap_cost_over_average_nav, title="Daily Average Actual Swap Cost bps of NAV (method 2)"),

    # these are side by side
    px.bar(
        rebalance_df["swap_cost_in_bps_of_value_out"],
        title="Per Rebalance Actual Spot Swap Cost bps of spot value out",
    ),
    px.bar(rebalance_df["swap_cost_in_bps_of_aum"], title="Per Rebalance Actual Spot Swap Cost bps of NAV"),


    # these each are side by side
    px.line(
    rebalance_df["swap_cost_in_bps_of_aum"].resample("1d").mean(),
    title="Daily Average Actual Swap Cost bps of NAV (method 1)",
    ),

    px.line(
        rebalance_df["swap_cost_in_bps_of_aum"].resample("1d").mean().rolling(7).mean(),
        title="7-day rolling Daily Average Actual Swap Cost bps of NAV (method 1)",
    ),

    px.line(
        rebalance_df["swap_cost_in_bps_of_aum"].resample("1d").mean().rolling(28).mean(),
        title="28-day rolling Daily Average Actual Swap Cost bps of NAV (method 1)",
    ),


    # these each are also side by side
     px.line(total_daily_swap_cost_over_average_nav, title="Daily Average Actual Swap Cost bps of NAV (method 2)"),




if __name__ == "__main__":
    from mainnet_launch.constants import AUTO_LRT, AUTO_USD

    fetch_and_render_rebalance_events_data(AUTO_LRT)
