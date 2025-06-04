import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.database.schema.postgres_operations import (
    merge_tables_as_df,
    TableSelector,
    get_full_table_as_df,
)
from mainnet_launch.database.schema.full import (
    RebalanceEvents,
    RebalancePlans,
    Blocks,
    Destinations,
    Transactions,
    DestinationTokens,
    Tokens,
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
                AutopoolStates, [AutopoolStates.total_nav], join_on=(AutopoolStates.block == Transactions.block)
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

    # plan_df = get_full_table_as_df(
    #     RebalancePlans, where_clause=RebalancePlans.autopool_vault_address == autopool.autopool_eth_addr
    # )

    #     autopool_state_df = get_full_table_as_df(
    #     AutopoolStates, where_clause=AutopoolStates.autopool_vault_address == autopool.autopool_eth_addr
    # )[["block", "total_nav"]]

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

    destination_to_underlying = destinations_df.set_index("destination_vault_address")["underlying_symbol"].to_dict()

    rebalance_df["destination_in_symbol"] = rebalance_df["destination_in"].map(destination_to_underlying)
    rebalance_df["destination_out_symbol"] = rebalance_df["destination_out"].map(destination_to_underlying)

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

    rebalance_df = rebalance_df.set_index("datetime")
    rebalance_df["swap_cost_in_bps_of_value_out"] = rebalance_df["spot_slippage_bps"]
    rebalance_df["swap_cost_in_bps_of_NAV"] = 10_000 * rebalance_df["spot_swap_cost"] / rebalance_df["total_nav"]

    rebalance_df["from_idle"] = rebalance_df["destination_out"] == autopool.autopool_eth_addr
    

    rebalance_df["swap_cost_in_bps_of_value_out_from_idle"] = rebalance_df["swap_cost_in_bps_of_value_out"].where(
        rebalance_df["from_idle"], 0
    )
    rebalance_df["swap_cost_in_bps_of_NAV_from_idle"] = rebalance_df["swap_cost_in_bps_of_NAV"].where(
        rebalance_df["from_idle"], 0
    )

    rebalance_df["swap_cost_in_bps_of_value_out_not_from_idle"] = rebalance_df["swap_cost_in_bps_of_value_out"].where(
        ~rebalance_df["from_idle"], 0
    )
    rebalance_df["swap_cost_in_bps_of_NAV_not_from_idle"] = rebalance_df["swap_cost_in_bps_of_NAV"].where(
        ~rebalance_df["from_idle"], 0
    )

    return rebalance_df


def fetch_and_render_rebalance_events_data(autopool: AutopoolConstants):
    rebalance_df = _load_full_rebalance_event_df(autopool)

    rebalance_figures = _make_rebalance_events_plots(rebalance_df)
    st.header(f"{autopool.symbol} Rebalance Events")

    for figure in rebalance_figures:
        st.plotly_chart(figure, use_container_width=True)


def _make_rebalance_events_plots(rebalance_df: pd.DataFrame):
    # per‐event stacked on value_out
    fig1 = px.bar(
        rebalance_df,
        x=rebalance_df.index,
        y=[
            "swap_cost_in_bps_of_value_out_from_idle",
            "swap_cost_in_bps_of_value_out_not_from_idle",
        ],
        title="per rebalance actual spot swap cost bps of spot value out (idle vs not)",
        barmode="stack",
    )

    # per‐event stacked on NAV
    fig2 = px.bar(
        rebalance_df,
        x=rebalance_df.index,
        y=["swap_cost_in_bps_of_NAV_from_idle", "swap_cost_in_bps_of_NAV_not_from_idle"],
        title="per rebalance actual spot swap cost bps of NAV (idle vs not)",
        barmode="stack",
    )

    # daily sum stacked on NAV
    daily_nav = rebalance_df.resample("1d")[
        ["swap_cost_in_bps_of_NAV_from_idle", "swap_cost_in_bps_of_NAV_not_from_idle"]
    ].sum()
    fig3 = px.bar(
        daily_nav,
        x=daily_nav.index,
        y=["swap_cost_in_bps_of_NAV_from_idle", "swap_cost_in_bps_of_NAV_not_from_idle"],
        title="daily sum actual swap cost bps of NAV (idle vs not)",
        barmode="stack",
    )

    # 7‐day rolling sum stacked on NAV
    rolling7 = daily_nav.rolling(7).sum().dropna()
    fig4 = px.bar(
        rolling7,
        x=rolling7.index,
        y=["swap_cost_in_bps_of_NAV_from_idle", "swap_cost_in_bps_of_NAV_not_from_idle"],
        title="7‑day rolling daily sum actual swap cost bps of NAV (idle vs not)",
        barmode="stack",
    )

    # 28‐day rolling sum stacked on NAV
    rolling28 = daily_nav.rolling(28).sum().dropna()
    fig5 = px.bar(
        rolling28,
        x=rolling28.index,
        y=["swap_cost_in_bps_of_NAV_from_idle", "swap_cost_in_bps_of_NAV_not_from_idle"],
        title="28‑day rolling daily sum actual swap cost bps of NAV (idle vs not)",
        barmode="stack",
    )

    return [fig1, fig2, fig3, fig4, fig5]


def make_expoded_box_plot(df: pd.DataFrame, col: str, resolution: str = "1W"):
    # assumes df is timestmap index
    list_df = df.resample(resolution)[col].apply(list).reset_index()
    exploded_df = list_df.explode(col)
    return px.box(exploded_df, x="timestamp", y=col, title=f"Distribution of {col}")


if __name__ == "__main__":
    from mainnet_launch.constants import AUTO_LRT, AUTO_USD

    fetch_and_render_rebalance_events_data(AUTO_USD)
