import pandas as pd
import streamlit as st
import plotly.express as px
import json
import boto3
from botocore import UNSIGNED
from botocore.config import Config


from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.database.postgres_operations import (
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
                AutopoolStates,
                [AutopoolStates.total_nav],
                join_on=(AutopoolStates.block == Transactions.block),
                row_filter=AutopoolStates.autopool_vault_address == autopool.autopool_eth_addr,
            ),
            TableSelector(
                RebalancePlans,
                [RebalancePlans.move_name],
                join_on=(RebalancePlans.file_name == RebalanceEvents.rebalance_file_path),
                row_filter=RebalancePlans.autopool_vault_address == autopool.autopool_eth_addr,
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

    destinations_df = get_full_table_as_df(Destinations, where_clause=Destinations.chain_id == autopool.chain.chain_id)

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

    #     df["long_move_name"] = df["move_name"] + "   " + df["tokens_move_name"]
    # cond = df["move_name"].isin(["autoDOLA -> sDOLA", "baseUSD -> fUSDC", "baseUSD -> smUSDC", "baseUSD -> mwUSDC"])
    # cond = cond | (df["tokens_move_name"] == "('USDC',) -> ('USDC',)")
    # df["adjusted_spot_swap_cost"] = df["spot_value_in_solver_change"].where(
    #     cond, df["spot_swap_cost"] - df["spot_value_in_solver_change"]
    # )
    # df["adjusted_spot_swap_cost_in_bps_of_value_out"] = 10_000 * df["adjusted_spot_swap_cost"] / df["spot_value_out"]
    # df["adjusted_spot_swap_cost_in_bps_of_NAV"] = 10_000 * df["adjusted_spot_swap_cost"] / df["total_nav"]

    # if the swap should be not losses less (eg staking lending base -> base asset deployments)
    # swap cost = spot value out - spot value in
    # if we do think it should be lossless then
    # swap cost = spot value difference in solver

    # todo I don't like this pattern

    autoUSD_lossless_move_names = [
        "autoUSD -> fUSDC",
        "gtUSDCcore -> fUSDC",
        "autoUSD -> gtUSDC",
        "gtUSDC -> fUSDC",
        "steakUSDC -> fUSDC",
        "fUSDC -> gtUSDCcore",
        "autoUSD -> gtUSDCcore",
        "autoUSD -> steakUSDC",
    ]

    autoDOLA_lossless_move_names = ["autoDOLA -> sDOLA"]

    baseUSD_lossless_move_names = [
        "baseUSD -> smUSDC",
        "baseUSD -> fUSDC",
        "baseUSD -> mwUSDC",
        "baseUSD -> eUSDC-1",
        "eUSDC-1 -> smUSDC",
        "smUSDC -> mwUSDC",
    ]

    rebalance_df["expected_to_be_lossless"] = rebalance_df["move_name"].isin(
        [*autoUSD_lossless_move_names, *autoDOLA_lossless_move_names, *baseUSD_lossless_move_names]
    )
    rebalance_df["spot_swap_cost"] = rebalance_df["spot_value_out"] - rebalance_df["spot_value_in"]

    rebalance_df["spot_swap_cost"] = rebalance_df["spot_swap_cost"].where(
        ~rebalance_df["expected_to_be_lossless"], rebalance_df["spot_value_in_solver_change"]
    )

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

    rebalance_df["spot_swap_cost_less_value_in_solver"] = (
        rebalance_df["spot_value_out"] - rebalance_df["spot_value_in"]
    ) - rebalance_df["spot_value_in_solver_change"]
    rebalance_df["spot_swap_cost_less_value_in_solver"] = rebalance_df["spot_swap_cost_less_value_in_solver"].where(
        ~rebalance_df["expected_to_be_lossless"], rebalance_df["spot_value_in_solver_change"]
    )
    rebalance_df["spot_slippage_bps_less_value_in_solver"] = (
        rebalance_df["spot_swap_cost_less_value_in_solver"] / rebalance_df["spot_value_out"] * 10_000
    )

    return rebalance_df


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
    rolling7 = daily_nav.rolling(7).sum()
    fig4 = px.bar(
        rolling7,
        x=rolling7.index,
        y=["swap_cost_in_bps_of_NAV_from_idle", "swap_cost_in_bps_of_NAV_not_from_idle"],
        title="7-day rolling daily sum actual swap cost bps of NAV (idle vs not)",
        barmode="stack",
    )

    # 28‐day rolling sum stacked on NAV
    rolling28 = daily_nav.rolling(28).sum()
    fig5 = px.bar(
        rolling28,
        x=rolling28.index,
        y=["swap_cost_in_bps_of_NAV_from_idle", "swap_cost_in_bps_of_NAV_not_from_idle"],
        title="28-day rolling daily sum actual swap cost bps of NAV (idle vs not)",
        barmode="stack",
    )

    return [fig1, fig2, fig3, fig4, fig5]


def make_expoded_box_plot(df: pd.DataFrame, col: str, resolution: str = "1W"):
    # assumes df is timestmap index
    list_df = df.resample(resolution)[col].apply(list).reset_index()
    exploded_df = list_df.explode(col)
    return px.box(exploded_df, x="timestamp", y=col, title=f"Distribution of {col}")


def fetch_and_render_rebalance_events_data(autopool: AutopoolConstants):
    rebalance_df = _load_full_rebalance_event_df(autopool)

    rebalance_figures = _make_rebalance_events_plots(rebalance_df)
    st.header(f"{autopool.symbol} Rebalance Events")

    for figure in rebalance_figures:
        st.plotly_chart(figure, use_container_width=True)

    st.subheader("Individual Rebalance Events Data")

    date_cutoff = st.date_input(
        "show events after",
        value=rebalance_df.index.min().tz_convert("UTC").date(),
        min_value=rebalance_df.index.min().tz_convert("UTC").date(),
        max_value=rebalance_df.index.max().tz_convert("UTC").date(),
    )
    date_cutoff = pd.Timestamp(date_cutoff, tz="UTC")
    filtered_rebalance_df = rebalance_df[rebalance_df.index >= date_cutoff]

    render_average_destination_to_destination_move_performance(filtered_rebalance_df)

    st.plotly_chart(
        px.scatter(
            filtered_rebalance_df,
            x=filtered_rebalance_df.index,
            y="spot_slippage_bps_less_value_in_solver",
            color="move_name",
        ),
        use_container_width=True,
    )
    st.plotly_chart(
        px.scatter(filtered_rebalance_df, x=filtered_rebalance_df.index, y="spot_value_out", color="move_name"),
        use_container_width=True,
    )

    st.plotly_chart(
        px.scatter(
            filtered_rebalance_df, x="spot_value_out", y="spot_slippage_bps_less_value_in_solver", color="move_name"
        ),
        use_container_width=True,
    )

    render_fetch_plan_ui(rebalance_df, autopool)

    with st.expander("All Rebalance Events"):
        st.download_button(
            label="Download Rebalance Events Data",
            data=rebalance_df.to_csv().encode("utf-8"),
            file_name=f"{autopool.name}_rebalance_events.csv",
            mime="text/csv",
        )

        st.dataframe(filtered_rebalance_df, use_container_width=True)


def render_average_destination_to_destination_move_performance(rebalance_df: pd.DataFrame):
    grp = (
        rebalance_df.groupby("move_name")
        .sum()[
            [
                "spot_value_out",
                "spot_swap_cost",
                "spot_swap_cost_less_value_in_solver",
            ]
        ]
        .rename(
            columns={
                "spot_value_out": "Total Value Out",
                "spot_swap_cost": "Total Swap Cost",
                "spot_swap_cost_less_value_in_solver": "Total Swap Cost (less solver)",
            }
        )
    )

    # bps averages
    grp["Average Swap Cost bps"] = grp["Total Swap Cost"] / grp["Total Value Out"] * 10_000
    grp["Average Swap Cost bps (less solver)"] = grp["Total Swap Cost (less solver)"] / grp["Total Value Out"] * 10_000

    st.subheader("Move Performance Summary")
    st.dataframe(
        grp[["Total Value Out", "Average Swap Cost bps", "Average Swap Cost bps (less solver)"]].style.format("{:.2f}"),
        use_container_width=True,
    )


def render_fetch_plan_ui(rebalance_df: pd.DataFrame, autopool: AutopoolConstants):
    with st.form("fetch_plan_form"):
        tx = st.text_input("rebalance event transaction hash")
        submitted = st.form_submit_button("fetch plan")
    if submitted:
        try:
            with st.spinner("fetching plan..."):
                plan = _fetch_plan_for_tx(tx.strip(), rebalance_df, autopool)
            st.json(plan)
        except KeyError:
            st.error(f"no plan found for tx {tx}")
        except Exception as e:
            st.error(f"unexpected error: {e}")


def _fetch_plan_for_tx(tx_hash: str, rebalance_df: pd.DataFrame, autopool: AutopoolConstants) -> dict:
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    row = rebalance_df.loc[rebalance_df["tx_hash"].str.lower() == tx_hash.lower()]
    if row.empty:
        raise KeyError(f"no plan for tx {tx_hash}")
    key = row.iloc[0]["rebalance_file_path"]

    resp = s3.get_object(Bucket=autopool.solver_rebalance_plans_bucket, Key=key)
    return json.loads(resp["Body"].read())


if __name__ == "__main__":
    from mainnet_launch.constants import *

    rebalance_df = fetch_and_render_rebalance_events_data(AUTO_DOLA)

    # rebalance_df.to_csv("mainnet_launch/working_data/autoUSD_rebalance_df_swap_costs.csv")
