import pandas as pd
import streamlit as st

import plotly.express as px
from mainnet_launch.constants import AutopoolConstants

from mainnet_launch.database.schema.full import *
from mainnet_launch.database.schema.postgres_operations import TableSelector, merge_tables_as_df, get_full_table_as_df


def _load_actual_rebalance_events_df(autopool: AutopoolConstants) -> pd.DataFrame:
    actual_rebalance_events_df = merge_tables_as_df(
        selectors=[
            TableSelector(
                RebalanceEvents,
                [
                    RebalanceEvents.quantity_out,
                    RebalanceEvents.quantity_in,
                    RebalanceEvents.safe_value_out,
                    RebalanceEvents.safe_value_in,
                    RebalanceEvents.spot_value_out,
                    RebalanceEvents.spot_value_in,
                    RebalanceEvents.spot_value_in_solver_change,
                ],
            ),
            TableSelector(
                RebalancePlans,
                join_on=(RebalancePlans.file_name == RebalanceEvents.rebalance_file_path),
                row_filter=RebalancePlans.autopool_vault_address == autopool.autopool_eth_addr,
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
    actual_rebalance_events_df["latency"] = (
        (actual_rebalance_events_df["datetime"] - actual_rebalance_events_df["datetime_generated"]).dt.total_seconds()
    ) / 60

    return actual_rebalance_events_df


def _load_dex_swap_steps_df(autopool: AutopoolConstants) -> pd.DataFrame:
    dex_swap_steps_df = merge_tables_as_df(
        selectors=[
            TableSelector(
                DexSwapSteps,
                [DexSwapSteps.step_index, DexSwapSteps.dex, DexSwapSteps.aggregator_names],
            ),
            TableSelector(
                RebalanceEvents,
                [
                    RebalanceEvents.tx_hash,
                ],
                join_on=(DexSwapSteps.file_name == RebalanceEvents.rebalance_file_path),
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
    return dex_swap_steps_df


def fetch_and_render_solver_diagnostics_data(autopool: AutopoolConstants):
    actual_rebalance_events_df = _load_actual_rebalance_events_df(autopool)
    dex_swap_steps_df = _load_dex_swap_steps_df(autopool)
    proposed_rebalances_df = get_full_table_as_df(
        RebalancePlans, where_clause=RebalancePlans.autopool_vault_address == autopool.autopool_eth_addr
    ).sort_values("datetime_generated")

    st.header(f"Solver Diagnostics for {autopool.name}")

    render_dex_win_by_steps(dex_swap_steps_df)  # dex win count, by step

    st.plotly_chart(
        px.scatter(
            actual_rebalance_events_df,
            x="datetime",
            y="latency",
            title="Latency (minutes) Between Solver Generation and Execution",
        )
    )

    actual_rebalance_events_df["hoursBetween"] = (
        actual_rebalance_events_df["datetime"].diff().dt.total_seconds()
    ) / 3600
    st.plotly_chart(
        px.scatter(
            y=actual_rebalance_events_df["hoursBetween"],
            x=actual_rebalance_events_df["datetime"],
            title="Hours Between Rebalance Events",
            labels={"x": "Rebalances", "y": "Hours"},
        )
    )

    proposed_rebalances_df["hoursBetween"] = (
        proposed_rebalances_df["datetime_generated"].diff().dt.total_seconds()
    ) / 3600
    st.plotly_chart(
        px.scatter(
            y=proposed_rebalances_df["hoursBetween"],
            x=proposed_rebalances_df["datetime_generated"],
            title="Hours Between Rebalance Plans Generated",
            labels={"x": "Rebalances", "y": "Hours"},
        )
    )

    return actual_rebalance_events_df, dex_swap_steps_df, proposed_rebalances_df


def render_dex_win_by_steps(dex_swap_steps_df: pd.DataFrame) -> pd.DataFrame:
    dex_swap_steps_df["step_dex_info"] = (
        dex_swap_steps_df["dex"] + " " + dex_swap_steps_df["aggregator_names"].fillna("").astype(str)
    )

    latest = pd.Timestamp.now(tz="UTC")
    thirty_days_ago = latest - pd.Timedelta(days=30)
    seven_days_ago = latest - pd.Timedelta(days=7)

    counts_all = dex_swap_steps_df["step_dex_info"].value_counts(normalize=True)
    counts_30 = dex_swap_steps_df[dex_swap_steps_df["datetime"] >= thirty_days_ago]["step_dex_info"].value_counts(
        normalize=True
    )
    counts_7 = dex_swap_steps_df[dex_swap_steps_df["datetime"] >= seven_days_ago]["step_dex_info"].value_counts(
        normalize=True
    )

    percents_df = (
        (100 * pd.DataFrame({"All Time": counts_all, "Last 30 Days": counts_30, "Last Seven Days": counts_7}))
        .round()
        .fillna(0.0)
    )

    counts_all = dex_swap_steps_df["step_dex_info"].value_counts(normalize=False)
    counts_30 = dex_swap_steps_df[dex_swap_steps_df["datetime"] >= thirty_days_ago]["step_dex_info"].value_counts(
        normalize=False
    )
    counts_7 = dex_swap_steps_df[dex_swap_steps_df["datetime"] >= seven_days_ago]["step_dex_info"].value_counts(
        normalize=False
    )

    st.subheader(f"Count, (Percent) of Swap Steps by DEX and Aggregator")
    counts_df = pd.DataFrame({"All Time": counts_all, "Last 30 Days": counts_30, "Last Seven Days": counts_7}).fillna(0)
    st.dataframe(counts_df.combine(percents_df, func=lambda x, y: x.astype(str) + " (" + y.astype(str) + "%)"))


if __name__ == "__main__":
    # streamlit run mainnet_launch/pages/solver_diagnostics/solver_diagnostics.py
    from mainnet_launch.constants import ALL_AUTOPOOLS

    for autopool in ALL_AUTOPOOLS:
        fetch_and_render_solver_diagnostics_data(autopool)
        st.write(f"### \n\n\n")
