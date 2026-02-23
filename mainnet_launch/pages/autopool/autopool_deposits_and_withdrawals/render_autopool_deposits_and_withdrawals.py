import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import plotly.express as px

from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS

from mainnet_launch.database.postgres_operations import _exec_sql_and_cache

from mainnet_launch.database.postgres_operations import merge_tables_as_df, TableSelector
from mainnet_launch.database.schema.full import AutopoolDeposit, AutopoolWithdrawal, Transactions, Blocks

# TODO: not using recent dates filter yet


def fetch_and_render_autopool_deposit_and_withdrawals(autopool: AutopoolConstants):
    deposit_df, withdraw_df = fetch_autopool_deposit_and_withdraw_events(autopool)
    st.header("Autopool Deposit and Withdrawal Stats")
    for n_days in [7, 30]:
        change_fig = _make_deposit_and_withdraw_figure(autopool, deposit_df, withdraw_df, n_days)
        st.plotly_chart(change_fig, use_container_width=True)

    scatter_plot_fig = _make_scatter_plot_figure(autopool, deposit_df, withdraw_df)
    st.plotly_chart(scatter_plot_fig, use_container_width=True)

    with st.expander("Raw Deposit Data"):
        st.dataframe(deposit_df)
    with st.expander("Raw Withdrawal Data"):
        st.dataframe(withdraw_df)


def fetch_autopool_deposit_and_withdraw_events(
    autopool: AutopoolConstants,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # --- Deposits ---
    deposit_selectors = [
        TableSelector(
            AutopoolDeposit,
            [
                AutopoolDeposit.assets,
                AutopoolDeposit.shares,
                AutopoolDeposit.tx_hash,
            ],
        ),
        TableSelector(
            Transactions,
            [Transactions.block, Transactions.chain_id],
            join_on=(Transactions.tx_hash == AutopoolDeposit.tx_hash),
        ),
        TableSelector(
            Blocks,
            [Blocks.datetime],
            join_on=(Transactions.block == Blocks.block) & (Transactions.chain_id == Blocks.chain_id),
        ),
    ]

    deposit_where = (
        (AutopoolDeposit.autopool_vault_address == autopool.autopool_eth_addr)
        & (AutopoolDeposit.chain_id == autopool.chain.chain_id)
        & (Blocks.datetime >= autopool.get_display_date())
    )

    deposits_df = merge_tables_as_df(
        selectors=deposit_selectors,
        where_clause=deposit_where,
        order_by=Blocks.datetime,
    ).set_index("datetime")

    withdrawal_selectors = [
        TableSelector(
            AutopoolWithdrawal,
            [
                AutopoolWithdrawal.assets,
                AutopoolWithdrawal.shares,
                AutopoolWithdrawal.tx_hash,
            ],
        ),
        TableSelector(
            Transactions,
            [Transactions.block, Transactions.chain_id],
            join_on=(Transactions.tx_hash == AutopoolWithdrawal.tx_hash),
        ),
        TableSelector(
            Blocks,
            [Blocks.datetime],
            join_on=(Transactions.block == Blocks.block) & (Transactions.chain_id == Blocks.chain_id),
        ),
    ]

    withdrawal_where = (
        (AutopoolWithdrawal.autopool_vault_address == autopool.autopool_eth_addr)
        & (AutopoolWithdrawal.chain_id == autopool.chain.chain_id)
        & (Blocks.datetime >= autopool.get_display_date())
    )

    withdrawals_df = merge_tables_as_df(
        selectors=withdrawal_selectors,
        where_clause=withdrawal_where,
        order_by=Blocks.datetime,
    ).set_index("datetime")

    return deposits_df, withdrawals_df


def _make_deposit_and_withdraw_figure(
    autopool: AutopoolConstants, deposit_df: pd.DataFrame, withdraw_df: pd.DataFrame, n_days: int
) -> go.Figure:
    if deposit_df.empty and withdraw_df.empty:
        fig = go.Figure()
        fig.update_layout(title=f"{autopool.name} No deposit/withdraw data", height=400)
        return fig

    change_df = (
        pd.merge(
            deposit_df.rename(columns={"assets": "deposits"})["deposits"],
            withdraw_df.rename(columns={"assets": "withdrawals"})["withdrawals"],
            left_index=True,
            right_index=True,
            how="outer",
        )
        .astype(float)
        .fillna(0)
        .round(2)
    )
    change_df["withdrawals"] = -change_df["withdrawals"]  # Make withdrawals negative for net change calculation

    change_df = change_df.resample(f"{n_days}D").sum()
    change_df["net_change"] = change_df["deposits"] + change_df["withdrawals"]

    fig = px.bar(change_df, x=change_df.index, y=["deposits", "withdrawals", "net_change"])

    fig.update_layout(
        title=f"{autopool.name} Total Withdrawals, Deposits, and Net Change per {n_days} Days",
        xaxis_tickformat="%Y-%m-%d",
        xaxis_title="Date",
        yaxis_title="Base Asset",
        barmode="group",
        xaxis_tickangle=-45,
        width=900,
        height=500,
    )
    return fig


def _make_scatter_plot_figure(
    autopool: AutopoolConstants, deposit_df: pd.DataFrame, withdraw_df: pd.DataFrame
) -> go.Figure:
    if deposit_df.empty and withdraw_df.empty:
        fig = go.Figure()
        fig.update_layout(title=f"{autopool.name} No deposit/withdraw data", height=400)
        return fig

    deposits = deposit_df[["assets"]].copy()
    withdraws = (-withdraw_df[["assets"]]).copy()

    change_df = pd.concat([df for df in [withdraws, deposits] if not df.empty])
    change_df["color"] = change_df["assets"].apply(lambda x: "Deposit" if x >= 0 else "Withdrawal")

    fig = px.scatter(
        change_df,
        y="assets",
        size=change_df["assets"].abs(),
        color=change_df["color"],
        color_discrete_map={"Deposit": "blue", "Withdrawal": "red"},
    )
    fig.update_layout(
        title=f"{autopool.name} Individual Deposits and Withdrawals",
        xaxis_title="Date",
        yaxis_title="Base Asset",
        xaxis_tickangle=-45,
        width=900,
        height=500,
    )
    return fig


if __name__ == "__main__":
    fetch_and_render_autopool_deposit_and_withdrawals(ALL_AUTOPOOLS[0])
