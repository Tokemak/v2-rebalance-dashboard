"""Returns of the autopool before and after expenses and fees"""

import pandas as pd
import streamlit as st
from multicall import Call
import plotly.graph_objects as go

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)

from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats

from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.constants import AutopoolConstants, CACHE_TIME
from mainnet_launch.abis import AUTOPOOL_VAULT_ABI

from mainnet_launch.pages.rebalance_events.fetch_rebalance_events import (
    fetch_rebalance_events_df,
)


@st.cache_data(ttl=CACHE_TIME)
def fetch_nav_and_shares_and_factors_that_impact_nav_per_share(autopool: AutopoolConstants) -> pd.DataFrame:
    daily_nav_shares_df = _fetch_actual_nav_per_share_by_day(autopool)
    daily_shares_minted = _fetch_shares_minted_per_day(autopool)
    cumulative_rebalance_from_idle_swap_cost, cumulative_rebalance_not_from_idle_swap_cost = (
        _fetch_daily_nav_lost_to_rebalances(autopool)
    )
    implied_extra_nav_if_price_return_is_zero = _fetch_implied_extra_nav_if_price_return_is_zero(autopool)
    df = pd.concat(
        [
            daily_nav_shares_df,
            daily_shares_minted,
            cumulative_rebalance_from_idle_swap_cost,
            cumulative_rebalance_not_from_idle_swap_cost,
            implied_extra_nav_if_price_return_is_zero,
        ],
        axis=1,
    ).fillna(0)

    # handle fractinal days in blocks
    # df.iloc[0] = df.iloc[0].fillna(0)
    # df = df.ffill()
    # df = df.resample("1D").last()  # just drop the tail

    # last full day, but we have a day lag
    # this should be the last full day,
    # eg we could be sampling hour 6, or 12 or 18 instead of hour 24
    # just ignore the current day since we could exculde it
    # we have a day lag in the subgraph

    # if we resample for 1 day, last value in the day
    # if we skip the last sample
    # skip the current day

    # look through make we don't treat fractinal days as full days

    return df


def _fetch_implied_extra_nav_if_price_return_is_zero(autopool: AutopoolConstants) -> pd.DataFrame:
    pricePerShare_df = fetch_destination_summary_stats(autopool, "pricePerShare")
    ownedShares_df = fetch_destination_summary_stats(autopool, "ownedShares")
    allocation_df = pricePerShare_df * ownedShares_df

    priceReturn_df = fetch_destination_summary_stats(autopool, "priceReturn")

    implied_extra_nav_if_price_return_is_zero = (allocation_df * priceReturn_df).sum(axis=1).resample("1D").last()
    implied_extra_nav_if_price_return_is_zero.name = "additional_nav_if_price_return_was_0"
    return implied_extra_nav_if_price_return_is_zero


def _fetch_actual_nav_per_share_by_day(autopool: AutopoolConstants) -> pd.DataFrame:

    def handle_getAssetBreakdown(success, AssetBreakdown):
        if success:
            totalIdle, totalDebt, totalDebtMin, totalDebtMax = AssetBreakdown
            return int(totalIdle + totalDebt) / 1e18
        return None

    calls = [
        Call(
            autopool.autopool_eth_addr,
            ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
            [("actual_nav", handle_getAssetBreakdown)],
        ),
        Call(
            autopool.autopool_eth_addr,
            ["totalSupply()(uint256)"],
            [("actual_shares", safe_normalize_with_bool_success)],
        ),
    ]

    blocks = build_blocks_to_use(autopool.chain)
    df = get_raw_state_by_blocks(calls, blocks, autopool.chain)
    daily_nav_shares_df = df.resample("1D").last()
    return daily_nav_shares_df


def _fetch_daily_nav_lost_to_rebalances(autopool: AutopoolConstants) -> pd.DataFrame:
    rebalance_df = fetch_rebalance_events_df(autopool)

    rebalance_from_idle_df = rebalance_df[
        rebalance_df["outDestinationVault"].str.lower() == autopool.autopool_eth_addr.lower()
    ].copy()
    rebalance_not_from_idle_df = rebalance_df[
        ~(rebalance_df["outDestinationVault"].str.lower() == autopool.autopool_eth_addr.lower())
    ].copy()

    daily_rebalance_from_idle_swap_cost = rebalance_from_idle_df["swapCost"].resample("1D").sum()
    daily_rebalance_from_idle_swap_cost.name = "rebalance_from_idle_swap_cost"

    daily_rebalance_not_from_idle_swap_cost = rebalance_not_from_idle_df["swapCost"].resample("1D").sum()
    daily_rebalance_not_from_idle_swap_cost.name = "rebalance_not_idle_swap_cost"
    return daily_rebalance_not_from_idle_swap_cost, daily_rebalance_from_idle_swap_cost


def _fetch_shares_minted_per_day(autopool: AutopoolConstants) -> pd.DataFrame:
    vault_contract = autopool.chain.client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
    FeeCollected_df = add_timestamp_to_df_with_block_column(
        fetch_events(vault_contract.events.FeeCollected), autopool.chain
    )

    PeriodicFeeCollected_df = add_timestamp_to_df_with_block_column(
        fetch_events(vault_contract.events.PeriodicFeeCollected), autopool.chain
    )
    PeriodicFeeCollected_df["new_shares_from_periodic_fees"] = PeriodicFeeCollected_df["mintedShares"] / 1e18
    FeeCollected_df["new_shares_from_streaming_fees"] = FeeCollected_df["mintedShares"] / 1e18
    fee_df = pd.concat(
        [
            PeriodicFeeCollected_df[["new_shares_from_periodic_fees"]],
            FeeCollected_df[["new_shares_from_streaming_fees"]],
        ]
    )
    daily_fee_shares_df = fee_df.resample("1D").sum()
    return daily_fee_shares_df


def _compute_adjusted_nav_per_share_n_days(
    df: pd.DataFrame,
    n_days: int,
    apply_periodic_fees: bool,
    apply_streaming_fees: bool,
    apply_rebalance_from_idle_swap_cost: bool,
    apply_rebalance_not_idle_swap_cost: bool,
    apply_nav_lost_to_depeg: bool,
):
    adjusted_shares = df["actual_shares"].copy()
    adjusted_nav = df["actual_nav"].copy()
    nav_per_share_df = df.copy()

    if apply_periodic_fees:
        adjusted_shares -= df["new_shares_from_periodic_fees"].rolling(n_days).sum()
    if apply_streaming_fees:
        adjusted_shares -= df["new_shares_from_streaming_fees"].rolling(n_days).sum()
    if apply_rebalance_from_idle_swap_cost:
        adjusted_nav += df["rebalance_from_idle_swap_cost"].rolling(n_days).sum()
    if apply_rebalance_not_idle_swap_cost:
        adjusted_nav += df["rebalance_not_idle_swap_cost"].rolling(n_days).sum()
    if apply_nav_lost_to_depeg:
        # change the nav to what it would be at the each block if it was at peg
        adjusted_nav += df["additional_nav_if_price_return_was_0"]

    nav_per_share_df["adjusted_nav_per_share"] = adjusted_nav / adjusted_shares
    nav_per_share_df["actual_nav_per_share"] = df["actual_nav"] / df["actual_shares"]

    nav_per_share_df[f"actual_{n_days}_days_annualized_apr"] = (
        100
        * (365 / n_days)
        * (
            (nav_per_share_df["actual_nav_per_share"] - nav_per_share_df["actual_nav_per_share"].shift(n_days))
            / nav_per_share_df["actual_nav_per_share"].shift(n_days)
        )
    )
    nav_per_share_df[f"adjusted_{n_days}_days_annualized_apr"] = (
        100
        * (365 / n_days)
        * (
            (nav_per_share_df["adjusted_nav_per_share"] - nav_per_share_df["actual_nav_per_share"].shift(n_days))
            / nav_per_share_df["actual_nav_per_share"].shift(n_days)
        )
    )

    return nav_per_share_df[
        [
            "adjusted_nav_per_share",
            "actual_nav_per_share",
            f"adjusted_{n_days}_days_annualized_apr",
            f"actual_{n_days}_days_annualized_apr",
        ]
    ].copy()


def _make_nav_per_share_figure(nav_per_share_df: pd.DataFrame, n_days: int) -> go.Figure:
    nav_per_share_fig = go.Figure()
    nav_per_share_fig.add_trace(
        go.Scatter(
            x=nav_per_share_df.index,
            y=nav_per_share_df[f"actual_nav_per_share"],
            mode="lines+markers",
            name="Actual Nav Per Share",
        )
    )
    nav_per_share_fig.add_trace(
        go.Scatter(
            x=nav_per_share_df.index,
            y=nav_per_share_df[f"adjusted_nav_per_share"],
            mode="lines+markers",
            name="Adjusted Nav Per Share",
        )
    )

    # nav_per_share_fig.add_trace(
    #     go.Scatter(
    #         x=nav_per_share_df.index,
    #         y=nav_per_share_df[f"adjusted_nav_per_share"] - nav_per_share_df[f"actual_nav_per_share"],
    #         mode="lines+markers",
    #         name="Adjusted - Actual NAV Per Share",
    #         yaxis="y2",
    #     )
    # )

    nav_per_share_fig.update_layout(
        title=f"{n_days} Days Nav Per Share",
        xaxis_title="Date",
        yaxis_title="NAV Per Share",
        # legend_title="Legend",
        # yaxis2=dict(title="Difference (Adjusted - Actual) Nav Per Share", overlaying="y", side="right"),
    )
    return nav_per_share_fig


def _make_apr_figure(nav_per_share_df: pd.DataFrame, n_days: int) -> go.Figure:
    apr_fig = go.Figure()

    apr_fig.add_trace(
        go.Scatter(
            x=nav_per_share_df.index,
            y=nav_per_share_df[f"actual_{n_days}_days_annualized_apr"],
            mode="lines+markers",
            name="Original APR",
        )
    )

    apr_fig.add_trace(
        go.Scatter(
            x=nav_per_share_df.index,
            y=nav_per_share_df[f"adjusted_{n_days}_days_annualized_apr"],
            mode="lines+markers",
            name="Adjusted APR",
        )
    )

    # apr_fig.add_trace(
    #     go.Scatter(
    #         x=nav_per_share_df.index,
    #         y=nav_per_share_df[f"adjusted_{n_days}_days_annualized_apr"]
    #         - nav_per_share_df[f"actual_{n_days}_days_annualized_apr"],
    #         mode="lines+markers",
    #         name="Adjusted - Original APR Difference",
    #         yaxis="y2",
    #     )
    # )

    apr_fig.update_layout(
        title=f"{n_days} Days Annualized APR",
        xaxis_title="Date",
        yaxis_title="APR",
        # legend_title="Legend",
        # yaxis2=dict(title="Difference (Adjusted - Original APR)", overlaying="y", side="right"),
    )
    return apr_fig


def _create_n_days_apr_fig(
    df: pd.DataFrame,
    n_days: int,
    apply_periodic_fees: bool,
    apply_streaming_fees: bool,
    apply_rebalance_from_idle_swap_cost: bool,
    apply_rebalance_not_idle_swap_cost: bool,
    apply_nav_lost_to_depeg: bool,
):
    nav_per_share_df = _compute_adjusted_nav_per_share_n_days(
        df,
        n_days,
        apply_periodic_fees,
        apply_streaming_fees,
        apply_rebalance_from_idle_swap_cost,
        apply_rebalance_not_idle_swap_cost,
        apply_nav_lost_to_depeg,
    )
    nav_per_share_fig = _make_nav_per_share_figure(nav_per_share_df, n_days)
    apr_fig = _make_apr_figure(nav_per_share_df, n_days)

    last_row = nav_per_share_df.tail(1).to_dict(orient="records")[0]

    bridge_fig = _make_bridge_figure(
        [
            last_row[f"actual_{n_days}_days_annualized_apr"],
            last_row[f"adjusted_{n_days}_days_annualized_apr"] - last_row[f"actual_{n_days}_days_annualized_apr"],
            last_row[f"adjusted_{n_days}_days_annualized_apr"],
        ],
        ["Acutal APR", "Diff", "Adjusted APR"],
        title=f"{n_days} Days Annualized APR",
    )

    return nav_per_share_fig, apr_fig, bridge_fig


def _make_bridge_figure(values: list[float], names: list[str], title: str):

    measure = ["absolute", "relative", "total"]
    fig = go.Figure(
        go.Waterfall(
            name="APR Comparison",
            orientation="v",
            measure=measure,  #
            x=names,
            y=values,
            connector={"line": {"color": "rgb(63, 63, 63)"}},
        )
    )

    fig.update_layout(
        title=title,
        waterfallgap=0.3,
        showlegend=True,
        xaxis_title="APR Components",
        yaxis_title="Values",
    )

    return fig


def _create_figs(
    df: pd.DataFrame,
    apply_periodic_fees: bool,
    apply_streaming_fees: bool,
    apply_rebalance_from_idle_swap_cost: bool,
    apply_rebalance_not_idle_swap_cost: bool,
    apply_nav_lost_to_depeg: bool,
):
    nav_per_share_fig_30_days, apr_fig_30_day, bridge_fig_30_day = _create_n_days_apr_fig(
        df=df,
        n_days=30,
        apply_periodic_fees=apply_periodic_fees,
        apply_streaming_fees=apply_streaming_fees,
        apply_rebalance_from_idle_swap_cost=apply_rebalance_from_idle_swap_cost,
        apply_rebalance_not_idle_swap_cost=apply_rebalance_not_idle_swap_cost,
        apply_nav_lost_to_depeg=apply_nav_lost_to_depeg,
    )

    nav_per_share_fig_7_days, apr_fig_7_day, bridge_fig_7_day = _create_n_days_apr_fig(
        df=df,
        n_days=7,
        apply_periodic_fees=apply_periodic_fees,
        apply_streaming_fees=apply_streaming_fees,
        apply_rebalance_from_idle_swap_cost=apply_rebalance_from_idle_swap_cost,
        apply_rebalance_not_idle_swap_cost=apply_rebalance_not_idle_swap_cost,
        apply_nav_lost_to_depeg=apply_nav_lost_to_depeg,
    )

    n_days = len(df) - 1

    all_time_nav_per_share_fig, all_time_apr_fig, all_time_bridge_fig = _create_n_days_apr_fig(
        df=df,
        n_days=n_days,
        apply_periodic_fees=apply_periodic_fees,
        apply_streaming_fees=apply_streaming_fees,
        apply_rebalance_from_idle_swap_cost=apply_rebalance_from_idle_swap_cost,
        apply_rebalance_not_idle_swap_cost=apply_rebalance_not_idle_swap_cost,
        apply_nav_lost_to_depeg=apply_nav_lost_to_depeg,
    )

    return (
        apr_fig_30_day,
        bridge_fig_30_day,
        apr_fig_7_day,
        bridge_fig_7_day,
        all_time_bridge_fig,
    )


# TODO cache these
def fetch_and_render_autopool_return_and_expenses_metrics(autopool: AutopoolConstants):
    df = fetch_nav_and_shares_and_factors_that_impact_nav_per_share(autopool)
    st.title("APR Before Fees, Costs and Depegs")
    apply_periodic_fees = st.checkbox("Remove Periodic Fees")
    apply_streaming_fees = st.checkbox("Remove Streaming Fees")
    apply_rebalance_from_idle_swap_cost = st.checkbox("Remove Rebalance From Idle Swap Cost")
    apply_rebalance_not_idle_swap_cost = st.checkbox("Remove Rebalance Not Idle Swap Cost")
    apply_nav_lost_to_depeg = st.checkbox("Add Back In Nav Lost To Depeg")

    (
        apr_fig_30_day,
        bridge_fig_30_day,
        apr_fig_7_day,
        bridge_fig_7_day,
        all_time_bridge_fig,
    ) = _create_figs(
        df,
        apply_periodic_fees,
        apply_streaming_fees,
        apply_rebalance_from_idle_swap_cost,
        apply_rebalance_not_idle_swap_cost,
        apply_nav_lost_to_depeg,
    )

    row1_cols = st.columns(2)

    with row1_cols[0]:
        st.plotly_chart(apr_fig_7_day, width=500, height=500)
    with row1_cols[1]:
        st.plotly_chart(bridge_fig_7_day, width=500, height=500)

    row2_cols = st.columns(2)

    with row2_cols[0]:
        st.plotly_chart(apr_fig_30_day, width=500, height=500)
    with row2_cols[1]:
        st.plotly_chart(bridge_fig_30_day, width=500, height=500)

    st.plotly_chart(all_time_bridge_fig, width=500, height=500)


if __name__ == "__main__":
    # to test run $ streamlit run mainnet_launch/autopool_diagnostics/returns_before_expenses.py

    from mainnet_launch.constants import AUTO_LRT

    fetch_and_render_autopool_return_and_expenses_metrics(AUTO_LRT)
