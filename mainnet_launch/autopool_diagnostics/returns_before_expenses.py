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
from mainnet_launch.destination_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats

from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.constants import AutopoolConstants, CACHE_TIME
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI

from mainnet_launch.solver_diagnostics.fetch_rebalance_events import (
    fetch_rebalance_events_actual_amounts,
)


@st.cache_data(ttl=CACHE_TIME)
def fetch_nav_and_shares_and_factors_that_impact_nav_per_share(autopool: AutopoolConstants) -> pd.DataFrame:
    daily_nav_shares_df = _fetch_actual_nav_per_share_by_day(autopool)
    cumulative_new_shares_df = _fetch_cumulative_fee_shares_minted_by_day(autopool)
    cumulative_rebalance_from_idle_swap_cost, cumulative_rebalance_not_from_idle_swap_cost = (
        _fetch_cumulative_nav_lost_to_rebalances(autopool)
    )
    implied_extra_nav_if_price_return_is_zero = _fetch_implied_extra_nav_if_price_return_is_zero(autopool)
    df = pd.concat(
        [
            daily_nav_shares_df,
            cumulative_new_shares_df,
            cumulative_rebalance_from_idle_swap_cost,
            cumulative_rebalance_not_from_idle_swap_cost,
            implied_extra_nav_if_price_return_is_zero,
        ],
        axis=1,
    )
    df.iloc[0] = df.iloc[0].fillna(0)
    df = df.ffill()
    df = df.resample("1D").last()  # just drop the tail

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
    blocks = build_blocks_to_use(autopool.chain)
    uwcr_df, allocation_df, compositeReturn_out_df, total_nav_series, summary_stats_df, priceReturn_df = (
        fetch_destination_summary_stats(blocks, autopool)
    )
    implied_extra_nav_if_price_return_is_zero = (allocation_df * priceReturn_df).sum(axis=1)
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


def _fetch_cumulative_nav_lost_to_rebalances(autopool: AutopoolConstants) -> pd.DataFrame:
    rebalance_df = fetch_rebalance_events_actual_amounts(autopool)

    rebalance_from_idle_df = rebalance_df[
        rebalance_df["outDestinationVault"].str.lower() == autopool.autopool_eth_addr.lower()
    ].copy()
    rebalance_not_from_idle_df = rebalance_df[
        ~(rebalance_df["outDestinationVault"].str.lower() == autopool.autopool_eth_addr.lower())
    ].copy()

    cumulative_rebalance_from_idle_swap_cost = rebalance_from_idle_df["swap_cost"].resample("1D").sum().cumsum()
    cumulative_rebalance_from_idle_swap_cost.name = "rebalance_from_idle_swap_cost"

    cumulative_rebalance_not_from_idle_swap_cost = rebalance_not_from_idle_df["swap_cost"].resample("1D").sum().cumsum()
    cumulative_rebalance_not_from_idle_swap_cost.name = "rebalance_not_idle_swap_cost"
    return cumulative_rebalance_from_idle_swap_cost, cumulative_rebalance_not_from_idle_swap_cost


def _fetch_cumulative_fee_shares_minted_by_day(autopool: AutopoolConstants) -> pd.DataFrame:
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
    daily_fee_share_df = fee_df.resample("1D").sum()
    cumulative_new_shares_df = daily_fee_share_df.cumsum()
    return cumulative_new_shares_df


def _compute_adjusted_nav_per_share(
    df: pd.DataFrame,
    apply_periodic_fees: bool,
    apply_streaming_fees: bool,
    apply_rebalance_from_idle_swap_cost: bool,
    apply_rebalance_not_idle_swap_cost: bool,
    apply_nav_lost_to_depeg: bool,
):
    adjusted_shares = df["actual_shares"].copy()
    adjusted_nav = df["actual_nav"].copy()

    # nav is really always 100,
    # we think at peg nav should be 105

    if apply_periodic_fees:
        adjusted_shares -= df["new_shares_from_periodic_fees"]
    if apply_streaming_fees:
        adjusted_shares -= df["new_shares_from_streaming_fees"]
    if apply_rebalance_from_idle_swap_cost:
        adjusted_nav += df["rebalance_from_idle_swap_cost"]
    if apply_rebalance_not_idle_swap_cost:
        adjusted_nav += df["rebalance_not_idle_swap_cost"]
    if apply_nav_lost_to_depeg:
        adjusted_nav += df["additional_nav_if_price_return_was_0"]  # +5

    df["adjusted_nav_per_share"] = adjusted_nav / adjusted_shares
    df["actual_nav_per_share"] = df["actual_nav"] / df["actual_shares"]

    return df[["adjusted_nav_per_share", "actual_nav_per_share"]]


def _create_n_days_apr_fig(df: pd.DataFrame, n_days: int, title: str):
    df[f"actual_{n_days}_days_annualized_apr"] = (
        100
        * (365 / n_days)
        * (
            (df["actual_nav_per_share"] - df["actual_nav_per_share"].shift(n_days))
            / df["actual_nav_per_share"].shift(n_days)
        )
    )
    df[f"adjusted_{n_days}_days_annualized_apr"] = (
        100
        * (365 / n_days)
        * (
            (df["adjusted_nav_per_share"] - df["adjusted_nav_per_share"].shift(n_days))
            / df["adjusted_nav_per_share"].shift(n_days)
        )
    )

    apr_fig = go.Figure()
    apr_fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df[f"actual_{n_days}_days_annualized_apr"],
            mode="lines+markers",
            name="Original APR",
        )
    )
    apr_fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df[f"adjusted_{n_days}_days_annualized_apr"],
            mode="lines+markers",
            name="Adjusted APR",
        )
    )
    apr_fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="APR",
        legend_title="Legend",
    )
    return apr_fig


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
        waterfallgap=0.3,  # Gap between bars
        showlegend=True,
        xaxis_title="APR Components",
        yaxis_title="Values",
    )

    return fig


def _create_figs(df: pd.DataFrame):
    apr_30_day_fig = _create_n_days_apr_fig(df, 30, "30 Day Annualized APR")
    apr_7_day_fig = _create_n_days_apr_fig(df, 7, "7 Day Annualized APR")
    n_days = len(df) - 1
    since_inception_fig = _create_n_days_apr_fig(df, n_days, f"Since Inception Annualized APR")

    last_row = df.tail(1).to_dict(orient="records")[0]

    bridge_lifetime = _make_bridge_figure(
        [
            last_row[f"actual_{n_days}_days_annualized_apr"],
            last_row[f"adjusted_{n_days}_days_annualized_apr"] - last_row[f"actual_{n_days}_days_annualized_apr"],
            last_row[f"adjusted_{n_days}_days_annualized_apr"],
        ],
        ["Acutal APR", "Diff", "Adjusted APR"],
        title="Since Inception Annualized APR",
    )

    bridge_30_days = _make_bridge_figure(
        [
            last_row["actual_30_days_annualized_apr"],
            last_row["adjusted_30_days_annualized_apr"] - last_row["actual_30_days_annualized_apr"],
            last_row["adjusted_30_days_annualized_apr"],
        ],
        ["Acutal APR", "Diff", "Adjusted APR"],
        title="30 Day Annualized APR",
    )

    bridge_7_days = _make_bridge_figure(
        [
            last_row["actual_7_days_annualized_apr"],
            last_row["adjusted_7_days_annualized_apr"] - last_row["actual_7_days_annualized_apr"],
            last_row["adjusted_7_days_annualized_apr"],
        ],
        ["Acutal APR", "Diff", "Adjusted APR"],
        title="7 Day Annualized APR",
    )

    nav_per_share_fig = go.Figure()
    nav_per_share_fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df["actual_nav_per_share"],
            mode="lines+markers",
            name="Original NAV Per Share",
        )
    )
    nav_per_share_fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df["adjusted_nav_per_share"],
            mode="lines+markers",
            name="Adjusted NAV Per Share",
        )
    )
    nav_per_share_fig.update_layout(
        title="NAV Per Share Over Time",
        xaxis_title="Date",
        yaxis_title="NAV Per Share",
        legend_title="Legend",
    )
    return apr_30_day_fig, apr_7_day_fig, nav_per_share_fig, bridge_lifetime, bridge_30_days, bridge_7_days


def fetch_autopool_return_and_expenses_metrics(autopool: AutopoolConstants):
    df = fetch_nav_and_shares_and_factors_that_impact_nav_per_share(autopool)


def fetch_and_render_autopool_return_and_expenses_metrics(autopool: AutopoolConstants):
    df = fetch_nav_and_shares_and_factors_that_impact_nav_per_share(autopool)
    st.title("APR Before Fees, Costs and Depegs")
    apply_periodic_fees = st.checkbox("Remove Periodic Fees")
    apply_streaming_fees = st.checkbox("Remove Streaming Fees")
    apply_rebalance_from_idle_swap_cost = st.checkbox("Remove Rebalance From Idle Swap Cost")
    apply_rebalance_not_idle_swap_cost = st.checkbox("Remove Rebalance Not Idle Swap Cost")
    apply_nav_lost_to_depeg = st.checkbox("Add Back In Nav Lost To Depeg")

    df = _compute_adjusted_nav_per_share(
        df,
        apply_periodic_fees,
        apply_streaming_fees,
        apply_rebalance_from_idle_swap_cost,
        apply_rebalance_not_idle_swap_cost,
        apply_nav_lost_to_depeg,
    )

    apr_30_day_fig, apr_7_day_fig, nav_per_share_fig, bridge_lifetime, bridge_30_days, bridge_7_days = _create_figs(df)

    plot_height = 300

    row1_cols = st.columns(2)
    with row1_cols[0]:
        st.plotly_chart(apr_30_day_fig, use_container_width=True, height=plot_height)
    with row1_cols[1]:
        st.plotly_chart(bridge_30_days, use_container_width=True, height=plot_height)

    # Row 2
    row2_cols = st.columns(2)
    with row2_cols[0]:
        st.plotly_chart(apr_7_day_fig, use_container_width=True, height=plot_height)
    with row2_cols[1]:
        st.plotly_chart(bridge_7_days, use_container_width=True, height=plot_height)

    # Row 3
    row3_cols = st.columns(2)
    with row3_cols[0]:
        st.plotly_chart(bridge_lifetime, use_container_width=True, height=plot_height)
    with row3_cols[1]:
        st.plotly_chart(nav_per_share_fig, use_container_width=True, height=plot_height)


if __name__ == "__main__":
    # to test run streamlit run mainnet_launch/autopool_diagnostics/returns_before_expenses.py
    from mainnet_launch.constants import AUTO_LRT, BASE_ETH

    fetch_and_render_autopool_return_and_expenses_metrics(AUTO_LRT)


# """Returns of the autopool before and after expenses and fees"""

# import pandas as pd
# import streamlit as st
# from datetime import timedelta, datetime, timezone
# from multicall import Call
# import plotly.express as px
# import plotly.graph_objects as go
# import json
# import numpy as np

# from mainnet_launch.data_fetching.get_state_by_block import (
#     get_raw_state_by_blocks,
#     get_state_by_one_block,
#     identity_with_bool_success,
#     safe_normalize_with_bool_success,
#     build_blocks_to_use,
# )
# from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
# from mainnet_launch.top_level.key_metrics import fetch_key_metrics_data
# from mainnet_launch.data_fetching.get_events import fetch_events
# from mainnet_launch.constants import AUTO_ETH, AUTO_LRT, BAL_ETH, AutopoolConstants, CACHE_TIME, BASE_ETH
# from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI, AUTOPOOL_ETH_STRATEGY_ABI
# from mainnet_launch.solver_diagnostics.fetch_rebalance_events import (
#     fetch_rebalance_events_df,
#     fetch_rebalance_events_actual_amounts,
# )

# # I don't relaly like the name here
# def fetch_nav_and_shares_and_factors_that_impact_nav_per_share(autopool: AutopoolConstants) -> pd.DataFrame:
#     daily_nav_shares_df = _fetch_actual_nav_per_share_by_day(autopool)
#     cumulative_new_shares_df = _fetch_cumulative_fee_shares_minted_by_day(autopool)
#     cumulative_rebalance_from_idle_swap_cost, cumulative_rebalance_not_from_idle_swap_cost = (
#         _fetch_cumulative_nav_lost_to_rebalances(autopool)
#     )
#     df = pd.concat(
#         [
#             daily_nav_shares_df,
#             cumulative_new_shares_df,
#             cumulative_rebalance_from_idle_swap_cost,
#             cumulative_rebalance_not_from_idle_swap_cost,
#         ],
#         axis=1,
#     )
#     df.iloc[0] = df.iloc[0].fillna(0)
#     df = df.ffill()
#     return df


# def _fetch_actual_nav_per_share_by_day(autopool: AutopoolConstants) -> pd.DataFrame:

#     def handle_getAssetBreakdown(success, AssetBreakdown):
#         if success:
#             # not correct variable names
#             totalIdle, totalDebt, totalDebtMin, totalDebtMin = AssetBreakdown
#             return int(totalIdle + totalDebt) / 1e18
#         return None

#     calls = [
#         Call(
#             autopool.autopool_eth_addr,
#             ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
#             [("actual_nav", handle_getAssetBreakdown)],
#         ),
#         Call(
#             autopool.autopool_eth_addr,
#             ["totalSupply()(uint256)"],
#             [("actual_shares", safe_normalize_with_bool_success)],
#         ),
#     ]

#     blocks = build_blocks_to_use(autopool.chain)
#     df = get_raw_state_by_blocks(calls, blocks, autopool.chain)
#     df["actual_nav_per_share"] = df["actual_nav"] / df["actual_shares"]
#     daily_nav_shares_df = df.resample("1D").last()
#     return daily_nav_shares_df


# def _fetch_cumulative_nav_lost_to_rebalances(autopool: AutopoolConstants):
#     rebalance_df = fetch_rebalance_events_actual_amounts(autopool)

#     rebalance_from_idle_df = rebalance_df[
#         rebalance_df["outDestinationVault"].str.lower() == autopool.autopool_eth_addr.lower()
#     ].copy()
#     rebalance_not_from_idle_df = rebalance_df[
#         ~(rebalance_df["outDestinationVault"].str.lower() == autopool.autopool_eth_addr.lower())
#     ].copy()

#     cumulative_rebalance_from_idle_swap_cost = rebalance_from_idle_df["swap_cost"].resample("1D").sum().cumsum()
#     cumulative_rebalance_from_idle_swap_cost.name = "rebalance_from_idle_swap_cost"

#     cumulative_rebalance_not_from_idle_swap_cost = rebalance_not_from_idle_df["swap_cost"].resample("1D").sum().cumsum()
#     cumulative_rebalance_not_from_idle_swap_cost.name = "rebalance_not_idle_swap_cost"
#     return cumulative_rebalance_from_idle_swap_cost, cumulative_rebalance_not_from_idle_swap_cost


# def _fetch_cumulative_fee_shares_minted_by_day(autopool: AutopoolConstants) -> pd.DataFrame:
#     vault_contract = autopool.chain.client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
#     FeeCollected_df = add_timestamp_to_df_with_block_column(
#         fetch_events(vault_contract.events.FeeCollected), autopool.chain
#     )
#     PeriodicFeeCollected_df = add_timestamp_to_df_with_block_column(
#         fetch_events(vault_contract.events.PeriodicFeeCollected), autopool.chain
#     )
#     PeriodicFeeCollected_df["new_shares_from_periodic_fees"] = PeriodicFeeCollected_df["mintedShares"] / 1e18
#     FeeCollected_df["new_shares_from_streaming_fees"] = FeeCollected_df["mintedShares"] / 1e18
#     fee_df = pd.concat(
#         [
#             PeriodicFeeCollected_df[["new_shares_from_periodic_fees"]],
#             FeeCollected_df[["new_shares_from_streaming_fees"]],
#         ]
#     )
#     daily_fee_share_df = fee_df.resample("1D").sum()
#     cumulative_new_shares_df = daily_fee_share_df.cumsum()
#     return cumulative_new_shares_df


# #
# # def _compute_30_day_and_lifetime_annualized_return(autopool_return_and_expenses_df: pd.DataFrame, col: str):

# #     current_value = autopool_return_and_expenses_df.iloc[-1][col]

# #     value_30_days_ago = autopool_return_and_expenses_df.iloc[-31][col]  # this gets the value from 30 days ago
# #     value_7_days_ago = autopool_return_and_expenses_df.iloc[-8][col]  # this gets the value from 7 days ago
# #     today = datetime.now(timezone.utc)
# #     recent_year_df = autopool_return_and_expenses_df[
# #         autopool_return_and_expenses_df.index >= today - timedelta(days=365)
# #     ].copy()

# #     thirty_day_annualized_return = (100 * (current_value - value_30_days_ago) / value_30_days_ago) * (365 / 30)
# #     seven_day_annualized_return = (100 * (current_value - value_7_days_ago) / value_7_days_ago) * (365 / 7)

# #     num_days = len(recent_year_df)
# #     initial_value = recent_year_df.iloc[0][col]
# #     lifetime_annualized_return = (100 * (current_value - initial_value) / initial_value) * (365 / num_days)

# #     return thirty_day_annualized_return, lifetime_annualized_return, seven_day_annualized_return


# # def _compute_returns(autopool_return_and_expenses_df: pd.DataFrame) -> dict:
# #     return_metrics = {}
# #     for col in [
# #         "actual_nav_per_share",
# #         "nav_per_share_if_no_fees",
# #         "nav_per_share_if_no_value_lost_from_rebalances",
# #         "nav_per_share_if_no_depegs",
# #         "nav_per_share_if_no_value_lost_from_rebalancesIdle",
# #         "nav_per_share_if_no_value_lost_from_rebalancesChurn",
# #     ]:
# #         thirty_day_annualized_return, lifetime_annualized_return, seven_day_annualized_return = (
# #             _compute_30_day_and_lifetime_annualized_return(autopool_return_and_expenses_df, col)
# #         )
# #         return_metrics[f"{col} 30days"] = thirty_day_annualized_return
# #         return_metrics[f"{col} lifetime"] = lifetime_annualized_return
# #         return_metrics[f"{col} 7days"] = seven_day_annualized_return

# #     return_metrics["7_day_return_lost_to_rebalance_costs"] = (
# #         return_metrics["nav_per_share_if_no_value_lost_from_rebalances 7days"]
# #         - return_metrics["actual_nav_per_share 7days"]
# #     )

# #     return_metrics["7_day_return_lost_to_rebalance_costsIdle"] = (
# #         return_metrics["nav_per_share_if_no_value_lost_from_rebalancesIdle 7days"]
# #         - return_metrics["actual_nav_per_share 7days"]
# #     )

# #     return_metrics["7_day_return_lost_to_rebalance_costsChurn"] = (
# #         return_metrics["nav_per_share_if_no_value_lost_from_rebalancesChurn 7days"]
# #         - return_metrics["actual_nav_per_share 7days"]
# #     )
# #     return_metrics["7_day_return_lost_to_fees"] = (
# #         return_metrics["nav_per_share_if_no_fees 7days"] - return_metrics["actual_nav_per_share 7days"]
# #     )

# #     return_metrics["7_day_return_lost_to_depegs"] = (
# #         return_metrics["nav_per_share_if_no_depegs 7days"] - return_metrics["actual_nav_per_share 7days"]
# #     )

# #     return_metrics["7_day_return_if_no_fees_or_rebalance_costs"] = (
# #         return_metrics["actual_nav_per_share 7days"]
# #         + return_metrics["7_day_return_lost_to_rebalance_costsIdle"]
# #         + return_metrics["7_day_return_lost_to_rebalance_costsChurn"]
# #         + return_metrics["7_day_return_lost_to_fees"]
# #     )

# #     return_metrics["7_day_return_if_no_fees_or_rebalance_costs_depegs"] = (
# #         return_metrics["actual_nav_per_share 7days"]
# #         + return_metrics["7_day_return_lost_to_rebalance_costsIdle"]
# #         + return_metrics["7_day_return_lost_to_rebalance_costsChurn"]
# #         + return_metrics["7_day_return_lost_to_fees"]
# #         + return_metrics["7_day_return_lost_to_depegs"]
# #     )

# #     return_metrics["30_day_return_lost_to_rebalance_costs"] = (
# #         return_metrics["nav_per_share_if_no_value_lost_from_rebalances 30days"]
# #         - return_metrics["actual_nav_per_share 30days"]
# #     )

# #     return_metrics["30_day_return_lost_to_rebalance_costsIdle"] = (
# #         return_metrics["nav_per_share_if_no_value_lost_from_rebalancesIdle 30days"]
# #         - return_metrics["actual_nav_per_share 30days"]
# #     )

# #     return_metrics["30_day_return_lost_to_rebalance_costsChurn"] = (
# #         return_metrics["nav_per_share_if_no_value_lost_from_rebalancesChurn 30days"]
# #         - return_metrics["actual_nav_per_share 30days"]
# #     )
# #     return_metrics["30_day_return_lost_to_fees"] = (
# #         return_metrics["nav_per_share_if_no_fees 30days"] - return_metrics["actual_nav_per_share 30days"]
# #     )

# #     return_metrics["30_day_return_lost_to_depegs"] = (
# #         return_metrics["nav_per_share_if_no_depegs 30days"] - return_metrics["actual_nav_per_share 30days"]
# #     )

# #     return_metrics["30_day_return_if_no_fees_or_rebalance_costs"] = (
# #         return_metrics["actual_nav_per_share 30days"]
# #         + return_metrics["30_day_return_lost_to_rebalance_costsIdle"]
# #         + return_metrics["30_day_return_lost_to_rebalance_costsChurn"]
# #         + return_metrics["30_day_return_lost_to_fees"]
# #     )

# #     return_metrics["30_day_return_if_no_fees_or_rebalance_costs_depegs"] = (
# #         return_metrics["actual_nav_per_share 30days"]
# #         + return_metrics["30_day_return_lost_to_rebalance_costsIdle"]
# #         + return_metrics["30_day_return_lost_to_rebalance_costsChurn"]
# #         + return_metrics["30_day_return_lost_to_fees"]
# #         + return_metrics["30_day_return_lost_to_depegs"]
# #     )

# #     return_metrics["lifetime_return_lost_to_rebalance_costs"] = (
# #         return_metrics["nav_per_share_if_no_value_lost_from_rebalances lifetime"]
# #         - return_metrics["actual_nav_per_share lifetime"]
# #     )

# #     return_metrics["lifetime_return_lost_to_rebalance_costsIdle"] = (
# #         return_metrics["nav_per_share_if_no_value_lost_from_rebalancesIdle lifetime"]
# #         - return_metrics["actual_nav_per_share lifetime"]
# #     )

# #     return_metrics["lifetime_return_lost_to_rebalance_costsChurn"] = (
# #         return_metrics["nav_per_share_if_no_value_lost_from_rebalancesChurn lifetime"]
# #         - return_metrics["actual_nav_per_share lifetime"]
# #     )

# #     return_metrics["lifetime_return_lost_to_fees"] = (
# #         return_metrics["nav_per_share_if_no_fees lifetime"] - return_metrics["actual_nav_per_share lifetime"]
# #     )

# #     return_metrics["lifetime_return_lost_to_depegs"] = (
# #         return_metrics["nav_per_share_if_no_depegs lifetime"] - return_metrics["actual_nav_per_share lifetime"]
# #     )

# #     return_metrics["lifetime_return_if_no_fees_or_rebalance_costs"] = (
# #         return_metrics["actual_nav_per_share lifetime"]
# #         + return_metrics["lifetime_return_lost_to_rebalance_costsIdle"]
# #         + return_metrics["lifetime_return_lost_to_rebalance_costsChurn"]
# #         + return_metrics["lifetime_return_lost_to_fees"]
# #     )

# #     return_metrics["lifetime_return_if_no_fees_or_rebalance_costs_depegs"] = (
# #         return_metrics["actual_nav_per_share lifetime"]
# #         + return_metrics["lifetime_return_lost_to_rebalance_costsIdle"]
# #         + return_metrics["lifetime_return_lost_to_rebalance_costsChurn"]
# #         + return_metrics["lifetime_return_lost_to_fees"]
# #         + return_metrics["lifetime_return_lost_to_depegs"]
# #     )

# #     for k, v in return_metrics.items():
# #         return_metrics[k] = round(float(v), 4)

# #     return return_metrics


# # @st.cache_data(ttl=CACHE_TIME)
# # def fetch_autopool_return_and_expenses_metrics(autopool: AutopoolConstants) -> dict[str, float]:

# #     cumulative_shares_minted_df = _fetch_cumulative_fee_shares_minted_by_day(autopool)
# #     daily_nav_and_shares_df = _fetch_actual_nav_per_share_by_day(autopool)
# #     key_metrics_data = fetch_key_metrics_data(autopool)
# #     total_nav_series = key_metrics_data["allocation_df"].sum(axis=1)
# #     portion_df = key_metrics_data["allocation_df"].div(total_nav_series, axis=0)
# #     wpReturn = (key_metrics_data["priceReturn_df"].fillna(0) * portion_df.fillna(0)).sum(axis=1)
# #     wpReturn = wpReturn.resample("1D").last()
# #     wpReturn = wpReturn.rename("wpr")
# #     autopool_return_and_expenses_df = daily_nav_and_shares_df.join(wpReturn, how="left")
# #     autopool_return_and_expenses_df = autopool_return_and_expenses_df.join(cumulative_shares_minted_df, how="left")

# #     autopool_return_and_expenses_df[["new_shares_from_periodic_fees", "new_shares_from_streaming_fees"]] = (
# #         autopool_return_and_expenses_df[["new_shares_from_periodic_fees", "new_shares_from_streaming_fees"]].ffill()
# #     )
# #     # if there were no shares minted on a day the cumulative number of new shares minted has not changed
# #     # rebalance_df = fetch_and_clean_rebalance_between_destination_events(autopool)
# #     # cumulative_nav_lost_to_rebalances = (rebalance_df[["swapCost"]].resample("1D").sum()).cumsum()
# #     # cumulative_nav_lost_to_rebalancesChurn = (rebalance_df[["swapCostChurn"]].resample("1D").sum()).cumsum()
# #     # cumulative_nav_lost_to_rebalancesIdle = (rebalance_df[["swapCostIdle"]].resample("1D").sum()).cumsum()
# #     # cumulative_nav_lost_to_rebalances.columns = ["eth_nav_lost_by_rebalance_between_destinations"]
# #     # cumulative_nav_lost_to_rebalances["swapCostETHIdle"] = cumulative_nav_lost_to_rebalancesIdle
# #     # cumulative_nav_lost_to_rebalances["swapCostETHChurn"] = cumulative_nav_lost_to_rebalancesChurn

# #     rebalance_df = fetch_rebalance_events_df(autopool)
# #     cumulative_nav_lost_to_rebalances = (rebalance_df[["swapCost"]].resample("1D").sum()).cumsum()
# #     cumulative_nav_lost_to_rebalances.columns = ["eth_nav_lost_by_rebalance_between_destinations"]

# #     swap_cost_rebalances_from_idle = (
# #         rebalance_df[rebalance_df["out_destination"] == autopool.autopool_eth_addr]
# #         .resample("1D")[["swapCost"]]
# #         .sum()
# #         .cumsum()
# #     )

# #     swap_cost_rebalances_from_idle.columns = ["swapCostETHIdle"]

# #     swap_cost_rebalances_churn = (
# #         rebalance_df[rebalance_df["out_destination"] != autopool.autopool_eth_addr]
# #         .resample("1D")[["swapCost"]]
# #         .sum()
# #         .cumsum()
# #     )

# #     swap_cost_rebalances_churn.columns = ["swapCostETHChurn"]

# #     autopool_return_and_expenses_df = autopool_return_and_expenses_df.join(
# #         cumulative_nav_lost_to_rebalances, how="left"
# #     )

# #     autopool_return_and_expenses_df = autopool_return_and_expenses_df.join(swap_cost_rebalances_from_idle, how="left")

# #     autopool_return_and_expenses_df = autopool_return_and_expenses_df.join(swap_cost_rebalances_churn, how="left")

# #     # if there are no rebalances on the current day then the cumulative eth lost has not changed so we can ffill
# #     autopool_return_and_expenses_df[
# #         ["eth_nav_lost_by_rebalance_between_destinations", "swapCostETHChurn", "swapCostETHIdle"]
# #     ] = autopool_return_and_expenses_df[
# #         ["eth_nav_lost_by_rebalance_between_destinations", "swapCostETHChurn", "swapCostETHIdle"]
# #     ].ffill()

# #     # at the start there can be np.Nan streaming fees, periodic_fees or eth lost to rebalances
# #     # this is because for the first few days, there were no fees or rebalances. So we can safely
# #     # replace them with 0
# #     autopool_return_and_expenses_df[
# #         [
# #             "new_shares_from_periodic_fees",
# #             "new_shares_from_streaming_fees",
# #             "eth_nav_lost_by_rebalance_between_destinations",
# #             "swapCostETHIdle",
# #             "swapCostETHChurn",
# #         ]
# #     ] = autopool_return_and_expenses_df[
# #         [
# #             "new_shares_from_periodic_fees",
# #             "new_shares_from_streaming_fees",
# #             "eth_nav_lost_by_rebalance_between_destinations",
# #             "swapCostETHIdle",
# #             "swapCostETHChurn",
# #         ]
# #     ].fillna(
# #         0
# #     )

# #     autopool_return_and_expenses_df["nav_per_share_if_no_fees"] = autopool_return_and_expenses_df["actual_nav"] / (
# #         autopool_return_and_expenses_df["actual_shares"]
# #         - autopool_return_and_expenses_df["new_shares_from_periodic_fees"]
# #         - autopool_return_and_expenses_df["new_shares_from_streaming_fees"]
# #     )

# #     autopool_return_and_expenses_df["nav_per_share_if_no_value_lost_from_rebalances"] = (
# #         autopool_return_and_expenses_df["actual_nav"]
# #         + autopool_return_and_expenses_df["eth_nav_lost_by_rebalance_between_destinations"]
# #     ) / autopool_return_and_expenses_df["actual_shares"]

# #     autopool_return_and_expenses_df["nav_per_share_if_no_value_lost_from_rebalancesIdle"] = (
# #         autopool_return_and_expenses_df["actual_nav"] + autopool_return_and_expenses_df["swapCostETHIdle"]
# #     ) / autopool_return_and_expenses_df["actual_shares"]

# #     autopool_return_and_expenses_df["nav_per_share_if_no_value_lost_from_rebalancesChurn"] = (
# #         autopool_return_and_expenses_df["actual_nav"] + autopool_return_and_expenses_df["swapCostETHChurn"]
# #     ) / autopool_return_and_expenses_df["actual_shares"]

# #     autopool_return_and_expenses_df["shares_if_no_fees_minted"] = autopool_return_and_expenses_df["actual_shares"] - (
# #         autopool_return_and_expenses_df["new_shares_from_periodic_fees"]
# #         + autopool_return_and_expenses_df["new_shares_from_streaming_fees"]
# #     )
# #     autopool_return_and_expenses_df["nav_if_no_losses_from_rebalances"] = (
# #         autopool_return_and_expenses_df["actual_nav"]
# #         + autopool_return_and_expenses_df["eth_nav_lost_by_rebalance_between_destinations"]
# #     )
# #     autopool_return_and_expenses_df["nav_per_share_if_no_fees_or_rebalances"] = (
# #         autopool_return_and_expenses_df["nav_if_no_losses_from_rebalances"]
# #         / autopool_return_and_expenses_df["shares_if_no_fees_minted"]
# #     )

# #     autopool_return_and_expenses_df["nav_per_share_if_no_depegs"] = autopool_return_and_expenses_df[
# #         "actual_nav_per_share"
# #     ] * (1 + autopool_return_and_expenses_df["wpr"])

# #     returns_metrics = _compute_returns(autopool_return_and_expenses_df)
# #     return returns_metrics, autopool_return_and_expenses_df


# # def fetch_and_render_autopool_return_and_expenses_metrics(autopool: AutopoolConstants):
# #     returns_metrics, autopool_return_and_expenses_df = fetch_autopool_return_and_expenses_metrics(autopool)

# #     bridge_fig_7_days_apr = _make_bridge_plot(
# #         [
# #             returns_metrics["actual_nav_per_share 7days"],
# #             +returns_metrics["7_day_return_lost_to_fees"],
# #             +returns_metrics["7_day_return_lost_to_rebalance_costsIdle"],
# #             +returns_metrics["7_day_return_lost_to_rebalance_costsChurn"],
# #             +returns_metrics["7_day_return_lost_to_depegs"],
# #             returns_metrics["7_day_return_if_no_fees_or_rebalance_costs_depegs"],
# #         ],
# #         names=[
# #             "Net Return",
# #             "Return Lost to Fees",
# #             "Return Lost to Rebalance Idle",
# #             "Return Lost to Rebalance dest2dest",
# #             "depeg loss",
# #             "Gross Return",
# #         ],
# #         title="Annualized 7-Day Returns",
# #     )

# #     bridge_fig_30_days_apr = _make_bridge_plot(
# #         [
# #             returns_metrics["actual_nav_per_share 30days"],
# #             +returns_metrics["30_day_return_lost_to_fees"],
# #             +returns_metrics["30_day_return_lost_to_rebalance_costsIdle"],
# #             +returns_metrics["30_day_return_lost_to_rebalance_costsChurn"],
# #             +returns_metrics["30_day_return_lost_to_depegs"],
# #             returns_metrics["30_day_return_if_no_fees_or_rebalance_costs_depegs"],
# #         ],
# #         names=[
# #             "Net Return",
# #             "Return Lost to Fees",
# #             "Return Lost to Rebalance Idle",
# #             "Return Lost to Rebalance dest2dest",
# #             "depeg loss",
# #             "Gross Return",
# #         ],
# #         title="Annualized 30-Day Returns",
# #     )

# #     bridge_fig_year_to_date = _make_bridge_plot(
# #         [
# #             returns_metrics["actual_nav_per_share lifetime"],
# #             +returns_metrics["lifetime_return_lost_to_fees"],
# #             +returns_metrics["lifetime_return_lost_to_rebalance_costsIdle"],
# #             +returns_metrics["lifetime_return_lost_to_rebalance_costsChurn"],
# #             +returns_metrics["30_day_return_lost_to_depegs"],
# #             returns_metrics["lifetime_return_if_no_fees_or_rebalance_costs"],
# #         ],
# #         names=[
# #             "Net Return",
# #             "Return Lost to Fees",
# #             "Return Lost to Rebalance Idle",
# #             "Return Lost to Rebalance dest2dest",
# #             "depeg loss",
# #             "Gross Return",
# #         ],
# #         title="Annualized Year-to-Date Returns",
# #     )

# #     autopool_return_and_expenses_df["30_day_annualized_gross_return"] = (
# #         (
# #             autopool_return_and_expenses_df["nav_per_share_if_no_fees_or_rebalances"].diff(30)
# #             / autopool_return_and_expenses_df["nav_per_share_if_no_fees_or_rebalances"].shift(30)
# #         )
# #         * (365 / 30)
# #         * 100
# #     )

# #     autopool_return_and_expenses_df["30_day_annualized_return_if_no_loss_from_rebalances"] = (
# #         (
# #             autopool_return_and_expenses_df["nav_per_share_if_no_value_lost_from_rebalances"].diff(30)
# #             / autopool_return_and_expenses_df["nav_per_share_if_no_value_lost_from_rebalances"].shift(30)
# #         )
# #         * (365 / 30)
# #         * 100
# #     )

# #     autopool_return_and_expenses_df["30_day_annualized_return_if_no_fees"] = (
# #         (
# #             autopool_return_and_expenses_df["nav_per_share_if_no_fees"].diff(30)
# #             / autopool_return_and_expenses_df["nav_per_share_if_no_fees"].shift(30)
# #         )
# #         * (365 / 30)
# #         * 100
# #     )

# #     autopool_return_and_expenses_df["30_day_annualized_net_return"] = (
# #         (
# #             autopool_return_and_expenses_df["actual_nav_per_share"].diff(30)
# #             / autopool_return_and_expenses_df["actual_nav_per_share"].shift(30)
# #         )
# #         * (365 / 30)
# #         * 100
# #     )

# #     line_plot_of_apr_over_time = px.line(
# #         autopool_return_and_expenses_df[
# #             [
# #                 "30_day_annualized_gross_return",
# #                 "30_day_annualized_return_if_no_loss_from_rebalances",
# #                 "30_day_annualized_return_if_no_fees",
# #                 "30_day_annualized_net_return",
# #             ]
# #         ],
# #         title="Autopool Gross and Net Return",
# #     )

# #     st.plotly_chart(bridge_fig_7_days_apr, use_container_width=True)
# #     st.plotly_chart(bridge_fig_30_days_apr, use_container_width=True)
# #     st.plotly_chart(bridge_fig_year_to_date, use_container_width=True)
# #     st.plotly_chart(line_plot_of_apr_over_time, use_container_width=True)

# #     with st.expander("See explanation of Autopool Gross and Net Return"):
# #         st.write(
# #             """
# #             Depositors in the Autopool experience two main costs that reduce NAV per share

# #             #### 1. Tokemak Protocol-Level Fees
# #             Autopool shares are minted to Tokemak as a periodic and streaming fees. To account for this, we track the total shares minted to Tokemak since deployment.

# #             By subtracting this amount from the total supply of shares, we get the **"total supply of shares if no fees"**. Using this adjusted supply, we calculate the NA per share as if Tokemak had not charged fees:
# #             """
# #         )
# #         st.latex(
# #             r"\text{NAV per share (no fees)} = \frac{\text{NAV}}{\text{total supply of shares} - \text{shares minted for fees}}"
# #         )

# #         st.write(
# #             """
# #             #### 2. ETH Value Lost Due to Rebalances
# #             During rebalances ETH value is lost to slippage, swap costs and (later) a solver profit margin. The difference in ETH value from these changes is the **value lost to rebalances**.

# #             By tracking this ETH loss since deployment and adding it back to NAV, we calculate the NAV per share as if no value was lost due to rebalances:
# #             """
# #         )
# #         st.latex(
# #             r"\text{NAV per share (no rebalance loss)} = \frac{\text{NAV} + \text{ETH lost to rebalances}}{\text{total supply of shares}}"
# #         )

# #         st.write(
# #             """
# #             #### Gross Return
# #             Gross Return is the return as if there were no fees or rebalancing costs:
# #             """
# #         )
# #         st.latex(
# #             r"\text{Gross Return} = \frac{\text{NAV} + \text{ETH lost to rebalances}}{\text{total supply of shares} - \text{shares minted for fees}}"
# #         )

# #         st.write(
# #             """
# #             #### Net Return
# #             Net Return represents the annualized rate of change in NAV per share. This is the actual base return experienced by depositors:
# #             """
# #         )
# #         st.latex(r"\text{Net Return} = \text{annualized change in NAV per share}")


# if __name__ == "__main__":
#     pass

#     # import json

#     # returns_metrics, autopool_return_and_expenses_df = fetch_autopool_return_and_expenses_metrics(AUTO_LRT)
#     # autopool_return_and_expenses_df.to_csv("AUTO_LRT_returns_before_expenses.csv")
#     # with open("AUTOLRT_return_metrics.json", "w") as fout:
#     #     json.dump(returns_metrics, fout)

#     # returns_metrics, autopool_return_and_expenses_df = fetch_autopool_return_and_expenses_metrics(AUTO_ETH)
#     # autopool_return_and_expenses_df.to_csv("AUTO_ETH_returns_before_expenses.csv")
#     # with open("AUTO_ETH_return_metrics.json", "w") as fout:
#     #     json.dump(returns_metrics, fout)

#     # returns_metrics, autopool_return_and_expenses_df = fetch_autopool_return_and_expenses_metrics(BASE_ETH)
#     # autopool_return_and_expenses_df.to_csv("BASE_ETH_returns_before_expenses.csv")
#     # with open("BASE_ETH_return_metrics.json", "w") as fout:
#     #     json.dump(returns_metrics, fout)
