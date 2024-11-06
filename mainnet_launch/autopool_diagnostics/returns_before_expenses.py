"""Returns of the autopool before and after expenses and fees"""

import pandas as pd
import streamlit as st
from datetime import timedelta, datetime, timezone
from multicall import Call
import plotly.express as px
import plotly.graph_objects as go
import json
import numpy as np

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    get_state_by_one_block,
    identity_with_bool_success,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
    eth_client,
)
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
from mainnet_launch.top_level.key_metrics import fetch_key_metrics_data
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.constants import AUTO_ETH, AUTO_LRT, BAL_ETH, AutopoolConstants, CACHE_TIME
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI, AUTOPOOL_ETH_STRATEGY_ABI
from mainnet_launch.solver_diagnostics.fetch_rebalance_events import (
    fetch_and_clean_rebalance_between_destination_events,
)
from mainnet_launch.autopool_diagnostics.nav_if_no_discount import fetch_destination_totalEthValueHeldIfNoDiscount


def handle_getAssetBreakdown(success, AssetBreakdown):
    if success:
        totalIdle, totalDebt, totalDebtMin, totalDebtMin = AssetBreakdown
        return int(totalIdle + totalDebt) / 1e18
    return None


def getAssetBreakdown_call(name: str, autopool_vault_address: str) -> Call:
    return Call(
        autopool_vault_address,
        ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
        [(name, handle_getAssetBreakdown)],
    )


def totalSupply_call(name: str, autopool_vault_address: str) -> Call:
    return Call(
        autopool_vault_address,
        ["totalSupply()(uint256)"],
        [(name, safe_normalize_with_bool_success)],
    )


def _fetch_actual_nav_per_share_by_day(autopool: AutopoolConstants) -> pd.DataFrame:
    calls = [
        getAssetBreakdown_call("actual_nav", autopool.autopool_eth_addr),
        totalSupply_call("actual_shares", autopool.autopool_eth_addr),
    ]
    blocks = build_blocks_to_use()
    df = get_raw_state_by_blocks(calls, blocks)
    df["actual_nav_per_share"] = df["actual_nav"] / df["actual_shares"]
    daily_nav_shares_df = df.resample("1D").last()
    return daily_nav_shares_df


def _fetch_cumulative_fee_shares_minted_by_day(autopool: AutopoolConstants) -> pd.DataFrame:
    autoETH_vault = eth_client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
    FeeCollected_df = add_timestamp_to_df_with_block_column(fetch_events(autoETH_vault.events.FeeCollected))
    PeriodicFeeCollected_df = add_timestamp_to_df_with_block_column(
        fetch_events(autoETH_vault.events.PeriodicFeeCollected)
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


def _compute_returns(autopool_return_and_expenses_df: pd.DataFrame) -> dict:

    thirty_days_return_df = _build_n_day_apr_df(autopool_return_and_expenses_df, n_days=30)

    lifetime_return_df = _build_n_day_apr_df(
        autopool_return_and_expenses_df, n_days=len(autopool_return_and_expenses_df) - 1
    )

    thirty_day_return_metrics = _compute_returns_lost_do_different_sources(
        thirty_days_return_df, n_days=30, prefix="30 days"
    )

    lifetime_return_metrics = _compute_returns_lost_do_different_sources(
        lifetime_return_df, n_days=len(autopool_return_and_expenses_df) - 1, prefix="lifetime"
    )

    return thirty_days_return_df, thirty_day_return_metrics, lifetime_return_metrics


def _build_n_day_apr_df(autopool_return_and_expenses_df: pd.DataFrame, n_days: int) -> pd.DataFrame:
    nav_per_share_cols = [
        "actual_nav_per_share",
        "nav_per_share_if_no_fees",
        "nav_per_share_if_no_value_lost_from_rebalances",
        "nav_per_share_if_no_value_lost_from_rebalancesIdle",
        "nav_per_share_if_no_value_lost_from_rebalancesChurn",
        "nav_per_share_if_no_discounts",
    ]

    n_days_return_df = (
        (
            autopool_return_and_expenses_df[nav_per_share_cols].diff(n_days)
            / autopool_return_and_expenses_df[nav_per_share_cols].shift(n_days)
        )
        * (365 / n_days)
        * 100
    )
    n_days_return_df.columns = [
        f"{n_days}_day_annualized_return_actual",
        f"{n_days}_day_annualized_return_if_no_fees",
        f"{n_days}_day_annualized_return_if_no_value_lost_from_rebalances",
        f"{n_days}_day_annualized_return_if_no_value_lost_from_rebalancesIdle",
        f"{n_days}_day_annualized_return_if_no_value_lost_from_rebalancesChurn",
        f"{n_days}_day_annualized_return_if_no_discounts",
    ]

    return n_days_return_df


def _compute_returns_lost_do_different_sources(n_days_return_df: pd.DataFrame, n_days: int, prefix: str) -> dict:
    return_metrics = {}

    last_day_of_returns = (
        n_days_return_df.tail(1).iloc[-1].to_dict()
    )  # the bridge plot only shows the most recent returns

    return_metrics[f"{prefix}_return_actual"] = last_day_of_returns[f"{n_days}_day_annualized_return_actual"]

    return_metrics[f"{prefix}_return_lost_to_rebalance_costsIdle"] = (
        last_day_of_returns[f"{n_days}_day_annualized_return_if_no_value_lost_from_rebalancesIdle"]
        - last_day_of_returns[f"{n_days}_day_annualized_return_actual"]
    )
    return_metrics[f"{prefix}_return_lost_to_rebalance_costsChurn"] = (
        last_day_of_returns[f"{n_days}_day_annualized_return_if_no_value_lost_from_rebalancesChurn"]
        - last_day_of_returns[f"{n_days}_day_annualized_return_actual"]
    )
    return_metrics[f"{prefix}_return_lost_to_fees"] = (
        last_day_of_returns[f"{n_days}_day_annualized_return_if_no_fees"]
        - last_day_of_returns[f"{n_days}_day_annualized_return_actual"]
    )

    return_metrics[f"{prefix}_return_lost_to_asset_discounts"] = (
        last_day_of_returns[f"{n_days}_day_annualized_return_if_no_discounts"]
        - last_day_of_returns[f"{n_days}_day_annualized_return_actual"]
    )

    return_metrics[f"{prefix} gross_return"] = (
        return_metrics[f"{prefix}_return_actual"]
        + return_metrics[f"{prefix}_return_lost_to_rebalance_costsIdle"]
        + return_metrics[f"{prefix}_return_lost_to_rebalance_costsChurn"]
        + return_metrics[f"{prefix}_return_lost_to_fees"]
        + return_metrics[f"{prefix}_return_lost_to_asset_discounts"]
    )

    return return_metrics


@st.cache_data(ttl=CACHE_TIME)
def fetch_autopool_return_and_expenses_metrics(autopool: AutopoolConstants):

    cumulative_shares_minted_df = _fetch_cumulative_fee_shares_minted_by_day(autopool)
    daily_nav_and_shares_df = _fetch_actual_nav_per_share_by_day(autopool)
    # key_metrics_data = fetch_key_metrics_data(autopool)

    autopool_return_and_expenses_df = daily_nav_and_shares_df.join(cumulative_shares_minted_df, how="left")

    autopool_return_and_expenses_df[["new_shares_from_periodic_fees", "new_shares_from_streaming_fees"]] = (
        autopool_return_and_expenses_df[["new_shares_from_periodic_fees", "new_shares_from_streaming_fees"]].ffill()
    )
    # if there were no shares minted on a day the cumulative number of new shares minted has not changed
    rebalance_df = fetch_and_clean_rebalance_between_destination_events(autopool)
    cumulative_nav_lost_to_rebalances = (rebalance_df[["swapCost"]].resample("1D").sum()).cumsum()
    cumulative_nav_lost_to_rebalancesChurn = (rebalance_df[["swapCostChurn"]].resample("1D").sum()).cumsum()
    cumulative_nav_lost_to_rebalancesIdle = (rebalance_df[["swapCostIdle"]].resample("1D").sum()).cumsum()
    cumulative_nav_lost_to_rebalances.columns = ["eth_nav_lost_by_rebalance_between_destinations"]
    cumulative_nav_lost_to_rebalances["swapCostETHIdle"] = cumulative_nav_lost_to_rebalancesIdle
    cumulative_nav_lost_to_rebalances["swapCostETHChurn"] = cumulative_nav_lost_to_rebalancesChurn

    autopool_return_and_expenses_df = autopool_return_and_expenses_df.join(
        cumulative_nav_lost_to_rebalances, how="left"
    )

    # if there are no rebalances on the current day then the cumulative eth lost has not changed so we can ffill
    autopool_return_and_expenses_df[
        ["eth_nav_lost_by_rebalance_between_destinations", "swapCostETHChurn", "swapCostETHIdle"]
    ] = autopool_return_and_expenses_df[
        ["eth_nav_lost_by_rebalance_between_destinations", "swapCostETHChurn", "swapCostETHIdle"]
    ].ffill()

    # at the start there can be np.Nan streaming fees, periodic_fees or eth lost to rebalances
    # this is because for the first few days, there were no fees or rebalances. So we can safely
    # replace them with 0
    autopool_return_and_expenses_df[
        [
            "new_shares_from_periodic_fees",
            "new_shares_from_streaming_fees",
            "eth_nav_lost_by_rebalance_between_destinations",
            "swapCostETHIdle",
            "swapCostETHChurn",
        ]
    ] = autopool_return_and_expenses_df[
        [
            "new_shares_from_periodic_fees",
            "new_shares_from_streaming_fees",
            "eth_nav_lost_by_rebalance_between_destinations",
            "swapCostETHIdle",
            "swapCostETHChurn",
        ]
    ].fillna(
        0
    )

    autopool_return_and_expenses_df["nav_per_share_if_no_fees"] = autopool_return_and_expenses_df["actual_nav"] / (
        autopool_return_and_expenses_df["actual_shares"]
        - autopool_return_and_expenses_df["new_shares_from_periodic_fees"]
        - autopool_return_and_expenses_df["new_shares_from_streaming_fees"]
    )

    autopool_return_and_expenses_df["nav_per_share_if_no_value_lost_from_rebalances"] = (
        autopool_return_and_expenses_df["actual_nav"]
        + autopool_return_and_expenses_df["eth_nav_lost_by_rebalance_between_destinations"]
    ) / autopool_return_and_expenses_df["actual_shares"]

    autopool_return_and_expenses_df["nav_per_share_if_no_value_lost_from_rebalancesIdle"] = (
        autopool_return_and_expenses_df["actual_nav"] + autopool_return_and_expenses_df["swapCostETHIdle"]
    ) / autopool_return_and_expenses_df["actual_shares"]

    autopool_return_and_expenses_df["nav_per_share_if_no_value_lost_from_rebalancesChurn"] = (
        autopool_return_and_expenses_df["actual_nav"] + autopool_return_and_expenses_df["swapCostETHChurn"]
    ) / autopool_return_and_expenses_df["actual_shares"]

    autopool_return_and_expenses_df["shares_if_no_fees_minted"] = autopool_return_and_expenses_df["actual_shares"] - (
        autopool_return_and_expenses_df["new_shares_from_periodic_fees"]
        + autopool_return_and_expenses_df["new_shares_from_streaming_fees"]
    )
    autopool_return_and_expenses_df["nav_if_no_losses_from_rebalances"] = (
        autopool_return_and_expenses_df["actual_nav"]
        + autopool_return_and_expenses_df["eth_nav_lost_by_rebalance_between_destinations"]
    )
    autopool_return_and_expenses_df["nav_per_share_if_no_fees_or_rebalances"] = (
        autopool_return_and_expenses_df["nav_if_no_losses_from_rebalances"]
        / autopool_return_and_expenses_df["shares_if_no_fees_minted"]
    )

    blocks = build_blocks_to_use()

    eth_value_if_no_discount_df = fetch_destination_totalEthValueHeldIfNoDiscount(autopool, blocks)
    eth_value_if_no_discount_df = eth_value_if_no_discount_df.resample("1D").last()

    autopool_return_and_expenses_df["nav_if_all_lp_tokens_return_to_peg"] = eth_value_if_no_discount_df[
        "nav_if_all_lp_tokens_return_to_peg"
    ]

    autopool_return_and_expenses_df["nav_per_share_if_no_discounts"] = (
        autopool_return_and_expenses_df["nav_if_all_lp_tokens_return_to_peg"]
        / autopool_return_and_expenses_df["actual_shares"]
    )

    thirty_days_return_df, thirty_day_return_metrics, lifetime_return_metrics = _compute_returns(
        autopool_return_and_expenses_df
    )
    return thirty_days_return_df, thirty_day_return_metrics, lifetime_return_metrics, autopool_return_and_expenses_df


def _compute_returns(autopool_return_and_expenses_df: pd.DataFrame) -> dict:

    thirty_days_return_df = _build_n_day_apr_df(autopool_return_and_expenses_df, n_days=30)

    lifetime_return_df = _build_n_day_apr_df(
        autopool_return_and_expenses_df, n_days=len(autopool_return_and_expenses_df) - 1
    )

    thirty_day_return_metrics = _compute_returns_lost_do_different_sources(
        thirty_days_return_df, n_days=30, prefix="30 days"
    )

    lifetime_return_metrics = _compute_returns_lost_do_different_sources(
        lifetime_return_df, n_days=len(autopool_return_and_expenses_df) - 1, prefix="lifetime"
    )
    # thirty_days_return_df['Gross Return'] = # add a gross return to the 30 day return
    return thirty_days_return_df, thirty_day_return_metrics, lifetime_return_metrics


def _build_n_day_apr_df(autopool_return_and_expenses_df: pd.DataFrame, n_days: int) -> pd.DataFrame:
    nav_per_share_cols = [
        "actual_nav_per_share",
        "nav_per_share_if_no_fees",
        "nav_per_share_if_no_value_lost_from_rebalances",
        "nav_per_share_if_no_value_lost_from_rebalancesIdle",
        "nav_per_share_if_no_value_lost_from_rebalancesChurn",
        "nav_per_share_if_no_discounts",
    ]

    n_days_return_df = (
        (
            autopool_return_and_expenses_df[nav_per_share_cols].diff(n_days)
            / autopool_return_and_expenses_df[nav_per_share_cols].shift(n_days)
        )
        * (365 / n_days)
        * 100
    )
    n_days_return_df.columns = [
        f"{n_days}_day_annualized_return_actual",
        f"{n_days}_day_annualized_return_if_no_fees",
        f"{n_days}_day_annualized_return_if_no_value_lost_from_rebalances",
        f"{n_days}_day_annualized_return_if_no_value_lost_from_rebalancesIdle",
        f"{n_days}_day_annualized_return_if_no_value_lost_from_rebalancesChurn",
        f"{n_days}_day_annualized_return_if_no_discounts",
    ]

    return n_days_return_df


def fetch_and_render_autopool_return_and_expenses_metrics(autopool: AutopoolConstants):
    thirty_days_return_df, thirty_day_return_metrics, lifetime_return_metrics, autopool_return_and_expenses_df = (
        fetch_autopool_return_and_expenses_metrics(autopool)
    )

    bridge_fig_30_days_apr = _make_bridge_plot(
        [
            thirty_day_return_metrics["30 days gross_return"],
            -thirty_day_return_metrics["30 days_return_lost_to_asset_discounts"],
            -thirty_day_return_metrics["30 days_return_lost_to_rebalance_costsIdle"],
            -thirty_day_return_metrics["30 days_return_lost_to_rebalance_costsChurn"],
            -thirty_day_return_metrics["30 days_return_lost_to_fees"],
            thirty_day_return_metrics["30 days_return_actual"],
        ],
        names=[
            "Gross Return",
            "Return Lost to Asset Discounts",
            "Return Lost to Rebalance Idle",
            "Return Lost to Rebalance dest2dest",
            "Return Lost to Fees",
            "Net Return",
        ],
        title="Annualized 30-Day Returns",
    )

    bridge_fig_lifetime = _make_bridge_plot(
        [
            lifetime_return_metrics["lifetime gross_return"],
            -lifetime_return_metrics["lifetime_return_lost_to_asset_discounts"],
            -lifetime_return_metrics["lifetime_return_lost_to_rebalance_costsIdle"],
            -lifetime_return_metrics["lifetime_return_lost_to_rebalance_costsChurn"],
            -lifetime_return_metrics["lifetime_return_lost_to_fees"],
            lifetime_return_metrics["lifetime_return_actual"],
        ],
        names=[
            "Gross Return",
            "Return Lost to Asset Discounts",
            "Return Lost to Rebalance Idle",
            "Return Lost to Rebalance dest2dest",
            "Return Lost to Fees",
            "Net Return",
        ],
        title=f"Annualized Lifetime ({len(autopool_return_and_expenses_df)}-Day) Returns",
    )

    line_fig = px.line(thirty_days_return_df, title="30-Day Annualized APR costs")
    st.plotly_chart(bridge_fig_30_days_apr, use_container_width=True)
    st.plotly_chart(bridge_fig_lifetime, use_container_width=True)
    st.plotly_chart(line_fig, use_container_width=True)

    # TODO edit this so to match the added lines
    with st.expander("See explanation of Autopool Gross and Net Return"):
        st.write(
            """
            Depositors in the Autopool experience two main costs that reduce NAV per share

            #### 1. Tokemak Protocol-Level Fees
            Autopool shares are minted to Tokemak as a periodic and streaming fees. To account for this, we track the total shares minted to Tokemak since deployment. 

            By subtracting this amount from the total supply of shares, we get the **"total supply of shares if no fees"**. Using this adjusted supply, we calculate the NA per share as if Tokemak had not charged fees:
            """
        )
        st.latex(
            r"\text{NAV per share (no fees)} = \frac{\text{NAV}}{\text{total supply of shares} - \text{shares minted for fees}}"
        )

        st.write(
            """
            #### 2. ETH Value Lost Due to Rebalances
            During rebalances ETH value is lost to slippage, swap costs and (later) a solver profit margin. The difference in ETH value from these changes is the **value lost to rebalances**. 

            By tracking this ETH loss since deployment and adding it back to NAV, we calculate the NAV per share as if no value was lost due to rebalances:
            """
        )
        st.latex(
            r"\text{NAV per share (no rebalance loss)} = \frac{\text{NAV} + \text{ETH lost to rebalances}}{\text{total supply of shares}}"
        )

        st.write(
            """
            #### Gross Return
            Gross Return is the return as if there were no fees or rebalancing costs:
            """
        )
        st.latex(
            r"\text{Gross Return} = \frac{\text{NAV} + \text{ETH lost to rebalances}}{\text{total supply of shares} - \text{shares minted for fees}}"
        )

        st.write(
            """
            #### Net Return
            Net Return represents the annualized rate of change in NAV per share. This is the actual base return experienced by depositors:
            """
        )
        st.latex(r"\text{Net Return} = \text{annualized change in NAV per share}")


def _make_bridge_plot(values: list[float], names: list[str], title: str):

    measure = ["absolute", "relative", "relative", "relative", "relative", "absolute"]

    fig = go.Figure(
        go.Waterfall(
            name="Annualized Return",
            orientation="v",
            measure=measure,
            x=names,
            y=values,
            connector={"line": {"color": "rgb(63, 63, 63)"}},
        )
    )

    fig.update_layout(title=title, waterfallgap=0.3, showlegend=True)
    return fig


if __name__ == "__main__":
    fetch_and_render_autopool_return_and_expenses_metrics(AUTO_LRT)
