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
    add_timestamp_to_df_with_block_column,
    build_blocks_to_use,
    eth_client,
)

from mainnet_launch.data_fetching.get_events import fetch_events

from mainnet_launch.constants import AUTO_ETH, AUTO_LRT, BAL_ETH, AutopoolConstants, CACHE_TIME

from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI, AUTOPOOL_ETH_STRATEGY_ABI

from mainnet_launch.solver_diagnostics.fetch_rebalance_events import (
    fetch_and_clean_rebalance_between_destination_events,
)


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


def build_actual_nav_per_share_df(autopool: AutopoolConstants) -> pd.DataFrame:

    calls = [
        getAssetBreakdown_call("actual_nav", autopool.autopool_eth_addr),
        totalSupply_call("actual_shares", autopool.autopool_eth_addr),
    ]
    blocks = build_blocks_to_use()
    df = get_raw_state_by_blocks(calls, blocks)
    df["actual_nav_per_share"] = df["actual_nav"] / df["actual_shares"]
    daily_nav_shares_df = df.resample("1D").last()
    return daily_nav_shares_df


def build_fee_shares_minted_df(autopool: AutopoolConstants):
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
    daily_fee_share_df = fee_df.resample("1D").sum()  # double check
    return daily_fee_share_df.cumsum()


def _compute_30_day_and_lifetime_annualized_return(autopool_return_and_expenses_df: pd.DataFrame, col: str):

    current_value = autopool_return_and_expenses_df.iloc[-1][col]

    value_30_days_ago = autopool_return_and_expenses_df.iloc[-31][col]  # this gets the value from 30 days ago
    today = datetime.now(timezone.utc)
    recent_year_df = autopool_return_and_expenses_df[
        autopool_return_and_expenses_df.index >= today - timedelta(days=365)
    ].copy()

    thirty_day_annualized_return = (100 * (current_value - value_30_days_ago) / value_30_days_ago) * (365 / 30)

    num_days = len(recent_year_df)
    initial_value = recent_year_df.iloc[0][col]
    lifetime_annualized_return = (100 * (current_value - initial_value) / initial_value) * (365 / num_days)

    return thirty_day_annualized_return, lifetime_annualized_return


def _compute_returns(autopool_return_and_expenses_df) -> dict:
    return_metrics = {}
    for col in ["actual_nav_per_share", "nav_per_share_if_no_fees", "nav_per_share_if_no_value_lost_from_rebalances"]:
        thirty_day_annualized_return, lifetime_annualized_return = _compute_30_day_and_lifetime_annualized_return(
            autopool_return_and_expenses_df, col
        )
        return_metrics[f"{col} 30days"] = thirty_day_annualized_return
        return_metrics[f"{col} lifetime"] = lifetime_annualized_return

    return_metrics["30_day_return_lost_to_rebalance_costs"] = (
        return_metrics["nav_per_share_if_no_value_lost_from_rebalances 30days"]
        - return_metrics["actual_nav_per_share 30days"]
    )
    return_metrics["30_day_return_lost_to_fees"] = (
        return_metrics["nav_per_share_if_no_fees 30days"] - return_metrics["actual_nav_per_share 30days"]
    )
    return_metrics["30_day_return_if_no_fees_or_rebalance_costs"] = (
        return_metrics["actual_nav_per_share 30days"]
        + return_metrics["30_day_return_lost_to_rebalance_costs"]
        + return_metrics["30_day_return_lost_to_fees"]
    )

    return_metrics["lifetime_return_lost_to_rebalance_costs"] = (
        return_metrics["nav_per_share_if_no_value_lost_from_rebalances lifetime"]
        - return_metrics["actual_nav_per_share lifetime"]
    )
    return_metrics["lifetime_return_lost_to_fees"] = (
        return_metrics["nav_per_share_if_no_fees lifetime"] - return_metrics["actual_nav_per_share lifetime"]
    )
    return_metrics["lifetime_return_if_no_fees_or_rebalance_costs"] = (
        return_metrics["actual_nav_per_share lifetime"]
        + return_metrics["lifetime_return_lost_to_rebalance_costs"]
        + return_metrics["lifetime_return_lost_to_fees"]
    )

    return return_metrics


@st.cache_data(ttl=CACHE_TIME)
def fetch_autopool_return_and_expenses_metrics(autopool: AutopoolConstants) -> dict[str, float]:

    daily_fee_share_df = build_fee_shares_minted_df(autopool)
    daily_nav_shares_df = build_actual_nav_per_share_df(autopool)
    autopool_return_and_expenses_df = daily_nav_shares_df.join(daily_fee_share_df, how="left").fillna(0)

    rebalance_df = fetch_and_clean_rebalance_between_destination_events(autopool)
    daily_nav_lost_to_rebalances = (rebalance_df[["swapCost"]].resample("1D").sum()).cumsum()
    daily_nav_lost_to_rebalances.columns = ["eth_nav_lost_by_rebalance_between_destinations"]

    autopool_return_and_expenses_df = autopool_return_and_expenses_df.join(
        daily_nav_lost_to_rebalances, how="left"
    ).fillna(0)

    autopool_return_and_expenses_df["nav_per_share_if_no_fees"] = autopool_return_and_expenses_df["actual_nav"] / (
        autopool_return_and_expenses_df["actual_shares"]
        - autopool_return_and_expenses_df["new_shares_from_periodic_fees"]
        - autopool_return_and_expenses_df["new_shares_from_streaming_fees"]
    )

    autopool_return_and_expenses_df["nav_per_share_if_no_value_lost_from_rebalances"] = (
        autopool_return_and_expenses_df["actual_nav"]
        + autopool_return_and_expenses_df["eth_nav_lost_by_rebalance_between_destinations"]
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

    returns_metrics = _compute_returns(autopool_return_and_expenses_df)
    return returns_metrics, autopool_return_and_expenses_df


def fetch_and_render_autopool_return_and_expenses_metrics(autopool: AutopoolConstants):
    returns_metrics, autopool_return_and_expenses_df = fetch_autopool_return_and_expenses_metrics(autopool)

    bridge_fig_30_days_apr = _make_bridge_plot(
        [
            returns_metrics["30_day_return_if_no_fees_or_rebalance_costs"],
            -returns_metrics["30_day_return_lost_to_fees"],
            -returns_metrics["30_day_return_lost_to_rebalance_costs"],
            returns_metrics["actual_nav_per_share 30days"],
        ],
        names=["Gross Return", "Return Lost to Fees", "Return Lost to Rebalance Costs", "Net Return"],
        title="Annualized 30-Day Returns",
    )

    bridge_fig_year_to_date = _make_bridge_plot(
        [
            returns_metrics["lifetime_return_if_no_fees_or_rebalance_costs"],
            -returns_metrics["lifetime_return_lost_to_fees"],
            -returns_metrics["lifetime_return_lost_to_rebalance_costs"],
            returns_metrics["actual_nav_per_share lifetime"],
        ],
        names=["Gross Return", "Return Lost to Fees", "Return Lost to Rebalance Costs", "Net Return"],
        title="Annualized Year-to-Date Returns",
    )

    autopool_return_and_expenses_df["30_day_annualized_gross_return"] = (
        (
            autopool_return_and_expenses_df["nav_per_share_if_no_fees_or_rebalances"].diff(30)
            / autopool_return_and_expenses_df["nav_per_share_if_no_fees_or_rebalances"].shift(30)
        )
        * (365 / 30)
        * 100
    )

    autopool_return_and_expenses_df["30_day_annualized_return_if_no_loss_from_rebalances"] = (
        (
            autopool_return_and_expenses_df["nav_per_share_if_no_value_lost_from_rebalances"].diff(30)
            / autopool_return_and_expenses_df["nav_per_share_if_no_value_lost_from_rebalances"].shift(30)
        )
        * (365 / 30)
        * 100
    )

    autopool_return_and_expenses_df["30_day_annualized_return_if_no_fees"] = (
        (
            autopool_return_and_expenses_df["nav_per_share_if_no_fees"].diff(30)
            / autopool_return_and_expenses_df["nav_per_share_if_no_fees"].shift(30)
        )
        * (365 / 30)
        * 100
    )

    autopool_return_and_expenses_df["30_day_annualized_net_return"] = (
        (
            autopool_return_and_expenses_df["actual_nav_per_share"].diff(30)
            / autopool_return_and_expenses_df["actual_nav_per_share"].shift(30)
        )
        * (365 / 30)
        * 100
    )

    line_plot_of_apr_over_time = px.line(
        autopool_return_and_expenses_df[
            [
                "30_day_annualized_gross_return",
                "30_day_annualized_return_if_no_loss_from_rebalances",
                "30_day_annualized_return_if_no_fees",
                "30_day_annualized_net_return",
            ]
        ],
        title="Autopool Gross and Net Return",
    )

    st.plotly_chart(bridge_fig_30_days_apr, use_container_width=True)
    st.plotly_chart(bridge_fig_year_to_date, use_container_width=True)
    st.plotly_chart(line_plot_of_apr_over_time, use_container_width=True)

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

    measure = ["relative", "relative", "relative", "total"]

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