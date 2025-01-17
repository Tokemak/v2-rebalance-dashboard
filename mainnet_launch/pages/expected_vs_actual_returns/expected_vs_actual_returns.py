import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


import plotly.graph_objects as go
from plotly.subplots import make_subplots

from mainnet_launch.constants import AutopoolConstants

from mainnet_launch.pages.autopool_diagnostics.returns_before_expenses import (
    fetch_nav_and_shares_and_factors_that_impact_nav_per_share,
    _compute_adjusted_nav_per_share_n_days,
)
from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats
from mainnet_launch.pages.key_metrics.fetch_nav_per_share import fetch_nav_per_share


def build_CR_out_vs_gross_and_net_performance_fig(autopool: AutopoolConstants, n_days: int):

    compositeReturn_out_df = 100 * fetch_destination_summary_stats(autopool, "compositeReturn")
    pricePerShare_df = 100 * fetch_destination_summary_stats(autopool, "pricePerShare")
    ownedShares_df = 100 * fetch_destination_summary_stats(autopool, "ownedShares")
    allocation_df = pricePerShare_df * ownedShares_df
    portion_allocation_df = allocation_df.div(allocation_df.sum(axis=1), axis=0)
    autopool_weighted_expected_return = (compositeReturn_out_df * portion_allocation_df).sum(axis=1)
    compositeReturn_out_df[f"{autopool.name} CR"] = autopool_weighted_expected_return

    priceReturn_df = 100 * fetch_destination_summary_stats(autopool, "priceReturn")
    weighted_price_return_df = (priceReturn_df * portion_allocation_df).sum(axis=1)

    nav_per_share_df = fetch_nav_per_share(autopool)

    df = pd.concat(
        [compositeReturn_out_df.resample("1D").last(), nav_per_share_df[f"{n_days}_day_annualized_return"]], axis=1
    )

    df[f"avg_cr_out_prior_{n_days}_days"] = df[f"{autopool.name} CR"].rolling(n_days).mean()

    nav_per_share_df = fetch_nav_and_shares_and_factors_that_impact_nav_per_share(autopool)
    adjusted_nav_per_share_df = _compute_adjusted_nav_per_share_n_days(
        nav_per_share_df,
        n_days=n_days,
        apply_periodic_fees=True,
        apply_streaming_fees=True,
        apply_rebalance_from_idle_swap_cost=True,
        apply_rebalance_not_idle_swap_cost=True,
        apply_nav_lost_to_depeg=False,
    )

    gross_net_and_projected_returns_df = pd.concat(
        [
            df[[f"{autopool.name} CR", f"{n_days}_day_annualized_return", f"avg_cr_out_prior_{n_days}_days"]],
            adjusted_nav_per_share_df,
        ],
        axis=1,
    )

    gross_net_and_projected_returns_df["diff_between_rolling_weighted_CR_and_adjusted"] = (
        gross_net_and_projected_returns_df[f"adjusted_{n_days}_days_annualized_apr"]
        - gross_net_and_projected_returns_df[f"avg_cr_out_prior_{n_days}_days"]
    )
    gross_net_and_projected_returns_df["diff_between_rolling_weighted_CR_and_actual"] = (
        gross_net_and_projected_returns_df[f"actual_{n_days}_days_annualized_apr"]
        - gross_net_and_projected_returns_df[f"avg_cr_out_prior_{n_days}_days"]
    )

    weighted_price_return_df = weighted_price_return_df.resample("1D").last()

    gross_net_and_projected_returns_df["autopool_price_return"] = weighted_price_return_df
    gross_net_and_projected_returns_df["change_in_price_return"] = gross_net_and_projected_returns_df[
        "autopool_price_return"
    ].diff(n_days)

    gross_net_and_projected_returns_df = gross_net_and_projected_returns_df.dropna()
    return gross_net_and_projected_returns_df


def _create_autopool_cr_gross_and_net_and_price_price_return_figure(
    autopool: AutopoolConstants, df: pd.DataFrame, n_days: int
):
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    apr_columns = [
        f"adjusted_{n_days}_days_annualized_apr",
        f"actual_{n_days}_days_annualized_apr",
        f"avg_cr_out_prior_{n_days}_days",
    ]

    readable_column_names = ["Gross Return", "Net Return", f"{n_days} Days Rolling Avg Composite Return"]

    for col, name in zip(apr_columns, readable_column_names):
        fig.add_trace(go.Scatter(x=df.index, y=df[col], name=name), secondary_y=False)

    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df["change_in_price_return"],
            name=f"{n_days} day change in Price Return",
            mode="lines",
            line=dict(
                dash="dash",
                color="red",
            ),
        ),
        secondary_y=True,
    )

    fig.update_yaxes(title_text="Gross Net and CR", secondary_y=False)  # Left Y-axis
    fig.update_yaxes(title_text=f"Percent Change in Price Return", secondary_y=True)  # Right Y-axis

    # Optionally update x-axis and layout settings
    fig.update_xaxes(title_text="Date")
    fig.update_layout(title_text=f"{autopool.name} {n_days} Days APR and Change Price Return Over Time")

    return fig


def fetch_and_render_actual_and_gross_and_projected_returns(autopool: AutopoolConstants):
    df_30_days = build_CR_out_vs_gross_and_net_performance_fig(autopool, 30)
    figure_30_days = _create_autopool_cr_gross_and_net_and_price_price_return_figure(autopool, df_30_days, 30)

    st.plotly_chart(figure_30_days, use_container_width=True)

    df_7_days = build_CR_out_vs_gross_and_net_performance_fig(autopool, 7)
    figure_7_days = _create_autopool_cr_gross_and_net_and_price_price_return_figure(autopool, df_7_days, 7)

    st.plotly_chart(figure_7_days, use_container_width=True)

    with st.expander("Explanation"):
        st.markdown(
            """
        This is a measure of how well the Autopool Composite Return predicts the actual return.

        **Net Return:** 
        - The *actual* annualized growth in NAV per share.

        **Gross Return:**
        - The *hypothetical* annualized growth in NAV per share, assuming:
            - There were no Tokemak fees, and
            - No NAV was lost to rebalances.

        **N-day Rolling Average Composite Return:**
        - The weighted average of the Composite Return of the Autopool for the previous N days.
        - The weights are the percent of NAV in that destination, and the values are the Composite Return out of that destination.
        - This tries to give a near 1:1 comparison with Composite Return Out, because Composite Return does not factor in the costs to enter a destination.

        **Change in Price Return:**
        - This compares the total Price Return of the autopool N days ago with the current Price Return.

        Change in Price Return = Current Autopool Price Return - Price Return N days ago

        - A positive value means the total Price Return of the autopool increased in the last N days.
            - e.g., the LST discount increased, making our assets decrease in value.
        - A negative value means the total Price Return of the autopool decreased in the last N days.
            - e.g., the LST discount decreased, making our assets increase in value.

        When the Price Return moves in our favor, it makes the APR look better.
        When the Price Return moves against us, it makes the APR look worse.
        
        This is more exaggerated the shorter the window used to annualize. 
        """
        )


if __name__ == "__main__":
    from mainnet_launch.constants import AUTO_ETH

    fetch_and_render_actual_and_gross_and_projected_returns(AUTO_ETH)
