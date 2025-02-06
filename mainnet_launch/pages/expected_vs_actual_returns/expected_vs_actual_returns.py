import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import pandas as pd
import streamlit as st
from plotly.subplots import make_subplots

from mainnet_launch.constants import AutopoolConstants

from mainnet_launch.pages.autopool_diagnostics.returns_before_expenses import (
    fetch_nav_and_shares_and_factors_that_impact_nav_per_share,
    _compute_adjusted_nav_per_share_n_days,
)
from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats
from mainnet_launch.pages.key_metrics.fetch_nav_per_share import fetch_nav_per_share


def _build_expected_return_df(autopool: AutopoolConstants, n_days: int) -> pd.DataFrame:
    """
    CR out is our at moment prediction of the return of the autopool
    Change in price return is the dominating factor why this is different

    If CR out was perfectly accurate and annualized change in price return is also accurate

    gross return = CR out - Change in Price Return

    where gross return is the growth in nav per share if there were no rebalance costs and no fees


    Therefore the difference between these comes from


    gross return + noise = (CR out + noise) - (Change in Price return + noise)

    Sources of noise

    Gross return noise
    - in accuracy in backing out fees and rebalance costs

    CR out noise
    - Incentive token price movements also scaling down by .9
    - Fee apr diff
    - Points apr


    Price return noise
    - We hold more of an asset when it trades at a discount than we do when it pops back up to peg
    - Scalng down by .75


    """
    compositeReturn_out_df = 100 * fetch_destination_summary_stats(autopool, "compositeReturn")
    pricePerShare_df = 100 * fetch_destination_summary_stats(autopool, "pricePerShare")
    ownedShares_df = 100 * fetch_destination_summary_stats(autopool, "ownedShares")
    allocation_df = pricePerShare_df * ownedShares_df
    portion_allocation_df = allocation_df.div(allocation_df.sum(axis=1), axis=0)
    compositeReturn_out_df[f"Composite Return Out"] = (compositeReturn_out_df * portion_allocation_df).sum(axis=1)

    priceReturn_df = fetch_destination_summary_stats(autopool, "priceReturn")

    compositeReturn_out_df["Weighted Price Return"] = 100 * (priceReturn_df * portion_allocation_df).sum(axis=1)

    compositeReturn_out_df = compositeReturn_out_df.resample("1D").last()[
        [
            "Weighted Price Return",
            "Composite Return Out",
        ]
    ]
    compositeReturn_out_df["Annualized Change In Price Return"] = (
        compositeReturn_out_df["Weighted Price Return"].diff(n_days) * 365 / n_days
    )

    compositeReturn_out_df["Rolling Average Composite Return Out"] = (
        compositeReturn_out_df["Composite Return Out"].rolling(n_days).mean()
    )

    # if change in price return is positive then the discount increased. So our assets are worth less than they were before
    compositeReturn_out_df["Expected Return"] = (
        compositeReturn_out_df[f"Rolling Average Composite Return Out"]
        - compositeReturn_out_df["Annualized Change In Price Return"]
    )

    return compositeReturn_out_df


def _build_autopool_gross_and_net_return_df(autopool: AutopoolConstants, n_days: int) -> pd.DataFrame:
    """
    Net return: Annualized Return in Nav per share over n days
    Gross return: Annualized Return in Nav per share over n days if there were no fees or rebalance costs
    """

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

    adjusted_nav_per_share_df["Net Return"] = adjusted_nav_per_share_df[f"actual_{n_days}_days_annualized_apr"]
    adjusted_nav_per_share_df["Gross Return"] = adjusted_nav_per_share_df[f"adjusted_{n_days}_days_annualized_apr"]

    return adjusted_nav_per_share_df[["Net Return", "Gross Return"]]


def _fetch_expected_and_gross_apr_df(autopool: AutopoolConstants, n_days: int) -> pd.DataFrame:
    gross_and_net_return_df = _build_autopool_gross_and_net_return_df(autopool, n_days)
    cr_and_price_return_df = _build_expected_return_df(autopool, n_days)

    apr_df = pd.concat([gross_and_net_return_df, cr_and_price_return_df], axis=1)
    apr_df["Gross + Price Return"] = apr_df["Gross Return"] + apr_df["Annualized Change In Price Return"]
    apr_df = apr_df.dropna()
    return apr_df


def _make_scatter_plot(apr_df, x_col, y_col, autopool, n_days):
    scatter_plot_fig = px.scatter(
        apr_df,
        x=x_col,
        y=y_col,
        title=f"{x_col} vs {y_col} {autopool.name} {n_days} days",
        hover_data={"index": apr_df.index.date},
    )
    line_max = max(apr_df[y_col].max(), apr_df[y_col].max()) + 3
    line_min = max(apr_df[y_col].min(), apr_df[y_col].min()) - 3

    scatter_plot_fig.add_shape(
        type="line",
        x0=line_min,
        y0=line_min,
        x1=line_max,
        y1=line_max,
        line=dict(
            color="black",
            dash="dash",
        ),
    )

    scatter_plot_fig.add_annotation(
        x=0.25 * line_max,
        y=0.75 * line_max,
        text="More than Expected",
        showarrow=False,
        font=dict(color="green", size=12),
    )

    scatter_plot_fig.add_annotation(
        x=0.75 * line_max,
        y=0.25 * line_max,
        text="Less than Expected",
        showarrow=False,
        font=dict(color="red", size=12),
    )
    return scatter_plot_fig


def _make_apr_figures(apr_df: pd.DataFrame, autopool: AutopoolConstants, n_days: int):

    apr_line_fig = px.line(
        apr_df[
            [
                "Expected Return",
                "Annualized Change In Price Return",
                "Gross Return",
                "Gross + Price Return",
                "Rolling Average Composite Return Out",
            ]
        ],
        title=f"{autopool.name} Expected and Acutal Performance Window size {n_days} days",
    )

    expected_vs_gross_scatter_fig = _make_scatter_plot(apr_df, "Expected Return", "Gross Return", autopool, n_days)

    return apr_line_fig, expected_vs_gross_scatter_fig


def fetch_and_render_actual_and_gross_and_projected_returns(autopool: AutopoolConstants):

    for n_days in [30, 7]:
        apr_df = _fetch_expected_and_gross_apr_df(autopool, n_days)
        apr_line_fig, expected_vs_gross_scatter_fig = _make_apr_figures(apr_df, autopool, n_days)
        col1, col2 = st.columns(2)

        with col1:
            st.plotly_chart(apr_line_fig, use_container_width=True)
        with col2:
            st.plotly_chart(expected_vs_gross_scatter_fig, use_container_width=True)

    with st.expander("Explanation"):
        st.markdown(
            """
                ## Key Metrics Explained

                ### Gross Return
 
                - The *hypothetical* `n-day` annualized growth in NAV per share, calculated by removing Tokemak fees and rebalance costs.

                
                ### Annualized Change in Price Return
                - The annualized change in the autopool weighted Price Return over an `n-day` period.

                > Annualized Change in Price Return = (Change in Weighted Price Return over n days) x (365 / n)

                - **Interpretation:**  
                - A **positive** value indicates that the Price Return has increased (e.g., an increase in LST discounts causing assets to be valued lower).
                - A **negative** value indicates a decrease in the Price Return (e.g., a decrease in LST discount causing asset values to increase).

                - This change can significantly affect top line APR. This is more exagerated than with a shorter n-day period.

                ### Expected Return
                - Calculated as the difference between the `n-day` Rolling Average Composite Return Out and the Annualized Change in Price Return.

                > Expected Return = Rolling Average Composite Return Out - Annualized Change in Price Return

                - The is what the autopool APR *should* be if the `n-day` Rolling Average Composite Return Out was perfectly accurate and the change in price return fully captured the change in asset value.

                The Solver makes rebalance decisions to exit a destination based on Composite Return Out. 

                The price return of a destination is a relativly small factor of price return. It is not scaled at all. 

                """
        )


if __name__ == "__main__":
    from mainnet_launch.constants import AUTO_ETH

    fetch_and_render_actual_and_gross_and_projected_returns(AUTO_ETH)
