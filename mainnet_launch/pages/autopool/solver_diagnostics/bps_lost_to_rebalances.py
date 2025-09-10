# from plotly.subplots import make_subplots
# import plotly.graph_objects as go
# import numpy as np
# import pandas as pd
# import streamlit as st

# from mainnet_launch.constants import AutopoolConstants
# from mainnet_launch.pages.autopool_diagnostics.returns_before_expenses import (
#     fetch_nav_and_shares_and_factors_that_impact_nav_per_share,
# )


# def _build_nav_bps_lost_to_rebalance_by_day(autopool: AutopoolConstants) -> pd.DataFrame:
#     # Notes swap cost is computed from the spot value of WETH or LP tokens moved
#     nav_per_share_df = fetch_nav_and_shares_and_factors_that_impact_nav_per_share(autopool)
#     nav_per_share_df["rebalance_from_idle_bps"] = (
#         10_000 * nav_per_share_df["rebalance_from_idle_swap_cost"] / nav_per_share_df["actual_nav"]
#     )
#     nav_per_share_df["rebalance_not_idle_bps"] = (
#         10_000 * nav_per_share_df["rebalance_not_idle_swap_cost"] / nav_per_share_df["actual_nav"]
#     )

#     daily_bps_lost_to_rebalances_df = (
#         nav_per_share_df[["rebalance_from_idle_bps", "rebalance_not_idle_bps"]].resample("1D").sum()
#     )

#     return daily_bps_lost_to_rebalances_df


# def _make_daily_bps_lost_to_rebalance_figures(autopool: AutopoolConstants):
#     daily_bps_lost_to_rebalances_df = _build_nav_bps_lost_to_rebalance_by_day(autopool)
#     cumsum_daily_bps_lost_to_rebalances_df = daily_bps_lost_to_rebalances_df.cumsum()
#     rolling_daily_bps_lost_to_rebalances_df = daily_bps_lost_to_rebalances_df.rolling(30, min_periods=1).sum()

#     reset_groups = np.floor_divide(np.arange(len(daily_bps_lost_to_rebalances_df)), 30)
#     reset_cumsum = daily_bps_lost_to_rebalances_df.groupby(reset_groups).cumsum()

#     colors = {"rebalance_from_idle_bps": "red", "rebalance_not_idle_bps": "blue"}

#     fig_daily = _create_bps_lost_to_rebalances_bar_plot(
#         daily_bps_lost_to_rebalances_df,
#         title=f"Daily Basis Points Lost to Rebalances for {autopool.name}",
#         colors=colors,
#         legendgroup_prefix="daily",
#     )

#     fig_cumulative = _create_bps_lost_to_rebalances_bar_plot(
#         cumsum_daily_bps_lost_to_rebalances_df,
#         title=f"Cumulative Basis Points Lost to Rebalances for {autopool.name}",
#         colors=colors,
#         legendgroup_prefix="cumulative",
#     )

#     fig_rolling = _create_bps_lost_to_rebalances_bar_plot(
#         rolling_daily_bps_lost_to_rebalances_df,
#         title=f"30-day Rolling Sum of Basis Points Lost to Rebalances for {autopool.name}",
#         colors=colors,
#         legendgroup_prefix="rolling",
#     )

#     fig_resetting = _create_bps_lost_to_rebalances_bar_plot(
#         reset_cumsum,
#         title=f"Resetting 30-day Cumulative Basis Points Lost to Rebalances for {autopool.name}",
#         colors=colors,
#         legendgroup_prefix="reset",
#     )
#     return fig_daily, fig_cumulative, fig_rolling, fig_resetting


# def _create_bps_lost_to_rebalances_bar_plot(data, title, colors, legendgroup_prefix="") -> go.Figure:
#     fig = go.Figure()

#     fig.add_trace(
#         go.Bar(
#             x=data.index,
#             y=data["rebalance_from_idle_bps"],
#             name="Rebalance From Idle",
#             marker_color=colors["rebalance_from_idle_bps"],
#             legendgroup=f"{legendgroup_prefix}_rebalance_from_idle_bps",
#             hovertemplate="Date: %{x}<br>bps: %{y:.3f}<extra></extra>",
#         )
#     )

#     fig.add_trace(
#         go.Bar(
#             x=data.index,
#             y=data["rebalance_not_idle_bps"],
#             name="Rebalance Not From Idle",
#             marker_color=colors["rebalance_not_idle_bps"],
#             legendgroup=f"{legendgroup_prefix}_rebalance_not_idle_bps",
#             hovertemplate="Date: %{x}<br>bps: %{y:.3f}<extra></extra>",
#         )
#     )

#     fig.update_layout(
#         title_text=title,
#         barmode="stack",
#         yaxis_title="Basis Points",
#         xaxis_title="Date",
#     )

#     return fig


# def fetch_and_render_bps_lost_to_rebalances(autopool: AutopoolConstants):
#     fig_daily, fig_cumulative, fig_rolling, fig_resetting = _make_daily_bps_lost_to_rebalance_figures(autopool)
#     col1, col2 = st.columns(2)
#     with col1:
#         st.plotly_chart(fig_daily, use_container_width=True)
#     with col2:
#         st.plotly_chart(fig_cumulative, use_container_width=True)

#     col3, col4 = st.columns(2)
#     with col3:
#         st.plotly_chart(fig_rolling, use_container_width=True)
#     with col4:
#         st.plotly_chart(fig_resetting, use_container_width=True)

#     with st.expander("Explanation for Daily Basis Points Lost to Rebalances"):
#         st.markdown(
#             """
#             This is a way of gauging the value lost to slippage and swap fees during rebalances. It does not include gas costs.

#             Swap Cost the difference in spot value of tokens (base asset or LP tokens) entering or leaving the vault during the rebalance.

#             Swap Cost can sometime be negative (the solve put more value into the vault than it took out). This happened at the start
#             when we donated to the pool to make up for the deployment costs.

#             **Each bar is a daily sum of:**

#             ```
#             Basis Points of NAV Lost to rebalance = 10,000 * Swap Cost / Autopool NAV at that block
#             ```

#             for each rebalance during that day.
#             """
#         )


# if __name__ == "__main__":
#     from mainnet_launch.constants import AUTO_ETH

#     fig = _make_daily_bps_lost_to_rebalance_figures(AUTO_ETH)
#     fig.show()
