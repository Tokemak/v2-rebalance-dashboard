# import pandas as pd
# import streamlit as st
# import plotly.express as px
# import plotly.graph_objects as go

# from mainnet_launch.constants import eth_client, AutopoolConstants, ALL_AUTOPOOLS
# from mainnet_launch.data_fetching.get_events import fetch_events
# from mainnet_launch.data_fetching.get_state_by_block import (
#     get_raw_state_by_blocks,
#     add_timestamp_to_df_with_block_column,
# )
# from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI


# start_block = 20759126  # Sep 15, 2024


# def display_autopool_lp_stats(autopool: AutopoolConstants):
#     st.header("Autopool Allocation Over Time By Destination")
#     deposit_df, withdraw_df = _fetch_raw_deposit_and_withdrawal_dfs(autopool)
#     daily_change_fig = _make_deposit_and_withdraw_figure(autopool, deposit_df, withdraw_df)
#     lp_deposit_and_withdraw_df = _make_scatter_plot_figure(autopool, deposit_df, withdraw_df)

#     fee_df = _fetch_autopool_fee_df(autopool)
#     daily_fee_fig, cumulative_fee_fig, weekly_fee_fig = _build_fee_figures(autopool, fee_df)

#     st.header(f"{autopool.name} Our LP Stats")
#     st.plotly_chart(daily_change_fig, use_container_width=True)
#     st.plotly_chart(lp_deposit_and_withdraw_df, use_container_width=True)
#     st.plotly_chart(daily_fee_fig, use_container_width=True)
#     st.plotly_chart(weekly_fee_fig, use_container_width=True)
#     st.plotly_chart(cumulative_fee_fig, use_container_width=True)

#     with st.expander(f"See explanation"):
#         st.write(
#             """
#             - Total Deposits and Withdrawals per Day: Daily total Deposit and Withdrawals in ETH per day
#             - Individual Deposits and Withdrawals per Day: Each point is scaled by the size of the deposit or withdrawal
#             """
#         )


# def _fetch_number_of_destinations_over_time() -> pd.DataFrame:
#     return pd.DataFrame()


# def display_number_of_destinations(df: pd.DataFrame):
#     pass


# @st.cache_data(ttl=3600)
# def _fetch_autopool_fee_df(autopool: AutopoolConstants) -> pd.DataFrame:
#     contract = eth_client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
#     streaming_fee_df = fetch_events(contract.events.FeeCollected, start_block=start_block)
#     periodic_fee_df = fetch_events(contract.events.PeriodicFeeCollected, start_block=start_block)
#     streaming_fee_df = add_timestamp_to_df_with_block_column(streaming_fee_df)
#     periodic_fee_df = add_timestamp_to_df_with_block_column(periodic_fee_df)

#     periodic_fee_df["normalized_fees"] = periodic_fee_df["fees"].apply(lambda x: int(x) / 1e18)
#     if len(streaming_fee_df) > 0:
#         raise ValueError("there are streaming fees now, need to double check _fetch_autopool_fee_df function")
#         # not tested, double check once we have some fees collected
#         # streaming_fee_df['normalized_fees'] = streaming_fee_df['fees'].apply(lambda x: int(x) / 1e18)

#     # fee_df = pd.concat([streaming_fee_df[['normalized_fees']], periodic_fee_df[['normalized_fees']]])
#     fee_df = periodic_fee_df[["normalized_fees"]].copy()
#     return fee_df


# def _build_fee_figures(autopool: AutopoolConstants, fee_df: pd.DataFrame):
#     # Ensure the 'fee_df' is indexed by datetime
#     fee_df.index = pd.to_datetime(fee_df.index)

#     # 1. Daily Fees
#     daily_fees_df = fee_df.resample("1D").sum()
#     daily_fee_fig = px.bar(daily_fees_df)
#     daily_fee_fig.update_layout(
#         title=f"{autopool.name} Total Daily Fees",
#         xaxis_tickformat="%Y-%m-%d",
#         xaxis_title="Date",
#         yaxis_title="ETH",
#         xaxis_tickangle=-45,
#         width=900,
#         height=500,
#     )

#     # 2. Cumulative Lifetime Fees
#     cumulative_fees_df = daily_fees_df.cumsum()
#     cumulative_fee_fig = px.line(cumulative_fees_df)
#     cumulative_fee_fig.update_layout(
#         title=f"{autopool.name} Cumulative Lifetime Fees",
#         xaxis_tickformat="%Y-%m-%d",
#         xaxis_title="Date",
#         yaxis_title="Cumulative ETH",
#         xaxis_tickangle=-45,
#         width=900,
#         height=500,
#     )

#     # 3. Weekly Fees
#     weekly_fees_df = fee_df.resample("1W").sum()
#     weekly_fee_fig = px.bar(weekly_fees_df)
#     weekly_fee_fig.update_layout(
#         title=f"{autopool.name} Total Weekly Fees",
#         xaxis_tickformat="%Y-%m-%d",
#         xaxis_title="Date",
#         yaxis_title="ETH",
#         xaxis_tickangle=-45,
#         width=900,
#         height=500,
#     )

#     # Return all three figures
#     return daily_fee_fig, cumulative_fee_fig, weekly_fee_fig


# # def _build_fee_figure(autopool: AutopoolConstants, fee_df: pd.DataFrame) -> go.Figure:
# #     daily_fees_df = fee_df.resample("1D").sum()

# #     fig = px.bar(daily_fees_df)
# #     fig.update_layout(
# #         title=f"{autopool.name} Total Daily Fees",
# #         xaxis_tickformat="%Y-%m-%d",
# #         xaxis_title="Date",
# #         yaxis_title="ETH",
# #         xaxis_tickangle=-45,
# #         width=900,
# #         height=500,
# #     )
# #     return fig


# @st.cache_data(ttl=3600)  # 1 hours
# def _fetch_raw_deposit_and_withdrawal_dfs(autopool: AutopoolConstants) -> tuple[pd.DataFrame, pd.DataFrame]:
#     contract = eth_client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)

#     deposit_df = fetch_events(contract.events.Deposit, start_block=start_block)
#     withdraw_df = fetch_events(contract.events.Withdraw, start_block=start_block)

#     deposit_df = add_timestamp_to_df_with_block_column(deposit_df)
#     withdraw_df = add_timestamp_to_df_with_block_column(withdraw_df)

#     deposit_df["normalized_assets"] = deposit_df["assets"].apply(lambda x: int(x) / 1e18)
#     withdraw_df["normalized_assets"] = withdraw_df["assets"].apply(lambda x: int(x) / 1e18)

#     return deposit_df, withdraw_df


# def _make_deposit_and_withdraw_figure(
#     autopool: AutopoolConstants, deposit_df: pd.DataFrame, withdraw_df: pd.DataFrame
# ) -> go.Figure:
#     total_withdrawals_per_day = -withdraw_df["normalized_assets"].resample("1D").sum()
#     total_deposits_per_day = deposit_df["normalized_assets"].resample("1D").sum()

#     fig = go.Figure()

#     fig.add_trace(
#         go.Bar(x=total_withdrawals_per_day.index, y=total_withdrawals_per_day, name="Withdrawals", marker_color="red")
#     )

#     fig.add_trace(
#         go.Bar(x=total_deposits_per_day.index, y=total_deposits_per_day, name="Deposits", marker_color="blue")
#     )

#     fig.update_layout(
#         title=f"{autopool.name} Total Withdrawals and Deposits per Day",
#         xaxis_tickformat="%Y-%m-%d",
#         xaxis_title="Date",
#         yaxis_title="ETH",
#         barmode="group",
#         xaxis_tickangle=-45,
#         width=900,
#         height=500,
#     )

#     return fig


# def _make_scatter_plot_figure(
#     autopool: AutopoolConstants, deposit_df: pd.DataFrame, withdraw_df: pd.DataFrame
# ) -> go.Figure:

#     deposits = deposit_df[["normalized_assets"]]
#     withdraws = -withdraw_df[["normalized_assets"]]

#     # Concatenate withdraws and deposits
#     change_df = pd.concat([withdraws, deposits])

#     # Create a column to indicate whether the value is positive (above zero) or negative (below zero)
#     change_df["color"] = change_df["normalized_assets"].apply(lambda x: "Deposit" if x >= 0 else "Withdrawal")

#     # Plot with different colors based on the value of 'normalized_assets'
#     fig = px.scatter(
#         change_df,
#         y="normalized_assets",
#         size=change_df["normalized_assets"].abs(),
#         color=change_df["color"],  # Use the new 'color' column to map colors
#         color_discrete_map={"Deposit": "blue", "Withdrawal": "red"},
#     )  # Specify color map

#     fig.update_layout(
#         title=f"{autopool.name} Individual Deposits and Withdrawals per Day",
#         xaxis_tickformat="%Y-%m-%d",
#         xaxis_title="Date",
#         yaxis_title="ETH",
#         barmode="group",
#         xaxis_tickangle=-45,
#         width=900,
#         height=500,
#     )

#     return fig
