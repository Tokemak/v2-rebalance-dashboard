# import pandas as pd
# import streamlit as st
# import plotly.express as px
# from plotly.subplots import make_subplots
# import plotly.graph_objs as go

# from datetime import datetime, timedelta, timezone


# from mainnet_launch.destinations import get_destination_details
# from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS, AUTO_LRT
# from mainnet_launch.data_fetching.get_events import fetch_events
# from mainnet_launch.data_fetching.add_info_to_dataframes import (
#     add_timestamp_to_df_with_block_column,
#     add_transaction_gas_info_to_df_with_tx_hash,
# )
# from mainnet_launch.abis import AUTOPOOL_VAULT_ABI


# from mainnet_launch.database.database_operations import (
#     write_dataframe_to_table,
#     get_earliest_block_from_table_with_autopool,
#     get_all_rows_in_table_by_autopool,
# )

# from mainnet_launch.database.should_update_database import (
#     should_update_table,
# )


# AUTOPOOL_FEE_EVENTS_TABLE = "AUTOPOOL_FEE_EVENTS_TABLE"
# DESTINATION_DEBT_REPORTING_EVENTS_TABLE = "DESTINATION_DEBT_REPORTING_EVENTS_TABLE"


# def add_new_fee_events_to_table():
#     if should_update_table(AUTOPOOL_FEE_EVENTS_TABLE):
#         for autopool in ALL_AUTOPOOLS:
#             highest_block_already_fetched = get_earliest_block_from_table_with_autopool(
#                 AUTOPOOL_FEE_EVENTS_TABLE, autopool
#             )
#             vault_contract = autopool.chain.client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)

#             streaming_fee_df = fetch_events(
#                 vault_contract.events.FeeCollected, chain=autopool.chain, start_block=highest_block_already_fetched
#             )

#             streaming_fee_df["normalized_fees"] = streaming_fee_df["fees"].apply(lambda x: int(x) / 1e18)
#             streaming_fee_df["new_shares_from_streaming_fees"] = streaming_fee_df["mintedShares"] / 1e18
#             streaming_fee_df["new_shares_from_periodic_fees"] = 0.0  # so that the columns line up

#             periodic_fee_df = fetch_events(
#                 vault_contract.events.PeriodicFeeCollected,
#                 chain=autopool.chain,
#                 start_block=highest_block_already_fetched,
#             )

#             periodic_fee_df["normalized_fees"] = periodic_fee_df["fees"].apply(lambda x: int(x) / 1e18)
#             periodic_fee_df["new_shares_from_streaming_fees"] = 0  # so the columns line up
#             periodic_fee_df["new_shares_from_periodic_fees"] = periodic_fee_df["mintedShares"] / 1e18

#             cols_to_keep = [
#                 "event",
#                 "block",
#                 "hash",
#                 "normalized_fees",
#                 "new_shares_from_streaming_fees",
#                 "new_shares_from_periodic_fees",
#             ]
#             fee_df = pd.concat([streaming_fee_df, periodic_fee_df], axis=0)
#             fee_df = fee_df[cols_to_keep].copy()
#             fee_df["autopool"] = autopool.name
#             fee_df = add_timestamp_to_df_with_block_column(fee_df, autopool.chain).reset_index()
#             write_dataframe_to_table(fee_df, AUTOPOOL_FEE_EVENTS_TABLE)


# def fetch_all_autopool_fee_events(autopool: AutopoolConstants) -> pd.DataFrame:
#     add_new_fee_events_to_table()
#     fee_event_df = get_all_rows_in_table_by_autopool(AUTOPOOL_FEE_EVENTS_TABLE, autopool)
#     return fee_event_df


# def fetch_autopool_fee_data(autopool: AutopoolConstants):
#     # TODO edit this to only use fetch_all_autopool_fee_events and it splits them downstream
#     fee_event_df = fetch_all_autopool_fee_events(autopool)
#     streaming_fee_df = fee_event_df[fee_event_df["event"] == "FeeCollected"]["normalized_fees"].copy()
#     periodic_fee_df = fee_event_df[fee_event_df["event"] == "PeriodicFeeCollected"]["normalized_fees"].copy()

#     periodic_fee_df.columns = [f"{autopool.name}_periodic"]
#     streaming_fee_df.columns = [f"{autopool.name}_streaming"]

#     return periodic_fee_df, streaming_fee_df


# # TODO move this into another file
# def add_new_debt_reporting_events_to_table():
#     if should_update_table(DESTINATION_DEBT_REPORTING_EVENTS_TABLE):

#         for autopool in ALL_AUTOPOOLS:
#             highest_block_already_fetched = get_earliest_block_from_table_with_autopool(
#                 DESTINATION_DEBT_REPORTING_EVENTS_TABLE, autopool
#             )
#             highest_block_already_fetched = autopool.chain.block_autopool_first_deployed
#             vault_contract = autopool.chain.client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
#             debt_reporting_events_df = fetch_events(
#                 vault_contract.events.DestinationDebtReporting,
#                 chain=autopool.chain,
#                 start_block=highest_block_already_fetched,
#             )

#             debt_reporting_events_df = add_timestamp_to_df_with_block_column(
#                 debt_reporting_events_df, autopool.chain
#             ).reset_index()
#             debt_reporting_events_df["eth_claimed"] = debt_reporting_events_df["claimed"] / 1e18  # claimed is in ETH
#             vault_to_name = {d.vaultAddress: d.vault_name for d in get_destination_details(autopool)}
#             debt_reporting_events_df["destinationName"] = debt_reporting_events_df["destination"].apply(
#                 lambda x: vault_to_name[x]
#             )
#             debt_reporting_events_df["autopool"] = autopool.name
#             cols = ["eth_claimed", "hash", "destinationName", "autopool", "timestamp", "block", "log_index"]
#             debt_reporting_events_df = debt_reporting_events_df[cols].copy()
#             debt_reporting_events_df = add_transaction_gas_info_to_df_with_tx_hash(
#                 debt_reporting_events_df, autopool.chain
#             )
#             write_dataframe_to_table(debt_reporting_events_df, DESTINATION_DEBT_REPORTING_EVENTS_TABLE)


# def fetch_autopool_destination_debt_reporting_events(autopool: AutopoolConstants) -> pd.DataFrame:
#     add_new_debt_reporting_events_to_table()

#     debt_reporting_events_df = get_all_rows_in_table_by_autopool(DESTINATION_DEBT_REPORTING_EVENTS_TABLE, autopool)
#     return debt_reporting_events_df


# def fetch_and_render_autopool_rewardliq_plot(autopool: AutopoolConstants):
#     debt_reporting_events_df = fetch_autopool_destination_debt_reporting_events(autopool)
#     destination_cumulative_sum = debt_reporting_events_df.pivot_table(
#         values="eth_claimed", columns="destinationName", index="timestamp", fill_value=0
#     ).cumsum()
#     cumulative_eth_claimed_area_plot = px.area(
#         destination_cumulative_sum, title="Cumulative ETH value of rewards claimed by destination"
#     )
#     cumulative_eth_claimed_area_plot.update_layout(yaxis_title="ETH", xaxis_title="Date")

#     individual_reward_claim_events_fig = px.scatter(
#         debt_reporting_events_df,
#         x=debt_reporting_events_df.index,
#         y="eth_claimed",
#         color="destinationName",
#         size="eth_claimed",
#         size_max=40,
#         title="Individual reward claiming and liquidation events",
#     )

#     individual_reward_claim_events_fig.update_layout(yaxis_title="ETH", xaxis_title="Date")
#     st.plotly_chart(cumulative_eth_claimed_area_plot, use_container_width=True)
#     st.plotly_chart(individual_reward_claim_events_fig, use_container_width=True)


# def fetch_and_render_autopool_fee_data(autopool: AutopoolConstants):
#     fee_event_df = fetch_all_autopool_fee_events(autopool)
#     # add 0 fees rows at the highest date so that it resamples with the same axis

#     st.header(f"{autopool.name} Autopool Fees")
#     _display_headline_fee_metrics(fee_event_df)

#     start_date = fee_event_df.index.min()
#     end_date = fee_event_df.index.max()
#     for event, readable_event_name in zip(["PeriodicFeeCollected", "FeeCollected"], ["Periodic Fees", "Streaming Fee"]):

#         daily = fee_event_df[fee_event_df["event"] == event]["normalized_fees"].resample("1D").sum()

#         cumulative = daily.cumsum()
#         # have the weeks start and end on Wednesday 4:00 PM to Wednesday 4:00 PM UTC (hour 16)
#         # this make it line up with sTOKE fees
#         weekly = daily.shift(-16, freq="h").resample("W-WED").sum()
#         weekly.index = weekly.index + pd.Timedelta(hours=16)

#         fig = make_subplots(rows=1, cols=3, subplot_titles=("Daily Fees", "Weekly Fees", "Cumulative Fees"))

#         fig.add_trace(go.Bar(x=daily.index, y=daily, name="Daily"), row=1, col=1)
#         fig.add_trace(go.Bar(x=weekly.index, y=weekly, name="Weekly"), row=1, col=2)
#         fig.add_trace(go.Bar(x=cumulative.index, y=cumulative, name="Cumulative"), row=1, col=3)

#         for col in [1, 2, 3]:
#             fig.update_xaxes(range=[start_date, end_date], row=1, col=col)

#         fig.update_yaxes(title_text="ETH", row=1, col=1)
#         fig.update_layout(
#             xaxis=dict(range=[start_date, end_date]),
#             yaxis_title="ETH",
#             title="Daily Periodic Fees",
#             title_text=f"{autopool.name} {readable_event_name}",
#             showlegend=False,
#         )

#         st.plotly_chart(fig, use_container_width=True)


# def _compute_fee_values(fee_event_df: pd.DataFrame):
#     """Returns what fees and how much over time"""
#     today = datetime.now(timezone.utc)

#     seven_days_ago = today - timedelta(days=7)
#     thirty_days_ago = today - timedelta(days=30)
#     year_ago = today - timedelta(days=365)

#     periodic_fees = {}
#     streaming_fees = {}

#     for window, window_name in zip(
#         [seven_days_ago, thirty_days_ago, year_ago],
#         [
#             "seven_days_ago",
#             "thirty_days_ago",
#             "year_ago",
#         ],
#     ):
#         recent_df = fee_event_df[fee_event_df.index >= window]

#         periodic_fees[window_name] = recent_df[recent_df["event"] == "PeriodicFeeCollected"]["normalized_fees"].sum()
#         streaming_fees[window_name] = recent_df[recent_df["event"] == "FeeCollected"]["normalized_fees"].sum()

#     return periodic_fees, streaming_fees


# def _display_headline_fee_metrics(
#     fee_event_df: pd.DataFrame,
# ):
#     periodic_fees, streaming_fees = _compute_fee_values(fee_event_df)

#     col1, col2, col3 = st.columns(3)

#     with col1:
#         st.metric(label="Periodic Fees Earned Over Last 7 Days (ETH)", value=f"{periodic_fees['seven_days_ago']:.2f}")
#         st.metric(label="Streaming Fees Earned Over Last 7 Days (ETH)", value=f"{streaming_fees['seven_days_ago']:.2f}")

#     with col2:
#         st.metric(label="Periodic Fees Earned Over Last 30 Days (ETH)", value=f"{periodic_fees['thirty_days_ago']:.2f}")
#         st.metric(
#             label="Streaming Fees Earned Over Last 30 Days (ETH)", value=f"{streaming_fees['thirty_days_ago']:.2f}"
#         )

#     with col3:
#         st.metric(label="Periodic Fees Earned Over Last 1 Year (ETH)", value=f"{periodic_fees['year_ago']:.2f}")
#         st.metric(label="Streaming Fees Earned Over Last 1 Year (ETH)", value=f"{streaming_fees['year_ago']:.2f}")


# def _build_fee_figures(autopool: AutopoolConstants, fee_df: pd.DataFrame):
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
#     # Resample from Wednesday 4:00 PM to Wednesday 4:00 PM UTC (hour 16)
#     shifted_fee_df = fee_df.shift(-16, freq="h")
#     weekly_fees_df = shifted_fee_df.resample("W-WED").sum()
#     weekly_fees_df.index = weekly_fees_df.index + pd.Timedelta(hours=16)

#     weekly_fee_fig = px.bar(weekly_fees_df)
#     weekly_fee_fig.update_layout(
#         title=f"{autopool.name} Total Weekly Fees",
#         xaxis_tickformat="%Y-%m-%d %H:%M",
#         xaxis_title="Date",
#         yaxis_title="ETH",
#         xaxis_tickangle=-45,
#         width=900,
#         height=500,
#     )

#     return daily_fee_fig, cumulative_fee_fig, weekly_fee_fig


# if __name__ == "__main__":
#     fetch_and_render_autopool_fee_data(AUTO_LRT)
