# import streamlit as st
# import pandas as pd
# import plotly.express as px
# from datetime import datetime, timedelta, timezone

# from mainnet_launch.abis import CHAINLINK_KEEPER_REGISTRY_ABI
# from mainnet_launch.constants import ALL_AUTOPOOLS, ETH_CHAIN


# from mainnet_launch.data_fetching.get_events import fetch_events
# from mainnet_launch.data_fetching.add_info_to_dataframes import (
#     add_timestamp_to_df_with_block_column,
#     add_transaction_gas_info_to_df_with_tx_hash,
# )

# from mainnet_launch.pages.rebalance_events.rebalance_events import (
#     fetch_rebalance_events_df,
# )


# from mainnet_launch.database.database_operations import (
#     write_dataframe_to_table,
#     get_earliest_block_from_table_with_chain,
#     get_all_rows_in_table_by_chain,
# )

# from mainnet_launch.database.should_update_database import (
#     should_update_table,
# )


# KEEPER_REGISTRY_CONTRACT_ADDRESS = "0x6593c7De001fC8542bB1703532EE1E5aA0D458fD"


# OLD_CALCULATOR_KEEPER_ORACLE_TOPIC_ID = "1344461886831441856282597505993515040672606510446374000438363195934269203116"
# NEW_CALCULATOR_KEEPER_ORACLE_TOPIC_ID = "113129673265054907567420460651277872997162644350081440026681710279139531871240"
# NEW2_CALCULATOR_KEEPER_ORACLE_TOPIC_ID = "93443706906332180407535184303815616290343141548650473059299738217546322242910"
# INCENTIVE_PRICING_KEEPER_ORACLE_ID = "84910810589923801598536031507827941923735631663622593132512932471876788938876"
# ETH_PER_TOKEN_SENDER_ORACLE_ID = (
#     "2774403708484311544165440706031341871504925629391958533428545305548524420937"  # sends pricing info to Base
# )


# CALCULATOR_TOPIC_IDS = [
#     OLD_CALCULATOR_KEEPER_ORACLE_TOPIC_ID,
#     NEW_CALCULATOR_KEEPER_ORACLE_TOPIC_ID,
#     NEW2_CALCULATOR_KEEPER_ORACLE_TOPIC_ID,
# ]
# INCENTIVE_PRICING_TOPIC_IDS = [INCENTIVE_PRICING_KEEPER_ORACLE_ID]

# ETH_PER_TOKEN_SENDER_TOPIC_IDS = [ETH_PER_TOKEN_SENDER_ORACLE_ID]

# CHAINLINK_UPKEEP_PERFORMED_EVENT_TABLE = "CHAINLINK_UPKEEP_PERFORMED_EVENT_TABLE"


# def _fetch_and_add_rows_from_start():
#     # useful if new data is need from the past,
#     for chain in [ETH_CHAIN]:  # ignoring BASE for now
#         highest_block_already_fetched = chain.block_autopool_first_deployed
#         df = _fetch_our_chainlink_upkeep_events_from_external_source(chain, highest_block_already_fetched)
#         df["chain"] = chain.name

#         cols = [
#             "id",
#             "hash",
#             "log_index",
#             "chain",
#             "block",
#             "timestamp",
#         ]
#         write_dataframe_to_table(df[cols], CHAINLINK_UPKEEP_PERFORMED_EVENT_TABLE)


# def add_chainlink_upkeep_events_to_table():
#     if should_update_table(CHAINLINK_UPKEEP_PERFORMED_EVENT_TABLE):
#         for chain in [ETH_CHAIN]:  # ignoring BASE for now
#             highest_block_already_fetched = get_earliest_block_from_table_with_chain(
#                 CHAINLINK_UPKEEP_PERFORMED_EVENT_TABLE, chain
#             )

#             df = _fetch_our_chainlink_upkeep_events_from_external_source(chain, highest_block_already_fetched)
#             df["chain"] = chain.name

#             cols = [
#                 "id",
#                 "hash",
#                 "log_index",
#                 "chain",
#                 "block",
#                 "timestamp",
#             ]
#             write_dataframe_to_table(df[cols], CHAINLINK_UPKEEP_PERFORMED_EVENT_TABLE)


# def _fetch_our_chainlink_upkeep_events_from_external_source(chain, start_block) -> pd.DataFrame:
#     contract = chain.client.eth.contract(KEEPER_REGISTRY_CONTRACT_ADDRESS, abi=CHAINLINK_KEEPER_REGISTRY_ABI)
#     our_upkeep_df = fetch_events(
#         contract.events.UpkeepPerformed,
#         chain=chain,
#         start_block=start_block,
#         argument_filters={
#             "id": [
#                 int(i) for i in [*CALCULATOR_TOPIC_IDS, *INCENTIVE_PRICING_TOPIC_IDS, *ETH_PER_TOKEN_SENDER_TOPIC_IDS]
#             ]
#         },
#     )
#     our_upkeep_df["id"] = our_upkeep_df["id"].apply(str)

#     our_upkeep_df = add_transaction_gas_info_to_df_with_tx_hash(our_upkeep_df, ETH_CHAIN)
#     our_upkeep_df = add_timestamp_to_df_with_block_column(our_upkeep_df, ETH_CHAIN)

#     our_upkeep_df = our_upkeep_df.reset_index()

#     return our_upkeep_df


# def fetch_keeper_network_gas_costs() -> pd.DataFrame:
#     add_chainlink_upkeep_events_to_table()

#     our_upkeep_df = get_all_rows_in_table_by_chain(CHAINLINK_UPKEEP_PERFORMED_EVENT_TABLE, ETH_CHAIN)
#     our_upkeep_df = add_transaction_gas_info_to_df_with_tx_hash(our_upkeep_df, ETH_CHAIN)
#     our_upkeep_df = add_timestamp_to_df_with_block_column(our_upkeep_df, ETH_CHAIN)

#     # only count gas costs after mainnet launch on September 15
#     our_upkeep_df = our_upkeep_df[our_upkeep_df.index >= pd.Timestamp("2024-09-15", tz="UTC")].copy()

#     our_upkeep_df["gasCostInETH_without_chainlink_overhead"] = our_upkeep_df["gasCostInETH"]
#     our_upkeep_df["gasCostInETH_with_chainlink_premium"] = our_upkeep_df["gasCostInETH"] * 1.2  # 20% premium

#     return our_upkeep_df


# def fetch_and_render_keeper_network_gas_costs():

#     our_upkeep_df = fetch_keeper_network_gas_costs()

#     st.header("Gas Costs")

#     _display_gas_cost_metrics(our_upkeep_df)

#     daily_gasPrice_box_and_whisker_fig = _daily_box_plot_of_gas_prices(our_upkeep_df)
#     st.plotly_chart(daily_gasPrice_box_and_whisker_fig, use_container_width=True)

#     hourly_gas_price_box_and_whisker_fig = _hourly_box_plot_of_gas_prices(our_upkeep_df)
#     st.plotly_chart(hourly_gas_price_box_and_whisker_fig, use_container_width=True)

#     eth_spent_per_day_fig = _make_gas_spent_df(our_upkeep_df)
#     st.plotly_chart(eth_spent_per_day_fig, use_container_width=True)

#     with st.expander("See explanation for Gas Costs"):
#         st.write(
#             """
#         Top level metrics.

#         - For Chainlink Keepers we pay (in LINK) the ETH cost of the transaction + a 20% premium. 
#         - We don't pay a premium for the Solver because it is in-house.
#         - Currently Keeper transactions are set to execute at any gas price. 
#         - We can set a max gas price here https://docs.chain.link/chainlink-automation/guides/gas-price-threshold
#         - This max price can be updated frequently
#         )
#         """
#         )


# def _display_gas_cost_metrics(our_upkeep_df: pd.DataFrame):
#     calculator_df = our_upkeep_df[our_upkeep_df["id"].apply(str).isin(CALCULATOR_TOPIC_IDS)]
#     incentive_pricing_df = our_upkeep_df[our_upkeep_df["id"].apply(str).isin(INCENTIVE_PRICING_TOPIC_IDS)]
#     eth_per_token_sender_df = our_upkeep_df[our_upkeep_df["id"].apply(str).isin(ETH_PER_TOKEN_SENDER_TOPIC_IDS)]

#     calculator_gas_costs_7, calculator_gas_costs_30, calculator_gas_costs_365 = get_gas_costs(
#         calculator_df, "gasCostInETH_with_chainlink_premium"
#     )
#     incentive_gas_costs_7, incentive_gas_costs_30, incentive_gas_costs_365 = get_gas_costs(
#         incentive_pricing_df, "gasCostInETH_with_chainlink_premium"
#     )

#     eth_per_token_gas_costs_7, eth_per_token_gas_costs_30, eth_per_token_gas_costs_365 = get_gas_costs(
#         eth_per_token_sender_df, "gasCostInETH_with_chainlink_premium"
#     )

#     solver_cost_7, solver_cost_30, solver_cost_365 = fetch_solver_metrics()  # col3

#     col1, col2, col3, col4 = st.columns(4)

#     col1.metric(label="Calculator Keeper ETH Cost (Last 7 Days)", value=f"{calculator_gas_costs_7:.4f} ETH")
#     col1.metric(label="Calculator Keeper ETH Cost (Last 30 Days)", value=f"{calculator_gas_costs_30:.4f} ETH")
#     col1.metric(label="Calculator Keeper ETH Cost (Last 1 Year)", value=f"{calculator_gas_costs_365:.4f} ETH")

#     col2.metric(label="Incentive Keeper ETH Cost (Last 7 Days)", value=f"{incentive_gas_costs_7:.4f} ETH")
#     col2.metric(label="Incentive Keeper ETH Cost (Last 30 Days)", value=f"{incentive_gas_costs_30:.4f} ETH")
#     col2.metric(label="Incentive Keeper ETH Cost (Last 1 Year)", value=f"{incentive_gas_costs_365:.4f} ETH")

#     col3.metric(label="Solver ETH Cost (Last 7 Days)", value=f"{solver_cost_7:.4f} ETH")
#     col3.metric(label="Solver ETH Cost (Last 30 Days)", value=f"{solver_cost_30:.4f} ETH")
#     col3.metric(label="Solver ETH Cost (Last 1 Year)", value=f"{solver_cost_365:.4f} ETH")

#     col4.metric(
#         label="Eth Per Token (send data to Base) ETH Cost (Last 7 Days)", value=f"{eth_per_token_gas_costs_7:.4f} ETH"
#     )
#     col4.metric(
#         label="Eth Per Token (send data to Base) ETH Cost (Last 30 Days)", value=f"{eth_per_token_gas_costs_30:.4f} ETH"
#     )
#     col4.metric(
#         label="Eth Per Token (send data to Base) ETH Cost (Last 1 Year)", value=f"{eth_per_token_gas_costs_365:.4f} ETH"
#     )


# def get_gas_costs(df: pd.DataFrame, column: str):
#     today = datetime.now(timezone.utc)
#     return (
#         df[df.index >= today - timedelta(days=7)][column].sum(),
#         df[df.index >= today - timedelta(days=30)][column].sum(),
#         df[df.index >= today - timedelta(days=365)][column].sum(),
#     )


# def _make_gas_spent_df(our_upkeep_df: pd.DataFrame):
#     gas_spent_with_chainlink_premium = our_upkeep_df.resample("1D")["gasCostInETH_with_chainlink_premium"].sum()
#     return px.bar(gas_spent_with_chainlink_premium, title="Total ETH spent per day on Chainlink Keepers")


# def _daily_box_plot_of_gas_prices(our_upkeep_df: pd.DataFrame):
#     daily_gas_price = our_upkeep_df.groupby(our_upkeep_df.index.date)["gas_price"]
#     daily_gas_price_df = daily_gas_price.apply(list).reset_index()
#     daily_gas_price_df.columns = ["Date", "GasPrices"]
#     exploded_df = daily_gas_price_df.explode("GasPrices")
#     exploded_df["GasPrices"] = exploded_df["GasPrices"].astype(float)
#     daily_gasPrice_box_and_whisker_fig = px.box(
#         exploded_df, x="Date", y="GasPrices", title="Distribution of Gas Prices"
#     )

#     return daily_gasPrice_box_and_whisker_fig


# def _hourly_box_plot_of_gas_prices(our_upkeep_df: pd.DataFrame):
#     # Group by hour of the day and aggregate gas prices
#     hourly_gas_price = our_upkeep_df.groupby(our_upkeep_df.index.hour)["gas_price"]
#     hourly_gas_price_df = hourly_gas_price.apply(list).reset_index()
#     hourly_gas_price_df.columns = ["Hour", "GasPrices"]

#     # Explode the gas prices into individual rows
#     exploded_df = hourly_gas_price_df.explode("GasPrices")
#     exploded_df["GasPrices"] = exploded_df["GasPrices"].astype(float)

#     # Create the box plot for hourly distribution of gas prices
#     hourly_gas_price_box_and_whisker_fig = px.box(
#         exploded_df, x="Hour", y="GasPrices", title="UTC, Hourly Distribution of Gas Prices"
#     )

#     return hourly_gas_price_box_and_whisker_fig


# def fetch_solver_metrics():
#     rebalance_gas_cost_df = fetch_solver_gas_costs()
#     today = datetime.now(timezone.utc)
#     # Calculate costs over different periods
#     cost_last_7_days = rebalance_gas_cost_df[rebalance_gas_cost_df.index >= today - timedelta(days=7)][
#         "gasCostInETH"
#     ].sum()
#     cost_last_30_days = rebalance_gas_cost_df[rebalance_gas_cost_df.index >= today - timedelta(days=30)][
#         "gasCostInETH"
#     ].sum()
#     cost_last_1_year = rebalance_gas_cost_df[rebalance_gas_cost_df.index >= today - timedelta(days=365)][
#         "gasCostInETH"
#     ].sum()

#     return cost_last_7_days, cost_last_30_days, cost_last_1_year


# def fetch_solver_gas_costs() -> pd.DataFrame:
#     """Returns a dataframe of all the rebalance events along with the gas costs"""

#     dfs = []
#     for autopool in ALL_AUTOPOOLS:
#         if autopool.chain == ETH_CHAIN:
#             df = fetch_rebalance_events_df(autopool)  # this is fast, is using database
#             dfs.append(df)

#     clean_rebalance_df = pd.concat(dfs)

#     return clean_rebalance_df


# if __name__ == "__main__":
#     _fetch_and_add_rows_from_start()
