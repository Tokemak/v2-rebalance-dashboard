# from datetime import datetime, timedelta, timezone

# import streamlit as st
# import pandas as pd

# from mainnet_launch.pages.gas_costs.keeper_network_gas_costs import (
#     fetch_solver_gas_costs,
#     fetch_keeper_network_gas_costs,
# )

# from mainnet_launch.pages.protocol_level_profit_and_loss.fees import (
#     AUTOPOOL_FEE_EVENTS_TABLE,
#     DESTINATION_DEBT_REPORTING_EVENTS_TABLE,
#     add_new_debt_reporting_events_to_table,
# )
# from mainnet_launch.database.database_operations import run_read_only_query
# from mainnet_launch.database.should_update_database import should_update_table

# # TODO this is not accurate because fees are going to sTOKE.
# # update this to match the pattern, if should update the data when needed


# def fetch_protocol_level_profit_and_loss_data():
#     gas_cost_df = fetch_gas_cost_df()
#     fee_df = fetch_fees_by_autopool_by_type()
#     return gas_cost_df, fee_df


# def fetch_fees_by_autopool_by_type() -> pd.DataFrame:
#     query = f"""SELECT event, autopool, normalized_fees, timestamp from {AUTOPOOL_FEE_EVENTS_TABLE}"""
#     fee_df = run_read_only_query(query, params=None)
#     fee_df = fee_df.set_index("timestamp")

#     periodic_fee_events_df = fee_df[fee_df["event"] == "PeriodicFeeCollected"].pivot_table(
#         columns="autopool", values="normalized_fees", index="timestamp"
#     )
#     periodic_fee_events_df.columns = [f"{autopool_name}_periodic" for autopool_name in periodic_fee_events_df.columns]

#     streaming_fee_events_df = fee_df[fee_df["event"] == "FeeCollected"].pivot_table(
#         columns="autopool", values="normalized_fees", index="timestamp"
#     )
#     streaming_fee_events_df.columns = [
#         f"{autopool_name}_streaming" for autopool_name in streaming_fee_events_df.columns
#     ]
#     flat_fee_df = pd.concat([streaming_fee_events_df, periodic_fee_events_df]).fillna(0).sort_index()
#     return flat_fee_df


# def fetch_and_render_protocol_level_profit_and_loss_data():
#     gas_cost_df, fee_df = fetch_protocol_level_profit_and_loss_data()

#     today = datetime.now(timezone.utc)

#     seven_days_ago = today - timedelta(days=7)
#     thirty_days_ago = today - timedelta(days=30)
#     one_year_ago = today - timedelta(days=365)

#     for window, window_name in zip([seven_days_ago, thirty_days_ago, one_year_ago], ["7-Day", "30-Day", "1-Year"]):
#         _render_protocol_level_profit_and_loss_tables(gas_cost_df, fee_df, window, window_name)


# def _render_protocol_level_profit_and_loss_tables(
#     gas_cost_df: pd.DataFrame, fee_df: pd.DataFrame, window: timedelta, window_name: str
# ):
#     gas_costs_within_window_raw = (
#         gas_cost_df[gas_cost_df.index > window][
#             ["debt_reporting_gas_cost_in_eth", "solver_gas_cost_in_eth", "calculator_gas_cost_in_eth"]
#         ]
#         .sum()
#         .round(2)
#         .to_dict()
#     )

#     gas_costs_within_window = {
#         "Debt Reporting Gas Costs": -gas_costs_within_window_raw["debt_reporting_gas_cost_in_eth"],
#         "Solver Gas Costs": -gas_costs_within_window_raw["solver_gas_cost_in_eth"],
#         "Calculator Gas Costs": -gas_costs_within_window_raw["calculator_gas_cost_in_eth"],
#     }

#     gas_costs_within_window["Total Expenses"] = sum(gas_costs_within_window.values())

#     fees_within_window_raw = fee_df[fee_df.index > window].sum().round(2).to_dict()

#     fees_within_window = {
#         "autoETH Periodic": fees_within_window_raw["autoETH_periodic"],
#         "autoETH Streaming": fees_within_window_raw["autoETH_streaming"],
#         "balETH Periodic": fees_within_window_raw["balETH_periodic"],
#         "balETH Streaming": fees_within_window_raw["balETH_streaming"],
#         "autoLRT Periodic": fees_within_window_raw["autoLRT_periodic"],
#         "autoLRT Streaming": fees_within_window_raw["autoLRT_streaming"],
#     }

#     fees_within_window["Total Revenue"] = sum(fees_within_window.values())

#     net_profit_dict = {
#         "Net Profit": round(fees_within_window["Total Revenue"] + gas_costs_within_window["Total Expenses"], 2)
#     }

#     profit_and_loss_dict = {**gas_costs_within_window, **fees_within_window, **net_profit_dict}

#     profit_and_loss_df = pd.DataFrame(list(profit_and_loss_dict.items()), columns=["Description", "Amount (ETH)"])

#     st.header(f"ETH Profit and Loss ({window_name})")
#     st.table(profit_and_loss_df)


# def fetch_gas_cost_df() -> pd.DataFrame:
#     """Fetch the gas costs for running the solver, reward token liqudation / debt reporting, and calculators (chainlink keeper network)"""

#     add_new_debt_reporting_events_to_table()

#     destination_debt_reporting_df = run_read_only_query(
#         f"""SELECT * FROM {DESTINATION_DEBT_REPORTING_EVENTS_TABLE}""", params=None
#     ).set_index("timestamp")

#     rebalance_gas_cost_df = fetch_solver_gas_costs()  # is reading from cache
#     keeper_gas_costs_df = fetch_keeper_network_gas_costs()

#     gas_cost_columns = ["hash", "gas_price", "gas_used", "gasCostInETH"]

#     debt_reporting_costs = destination_debt_reporting_df[gas_cost_columns].copy().drop_duplicates()
#     debt_reporting_costs.columns = [
#         "hash",
#         "debt_reporting_gas_price",
#         "debt_reporting_gas_used",
#         "debt_reporting_gas_cost_in_eth",
#     ]

#     solver_costs = rebalance_gas_cost_df[gas_cost_columns].copy().drop_duplicates()
#     solver_costs.columns = ["hash", "solver_gas_price", "solver_gas_used", "solver_gas_cost_in_eth"]

#     keeper_costs = keeper_gas_costs_df[gas_cost_columns].copy().drop_duplicates()
#     keeper_costs.columns = ["hash", "calculator_gas_price", "calculator_gas_used", "calculator_gas_cost_in_eth"]

#     # sometimes the solver rebalancing causes destination debt reporting
#     # in that case because this only tracks gas cost at the transaction level,
#     # drop all the rows in debt_reporting_costs where the solver also executed a rebalance
#     # this avoids double counting

#     # the solver a little inflated and the debt reporting is a little under.

#     debt_reporting_costs = debt_reporting_costs[~debt_reporting_costs["hash"].isin(solver_costs["hash"])].copy()

#     gas_cost_df = pd.concat([debt_reporting_costs, solver_costs, keeper_costs])

#     if len(gas_cost_df["hash"].unique()) != len(gas_cost_df):
#         raise ValueError("unexpected duplicate hashes found in gas_cost_df")

#     return gas_cost_df


# if __name__ == "__main__":
#     fetch_and_render_protocol_level_profit_and_loss_data()
