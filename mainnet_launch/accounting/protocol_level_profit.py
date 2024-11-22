from datetime import datetime, timedelta, timezone

import streamlit as st
import pandas as pd

from mainnet_launch.constants import CACHE_TIME, ALL_AUTOPOOLS, ETH_CHAIN, BASE_CHAIN, AutopoolConstants, ChainData
from mainnet_launch.gas_costs.keeper_network_gas_costs import (
    fetch_solver_gas_costs,
    fetch_keeper_network_gas_costs,
    fetch_all_autopool_debt_reporting_events,
)

from mainnet_launch.autopool_diagnostics.fees import fetch_autopool_fee_data
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column


@st.cache_data(ttl=CACHE_TIME)
def fetch_protocol_level_profit_and_loss_data():
    gas_cost_df = fetch_gas_cost_df()
    eth_fee_df = fetch_fee_df(ETH_CHAIN)
    base_fee_df = fetch_fee_df(BASE_CHAIN)
    fee_df = pd.concat([eth_fee_df, base_fee_df]).fillna(0.0)
    fee_df.sort_index(inplace=True)
    return gas_cost_df, fee_df


def fetch_and_render_protocol_level_profit_and_loss_data():
    gas_cost_df, fee_df = fetch_protocol_level_profit_and_loss_data()

    today = datetime.now(timezone.utc)

    seven_days_ago = today - timedelta(days=7)
    thirty_days_ago = today - timedelta(days=30)
    one_year_ago = today - timedelta(days=365)

    for window, window_name in zip([seven_days_ago, thirty_days_ago, one_year_ago], ["7-Day", "30-Day", "1-Year"]):
        _render_protocol_level_profit_and_loss_tables(gas_cost_df, fee_df, window, window_name)


def _render_protocol_level_profit_and_loss_tables(
    gas_cost_df: pd.DataFrame, fee_df: pd.DataFrame, window: timedelta, window_name: str
):
    gas_costs_within_window_raw = (
        gas_cost_df[gas_cost_df.index > window][
            ["debt_reporting_gas_cost_in_eth", "solver_gas_cost_in_eth", "calculator_gas_cost_in_eth"]
        ]
        .sum()
        .round(2)
        .to_dict()
    )

    gas_costs_within_window = {
        "Debt Reporting Gas Costs": -gas_costs_within_window_raw["debt_reporting_gas_cost_in_eth"],
        "Solver Gas Costs": -gas_costs_within_window_raw["solver_gas_cost_in_eth"],
        "Calculator Gas Costs": -gas_costs_within_window_raw["calculator_gas_cost_in_eth"],
    }

    gas_costs_within_window["Total Expenses"] = sum(gas_costs_within_window.values())

    fees_within_window_raw = fee_df[fee_df.index > window].sum().round(2).to_dict()

    fees_within_window = {
        "autoETH Periodic": fees_within_window_raw["autoETH_periodic"],
        "autoETH Streaming": fees_within_window_raw["autoETH_streaming"],
        "balETH Periodic": fees_within_window_raw["balETH_periodic"],
        "balETH Streaming": fees_within_window_raw["balETH_streaming"],
        "autoLRT Periodic": fees_within_window_raw["autoLRT_periodic"],
        "autoLRT Streaming": fees_within_window_raw["autoLRT_streaming"],
    }

    fees_within_window["Total Revenue"] = sum(fees_within_window.values())

    net_profit_dict = {
        "Net Profit": round(fees_within_window["Total Revenue"] + gas_costs_within_window["Total Expenses"], 2)
    }

    profit_and_loss_dict = {**gas_costs_within_window, **fees_within_window, **net_profit_dict}

    profit_and_loss_df = pd.DataFrame(list(profit_and_loss_dict.items()), columns=["Description", "Amount (ETH)"])

    st.header(f"ETH Profit and Loss ({window_name})")
    st.table(profit_and_loss_df)


def fetch_gas_cost_df() -> pd.DataFrame:
    """Fetch the gas costs for running the solver, reward token liqudation / debt reporting, and calculators (chainlink keeper network)"""
    # only tracking gas costs of ethereum mainnet, not Base because gas is near free on Base
    destination_debt_reporting_df = fetch_all_autopool_debt_reporting_events(ETH_CHAIN)
    rebalance_gas_cost_df = fetch_solver_gas_costs()
    keeper_gas_costs_df = fetch_keeper_network_gas_costs()

    gas_cost_columns = ["hash", "gas_price", "gas_used", "gasCostInETH"]

    debt_reporting_costs = destination_debt_reporting_df[gas_cost_columns].copy().drop_duplicates()
    debt_reporting_costs.columns = [
        "hash",
        "debt_reporting_gas_price",
        "debt_reporting_gas_used",
        "debt_reporting_gas_cost_in_eth",
    ]

    solver_costs = rebalance_gas_cost_df[gas_cost_columns].copy().drop_duplicates()
    solver_costs.columns = ["hash", "solver_gas_price", "solver_gas_used", "solver_gas_cost_in_eth"]

    keeper_costs = keeper_gas_costs_df[gas_cost_columns].copy().drop_duplicates()
    keeper_costs.columns = ["hash", "calculator_gas_price", "calculator_gas_used", "calculator_gas_cost_in_eth"]

    # sometimes the solver rebalancing causes destination debt reporting
    # in that case because this only tracks gas cost at the transaction level,
    # drop all the rows in debt_reporting_costs where the solver also executed a rebalance
    # this avoids double counting

    # the solver a little inflated and the debt reporting is a little under.

    debt_reporting_costs = debt_reporting_costs[~debt_reporting_costs["hash"].isin(solver_costs["hash"])].copy()

    gas_cost_df = pd.concat([debt_reporting_costs, solver_costs, keeper_costs])

    if len(gas_cost_df["hash"].unique()) != len(gas_cost_df):
        raise ValueError("unexpected duplicate hashes found in gas_cost_df")

    return gas_cost_df


def fetch_fee_df(chain: ChainData) -> pd.DataFrame:
    """
    Fetch all the the fees in ETH from the feeCollected and PeriodicFeeCollected events for each autopool
    """
    fee_dfs = []
    for autopool in ALL_AUTOPOOLS:
        if autopool.chain == chain:

            periodic_fee_df, streaming_fee_df = fetch_autopool_fee_data(autopool)
            fee_dfs.extend([periodic_fee_df, streaming_fee_df])
    fee_df = pd.concat(fee_dfs).fillna(0.0)
    fee_df.sort_index(inplace=True)
    return fee_df


if __name__ == "__main__":
    fetch_and_render_protocol_level_profit_and_loss_data()
    pass
