from concurrent.futures import ThreadPoolExecutor
import threading
import numpy as np
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta, timezone

from mainnet_launch.abis.abis import CHAINLINK_KEEPER_REGISTRY_ABI, AUTOPOOL_VAULT_ABI
from mainnet_launch.constants import (
    CACHE_TIME,
    ALL_AUTOPOOLS,
    AutopoolConstants,
    WORKING_DATA_DIR,
    ETH_CHAIN,
    ChainData,
)


from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.add_info_to_dataframes import (
    add_timestamp_to_df_with_block_column,
    add_transaction_gas_info_to_df_with_tx_hash,
)

from mainnet_launch.solver_diagnostics.fetch_rebalance_events import (
    fetch_and_clean_rebalance_between_destination_events,
)

KEEPER_REGISTRY_CONTRACT_ADDRESS = "0x6593c7De001fC8542bB1703532EE1E5aA0D458fD"


OLD_CALCULATOR_KEEPER_ORACLE_TOPIC_ID = "1344461886831441856282597505993515040672606510446374000438363195934269203116"
NEW_CALCULATOR_KEEPER_ORACLE_TOPIC_ID = "113129673265054907567420460651277872997162644350081440026681710279139531871240"
NEW2_CALCULATOR_KEEPER_ORACLE_TOPIC_ID = "93443706906332180407535184303815616290343141548650473059299738217546322242910"
INCENTIVE_PRICING_KEEPER_ORACLE_ID = "84910810589923801598536031507827941923735631663622593132512932471876788938876"


CALCULATOR_TOPIC_IDS = [
    OLD_CALCULATOR_KEEPER_ORACLE_TOPIC_ID,
    NEW_CALCULATOR_KEEPER_ORACLE_TOPIC_ID,
    NEW2_CALCULATOR_KEEPER_ORACLE_TOPIC_ID,
]
INCENTIVE_PRICING_TOPIC_IDS = [INCENTIVE_PRICING_KEEPER_ORACLE_ID]


def fetch_our_chainlink_upkeep_events() -> pd.DataFrame:
    contract = ETH_CHAIN.client.eth.contract(KEEPER_REGISTRY_CONTRACT_ADDRESS, abi=CHAINLINK_KEEPER_REGISTRY_ABI)
    upkeep_df = fetch_events(contract.events.UpkeepPerformed, ETH_CHAIN.block_autopool_first_deployed)
    our_chainlink_upkeep_events = upkeep_df[
        upkeep_df["id"].apply(str).isin([*CALCULATOR_TOPIC_IDS, *INCENTIVE_PRICING_TOPIC_IDS])
    ].copy()

    our_chainlink_upkeep_events = add_timestamp_to_df_with_block_column(our_chainlink_upkeep_events, ETH_CHAIN)
    return our_chainlink_upkeep_events


def fetch_keeper_network_gas_costs() -> pd.DataFrame:
    our_upkeep_df = fetch_our_chainlink_upkeep_events()
    our_upkeep_df = add_transaction_gas_info_to_df_with_tx_hash(our_upkeep_df, ETH_CHAIN)
    our_upkeep_df = add_timestamp_to_df_with_block_column(our_upkeep_df, ETH_CHAIN)
    # only count gas costs after mainnet launch on September 15
    our_upkeep_df = our_upkeep_df[our_upkeep_df.index >= pd.Timestamp("2024-09-15", tz="UTC")].copy()

    our_upkeep_df["gasCostInETH_with_chainlink_premium"] = our_upkeep_df["gasCostInETH"] * 1.2  # 20% premium
    our_upkeep_df["gasCostInETH_without_chainlink_overhead"] = our_upkeep_df["gasPrice"].astype(int) * our_upkeep_df[
        "gasUsed"
    ].apply(lambda x: int(x) / 1e18)

    return our_upkeep_df


def fetch_and_render_keeper_network_gas_costs():

    our_upkeep_df = fetch_keeper_network_gas_costs()

    st.header("Gas Costs")

    _display_gas_cost_metrics(our_upkeep_df)

    daily_gasPrice_box_and_whisker_fig = _daily_box_plot_of_gas_prices(our_upkeep_df)
    st.plotly_chart(daily_gasPrice_box_and_whisker_fig, use_container_width=True)

    hourly_gas_price_box_and_whisker_fig = _hourly_box_plot_of_gas_prices(our_upkeep_df)
    st.plotly_chart(hourly_gas_price_box_and_whisker_fig, use_container_width=True)

    eth_spent_per_day_fig = _make_gas_spent_df(our_upkeep_df)
    st.plotly_chart(eth_spent_per_day_fig, use_container_width=True)

    with st.expander("See explanation for Gas Costs"):
        st.write(
            """
        Top level metrics.

        - For Chainlink Keepers we pay (in LINK) the ETH cost of the transaction + a 20% premium. 
        - We don't pay a premium for the Solver because it is in-house.
        - Currently Keeper transactions are set to execute at any gas price. 
        - We can set a max gas price here https://docs.chain.link/chainlink-automation/guides/gas-price-threshold
        - This max price can be updated frequently
        )
        """
        )


def _display_gas_cost_metrics(our_upkeep_df: pd.DataFrame):
    calculator_df = our_upkeep_df[our_upkeep_df["id"].apply(str).isin([CALCULATOR_TOPIC_IDS])]
    incentive_pricing_df = our_upkeep_df[our_upkeep_df["id"].apply(str) == INCENTIVE_PRICING_KEEPER_ORACLE_ID]

    calculator_gas_costs_7, calculator_gas_costs_30, calculator_gas_costs_365 = get_gas_costs(
        calculator_df, "gasCostInETH_with_chainlink_premium"
    )
    incentive_gas_costs_7, incentive_gas_costs_30, incentive_gas_costs_365 = get_gas_costs(
        incentive_pricing_df, "gasCostInETH_with_chainlink_premium"
    )

    solver_cost_7, solver_cost_30, solver_cost_365 = fetch_solver_metrics()  # col3

    col1, col2, col3 = st.columns(3)

    col1.metric(label="Calculator Keeper ETH Cost (Last 7 Days)", value=f"{calculator_gas_costs_7:.4f} ETH")
    col1.metric(label="Calculator Keeper ETH Cost (Last 30 Days)", value=f"{calculator_gas_costs_30:.4f} ETH")
    col1.metric(label="Calculator Keeper ETH Cost (Last 1 Year)", value=f"{calculator_gas_costs_365:.4f} ETH")

    col2.metric(label="Incentive Keeper ETH Cost (Last 7 Days)", value=f"{incentive_gas_costs_7:.4f} ETH")
    col2.metric(label="Incentive Keeper ETH Cost (Last 30 Days)", value=f"{incentive_gas_costs_30:.4f} ETH")
    col2.metric(label="Incentive Keeper ETH Cost (Last 1 Year)", value=f"{incentive_gas_costs_365:.4f} ETH")

    col3.metric(label="Solver ETH Cost (Last 7 Days)", value=f"{solver_cost_7:.4f} ETH")
    col3.metric(label="Solver ETH Cost (Last 30 Days)", value=f"{solver_cost_30:.4f} ETH")
    col3.metric(label="Solver ETH Cost (Last 1 Year)", value=f"{solver_cost_365:.4f} ETH")


def get_gas_costs(df: pd.DataFrame, column: str):
    today = datetime.now(timezone.utc)

    return (
        df[df.index >= today - timedelta(days=7)][column].sum(),
        df[df.index >= today - timedelta(days=30)][column].sum(),
        df[df.index >= today - timedelta(days=365)][column].sum(),
    )


def _make_gas_spent_df(our_upkeep_df: pd.DataFrame):
    gas_spent_with_chainlink_premium = our_upkeep_df.resample("1D")["gasCostInETH_with_chainlink_premium"].sum()
    return px.bar(gas_spent_with_chainlink_premium, title="Total ETH spent per day on Chainlink Keepers")


def _daily_box_plot_of_gas_prices(our_upkeep_df: pd.DataFrame):
    daily_gas_price = our_upkeep_df.groupby(our_upkeep_df.index.date)["gasPrice"]
    daily_gas_price_df = daily_gas_price.apply(list).reset_index()
    daily_gas_price_df.columns = ["Date", "GasPrices"]
    exploded_df = daily_gas_price_df.explode("GasPrices")
    exploded_df["GasPrices"] = exploded_df["GasPrices"].astype(float)
    daily_gasPrice_box_and_whisker_fig = px.box(
        exploded_df, x="Date", y="GasPrices", title="Distribution of Gas Prices"
    )

    return daily_gasPrice_box_and_whisker_fig


def _hourly_box_plot_of_gas_prices(our_upkeep_df: pd.DataFrame):
    # Group by hour of the day and aggregate gas prices
    hourly_gas_price = our_upkeep_df.groupby(our_upkeep_df.index.hour)["gasPrice"]
    hourly_gas_price_df = hourly_gas_price.apply(list).reset_index()
    hourly_gas_price_df.columns = ["Hour", "GasPrices"]

    # Explode the gas prices into individual rows
    exploded_df = hourly_gas_price_df.explode("GasPrices")
    exploded_df["GasPrices"] = exploded_df["GasPrices"].astype(float)

    # Create the box plot for hourly distribution of gas prices
    hourly_gas_price_box_and_whisker_fig = px.box(
        exploded_df, x="Hour", y="GasPrices", title="UTC, Hourly Distribution of Gas Prices"
    )

    return hourly_gas_price_box_and_whisker_fig


def fetch_solver_metrics():
    rebalance_gas_cost_df = fetch_solver_gas_costs()
    today = datetime.now(timezone.utc)
    # Calculate costs over different periods
    cost_last_7_days = rebalance_gas_cost_df[rebalance_gas_cost_df.index >= today - timedelta(days=7)][
        "gasCostInETH"
    ].sum()
    cost_last_30_days = rebalance_gas_cost_df[rebalance_gas_cost_df.index >= today - timedelta(days=30)][
        "gasCostInETH"
    ].sum()
    cost_last_1_year = rebalance_gas_cost_df[rebalance_gas_cost_df.index >= today - timedelta(days=365)][
        "gasCostInETH"
    ].sum()

    return cost_last_7_days, cost_last_30_days, cost_last_1_year


def fetch_solver_gas_costs() -> pd.DataFrame:
    """Returns a dataframe of all the rebalanc events along with the gas costs"""
    # solver gas costs on base are near free
    clean_rebalance_df = pd.concat(
        [fetch_and_clean_rebalance_between_destination_events(a) for a in ALL_AUTOPOOLS if a.chain == ETH_CHAIN]
    )
    clean_rebalance_df = add_transaction_gas_info_to_df_with_tx_hash(clean_rebalance_df, ETH_CHAIN)
    clean_rebalance_df = add_timestamp_to_df_with_block_column(clean_rebalance_df, ETH_CHAIN)
    return clean_rebalance_df


def fetch_all_autopool_debt_reporting_events(chain: ChainData) -> pd.DataFrame:
    debt_reporting_dfs = []

    for autopool in ALL_AUTOPOOLS:
        if autopool.chain == chain:
            vault_contract = chain.client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
            destination_debt_reporting_df = fetch_events(vault_contract.events.DestinationDebtReporting)
            debt_reporting_dfs.append(destination_debt_reporting_df)

    destination_debt_reporting_df = pd.concat(debt_reporting_dfs)
    destination_debt_reporting_df = add_transaction_gas_info_to_df_with_tx_hash(destination_debt_reporting_df, chain)
    destination_debt_reporting_df = add_timestamp_to_df_with_block_column(destination_debt_reporting_df, chain)
    return destination_debt_reporting_df


if __name__ == "__main__":
    fetch_and_render_keeper_network_gas_costs()
