from concurrent.futures import ThreadPoolExecutor
import threading
import numpy as np
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

from mainnet_launch.abis.abis import CHAINLINK_KEEPER_REGISTRY_ABI
from mainnet_launch.constants import (
    CACHE_TIME,
    eth_client,
    ALL_AUTOPOOLS,
    BAL_ETH,
    AUTO_ETH,
    AUTO_LRT,
    AutopoolConstants,
)


from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    get_state_by_one_block,
    add_timestamp_to_df_with_block_column,
)


from mainnet_launch.solver_diagnostics.fetch_rebalance_events import (
    _calc_gas_used_by_transaction_in_eth,
    fetch_and_clean_rebalance_between_destination_events,
)

CALCULATOR_KEEPER_ORACLE_TOPIC_ID = "1344461886831441856282597505993515040672606510446374000438363195934269203116"
INCENTIVE_PRICING_KEEPER_ORACLE_ID = "84910810589923801598536031507827941923735631663622593132512932471876788938876"
# there can be more KEEPER IDs added later

KEEPER_REGISTRY_CONTRACT_ADDRESS = "0x6593c7De001fC8542bB1703532EE1E5aA0D458fD"
START_BLOCK = 20500000  # AUG 10, 2024


@st.cache_data(ttl=CACHE_TIME)
def fetch_keeper_network_gas_costs() -> pd.DataFrame:

    contract = eth_client.eth.contract(KEEPER_REGISTRY_CONTRACT_ADDRESS, abi=CHAINLINK_KEEPER_REGISTRY_ABI)

    upkeep_df = fetch_events(contract.events.UpkeepPerformed, START_BLOCK)
    our_upkeep_df = upkeep_df[
        upkeep_df["id"].apply(str).isin([CALCULATOR_KEEPER_ORACLE_TOPIC_ID, INCENTIVE_PRICING_KEEPER_ORACLE_ID])
    ].copy()
    our_upkeep_df = add_timestamp_to_df_with_block_column(our_upkeep_df)

    hash_to_gas_cost_in_ETH = {}
    hash_to_gasPrice = {}
    has_to_gas_used = {}

    lock = threading.Lock()

    def batch_calc_gas_used_by_transaction_in_eth(tx_hashes: list[str]):
        for tx_hash in tx_hashes:
            # add a while loop to make sure this works
            tx_receipt = eth_client.eth.get_transaction_receipt(tx_hash)
            tx = eth_client.eth.get_transaction(tx_hash)
            gas_cost_in_ETH = float(eth_client.fromWei(tx["gasPrice"] * tx_receipt["gasUsed"], "ether"))
            with lock:
                has_to_gas_used[tx_hash] = tx_receipt["gasUsed"]
                hash_to_gas_cost_in_ETH[tx_hash] = gas_cost_in_ETH
                hash_to_gasPrice[tx_hash] = tx["gasPrice"]

    with ThreadPoolExecutor(max_workers=24) as executor:
        hash_groups = np.array_split(our_upkeep_df["hash"], 100)
        for tx_hashes in hash_groups:
            executor.submit(batch_calc_gas_used_by_transaction_in_eth, tx_hashes)

    our_upkeep_df["gasCostInETH"] = our_upkeep_df["hash"].apply(lambda x: hash_to_gas_cost_in_ETH[x])
    our_upkeep_df["gasPrice"] = our_upkeep_df["hash"].apply(lambda x: hash_to_gasPrice[x])
    our_upkeep_df["full_tx_gas_used"] = our_upkeep_df["hash"].apply(lambda x: has_to_gas_used[x])
    our_upkeep_df["gasCostInETH_with_chainlink_premium"] = (
        our_upkeep_df["gasCostInETH"] * 1.2
    )  # it costs a 20% premium on ETH in overhead
    our_upkeep_df["gasCostInETH_without_chainlink_overhead"] = our_upkeep_df["gasPrice"].astype(int) * our_upkeep_df[
        "gasUsed"
    ].apply(lambda x: int(x) / 1e18)

    fetch_solver_gas_costs()  # just to add caching

    return our_upkeep_df


def fetch_and_render_keeper_network_gas_costs():

    our_upkeep_df = fetch_keeper_network_gas_costs()

    st.header("Operation Gas Costs")

    today = datetime.now()
    cost_last_7_days = our_upkeep_df[our_upkeep_df.index >= today - timedelta(days=7)][
        "gasCostInETH_with_chainlink_premium"
    ].sum()
    cost_last_30_days = our_upkeep_df[our_upkeep_df.index >= today - timedelta(days=30)][
        "gasCostInETH_with_chainlink_premium"
    ].sum()
    cost_last_1_year = our_upkeep_df[our_upkeep_df.index >= today - timedelta(days=365)][
        "gasCostInETH_with_chainlink_premium"
    ].sum()
    st.metric(label="Keeper ETH Cost (Last 7 Days)", value=f"{cost_last_7_days:.4f} ETH")
    st.metric(label="Keeper ETH Cost (Last 30 Days)", value=f"{cost_last_30_days:.4f} ETH")
    st.metric(label="Keeper ETH Cost (Last 1 Year)", value=f"{cost_last_1_year:.4f} ETH")

    fetch_and_render_solver_gas_costs()

    daily_gasPrice_box_and_whisker_fig = _daily_box_plot_of_gas_prices(our_upkeep_df)
    st.plotly_chart(daily_gasPrice_box_and_whisker_fig, use_container_width=True)

    hourly_gas_price_box_and_whisker_fig = _hourly_box_plot_of_gas_prices(our_upkeep_df)
    st.plotly_chart(hourly_gas_price_box_and_whisker_fig, use_container_width=True)


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
        exploded_df, x="Hour", y="GasPrices", title="Hourly Distribution of Gas Prices"
    )

    return hourly_gas_price_box_and_whisker_fig


def fetch_and_render_solver_gas_costs():

    rebalance_gas_cost_df = fetch_solver_gas_costs()
    today = datetime.now()
    cost_last_7_days = rebalance_gas_cost_df[rebalance_gas_cost_df.index >= today - timedelta(days=7)][
        "gasCostInETH"
    ].sum()
    cost_last_30_days = rebalance_gas_cost_df[rebalance_gas_cost_df.index >= today - timedelta(days=30)][
        "gasCostInETH"
    ].sum()
    cost_last_1_year = rebalance_gas_cost_df[rebalance_gas_cost_df.index >= today - timedelta(days=365)][
        "gasCostInETH"
    ].sum()
    st.metric(label="All Solvers ETH Cost (Last 7 Days)", value=f"{cost_last_7_days:.4f} ETH")
    st.metric(label="All Solvers ETH Cost (Last 30 Days)", value=f"{cost_last_30_days:.4f} ETH")
    st.metric(label="All Solvers ETH Cost (Last 1 Year)", value=f"{cost_last_1_year:.4f} ETH")


@st.cache_data(ttl=CACHE_TIME)
def fetch_solver_gas_costs():
    rebalance_dfs = []
    for autopool in ALL_AUTOPOOLS:
        clean_rebalance_df = fetch_and_clean_rebalance_between_destination_events(autopool)

        clean_rebalance_df["gasCostInETH"] = clean_rebalance_df.apply(
            lambda row: _calc_gas_used_by_transaction_in_eth(row["hash"]), axis=1
        )
        rebalance_dfs.append(clean_rebalance_df[["gasCostInETH"]])

    rebalance_gas_cost_df = pd.concat(rebalance_dfs, axis=0)
    return rebalance_gas_cost_df
